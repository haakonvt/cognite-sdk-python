from __future__ import annotations
from abc import ABC
from pprint import pprint, pformat
import math
import numbers
from typing import List, Union, Optional, Dict, NoReturn
from functools import partial, cached_property
from datetime import datetime
import enum
import dataclasses
import itertools
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
# from timeit import default_timer as timer

from cognite.client.utils._time import granularity_to_ms, timestamp_to_ms, granularity_unit_to_ms
from cognite.client.data_classes import DatapointsQuery
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils._auxiliary import to_camel_case

print("RUNNING REPOS/COG-SDK, NOT FROM PIP")

TIME_UNIT_IN_MS = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}

"""
print(f"\nALL PARSED TS:\n {pformat(sorted({q.identifier_tpl for q in queries}), indent=4, width=135, sort_dicts=False)}")
print(f"\nINFERRED STRING TS:\n{pformat(sorted({q.identifier_tpl for q in string_qs}), indent=4, width=135, sort_dicts=False)}")
print(f"\nINFERRED MISSING TS:\n{pformat(sorted({q.identifier_tpl for q in missing}), indent=4, width=135, sort_dicts=False)}")
print(f"\nAFTER REMOVING MISSING TS:\n{pformat(sorted({q.identifier_tpl for q in queries}), indent=4, width=135, sort_dicts=False)}")
"""


def align_window_start_and_end(start: int, end: int, granularity: str) -> Tuple[int, int]:
    # Note the API always aligns `start` with 1s, 1m, 1h or 1d (even when given e.g. 73h)
    remainder = start % granularity_unit_to_ms(granularity)
    if remainder:
        # Floor `start` when not exactly at boundary
        start -= remainder
    gms = granularity_to_ms(granularity)
    remainder = (end - start) % gms
    if remainder:
        # Ceil `end` when not exactly at boundary decided by `start + N * granularity`
        end += gms - remainder
    return start, end


def find_count_granularity_for_query(
    start: int, end: int, granularity: str, limit: Optional[int], dps_api_limit: int
) -> Tuple[int, str]:
    td = end - start
    if limit is not None:
        max_dps = limit
    else:
        # If every single period of aggregate dps exist, we get a maximum number of dps:
        max_dps = max(1, td / granularity_to_ms(granularity))

    # We want to parallelize requests, each maxing out at `dps_api_limit`. When asking for all the
    # `count` aggregates, we want to speed this up by grouping time series (thus we cap at 1k):
    n_timedeltas = min(1000, math.ceil(max_dps / dps_api_limit))  # This will become the `limit`
    gran_ms = min(td, td / n_timedeltas)
    if gran_ms < 120 * TIME_UNIT_IN_MS["s"]:
        n = math.ceil(gran_ms / TIME_UNIT_IN_MS["s"])
        return n_timedeltas, f"{n}s",

    elif gran_ms < 120 * TIME_UNIT_IN_MS["m"]:
        n = math.ceil(gran_ms / TIME_UNIT_IN_MS["m"])
        return n_timedeltas, f"{n}m",

    elif gran_ms < 100_000 * TIME_UNIT_IN_MS["h"]:
        n = math.ceil(gran_ms / TIME_UNIT_IN_MS["h"])
        return n_timedeltas, f"{n}h",

    elif gran_ms < 100_000 * TIME_UNIT_IN_MS["d"]:
        n = math.ceil(gran_ms / TIME_UNIT_IN_MS["d"])
        return n_timedeltas, f"{n}d",
    else:
        # Not possible with current TimeSeriesAPI in v1. To futureproof for potential increase
        # of time range, we return max granularity and an a wild overestimate of required time windows:
        return 1000, "100000d"  # 274k years. Current API limit is 80 years...


def chunk_queries_to_allowed_limits(payload, max_items=100, max_dps=10_000):
    chunk, n_items, n_dps = [], 0, 0
    for item in payload.pop("items"):
        # If limit not given per item, we require default to exist (if not, raise KeyError):
        dps_limit = item.get("limit", payload["limit"])
        if dps_limit is None:  # Need explicit None-check because falsy 0 is an allowed limit
             dps_limit = max_dps
        if n_items + 1 > max_items or n_dps + dps_limit > max_dps:
            yield {**payload, "items": chunk}
            chunk, n_items, n_dps = [], 0, 0
        chunk.append(item)
        n_items += 1
        n_dps += dps_limit
    if chunk:
        yield {**payload, "items": chunk}


def single_datapoints_api_call(client, payload):
    return (
        client.datapoints._post(client.datapoints._RESOURCE_PATH + "/list", json=payload).json()
    )["items"]


def build_count_query_payload(queries):
    return {
        "aggregates": ["count"],
        "ignoreUnknownIds": True,  # Avoids a potential extra query
        "items": [q.get_count_query_params() for q in queries],
    }


def handle_missing_ts(res, queries):
    missing = []
    not_missing = {("id", r["id"]) for r in res}.union(("externalId", r["externalId"]) for r in res)
    for q in queries:
        q.is_missing = q.identifier_tpl not in not_missing
        if q.is_missing:
            missing.append(q)
    # We might be handling multiple simultaneous top-level queries, each with
    # different settings for "ignore unknown":
    missing_to_raise = [q.identifier_dct for q in missing if q.is_missing and not q.ignore_unknown_ids]
    if missing_to_raise:
        raise CogniteNotFoundError(not_found=missing)
    return [q for q in queries if not q.is_missing], missing


def handle_string_ts(string_ts, queries):
    not_supported_qs = []
    for q in queries:
        id_type, identifier = q.identifier_tpl
        q.is_string = identifier in string_ts[id_type]
        if q.is_string and not q.is_raw_query:
            not_supported_qs.append(q.identifier_dct)
    if not_supported_qs:
        raise ValueError(
            f"Aggregates are not supported for string time series: {not_supported_qs}"
        ) from None


def get_is_string_property(client, queries):
    # We do not know if duplicates exist between those given by `id` and `external_id`.
    # Quick fix is to send two separate queries ಠಿ_ಠ
    futures = []
    # TODO(haakonvt): Dont really want another TPE inside here:
    with ThreadPoolExecutor(max_workers=2) as pool:
        for identifier_type in ["id", "externalId"]:
            items = set(q.identifier for q in queries if q.identifier_type == identifier_type)
            # Note: We do not call client.time_series.retrieve_multiple since it spins up
            # a separate thread pool:
            future = pool.submit(
                client.time_series._post,
                url_path="/timeseries/byids",
                json={
                    "items": [{identifier_type: itm} for itm in items],
                    "ignoreUnknownIds": True
                }
            )
            futures.append(future)
    return {
        "id": {ts["id"] for ts in futures[0].result().json()["items"] if ts["isString"]},
        "externalId": {ts["externalId"] for ts in futures[1].result().json()["items"] if ts["isString"]},
    }


def remove_string_ts(client, queries, items):
    string_ts = get_is_string_property(client, queries)
    handle_string_ts(string_ts, queries)
    keep = {q.identifier_tpl for q in queries if not q.is_string}
    keep_items = [
        ic for ic in items
        # TODO: Can time series have external_id=None? Might need ic.get("xid", NOT_NONE)
        if ("id", ic.get("id")) in keep or ("externalId", ic.get("externalId")) in keep
    ]
    string_qs = [q for q in queries if q.is_string]
    queries = [q for q in queries if not q.is_string]
    return keep_items, queries, string_qs
