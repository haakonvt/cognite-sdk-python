from __future__ import annotations
from abc import ABC
from pprint import pprint
import numbers
from typing import List, Union, Optional, Dict, NoReturn
from functools import partial, cached_property
from datetime import datetime
import dataclasses
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
# from timeit import default_timer as timer

from cognite.client.utils._time import granularity_to_ms, timestamp_to_ms, granularity_unit_to_ms
from cognite.client.data_classes import DatapointsQuery
from cognite.client.utils._auxiliary import to_camel_case

print("RUNNING REPOS/COG-SDK, NOT FROM PIP\n")
print("RUNNING REPOS/COG-SDK, NOT FROM PIP\n")


class NewDatapointsQuery(DatapointsQuery):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.defaults = dict(
            start=self.start,  # Optional. Default: 1970-01-01
            end=self.end,  # Optional. Default: "now"
            limit=self.limit,
            aggregates=self.aggregates,
            granularity=self.granularity,
            include_outside_points=self.include_outside_points,
        )

    @cached_property  # TODO: 3.8 feature
    def all_queries(self):
        return self._validate_and_create_queries()

    def _validate_and_create_queries(self):
        all_queries = []
        if self.id is not None:
            all_queries.extend(
                self._validate_id_or_xid(
                    id_or_xid=self.id, is_external_id=False, defaults=self.defaults,
                )
            )
        if self.external_id is not None:
            all_queries.extend(
                self._validate_id_or_xid(
                    id_or_xid=self.external_id, is_external_id=True, defaults=self.defaults,
                )
            )
        if all_queries:
            return all_queries
        raise ValueError("Pass at least one time series `id` or `external_id`!")

    def _validate_id_or_xid(self, id_or_xid, is_external_id: bool, defaults: Dict):
        if is_external_id:
            arg_name, exp_type = "external_id", str
        else:
            arg_name, exp_type = "id", numbers.Integral

        if isinstance(id_or_xid, (exp_type, dict)):
            id_or_xid = [id_or_xid]

        if not isinstance(id_or_xid, list):
            self._raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type)

        queries = []
        for ts in id_or_xid:
            if isinstance(ts, exp_type):
                queries.append(SingleTSQuery.from_dict_with_validation({arg_name: ts}, defaults=self.defaults))

            elif isinstance(ts, dict):
                ts_validated = self._validate_ts_query_dct(ts, arg_name, exp_type)
                queries.append(SingleTSQuery.from_dict_with_validation(ts_validated, defaults=self.defaults))
            else:
                self._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
        return queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type) -> NoReturn:
        raise TypeError(
            f"Got unsupported type {type(id_or_xid)}, as, or part of argument `{arg_name}`. Expected one of "
            f"{exp_type}, {dict} or a (mixed) list of these, but got `{id_or_xid}`."
        )

    @staticmethod
    def _validate_ts_query_dct(dct, arg_name, exp_type):
        if arg_name not in dct:
            if to_camel_case(arg_name) in dct:
                # For backwards compatability we accept identifier in camel case:
                dct = dct.copy()  # Avoid side effects for user's input. Also means we need to return it.
                dct[arg_name] = dct.pop(to_camel_case(arg_name))
            else:
                raise KeyError(f"Missing key `{arg_name}` in dict passed as, or part of argument `{arg_name}`")

        ts_identifier = dct[arg_name]
        if not isinstance(ts_identifier, exp_type):
            NewDatapointsQuery._raise_on_wrong_ts_identifier_type(ts_identifier, arg_name, exp_type)

        opt_dct_keys = {"start", "end", "aggregates", "granularity", "include_outside_points", "limit"}
        bad_keys = set(dct) - opt_dct_keys - {arg_name}
        if not bad_keys:
            return dct
        raise KeyError(
            f"Dict provided by argument `{arg_name}` included key(s) not understood: {sorted(bad_keys)}. "
            f"Required key: `{arg_name}`. Optional: {list(opt_dct_keys)}."
        )


@dataclass
class SingleTSQuery:
    id: Optional[int] = None
    external_id: Optional[str] = None
    start: Union[int, str, datetime, None] = None
    end: Union[int, str, datetime, None] = None
    granularity: Optional[str] = None
    include_outside_points: Optional[bool] = None
    limit: Optional[int] = None
    aggregation: Optional[List[str]] = None

    def get_count_agg_parameters(self):
        if self.is_raw_query:
            # Millsecond resolution means at most 1k dps/sec
            count_gran = None
        else:
            # Aggregates have at most 1 dp/gran (and maxes out at 1 dp/sec)
            count_gran = None
        return {
            **self._identifier_dct,
            "start": start,
            "end": end,
            "granularity": count_gran,
        }

    def __post_init__(self):
        self._is_missing = None  # I.e. not set
        self.start, self.end = self.verify_time_range(self.start, self.end)
        if self.id is not None:
            self._identifier_dct = {"id": self.id}
        elif self.external_id is not None:
            self._identifier_dct = {"external_id": self.external_id}
        raise ValueError("Pass exactly one of `id` or `external_id`. Got neither.")

    @staticmethod
    def verify_time_range(start, end):
        if start is None:
            start = 0
        else:
            start = timestamp_to_ms(start)
        if end is None:
            end = "now"
        end = timestamp_to_ms(end)

        if end <= start:
            raise ValueError("Invalid time range, `end` must be later than `start`")
        return start, end

    @property
    def is_missing(self):
        return self._is_missing

    @is_missing.setter
    def is_missing(self, value):
        assert isinstance(value, bool)
        self._is_missing = value

    @property
    def is_raw_query(self):
        return self.aggregation is None

    @classmethod
    def from_dict_with_validation(cls, ts_dct, defaults) -> List[SingleTSQuery]:
        # We merge 'defaults' and given ts-dict, ts-dict takes precedence:
        dct = {**defaults, **ts_dct}
        granularity, aggregates = dct["granularity"], dct["aggregates"]

        if not (granularity is None or isinstance(granularity, str)):
            raise TypeError(f"Expected `granularity` to be of type `str` or None, not {type(granularity)}")

        elif not (aggregates is None or isinstance(aggregates, list)):
            raise TypeError(f"Expected `aggregates` to be of type `list[str]` or None, not {type(aggregates)}")

        elif aggregates is None:
            if granularity is None:
                return cls(**dct)  # Request for raw datapoints
            raise KeyError(f"When passing `granularity`, argument `aggregates` is also required.")

        # Aggregates must be a list at this point:
        elif len(aggregates) == 0:
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")

        elif granularity is None:
            raise KeyError(f"When passing `aggregates`, argument `granularity` is also required.")

        elif dct["include_outside_points"] is True:
            raise ValueError("'Including outside points' is not supported for aggregates")
        return cls(**dct)  # Request for one or more aggregates

    def __repr__(self):
        # TODO(haakonvt): REMOVE
        s = ", ".join(
            f'{field.name}={getattr(self, field.name)!r}'
            for field in dataclasses.fields(self)
            if getattr(self, field.name) is not None
        )
        return f'{type(self).__name__}({s})'


# def sequence_split_gen(seq, n):
#     yield from (seq[i:i + n] for i in range(0, len(seq), n))

def chunk_queries_to_allowed_limits(payload, max_items=100, max_dps=10_000):
    chunk, n_items, n_dps = [], 0, 0
    for item in payload.pop("items"):
        # If limit not given per item, we require default to exist (if not, raise KeyError):
        dps_limit = item.get("limit") or payload["limit"]
        if n_items + 1 > max_items or n_dps + dps_limit > max_dps:
            yield {**payload, "items": chunk}
            chunk, n_items, n_dps = [], 0, 0
        chunk.append(item)
        n_items += 1
        n_dps += dps_limit
    if chunk:
        yield {**payload, "items": chunk}


def single_datapoints_api_call(client, payload):
    res = client._post(client._RESOURCE_PATH + "/list", json=payload).json()["items"]


def build_count_query_payload(queries):
    return {
        "aggregates": ["count"],
        "ignoreUnknownIds": True,  # Avoids a potential extra query
        "items": [q.get_count_agg_parameters() for q in queries]
    }


def get_counts_and_create_tasks(queries, client):
    payload = build_count_query_payload(queries)
    res = single_datapoints_api_call(client, payload)


def count_based_task_splitting(queries, client, max_workers=10):
    agg_queries, raw_queries = [], []
    for q in queries:
        (agg_queries, raw_queries)[q.is_raw_query].append(q)

    # Set up pool using `max_workers` using at least 1 thread:
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        futures = []
        for queries in (raw_queries, agg_queries):
            if not queries:
                continue
            count_tasks = get_counts_and_create_tasks(queries, client)
            # futures.append(
            #     pool.submit(get_counts_and_create_tasks, q_grp, client)
            # )


        futures = [pool.submit(do_stuff, x) for x in range(1, 6)]
        new_futures = []
        while True:
            for task in as_completed(futures):






class DpsFetchOrchestrator:
    def __init__(self, queries: List[SingleTSQuery]):
        self.queries = queries

    def plan_and_execute_queries(self, override_strategy=None):
        if override_strategy is not None:
            return _fetch_datapoints_until_completion(strategy)
        info = self._inspect_queries()
        strategy = self._select_fetch_strategy(info)
        return _fetch_datapoints_until_completion(strategy)

    def _inspect_queries(self):
        for q in self.queries:
            pass

    def _select_fetch_strategy(self, info):
        # Number of ts >>
        return

    def _fetch_datapoints_until_completion(self, strategy):
        # Pseudo-code for illustration:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(do_stuff, x) for x in range(1, 6)]
            new_futures = []
            while True:
                for task in as_completed(futures):
                    res = task.result()
                    if isinstance(res, NewDPSTask):
                        new_futures.append(pool.submit(do_stuff, f"res-{res}"))
                if not new_futures:
                    return final_result
                futures, new_futures = new_futures, []



if __name__ == "__main__":
    # Specify the aggregates to return. Use default if null.
    # If the default is a set of aggregates,
    # specify an empty string to get raw data.
    START = None
    END = None
    AGGREGATES = ["count"]
    GRANULARITY = "4d"
    INCLUDE_OUTSIDE_POINTS = None
    LIMIT = None
    IGNORE_UNKNOWN_IDS = False
    ID = 12345678
    # ID = [
    #     12345678,
    #     {"id": 123, "aggregates": ["count", "average"]},
    # ]
    EXTERNAL_ID = [
        # "xiiiiiiid",
        # {"external_id": "asdf"},
        # {"externalId": "ASDF", "limit": -1, "granularity": "8h"},
        # None,
    ]
    EXTERNAL_ID = [{"limit": 4, "external_id": "FUCK"}]

    query = NewDatapointsQuery(
        start=START,
        end=END,
        id=ID,
        external_id=EXTERNAL_ID,
        aggregates=AGGREGATES,
        granularity=GRANULARITY,
        include_outside_points=INCLUDE_OUTSIDE_POINTS,
        limit=LIMIT,
        ignore_unknown_ids=IGNORE_UNKNOWN_IDS,
    )
    q = query.all_queries
    pprint(q)
