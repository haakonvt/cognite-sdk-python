from __future__ import annotations
from abc import abstractmethod
from pprint import pprint, pformat  # noqa
import math
import numbers
import operator as op
from typing import List, Union, Optional, Dict, NoReturn, Tuple, Callable
from functools import cached_property
from datetime import datetime
import enum
import dataclasses
import itertools
from dataclasses import dataclass, InitVar
from concurrent.futures import ThreadPoolExecutor, as_completed
# from timeit import default_timer as timer

from cognite.client._api.datapoints_extra import (
    align_window_start_and_end,
    find_count_granularity_for_query,
    chunk_queries_to_allowed_limits,
    single_datapoints_api_call,
    build_count_query_payload,
    handle_missing_ts,
    remove_string_ts,
)
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client.utils._time import granularity_to_ms, timestamp_to_ms
from cognite.client.data_classes import DatapointsQuery
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._auxiliary import to_camel_case

print("RUNNING REPOS/COG-SDK, NOT FROM PIP")
print("RUNNING REPOS/COG-SDK, NOT FROM PIP")

TIME_UNIT_IN_MS = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}

# Notes list
# - Union[int, float] is the same as `float`.

# TODO list
# - Should be a special method named "retrieve_string_datapoints"
# - Should be possible to specify `is_string` both as default and per-ts kwarg
# -

# Benchmark list
# - Set limit to e.g. 10 million
# - Set limit to 0 (zero), what happens? API accepts it
# - Try to break current "count-based" for aggregate-queries with some dense dps with at least 10k dps/day
#   using i.e. "25h" or higher as granularity
# - Fetch 10_001 aggs or 100_001 raw dps
# - client.datapoints.query() with a bunch of small queries

# Question list
# - Why fetch count aggregate for aggregates-queries? Just to pick up potential empty periods?


class NewDatapointsQuery(DatapointsQuery):
    def __init__(self, *args, **kwargs):
        self.client = kwargs.pop("client")
        super().__init__(*args, **kwargs)
        self.defaults = dict(
            start=self.start,  # Optional. Default: 1970-01-01
            end=self.end,  # Optional. Default: "now"
            limit=self.limit,
            aggregates=self.aggregates,
            granularity=self.granularity,
            include_outside_points=self.include_outside_points,
            ignore_unknown_ids=self.ignore_unknown_ids,
        )

    @cached_property  # TODO: 3.8 feature
    def all_validated_queries(self) -> TSQueryList:
        return self._validate_and_create_queries()

    def _validate_and_create_queries(self) -> TSQueryList:
        queries = []
        if self.id is not None:
            queries.extend(
                self._validate_id_or_xid(
                    id_or_xid=self.id, is_external_id=False, defaults=self.defaults,
                )
            )
        if self.external_id is not None:
            queries.extend(
                self._validate_id_or_xid(
                    id_or_xid=self.external_id, is_external_id=True, defaults=self.defaults,
                )
            )
        if queries:
            return TSQueryList(queries)
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
                queries.append(TSQuery.from_dict_with_validation({arg_name: ts}, self.client, self.defaults))

            elif isinstance(ts, dict):
                ts_validated = self._validate_ts_query_dct(ts, arg_name, exp_type)
                queries.append(TSQuery.from_dict_with_validation(ts_validated, self.client, self.defaults))
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
class TSQueryList:
    queries: List[TSQuery]

    def __post_init__(self):
        # We split because it is likely that a user asking for aggregates knows not to ask for
        # string time series, making the need for additional API calls less likely:
        split_qs = [], []
        for q in self.queries:
            split_qs[q.is_raw_query].append(q)
        self._aggregate_queries, self._raw_queries = split_qs

    @property
    def raw_queries(self):
        return self._raw_queries

    @property
    def aggregate_queries(self):
        return self._aggregate_queries


@dataclass
class TSQuery:
    client: InitVar[DatapointsAPI]
    id: Optional[int] = None
    external_id: Optional[str] = None
    start: Union[int, str, datetime, None] = None
    end: Union[int, str, datetime, None] = None
    granularity: Optional[str] = None
    include_outside_points: Optional[bool] = None
    limit: Optional[int] = None
    aggregates: Optional[List[str]] = None
    ignore_unknown_ids: Optional[bool] = None

    def __post_init__(self):
        self._DPS_LIMIT_AGG = self.client._DPS_LIMIT_AGG
        self._DPS_LIMIT = self.client._DPS_LIMIT
        del self.client
        self._is_missing = None  # I.e. not set...
        self._is_string = None  # ...or unknown
        self._verify_time_range()
        self._verify_limit()
        self._verify_identifier()

    def get_count_query_params(self):
        if self.is_raw_query:
            # With a maximum of millisecond resolution, we peak at 1k dps/sec. Realistically
            # though, most time series are far less dense. We make a guess of '1s' (prob too high even):
            granularity = "1s"  # TODO: Can be tweaked with real-world data?
        else:  # Aggregates have at most 1 dp/gran (and maxes out at 1 dp/sec):
            granularity = self.granularity
        limit, count_gran = find_count_granularity_for_query(
            self.start, self.end, granularity, self.limit, self.max_query_limit,
        )
        return {
            **self.identifier_dct,
            "start": self.start,
            "end": self.end,
            "granularity": count_gran,
            "limit": limit,
        }

    def _verify_identifier(self):
        if self.id is not None:
            self.identifier_type = "id"
            self.identifier = int(self.id)
        elif self.external_id is not None:
            self.identifier_type = "externalId"
            self.identifier = str(self.external_id)
        else:
            raise ValueError("Pass exactly one of `id` or `external_id`. Got neither.")
        # Shortcuts for hashing and API queries:
        self.identifier_tpl = (self.identifier_type, self.identifier)
        self.identifier_dct = {self.identifier_type: self.identifier}

    def _verify_limit(self):
        if self.limit in {None, -1, math.inf}:
            self.limit = None
        elif isinstance(self.limit, numbers.Integral):  # limit=0 is accepted by the API
            self.limit = int(self.limit)  # We don't want weird stuff like numpy dtypes etc.
        else:
            raise TypeError(f"Limit must be an integer or one of [None, -1, inf], got {type(self.limit)}")

    def _verify_time_range(self):
        if self.start is None:
            self.start = 0  # 1970-01-01
        else:
            self.start = timestamp_to_ms(self.start)
        if self.end is None:
            self.end = "now"
        self.end = timestamp_to_ms(self.end)

        if self.end <= self.start:
            raise ValueError("Invalid time range, `end` must be later than `start`")

        if not self.is_raw_query:  # API rounds aggregate queries in a very particular fashion
            self.start, self.end = align_window_start_and_end(self.start, self.end, self.granularity)

    @property
    def is_missing(self):
        if self._is_missing is None:
            raise RuntimeError("Before making API-calls the `is_missing` status is unknown")
        return self._is_missing

    @is_missing.setter
    def is_missing(self, value):
        assert isinstance(value, bool)
        self._is_missing = value

    @property
    def is_string(self):
        return self._is_string

    @is_string.setter
    def is_string(self, value):
        assert isinstance(value, bool)
        self._is_string = value

    @property
    def is_raw_query(self):
        return self.aggregates is None

    @property
    def max_query_limit(self):
        if self.is_raw_query:
            self._DPS_LIMIT  # 100k
        return self._DPS_LIMIT_AGG  # 10k

    @classmethod
    def from_dict_with_validation(cls, ts_dct, client, defaults) -> TSQuery:
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
            raise ValueError("When passing `granularity`, argument `aggregates` is also required.")

        # Aggregates must be a list at this point:
        elif len(aggregates) == 0:
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")

        elif granularity is None:
            raise ValueError("When passing `aggregates`, argument `granularity` is also required.")

        elif dct["include_outside_points"] is True:
            raise ValueError("'Include outside points' is not supported for aggregates.")
        return cls(**dct)  # Request for one or more aggregates

    def __repr__(self):
        # TODO(haakonvt): Remove
        s = ", ".join(
            f'{field.name}={getattr(self, field.name)!r}'
            for field in dataclasses.fields(self)
            if field.name == "limit" or getattr(self, field.name) is not None
        )
        return f'{type(self).__name__}({s})'


class DpsTaskType(enum.Enum):
    CREATE_TASKS = enum.auto()
    DATAPOINTS = enum.auto()


def dps_fetch_strategy_selector(query: TSQuery, parallel: bool) -> BaseDpsTask:
    selector = (
        query.is_raw_query,
        parallel,
        bool(query.is_string),  # Status is typically unknown, except where checked to be True
    )
    return {
        # raw parall string
        (True, True, True): RawParallelDpsTask,
        (True, True, False): RawParallelDpsTask,
        (True, False, True): RawSerialDpsTask,
        (True, False, False): RawSerialDpsTask,
        # (None, None, None, None): None,
    }[selector]


@dataclass
class BaseDpsTask:
    query: TSQuery
    client: DatapointsAPI
    is_done: bool = dataclasses.field(default=False, init=False)
    n_dps_fetched: int = dataclasses.field(default=0, init=False)
    dps_lsts: List[List[Union[Tuple, List[Tuple]]]] = dataclasses.field(default_factory=list, init=False)

    # TODO: If python allows defining abstract dataclasses (easily) at some point, change this class:
    #       https://github.com/python/mypy/issues/5374  (workaround too ugly lol)
    @abstractmethod
    def get_result(self, finalize):
        ...


class RawDpsTask(BaseDpsTask):
    offset_next: int = dataclasses.field(default=1, init=False)
    unpack_fn: Callable[[str, str], Tuple[int, Union[str, float]]] = dataclasses.field(
        default=op.itemgetter("timestamp", "value"), init=False  # NB: timestamp must be first
    )


class SerialDpsTask(BaseDpsTask):
    def get_result(self, finalize=True):
        if self.is_done:
            res = itertools.chain.from_iterable(self.dps_lsts)
            return list(res) if finalize else res
        raise RuntimeError("Datapoints task asked for final result before fetching was done")


class ParallelDpsTask(BaseDpsTask):
    def get_result(self, finalize=True):
        if self.is_done:
            res = itertools.chain.from_iterable(dps.get_result(finalize=False) for dps in self.dps_lsts)
            return list(res) if finalize else res
        raise RuntimeError("Datapoints task asked for final result before fetching was done")


class RawSerialDpsTask(SerialDpsTask, RawDpsTask):
    """A raw datapoints fetching task for numeric and string data that executes in serial"""

    def __post_init__(self):
        self.is_first_query = True
        self.n_dps_left = self.query.limit
        self.next_start = self.query.start

    def _create_payload_item(self):
        return {
            **self.query.identifier_dct,
            "start": self.next_start,
            "end": self.query.end,
            "includeOutsidePoints": self.query.include_outside_points,
            "limit": min(self.n_dps_left, self.query._DPS_LIMIT),
         }

    def get_next_task(self):
        if self.is_done:
            return None
        return self._create_payload_item()

    def store_partial_result(self, res):
        if not res:
            self.is_done = True
            return

        first_idx = 0
        if self.query.include_outside_points and not self.is_first_query:
            # For all queries -not the first-, we might need to chop off the first dp.
            start_ts = res[0]["timestamp"]
            prev_last_ts = self.n_dps_fetched[-1][-1][0]  # Timestamp is first entry
            if start_ts == prev_last_ts:
                first_idx = 1
        self.is_first_query = False

        last_idx = None
        self.n_dps_left = min(0, self.n_dps_left - len(res) + first_idx)
        if not self.n_dps_left:
            self.is_done = True
            return

        last_idx = self.n_dps_left + first_idx
        if first_idx or last_idx:
            # List slices copies the ref. array; minor speedup using islice:
            res = itertools.islice(res, first_idx, last_idx)
        res = list(map(self.unpack_fn, res))
        self.dps_lsts.append(res)
        self.next_start = res[-1][0] + self.offset_next
        self.n_dps_fetched += len(res)

        self.is_done = ...  # TODO(haakonvt): ARE WE DONE?


class RawParallelDpsTask(ParallelDpsTask):
    """A raw datapoints fetching task for numeric and string data that executes in parallel"""
    # self.offset_next = granularity_to_ms(self.query.granularity)
    # self.unpack_fn = op.itemgetter("timestamp", *self.query.aggregates)


class DpsFetchOrchestrator:
    def __init__(self, client):
        self.client = client

    # "ignoreUnknownIds": False,  # If a ts is deleted while fetching, we want error instead of a part result

    @staticmethod
    def generate_string_ts_tasks(queries):
        print("- NOT IMPLEMENTED: `generate_string_ts_tasks`")
        tasks = []
        return tasks

    @staticmethod
    def generate_ts_tasks(queries, res):
        tasks = []
        for q, r in zip(queries, res):
            assert r[q.identifier_type] == q.identifier, "Counts belong to wrong time series."
            counts = r["datapoints"]
            approx_tot = sum(dp["count"] for dp in counts)
            if approx_tot < q.max_query_limit:
                # We are expecting a full fetch in a single request
                tasks.append((SerialDpsTask(q), DpsTaskType.DATAPOINTS))
        return tasks

    def get_available_counts(self, queries, count_query):
        try:
            res, string_qs = [], []
            res = single_datapoints_api_call(self.client, count_query)
        except CogniteAPIError as e:
            # Note: We do not get '400-IDs not found', because count-query uses 'ignore unknown'
            if e.code != 400:
                raise
            # Likely: "Aggregates are not supported for string time series"
            keep_items, queries, string_qs = remove_string_ts(self.client, queries, count_query["items"])
            if keep_items:
                res = single_datapoints_api_call(self.client, {**count_query, "items": keep_items})

        # With string-ts removed, we can assume any still missing as not-existing:
        queries, missing = handle_missing_ts(res, queries)
        return queries, string_qs, missing, res

    def create_tasks_from_counts(self, queries, count_query):
        queries, string_qs, missing, counts_dps = self.get_available_counts(queries, count_query)

        # Create dps fetch tasks for string first (since we cannot parallelize fetching cleverly with counts):
        tasks = []
        if string_qs:
            tasks.extend(self.generate_string_ts_tasks(string_qs))
        if queries:
            tasks.extend(self.generate_ts_tasks(queries, counts_dps))
        print(f"{tasks = }")
        input("...")
        return tasks

    # def handle_and_create_tasks_from_new_dps(queries, new_dps):
    #     for q, dps in zip(queries, new_dps):
    #         self.data_dct[q].store_partial_result(dps)


def count_based_task_splitting(query_lst, client, max_workers=10):
    dps_orchestrator = DpsFetchOrchestrator(client)
    # Set up pool using `max_workers` using at least 1 thread:
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        futures_dct = {}
        for queries in (query_lst.raw_queries, query_lst.aggregate_queries):
            if not queries:
                continue
            qs_it = iter(queries)
            count_tasks = build_count_query_payload(queries)
            # Smash together as many of the count-aggregate requets as possible:
            for count_task_chunk in chunk_queries_to_allowed_limits(count_tasks):
                qs_chunk = list(itertools.islice(qs_it, len(count_task_chunk["items"])))
                future = pool.submit(dps_orchestrator.create_tasks_from_counts, qs_chunk, count_task_chunk)
                futures_dct[future] = DpsTaskType.CREATE_TASKS

        for future in as_completed(futures_dct):
            print("Calling .result() in 1")
            print(f"{future.result() = }")
        raise SystemExit(0)

        # while futures_dct:
        #     new_futures_dct = {}
        #     # Queue up new work as soon as possible by using `as_completed`:
        #     for future in as_completed(futures_dct):
        #         res = future.result()
        #         task_type = futures_dct[future]
        #         if task_type is DpsTaskType.CREATE_TASKS:
        #             new_tasks = res
        #         elif task_type is DpsTaskType.DATAPOINTS:
        #             new_tasks = dps_orchestrator.handle_and_create_tasks_from_new_dps(res)
        #         else:
        #             raise ValueError(f"Task type not understood, expected {DpsTaskType}, not {task_type}")
        #
        #         for new_task in new_tasks:
        #             future = pool.submit(*new_task)
        #             new_futures_dct[future] = new_task.task_type
        #         # Swap
        #         futures_dct = new_futures_dct


if __name__ == "__main__":
    # Specify the aggregates to return. Use default if null.
    # If the default is a set of aggregates,
    # specify an empty string to get raw data.
    START = None
    END = None
    AGGREGATES = None  # ["average"]
    GRANULARITY = None  # "12h"
    INCLUDE_OUTSIDE_POINTS = None
    LIMIT = None
    IGNORE_UNKNOWN_IDS = True
    # IGNORE_UNKNOWN_IDS = False
    ID = None
    ID = [
        {"id": 226740051491},
        {"id": 2546012653669},  # string
        {"id": 1111111111111},  # prob missing...
        # {"id": 2546012653669, "aggregates": ["max", "average"], "granularity": "1d"},  # string
    ]
    EXTERNAL_ID = [
        {"limit": None, "external_id": "ts-test-#01-daily-111/650"},
        {"limit": None, "external_id": "ts-test-#01-daily-222/650"},
        {"limit": None, "external_id": "ts-test-#01-daily-64/650"},
        {"limit": None, "external_id": "ts-test-#01-daily-444/650"},
        {"limit": None, "external_id": "8400074_destination"},  # string
        {"limit": None, "external_id": "9624122_cargo_type"},  # string
        {"limit": None, "external_id": "ts-test-#01-daily-651/650"},  # missing
    ]
    from local_cog_client import setup_local_cog_client
    client = setup_local_cog_client()
    query = NewDatapointsQuery(
        client=client.datapoints,
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
    q = query.all_validated_queries
    pprint(q)
    count_based_task_splitting(q, client, max_workers=1)
