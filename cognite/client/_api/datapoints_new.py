from __future__ import annotations
from pprint import pprint
import numbers
from typing import List, Union, Optional, Dict, NoReturn
from datetime import datetime
import dataclasses
from dataclasses import dataclass

from cognite.client.data_classes import DatapointsQuery
from cognite.client.utils._auxiliary import to_camel_case

print("RUNNING REPOS/COG-SDK, NOT FROM PIP\n")
print("RUNNING REPOS/COG-SDK, NOT FROM PIP\n")


def dps_new(query):
    return


class NewDatapointsQuery(DatapointsQuery):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.defaults = dict(
            start=self.start,
            end=self.end,
            limit=self.limit,
            aggregates=self.aggregates,
            granularity=self.granularity,
            include_outside_points=self.include_outside_points,
        )
        self.all_queries = self.validate_and_create_queries()

    def validate_and_create_queries(self):
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
        if not all_queries:
            raise ValueError("Pass at least one time series `id` or `external_id`!")
        return self._split_queries_into_single_aggs(all_queries)

    @staticmethod
    def _split_queries_into_single_aggs(queries):
        final_queries = []
        for q in queries:
            if isinstance(q.aggregates, list) and len(q.aggregates) > 1:
                for agg in q.aggregates:
                    q_new = SingleTSQuery(**dataclasses.asdict(q))
                    q_new.aggregates = [agg]
                    final_queries.append(q_new)
            else:
                final_queries.append(q)
        return final_queries

    def _validate_id_or_xid(self, id_or_xid, is_external_id: bool, defaults: Dict):
        if is_external_id:
            arg_name, exp_type = "external_id", str
        else:
            arg_name, exp_type = "id", numbers.Integral

        if isinstance(id_or_xid, (exp_type, dict)):
            id_or_xid = [id_or_xid]

        if not isinstance(id_or_xid, list):
            self._raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type)
        single_ts_queries = []
        for ts in id_or_xid:
            if isinstance(ts, exp_type):
                single_ts_queries.extend(
                    SingleTSQuery.create_multiple_from_dicts({arg_name: ts}, defaults=self.defaults)
                )
            elif isinstance(ts, dict):
                # Note: merge 'defaults' and given ts-dict; ts-dict takes precedence:
                ts_dct = {**defaults, **ts}
                self._validate_ts_query_dct(ts_dct, arg_name, exp_type)
                single_ts_queries.append(SingleTSQuery(**ts_dct))
            else:
                self._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
        return single_ts_queries

    @staticmethod
    def _raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type) -> NoReturn:
        raise TypeError(
            f"Got unsupported type as, or part of argument `{arg_name}` ({type(id_or_xid)}). Expected one of "
            f"{exp_type} or {dict}, or a (mixed) list of these, got `{id_or_xid!r}`."
        )

    @staticmethod
    def _validate_ts_query_dct(dct, arg_name, exp_type):
        if arg_name not in dct:
            if to_camel_case(arg_name) in dct:
                # For backwards compatability we accept identifier in camel case:
                dct = dct.copy()  # Avoid side effects
                dct[arg_name] = dct.pop(to_camel_case(arg_name))
            else:
                raise KeyError(f"Missing key `{arg_name}` in dict passed as, or part of argument `{arg_name}`")

        ts_identifier = dct[arg_name]
        if not isinstance(ts_identifier, exp_type):
            NewDatapointsQuery._raise_on_wrong_ts_identifier_type(ts_identifier, arg_name, exp_type)

        opt_dct_keys = {"start", "end", "aggregates", "granularity", "include_outside_points", "limit"}
        bad_keys = set(dct) - opt_dct_keys - {arg_name}
        if bad_keys:
            raise KeyError(
                f"Dict provided by argument `{arg_name}` included key(s) not understood: {sorted(bad_keys)}. "
                f"Required key: `{arg_name}`. Optional: {list(opt_dct_keys)}."
            )


@dataclass
class SingleTSQuery:
    id: Optional[int] = None
    external_id: Optional[str] = None
    start: Union[int, str, datetime] = None
    end: Union[int, str, datetime] = None
    aggregates: Optional[List[str]] = None
    granularity: Optional[str] = None
    include_outside_points: Optional[bool] = None
    limit: Optional[int] = None

    @classmethod
    def create_multiple_from_dicts(cls, ts_dct, defaults) -> List[SingleTSQuery]:
        agg = ts_dct.get("aggregates") or defaults["aggregates"]
        gran = ts_dct.get("granularity") or defaults["granularity"]

    def validate(self):
        if self.aggregates is None:
            if self.granularity is not None:
                # TODO(haakonvt): What if granularity comes from 'defaults'?
                raise ValueError(
                    "When supplying granularity, you must also specify aggregates. "
                    f"Failed datapoints query: {dataclasses.asdict(self)}"
                )

    def __repr__(self):
        # TODO(haakonvt): REMOVE
        s = ", ".join(
            f'{field.name}={getattr(self, field.name)!r}'
            for field in dataclasses.fields(self)
            if getattr(self, field.name) is not None
        )
        return f'{type(self).__name__}({s})'


def parse_dps_queries(queries: Union[NewDatapointsQuery, List[NewDatapointsQuery]]):
    if isinstance(query, NewDatapointsQuery):
        queries = [queries]


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
    # ID = None
    ID = [
        12345678,
        {"id": 123, "aggregates": ["count", "average"]},
    ]
    EXTERNAL_ID = [
        # "xiiiiiiid",
        # {"external_id": "asdf"},
        # {"externalId": "ASDF", "limit": -1, "granularity": "8h"},
        # None,
    ]
    EXTERNAL_ID = None

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
