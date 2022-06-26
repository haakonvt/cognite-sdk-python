from __future__ import annotations
from pprint import pprint
import numbers
from typing import List, Union, Optional, Dict, NoReturn
from functools import partial, cached_property
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
        parse_fn = partial(SingleTSQuery.parse_multiple_to_list, defaults=self.defaults)
        for ts in id_or_xid:
            if isinstance(ts, exp_type):
                queries.extend(parse_fn({arg_name: ts}))

            elif isinstance(ts, dict):
                ts_validated = self._validate_ts_query_dct(ts, arg_name, exp_type)
                queries.extend(parse_fn(ts_validated))
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
    # Note `agg`: Only notable difference from regular query,
    # enforcing exactly 1 aggregate or None
    agg: Optional[str] = None

    @classmethod
    def parse_multiple_to_list(cls, ts_dct, defaults) -> List[SingleTSQuery]:
        # Note: We merge 'defaults' and given ts-dict, ts-dict takes precedence:
        dct = {**defaults, **ts_dct}
        granularity, aggregates = dct["granularity"], dct.pop("aggregates")

        if not (granularity is None or isinstance(granularity, str)):
            raise TypeError(f"Expected `granularity` to be of type `str` or None, not {type(granularity)}")

        elif not (aggregates is None or isinstance(aggregates, list)):
            raise TypeError(f"Expected `aggregates` to be of type `list[str]` or None, not {type(aggregates)}")

        elif aggregates is None:
            if granularity is None:
                return [cls(**dct, agg=aggregates)]
            raise KeyError(f"When passing `granularity`, argument `aggregates` is also required.")

        # Aggregates must be a list at this point:
        elif len(aggregates) == 0:
            raise ValueError("Empty list of `aggregates` passed, expected at least one!")

        elif granularity is None:
            raise KeyError(f"When passing `aggregates`, argument `granularity` is also required.")

        elif dct["include_outside_points"] is True:
            raise ValueError("'Including outside points' is not supported for aggregates")

        return [cls(**dct, agg=agg) for agg in aggregates]  # Finally

    def __repr__(self):
        # TODO(haakonvt): REMOVE
        s = ", ".join(
            f'{field.name}={getattr(self, field.name)!r}'
            for field in dataclasses.fields(self)
            if getattr(self, field.name) is not None
        )
        return f'{type(self).__name__}({s})'


class QueryPlanner:
    def __init__(self, queries: List[SingleTSQuery]):
        self.queries = queries


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
