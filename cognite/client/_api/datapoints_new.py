from pprint import pprint
import numbers
from typing import List, Union, Optional, Dict, NoReturn
from datetime import datetime
from dataclasses import dataclass, fields

from cognite.client.data_classes import DatapointsQuery
from cognite.client.utils._auxiliary import to_snake_case, to_camel_case

print("RUNNING REPOS/COG-SDK, NOT FROM PIP\n")


def dps_new(query):
    return


class NewDatapointsQuery(DatapointsQuery):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_queries = self.validate_and_create_queries()

        """
        self.id = id
        self.external_id = external_id
        self.start = start
        self.end = end
        self.limit = limit
        self.aggregates = aggregates
        self.granularity = granularity
        self.include_outside_points = include_outside_points
        self.ignore_unknown_ids = ignore_unknown_ids

        # ID
        int,  12345
        List[int], [123, 345]
        Dict[str, Any]
        List[Union[int, Dict[str, Any]]]

        # XID
        str,
        List[str],
        Dict[str, Any]
        List[Union[str, Dict[str, Any]]]

        {'aggregates': ['average'],
         'end': 0,
         'granularity': '1h',
         'ignoreUnknownIds': False,
         'includeOutsidePoints': False,
         'items': [{'aggregates': ['average'],
                    'end': 0,
                    'granularity': '1h',
                    'id': 1,
                    'includeOutsidePoints': False,
                    'limit': 0,
                    'start': 0}],
         'limit': 100,
         'start': 0}

        client.datapoints.retrieve(
            id=[
                {"id": 1, "aggregates": ["average"]},
                {"id": 1, "aggregates": ["min"]}
            ],
            external_id={
                "externalId": "1", "aggregates": ["max"]
            },
            start="1d-ago",
            end="now",
            granularity="1h"
        )
        """

    def validate_and_create_queries(self):
        defaults = dict(
            start=self.start,
            end=self.end,
            limit=self.limit,
            aggregates=self.aggregates,
            granularity=self.granularity,
            include_outside_points=self.include_outside_points,
        )
        all_queries = []
        if self.id is not None:
            all_queries.extend(
                self._validate_id_or_xid(
                    id_or_xid=self.id, is_external_id=False, defaults=defaults,
                )
            )
        if self.external_id is not None:
            all_queries.extend(
                self._validate_id_or_xid(
                    id_or_xid=self.external_id, is_external_id=True, defaults=defaults,
                )
            )
        return all_queries

    @staticmethod
    def _validate_id_or_xid(id_or_xid, is_external_id: bool, defaults: Dict):
        if is_external_id:
            arg_name, exp_type = "external_id", str
        else:
            arg_name, exp_type = "id", numbers.Integral

        if isinstance(id_or_xid, (exp_type, dict)):
            id_or_xid = [id_or_xid]

        if not isinstance(id_or_xid, list):
            NewDatapointsQuery._raise_on_wrong_ts_identifier_type(id_or_xid, arg_name, exp_type)
        single_ts_queries = []
        for ts in id_or_xid:
            if isinstance(ts, exp_type):
                single_ts_queries.append(SingleTSQuery(**{**defaults, arg_name: ts}))

            elif isinstance(ts, dict):
                # Note: merge 'defaults' and given ts-dict; ts-dict takes precedence:
                ts_dct = {**defaults, **ts}
                NewDatapointsQuery._validate_ts_query_dct(ts_dct, arg_name, exp_type)
                single_ts_queries.append(SingleTSQuery(**ts_dct))
            else:
                NewDatapointsQuery._raise_on_wrong_ts_identifier_type(ts, arg_name, exp_type)
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
                # For backwards compatability:
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

    def __repr__(self):
        # TODO(haakonvt): REMOVE
        s = ", ".join(
            f'{field.name}={getattr(self, field.name)!r}'
            for field in fields(self)
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
    START = 0
    END = 100
    AGGREGATES = ["count"]
    GRANULARITY = "4d"
    INCLUDE_OUTSIDE_POINTS = False
    LIMIT = 50
    IGNORE_UNKNOWN_IDS = False
    ID = [
        12345678,
        {"id": 123, "include_outside_points": not INCLUDE_OUTSIDE_POINTS, "aggregates": ""},
    ]
    EXTERNAL_ID = [
        "xiiiiiiid",
        {"external_id": "asdf"},
        {"externalId": "ASDF", "limit": -1, "granularity": "8h"},
        None
    ]

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
    pprint(query.all_queries)
