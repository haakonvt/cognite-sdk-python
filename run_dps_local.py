from pprint import pprint
from local_cog_client import setup_local_cog_client

from cognite.client._api.datapoints_new import NewDatapointsQuery
from cognite.client._api.datapoints_new import count_based_task_splitting

# Specify the aggregates to return. Use default if null.
# If the default is a set of aggregates,
# specify an empty string to get raw data.
START = None
END = None
AGGREGATES = None  # ["average"]
GRANULARITY = None  # "12h"
INCLUDE_OUTSIDE_POINTS = None
LIMIT = 10
IGNORE_UNKNOWN_IDS = True
# IGNORE_UNKNOWN_IDS = False
ID = None
ID = [
    # {"id": 226740051491},
    {"id": 2546012653669},  # string
    {"id": 1111111111111},  # missing...
    # {"id": 2546012653669, "aggregates": ["max", "average"], "granularity": "1d"},  # string
]
EXTERNAL_ID = [
    # {"limit": None, "external_id": "ts-test-#01-daily-111/650"},
    # {"limit": None, "external_id": "ts-test-#01-daily-222/650"},
    {"limit": 4, "external_id": "ts-test-#01-daily-64/650"},
    # {"limit": None, "external_id": "ts-test-#01-daily-444/650"},
    # {"limit": None, "external_id": "8400074_destination"},  # string
    {"limit": 15, "external_id": "9624122_cargo_type"},  # string
    {"limit": None, "external_id": "ts-test-#01-daily-651/650"},  # missing
]
client = setup_local_cog_client(debug=False)
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
