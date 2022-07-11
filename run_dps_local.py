import numpy as np
from pprint import pprint
from local_cog_client import setup_local_cog_client

from cognite.client._api.datapoints_new import NewDatapointsQuery
from cognite.client._api.datapoints_new import count_based_task_splitting

# Specify the aggregates to return. Use default if null.
# If the default is a set of aggregates,
# specify an empty string to get raw data.
START = None
END = None
LIMIT = None
AGGREGATES = None  # ["average"]
GRANULARITY = None  # "12h"
INCLUDE_OUTSIDE_POINTS = None
IGNORE_UNKNOWN_IDS = False
ID = None
ID = [
    # {"id": 226740051491},
    # {"id": 2546012653669},  # string
    # {"id": 1111111111111},  # missing...
    # {"id": 2546012653669, "aggregates": ["max", "average"], "granularity": "1d"},  # string
]
EXTERNAL_ID = [
    # {"limit": None, "external_id": "ts-test-#01-daily-111/650"},
    # {"limit": None, "external_id": "ts-test-#01-daily-222/650"},
    # {"limit": None, "external_id": "ts-test-#01-daily-444/650"},
    # {"limit": None, "external_id": "8400074_destination"},  # string
    # {"limit": 15, "external_id": "9624122_cargo_type"},  # string
    # {"limit": None, "external_id": "ts-test-#01-daily-651/650"},  # missing
    {
        "include_outside_points": True,
        "limit": 3,
        "external_id": "ts-test-#04-ten-mill-dps-1/1",
        "start": 31536472487-1,
        "end": 31536698071+1,
    },
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
# pprint(q)
finished_tasks = count_based_task_splitting(q, client, max_workers=1)
res = [t.get_result() for t in finished_tasks]
r0 = np.array(res[0])
print(r0)
print(f"{r0.shape=}")
ts = r0[:, 0].astype(int)
x = r0[:, 1]
# print(f"{ts=}")

print("client.datapoints.retrieve(...):")
print(
    client.datapoints.retrieve(
        **EXTERNAL_ID[0]
    ).to_pandas()
)
