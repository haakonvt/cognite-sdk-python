from inspect import signature
from cognite.client import CogniteClient, __version__

print(f"{__version__=}")

TENANT_ID = "0d75f6b8-c6b9-4e84-baca-503e08aa7e4a"
CLIENT_ID = "d9b2bd26-2d84-4960-bbdc-ecdd9743975d"
CDF_CLUSTER = "greenfield"
TOKEN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPES = [f"https://{CDF_CLUSTER}.cognitedata.com/.default"]
client = CogniteClient(
    token_url=TOKEN_URL,
    token_client_id=CLIENT_ID,
    token_client_secret="144DAaodIqc3-OPa~I5DJI-LJU-T.~40fb",
    token_scopes=[f"https://{CDF_CLUSTER}.cognitedata.com/.default"],
    base_url=f"https://{CDF_CLUSTER}.cognitedata.com",
    client_name="client_secret_test_script",
    project="forge-sandbox",
    disable_pypi_version_check=True,
)

# Manual requests lol
# payload = {
#     "items": [task.ts_item],
#     "start": window.start,
#     "end": window.end,
#     "aggregates": task.aggregates,
#     "granularity": task.granularity,
#     "includeOutsidePoints": task.include_outside_points and first_page,
#     "ignoreUnknownIds": task.ignore_unknown_ids,
#     "limit": min(window.limit, task.request_limit),
# }
# res = self.client._post(self.client._RESOURCE_PATH + "/list", json=payload).json()["items"]






test_tss = client.time_series.list(external_id_prefix="hakon-test-#########-", limit=50)
# print(test_tss.to_pandas())
# dps = client.datapoints.retrieve(
#     # id=[t.id for t in test_tss],
#     external_id=[t.external_id for t in test_tss],
#     # external_id=test_tss[2].external_id,
#     start="50d-ago",
#     end="now",
#     granularity="1d",
#     aggregates=["average", "min", "max"],
# )
dps = client.datapoints.retrieve(
    id=[
        {"id": test_tss[0].id, "aggregates": ["average", "totalVariation"]},
        {"id": test_tss[1].id, "aggregates": ["min"]}
    ],
    external_id={"externalId": test_tss[2].external_id, "aggregates": ["max"]},
    start="1000w-ago",
    end="now",
    granularity="1d"
)
print(dps.to_pandas(column_names="id"))
