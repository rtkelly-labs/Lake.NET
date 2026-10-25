"""Create the regression catalog in Polaris (idempotent): S3 storage on moto without STS, and give the bootstrap
principal's catalog_admin role content rights. Run by the polaris-setup compose service."""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("POLARIS_URL", "http://polaris:8181")
WAREHOUSE = "s3://iceberg-regression/polaris-warehouse"
# The suite drops tables with purge=true when cleaning up; Polaris refuses that unless enabled per catalog.
PROPERTIES = {
    "default-base-location": WAREHOUSE,
    "polaris.config.drop-with-purge.enabled": "true",
}


def call(
    method: str,
    path: str,
    token: str | None = None,
    body: dict | None = None,
    form: dict | None = None,
) -> dict:
    data = (
        urllib.parse.urlencode(form).encode()
        if form
        else json.dumps(body).encode()
        if body
        else None
    )
    req = urllib.request.Request(BASE + path, data=data, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if body:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 - fixed internal URL
        text = r.read()
        return json.loads(text) if text else {}


token = call(
    "POST",
    "/api/catalog/v1/oauth/tokens",
    form={
        "grant_type": "client_credentials",
        "client_id": "root",
        "client_secret": "s3cr3t",
        "scope": "PRINCIPAL_ROLE:ALL",
    },
)["access_token"]
try:
    call(
        "POST",
        "/api/management/v1/catalogs",
        token,
        {
            "catalog": {
                "name": "regression",
                "type": "INTERNAL",
                "properties": PROPERTIES,
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": [WAREHOUSE],
                    "endpoint": f"http://localhost:{os.environ.get('MOTO_PORT', '5000')}",  # what clients (the test host) use
                    "endpointInternal": "http://moto:5000",  # what Polaris itself uses
                    "pathStyleAccess": True,
                    "stsUnavailable": True,  # no credential vending: clients use their own S3 credentials
                },
            }
        },
    )
    print("created catalog regression")
except urllib.error.HTTPError as e:
    if e.code != 409:
        raise
    current = call("GET", "/api/management/v1/catalogs/regression", token)
    call(
        "PUT",
        "/api/management/v1/catalogs/regression",
        token,
        {
            "currentEntityVersion": current["entityVersion"],
            "properties": current.get("properties", {}) | PROPERTIES,
        },
    )
    print("catalog regression exists; properties updated")
call(
    "PUT",
    "/api/management/v1/catalogs/regression/catalog-roles/catalog_admin/grants",
    token,
    {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
)
print("granted CATALOG_MANAGE_CONTENT to catalog_admin")
