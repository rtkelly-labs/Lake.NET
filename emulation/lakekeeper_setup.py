"""Bootstrap Lakekeeper and create the regression warehouse (idempotent): S3 storage on moto without STS, so
clients bring their own credentials. Run by the lakekeeper-setup compose service, which shares the host network
with Lakekeeper (see docker-compose.yml)."""

import json
import os
import urllib.error
import urllib.request

BASE = os.environ.get("LAKEKEEPER_URL", "http://localhost:8283")
WAREHOUSE = {
    "warehouse-name": "regression",
    "storage-profile": {
        "type": "s3",
        "bucket": "iceberg-regression",
        "key-prefix": "lakekeeper",
        # One endpoint serves Lakekeeper and its clients: both reach moto as localhost:5000 (host network).
        "endpoint": "http://localhost:5000",
        "region": "eu-west-1",
        "path-style-access": True,
        "flavor": "s3-compat",
        "sts-enabled": False,
    },
    "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "access-key-id": "test",
        "secret-access-key": "test",
    },
    # The suite drops tables with purge=true; delete them at once rather than after a soft-delete expiry.
    "delete-profile": {"type": "hard"},
}


def call(method: str, path: str, body: dict | None = None) -> tuple[int, str]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 - fixed local URL
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def main() -> None:
    status, text = call("POST", "/management/v1/bootstrap", {"accept-terms-of-use": True})
    if status >= 300 and "already" not in text.lower():
        raise SystemExit(f"bootstrap failed ({status}): {text}")
    status, text = call("GET", "/management/v1/warehouse")
    if status != 200:
        raise SystemExit(f"listing warehouses failed ({status}): {text}")
    if any(w.get("name") == "regression" for w in json.loads(text).get("warehouses", [])):
        print("warehouse regression exists")
        return
    status, text = call("POST", "/management/v1/warehouse", WAREHOUSE)
    if status >= 300:
        raise SystemExit(f"creating the warehouse failed ({status}): {text}")
    print("created warehouse regression")


if __name__ == "__main__":
    main()
