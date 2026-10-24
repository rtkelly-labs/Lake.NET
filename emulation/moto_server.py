"""moto server with fidelity shims for behaviour Iceberg commits depend on.

moto is the emulator of record (S3 + Glue + STS on one port), but it does not model every
concurrency guarantee of the real services. Each shim below enforces a documented AWS
behaviour that moto skips, so that the regression suite fails in emulation exactly where it
would fail in AWS. Every shim is listed in docs/emulation.md; remove one when moto gains it.

Run: python -I emulation/moto_server.py [--port 5000]
"""

from __future__ import annotations

import argparse
import json
import threading
from typing import Any

from moto.athena.responses import AthenaResponse
from moto.core.exceptions import JsonRESTError
from moto.glue.models import GlueBackend
from moto.glue.responses import GlueResponse
from moto.moto_server.werkzeug_app import (
    DomainDispatcherApplication,
    create_backend_app,
)
from werkzeug.serving import run_simple

_glue_lock = threading.Lock()


class ConcurrentModificationError(JsonRESTError):
    code = 400

    def __init__(self, message: str) -> None:
        super().__init__("ConcurrentModificationException", message)


# Shim 1: Glue UpdateTable honours VersionId as an optimistic lock.
# Real Glue rejects an UpdateTable whose VersionId is not the table's current version; moto
# ignores the parameter, which silently turns concurrent Iceberg commits into lost updates.
_original_update = GlueResponse.update_table


def _update_table_with_version_check(self: GlueResponse) -> Any:
    expected = self.parameters.get("VersionId")
    with _glue_lock:
        if expected is not None:
            database = self.parameters.get("DatabaseName")
            name = self.parameters["TableInput"]["Name"]
            table = self.glue_backend.get_table(database, name)
            current = str(table._current_version)  # noqa: SLF001 - moto keeps no public accessor
            if expected != current:
                raise ConcurrentModificationError(
                    f"Update table failed: version {expected} is not current ({current})"
                )
        return _original_update(self)


GlueResponse.update_table = _update_table_with_version_check  # type: ignore[method-assign]

# Shim 2: serialise CreateTable so two concurrent creates cannot both succeed.
_original_create = GlueBackend.create_table


def _create_table_locked(self: GlueBackend, *args: Any, **kwargs: Any) -> Any:
    with _glue_lock:
        return _original_create(self, *args, **kwargs)


GlueBackend.create_table = _create_table_locked  # type: ignore[method-assign]


# Shim 3: Athena GetQueryResults pages. Real Athena returns at most MaxResults rows (default and maximum 1000,
# the header row of a SELECT counting as one) and a NextToken for the rest; moto returns every row at once, so a
# client that ignores NextToken would pass in emulation and silently drop rows on AWS.
_ATHENA_PAGE = 1000
_original_get_query_results = AthenaResponse.get_query_results


def _get_query_results_paged(self: AthenaResponse) -> Any:
    max_results = int(self._get_param("MaxResults") or _ATHENA_PAGE)
    if not 1 <= max_results <= _ATHENA_PAGE:
        return self.error(f"MaxResults must be between 1 and {_ATHENA_PAGE}", 400)
    token = self._get_param("NextToken")
    start = int(token) if token else 0
    body = json.loads(_original_get_query_results(self))
    rows = body["ResultSet"]["Rows"]
    body["ResultSet"]["Rows"] = rows[start : start + max_results]
    if start + max_results < len(rows):
        body["NextToken"] = str(start + max_results)
    return json.dumps(body)


AthenaResponse.get_query_results = _get_query_results_paged  # type: ignore[method-assign]

# Which shims this process applies, served at /__shims so tests can tell a stale emulator (started before a shim
# was added) from a missing AWS guarantee.
SHIMS = [
    "glue-update-table-version-id",
    "glue-create-table-atomic",
    "athena-get-query-results-paging",
]


def with_shim_listing(app: Any) -> Any:
    def wrapped(environ: dict[str, Any], start_response: Any) -> Any:
        if environ.get("PATH_INFO") == "/__shims":
            start_response("200 OK", [("Content-Type", "application/json")])
            return [json.dumps(SHIMS).encode()]
        return app(environ, start_response)

    return wrapped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")  # noqa: S104 - emulator, local/CI only
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()
    app = DomainDispatcherApplication(create_backend_app)
    run_simple(
        args.host, args.port, with_shim_listing(app), threaded=True, use_reloader=False
    )


if __name__ == "__main__":
    main()
