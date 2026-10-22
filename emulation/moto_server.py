"""moto server with fidelity shims for behaviour Iceberg commits depend on.

moto is the emulator of record (S3 + Glue + STS on one port), but it does not model every
concurrency guarantee of the real services. Each shim below enforces a documented AWS
behaviour that moto skips, so that the regression suite fails in emulation exactly where it
would fail in AWS. Every shim is listed in docs/emulation.md; remove one when moto gains it.

Run: python -I emulation/moto_server.py [--port 5000]
"""

from __future__ import annotations

import argparse
import threading
from typing import Any

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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")  # noqa: S104 - emulator, local/CI only
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()
    app = DomainDispatcherApplication(create_backend_app)
    run_simple(args.host, args.port, app, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
