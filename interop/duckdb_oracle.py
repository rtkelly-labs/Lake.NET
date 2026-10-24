"""DuckDB's `iceberg` extension as a fourth independent reader of tables the .NET port writes.

    duckdb_oracle.py probe                     {"available": true, "duckdb": ..., "iceberg": ...} or {"available": false, "reason": ...}
    duckdb_oracle.py read <metadata.json>...   {location: {"columns": [...], "rows": [[...], ...]} | {"error": ...}}

Output is the JSON document after the last @@ORACLE@@ marker on stdout. The extension is loaded from DuckDB's
extension directory, installed from extensions.duckdb.org if missing (CI), or, offline, from the
`duckdb-extension-iceberg` / `duckdb-extension-avro` wheels when they are installed (they repackage DuckDB's signed
builds; DuckDB still verifies the signature on LOAD). Run it with the interop environment plus the `duckdb` group:

    uv sync --project interop --group duckdb
    uv run --project interop python -I interop/duckdb_oracle.py probe
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import sys
import uuid
from typing import Any

MARKER = "@@ORACLE@@"


def emit(document: Any) -> None:
    print(MARKER + json.dumps(document), flush=True)


def connect() -> tuple[Any, str]:
    """A connection with the iceberg extension loaded, or raise with every reason it could not be."""
    import duckdb

    con = duckdb.connect()
    errors: list[str] = []
    for name in ("avro", "iceberg"):
        try:
            con.sql(f"LOAD {name}")
            continue
        except duckdb.Error as e:
            errors.append(f"LOAD {name}: {e}")
        try:
            con.sql(f"INSTALL {name}")
            con.sql(f"LOAD {name}")
            continue
        except duckdb.Error as e:
            errors.append(f"INSTALL {name}: {str(e).splitlines()[0]}")
        try:
            from duckdb_extensions import import_extension

            import_extension(name, con=con)
            con.sql(f"LOAD {name}")
        except Exception as e:  # noqa: BLE001 - every failure is a reason to report
            raise RuntimeError("; ".join([*errors, f"wheel {name}: {e}"])) from e
    version = con.sql(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'"
    ).fetchone()
    return con, str(version[0]) if version else "?"


def jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return int(value.timestamp() * 1_000_000)
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, (uuid.UUID, decimal.Decimal)):
        return str(value)
    if isinstance(value, float) and value != value:  # NaN
        return "NaN"
    return value


def cmd_probe() -> dict[str, Any]:
    try:
        import duckdb

        _, iceberg = connect()
        return {"available": True, "duckdb": duckdb.__version__, "iceberg": iceberg}
    except Exception as e:  # noqa: BLE001 - the reason is the answer
        return {"available": False, "reason": f"{type(e).__name__}: {e}"}


def cmd_read(locations: list[str]) -> dict[str, Any]:
    con, _ = connect()
    out: dict[str, Any] = {}
    for location in locations:
        try:
            literal = "'" + location.replace("'", "''") + "'"
            relation = con.sql(f"SELECT * FROM iceberg_scan({literal})")
            out[location] = {
                "columns": relation.columns,
                "rows": [jsonable(list(r)) for r in relation.fetchall()],
            }
        except Exception as e:  # noqa: BLE001 - reported per table
            out[location] = {"error": f"{type(e).__name__}: {e}"}
    return out


def main(argv: list[str]) -> None:
    if not argv:
        raise SystemExit(__doc__)
    if argv[0] == "probe":
        emit(cmd_probe())
    elif argv[0] == "read":
        emit(cmd_read(argv[1:]))
    else:
        raise SystemExit(f"unknown command {argv[0]}")


if __name__ == "__main__":
    main(sys.argv[1:])
