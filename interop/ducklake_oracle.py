"""DuckDB's `ducklake` extension (the DuckLake reference implementation) as the oracle for Iceberg.Net.DuckLake.

    ducklake_oracle.py probe            {"available": true, "duckdb": ..., "ducklake": ...} or {"available": false, "reason": ...}
    ducklake_oracle.py run <job.json>   [{"columns": [...], "rows": [[...], ...]} | {"error": ...}, ...]

A job is {"catalog": <SQLite catalog file>, "data_path": <path or null>, "statements": [<sql>, ...]}. The catalog is
attached as `lake` (`ATTACH 'ducklake:sqlite:<catalog>' AS lake`, with DATA_PATH when given, which creates the
catalog if the file is new) and the statements run in order, each reporting its result; the first failure is
reported and stops the job. Values are made JSON the way duckdb_oracle.py does (timestamps as epoch microseconds, dates
ISO, blobs hex, decimals and UUIDs as strings).

Output is the JSON document after the last @@ORACLE@@ marker on stdout. Extensions (parquet, sqlite_scanner,
ducklake) load from DuckDB's extension directory, install from extensions.duckdb.org, or, offline, from the
duckdb-extension-* wheels in the interop `duckdb` dependency group:

    uv sync --project interop --group duckdb
    uv run --project interop python -I interop/ducklake_oracle.py probe
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import sys
import uuid
from typing import Any

MARKER = "@@ORACLE@@"
EXTENSIONS = ("parquet", "sqlite_scanner", "ducklake")


def emit(document: Any) -> None:
    print(MARKER + json.dumps(document), flush=True)


def connect() -> tuple[Any, str]:
    """A connection with the DuckLake extensions loaded, or raise with every reason one could not be."""
    import duckdb

    con = duckdb.connect()
    errors: list[str] = []
    for name in EXTENSIONS:
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
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
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
        delta = value - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dt.time):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, (uuid.UUID, decimal.Decimal)):
        return str(value)
    if isinstance(value, float) and value != value:  # NaN
        return "NaN"
    return value


def literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def cmd_probe() -> dict[str, Any]:
    try:
        import duckdb

        _, ducklake = connect()
        return {"available": True, "duckdb": duckdb.__version__, "ducklake": ducklake}
    except Exception as e:  # noqa: BLE001 - the reason is the answer
        return {"available": False, "reason": f"{type(e).__name__}: {e}"}


def cmd_run(job_path: str) -> list[dict[str, Any]]:
    with open(job_path, encoding="utf-8") as f:
        job = json.load(f)
    con, _ = connect()
    attach = f"ATTACH {literal('ducklake:sqlite:' + job['catalog'])} AS lake"
    if job.get("data_path"):
        attach += f" (DATA_PATH {literal(job['data_path'])})"
    results: list[dict[str, Any]] = []
    try:
        con.sql(attach)
    except Exception as e:  # noqa: BLE001 - reported as the first result
        return [{"error": f"{type(e).__name__}: {e}"}]
    for statement in job["statements"]:
        try:
            relation = con.sql(statement)
            if relation is None:
                results.append({"columns": [], "rows": []})
            else:
                results.append(
                    {
                        "columns": relation.columns,
                        "rows": [jsonable(list(r)) for r in relation.fetchall()],
                    }
                )
        except Exception as e:  # noqa: BLE001 - reported per statement
            results.append({"error": f"{type(e).__name__}: {e}"})
            break
    con.close()
    return results


def main(argv: list[str]) -> None:
    if not argv:
        raise SystemExit(__doc__)
    if argv[0] == "probe":
        emit(cmd_probe())
    elif argv[0] == "run" and len(argv) == 2:
        emit(cmd_run(argv[1]))
    else:
        raise SystemExit(f"unknown command {' '.join(argv)}")


if __name__ == "__main__":
    main(sys.argv[1:])
