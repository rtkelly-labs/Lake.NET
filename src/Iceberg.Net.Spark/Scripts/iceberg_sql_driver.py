"""Run a list of Spark SQL statements written by Iceberg.Net.Spark, in order, and print each result.

    spark-submit iceberg_sql_driver.py --statements <path or s3:// URI of a JSON array of strings> [--max-rows N]

The statements are JSON rather than a `;`-separated script so that no SQL splitting is needed (literals may contain
semicolons). AWS Glue passes job arguments as `--name value` pairs as well, so the same file is a Glue job script;
arguments it does not know (Glue's `--JOB_NAME`, ...) are ignored. The last stdout line starting with @@RESULTS@@ is
a JSON array of {statement, columns, rows}; a failing statement fails the job.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from pyspark.sql import SparkSession

MARKER = "@@RESULTS@@"


def argument(argv: list[str], name: str, default: str | None = None) -> str | None:
    for i, arg in enumerate(argv):
        if arg == name and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith(name + "="):
            return arg.split("=", 1)[1]
    return default


def jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def main(argv: list[str]) -> None:
    path = argument(argv, "--statements")
    if not path:
        raise SystemExit(
            "usage: iceberg_sql_driver.py --statements <path> [--max-rows N]"
        )
    max_rows = int(argument(argv, "--max-rows", "100") or "100")
    spark = SparkSession.builder.appName(
        argument(argv, "--app-name", "iceberg-net-sql") or "iceberg-net-sql"
    ).getOrCreate()
    text = "\n".join(
        row.value for row in spark.read.text(path, wholetext=True).collect()
    )
    statements = json.loads(text)
    if not isinstance(statements, list) or not all(
        isinstance(s, str) for s in statements
    ):
        raise SystemExit(f"{path} must hold a JSON array of SQL strings")
    results = []
    for i, sql in enumerate(statements, 1):
        print(f"[{i}/{len(statements)}] {sql}", file=sys.stderr, flush=True)
        frame = spark.sql(sql)
        rows = (
            [
                jsonable(r.asDict(recursive=True))
                for r in frame.limit(max_rows).collect()
            ]
            if frame.columns
            else []
        )
        results.append({"statement": sql, "columns": frame.columns, "rows": rows})
    print(MARKER + json.dumps(results), flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
