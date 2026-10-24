"""Spark 4 + Iceberg Java's RESTCatalog against the .NET REST catalog server (driven by CrossEngineTests.cs).

Runs in the apache/spark image on the host network (so it reaches the server on 127.0.0.1):

    spark-submit --jars iceberg-spark-runtime.jar spark_rest_client.py <uri> <credential> <steps.json> [<catalog.json>]

<steps.json> is a JSON list of steps against the catalog "rest", each either a SQL statement or a DataFrame read
{"read": "rest.db.t", "options": {"start-snapshot-id": "1", ...}}; each step's rows (for queries and reads) are
returned in order. <catalog.json> holds extra catalog properties (e.g. "header.X-Iceberg-Access-Delegation").
Output is the JSON document after the last @@ORACLE@@ marker on stdout.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession


def session(uri: str, credential: str, extra: dict[str, str]) -> SparkSession:
    builder = (
        SparkSession.builder.appName("iceberg-rest-client")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type", "rest")
        .config("spark.sql.catalog.rest.uri", uri)
        .config("spark.sql.catalog.rest.credential", credential)
        .config("spark.sql.catalog.rest.scope", "catalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    for key, value in extra.items():
        builder = builder.config(f"spark.sql.catalog.rest.{key}", value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(argv: list[str]) -> None:
    uri, credential, steps_path = argv[:3]
    extra: dict[str, str] = json.loads(Path(argv[3]).read_text()) if len(argv) > 3 else {}
    steps: list[Any] = json.loads(Path(steps_path).read_text())
    spark = session(uri, credential, extra)
    results: list[Any] = []
    for step in steps:
        if isinstance(step, dict):
            reader = spark.read.format("iceberg")
            for key, value in step.get("options", {}).items():
                reader = reader.option(key, value)
            df = reader.load(step["read"])
        else:
            df = spark.sql(step)
        rows = [r.asDict() for r in df.collect()] if df.columns else []
        results.append(
            {"step": step, "rows": [{k: str(v) for k, v in r.items()} for r in rows]}
        )
    sys.stdout.write("\n@@ORACLE@@" + json.dumps(results) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
