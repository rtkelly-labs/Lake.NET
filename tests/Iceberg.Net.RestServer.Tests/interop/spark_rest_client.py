"""Spark 4 + Iceberg Java's RESTCatalog against the .NET REST catalog server (driven by CrossEngineTests.cs).

Runs in the apache/spark image on the host network (so it reaches the server on 127.0.0.1):

    spark-submit --jars iceberg-spark-runtime.jar spark_rest_client.py <uri> <credential> <steps.json>

<steps.json> is a JSON list of SQL statements against the catalog "rest"; each statement's rows (for queries) are
returned in order. Output is the JSON document after the last @@ORACLE@@ marker on stdout.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession


def session(uri: str, credential: str) -> SparkSession:
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
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(argv: list[str]) -> None:
    uri, credential, steps_path = argv
    steps: list[str] = json.loads(Path(steps_path).read_text())
    spark = session(uri, credential)
    results: list[Any] = []
    for sql in steps:
        df = spark.sql(sql)
        rows = [r.asDict() for r in df.collect()] if df.columns else []
        results.append(
            {"sql": sql, "rows": [{k: str(v) for k, v in r.items()} for r in rows]}
        )
    sys.stdout.write("\n@@ORACLE@@" + json.dumps(results) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
