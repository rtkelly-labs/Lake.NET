"""Spark 4 + Iceberg Java (the reference implementation) as an oracle for the .NET port.

Runs inside the apache/spark image via spark-submit (see TestSupport/SparkOracle.cs):

    read-metadata <metadata.json>   load the table from its metadata file and print rows, snapshots, deletes, lineage
    read-tables <metadata.json>...  the same for many tables in one JVM: {location: description | {"error": ...}}
    fixtures <out_dir>              write reference tables (DVs, position deletes, row lineage) with Spark

Output is the JSON document after the last @@ORACLE@@ marker on stdout; Spark's own logging goes to stderr.
Regenerate the committed L0 fixtures (from iceberg-dotnet/, after emulation/up.sh --spark):

    docker run --rm --user root --network none --hostname localhost -e SPARK_LOCAL_IP=127.0.0.1 \
      -v $PWD/emulation/.cache:/jars:ro -v $PWD/interop:/interop:ro -v $PWD/tests/Iceberg.Net.Spec.Tests/fixtures:/out \
      -v /tmp/sentric-spark-fixture-root:/tmp/sentric-spark-fixture-root apache/spark:4.0.1 \
      /opt/spark/bin/spark-submit --jars /jars/iceberg-spark-runtime-4.0_2.13-1.12.0.jar /interop/spark_oracle.py fixtures /out/spark
    find tests/Iceberg.Net.Spec.Tests/fixtures/spark -name '*.crc' -delete
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession

FIXTURE_ROOT = "/tmp/sentric-spark-fixture-root"


def session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("iceberg-oracle")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.fx", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.fx.type", "hadoop")
        .config("spark.sql.catalog.fx.warehouse", f"file://{FIXTURE_ROOT}/warehouse")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            # The Spark image ships Python 3.10, which has no datetime.UTC.
            value = value.replace(tzinfo=dt.timezone.utc)  # noqa: UP017
        return int(value.timestamp() * 1_000_000)
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (uuid.UUID, decimal.Decimal)):
        return str(value)
    if isinstance(value, float) and value != value:  # NaN
        return "NaN"
    return value


def describe(
    spark: SparkSession, load: Callable[[str], DataFrame], version: int
) -> dict[str, Any]:
    """Rows, snapshots, delete files and (v3) row lineage. load("") is the table, load("snapshots") a metadata table."""
    df = load("")
    key = df.columns[0]
    rows = [r.asDict() for r in df.orderBy(key).collect()]
    snapshots = (
        load("snapshots")
        .orderBy("committed_at")
        .select("snapshot_id", "operation")
        .collect()
    )
    deletes = load("delete_files").select("content", "file_format").collect()
    result = {
        "row_count": len(rows),
        "ids": [r[key] for r in rows],
        "rows": [{k: to_jsonable(v) for k, v in r.items()} for r in rows],
        "snapshot_count": len(snapshots),
        "operation": snapshots[-1]["operation"] if snapshots else None,
        "delete_files": [f"{r['content']}:{r['file_format']}" for r in deletes],
        "format_version": version,
        "engine": f"spark {spark.version}",
    }
    if version >= 3:
        lineage = (
            df.select(key, "_row_id", "_last_updated_sequence_number")
            .orderBy(key)
            .collect()
        )
        result["row_lineage"] = [
            {
                "id": r[key],
                "row_id": r["_row_id"],
                "last_updated_seq": r["_last_updated_sequence_number"],
            }
            for r in lineage
        ]
    return result


def describe_catalog_table(spark: SparkSession, table: str) -> dict[str, Any]:
    props = {
        r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    }
    return describe(
        spark,
        lambda m: spark.table(f"{table}.{m}" if m else table),
        int(props.get("format-version", "2")),
    )


def describe_metadata_file(spark: SparkSession, location: str) -> dict[str, Any]:
    """Load straight from a metadata.json (Hadoop FileSystem, so bare and file: paths both work)."""
    path = location.removeprefix("file://").removeprefix("file:")
    version = json.loads(Path(path).read_text())["format-version"]
    return describe(
        spark,
        lambda m: spark.read.format("iceberg").load(
            f"{location}#{m}" if m else location
        ),
        version,
    )


def cmd_read_tables(locations: list[str]) -> dict[str, Any]:
    spark = session()
    result: dict[str, Any] = {}
    for location in locations:
        try:
            result[location] = describe_metadata_file(spark, location)
        except Exception as e:  # noqa: BLE001 - report per table; one bad table must not hide the others
            java = getattr(e, "java_exception", None)
            result[location] = {
                "error": str(java) if java is not None else f"{type(e).__name__}: {e}"
            }
    return result


def cmd_fixtures(out_dir: Path) -> dict[str, Any]:
    # FIXTURE_ROOT is a bind mount: empty it rather than removing it.
    root = Path(FIXTURE_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    for child in root.iterdir():
        shutil.rmtree(child) if child.is_dir() else child.unlink()
    spark = session()
    mor = (
        "'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
        "'write.merge.mode'='merge-on-read'"
    )
    result: dict[str, Any] = {}
    for version in (2, 3):
        table = f"fx.ref.mor_v{version}"
        spark.sql(
            f"CREATE TABLE {table} (id bigint NOT NULL, name string, region string) USING iceberg "
            f"PARTITIONED BY (region) TBLPROPERTIES ('format-version'='{version}', {mor})"
        )
        spark.sql(
            f"INSERT INTO {table} SELECT id, concat('n', id), IF(id % 2 = 0, 'eu', 'us') FROM range(0, 40)"
        )
        spark.sql(f"DELETE FROM {table} WHERE id % 5 = 0")
        spark.sql(f"UPDATE {table} SET name = 'updated' WHERE id % 7 = 1")
        spark.sql(
            f"DELETE FROM {table} WHERE id IN (2, 3)"
        )  # second delete on the same files: DVs must merge
        info = describe_catalog_table(spark, table)
        meta_dir = (
            Path(FIXTURE_ROOT) / "warehouse" / "ref" / f"mor_v{version}" / "metadata"
        )
        latest = max(
            meta_dir.glob("v*.metadata.json"),
            key=lambda p: int(p.name[1:].split(".")[0]),
        )
        info["metadata_location"] = f"file://{latest}"
        result[f"mor_v{version}"] = info

    shutil.rmtree(out_dir, ignore_errors=True)
    shutil.copytree(f"{FIXTURE_ROOT}/warehouse", out_dir)
    (out_dir / "expected.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    return {
        k: {"rows": v["row_count"], "deletes": v["delete_files"]}
        for k, v in result.items()
    }


def main(argv: list[str]) -> None:
    match argv:
        case ["read-metadata", location]:
            result = cmd_read_tables([location])[location]
        case ["read-tables", *locations] if locations:
            result = cmd_read_tables(locations)
        case ["fixtures", out]:
            result = cmd_fixtures(Path(out))
        case _:
            raise SystemExit(__doc__)
    sys.stdout.write("\n@@ORACLE@@" + json.dumps(result) + "\n")


if __name__ == "__main__":
    main(sys.argv[1:])
