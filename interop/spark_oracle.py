"""Spark 4 + Iceberg Java (the reference implementation) as an oracle for the .NET port.

Runs inside the apache/spark image via spark-submit (see TestSupport/SparkOracle.cs):

    read-metadata <metadata.json>   load the table from its metadata file and print rows, snapshots, deletes, lineage
    read-tables <metadata.json>...  the same for many tables in one JVM: {location: description | {"error": ...}}
                                    an argument "merge:<spec.json>" instead replays a MERGE in Spark (see merge_in_spark)
                                    an argument "query:<json>" is instead a read query (see run_query), keyed by itself
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

from pyspark.sql import DataFrame, Row, SparkSession

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
    """JSON for a Spark value; structs are objects by field name, arrays lists, maps objects (keys as text)."""
    if isinstance(value, Row):
        value = value.asDict()
    if isinstance(value, dict):
        return {str(to_jsonable(k)): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            # The Spark image ships Python 3.10, which has no datetime.UTC.
            value = value.replace(tzinfo=dt.timezone.utc)  # noqa: UP017
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


MERGE_PREFIX = "merge:"


def merge_in_spark(spark: SparkSession, spec_path: str) -> dict[str, Any]:
    """Copy snapshot_id of target into a Spark table, run sql (MERGE INTO {target} USING {source}) with source, describe.

    spec: {"target": metadata.json, "snapshot_id": id, "format_version": n, "source": metadata.json, "sql": template}.
    """
    spec = json.loads(Path(spec_path).read_text())
    target = f"fx.merges.t_{uuid.uuid4().hex}"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS fx.merges")
    (
        spark.read.format("iceberg")
        .option("snapshot-id", str(spec["snapshot_id"]))
        .load(spec["target"])
        .writeTo(target)
        .tableProperty("format-version", str(spec["format_version"]))
        .create()
    )
    spark.read.format("iceberg").load(spec["source"]).createOrReplaceTempView(
        "merge_source"
    )
    spark.sql(spec["sql"].format(target=target, source="merge_source"))
    return describe_catalog_table(spark, target)


def run_query(spark: SparkSession, spec: dict[str, Any]) -> dict[str, Any]:
    """One read: {"location", "version_as_of"?, "timestamp_as_of"? (UTC 'yyyy-MM-dd HH:mm:ss.SSS'), "where"?, "columns"?}.

    versionAsOf/timestampAsOf are Spark's time-travel options, the DataFrame form of VERSION AS OF / TIMESTAMP AS OF.
    """
    zone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        reader = spark.read.format("iceberg")
        if "version_as_of" in spec:
            reader = reader.option("versionAsOf", str(spec["version_as_of"]))
        if "timestamp_as_of" in spec:
            reader = reader.option("timestampAsOf", spec["timestamp_as_of"])
        df = reader.load(spec["location"])
        if spec.get("where"):
            df = df.where(spec["where"])
        if spec.get("columns"):
            df = df.select(*spec["columns"])
        rows = df.collect()
    finally:
        spark.conf.set("spark.sql.session.timeZone", zone)
    return {
        "columns": df.columns,
        "rows": [{k: to_jsonable(v) for k, v in r.asDict().items()} for r in rows],
    }


def cmd_read_tables(locations: list[str]) -> dict[str, Any]:
    spark = session()
    result: dict[str, Any] = {}
    for location in locations:
        try:
            if location.startswith(MERGE_PREFIX):
                result[location] = merge_in_spark(
                    spark, location.removeprefix(MERGE_PREFIX)
                )
                continue
            if location.startswith("query:"):
                result[location] = run_query(
                    spark, json.loads(location[len("query:") :])
                )
                continue
            result[location] = describe_metadata_file(spark, location)
        except Exception as e:  # noqa: BLE001 - report per table; one bad table must not hide the others
            java = getattr(e, "java_exception", None)
            result[location] = {
                "error": str(java) if java is not None else f"{type(e).__name__}: {e}"
            }
    return result


def describe_fixture(spark: SparkSession, name: str) -> dict[str, Any]:
    info = describe_catalog_table(spark, f"fx.ref.{name}")
    meta_dir = Path(FIXTURE_ROOT) / "warehouse" / "ref" / name / "metadata"
    latest = max(
        meta_dir.glob("v*.metadata.json"), key=lambda p: int(p.name[1:].split(".")[0])
    )
    info["metadata_location"] = f"file://{latest}"
    return info


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
        result[f"mor_v{version}"] = describe_fixture(spark, f"mor_v{version}")

    # Format v1: appends, a dynamic partition overwrite and a copy-on-write delete (v1 has no delete files).
    spark.sql(
        "CREATE TABLE fx.ref.cow_v1 (id bigint NOT NULL, name string, region string) USING iceberg "
        "PARTITIONED BY (region) TBLPROPERTIES ('format-version'='1')"
    )
    spark.sql(
        "INSERT INTO fx.ref.cow_v1 SELECT id, concat('n', id), IF(id % 2 = 0, 'eu', 'us') FROM range(0, 20)"
    )
    spark.sql(
        "INSERT INTO fx.ref.cow_v1 SELECT id, concat('n', id), 'apac' FROM range(20, 25)"
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql(
        "INSERT OVERWRITE fx.ref.cow_v1 SELECT id, 'replaced', 'apac' FROM range(100, 103)"
    )
    spark.sql("DELETE FROM fx.ref.cow_v1 WHERE id % 3 = 0")
    result["cow_v1"] = describe_fixture(spark, "cow_v1")

    # Schema and partition evolution: rows written under every schema and both specs.
    t = "fx.ref.evolved_v2"
    spark.sql(
        f"CREATE TABLE {t} (id bigint NOT NULL, qty int, note string, region string) USING iceberg "
        "PARTITIONED BY (region) TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(
        f"INSERT INTO {t} SELECT id, CAST(id AS int), concat('old', id), IF(id % 2 = 0, 'eu', 'us') FROM range(0, 10)"
    )
    spark.sql(f"ALTER TABLE {t} ADD COLUMN score double")
    spark.sql(f"ALTER TABLE {t} RENAME COLUMN note TO label")
    spark.sql(f"ALTER TABLE {t} ALTER COLUMN qty TYPE bigint")
    spark.sql(
        f"INSERT INTO {t} SELECT id, id * 1000000000, concat('mid', id), 'eu', id / 2.0 FROM range(10, 15)"
    )
    spark.sql(f"ALTER TABLE {t} DROP COLUMN label")
    spark.sql(
        f"ALTER TABLE {t} ADD COLUMN label string"
    )  # a new column: new field ID, old values must not show
    spark.sql(f"ALTER TABLE {t} ADD PARTITION FIELD bucket(4, id)")
    spark.sql(
        f"INSERT INTO {t} SELECT id, id, 'us', 1.5, concat('new', id) FROM range(15, 20)"
    )
    result["evolved_v2"] = describe_fixture(spark, "evolved_v2")

    # Nested types with every null/empty shape, a merge-on-read delete, and a field added inside a struct.
    t = "fx.ref.nested_v2"
    spark.sql(
        f"CREATE TABLE {t} (id bigint NOT NULL, point struct<x: double, y: double, label: string>, "
        "tags array<string>, counts map<string, int>, events array<struct<at: timestamp, kind: string>>, "
        "grid array<array<int>>) USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', {mor})"
    )
    spark.sql(
        f"""INSERT INTO {t} SELECT id,
          IF(id % 7 = 0, NULL, named_struct('x', id / 2.0, 'y', -id * 1.5, 'label', IF(id % 3 = 0, NULL, concat('p', id)))),
          IF(id % 5 = 0, NULL, IF(id % 5 = 1, array(), array(concat('t', id), NULL, 'z'))),
          IF(id % 6 = 0, NULL, IF(id % 6 = 1, map(), map('a', CAST(id AS int), 'b', NULL))),
          IF(id % 4 = 0, NULL, IF(id % 4 = 1, array(), array(named_struct('at', TIMESTAMP '2024-01-02 03:04:05.000006' + make_interval(0, 0, 0, 0, 0, 0, id), 'kind', 'k'), NULL))),
          IF(id % 9 = 0, NULL, array(array(CAST(id AS int)), array(), NULL))
        FROM range(0, 30)"""
    )
    spark.sql(f"DELETE FROM {t} WHERE id IN (3, 17)")
    spark.sql(f"ALTER TABLE {t} ADD COLUMN point.z double")
    spark.sql(
        f"INSERT INTO {t} SELECT id, named_struct('x', 1.0, 'y', 2.0, 'label', 'new', 'z', 3.0), array('n'), map('c', 1), array(), array() FROM range(30, 32)"
    )
    result["nested_v2"] = describe_fixture(spark, "nested_v2")

    # Avro data and delete files (write.format.default=avro), with every logical type Spark writes and both map forms.
    t = "fx.ref.avro_v2"
    spark.sql(
        f"CREATE TABLE {t} (id bigint NOT NULL, name string, amount decimal(10,2), big decimal(38,6), day date, "
        "ts timestamp, ntz timestamp_ntz, flag boolean, score double, ratio float, n int, raw binary, "
        "point struct<x: double, label: string>, tags array<string>, counts map<string, int>, codes map<int, string>, "
        "region string) USING iceberg PARTITIONED BY (region) "
        f"TBLPROPERTIES ('format-version'='2', 'write.format.default'='avro', {mor})"
    )
    spark.sql(
        f"""INSERT INTO {t} SELECT id, IF(id % 4 = 0, NULL, concat('n', id)), CAST(id * 1.25 - 10 AS decimal(10,2)),
          CAST(id * 123456789.123456 AS decimal(38,6)), date_add(DATE '2024-01-01', CAST(id AS int)),
          TIMESTAMP '2024-01-02 03:04:05.000006' + make_interval(0, 0, 0, 0, 0, 0, id),
          TIMESTAMP_NTZ '2024-01-02 03:04:05.000007' + make_interval(0, 0, 0, 0, 0, 0, id),
          id % 2 = 0, IF(id % 6 = 0, NULL, id / 3.0), CAST(id / 4.0 AS float), CAST(id * 7 AS int), CAST(concat('b', id) AS binary),
          IF(id % 5 = 1, NULL, named_struct('x', id / 2.0, 'label', IF(id % 3 = 0, NULL, concat('p', id)))),
          IF(id % 5 = 2, NULL, array(concat('t', id), NULL)), map('a', CAST(id AS int), 'b', NULL),
          map(CAST(id AS int), concat('c', id)), IF(id % 2 = 0, 'eu', 'us')
        FROM range(0, 30)"""
    )
    spark.sql(f"DELETE FROM {t} WHERE id % 5 = 0")
    spark.sql(f"UPDATE {t} SET name = 'updated' WHERE id % 7 = 1")
    result["avro_v2"] = describe_fixture(spark, "avro_v2")

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
