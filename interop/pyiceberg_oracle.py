"""PyIceberg as a reference implementation for the .NET port's interop tests.

Commands (all output is JSON on stdout so the C# suite can assert on it):

    fixtures <out_dir>               build committed L0 fixtures with a local SQLite catalog
    write <catalog> <table> <rows>   create (or replace) a partitioned table and append rows
    read <catalog> <table>           print row count, rows, snapshot summary and spec details
    read-metadata <metadata.json>    same, for a table addressed only by its metadata file

<catalog> is "glue" or "rest". Endpoints and credentials come from the same environment
variables the C# regression suite reads (see docs/regression-suite.md), so both sides
always talk to the same emulator or AWS account.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable, Table
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

FIXTURE_ROOT = "/tmp/sentric-iceberg-fixture-root"

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "amount", DoubleType(), required=False),
    NestedField(4, "event_ts", TimestamptzType(), required=False),
    NestedField(5, "region", StringType(), required=False),
    NestedField(6, "qty", IntegerType(), required=False),
)

SPEC = PartitionSpec(
    PartitionField(
        source_id=4, field_id=1000, transform=DayTransform(), name="event_ts_day"
    ),
    PartitionField(
        source_id=1, field_id=1001, transform=BucketTransform(4), name="id_bucket"
    ),
    PartitionField(
        source_id=5, field_id=1002, transform=IdentityTransform(), name="region"
    ),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("amount", pa.float64()),
        pa.field("event_ts", pa.timestamp("us", tz="UTC")),
        pa.field("region", pa.string()),
        pa.field("qty", pa.int32()),
    ]
)


def rows(start: int, count: int) -> pa.Table:
    base = dt.datetime(2024, 1, 1, tzinfo=dt.UTC)
    ids = list(range(start, start + count))
    return pa.table(
        {
            "id": ids,
            "name": [None if i % 7 == 0 else f"name-{i}" for i in ids],
            "amount": [i * 1.5 for i in ids],
            "event_ts": [base + dt.timedelta(hours=i * 5) for i in ids],
            "region": ["eu" if i % 2 == 0 else "us" for i in ids],
            "qty": [i % 10 for i in ids],
        },
        schema=ARROW_SCHEMA,
    )


def env_catalog(kind: str) -> Catalog:
    region = os.environ.get("AWS_REGION", "eu-west-1")
    s3: dict[str, str] = {"client.region": region}
    if endpoint := os.environ.get("ICEBERG_S3_ENDPOINT"):
        s3 |= {"s3.endpoint": endpoint, "s3.path-style-access": "true"}
    if kind == "glue":
        props = {"type": "glue", "glue.region": region, **s3}
        if glue_endpoint := os.environ.get("ICEBERG_GLUE_ENDPOINT"):
            props["glue.endpoint"] = glue_endpoint
        return load_catalog("glue", **props)
    if kind == "rest":
        return load_catalog(
            "rest",
            type="rest",
            uri=os.environ.get("ICEBERG_REST_URI", "http://localhost:8181"),
            **s3,
        )
    raise SystemExit(f"unknown catalog kind {kind!r}")


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        return int(value.timestamp() * 1_000_000)
    return value


def describe(catalog: Catalog, identifier: str) -> dict[str, Any]:
    return describe_table(catalog.load_table(identifier))


def describe_table(table: Table) -> dict[str, Any]:
    data = table.scan().to_arrow().sort_by("id")
    snapshot = table.current_snapshot()
    return {
        "row_count": data.num_rows,
        "ids": data.column("id").to_pylist(),
        "rows": [{k: to_jsonable(v) for k, v in r.items()} for r in data.to_pylist()],
        "format_version": table.metadata.format_version,
        "snapshot_count": len(table.metadata.snapshots),
        "summary": dict(snapshot.summary.additional_properties)
        | {"operation": snapshot.summary.operation.value}
        if snapshot
        else None,
        "spec": [f"{f.name}:{f.transform}" for f in table.spec().fields],
        "metadata_location": table.metadata_location,
    }


def cmd_write(kind: str, identifier: str, count: int) -> dict[str, Any]:
    catalog = env_catalog(kind)
    namespace = identifier.rsplit(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)
    try:
        catalog.drop_table(identifier)
    except NoSuchTableError:
        pass
    table = catalog.create_table(identifier, schema=SCHEMA, partition_spec=SPEC)
    table.append(rows(0, count))
    return describe(catalog, identifier)


def cmd_fixtures(out_dir: Path) -> dict[str, Any]:
    """Tables whose files are committed under tests/.../fixtures/pyiceberg.

    Locations inside the metadata point at FIXTURE_ROOT; the C# tests remap that prefix
    onto the fixture directory, so the files are read exactly as PyIceberg wrote them.
    """
    shutil.rmtree(FIXTURE_ROOT, ignore_errors=True)
    Path(FIXTURE_ROOT).mkdir(parents=True)
    catalog = load_catalog(
        "fixture",
        type="sql",
        uri=f"sqlite:///{FIXTURE_ROOT}/catalog.db",
        warehouse=f"file://{FIXTURE_ROOT}/warehouse",
    )
    catalog.create_namespace("interop")

    # 1. Partitioned v2 table: two appends then a delete of one partition (copy-on-write).
    partitioned = catalog.create_table(
        "interop.partitioned", schema=SCHEMA, partition_spec=SPEC
    )
    partitioned.append(rows(0, 20))
    partitioned.append(rows(20, 10))
    partitioned.delete("region = 'us'")

    # 2. Schema evolution: rows written before a column existed must read back as null.
    evolved = catalog.create_table("interop.evolved", schema=Schema(*SCHEMA.fields[:3]))
    evolved.append(rows(0, 5).select(["id", "name", "amount"]))
    with evolved.update_schema() as update:
        update.add_column("note", StringType())
    evolved.append(
        pa.table(
            {"id": [100], "name": ["late"], "amount": [1.0], "note": ["added"]},
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    ("name", pa.string()),
                    ("amount", pa.float64()),
                    ("note", pa.string()),
                ]
            ),
        )
    )

    result = {}
    for name in ("interop.partitioned", "interop.evolved"):
        result[name] = describe(catalog, name)

    shutil.rmtree(out_dir, ignore_errors=True)
    shutil.copytree(f"{FIXTURE_ROOT}/warehouse", out_dir)
    (out_dir / "expected.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    return result


def main(argv: list[str]) -> None:
    match argv:
        case ["fixtures", out]:
            result = cmd_fixtures(Path(out))
        case ["write", kind, identifier, count]:
            result = cmd_write(kind, identifier, int(count))
        case ["read", kind, identifier]:
            result = describe(env_catalog(kind), identifier)
        case ["read-metadata", location]:
            result = describe_table(StaticTable.from_metadata(location))
        case _:
            raise SystemExit(__doc__)
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main(sys.argv[1:])
