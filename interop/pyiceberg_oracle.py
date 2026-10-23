"""PyIceberg as a reference implementation for the .NET port's interop tests.

Commands (all output is JSON on stdout so the C# suite can assert on it):

    fixtures <out_dir>               build committed L0 fixtures with a local SQLite catalog
    write <catalog> <table> <rows>   create (or replace) a partitioned table and append rows
    read <catalog> <table>           print row count, rows, snapshot summary and spec details
    read-metadata <metadata.json>    same, for a table addressed only by its metadata file
    fingerprint <metadata.json>      order-independent per-column + per-row hashes (shadow comparison; streams batches)
    puffin-fixtures <out_dir>        reference Theta sketches (datasketches) and deletion vectors (pyroaring)
    inspect-puffin <file.puffin>     decode a Puffin file with PyIceberg + datasketches and report contents

<catalog> is "glue" or "rest". Endpoints and credentials come from the same environment
variables the C# regression suite reads (see docs/regression-suite.md), so both sides
always talk to the same emulator or AWS account.
"""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import os
import shutil
import struct
import sys
import uuid
import zlib
from pathlib import Path
from typing import Any, Callable

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
        # Same default location rule as the C# GlueCatalog: {warehouse}/glue/{db}.db/{table}
        warehouse = os.environ["ICEBERG_WAREHOUSE"].rstrip("/") + "/glue"
        props = {"type": "glue", "glue.region": region, "warehouse": warehouse, **s3}
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
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (uuid.UUID, decimal.Decimal)):
        return str(value)
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


DV_MAGIC = bytes([0xD1, 0xD3, 0x39, 0x64])


def dv_blob(positions: list[int], run_optimize: bool) -> bytes:
    """deletion-vector-v1 framing around a Roaring64 portable bitmap, built independently of PyIceberg."""
    from pyroaring import BitMap

    by_key: dict[int, list[int]] = {}
    for p in positions:
        by_key.setdefault(p >> 32, []).append(p & 0xFFFFFFFF)
    vector = len(by_key).to_bytes(8, "little")
    for key in sorted(by_key):
        bm = BitMap(by_key[key])
        if run_optimize:
            bm.run_optimize()
        vector += key.to_bytes(4, "little") + bm.serialize()
    body = DV_MAGIC + vector
    crc = zlib.crc32(body).to_bytes(4, "big")
    return len(body).to_bytes(4, "big") + body + crc


def puffin(blobs: list[tuple[dict[str, Any], bytes]]) -> bytes:
    out = b"PFA1"
    metadata = []
    for meta, payload in blobs:
        metadata.append(meta | {"offset": len(out), "length": len(payload)})
        out += payload
    footer = json.dumps(
        {"blobs": metadata, "properties": {"created-by": "oracle"}}
    ).encode()
    return (
        out + b"PFA1" + footer + len(footer).to_bytes(4, "little") + bytes(4) + b"PFA1"
    )


def _canonical(field_type: Any) -> Callable[[Any], str]:
    """Canonical text per Iceberg type; must match TableFingerprint.Canonical in Iceberg.Net.Data."""
    from pyiceberg.types import (
        BinaryType,
        BooleanType,
        DecimalType,
        FixedType,
        FloatType,
        UUIDType,
    )

    if isinstance(field_type, BooleanType):
        return lambda v: "true" if v else "false"
    if isinstance(field_type, FloatType):
        return lambda v: "NaN" if v != v else struct.pack(">f", v).hex()
    if isinstance(field_type, DoubleType):
        return lambda v: "NaN" if v != v else struct.pack(">d", v).hex()
    if isinstance(field_type, DecimalType):
        scale = field_type.scale
        return lambda v: str(int(decimal.Decimal(v).scaleb(scale)))
    if isinstance(field_type, UUIDType):
        return lambda v: str(
            v if isinstance(v, uuid.UUID) else uuid.UUID(bytes=bytes(v))
        )
    if isinstance(field_type, (BinaryType, FixedType)):
        return lambda v: bytes(v).hex()
    return str  # int/long, temporal types (pre-cast to integers), strings


def _h(text: str) -> int:
    return int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "little")


def cmd_fingerprint(location: str) -> dict[str, Any]:
    import pyarrow.compute as pc

    s3 = {"client.region": os.environ.get("AWS_REGION", "eu-west-1")}
    if endpoint := os.environ.get("ICEBERG_S3_ENDPOINT"):
        s3 |= {"s3.endpoint": endpoint, "s3.path-style-access": "true"}
    table = StaticTable.from_metadata(location, properties=s3)
    fields = table.schema().columns
    canon = [_canonical(f.field_type) for f in fields]
    mask = (1 << 64) - 1
    columns = {
        f.field_id: {"name": f.name, "count": 0, "nulls": 0, "hash": 0} for f in fields
    }
    rows, row_hash = 0, 0
    for batch in table.scan().to_arrow_batch_reader():
        arrays = []
        for f in fields:
            arr = batch.column(f.name)
            if pa.types.is_temporal(
                arr.type
            ):  # days / micros / nanos as integers, exactly
                arr = pc.cast(
                    arr, pa.int32() if pa.types.is_date32(arr.type) else pa.int64()
                )
            arrays.append(arr.to_pylist())
        for i in range(batch.num_rows):
            parts = []
            for c, f in enumerate(fields):
                v = arrays[c][i]
                col = columns[f.field_id]
                col["count"] += 1
                if v is None:
                    col["nulls"] += 1
                    parts.append("\u0000")
                else:
                    text = canon[c](v)
                    col["hash"] = (col["hash"] + _h(text)) & mask
                    parts.append(text)
            row_hash = (row_hash + _h("\u001f".join(parts))) & mask
            rows += 1
    return {
        "rows": rows,
        "row_hash": f"{row_hash:016x}",
        "columns": {
            str(k): v | {"hash": f"{v['hash']:016x}"} for k, v in columns.items()
        },
    }


def cmd_puffin_fixtures(out_dir: Path) -> dict[str, Any]:
    from datasketches import update_theta_sketch

    out_dir.mkdir(parents=True, exist_ok=True)
    expected: dict[str, Any] = {"theta": {}, "dv": {}}
    for name, values in {
        "empty": [],
        "single": ["iceberg"],
        "exact3": ["v0", "v1", "v2"],
        "est10k": [f"v{i}" for i in range(10_000)],
    }.items():
        sketch = update_theta_sketch()
        for v in values:
            sketch.update(v)
        compact = sketch.compact()
        (out_dir / f"theta-{name}.bin").write_bytes(compact.serialize())
        expected["theta"][name] = {
            "estimate": compact.get_estimate(),
            "retained": compact.num_retained,
            "theta64": compact.theta64,
            "distinct": len(set(values)),
        }

    ranges = [
        (0, 1),
        (5, 6),
        (70_000, 70_001),
        (100_000, 110_000),
        ((1 << 32) + 3, (1 << 32) + 4),
    ]
    positions = [p for start, end in ranges for p in range(start, end)]
    blobs = []
    for i, run_optimize in enumerate([False, True]):
        data_file = f"s3://bucket/data/file-{i}.parquet"
        blobs.append(
            (
                {
                    "type": "deletion-vector-v1",
                    "fields": [2147483545],
                    "snapshot-id": -1,
                    "sequence-number": -1,
                    "properties": {
                        "referenced-data-file": data_file,
                        "cardinality": str(len(positions)),
                    },
                },
                dv_blob(positions, run_optimize),
            )
        )
    (out_dir / "dv.puffin").write_bytes(puffin(blobs))
    expected["dv"] = {"ranges": ranges, "cardinality": len(positions), "files": 2}
    (out_dir / "expected.json").write_text(json.dumps(expected, indent=2) + "\n")
    return expected


def cmd_inspect_puffin(location: str) -> dict[str, Any]:
    from datasketches import compact_theta_sketch
    from pyiceberg.table.deletion_vector import deletion_vectors_from_puffin_file
    from pyiceberg.table.puffin import PuffinFile

    raw = Path(location).read_bytes()
    pf = PuffinFile(raw)
    result: dict[str, Any] = {"blobs": [], "deletion_vectors": {}}
    for blob in pf.footer.blobs:
        payload = pf.get_blob_payload(blob)
        info: dict[str, Any] = {
            "type": blob.type,
            "fields": blob.fields,
            "properties": blob.properties,
        }
        if blob.type == "apache-datasketches-theta-v1":
            sketch = compact_theta_sketch.deserialize(payload)
            info |= {"estimate": sketch.get_estimate(), "retained": sketch.num_retained}
        if blob.type == "deletion-vector-v1":
            length = int.from_bytes(payload[:4], "big")
            info["crc_ok"] = zlib.crc32(payload[4 : 4 + length]) == int.from_bytes(
                payload[4 + length : 8 + length], "big"
            )
        result["blobs"].append(info)
    if any(b.type == "deletion-vector-v1" for b in pf.footer.blobs):
        for dv in deletion_vectors_from_puffin_file(pf):
            result["deletion_vectors"][dv.referenced_data_file] = (
                dv.to_vector().to_pylist()
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
        case ["fingerprint", location]:
            result = cmd_fingerprint(location)
        case ["puffin-fixtures", out]:
            result = cmd_puffin_fixtures(Path(out))
        case ["inspect-puffin", location]:
            result = cmd_inspect_puffin(location)
        case _:
            raise SystemExit(__doc__)
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main(sys.argv[1:])
