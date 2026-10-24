"""PyIceberg against the .NET REST catalog server (driven by CrossEngineTests.cs).

    write <uri> <credential> <namespace> <table> <start> <count> [stage]
        create the namespace and table if needed (with a staged create transaction when "stage" is given), append
        rows (id long, name string, region string) start..start+count-1, and print the table's state
    read <uri> <credential> <namespace> <table>
        print the ids, row count, snapshot count and schema column names
    evolve <uri> <credential> <namespace> <table> <column>
        add an optional string column

<credential> is "client_id:client_secret" for the OAuth2 client-credentials endpoint, or "-" for none.
Output is one JSON document on stdout.
"""

from __future__ import annotations

import json
import sys
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "region", StringType(), required=False),
)
SPEC = PartitionSpec(
    PartitionField(
        source_id=3, field_id=1000, transform=IdentityTransform(), name="region"
    )
)


def catalog(uri: str, credential: str) -> Catalog:
    props = {"type": "rest", "uri": uri}
    if credential != "-":
        props["credential"] = credential
        props["scope"] = "catalog"
    return load_catalog("dotnet", **props)


def describe(cat: Catalog, ident: tuple[str, str]) -> dict[str, Any]:
    table = cat.load_table(ident)
    arrow = table.scan().to_arrow()
    return {
        "rows": arrow.num_rows,
        "ids": sorted(arrow.column("id").to_pylist()),
        "snapshots": len(table.metadata.snapshots),
        "columns": [f.name for f in table.schema().fields],
        "metadata_location": table.metadata_location,
        "format_version": table.metadata.format_version,
    }


def cmd_write(
    uri: str, credential: str, ns: str, name: str, start: int, count: int, stage: bool
) -> dict[str, Any]:
    cat = catalog(uri, credential)
    try:
        cat.create_namespace(ns)
    except NamespaceAlreadyExistsError:
        pass
    ident = (ns, name)
    rows = pa.Table.from_pylist(
        [
            {"id": i, "name": f"py-{i}", "region": "eu" if i % 2 == 0 else "us"}
            for i in range(start, start + count)
        ],
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string()),
                pa.field("region", pa.string()),
            ]
        ),
    )
    try:
        table = cat.load_table(ident)
        table.append(rows)
    except NoSuchTableError:
        if stage:
            # A staged create (stage-create: true), committed with assert-create together with the first append.
            with cat.create_table_transaction(
                ident, schema=SCHEMA, partition_spec=SPEC
            ) as txn:
                txn.append(rows)
        else:
            cat.create_table(ident, schema=SCHEMA, partition_spec=SPEC).append(rows)
    return describe(cat, ident)


def cmd_read(uri: str, credential: str, ns: str, name: str) -> dict[str, Any]:
    return describe(catalog(uri, credential), (ns, name))


def cmd_evolve(
    uri: str, credential: str, ns: str, name: str, column: str
) -> dict[str, Any]:
    cat = catalog(uri, credential)
    table = cat.load_table((ns, name))
    with table.update_schema() as update:
        update.add_column(column, StringType())
    return describe(cat, (ns, name))


def main(argv: list[str]) -> None:
    cmd, *args = argv
    if cmd == "write":
        result = cmd_write(
            args[0],
            args[1],
            args[2],
            args[3],
            int(args[4]),
            int(args[5]),
            len(args) > 6 and args[6] == "stage",
        )
    elif cmd == "read":
        result = cmd_read(*args[:4])
    elif cmd == "evolve":
        result = cmd_evolve(*args[:5])
    else:
        raise SystemExit(f"unknown command {cmd}")
    print(json.dumps(result))


if __name__ == "__main__":
    main(sys.argv[1:])
