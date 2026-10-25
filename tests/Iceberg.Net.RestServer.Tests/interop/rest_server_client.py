"""PyIceberg against the .NET REST catalog server (driven by CrossEngineTests.cs).

    write <uri> <credential> <namespace> <table> <start> <count> [stage]
        create the namespace and table if needed (with a staged create transaction when "stage" is given), append
        rows (id long, name string, region string) start..start+count-1, and print the table's state
    read <uri> <credential> <namespace> <table>
        print the ids, row count, snapshot count and schema column names
    evolve <uri> <credential> <namespace> <table> <column>
        add an optional string column
    signed-append-read <uri> <credential> <namespace> <table> <start> <count>
        with X-Iceberg-Access-Delegation: remote-signing, append rows start..start+count-1 to an existing table and
        read it back (every S3 request signed by the server), printing the ids and the FileIO used
    plan-read <uri> <credential> <namespace> <table> [<max id>]
        read with server-side scan planning (scan-planning-mode=server), optionally with "id < <max id>", and print
        the ids and the planned data and delete files
    gcs-token <uri> <credential> <namespace> <table> <gcs service host>
        load a gs:// table (vended credentials, PyIceberg's default) and ask its FileIO whether a file under it
        exists, with GCS requests sent to <gcs service host>; print the FileIO and its gcs.oauth2.* properties

<credential> is "client_id:client_secret" for the OAuth2 client-credentials endpoint, or "-" for none.
Output is one JSON document on stdout.
"""

from __future__ import annotations

import json
import sys
from typing import Any

import fsspec.config
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

# PyIceberg 0.12 attaches its S3 remote signer (S3V4RestSigner) by unregistering and re-registering a botocore
# handler each time a FileIO first builds its S3 filesystem on a thread. fsspec caches that filesystem across threads,
# so the handler is swapped on a client other threads are using: a request made in the gap goes out unsigned (moto
# answers 403), or two threads collide in the unregister (KeyError: 1925). PyIceberg reads manifests and data files
# on a thread pool, so signed-append-read failed now and then (apache/iceberg-python#3896). A filesystem of its own
# per FileIO and thread means the signer is registered before anything can use that client.
fsspec.config.conf.setdefault("s3", {})["skip_instance_cache"] = True

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


def catalog(uri: str, credential: str, **extra: str) -> Catalog:
    props = {"type": "rest", "uri": uri, **extra}
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


def cmd_signed_append_read(
    uri: str, credential: str, ns: str, name: str, start: int, count: int
) -> dict[str, Any]:
    cat = catalog(
        uri, credential, **{"header.X-Iceberg-Access-Delegation": "remote-signing"}
    )
    table = cat.load_table((ns, name))
    rows = pa.Table.from_pylist(
        [
            {"id": i, "name": f"py-{i}", "region": "eu" if i % 2 == 0 else "us"}
            for i in range(start, start + count)
        ],
        schema=table.schema().as_arrow(),
    )
    table.append(rows)
    table = cat.load_table((ns, name))
    arrow = table.scan().to_arrow()
    return {
        "ids": sorted(arrow.column("id").to_pylist()),
        "io": type(table.io).__name__,
        "signer": table.io.properties.get("s3.signer"),
    }


def cmd_plan_read(
    uri: str, credential: str, ns: str, name: str, max_id: str | None
) -> dict[str, Any]:
    cat = catalog(uri, credential, **{"scan-planning-mode": "server"})
    table = cat.load_table((ns, name))
    scan = table.scan(row_filter=f"id < {int(max_id)}" if max_id else "True")
    tasks = list(scan.plan_files())
    arrow = scan.to_arrow()
    return {
        "server_side": cat.supports_server_side_planning(),
        "ids": sorted(arrow.column("id").to_pylist()),
        "data_files": len(tasks),
        "delete_files": sorted({d.file_path for t in tasks for d in t.delete_files}),
    }


def cmd_gcs_token(
    uri: str, credential: str, ns: str, name: str, service_host: str
) -> dict[str, Any]:
    cat = catalog(uri, credential, **{"gcs.service.host": service_host})
    table = cat.load_table((ns, name))
    exists = table.io.new_input(f"{table.location()}/metadata/probe.json").exists()
    return {
        "io": type(table.io).__name__,
        "token": table.io.properties.get("gcs.oauth2.token"),
        "expires_at": table.io.properties.get("gcs.oauth2.token-expires-at"),
        "exists": exists,
    }


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
    elif cmd == "signed-append-read":
        result = cmd_signed_append_read(*args[:4], int(args[4]), int(args[5]))
    elif cmd == "plan-read":
        result = cmd_plan_read(*args[:4], args[4] if len(args) > 4 else None)
    elif cmd == "gcs-token":
        result = cmd_gcs_token(*args[:5])
    elif cmd == "evolve":
        result = cmd_evolve(*args[:5])
    else:
        raise SystemExit(f"unknown command {cmd}")
    print(json.dumps(result))


if __name__ == "__main__":
    main(sys.argv[1:])
