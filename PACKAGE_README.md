# Iceberg.NET

A native .NET implementation of [Apache Iceberg](https://iceberg.apache.org/): create, write, read and maintain Iceberg
tables from C#, through AWS Glue, any Iceberg REST catalog, a JDBC/SQL catalog or an in-memory catalog.

> **Early development.** The API and the capability set for `0.0.1` are still being formalised. The implemented paths are tested
> against emulators (moto for S3 and Glue, the Iceberg REST fixture, Polaris, Nessie, Lakekeeper, Gravitino) and
> cross-checked with Spark, PyIceberg, Trino and DuckDB, but has **not yet run against real AWS** (S3, Glue, Athena).

## Packages

| Package | What it is |
|---|---|
| `Iceberg.Net.Core` | The table spec model: types, schemas, partitioning, snapshots, metadata JSON (also `netstandard2.0`) |
| `Iceberg.Net.Avro` | Manifest and manifest-list encoding, v1–v3 (also `netstandard2.0`) |
| `Iceberg.Net.Puffin` | Puffin files: the generic container, plus Iceberg's deletion vectors and theta sketches |
| `Iceberg.Net.IO` | File IO: local and S3 |
| `Iceberg.Net.Catalog` | Catalogs: in-memory, AWS Glue, Iceberg REST, JDBC/SQL |
| `Iceberg.Net.Data` | Table operations: scans, appends, upserts, deletes, UPDATE/MERGE, maintenance |
| `Iceberg.Net.Linq` | Typed rows and a LINQ provider that pushes filters and projections into scans |

## Example

```csharp
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

using var catalog = new InMemoryCatalog("/tmp/warehouse");
await catalog.CreateNamespaceAsync(Namespace.Of("sales"));
var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)],
    identifierFieldIds: [1]);
var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), schema, FormatVersion: 3));

await TableMutations.InsertAsync(table, [[1L, "a"], [2L, "b"]]);
await TableMutations.UpsertAsync(table, [[2L, "c"]], strategy: DeleteStrategy.PositionDeletes);
var rows = await TableScan.ReadAllAsync(table);
```

## Documentation

- [README](https://github.com/rtkelly-labs/Iceberg.NET#readme), what works today
- [Documentation index](https://github.com/rtkelly-labs/Iceberg.NET/tree/main/docs)
- [Spec coverage](https://github.com/rtkelly-labs/Iceberg.NET/blob/main/docs/spec-coverage.md)
- [Changelog](https://github.com/rtkelly-labs/Iceberg.NET/blob/main/CHANGELOG.md)
- [Security policy](https://github.com/rtkelly-labs/Iceberg.NET/blob/main/SECURITY.md)

Licensed under the Apache License 2.0; see the NOTICE file in the package for attributions.
