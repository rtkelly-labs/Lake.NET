# DuckLake (`Iceberg.Net.DuckLake`)

A C# implementation of the [DuckLake](https://ducklake.select/docs/stable/specification/introduction) table
format (1.0 and 1.1), built inside this repository on the Iceberg data plane and held to the same rules as the Iceberg code.

**Status:** evaluation build-out. It reads and writes lakes that DuckDB's `ducklake` extension (the reference
implementation) reads and writes, verified in both directions by cross-engine tests. Not packed or published
(`IsPackable=false`); the API is not settled. The catalog layer is EF Core on SQLite and PostgreSQL, both tested
against DuckDB, and both format versions are read and written.

## Guides

| Guide | Read it for |
|---|---|
| [Architecture and incubation](ducklake/architecture.md) | Shared data plane, catalog databases, EF Core, implementation layout and graduation criteria |
| [Compatibility](ducklake/compatibility.md) | Format 1.1, types and encodings, differences between DuckDB and the spec |
| [Operations](ducklake/operations.md) | Importing existing files, catalog objects, data inlining and maintenance |
| [Testing and constraints](ducklake/testing.md) | Repository guarantees, test suites, oracle setup and environment variables |
| [Performance](ducklake/performance.md) | Benchmark results, optimisations and remaining costs |

## Usage

```csharp
using Iceberg.Net.DuckLake;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;

var store = DuckLakeStore.Sqlite("Data Source=lake/catalog.sqlite");          // or DuckLakeStore.PostgreSql("Host=…")
var lake = await DuckLakeCatalog.CreateAsync(store, new LocalFileIO(), "lake/data/");   // or OpenAsync(store, io)

await using (var tx = lake.BeginTransaction(author: "etl", commitMessage: "load"))
{
    await tx.CreateTableAsync("main", "events",
        [new("id", DuckLakeType.Int64, Nullable: false), new("tags", DuckLakeType.List(DuckLakeType.Varchar))]);
    await tx.InsertAsync("main", "events", [[1L, new[] { "a" }], [2L, null]]);
    await tx.CommitAsync();                       // one snapshot
}
await using (var tx = lake.BeginTransaction())
{
    await tx.DeleteAsync("main", "events", Expr.Equal("id", 2L));
    await tx.CommitAsync();
}
var table = await lake.LoadTableAsync("main", "events");                 // latest, or snapshotId: n
var rows = await lake.ReadAsync(table, Expr.GreaterThan("id", 0L));      // pruned by file statistics, filtered exactly
await foreach (var e in lake.ReadAsync<Event>(table)) { }                 // record Event(long Id, List<object?>? Tags)
await DuckLakeValidator.AssertValidAsync(lake);                         // the catalog's invariants hold
```

`ReadAsync` returns each type's physical value (an `int` of days for a date, `long` microseconds for a timestamp, as
the Iceberg data plane holds them); `ReadClrAsync` and `ReadAsync<T>` give natural CLR values (`DateOnly`,
`DateTime`, `DateTimeOffset`, `decimal`, `ulong`, …; see `DuckLakeClr`), the latter bound to a record or class by
column name (`customer_name` ↔ `CustomerName`). Inserts accept either form.

DuckDB opens the same lake with `ATTACH '<store.DuckDbAttach()>' AS lake` (`ducklake:sqlite:lake/catalog.sqlite` or
`ducklake:postgres:dbname=… host=…`). A longer walk-through (nested types, partitioning, UPDATE, schema evolution,
time travel) is `examples/DuckLake.Quickstart`:

```bash
dotnet run --project examples/DuckLake.Quickstart [directory] [postgres connection string]
```

## What works

| Area | State |
|---|---|
| Catalog | Creates the catalog (matching DuckDB's schema for SQLite and PostgreSQL exactly, per format version) with snapshot 0 and schema `main`, or opens an existing 1.0 or 1.1 catalog. EF Core over SQLite or PostgreSQL |
| Reads | Snapshot-scoped catalog queries (spec "Queries"), relative/absolute path chains, nested schemas (1.1), delete files, inlined rows (primitive and nested), inlined deletions, DuckDB's partial (merged/flushed) data and delete files with per-row snapshots, row IDs, time travel by snapshot or timestamp |
| Pruning | File statistics through the Iceberg metrics evaluator; Parquet row groups through footer statistics; exact residual filter |
| Writes | CREATE/DROP SCHEMA (nested in 1.1), CREATE/DROP TABLE, INSERT (file, file and table statistics, row IDs), DELETE (merge-on-read; a replacement delete file per data file, emptied files removed), UPDATE (delete + insert in one snapshot; the new versions keep their row IDs in `_ducklake_internal_row_id`, as the spec allows and DuckDB does), ADD/DROP/RENAME COLUMN, type widening (and `null` promotion), partitioned inserts, author/message/extra info |
| Types | `boolean`, `int8`–`int64`, `uint8`–`uint64`, `float32/64`, `decimal(p,s)`, `date`, `time`, `timetz`, `timestamp`, `timestamp_s`, `timestamp_ms`, `timestamptz`, `timestamp_ns`, `interval`, `varchar`, `json`, `blob`, `uuid`, `list`, `struct`, `map`, and 1.1's `null` (see [Types](ducklake/compatibility.md#types)) |
| Concurrency | Optimistic commits: on a lost race for the snapshot ID, the `changes_made` of every snapshot since the base are checked against the spec's logical conflicts, then the metadata is retried without rewriting files; delete-vs-delete is decided per data file. Tested with concurrent writers on both databases, and with DuckDB writing in between |
| Catalog objects | Tags and comments (tables, columns and nested fields, views; view columns in 1.1), table renames, views, scalar and table macros, sort orders (written files follow the leading column keys); DuckDB uses each, and this library reads DuckDB's (see [Catalog objects](ducklake/operations.md#catalog-objects)) |
| Maintenance | Merging adjacent small files into partial files, rewriting files with many deleted rows, expiring snapshots, deleting scheduled and orphaned files; DuckDB reads the results and cleans up after them, and the reverse (see [Maintenance](ducklake/operations.md#maintenance)) |
| Existing files | `AddDataFilesAsync` registers Parquet files written elsewhere, in place, under a `map_by_name` column mapping with footer statistics, as `ducklake_add_data_files` does (Hive partition columns, missing and extra columns); files DuckDB added are read through their mapping (see [Adding existing files](ducklake/operations.md#adding-existing-files)) |
| Deletion vectors (1.1) | Puffin delete files read (bare `deletion-vector-v1` blobs, and containers of cumulative vectors keyed by `ducklake-snapshot-id`, the latest not after the scanned snapshot); written as bare vectors when `write_deletion_vectors` is set in the lake or `DuckLakeOptions.WriteDeletionVectors` is on |
| Encryption | Encrypted lakes (`DuckLakeOptions.Encrypted`, or an existing lake's `encrypted` setting): every Parquet data and delete file written with Parquet modular encryption (AES-GCM, encrypted footer) under a fresh 256-bit key per write, kept Base64 in `encryption_key` as DuckDB keeps it; DuckDB reads these lakes and writes to them, and this library reads DuckDB's. DuckDB cannot yet filter, delete from or update files encrypted here (a DuckDB bug with parquet-cpp's encryption; see [the differences table](ducklake/compatibility.md#where-the-reference-implementation-and-the-spec-differ)). Existing files cannot be added to an encrypted lake |
| Validation | `DuckLakeValidator` checks a whole lake (any writer's) and reports problems; `Deep` also reads every live file |

## What is not implemented

`variant` and `geometry`, `int128`/`uint128` (DuckDB writes them to Parquet as DOUBLE and cannot read
them back), `time_ns` (DuckDB cannot write it to Parquet), inlining into partitioned tables (their small inserts go to files), and
catalog migration (1.0 → 1.1 is DuckDB's). Each fails with `NotSupportedException` when met in a catalog
or file rather than reading wrong values. The same list is in `tools/spec_trace.py` (`NOT_IMPLEMENTED`).

Within a transaction, a delete must be the first data operation on its table, and a table created in the transaction
cannot be altered in it (define its columns at creation).

## Next steps

1. Typed rows through the LINQ layer's mapping, shared with the Iceberg tables.
2. Write `INT(8)`/`INT(16)` annotations in Iceberg.Net.Data.
3. An API review, then a named consumer, before graduation.
