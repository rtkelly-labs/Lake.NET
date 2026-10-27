# DuckLake (`Iceberg.Net.DuckLake`)

A C# implementation of the [DuckLake 1.0](https://ducklake.select/docs/stable/specification/introduction) table
format, built inside this repository on the Iceberg data plane and held to the same rules as the Iceberg code.

**Status:** evaluation build-out. It reads and writes lakes that DuckDB's `ducklake` extension (the reference
implementation) reads and writes, verified in both directions by cross-engine tests. Not packed or published
(`IsPackable=false`); the API is not settled. Tested on SQLite catalogs only.

## Why DuckLake fits here

DuckLake keeps table metadata in ordinary SQL tables (28 of them, in any database with transactions and primary
keys) instead of Iceberg's JSON + Avro manifest tree, but its data plane is the same: Parquet files whose
`field_id`s are the columns' IDs (DuckLake `column_id`), positional delete files, min/max statistics per file and
column. So a DuckLake table maps onto an Iceberg `Schema`, and everything below the metadata layer is reused as is.

| Reused from Iceberg.NET, unchanged | Replaced for DuckLake |
|---|---|
| `ParquetFiles`: field-ID writer and reader, nested columns (Dremel), ranged reads, row-group pruning | Metadata JSON, manifests, manifest lists, the Avro codec |
| The type model and physical values (`IcebergType`, `IcebergDecimal`, `IcebergTime`) | The metastore commit protocol (DuckLake commits are one SQL transaction) |
| `Binder`, `MetricsEvaluator`, `RowEvaluator` (pruning and exact residual filtering) | Catalogs (the SQL database *is* the catalog) |
| `IFileIO` (local, S3), write-once files | Puffin (DuckLake's deletion vectors are experimental and not read) |
| The `Corruption` contract, `[Spec]` traceability, the fuzz harness, the oracle-test pattern | |

About 2,500 lines of library code. No existing library changed; the shared pieces that did are listed under
[Tests](#tests).

## Evaluation of Iceberg.NET as the base

What made this quick, and what got in the way, for whoever takes either format further.

- **The data plane is genuinely format-neutral.** `ParquetFiles.WriteAsync/ReadBatchesAsync` take a `Schema`, write
  field IDs on every node and resolve by field ID on read; DuckDB reads its files, nested types included, with no
  changes. Row-group pruning, ranged reads and missing-column defaults (`initial-default`) came for free.
- **The expression stack composes.** `MetricsEvaluator.MightMatch(recordCount, statsOf)` takes typed statistics from
  any source, so DuckLake's string statistics prune files after one parse step; `RowEvaluator` gives exact filters.
- **The rules transfer.** "Resolve by field ID", "files are never overwritten", "nothing is deleted after an unknown
  commit", "decoders of untrusted input are guarded and fuzzed" and "behaviour other engines observe needs a
  cross-engine test" all apply to DuckLake unchanged (see [the same constraints](#the-same-constraints)).
- **Friction found:**
  - The positional-delete reader (`PositionDeleteFiles`) hard-codes Iceberg's reserved field IDs; DuckDB writes
    different ones, so DuckLake has its own small reader that accepts both.
  - `ParquetFiles.WriteAsync` does not report the footer length, which DuckLake records (`footer_size`); it costs one
    8-byte ranged read per file. Returning it from the writer would remove that.
  - `int8`/`int16` are written as plain `INT32` (Iceberg has no narrower ints). DuckDB reads them correctly, but the
    files lose the `INT(8)`/`INT(16)` annotation DuckDB itself writes.
  - Rows are `object?[]` of *physical* values (days, microseconds, unscaled decimals). Correct and fast, but a C#
    caller converts by hand; `DuckLakeValues.Normalize` accepts CLR types on the way in, nothing converts on the way
    out. The LINQ layer's mapping (`docs/linq.md`) is the natural next step for both formats.
  - `tools/fuzz.sh` fails at start-up in the cloud sandbox (an access violation inside libfuzzer-dotnet, for the
    existing targets too), so the new targets have only run as the in-process mutation test.

## Usage

```csharp
using Iceberg.Net.DuckLake;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Microsoft.Data.Sqlite;

SqliteConnection Connect() => new("Data Source=lake/catalog.sqlite");
var lake = await DuckLakeCatalog.CreateAsync(Connect, new LocalFileIO(), "lake/data/");   // or OpenAsync

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
```

DuckDB opens the same lake with `ATTACH 'ducklake:sqlite:lake/catalog.sqlite' AS lake`. A longer walk-through
(partitioning, UPDATE, schema evolution, time travel) is `examples/DuckLake.Quickstart`:

```bash
dotnet run --project examples/DuckLake.Quickstart [directory]
```

## Layout

| File | What |
|---|---|
| `DuckLakeCatalog.cs` | Create/open, snapshots, schema and table lookup at a snapshot, scan planning (statistics pruning), reads |
| `DuckLakeTransaction.cs` | Staged DDL and data operations, and the commit: ID assignment, conflict check, catalog rows |
| `DuckLakeChanges.cs` | `changes_made` parsing and formatting, and the spec's logical-conflict rules |
| `DuckLakeTypes.cs` | DuckLake types and columns, `ducklake_column` trees, the mapping to Iceberg fields |
| `DuckLakeValues.cs` | Value normalisation, statistics text encoding (spec "Type Encoding for Statistics"), statistics collection |
| `DuckLakeDeleteFiles.cs` | Parquet delete files; DuckDB's inlined data tables |
| `DuckLakeNestedText.cs` | DuckDB's text form of nested inlined values |
| `DuckLakePartitioning.cs` | Partitioned writes (`identity`, `year`, `month`, `day`, `hour`) |
| `DuckLakeSql.cs` | The catalog DDL per database dialect, ADO.NET helpers |

## What works

| Area | State |
|---|---|
| Catalog | Creates the 28 tables (matching DuckDB's SQLite schema exactly) and snapshot 0 with schema `main`, or opens an existing 1.0 catalog. Any ADO.NET provider; the dialect (`DuckLakeDialect`) says how BOOLEAN/UUID/TIMESTAMPTZ are stored |
| Reads | Snapshot-scoped catalog queries (spec "Queries"), relative/absolute path chains, delete files, inlined rows (primitive and nested), inlined deletions, DuckDB's partial (merged/flushed) data and delete files with per-row snapshots, row IDs, time travel by snapshot or timestamp |
| Pruning | File statistics through the Iceberg metrics evaluator; Parquet row groups through footer statistics; exact residual filter |
| Writes | CREATE/DROP SCHEMA, CREATE/DROP TABLE, INSERT (file, file and table statistics, row IDs), DELETE (merge-on-read; a replacement delete file per data file, emptied files removed), UPDATE (delete + insert), ADD/DROP/RENAME COLUMN, partitioned inserts, author/message/extra info |
| Types | `boolean`, `int8`–`int64`, `float32/64`, `decimal(p,s)`, `date`, `time`, `timestamp`, `timestamptz`, `timestamp_ns`, `varchar`, `json`, `blob`, `uuid`, `list`, `struct`, `map` |
| Concurrency | Optimistic commits: on a lost race for the snapshot ID, the `changes_made` of every snapshot since the base are checked against the spec's logical conflicts, then the metadata is retried without rewriting files; delete-vs-delete is decided per data file |

## The same constraints

| Iceberg.NET rule (AGENTS.md) | For DuckLake |
|---|---|
| The spec wins; tests are tagged with the section they verify | `[Spec("ducklake: INSERT")]` links to sections of the DuckLake spec pinned in `docs/spec/ducklake-sections.json`; `spec_trace.py matrix --check` fails on unknown links, `drift --ducklake` reports upstream changes. All 48 implementable sections have tests. Where DuckDB and the spec text disagree, the difference is listed below rather than silently followed |
| Resolve by field ID, never by name or position | Data files by `column_id` = `field_id`; delete files by their field IDs; inlined tables by the column IDs of the schema version they were created for |
| Metadata files are never overwritten; nothing written for an unknown commit is deleted | Data and delete files are written once (`overwrite: false`) with UUIDv7 names; files are deleted only when the commit is known not to have happened (conflict, error before COMMIT, abandoned transaction); a failure *during* COMMIT throws `DuckLakeCommitStateUnknownException` and keeps them |
| New behaviour other engines can observe needs a cross-engine test | `CrossEngineTests`: DuckDB reads every type, delete, update, schema change and partition written here and agrees with time travel at every snapshot; this library reads DuckDB's inlined rows, inlined deletions, flushed and merged partial files, and partitioned tables; the two engines write to one lake in turn and conflict correctly |
| Decoders of untrusted input follow the `Corruption` contract | Catalog rows are untrusted: `changes_made`, column trees, statistics, timestamps and inlined nested text fail with format errors (tested in `CatalogTests`/`EncodingTests`), nesting is bounded, and `tools/fuzz.sh` has `ducklake-changes` and `ducklake-inlined` targets plus an always-on mutation test |
| Fixtures are other engines' outputs, never hand-edited | No committed fixtures: the oracle generates DuckDB's lakes at test time |
| Warnings are errors | Unchanged (`Directory.Build.props`) |

## Where the reference implementation and the spec differ

DuckLake is a less formal specification than Iceberg's; several behaviours are defined only by DuckDB. Each item
below was observed with DuckDB 1.5.5 (`ducklake` extension `d8a1881e`) and is what this library does.

| Topic | Spec text | DuckDB | Here |
|---|---|---|---|
| `value_count` in file statistics | "number of values (including NULL and NaN values)" (Queries, INSERT) | Non-null values only | Write DuckDB's meaning; read either safely (an over-count only ever disables pruning) |
| Delete file layout | "the row ids of rows that are deleted" | Iceberg positional layout `(file_path, pos)`, `pos` = row position in the data file, field IDs 2147483646/2147483645 (not Iceberg's 2147483546/2147483545) | Write DuckDB's layout and IDs; read both ID pairs |
| `ducklake_snapshot` timestamp column | `snapshot_timestamp` in the Queries page's INSERT | `snapshot_time` (as the table page says) | `snapshot_time` |
| `file_order` | "needs to be unique within a snapshot" | Written as NULL | Written as the `data_file_id` |
| Partial files | `partial_max` described; the per-row columns are not | `_ducklake_internal_snapshot_id` (field 2147483539) and `_ducklake_internal_row_id` (2147483540) in data and delete files | Read both; rows and deletions after the scanned snapshot are ignored |
| Inlined data and deletions | Data inlining page (not the specification) | `ducklake_inlined_data_{table}_{schema_version}` and `ducklake_inlined_delete_{table}` (the latter not listed in any catalog table); nested values as DuckDB's text cast | Read both, nested values included; never written (inlining is optional) |
| Nested inlined values | Not specified | Text casts such as `{'c': {'x': CAST('2024-01-02' AS DATE)}}`, which DuckDB 1.5.5 itself fails to read back for dates inside structs | Parsed against the column type, `CAST('…' AS T)` included |
| `changes_made` kinds | 11 kinds | Also `inlined_insert`, `inlined_delete`, `inline_flush` | Treated as insert, delete and compaction for conflict checks; unknown kinds conflict only with tables this transaction touches |
| Statistics encodings | `boolean` as `0`/`1`, `blob` as hex | Inlined rows' table statistics hold `true`, `nan`, `\x00\xFF` | Write the spec's encodings (no bounds for blobs); parse all of these, and treat anything unparseable as "no bound" |

## What is not implemented

Views, macros, tags and comments, sort orders, encryption, name-mapped files (`ducklake_add_data_files` without
field IDs), `variant` and `geometry`, unsigned/128-bit integers, `interval`, `timetz`, second/millisecond timestamps,
the experimental Puffin deletion vectors, writing inlined data, and maintenance (snapshot expiry, file cleanup,
compaction). Each fails with `NotSupportedException` when met in a catalog or file rather than reading wrong values.
PostgreSQL and other catalog databases need a `DuckLakeDialect` matching DuckDB's schema for them; only SQLite is
tested. The same list is in `tools/spec_trace.py` (`NOT_IMPLEMENTED`).

Within a transaction, a delete must be the first data operation on its table, and a table created in the transaction
cannot be altered in it (define its columns at creation).

## Tests

| Suite | Needs | Command |
|---|---|---|
| `EncodingTests`, `ConflictRuleTests`, `CatalogTests` (104 tests) | nothing | `dotnet test tests/Iceberg.Net.DuckLake.Tests --filter "FullyQualifiedName!~CrossEngine"` |
| `CrossEngineTests` (11 tests) | a Python with the interop `duckdb` group | `ICEBERG_DUCKDB_PYTHON=<python> dotnet test tests/Iceberg.Net.DuckLake.Tests` |

The project is in the `cross-engine` CI suite, which installs the `duckdb` group (now including the `ducklake`,
`sqlite_scanner` and `parquet` extension wheels, so the oracle also loads offline) and sets
`ICEBERG_DUCKDB_REQUIRED=1`, so an oracle that cannot load fails instead of skipping. The oracle is
`interop/ducklake_oracle.py`: it attaches the test's SQLite catalog in DuckDB and runs SQL against it.

Shared pieces changed for this: `SpecAttribute` accepts the `ducklake:` prefix; `tools/spec_trace.py` pins and
checks the DuckLake spec (`snapshot --ducklake`, `drift --ducklake`, a DuckLake line in the matrix);
`tests/Iceberg.Net.Fuzz` and `tools/fuzz.sh` gained two targets; the interop `duckdb` dependency group gained the
extension wheels.

## Next steps

1. Run CI on the branch (the `cross-engine` job is the first run of the oracle on GitHub's runners).
2. A PostgreSQL dialect and a test against a PostgreSQL container (DuckLake's most common production catalog).
3. Typed rows through the LINQ layer's mapping, shared with the Iceberg tables.
4. Maintenance: `expire_snapshots` and file cleanup through `ducklake_files_scheduled_for_deletion`.
5. Return the footer size from `ParquetFiles.WriteAsync`, and write `INT(8)`/`INT(16)` annotations, in Iceberg.Net.Data.
