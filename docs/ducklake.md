# DuckLake (`Iceberg.Net.DuckLake`)

A C# implementation of the [DuckLake](https://ducklake.select/docs/stable/specification/introduction) table
format (1.0 and 1.1), built inside this repository on the Iceberg data plane and held to the same rules as the Iceberg code.

**Status:** evaluation build-out. It reads and writes lakes that DuckDB's `ducklake` extension (the reference
implementation) reads and writes, verified in both directions by cross-engine tests. Not packed or published
(`IsPackable=false`); the API is not settled. The catalog layer is EF Core on SQLite and PostgreSQL, both tested
against DuckDB, and both format versions are read and written.

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

About 3,400 lines of library code. No existing library changed; the shared pieces that did are listed under
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
  - `ParquetFiles.WriteAsync` did not report the footer length, which DuckLake records (`footer_size`), so each file
    cost an extra ranged read; `ParquetFiles.WriteFileAsync` now returns it (and the row-group count).
  - `int8`/`int16` are written as plain `INT32` (Iceberg has no narrower ints). DuckDB reads them correctly, but the
    files lose the `INT(8)`/`INT(16)` annotation DuckDB itself writes.
  - Rows are `object?[]` of *physical* values (days, microseconds, unscaled decimals). Correct and fast, but a C#
    caller converts by hand; `DuckLakeValues.Normalize` accepts CLR types on the way in, nothing converts on the way
    out. The LINQ layer's mapping (`docs/linq.md`) is the natural next step for both formats.
  - SharpFuzz-instrumented code must not run before `Fuzzer.LibFuzzer.Run` maps its coverage memory, so the fuzz
    harness builds its DuckLake type lazily (like its Iceberg metadata). An eager static field crashed every target
    at start-up with an access violation.

## Is this a good idea, and where should it live?

**Recommendation: yes, as an incubation project in this repository, behind clear graduation criteria.**

For:
- **Same job as Iceberg.NET.** It lets a C# producer commit to a lakehouse without handing the write to Spark or
  Python. DuckLake does that with one SQL transaction on a database the team already runs (PostgreSQL). There is no
  metadata-file tree, and no REST or Glue catalog to operate.
- **Cheap here.** Almost all of the hard part (Parquet with field IDs, nested columns, pruning, storage, the
  oracle-test setup) is shared. Reaching read/write parity with DuckDB, on two catalog databases and two format versions, took about 3,400 lines.
- **Momentum.** DuckLake 1.0 shipped in April 2026 and 1.1 in October 2026. DuckDB reports over 1.5 million downloads
  a week for the extension.
- **Multi-table transactions** are native, which Iceberg only gets through REST catalogs that support them.

Against:
- **Few readers.** In practice only DuckDB (and services built on it) reads DuckLake. Athena, Glue, Spark, Trino
  and Snowflake read Iceberg. A table written here as DuckLake is invisible to an AWS Athena/Glue estate. So it suits
  DuckDB-centred analytics, not tables that platform consumers query through Athena.
- **The reference implementation is the spec.** DuckDB Labs owns the format, and some behaviour is defined only by
  DuckDB (see the differences table below; 1.1 has since written several of those rules down). Releases come about
  every six months, each with a catalog migration. Keeping up is a standing cost.
- **The catalog database becomes production infrastructure.** Every read plans from it, and every commit is a
  transaction on it.

Why incubate here rather than in its own repository: the data plane is shared, and the oracle and traceability
tooling apply unchanged. A separate repository would have to copy or package `Iceberg.Net.Data` first. Rules while
incubating: no Iceberg project depends on DuckLake (one direction only), `IsPackable=false`, and DuckLake tests sit
in the `cross-engine` CI suite. Graduation criteria: CI green with the DuckDB oracle (1.0 and 1.1; done), a tested PostgreSQL
catalog (done), the current spec version supported (done), an API review, and a named consumer. At graduation, move the format-neutral
Parquet layer out of `Iceberg.Net.Data`, so DuckLake no longer pulls in Avro, Puffin and the Iceberg catalogs.

## Catalog databases

DuckLake's catalog is "any database with transactions and primary keys". What matters in practice is which
databases DuckDB can attach, because a lake that DuckDB cannot open has lost the format's only reader:

| Catalog | DuckDB | Use | From C# |
|---|---|---|---|
| PostgreSQL (12+) | `ATTACH 'ducklake:postgres:dbname=… host=…'` | Multi-user lakehouse with remote clients: the production choice | `DuckLakeStore.PostgreSql` (Npgsql EF Core), tested |
| SQLite file | `ATTACH 'ducklake:sqlite:metadata.sqlite'` | Several local processes, one writer at a time | `DuckLakeStore.Sqlite` (Microsoft.Data.Sqlite EF Core), tested |
| DuckDB file | `ATTACH 'ducklake:metadata.ducklake'` | One process | Only through DuckDB.NET; no maintained EF Core provider |
| Quack (experimental) | A DuckDB catalog served over the network | Several clients on a DuckDB catalog | No .NET client |

SQL Server, MySQL and others would work from C# but not from DuckDB, so they are out of scope.

What DuckDB creates in PostgreSQL (observed with DuckDB 1.5.5 and PostgreSQL 16; DuckLake 1.0):
- **Native types.** `boolean`, `uuid` and `timestamp with time zone` (SQLite gets `BIGINT` 0/1 and `VARCHAR`).
- **29 tables.** The 28 catalog tables, plus one inlined-data table per table and schema version.
- **Primary keys on 5 tables only:** `ducklake_snapshot`, `ducklake_snapshot_changes`, `ducklake_schema`,
  `ducklake_data_file` and `ducklake_delete_file`. Every other table has none.
- **Inlined values.** `VARCHAR` values are stored as `bytea`, as the spec's PostgreSQL inlining table says. Nested
  values and timestamps are stored as text.

So the catalog layer needs, per database: the column types DuckDB uses, the encodings of inlined values, a way to
check whether a table exists, and how it reports a unique-key violation. `DuckLakeDialect` holds these for SQLite
and PostgreSQL; `SpecSchemaTests` checks the DDL against the spec's script, and the cross-engine tests have DuckDB open the result.

## EF Core as the catalog layer

The catalog layer is EF Core 10 (`DuckLakeStore`, `DuckLakeDbContext`): the SQLite and PostgreSQL providers handle
connections, type mapping and transactions. Snapshot visibility (`s >= begin_snapshot && (end_snapshot == null || s < end_snapshot)`)
is one reusable LINQ filter (`VersionedQueries.LiveAt`), and `ExecuteUpdateAsync` does the set-based `end_snapshot`
updates. The repository already used EF Core 10 (`Iceberg.Net.EntityFrameworkCore`), so the team knows it.

**How it stays compatible with DuckDB:**
1. **No EF migrations or `EnsureCreated`.** The schema belongs to the spec and to DuckDB, which migrates it (1.0 → 1.1
   adds columns, a table, and renames inlined columns). `CreateAsync` runs the spec's script in DuckDB's column types
   for the database (`DuckLakeDialect.Ddl`); EF only queries and writes. No `__EFMigrationsHistory` is created.
2. **Column types match DuckDB's per provider.** On SQLite, value converters (bool ↔ `BIGINT`, Guid ↔ `VARCHAR`,
   `DateTimeOffset` ↔ DuckDB's text form); on PostgreSQL the native mappings already match.
3. **Keys exist in the EF model only.** Most tables have no primary key, and EF can't insert into a keyless entity, so
   the model declares composite keys (e.g. `(table_id, column_id, begin_snapshot)` for `ducklake_column`) that are
   never created in the database.
4. **No long-lived change tracking.** Reads are `AsNoTracking`. A commit is one explicit transaction of inserts
   (`SaveChanges` per operation, then the tracker is cleared) and `ExecuteUpdate`. A lost snapshot-ID race shows up
   as `DbUpdateException` and goes to the conflict check.
5. **Raw SQL for the dynamic tables.** `ducklake_inlined_data_{table}_{version}` and `ducklake_inlined_delete_{table}`
   have names and columns that vary per table; they are read through the context's connection and transaction, with
   quoted identifiers and the per-provider encodings in the dialect.
6. **A model per spec version.** 1.1 adds columns that 1.0 catalogs don't have, so the model is cached per
   (database, version) and chosen from `ducklake_metadata.version` at open time.

A DuckDB-file catalog stays out of reach: there is no maintained EF Core provider for DuckDB.

## DuckLake 1.1

DuckLake 1.1 shipped on 21 October 2026 with DuckDB 2.0. Both versions are supported side by side, chosen by
`ducklake_metadata.version` when a catalog is opened; new catalogs are 1.1 unless `CreateAsync` is given
`DuckLakeCatalog.FormatVersion10`. A catalog is never migrated here (DuckDB does that). The spec pages pinned in
`docs/spec/ducklake-sections.json` are `docs/stable` (1.1).

| 1.1 change | Here |
|---|---|
| `ducklake_schema.parent_schema_id` (nested schemas, e.g. `sales.europe.orders`) | Schemas are looked up by path; `CreateSchemaAsync("sales.europe")` creates a child; `changes_made` quotes each part (`"sales"."europe"`) |
| `row_group_count` on data and delete files | Written |
| `min_is_exact` / `max_is_exact` on file and table statistics | Written `true` where there is a bound (bounds come from the rows, untruncated), NULL where there is none |
| `ducklake_view_column_tag` table | Created; views are not implemented |
| Inlined tables' `row_id`/`begin_snapshot`/`end_snapshot` renamed `_ducklake_…` | Read by catalog version |
| `null` column type, promotable later | `DuckLakeType.Null`; columns are left out of data files and read as NULL; `AlterColumnTypeAsync` promotes them |
| Partition transforms `epoch_year`/`epoch_month`/`epoch_day`/`epoch_hour` | Iceberg.NET's time transforms (1.1 catalogs only) |
| Multi-snapshot Puffin delete files | Not read (Puffin deletion vectors are not implemented) |
| Partial files and the path chain, now in the spec's Queries page | Already implemented from DuckDB's behaviour |

The 1.1 reference is DuckDB 2.0's `ducklake` extension. PyPI has no wheel for it (the newest extension wheel is 1.5.5,
which writes 1.0), so its oracle downloads it from extensions.duckdb.org. The `cross-engine` CI job installs DuckDB 2.0
and requires it (`ICEBERG_DUCKLAKE11_REQUIRED=1`); every 1.1 case passes there. This sandbox cannot reach that host,
so locally the 1.1 oracle cases skip. The 1.0 oracle (DuckDB 1.5.5) runs everywhere.

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

## Layout

| File | What |
|---|---|
| `DuckLakeStore.cs` | The catalog database (`DuckLakeStore.Sqlite` / `PostgreSql`), the EF Core context and the entities for the fixed tables |
| `DuckLakeSql.cs` | `DuckLakeDialect`: the spec's DDL per database and format version, DuckDB's storage of BOOLEAN/UUID/TIMESTAMPTZ, raw-SQL helpers |
| `DuckLakeCatalog.cs` | Create/open, snapshots, schema and table lookup at a snapshot, scan planning (statistics pruning), reads |
| `DuckLakeTransaction.cs` | Staged DDL and data operations, and the commit: ID assignment, conflict check, catalog rows |
| `DuckLakeChanges.cs` | `changes_made` parsing and formatting, and the spec's logical-conflict rules |
| `DuckLakeTypes.cs` | DuckLake types and columns, `ducklake_column` trees, the mapping to Iceberg fields |
| `DuckLakeValues.cs` | Value normalisation, statistics text encoding (spec "Type Encoding for Statistics"), statistics collection |
| `DuckLakeDeleteFiles.cs` | Parquet delete files (partial ones too); DuckDB's inlined data and deletion tables, read and written |
| `DuckLakeNestedText.cs` | DuckDB's text form of nested inlined values |
| `DuckLakePartitioning.cs` | Partitioned writes (`identity`, `year`/`month`/`day`/`hour`, 1.1's `epoch_*`, `bucket(N)`) |
| `DuckLakeValidator.cs` | Whole-lake consistency check: snapshot and ID counters, versioned ranges, delete files, row IDs, files in storage |
| `DuckLakeObjects.cs`, `DuckLakeTransaction.Objects.cs` | Tags and comments, table renames, views, macros, sort orders: reading and writing |
| `DuckLakeTransaction.Inlining.cs` | Data inlining limits and flushing inlined data |
| `DuckLakeMaintenance.cs`, `DuckLakeTransaction.Maintenance.cs` | Expiry, cleanup and orphan removal; merging and rewriting files (staged and committed like other operations) |
| `DuckLakeTelemetry.cs` | Spans and metrics (`Iceberg.Net.DuckLake`), with the catalog commands each operation ran |

## What works

| Area | State |
|---|---|
| Catalog | Creates the catalog (matching DuckDB's schema for SQLite and PostgreSQL exactly, per format version) with snapshot 0 and schema `main`, or opens an existing 1.0 or 1.1 catalog. EF Core over SQLite or PostgreSQL |
| Reads | Snapshot-scoped catalog queries (spec "Queries"), relative/absolute path chains, nested schemas (1.1), delete files, inlined rows (primitive and nested), inlined deletions, DuckDB's partial (merged/flushed) data and delete files with per-row snapshots, row IDs, time travel by snapshot or timestamp |
| Pruning | File statistics through the Iceberg metrics evaluator; Parquet row groups through footer statistics; exact residual filter |
| Writes | CREATE/DROP SCHEMA (nested in 1.1), CREATE/DROP TABLE, INSERT (file, file and table statistics, row IDs), DELETE (merge-on-read; a replacement delete file per data file, emptied files removed), UPDATE (delete + insert in one snapshot; the new versions keep their row IDs in `_ducklake_internal_row_id`, as the spec allows and DuckDB does), ADD/DROP/RENAME COLUMN, type widening (and `null` promotion), partitioned inserts, author/message/extra info |
| Types | `boolean`, `int8`–`int64`, `uint8`–`uint64`, `float32/64`, `decimal(p,s)`, `date`, `time`, `timetz`, `timestamp`, `timestamp_s`, `timestamp_ms`, `timestamptz`, `timestamp_ns`, `interval`, `varchar`, `json`, `blob`, `uuid`, `list`, `struct`, `map`, and 1.1's `null` (see [Types](#types)) |
| Concurrency | Optimistic commits: on a lost race for the snapshot ID, the `changes_made` of every snapshot since the base are checked against the spec's logical conflicts, then the metadata is retried without rewriting files; delete-vs-delete is decided per data file. Tested with concurrent writers on both databases, and with DuckDB writing in between |
| Catalog objects | Tags and comments (tables, columns and nested fields, views; view columns in 1.1), table renames, views, scalar and table macros, sort orders (written files follow the leading column keys); DuckDB uses each, and this library reads DuckDB's (see [Catalog objects](#catalog-objects)) |
| Maintenance | Merging adjacent small files into partial files, rewriting files with many deleted rows, expiring snapshots, deleting scheduled and orphaned files; DuckDB reads the results and cleans up after them, and the reverse (see [Maintenance](#maintenance)) |
| Existing files | `AddDataFilesAsync` registers Parquet files written elsewhere, in place, under a `map_by_name` column mapping with footer statistics, as `ducklake_add_data_files` does (Hive partition columns, missing and extra columns); files DuckDB added are read through their mapping (see [Adding existing files](#adding-existing-files)) |
| Deletion vectors (1.1) | Puffin delete files read (bare `deletion-vector-v1` blobs, and containers of cumulative vectors keyed by `ducklake-snapshot-id`, the latest not after the scanned snapshot); written as bare vectors when `write_deletion_vectors` is set in the lake or `DuckLakeOptions.WriteDeletionVectors` is on |
| Encryption | Encrypted lakes (`DuckLakeOptions.Encrypted`, or an existing lake's `encrypted` setting): every Parquet data and delete file written with Parquet modular encryption (AES-GCM, encrypted footer) under a fresh 256-bit key per write, kept Base64 in `encryption_key` as DuckDB keeps it; DuckDB reads these lakes, and this library reads DuckDB's. Existing files cannot be added to an encrypted lake |
| Validation | `DuckLakeValidator` checks a whole lake (any writer's) and reports problems; `Deep` also reads every live file |

## The same constraints

| Iceberg.NET rule (AGENTS.md) | For DuckLake |
|---|---|
| The spec wins; tests are tagged with the section they verify | `[Spec("ducklake: INSERT")]` links to sections of the DuckLake spec pinned in `docs/spec/ducklake-sections.json`; `spec_trace.py matrix --check` fails on unknown links, `drift --ducklake` reports upstream changes. All 49 implementable sections (1.1) have tests. Where DuckDB and the spec text disagree, the difference is listed below rather than silently followed |
| Resolve by field ID, never by name or position | Data files by `column_id` = `field_id`; delete files by their field IDs; inlined tables by the column IDs of the schema version they were created for |
| Metadata files are never overwritten; nothing written for an unknown commit is deleted | Data and delete files are written once (`overwrite: false`) with UUIDv7 names; files are deleted only when the commit is known not to have happened (conflict, error before COMMIT, abandoned transaction); a failure *during* COMMIT throws `DuckLakeCommitStateUnknownException` and keeps them |
| New behaviour other engines can observe needs a cross-engine test | `CrossEngineTests` (each case on SQLite and PostgreSQL, 1.0 and 1.1) and the BDD `CrossEngine.feature`: DuckDB reads every type, delete, update, schema change and partition written here and agrees with time travel at every snapshot; this library reads DuckDB's inlined rows, inlined deletions, flushed and merged partial files, and partitioned tables; the two engines write to one lake in turn and conflict correctly |
| Decoders of untrusted input follow the `Corruption` contract | Catalog rows are untrusted: `changes_made`, column trees, statistics, timestamps and inlined nested text fail with format errors (tested in `CatalogTests`/`EncodingTests`), nesting is bounded, and `tools/fuzz.sh` has `ducklake-changes` and `ducklake-inlined` targets (in the weekly `fuzz` workflow; about 500,000 runs each without a finding so far) plus an always-on mutation test |
| Fixtures are other engines' outputs, never hand-edited | No committed fixtures: the oracle generates DuckDB's lakes at test time |
| A BDD specification for user-visible behaviour | `Iceberg.Net.DuckLake.Behaviour.Tests`: Gherkin features for tables, deletes, schema evolution, partitioning, concurrency, format versions and DuckDB interop; every scenario ends with a deep validation |
| Property tests against a model | `ModelBasedTests`: random operation sequences against an in-memory model, shrinking to a minimal failing sequence with a replay seed |
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
| Inlined data and deletions | Data inlining page and "Type Encoding for Data Inlining" | `ducklake_inlined_data_{table}_{schema_version}` and `ducklake_inlined_delete_{table}` (the latter not listed in any catalog table); nested values as DuckDB's text cast | Read and written with DuckDB's table layout and column types (see [Data inlining](#data-inlining)) |
| Inlined quotes in nested strings | Not specified | Writes `'it''s'` and reads it back as `its` | Written with backslash escapes (`'it\'s'`), which both read correctly; `''` is read as a quote |
| `next_file_id` on an inlined insert | Not specified | Advances by one per table, though no file is written | The same |
| Nested inlined values | Not specified | Text casts such as `{'c': {'x': CAST('2024-01-02' AS DATE)}}`, which DuckDB 1.5.5 itself fails to read back for dates inside structs | Parsed against the column type, `CAST('…' AS T)` included |
| Inlined deletions vs delete files | Not specified | A row deleted from a file while its deletions are inlined is recorded in `ducklake_inlined_delete_{table}`, not in the file's delete file; the two are combined on read | A replacement delete file holds the previous delete file's positions plus the new ones, never the inlined deletions (merging them made DuckDB count the rows twice) |
| Expiry and `ducklake_snapshot_changes` | "Snapshot expiration keeps rows in `ducklake_snapshot_changes`" (1.1) | 1.5.5 deletes them with the snapshots | Kept, as the spec says: a commit whose base snapshot was expired still sees every change since it |
| A `DELETE` that matches no rows | "still records a delete attempt for conflict checking" | Nothing is written for it (no snapshot on its own; only the insert alongside it) | The same: it counts in this transaction's conflict check only |
| Renames | `created_table`/`created_view` with the new name are not described for renames | Records a rename as `created_table`/`created_view` with the new name | The same; for conflicts a rename counts as altering the table or view |
| `{DUCKLAKE_CATALOG}` | Resolved when loading a view | DuckDB 1.5.5 qualifies view SQL with it, DuckDB 2.0 stores names unqualified; resolved in views; a macro body holding it fails to load; DuckDB stores macro bodies with the catalog name they were attached under | Stored as given; `DuckLakeView.SqlFor(catalog)` resolves it |
| `changes_made` kinds | 11 kinds | Also `inlined_insert`, `inlined_delete`, `inline_flush` | Treated as insert, delete and compaction for conflict checks; unknown kinds conflict only with tables this transaction touches |
| `timetz` and `interval` in Parquet | "Data Types" lists them; their Parquet form is not described | `timetz` as a UTC-adjusted TIME (the offset is lost, the time wraps into one day); `interval` as Parquet INTERVAL (milliseconds; negative intervals refused); no statistics for either | The same, so a file round trip loses what DuckDB's does; inlined rows keep offsets and microseconds |
| Inlined structs holding `timetz` | Not specified | Stored as `{'t': CAST('…' AS TIME WITH TIME ZONE)}`, which DuckDB 1.5.5 cannot read back | Written as quoted text, which both read |
| Statistics encodings | `boolean` as `0`/`1`, `blob` as hex | Inlined rows' table statistics hold `true`, `nan`, `\x00\xFF` | Write the spec's encodings (no bounds for blobs); parse all of these, and treat anything unparseable as "no bound" |

## What is not implemented

`variant` and `geometry`, `int128`/`uint128` (DuckDB writes them to Parquet as DOUBLE and cannot read
them back), `time_ns` (DuckDB cannot write it to Parquet), inlining into partitioned tables (their small inserts go to files), and
catalog migration (1.0 → 1.1 is DuckDB's). Each fails with `NotSupportedException` when met in a catalog
or file rather than reading wrong values. The same list is in `tools/spec_trace.py` (`NOT_IMPLEMENTED`).

Within a transaction, a delete must be the first data operation on its table, and a table created in the transaction
cannot be altered in it (define its columns at creation).

## Tests

| Suite | Needs | Command |
|---|---|---|
| `Iceberg.Net.DuckLake.Tests`: `EncodingTests`, `TypeTests`, `TemporalPropertyTests`, `AddFilesTests`, `ClrTests`, `DeletionVectorTests`, `EncryptionTests`, `ConflictRuleTests`, `CatalogTests`, `FormatVersionTests`, `ValidatorTests`, `ModelBasedTests`, and the inlining, maintenance, catalog object and performance tests | nothing (PostgreSQL variants skip without a server) | `dotnet test tests/Iceberg.Net.DuckLake.Tests --filter "FullyQualifiedName!~CrossEngine"` |
| `CrossEngineTests` | a Python with the interop `duckdb` group (1.0); DuckDB 2.0 (1.1) | `ICEBERG_DUCKDB_PYTHON=<python> dotnet test tests/Iceberg.Net.DuckLake.Tests` |
| `Iceberg.Net.DuckLake.Behaviour.Tests` (BDD) | as above for the DuckDB scenarios (`CrossEngine.feature` and the 1.1 ones in `FormatVersions.feature`) | `ICEBERG_DUCKDB_PYTHON=<python> dotnet test tests/Iceberg.Net.DuckLake.Behaviour.Tests` |

Every catalog-facing test class runs per database and format version (`SqliteCatalogTests10`, `PostgresCatalogTests11`,
…); `TestLake` (in `Iceberg.Net.DuckLake.TestSupport`) gives each test its own SQLite file or PostgreSQL database,
dropped afterwards, and validates the lake when the test ends. Environment:

| Variable | Meaning |
|---|---|
| `ICEBERG_DUCKLAKE_POSTGRES` | Npgsql connection string of a server the tests may create databases on (`emulation/up.sh` starts one on port 5434) |
| `ICEBERG_DUCKLAKE_POSTGRES_REQUIRED=1` | Fail instead of skipping when it is unset or unreachable (CI) |
| `ICEBERG_DUCKDB_PYTHON` / `ICEBERG_DUCKDB_REQUIRED=1` | The 1.0 oracle: a Python with the interop `duckdb` group |
| `ICEBERG_DUCKLAKE11_PYTHON` / `ICEBERG_DUCKLAKE11_REQUIRED=1` | The 1.1 oracle: a Python with `duckdb==2.0.0` (downloads the extension) |
| `ICEBERG_PBT_RUNS` / `ICEBERG_PBT_SEED` | Model-based test: number of cases, and a replay seed printed by a failure |

In CI (both databases, both oracles): 588 of the 590 tests (the two format-1.0 runs of the deletion-vector cross-engine test skip by design) and all 111 BDD scenarios pass. Locally without
the 1.1 oracle: 542 and 95 pass, the 48 and 16 others skip (the encrypted-lake test because DuckDB cannot download httpfs here). The projects are in the `cross-engine` CI suite, which starts the emulation stack's
PostgreSQL, installs both oracles and requires all three. The oracle is `interop/ducklake_oracle.py`: it attaches the
test's catalog (SQLite or PostgreSQL) in DuckDB and runs SQL against it.

Shared pieces changed for this: `SpecAttribute` accepts the `ducklake:` prefix; `tools/spec_trace.py` pins and
checks the DuckLake spec (`snapshot --ducklake`, `drift --ducklake`, a DuckLake line in the matrix);
`tests/Iceberg.Net.Fuzz` and `tools/fuzz.sh` gained two targets; the interop `duckdb` dependency group gained the
extension wheels (including `postgres_scanner`) and `pytz` (DuckDB's client needs it for `timestamptz` values); the emulation stack gained a PostgreSQL.

## Types

Every type is held in rows as the physical value of its Iceberg column type, so the Iceberg Parquet reader and writer
serve DuckLake unchanged; Parquet annotations make the files match DuckDB's byte for byte in layout.

| DuckLake type | Value in rows | Parquet column (as DuckDB writes it) | Statistics text |
|---|---|---|---|
| `int8`, `int16` | `int` | INT32, INT(8/16, signed) | decimal |
| `uint8`, `uint16` | `int` | INT32, INT(8/16, unsigned) | decimal |
| `uint32` | `long` | INT32, INT(32, unsigned) | decimal |
| `uint64` | `IcebergDecimal` with scale 0 (0 to 2⁶⁴−1) | INT64, INT(64, unsigned) | decimal |
| `timestamp_s`, `timestamp_ms` | `long` microseconds, truncated to the unit | INT64 TIMESTAMP(MICROS) / TIMESTAMP(MILLIS) | `2024-01-02 03:04:05[.678]` |
| `timetz` | `DuckLakeTimeTz` (microseconds of the day, offset in seconds) | INT64 TIME(MICROS, UTC-adjusted): the instant only | none |
| `interval` | `DuckLakeInterval` (months, days, microseconds) | FIXED_LEN_BYTE_ARRAY(12) INTERVAL: months, days, milliseconds | none |

Inserts accept the CLR forms too (`byte`…`ulong`, `DateTime`, `DateTimeOffset` for `timetz`, `TimeSpan` for `interval`,
and DuckDB's text for both), range-checked. A file keeps a `timetz` at offset `+00` and an interval's milliseconds, as
DuckDB's files do; inlined rows keep both exactly (DuckDB's text on SQLite, native `TIME WITH TIME ZONE` and `INTERVAL`
on PostgreSQL). Negative intervals can be inlined but not written to a file, so DuckDB and this library both refuse to
flush them. `timetz` and `interval` have no order shared with their Parquet form, so they cannot be filtered on or
partitioned by; unsigned and second/millisecond timestamp columns can be filtered on and partitioned by identity (and
by calendar transforms for the timestamps), not bucketed. The reader decodes unsigned and millisecond columns from the
file's own annotations, so Iceberg tables written by other engines with them read correctly too.

## Adding existing files

```csharp
await using var tx = lake.BeginTransaction();
await tx.AddDataFilesAsync("main", "events", ["s3://bucket/exports/region=eu/part-0.parquet"],
    new DuckLakeAddFileOptions { HivePartitioning = true, AllowMissing = true });
await tx.CommitAsync();
```

Each file's footer is read (not its data): columns are matched to the table's by name (struct fields by name, list
elements and map keys and values by position), whatever field IDs the file has, and each must be readable as its
column's type (a narrower integer into a wider one, millisecond into microsecond timestamps; not text into numbers).
Missing columns read as their default and extra columns are ignored only when allowed; with `HivePartitioning`,
columns the file lacks take their value from `name=value` segments of its path. The catalog gets what DuckDB 1.5.5
writes for the same file, row for row: a `ducklake_column_mapping` of type `map_by_name` (its ID taken from
`next_file_id` before the file's; files of one layout added in one commit share it), `ducklake_name_mapping` rows
in the file's field order (a list's element under its repeated group's name, `list`), the data file by absolute path
with `mapping_id`, and per-column statistics from the footer. Reads of any mapped file (DuckDB's or this library's)
match by those names; deletes, updates and file rewrites work on them as on any other file. Adjacent-file merging
leaves them alone, as their partition values live in their paths.

## Catalog objects

Metadata besides tables, written as DuckDB 1.5.5 writes it (probed with the oracle). Each change bumps the snapshot's
`schema_version`; renames also add a `ducklake_schema_versions` row, comments and sort orders do not.

| Call | Catalog rows | Change recorded |
|---|---|---|
| `SetTagAsync`, `SetCommentAsync` (key `comment`); `SetColumnTagAsync`/`SetColumnCommentAsync` (nested fields as `a.b`) | `ducklake_tag` / `ducklake_column_tag`: a new version, the old one ended; null removes | `altered_table:{id}` |
| `SetViewTagAsync`/`SetViewCommentAsync`; `SetViewColumnTagAsync` (1.1) | `ducklake_tag` with the view's ID; `ducklake_view_column_tag` | `altered_view:{id}` |
| `RenameTableAsync` / `RenameViewAsync` | A new version of the row, same ID (and, for a table, files and directory) | `created_table` / `created_view` with the new name |
| `CreateViewAsync(sql, columnAliases, dialect)`, `DropViewAsync` | `ducklake_view` (ID from `next_catalog_id`; aliases as `"a","b"`); dropping ends its tags | `created_view`, `dropped_view` |
| `CreateMacroAsync` (one or more implementations, parameters with types and defaults), `DropMacroAsync` | `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters` | `created_scalar_macro`/`created_table_macro`, `dropped_*_macro` |
| `SetSortOrderAsync(keys)` (empty removes) | `ducklake_sort_info`, `ducklake_sort_expression` (`ASC`/`DESC`, `NULLS_FIRST`/`NULLS_LAST`) | `altered_table:{id}` |

`LoadTableAsync` returns tags, column tags and sort keys; `ListViewsAsync`/`LoadViewAsync` and `ListMacrosAsync`/
`LoadMacroAsync` read views and macros, at any snapshot. Views and macros are stored and returned, not executed: this
library has no SQL engine. Tables and views share a schema's names, and a schema holding views or macros is not empty
for `DropSchemaAsync`. Dropping a column ends its tags (its fields' too, as the spec says); dropping a table ends its
tags and sort order. Conflicts follow the spec: two creates of one view or macro name, a create in a schema dropped
meanwhile, two drops of one macro, an alteration of a view altered or dropped meanwhile, two renames of one table.

## Data inlining

Opt in with `DuckLakeOptions.DataInliningRowLimit` (DuckDB's default is 10; here it is 0, off), or persist a limit in
the lake as DuckDB's `set_option('data_inlining_row_limit', …)` does (`ducklake_metadata`, scope `table`, `schema` or
catalog-wide), which takes precedence. Then:

- **Inserts** of at most the limit go into `ducklake_inlined_data_{table}_{schema_version}`, created on first use with
  the columns of the table's current schema version and registered in `ducklake_inlined_data_tables`. Rows get IDs from
  `next_row_id` (an UPDATE's new versions keep theirs) and count in the table statistics; the snapshot records
  `inlined_insert:{table}`. Partitioned tables are not inlined into.
- **Deletes** of inlined rows set their end snapshot; deletes of at most the limit from one data file go into
  `ducklake_inlined_delete_{table}` (`file_id`, position, snapshot), beside the file's delete file. Recorded as
  `inlined_delete:{table}`.
- **Column types and encodings** are DuckDB's (spec "Type Encoding for Data Inlining", checked against DuckDB 1.5.5): on
  SQLite `BIGINT` for booleans and integers, `BLOB` for blobs, `VARCHAR` text for everything else; on PostgreSQL native
  `BOOLEAN`, `SMALLINT`/`INTEGER`/`BIGINT`, `REAL`/`DOUBLE PRECISION`, `NUMERIC(p,s)`, `UUID`, `TIME`, `BYTEA` for
  varchar, json and blob, and text for dates, timestamps and nested values. Nested values are DuckDB's text cast.
  Decimals of more than 28 digits are not inlined on PostgreSQL (.NET's `decimal` cannot carry them).
- **`FlushInlinedDataAsync(schema?, table?)`** (`ducklake_flush_inlined_data`) moves them to Parquet in one snapshot
  (`inline_flush:{table}`, plus `deleted_from_table` when it writes delete files, as DuckDB records it): each inlined
  table becomes a partial data file in its own schema version's columns, holding every row version with its row ID and
  snapshot; rows deleted while inlined go into a partial delete file; inlined deletions of a data file are merged with
  its delete file into one partial delete file. Every snapshot reads what it did before. Rows inserted while a flush
  is staged stay inlined; inlined deletions committed meanwhile make it conflict (spec, Data).

DuckDB reads every type this library inlines (strings with quotes, backslashes and braces inside nested values
included), and each engine flushes the other's inlined data (`CrossEngineTests`). One-row commits get about a third
faster (see [Performance](#performance)); against object storage, where each file is a request, the gain is larger.

## Maintenance

DuckDB's maintenance functions are extension behaviour, not format; these do what DuckDB 1.5.5 does to the catalog
(probed with the oracle), and DuckDB and this library clean up after each other.

| Call | DuckDB | Catalog effect |
|---|---|---|
| `MergeAdjacentFilesAsync(schema?, table?, DuckLakeMergeOptions)` | `ducklake_merge_adjacent_files` | One snapshot (`merge_adjacent:{table}`). Files of one partition, in commit order, grouped towards `TargetFileSize`, become one *partial* file holding each row's row ID and writing snapshot (`_ducklake_internal_row_id`, `_ducklake_internal_snapshot_id`); it begins at the earliest input's snapshot, with `partial_max` the latest. The inputs leave `ducklake_data_file` and are scheduled for deletion. Files with deletes, and files from before the table's last schema change, are skipped |
| `RewriteDataFilesAsync(schema?, table?, deleteThreshold = 0.95)` | `ducklake_rewrite_data_files` | One snapshot (`rewrite_delete:{table}`). A file whose deleted share (delete file plus inlined deletions) reaches the threshold is replaced by its survivors, row IDs stored (`row_id_start` NULL); the old file and its delete file end, so older snapshots still read them |
| `ExpireSnapshotsAsync(versions)` / `ExpireSnapshotsAsync(olderThan)` | `ducklake_expire_snapshots` | No snapshot. Every catalog row no retained snapshot can see (`begin <= s < end` for none) is removed: files, delete files, dropped tables and their columns, statistics and inlined tables, tags, partition and sort specs, deleted inlined rows. Files among them are scheduled. The latest snapshot is never expired. `dryRun` reports without changing anything |
| `CleanupOldFilesAsync(olderThan?)` | `ducklake_cleanup_old_files` | Deletes scheduled files (relative paths resolve against `data_path`, as DuckDB writes them) and their rows |
| `DeleteOrphanedFilesAsync(olderThan = 3 days)` | `ducklake_delete_orphaned_files` | Deletes `.parquet`/`.puffin` files under `data_path` that no row refers to, older than the age given (at least an hour: younger ones may belong to a commit in flight). Needs an `IFileIO` that lists |

Compactions take part in conflict resolution per data file, as the spec's Compaction rules say: a delete and a merge or
rewrite of the same file conflict (whichever commits second loses), compactions of different files of one table
commit side by side. The guarantees are tested three ways: `MaintenanceTests` (every kept snapshot reads the same,
row IDs included, on both databases and versions), the model-based test (random compactions and expiries between
writes, checked against the model at every snapshot, and against DuckDB), and `CrossEngineTests` (DuckDB reads our
maintenance and cleans up our scheduled files; we read DuckDB's and clean up its).

## Performance

Measured with `tools/DuckLakeBench` (`dotnet run -c Release --project tools/DuckLakeBench [--postgres "<connection
string>"]`) on a 4-core container, local disk, a 5-column table (bigint, varchar, decimal, timestamptz, int):

| Scenario | SQLite catalog | PostgreSQL catalog (local) |
|---|---|---|
| Insert 1,000,000 rows in one commit | 5.9 s (170,000 rows/s) | 5.6 s (179,000 rows/s) |
| Insert 500,000 rows into 30 partitions | 3.1 s | 3.3 s |
| One-row commit | p50 10.5 ms, p95 15.6 ms | p50 16.7 ms, p95 27.9 ms |
| One-row commit, inlined (limit 10) | p50 6.9 ms, p95 11.4 ms | p50 12.8 ms, p95 18.5 ms |
| Scan planning over 1,000 live files | p50 5.1 ms; point lookup (statistics pruning) p50 10.8 ms | p50 6.2 ms; point lookup p50 14.8 ms |
| Full scan of 1,000,000 rows (10 files) | 1.26 s (790,000 rows/s) | 1.74 s |
| Delete 10% of 500,000 rows across 20 files | 0.55 s | 0.70 s |
| 4 writers, 25 one-row commits each, at once | 100 commits in 1.1 s, none lost | 100 commits in 1.1 s, none lost |

Features added later, same container (the PostgreSQL run predates the compiled `ReadAsync<T>` binder):

| Scenario | SQLite catalog | PostgreSQL catalog (local) |
|---|---|---|
| Full scan of 1,000,000 rows as records (`ReadAsync<T>`) | 1.86 s (537,000 rows/s; 765,000 rows/s as physical values) | 3.03 s (2.24 s as physical values) |
| Delete 10% of 500,000 rows across 20 files as deletion vectors (1.1) | 0.65 s (0.56 s as Parquet delete files); reading what is left 0.54 s | 0.75 s (1.01 s as Parquet delete files); reading 1.10 s |
| Register 100 existing files of 1,000,000 rows (`AddDataFilesAsync`) | 0.22 s (footers only); reading them through the name mapping 0.63 s | 0.15 s; reading 1.16 s |
| Insert 500,000 rows of `uint64`, `timestamp_ms`, `timetz`, `interval` | 2.05 s (244,000 rows/s); reading them 0.53 s | 2.37 s; reading 0.57 s |

`ReadAsync<T>` compiles its binder once per read (a constructor and property setters as an expression tree, each
column's conversion chosen once); binding by reflection per row ran at 336,000 rows/s. Name-mapped files are read
by the same reader, matching by name instead of field ID, so they cost what other files do; registering a file
reads only its footer (ranged reads for the last 64 KiB).

What decides these numbers, and what was changed for them:

- **Catalog round trips are constant.** A commit sends the same number of statements to the catalog database (14
  for an insert, 9 for a delete) whether it writes 2 files or 80, of 3 columns or 12; a scan plan sends 4. Per-table
  state is read with one set-based query per catalog table and merged in memory; rows go in as multi-row `INSERT`s
  built from the EF model's mapping (`BulkInsert`; EF Core's SQLite provider would send one statement per row). Before
  this, an 80-file commit took 1,658 statements on SQLite and 1,124 on PostgreSQL, and a delete 247.
  `PerformanceTests` hold the counts, read from the telemetry spans ([telemetry.md](telemetry.md#ducklake)).
- **The snapshot row is written first.** It is the row concurrent committers race for; a loser now learns it lost
  before writing anything else (PostgreSQL makes it wait for the winner, then fail at once), and retries after a
  jittered exponential backoff (`DuckLakeOptions.RetryWait`, `MaxRetryWait`). With it last, four PostgreSQL writers
  collided on `ducklake_data_file`'s key after doing all their work and ran out of retries.
- **Planning reads only the statistics it needs**: those of the columns the filter names, joined to the live files.
- **Writes skip work DuckLake does not use**: Iceberg's per-file metrics pass (`ParquetWriteOptions.Metrics`), a
  re-read of each file's footer (`ParquetFiles.WriteFileAsync` returns the footer size and row-group count), and a
  type switch per value for values already in their physical form (`DuckLakeValues.Normalizer`).
- **Row groups of 122,880 rows** (DuckDB's size, `DuckLakeOptions.Parquet`): Parquet statistics prune row groups
  inside a file, and a scan holds at most a few row groups per file in memory.
- **Scans read ahead** up to `DuckLakeOptions.MaxParallelFileReads` files (default 4), each into a queue of two row
  groups; rows still come out in file order. Locally this changes little (decoding into `object?[]` rows is bound by
  allocation, about one microsecond per row); against object storage it overlaps the request latency.

Remaining costs: decoding allocates a boxed array per row (shared with the Iceberg read path; a columnar or typed
reader is the way past it), and without data inlining each small commit writes a Parquet file.

## Next steps

1. Typed rows through the LINQ layer's mapping, shared with the Iceberg tables.
2. Puffin deletion vectors.
3. Write `INT(8)`/`INT(16)` annotations in Iceberg.Net.Data.
4. An API review, then a named consumer, before graduation.

