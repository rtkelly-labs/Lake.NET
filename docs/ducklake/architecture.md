# DuckLake architecture and incubation

[DuckLake overview and guides](../ducklake.md)

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
| `IFileIO` (local, S3), write-once files | DuckLake's bare and snapshot-keyed Puffin deletion vectors (1.1) |
| The `Corruption` contract, `[Spec]` traceability, the fuzz harness, the oracle-test pattern | |

About 3,400 lines of library code. No existing library changed; the shared pieces that did are listed under
[Tests](testing.md#tests).

## Evaluation of Iceberg.NET as the base

What made this quick, and what got in the way, for whoever takes either format further.

- **The data plane is genuinely format-neutral.** `ParquetFiles.WriteAsync/ReadBatchesAsync` take a `Schema`, write
  field IDs on every node and resolve by field ID on read; DuckDB reads its files, nested types included, with no
  changes. Row-group pruning, ranged reads and missing-column defaults (`initial-default`) came for free.
- **The expression stack composes.** `MetricsEvaluator.MightMatch(recordCount, statsOf)` takes typed statistics from
  any source, so DuckLake's string statistics prune files after one parse step; `RowEvaluator` gives exact filters.
- **The rules transfer.** "Resolve by field ID", "files are never overwritten", "nothing is deleted after an unknown
  commit", "decoders of untrusted input are guarded and fuzzed" and "behaviour other engines observe needs a
  cross-engine test" all apply to DuckLake unchanged (see [the same constraints](testing.md#the-same-constraints)).
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
  DuckDB (see [the differences table](compatibility.md#where-the-reference-implementation-and-the-spec-differ); 1.1 has since written several of those rules down). Releases come about
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
