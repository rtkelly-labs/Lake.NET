# DuckLake operations

[DuckLake overview and guides](../ducklake.md)

## Adding existing files

```csharp
await using var tx = lake.BeginTransaction();
await tx.AddDataFilesAsync("main", "events", ["s3://bucket/exports/region=eu/part-0.parquet"],
    new DuckLakeAddFileOptions { HivePartitioning = true, AllowMissing = true });
await tx.CommitAsync();
```

Registering a file transfers ownership to the lake: rewriting, snapshot expiry and cleanup can delete it, even
when its absolute path is outside `data_path`, as [DuckDB documents](https://ducklake.select/docs/stable/duckdb/metadata/adding_files).

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
faster (see [Performance](performance.md#performance)); against object storage, where each file is a request, the gain is larger.

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
