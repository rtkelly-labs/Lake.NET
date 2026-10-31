# DuckLake

**Status:** reference, checked 2026-10-31, spec DuckLake 1.1 (`duckdb/ducklake-web` `docs/stable`, pinned at
`9d095946` in `docs/spec/ducklake-sections.json`; re-read at `main` `a3fad4a1`, which differs from the pin only
editorially) and DuckLake 1.0 (`docs/1.0`). Lake.NET code is on branch `lake/recover-ducklake` (top of the PR stack
#49–#56), not on `main`; every path below is on that branch unless it says otherwise. Names are today's
(`Iceberg.Net.DuckLake`); the package becomes `Lake.DuckLake` at the rename.

Sources: the spec pages above; the branch's `docs/ducklake.md` and `docs/ducklake/*.md`; the committed spec vectors
`tests/Iceberg.Net.DuckLake.Tests/spec-vectors/schema-1.0.sql` and `schema-1.1.sql` (the spec's "Full Schema Creation
Script", verbatim); the code and tests. Side by side with the other formats: [README.md](README.md).

## 1. What DuckLake is

All metadata lives in ordinary SQL tables in a **catalog database** that has transactions and primary keys (spec
"Building Blocks"); all data lives in **Parquet** files in a data path (local, object or file storage). A commit is
one SQL transaction. There is no metadata-file tree, no manifest, and no catalog service besides the database.

```
 catalog database (SQLite, PostgreSQL or a DuckDB file)       storage under data_path
 +-----------------------------------------------------+      +--------------------------------+
 | ducklake_metadata   version, data_path, encrypted   |      | <schema>/<table>/              |
 | ducklake_snapshot   snapshot_id PK, schema_version, |      |   ducklake-<uuidv7>.parquet    |
 |                     next_catalog_id, next_file_id   |      |   ducklake-<uuidv7>-delete     |
 |  `- ducklake_snapshot_changes  changes_made         |      |     .parquet | .puffin (1.1)   |
 | ducklake_schema / _table / _column / _view ...      |      +---------------^----------------+
 |   versioned: visible at s iff                       |                      |
 |   begin_snapshot <= s AND (end IS NULL OR s < end)  |                      | path, path_is_relative
 | ducklake_data_file   path, record_count, row_id_.. -+----------------------+ field_id = column_id
 |  |- ducklake_file_column_stats  min/max as text     |                      |
 |  |- ducklake_file_partition_value                   |                      |
 |  `- ducklake_delete_file  format parquet|puffin    -+----------------------+
 | ducklake_inlined_data_{table}_{schema_version}      |  small inserts kept as rows
 | ducklake_inlined_delete_{table}                     |  small deletes of file rows
 | ducklake_files_scheduled_for_deletion               |  files awaiting cleanup
 +-----------------------------------------------------+
```

Reads resolve the chain `data_path` → schema `path` → table `path` → file `path`, each segment relative to its parent
when `path_is_relative` is true (spec "Note on Paths").

## 2. Versions 1.0 and 1.1

| | 1.0 | 1.1 |
|---|---|---|
| Released | April 2026 | 21 October 2026 |
| Written by | DuckDB 1.5 (`ducklake` extension; our oracle 1.5.5) | DuckDB 2.0 (extension downloaded unpinned at test time) |
| Catalog tables | 28 | 29 (adds `ducklake_view_column_tag`) |
| `ducklake_metadata.version` | `1.0` | `1.1` |
| Migration | — | DuckDB `ATTACH … (AUTOMATIC_MIGRATION TRUE)`; never done by Lake.NET |

What 1.1 adds (verified by diffing the two spec vectors and the 1.0 and `stable` spec pages):

| 1.1 change | Spec location | Lake.NET |
|---|---|---|
| `ducklake_schema.parent_schema_id`: nested schemas; `changes_made` names whole quoted paths (`"sales"."europe"."orders"`) | `ducklake_schema`, `ducklake_snapshot_changes` | ✅ `CreateSchemaAsync("sales.europe")`; `ChangeSet.QuotePath` |
| `row_group_count` on `ducklake_data_file` and `ducklake_delete_file` | table pages | ✅ written |
| `min_is_exact` / `max_is_exact` on file and table column stats (`NULL` = unknown) | `ducklake_file_column_stats`, `ducklake_table_column_stats` | ✅ `true` where a bound exists, `NULL` otherwise |
| `ducklake_view_column_tag` | new table | ✅ `SetViewColumnTagAsync` |
| Inlined-table system columns renamed `_ducklake_row_id`, `_ducklake_begin_snapshot`, `_ducklake_end_snapshot`; top-level names starting `_ducklake_` reserved | `ducklake_inlined_data_tables` | ✅ read by catalog version; `Column_names_starting_with_ducklake_are_reserved` |
| `null` column type, promotable later | Data Types | ✅ `DuckLakeType.Null`, promoted by `AlterColumnTypeAsync` |
| `epoch_year/month/day/hour` partition transforms | `ducklake_partition_column` | ✅ 1.1 catalogs only |
| Puffin delete files: bare `deletion-vector-v1` blob, or cumulative vectors keyed by `ducklake-snapshot-id` (experimental) | `ducklake_delete_file` | ✅ read and written (§6) |
| `changes_made` kinds `merge_adjacent`, `rewrite_delete`, `inlined_insert`, `inlined_delete`, `inline_flush` (1.0 spec lists 11 kinds; 1.1 lists 20) | `ducklake_snapshot_changes` | ✅ written and parsed |
| Expiry keeps `ducklake_snapshot_changes` rows | `ducklake_snapshot_changes` | ✅ kept (DuckDB 1.5.5 deletes them) |
| Table stats may be narrowed by a commit that removes files; a narrowing commit must advance `next_file_id` | `ducklake_table_column_stats` "Snapshot Validity" | Lake.NET only widens (`Table_statistics_only_widen`); whether its rewrites ever narrow is (unverified) |
| All-null statistics for leaf columns an added file lacks (`value_count` 0, `null_count` = rows) | Not in the spec text; DuckDB 2.0 `ducklake_add_data_files` behaviour | ✅ commit `a506b11`, 1.1 only |
| `skip_stats_columns` table option | `ducklake_metadata` | Not read (unverified that stats honour it) |
| `timestamptz_ns` type (and `epoch_*` for it) | Data Types, `ducklake_partition_column` | ⏳ absent from `DuckLakeTypes` and from the not-implemented list |

`DuckLakeCatalog.FormatVersion10` / `FormatVersion11` choose the version at create; `SupportedVersions` gates open
(`Opening_refuses_other_format_versions_and_takes_the_encryption_mode`). The EF model is cached per (database,
version) (`DuckLakeModelCacheKeyFactory`).

## 3. Catalog schema

Every table in the 1.1 script. "V" = versioned by `begin_snapshot`/`end_snapshot`. Primary keys exist only where
marked (PK); DuckDB creates no others, so Lake.NET's EF model declares composite keys that never reach the database.

| Table | V | Key columns | Purpose |
|---|---|---|---|
| `ducklake_metadata` | | `key`, `value`, `scope` (NULL/`schema`/`table`), `scope_id` | Format version, `data_path` (ends in `/`), `encrypted`, `created_by`; options such as `data_inlining_row_limit`, `write_deletion_vectors`, `target_file_size`, `rewrite_delete_threshold`, `delete_older_than`, `expire_older_than`, `skip_stats_columns`. Not versioned |
| `ducklake_snapshot` | | `snapshot_id` (PK), `snapshot_time`, `schema_version`, `next_catalog_id`, `next_file_id` | One row per commit; the PK is the commit lock |
| `ducklake_snapshot_changes` | | `snapshot_id` (PK), `changes_made`, `author`, `commit_message`, `commit_extra_info` | Change summary used for conflict resolution (§4) |
| `ducklake_schema` | V | `schema_id` (PK), `schema_uuid`, `schema_name`, `path`, `path_is_relative`, `parent_schema_id` (1.1) | Schemas |
| `ducklake_schema_versions` | | `begin_snapshot`, `schema_version`, `table_id` | Which table changed schema at which snapshot |
| `ducklake_table` | V | `table_id`, `table_uuid`, `schema_id`, `table_name`, `path`, `path_is_relative` | Tables (a rename is a new row version with the same ID) |
| `ducklake_column` | V | `column_id`, `table_id`, `column_order`, `column_name`, `column_type`, `initial_default`, `default_value`, `nulls_allowed`, `parent_column`, `default_value_type`, `default_value_dialect` | Column trees; nested fields via `parent_column` |
| `ducklake_view` | V | `view_id`, `view_uuid`, `schema_id`, `view_name`, `dialect`, `sql`, `column_aliases` | Views (stored, not executed) |
| `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters` | V (macro) | `macro_id`, `impl_id`, `dialect`, `sql`, `type`; parameters with types and defaults | Scalar and table macros |
| `ducklake_data_file` | V | `data_file_id` (PK), `table_id`, `path`, `path_is_relative`, `record_count`, `file_size_bytes`, `footer_size`, `row_id_start`, `partition_id`, `encryption_key`, `mapping_id`, `partial_max`, `row_group_count` (1.1), `file_order` | Data files |
| `ducklake_delete_file` | V | `delete_file_id` (PK), `data_file_id`, `path`, `format` (`parquet`/`puffin`), `delete_count`, `footer_size`, `encryption_key`, `partial_max`, `row_group_count` | At most one live per data file |
| `ducklake_file_column_stats` | | `data_file_id`, `column_id`, `value_count`, `null_count`, `min_value`, `max_value`, `contains_nan`, `extra_stats`, `min_is_exact`/`max_is_exact` (1.1) | Per-file stats, text-encoded |
| `ducklake_file_variant_stats` | | `data_file_id`, `column_id`, `variant_path`, `shredded_type`, … | Variant shredding stats (not implemented) |
| `ducklake_table_stats` | | `table_id`, `record_count`, `next_row_id`, `file_size_bytes` | Mutable, latest snapshot only; `next_row_id` allocates row IDs |
| `ducklake_table_column_stats` | | `table_id`, `column_id`, `contains_null`, `contains_nan`, `min_value`, `max_value`, exactness (1.1) | Mutable, latest snapshot only |
| `ducklake_partition_info` | V | `partition_id`, `table_id` | A partition spec |
| `ducklake_partition_column` | | `partition_id`, `partition_key_index`, `column_id`, `transform` | Its keys |
| `ducklake_file_partition_value` | | `data_file_id`, `partition_key_index`, `partition_value` | Transformed value per file, as text |
| `ducklake_sort_info`, `ducklake_sort_expression` | V (info) | `sort_id`, `sort_key_index`, `expression`, `dialect`, `sort_direction`, `null_order` | Sort order |
| `ducklake_tag`, `ducklake_column_tag`, `ducklake_view_column_tag` (1.1) | V | `object_id`/`table_id`+`column_id`/`view_id`+`column_name`, `key`, `value` | Tags; comments are key `comment` |
| `ducklake_column_mapping`, `ducklake_name_mapping` | | `mapping_id`, `type` (`map_by_name`); `column_id`, `source_name`, `target_field_id`, `parent_column`, `is_partition` | Name mapping for added files (§5) |
| `ducklake_inlined_data_tables` | | `table_id`, `table_name`, `schema_version` | Registry of inlined-data tables |
| `ducklake_files_scheduled_for_deletion` | | `data_file_id`, `path`, `path_is_relative`, `schedule_start` | Files no snapshot references, awaiting cleanup |

Not in the script but created at run time: `ducklake_inlined_data_{table_id}_{schema_version}` and
`ducklake_inlined_delete_{table_id}` (§7). There is **no encryption-key table**: keys are the `encryption_key`
columns of the two file tables (§9). The DDL per database is `DuckLakeDialect.Ddl` (`DuckLakeSql.cs`), checked against
the vectors by `SpecSchemaTests.Every_dialect_creates_the_specs_tables_and_columns` and against DuckDB's own
catalogs by `CatalogTests.The_dialect_matches_the_schema_DuckDB_creates`. Visibility is one LINQ filter,
`VersionedQueries.LiveAt` (`DuckLakeStore.cs`).

## 4. Snapshots and commits

| Rule | Spec | Lake.NET |
|---|---|---|
| Snapshot 0 is the initial catalog; IDs increase by one per commit | Conflict Resolution | `A_new_catalog_has_snapshot_0_and_the_main_schema` |
| `schema_version` increments on any schema-affecting change (DDL, comments, tags, sort orders) | Snapshot Creation | `Ddl_takes_catalog_ids_and_bumps_the_schema_version` |
| `next_catalog_id` allocates schema, table, view, macro, partition and sort IDs; `next_file_id` allocates data file, delete file and mapping IDs (and advances on an inlined insert, as DuckDB does) | Snapshot Creation | `DuckLakeValidator` checks both stay above every ID in use |
| One commit = one SQL transaction: snapshot row, changes row, every catalog row | Building Blocks | `DuckLakeTransaction.CommitAsync` |
| Row IDs: `row_id_start` per file from `ducklake_table_stats.next_row_id`; UPDATE keeps row IDs (stored in `_ducklake_internal_row_id`) | `ducklake_data_file`, Row Lineage | `Updated_rows_keep_their_row_ids` |

**Commit protocol in Lake.NET** (`DuckLakeTransaction.CommitAttemptsAsync`): open a DB transaction; write the
`ducklake_snapshot` row **first** (the row racers collide on, so a loser fails before doing other work); write the rest
with set-based multi-row inserts (`BulkInsert`) — constant statement count per commit (14 for an insert, 9 for a
delete, held by `PerformanceTests.Catalog_round_trips_do_not_grow_with_files_or_columns`). On `DbException` /
`DbUpdateException`: roll back; if the attempted snapshot ID now exists, read the `changes_made` of every snapshot
since the base, run `ChangeSet.ConflictWith` and retry after jittered exponential backoff (`DuckLakeOptions.RetryWait`
10 ms × 1.5ⁿ, capped by `MaxRetryWait` 1 s, at most `MaxCommitAttempts` 10); otherwise delete the written files and
rethrow. A failure inside the database `COMMIT` throws `DuckLakeCommitStateUnknownException` and **keeps** the files.
Data files are never rewritten on retry.

**`changes_made`** is a comma-separated list of `kind:target`; targets are IDs or SQL-quoted names
(`"this ""t"""`). Parsed by `DuckLakeChange.Parse` (untrusted text, `Corruption.Guard`), written by `ChangeSet.Entries`.

**Logical conflict rules** (spec "Logical Conflicts", 1.1), against snapshots committed since our base:

| Ours | Conflicts with a concurrent… | Lake.NET (`ChangeSet.ConflictWith`) |
|---|---|---|
| Create schema S | create of S under the same parent | ✅ |
| Drop schema S | drop of S; any create in S or a descendant | ✅ (descendants: unverified) |
| Create table/view N in S | create of N in S; drop of S | ✅ tables and views share names |
| Drop table/view T | drop of T | ✅ |
| Alter or rename T | drop, alter or rename of T | ✅ rename counts as alter |
| Set `NOT NULL` on T | insert into T | — no such API in Lake.NET |
| Create/drop macro | same name in S; drop of S; drop of the same macro | ✅ |
| Insert into T (file or inlined) | drop, alter, or delete from T | ✅ |
| Delete from T | drop, alter, or insert into T; delete from the **same data file** (file or inlined deletes); compaction of that file | ✅ same-file check against the catalog (`Deletes_conflict_only_on_the_same_data_file`) |
| A `DELETE` matching zero rows | still a delete attempt for conflicts (nothing written) | ✅ `A_delete_matching_nothing_still_conflicts_with_a_concurrent_insert` |
| Flush inlined data of T | drop of T; another flush of T; concurrent inlined deletion | ✅ `A_flush_conflicts_with_a_concurrent_inlined_delete_and_another_flush` |
| Compact (merge/rewrite) a file | delete from or compaction of that file; drop of T. Different files of one table commit side by side | ✅ `Compaction_conflicts_with_deletes_from_the_same_files_only` |
| (any) | a kind this library does not know | conflicts only if it names a table we touch (`Unknown_change_kinds_conflict_only_on_tables_we_touch`) |

Spec also retries SQLite `database is locked` during metadata writes; Lake.NET does not (gap D4). DuckDB never commits
*concurrently* with Lake.NET in tests, only in turn (gap D5).

## 5. Data files and Parquet

| Topic | Rule | Lake.NET |
|---|---|---|
| Column identity | Parquet `field_id` = `ducklake_column.column_id`, nested included | `DuckLakeTypes.ToIceberg` builds an Iceberg `Schema`; `ParquetFiles` writes and resolves by field ID |
| Paths | `path_is_relative` → relative to the parent (table → schema → `data_path`); else absolute | `Absolute_and_relative_paths_resolve_through_schema_and_table`; files named with UUIDv7, written with `overwrite: false` |
| Row groups | DuckDB writes 122,880 rows per group | `DuckLakeOptions.Parquet` default matches |
| `footer_size` | Thrift footer length (a read optimisation) | Returned by `ParquetFiles.WriteFileAsync` |
| Partial files | `partial_max` set: rows carry `_ducklake_internal_snapshot_id` (field 2147483539) and `_ducklake_internal_row_id` (2147483540); rows after the scanned snapshot are invisible | `DuckLakeCatalog.SnapshotIdFieldId`, `RowIdFieldId`; read and written |
| `file_order` | "unique within a snapshot"; DuckDB writes NULL | Written as `data_file_id` |
| `value_count` | 1.1 text: non-NULL values (1.0 text said "including NULL"; DuckDB always meant non-NULL) | Writes non-NULL count; an over-count only disables pruning |

**Statistics** are text ("Type Encoding for Statistics"): integers as decimal strings, `boolean` `0`/`1` (inlined
stats may hold `true`), blobs as hex, floats with `inf`/`nan` special values, timestamps as
`YYYY-MM-DD HH:MM:SS[.ffffff]`. Bounds need not be exact but must bound (`min_is_exact`). NaN is never a bound;
unparseable text means "no bound" (`EncodingTests.NaN_is_never_a_bound_and_unknown_text_never_prunes`).
Encoding/decoding: `DuckLakeValues`; collection: `StatsCollector`; pruning goes through Iceberg's `MetricsEvaluator`
then an exact `RowEvaluator` residual (`File_statistics_prune_and_the_residual_filter_is_exact`).

**Partitioning** (`ducklake_partition_column.transform`; `DuckLakePartitioning`):

| Transform | Sources | Value stored |
|---|---|---|
| `identity` | any except `null` | Source value as stats text |
| `bucket(N)` | any except `null` | `(murmur3_32(v) & INT_MAX) % N`, Iceberg's hash (`Transform.Bucket`) |
| `year`, `month`, `day` | date, timestamps | Calendar component (2023; 1–12; 1–31), **not** Iceberg's epoch offsets |
| `hour` | timestamps | 0–23 |
| `epoch_year/month/day/hour` (1.1) | date, timestamps (incl. `timestamptz_ns`) | Units since 1970 (can be negative), UTC instant |

Hive-style directories are an option (`hive_file_pattern`), not semantics; the value comes from
`ducklake_file_partition_value`.

**Added files** (`AddDataFilesAsync`, `DuckLakeAddFileOptions`; DuckDB `ducklake_add_data_files`): only the footer is
read. A `ducklake_column_mapping` of type `map_by_name` (ID from `next_file_id`; one mapping per layout per commit)
plus `ducklake_name_mapping` rows map source names to column IDs; struct fields match by name, list/map children by
position. Missing columns need `AllowMissing` (read as `initial_default`); `HivePartitioning` fills missing columns
from `name=value` path segments. Registered files transfer ownership to the lake: expiry and cleanup may delete them
even outside `data_path`. Read through `DuckLakeNameMap`. Tests: `AddFilesTests` (8 cases),
`Files_added_here_and_by_DuckDB_get_the_same_mapping_and_both_engines_read_both`.

## 6. Deletes

| Kind | Format | Lake.NET |
|---|---|---|
| Position delete file (1.0, 1.1) | Parquet `(file_path, pos)`; `pos` = 0-based row position. DuckDB uses field IDs 2147483646/2147483645, not Iceberg's 2147483546/2147483545 | `DuckLakeDeleteFiles` writes DuckDB's IDs, reads both |
| Replacement | A new delete file holds the previous file's positions plus the new ones; the old row ends and its file is scheduled for deletion | `Deletes_replace_the_files_delete_file_and_remove_emptied_files`; a fully deleted data file is removed instead |
| Partial delete file | `partial_max` set; per-row deletion snapshot in `_ducklake_internal_snapshot_id` | Read; written by flush |
| Deletion vector, bare (1.1) | `format = 'puffin'`, one `deletion-vector-v1` blob (Iceberg v3 layout); its snapshot is `begin_snapshot`; `footer_size` 0, `row_group_count` NULL | Written when `write_deletion_vectors` (any scope) or `DuckLakeOptions.WriteDeletionVectors`; encoded by `Puffin.DeletionVector.Encode` |
| Deletion vector, cumulative (1.1) | Puffin container, one vector per snapshot, property `ducklake-snapshot-id`; reader picks the greatest ≤ the scanned snapshot; `delete_count` = latest vector's count | Read only (`A_Puffin_container_of_cumulative_vectors_reads_the_latest_vector_not_after_the_snapshot`) |
| Inlined deletes | `ducklake_inlined_delete_{table}` (`file_id`, `row_id` = position, `begin_snapshot`) beside the file's delete file; combined on read | ✅ (§7). A replacement delete file never absorbs inlined deletions (merging them made DuckDB double-count) |

Puffin files are never encrypted, even in an encrypted lake (spec). Format 1.0 lakes never write vectors
(`Format_10_lakes_never_write_vectors`). Within one Lake.NET transaction a delete must be the first data operation on
its table.

## 7. Data inlining and flushing

| Aspect | Spec / DuckDB | Lake.NET |
|---|---|---|
| Enablement | DuckDB default limit 10 rows; persisted `data_inlining_row_limit` (table > schema > global) beats the attach option | Off by default (`DuckLakeOptions.DataInliningRowLimit` = 0); a persisted limit wins (`A_limit_persisted_in_the_lake_wins_over_the_option`) |
| Inserts | Into `ducklake_inlined_data_{table_id}_{schema_version}` with system columns (`_ducklake_row_id`, `_ducklake_begin_snapshot`, `_ducklake_end_snapshot` in 1.1; unprefixed in 1.0), registered in `ducklake_inlined_data_tables`; `changes_made` `inlined_insert:{id}` | ✅ new table per schema version; partitioned tables are not inlined into |
| Deletes of inlined rows | Set `_ducklake_end_snapshot` | ✅ |
| Deletes of file rows | ≤ limit go to `ducklake_inlined_delete_{table_id}`; `inlined_delete:{id}` | ✅ |
| Value encoding | "Type Encoding for Data Inlining": SQLite `BIGINT`/`BLOB`/`VARCHAR`; PostgreSQL native types, `BYTEA` for varchar/json/blob, text for dates, timestamps and nested values (DuckDB's text cast) | `DuckLakeInlinedData`, `DuckLakeNestedText`; decimals > 28 digits are not inlined on PostgreSQL |
| Flush (`ducklake_flush_inlined_data`) | One snapshot (`inline_flush:{id}`, plus `deleted_from_table` when delete files are written) | `FlushInlinedDataAsync`: each inlined table → a partial data file in its own schema version's columns; inlined deletions merged with the file's delete file into one partial delete file. Rows inserted while a flush is staged stay inlined |

Tests: `InliningTests` (7), `Each_engine_flushes_what_the_other_inlined`,
`Time_travel_agrees_with_DuckDB_across_its_inlining_flush_and_merge`; fuzz target `ducklake-inlined`.

## 8. Schema evolution, defaults and types

| Operation | Rule | Lake.NET |
|---|---|---|
| Add column | New `column_id` from `next_catalog_id`; `initial_default` fills old files | `AddColumnAsync` |
| Drop / rename column | End the row / new row version, same ID; dropping ends the column's tags | `DropColumnAsync`, `RenameColumnAsync` |
| Widen type | Promotions only, no file rewrite; `null` promotes to anything | `AlterColumnTypeAsync` (`Columns_widen_without_rewriting_files_and_never_narrow`) |
| Defaults | `initial_default` (old files) vs `default_value` (+ `_type`, `_dialect`) for inserts | Preserved across replacements (commit `65805f4`); no API sets `default_value` |
| Partition evolution | New `ducklake_partition_info`; old files keep their `partition_id` | `SetPartitioningAsync` |
| Limits (Lake.NET) | A table created in a transaction cannot be altered in the same transaction | — |

Types are held as the physical value of an Iceberg column type (decision D1 option (c), recorded in
`docs/lake/decisions.md` on branch `rtkelly13/refactor-rename-library`, still open). Types Iceberg lacks live in a side
model (`DuckLakePrimitive`, `DuckLakeTimeTz`, `DuckLakeInterval`):

| DuckLake type | Value in rows | Parquet (as DuckDB writes) | Stats | Limits |
|---|---|---|---|---|
| `int8`, `int16`, `uint8`, `uint16` | `int` | INT32 + INT(8/16, signed/unsigned) | decimal | — |
| `uint32` | `long` | INT32 + INT(32, unsigned) | decimal | not bucketable |
| `uint64` | `IcebergDecimal`, scale 0 | INT64 + INT(64, unsigned) | decimal | not bucketable |
| `timestamp_s`, `timestamp_ms` | `long` µs, truncated to unit | INT64 TIMESTAMP(MICROS/MILLIS) | timestamp text | identity and calendar partitions only |
| `timetz` | `DuckLakeTimeTz` (µs of day, offset s) | INT64 TIME(MICROS, UTC-adjusted): offset lost in files | none | no filter, no partition |
| `interval` | `DuckLakeInterval` (months, days, µs) | FLBA(12) INTERVAL (ms); negatives refused in files, kept inlined | none | no filter, no partition |
| `null` (1.1) | always null | not written | — | — |

Not implemented, failing with `NotSupportedException` when met: `variant`, geometry types, `int128`/`uint128`,
`time_ns`, and (not listed anywhere yet) `timestamptz_ns`. Lists of record: branch `docs/ducklake.md` and
`tools/spec_trace.py` (DuckLake entries in `NOT_IMPLEMENTED`). CLR conversion: `DuckLakeClr`, `ReadClrAsync`,
`ReadAsync<T>`.

## 9. Encryption

| Rule | Spec | Lake.NET |
|---|---|---|
| Mode | `ducklake_metadata.encrypted = 'true'`, fixed at creation; requesting a different mode is rejected | `DuckLakeOptions.Encrypted`; existing mode read at open |
| Cipher | Parquet modular encryption | AES-GCM, encrypted footer (parquet-cpp via ParquetSharp: `ParquetWriteOptions.EncryptionKey`, `DataFileReadOptions.DecryptionKey`) |
| Keys | 256-bit, fresh per write (files of one write may share), stored in the catalog's `encryption_key` column of data and delete files | Base64 as DuckDB stores it |
| Scope | All Parquet data and delete files; Puffin vectors unencrypted | Also maintenance and flush outputs (`Maintenance_and_flushes_write_encrypted_files_too`) |
| Added files | — | Refused (`Existing_files_cannot_be_added_to_an_encrypted_lake`) |

Security consequence: the catalog database holds every key, so catalog read access is data read access. Open
(gap S12): a NULL per-file key in an encrypted lake falls back to a plaintext read; no wrong-key, truncation,
flipped-byte, swapped-file or key-leak tests.

## 10. Maintenance

Maintenance functions are DuckDB extension behaviour, not format; Lake.NET does what DuckDB 1.5.5 does to the catalog.

| Lake.NET (`DuckLakeCatalog`) | DuckDB | Effect | Grace / safety |
|---|---|---|---|
| `MergeAdjacentFilesAsync(…, DuckLakeMergeOptions)` | `ducklake_merge_adjacent_files` | One snapshot `merge_adjacent:{id}`; files of one partition in commit order → one partial file (per-row row ID and snapshot); inputs scheduled | Skips files with deletes and files from before the last schema change |
| `RewriteDataFilesAsync(…, deleteThreshold = 0.95)` | `ducklake_rewrite_data_files` | One snapshot `rewrite_delete:{id}`; survivors rewritten with stored row IDs (`row_id_start` NULL); old file and delete file end | Older snapshots still read the old files |
| `ExpireSnapshotsAsync(versions)` / `(olderThan)` | `ducklake_expire_snapshots` | No snapshot. Removes every row no retained snapshot sees; files among them scheduled | Latest snapshot never expired; `dryRun` |
| `CleanupOldFilesAsync(olderThan?)` | `ducklake_cleanup_old_files` | Deletes scheduled files and rows | `olderThan: null` deletes everything scheduled (gap D7) |
| `DeleteOrphanedFilesAsync(olderThan = 3 days)` | `ducklake_delete_orphaned_files` | Deletes unreferenced `.parquet`/`.puffin` under `data_path` | Refuses ages below `MinOrphanAge` (1 h); needs `ISupportsListing`; compares against local clock (gap D6) |
| `FlushInlinedDataAsync` | `ducklake_flush_inlined_data` | §7 | — |

Tests: `MaintenanceTests` (6), the model-based test (`ModelBasedTests.Lake_always_matches_the_model`, compactions and
expiries between writes), `Maintenance_done_here_reads_the_same_in_DuckDB_which_cleans_up_after_it` and the reverse.

## 11. Writer invariants

| # | Invariant | Enforced in | Proved by |
|---|---|---|---|
| W1 | Snapshot IDs, `schema_version`, `next_catalog_id`, `next_file_id`, `snapshot_time` never go back; counters stay above every ID in use | `DuckLakeTransaction` ID assignment | `DuckLakeValidator`; `ValidatorTests.Counters_that_do_not_cover_the_ids_in_use` |
| W2 | Every versioned row has `begin < end`, neither after the latest snapshot, and is visible in some retained snapshot | commit and expiry | `ValidatorTests.A_row_no_retained_snapshot_can_see` |
| W3 | At most one live delete file per data file per snapshot; a delete file lives only within its data file's life and deletes ≤ `record_count` rows | `DeleteAsync`, flush, rewrite | `Two_delete_files_for_one_data_file_in_a_snapshot`, `A_delete_file_outliving_its_data_file` |
| W4 | Row IDs never overlap within a table; `next_row_id` above the last file's range; UPDATE keeps IDs | `InsertAsync`/`UpdateAsync` | `Overlapping_row_ids`, `Updated_rows_keep_their_row_ids` |
| W5 | Files are written once (`overwrite: false`, UUIDv7 names) and referenced by `field_id` = `column_id` | `DuckLakeTransaction` writes | `DuckDB_reads_every_type_this_library_writes` |
| W6 | Files are deleted only when the commit is known not to have happened; an unknown `COMMIT` outcome keeps them | `CommitAttemptsAsync`, `CleanUpAsync` | `Abandoned_transactions_delete_their_files_and_commit_is_single_use`; unknown-outcome path unreached (D4) |
| W7 | One commit is one SQL transaction; the snapshot row is written first | `CommitAttemptsAsync` | `Concurrent_inserts_both_commit_after_a_metadata_retry`, `PerformanceTests.Concurrent_writers_all_commit` |
| W8 | A retry happens only when no concurrent `changes_made` conflicts; otherwise `DuckLakeConflictException` and written files removed | `ChangeSet.ConflictWith` | `ConflictRuleTests` (10), `An_insert_conflicting_with_a_concurrent_delete_fails_and_cleans_up_its_files` |
| W9 | `changes_made` is written in the spec's format, with quoted names and schema paths | `ChangeSet.Entries` | `Our_changes_are_written_in_the_spec_format` |
| W10 | Table statistics only widen | commit | `Table_statistics_only_widen` |
| W11 | Live files exist in storage with the recorded size and row count | — | `DuckLakeValidator` (`Deep`); `A_missing_file_or_a_wrong_size_or_row_count` |
| W12 | Catalog text (changes, column trees, stats, timestamps, nested inlined values) fails only as a format error, bounded nesting | `Corruption.Guard` call sites | `Untrusted_catalog_rows_fail_as_format_errors`, `Mutated_catalog_text_only_ever_fails_as_a_format_error`; fuzz `ducklake-changes`, `ducklake-inlined` |
| W13 | Unsupported types and files are refused, never read wrong | `DuckLakeTypes` | `Unsupported_types_fail_loudly`, `Files_this_library_cannot_read_correctly_are_refused` |
| W14 | 1.1-only structures never written to a 1.0 lake | version checks (`Is11`) | `A_1_0_catalog_refuses_what_needs_1_1`, `Format_10_lakes_never_write_vectors` |

Every catalog-facing test runs per database and version (`SqliteCatalogTests10`, `PostgresCatalogTests11`, …), and
`TestLake` validates the lake when each test ends. The BDD suite (`Iceberg.Net.DuckLake.Behaviour.Tests`, 12
features) ends every scenario with a deep validation.

## 12. Interop with DuckDB

Where DuckDB and the spec text differ, Lake.NET follows DuckDB and records it (branch
`docs/ducklake/compatibility.md`). Known engine-version differences that touched tests:

| Difference | Seen in | Lake.NET handling | Commit |
|---|---|---|---|
| Lambda syntax changed between 1.5 and 2.0 | test SQL | tests use a form both accept | `a6d24b1` |
| View SQL: 1.5.5 qualifies names with `{DUCKLAKE_CATALOG}`, 2.0 stores them unqualified; a macro body holding the placeholder fails to load | views, macros | stored as given; `DuckLakeView.SqlFor(catalog)` resolves | `d635bc0` |
| DuckDB 1.5.5 and 2.0 read a parquet-cpp-encrypted file until any `WHERE` is applied: "Computed AES tag differs from read AES tag"; so DuckDB's DELETE/UPDATE on encrypted files written here fail | encrypted lakes | pinned as a characterization test that flips when DuckDB fixes it: `DuckDB_reads_encrypted_parquet_written_here_but_not_with_a_filter` | `5618778` |
| DuckDB 1.5.5 deletes `ducklake_snapshot_changes` on expiry; 1.1 spec keeps them | expiry | kept | — |
| Delete files: DuckDB's reserved field IDs, not Iceberg's | §6 | write DuckDB's, read both | — |
| Inlined nested strings: DuckDB writes `'it''s'` and reads back `its` | inlining | write backslash escapes; read `''` as a quote | — |
| `timetz` / `interval` in Parquet lose offset / sub-millisecond precision | types | same as DuckDB; inlined rows keep them | `18adafb` |
| DuckDB 1.5.5 cannot read back dates or `timetz` inside inlined structs | inlining | parsed against the column type; `timetz` written as quoted text | — |
| `int8`/`int16` annotations | Parquet | written as DuckDB does (`ParquetWriteOptions.Annotations`) | `18adafb` |

Oracle: `interop/ducklake_oracle.py` (Python `duckdb` 1.5.5 for 1.0; `duckdb==2.0.0` for 1.1, extension from
`extensions.duckdb.org`), driven by `CrossEngineTests` (25 named cases, each on SQLite and PostgreSQL, 1.0 and 1.1) and
`CrossEngine.feature`. Environment: `ICEBERG_DUCKDB_PYTHON`, `ICEBERG_DUCKLAKE11_PYTHON`, `*_REQUIRED=1` in CI. Plan to
move it in-process via DuckDB.NET: [../duckdb.md](../duckdb.md).

## 13. Implementation map and open gaps

| Spec area | Files / types | Tests | Oracle |
|---|---|---|---|
| Catalog DDL, both databases, both versions | `DuckLakeSql.cs` (`DuckLakeDialect`), `DuckLakeStore.cs` (`DuckLakeStore.Sqlite`/`PostgreSql`, `DuckLakeDbContext`) | `SpecSchemaTests`, `CatalogTests` | `DuckDB_attaches_a_catalog_created_here_and_this_library_opens_one_DuckDB_created` |
| Queries: snapshot-scoped reads, pruning, time travel | `DuckLakeCatalog.cs` (`LoadTableAsync`, `ReadAsync`, `DuckLakeScanPlan`) | `CatalogTests`, `TemporalPropertyTests` | `Time_travel_agrees_with_DuckDB_at_every_snapshot` |
| Writes, commit, conflicts | `DuckLakeTransaction.cs`, `DuckLakeChanges.cs` (`DuckLakeChange`, `ChangeSet`) | `ConflictRuleTests`, `CatalogTests`, `ModelBasedTests` | `Both_engines_write_to_the_same_lake_in_turn`, `A_commit_retries_over_DuckDB_inserts_and_refuses_over_DuckDB_deletes` |
| Types and stats encoding | `DuckLakeTypes.cs`, `DuckLakeValues.cs`, `DuckLakeTemporal.cs`, `DuckLakeClr.cs` | `TypeTests`, `EncodingTests`, `ClrTests` | `DuckDB_reads_every_type_this_library_writes` and the unsigned/timetz/interval pair |
| Deletes, DVs | `DuckLakeDeleteFiles.cs`, Iceberg `Puffin.DeletionVector` | `DeletionVectorTests` | `Each_engine_reads_the_deletion_vectors_the_other_writes` (1.1 only) |
| Inlining | `DuckLakeTransaction.Inlining.cs`, `DuckLakeDeleteFiles.cs` (`DuckLakeInlinedData`), `DuckLakeNestedText.cs` | `InliningTests` | `DuckDB_reads_every_type_this_library_inlines_at_every_snapshot` |
| Partitioning | `DuckLakePartitioning.cs` | `FormatVersionTests.Epoch_and_bucket_transforms_store_icebergs_values` | `Partitioned_writes_read_the_same_in_DuckDB`, `This_library_reads_and_prunes_DuckDB_partitioned_tables` |
| Added files, name mapping | `DuckLakeTransaction.AddFiles.cs`, `DuckLakeNameMap.cs` | `AddFilesTests` | `Files_added_here_and_by_DuckDB_get_the_same_mapping_and_both_engines_read_both` |
| Views, macros, tags, sort | `DuckLakeObjects.cs`, `DuckLakeTransaction.Objects.cs` | `CatalogObjectTests` | `DuckDB_uses_the_comments_views_macros_and_sort_orders_written_here` and reverse |
| Maintenance | `DuckLakeMaintenance.cs`, `DuckLakeTransaction.Maintenance.cs` | `MaintenanceTests` | both maintenance cross-engine cases |
| Encryption | `DuckLakeCatalog.cs` (`Encrypted`), Iceberg.Net.Data encryption options | `EncryptionTests` | `DuckDB_reads_an_encrypted_lake_written_here_and_writes_to_it_what_this_library_reads` |
| Validation | `DuckLakeValidator.cs` | `ValidatorTests` | — |
| Telemetry | `DuckLakeTelemetry.cs` (`DuckLakeCommandCounter`) | `PerformanceTests` | — |

Spec traceability: `[Spec("ducklake: …")]` links to `docs/spec/ducklake-sections.json` (98 sections);
`tools/spec_trace.py matrix --check`, `drift --ducklake`.

**Open gaps** ([gap review](../../verification/gap-review-2026-10.md)):

| Gap | What is missing | Where |
|---|---|---|
| D4 | No fault injection on the catalog database; SQLite BUSY and PostgreSQL `40001` fail instead of retrying; no idempotency token, so a retry after a lost ack can duplicate rows | `DuckLakeTransaction.CommitAttemptsAsync` |
| D5 | DuckDB never stages a commit *concurrently* with ours, so it never parses our `changes_made` under contention | `CrossEngineTests`, `DuckLakeChanges.cs` |
| D6 | Orphan cleanup uses the local clock; no `TimeProvider` | `DeleteOrphanedFilesAsync` |
| D7 | Conflict table covers only ours ∈ {schema, DDL, insert, delete}; retry exhaustion and `CleanupOldFilesAsync(null)` boundary untested | `ConflictRuleTests` |
| ST2 | Never runs on object storage: `TestLake` hard-codes `LocalFileIO` | `Iceberg.Net.DuckLake.TestSupport/TestLake.cs` |
| S10 | (bug?) Catalog paths joined unconfined; cleanup deletes any absolute path a row names; a schema named `..` writes outside `data_path` | `DuckLakeCatalog.cs`, `DuckLakeMaintenance.cs`, `DuckLakeTransaction.cs` |
| S11 | (bug?) Inlined-table names from the catalog dropped without a pattern check; `DuckDbAttach` builds the PostgreSQL string unquoted | `DuckLakeMaintenance.cs`, `DuckLakeStore.cs` |
| S12 | Encryption only happy-path tested; NULL key falls back to plaintext | `DuckLakeCatalog.cs`, `EncryptionTests` |
| §5 | DuckDB→us untested for rename/drop/widen, nested changes, SET DEFAULT, epoch/bucket partitions, partition evolution, the 1.0→1.1 migration; timestamp time travel; no committed DuckDB fixtures (G3) | `CrossEngineTests` |
| Spec 1.1 | `timestamptz_ns`; `skip_stats_columns`; table-stats narrowing rule; SQLite lock retry; `NOT NULL` conflict rule (no API) | this page, §2 and §4 |
