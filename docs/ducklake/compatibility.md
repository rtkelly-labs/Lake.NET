# DuckLake compatibility

[DuckLake overview and guides](../ducklake.md)

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
| `ducklake_view_column_tag` table | Created; view column tags are read and written |
| Inlined tables' `row_id`/`begin_snapshot`/`end_snapshot` renamed `_ducklake_…` | Read by catalog version |
| `null` column type, promotable later | `DuckLakeType.Null`; columns are left out of data files and read as NULL; `AlterColumnTypeAsync` promotes them |
| Partition transforms `epoch_year`/`epoch_month`/`epoch_day`/`epoch_hour` | Iceberg.NET's time transforms (1.1 catalogs only) |
| Multi-snapshot Puffin delete files | Read; selects the latest cumulative vector not after the scanned snapshot |
| Partial files and the path chain, now in the spec's Queries page | Already implemented from DuckDB's behaviour |

The 1.1 reference is DuckDB 2.0's `ducklake` extension. PyPI has no wheel for it (the newest extension wheel is 1.5.5,
which writes 1.0), so its oracle downloads it from extensions.duckdb.org. The `cross-engine` CI job installs DuckDB 2.0
and requires it (`ICEBERG_DUCKLAKE11_REQUIRED=1`); every 1.1 case passes there. This sandbox cannot reach that host,
so locally the 1.1 oracle cases skip. The 1.0 oracle (DuckDB 1.5.5) runs everywhere.

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
| Inlined data and deletions | Data inlining page and "Type Encoding for Data Inlining" | `ducklake_inlined_data_{table}_{schema_version}` and `ducklake_inlined_delete_{table}` (the latter not listed in any catalog table); nested values as DuckDB's text cast | Read and written with DuckDB's table layout and column types (see [Data inlining](operations.md#data-inlining)) |
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
| Encrypted Parquet | "Parquet encryption" (the Parquet modular encryption spec) | DuckDB 1.5.5 and 2.0 read a parquet-cpp-encrypted file (footer key, AES-GCM, as ParquetSharp writes it) until a filter is applied: any `WHERE` fails with "Computed AES tag differs from read AES tag", in plain `read_parquet` as in DuckLake, so its `DELETE`/`UPDATE` on such files fail too. No writer setting changes it (statistics, dictionaries, compression, page index, CTR mode, plaintext footer all tried) | DuckDB reads, inserts into and writes encrypted lakes created here, and this library reads DuckDB's; DuckDB's filtered reads, deletes and updates of files written here wait on a DuckDB fix. `DuckDB_reads_encrypted_parquet_written_here_but_not_with_a_filter` reproduces it without DuckLake and flips when DuckDB fixes it |
| Deleting files outside `data_path` | Not specified | `ducklake_cleanup_old_files` deletes a scheduled file wherever its path points, including registered files outside `data_path` | Only files under `data_path`; registered files outside it with `DuckLakeOptions.CleanupOutsideDataPath`. A relative path that climbs out (`../`, `%2e%2e/`) is a format error on read and never deleted |
| Schema and table names | Not restricted | Names become directories under `data_path` (percent-encoded) | `.`, `..` and names with `/`, `\` or NUL are refused (they would not be one directory) |
| A NULL `encryption_key` in an encrypted lake | Not specified | — | A format error for Parquet files (never read as plaintext); Puffin deletion vectors, which are not encrypted, may have none |
| Statistics encodings | `boolean` as `0`/`1`, `blob` as hex | Inlined rows' table statistics hold `true`, `nan`, `\x00\xFF` | Write the spec's encodings (no bounds for blobs); parse all of these, and treat anything unparseable as "no bound" |

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
