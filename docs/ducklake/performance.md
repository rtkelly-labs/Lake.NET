# DuckLake performance

[DuckLake overview and guides](../ducklake.md)

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

Features added later, same container:

| Scenario | SQLite catalog | PostgreSQL catalog (local) |
|---|---|---|
| Full scan of 1,000,000 rows as records (`ReadAsync<T>`) | 1.86 s (537,000 rows/s; 765,000 rows/s as physical values) | 2.01 s (497,000 rows/s; 1.36 s as physical values) |
| Encrypted lake: insert 1,000,000 rows in 10 files, full scan, delete 3.3% | 3.61 s; scan 1.20 s (834,000 rows/s); delete 1.28 s | 3.65 s; scan 1.22 s; delete 1.32 s |
| Delete 10% of 500,000 rows across 20 files as deletion vectors (1.1) | 0.65 s (0.56 s as Parquet delete files); reading what is left 0.54 s | 0.75 s (1.01 s as Parquet delete files); reading 1.10 s |
| Register 100 existing files of 1,000,000 rows (`AddDataFilesAsync`) | 0.22 s (footers only); reading them through the name mapping 0.63 s | 0.15 s; reading 1.16 s |
| Insert 500,000 rows of `uint64`, `timestamp_ms`, `timetz`, `interval` | 2.05 s (244,000 rows/s); reading them 0.53 s | 2.37 s; reading 0.57 s |

Encryption costs nothing measurable: AES-GCM runs in hardware (the encrypted lake inserts 10 files at the same rate
an unencrypted one does, and scans as fast). `ReadAsync<T>` compiles its binder once per read (a constructor and property setters as an expression tree, each
column's conversion chosen once); binding by reflection per row ran at 336,000 rows/s. Name-mapped files are read
by the same reader, matching by name instead of field ID, so they cost what other files do; registering a file
reads only its footer (ranged reads for the last 64 KiB).

What decides these numbers, and what was changed for them:

- **Catalog round trips are constant.** A commit sends the same number of statements to the catalog database (14
  for an insert, 9 for a delete) whether it writes 2 files or 80, of 3 columns or 12; a scan plan sends 4. Per-table
  state is read with one set-based query per catalog table and merged in memory; rows go in as multi-row `INSERT`s
  built from the EF model's mapping (`BulkInsert`; EF Core's SQLite provider would send one statement per row). Before
  this, an 80-file commit took 1,658 statements on SQLite and 1,124 on PostgreSQL, and a delete 247.
  `PerformanceTests` hold the counts, read from the telemetry spans ([telemetry.md](../telemetry.md#ducklake)).
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
