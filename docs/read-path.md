# Read path

How a read turns a `ScanRequest` into rows. Every reader goes through it: `TableScan.ReadRowsAsync`,
`ReadBatchesAsync`, `CountAsync`, the older `ReadAsync`/`ReadAllAsync` list APIs and `TableArrow.ReadBatchesAsync`.
That way pruning, projection and streaming work the same way for all of them.

```
ScanRequest ─► resolve snapshot ─► bind filter to the snapshot's schema
           ─► plan: manifests (partition summaries) ─► files (partition tuple, column metrics)
           ─► per file: row groups (footer statistics) ─► decode projected columns of one group
           ─► apply deletes (absolute positions, equality keys) ─► residual filter ─► project ─► yield batch
```

## Pruning levels

Each level is inclusive: it skips data only when it can prove no row there matches. Rows that survive are always
checked against the full filter afterwards, so pruning never changes the result.

| Level | Input | Evaluator | Counter (`ScanMetrics`) |
|---|---|---|---|
| Manifest | `partitions` field summaries in the manifest list | `ManifestEvaluator` over the inclusive partition projection | `ManifestsSkipped` |
| File (partition) | a data file's partition tuple | projected filter, compiled with `RowEvaluator` | `FilesSkippedByPartition` |
| File (metrics) | value/null/NaN counts, lower/upper bounds in the manifest | `MetricsEvaluator.MightMatch(DataFile)` | `FilesSkippedByMetrics` |
| Row group (Parquet) | footer column-chunk statistics | `MetricsEvaluator.MightMatch(rows, metricsOf)` (the same evaluator) | `RowGroupsSkipped` |

Row-group statistics are read by field ID (Parquet `field_id`, or names through `schema.name-mapping.default` for files
written without IDs). Only leaves outside lists and maps are used. Values are converted exactly as the reader converts
them: int → long, float → double, unscaled decimals. Statistics are optional and writers disagree about them, so
anything doubtful counts as unknown and is never used to prune:

- no statistics, or no min/max: the group is read (tested with a file written with statistics disabled);
- a column order other than the one Iceberg compares with (signed numerics, unsigned bytes): no bounds;
- a null count of 0 is trusted only when min/max are also present, because some writers omit `null_count` and it then
  reads as 0. A positive count is always trusted, so a group that is all null is still pruned for comparisons;
- string bounds that are not valid UTF-8 are dropped (a truncated max can split a character). A truncated min is still
  a prefix and a truncated max is rounded up by the writer, so both remain valid bounds;
- uuid, decimals stored as fixed or binary, and INT96 get no bounds, because writers order them differently. NaN
  counts are not in Parquet statistics, and NaN bounds are ignored.

Avro files have no per-block statistics, so every Avro block is read.

## Column projection

The scan decodes the union of three sets of top-level columns, resolved by field ID:

1. the requested columns;
2. the columns the filter references (for a predicate on `a.b.c`, the whole top-level struct `a`);
3. the equality-delete columns of the snapshot.

An equality-delete column the read schema lacks is still decoded, because the delete still applies. This happens when the
column was dropped after the delete, or when a time-travel snapshot's schema predates it. `DeleteIndex.WithEqualityColumns`
appends it from the newest table schema that has it. Delete keys and data values are both decoded by field ID with that
one type, so a rename or an `int` → `long` promotion between the delete and the read changes nothing. The same applies to
UPDATE/MERGE/DELETE, compaction, the change feed, lineage reads and fingerprints. The appended columns are dropped before
rows are returned. `ReadPathTests.Equality_deletes_apply_across_a_rename_and_a_type_promotion` and
`…_still_apply_after_their_column_is_dropped` cover this; before the fix, reads after the drop threw.

The filter is rebound by field ID to positions in that read schema. Columns the request does not ask for are dropped
before rows are returned.

- **Parquet:** only the leaf column chunks of the read schema are decoded. Struct, list and map columns are assembled
  from their own leaves.
- **Avro:** top-level fields outside the read schema are skipped by the decoder without being materialised. Avro is
  row-oriented, so their bytes are still scanned.
- **Missing columns:** a column the file does not contain reads as its `initial-default`, or null.
- **Counts:** `CountAsync` passes an empty projection. Counting a table that has deletion vectors decodes no column
  data at all.

## Streaming

Files are read one row group (or one Avro container block) at a time. `DataFiles.ReadBatchesAsync` yields a
`DataFileBatch(FirstPosition, Rows)`. Positions are absolute file positions even when earlier groups were skipped,
which keeps position deletes and deletion vectors correct (tested with a DV in a later row group).

`ReadRowsAsync` and `TableArrow.ReadBatchesAsync` yield after each group. Decoded rows are therefore bounded by one row
group, which `ScanMetrics.MaxBatchRows` reports and the tests assert while the scan is running.

`ReadAsync`, `ReadAllAsync` and `DataFiles.ReadAsync` keep returning lists. They collect the streaming path.

## Ranged reads

A Parquet data file is read through `RangeReadStream` (in `Iceberg.Net.IO`), a seekable stream over
`IFileIO.ReadRangeAsync` that fetches only the byte ranges the reader touches and keeps them:

1. the last 64 KiB (parquet-cpp's footer read size), fetched before the footer is parsed;
2. for each row group that survives row-group pruning, the column chunks of the projected leaf columns. parquet-cpp
   reads a chunk with one call, so each chunk is one request.

The file size comes from the manifest's `file_size_in_bytes` (`DataFileReadOptions.FileSize`), so no `HEAD` is needed;
a read by location alone asks `IFileIO.LengthAsync`. Two cases fetch the file in one request instead: a file of at
most 1 MiB, and a read that decodes every leaf column with no row-group filter (the rest after the footer, two
requests in all). ParquetSharp 25 does not expose `data_page_offset` or `dictionary_page_offset`, so chunk ranges
cannot be planned ahead: a chunk not yet fetched is fetched when the reader asks for it, synchronously on the thread
pool. `ReadPathTests.A_projected_read_downloads_only_the_footer_and_the_projected_column_chunks` checks the bytes with
a counting `IFileIO`: one column of five reads under 30% of a 4 MB file, and pruning to one row group of four halves
that again. Avro files are still fetched whole.

## Time travel semantics

- **Which snapshot:** `SnapshotSelector` picks it: `Current`, `Id`, `Ref` (a branch or tag), or `AsOf`. `AsOf` uses
  the snapshot log, so a rolled-back snapshot is not chosen for an instant after the rollback.
- **Which schema:** `ScanRequest` reads use `TimeTravel.SchemaFor`, the schema the snapshot was written with, as Spark
  does. The filter and the column names bind to that schema. A column renamed later is filtered by its old name, a
  column dropped later can still be selected, and today's names fail at the old snapshot. Planning (`PlanAsync(table,
  request)`) also reads partition specs and manifests with that schema.
- **Spark agreement:** `SparkCrossEngineTests.Spark_agrees_on_time_travel_and_filtered_reads` checks that Spark returns
  the same column names and rows. It uses `versionAsOf`/`timestampAsOf`, the DataFrame form of
  `VERSION AS OF`/`TIMESTAMP AS OF`, across a rename, a drop, an int → long widening and an added column.
- **The older list API stays on the current schema** (`ReadAsync(table, filter, snapshotId)`, `ReadAllAsync(table,
  snapshotId)`, `TableArrow.ReadBatchesAsync(table, filter, snapshotId)`):
  - their rows stay aligned with `Table.Schema` for callers that index by position, for example statistics and
    mutations;
  - their filter binds to the current schema;
  - a column dropped since the snapshot is absent, and one added since reads as its default.

  This is deliberate: use a `ScanRequest` when you want Spark's semantics.

## Counting

`CountAsync(table, request)` sums manifest record counts, without opening a data file, when both of these hold:

- the bound filter is `true`;
- the snapshot has no live delete files.

Otherwise it runs the scan with an empty projection. In that case only the filter and equality columns are decoded, and
row-group pruning still applies.
