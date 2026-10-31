# Delta Lake

**Status:** reference, checked 2026-10-31, spec Delta Transaction Log Protocol (`delta-io/delta` `PROTOCOL.md` at
`master` = `719768686369f1d52e6891aec1f7fa1be903975c`, read 2026-10-31; not yet a tagged pin). Lake.NET has **no
Delta code**. What exists: the feature declaration [`docs/spec/delta-support.json`](../../../spec/delta-support.json),
checked by `SupportDeclarationTests` (`tests/Iceberg.Net.Spec.Tests`), and the plan in
[gap review §8](../../verification/gap-review-2026-10.md) and [goal.md](../../goal.md) (W8). The package will be
`Lake.Delta`, built on the shared `Lake.Core`/`Lake.Data` layers, never on Iceberg.

Every protocol statement below is from `PROTOCOL.md` unless marked **(impl)**: behaviour of a reference
implementation (delta-spark, delta-rs, Delta Kernel) that the protocol does not mandate, or **(unverified)**.
Side by side with the other formats: [README.md](README.md).

## 1. What Delta is

A Delta table is a directory. Its state is an append-only log in `_delta_log/`: one newline-delimited JSON file of
**actions** per version (`00000000000000000042.json`, 20-digit zero padding), plus Parquet **checkpoints** that
summarize the log up to a version. Data is Parquet in the table root or any non-hidden subdirectory (not starting
with `_`). No catalog is required: the table is "self describing" and can be copied with filesystem tools.

```
 mytable/
 |- _delta_log/
 |   |- 00000000000000000042.checkpoint.parquet   full reconciled state at v42 (or V2: .checkpoint.<uuid>.json|parquet)
 |   |- 00000000000000000042.json                 kept: checkpoints drop commitInfo
 |   |- 00000000000000000043.json   --+
 |   |- 00000000000000000044.json     |  replay forward from the checkpoint
 |   |- 00000000000000000045.json   --+  (each = one atomic version; created put-if-absent)
 |   |- 00000000000000000043.00000000000000000045.compacted.json   optional log compaction
 |   |- 00000000000000000045.crc                  optional version checksum
 |   |- _last_checkpoint                          pointer {"version":42,"size":…}
 |   |- _sidecars/<uuid>.parquet                  V2 checkpoint file actions
 |   `- _staged_commits/<v>.<uuid>.json           catalog-managed tables only
 |- _change_data/cdc-….parquet                    change data feed files
 |- date=2017-12-10/part-00000-<uuid>.c000.snappy.parquet   data (layout is convention only)
 `- deletion_vector_<uuid>.bin  (or <prefix>/deletion_vector_<uuid>.bin)
```

**Commit = create the next version file if absent.** "Writers MUST never overwrite an existing log entry. Whenever
possible they should use atomic primitives of the underlying filesystem" (Additional Requirements for Writers). A
writer that loses the race for version `v` re-reads, checks for conflicts, and tries `v+1`. Checkpoints for `v` may
only be written after `v.json` exists. **Catalog-managed tables** (`catalogManaged` feature) move ratification into a
catalog (staged or inline commits, publication later); filesystem-only access to such tables is not supported.

## 2. Actions

At most one `metaData`, one `protocol`, and one `txn` per `appId` per commit; at most one `add` or `remove` per
`(path, dvId)` (Delta Log Entries). Unknown actions and fields must be ignored, not rejected.

| Action | Fields that matter | Rules |
|---|---|---|
| `metaData` | `id` (GUID), `schemaString` (Spark JSON schema), `partitionColumns`, `configuration` (table properties), `format.provider` = `parquet`, `createdTime` | Required in version 0; a later one replaces the whole metadata |
| `protocol` | `minReaderVersion`, `minWriterVersion`, `readerFeatures` (only when reader = 3), `writerFeatures` (only when writer = 7) | §3 |
| `add` | `path` (RFC 2396 URI, relative or absolute), `partitionValues` (map of strings), `size`, `modificationTime`, `dataChange`, `stats` (JSON string), `tags`, `deletionVector`, `baseRowId`, `defaultRowCommitVersion`, `clusteringProvider` | Logical file key = `(path, deletionVector.uniqueId)`; a re-add of the same key replaces its stats |
| `remove` | `path`, `deletionTimestamp`, `dataChange`, `extendedFileMetadata`, `partitionValues`, `size`, `deletionVector`, `baseRowId`, `defaultRowCommitVersion` | Stays as a tombstone until expiry; checkpoints keep tombstones but drop their `stats`/`tags` |
| `txn` (SetTransaction) | `appId`, `version`, `lastUpdated` | Latest `version` per `appId` wins; not assumed monotonic |
| `commitInfo` | free-form JSON; `inCommitTimestamp` (ICT); `txnId` (required for catalog-managed) | Must be **first** in the commit when ICT is enabled; never in checkpoints |
| `cdc` | `path` (under `_change_data`), `partitionValues`, `size`, `dataChange` = false | If a version has `cdc` actions, CDF readers use only them for that version |
| `domainMetadata` | `domain`, `configuration` (string), `removed` | `delta.`-prefixed domains are system-controlled; two concurrent commits touching one domain conflict |
| `sidecar` | `path` (file name in `_delta_log/_sidecars`), `sizeInBytes`, `modificationTime` | V2 checkpoints only |
| `checkpointMetadata` | `version`, `tags` (`sidecarNumActions`, `numOfAddFiles`, `sidecarFileSchema`, …) | Exactly one per V2 checkpoint; never in commits |

`add` with `dataChange=false` must refer to data already present or removed in the same version (rearrangement,
e.g. OPTIMIZE). A commit may `remove (p, dv1)` and `add (p, dv2)` in one version; adding `(p, dv2)` while `(p, dv1)`
is live without removing it is illegal.

## 3. Protocol versions and table features

| Reader version | Requires | Writer version | Requires |
|---|---|---|---|
| 1 | base | 2 | `appendOnly`, `invariants` |
| 2 | column mapping | 3 | `delta.checkpoint.writeStatsAsJson/AsStruct`, CHECK constraints |
| 3 | `readerFeatures` list (writer must be 7) | 4 | change data feed, generated columns |
| | | 5 | column mapping |
| | | 6 | identity columns |
| | | 7 | `writerFeatures` list |

Rules: readers must implement every feature in `readerFeatures`; writers every feature in `writerFeatures` **and**
every reader feature. A reader-writer feature appears in both lists; listing a feature only in `readerFeatures` is
illegal. Features are never removed by an ordinary writer. *Supported* (listed) ≠ *active* (listed and its property
set, e.g. `delta.appendOnly=true`). Upgrading a legacy table to (3, 7) must assume every feature its old versions
allowed was used, unless it can prove otherwise.

Reconciliation of [`delta-support.json`](../../../spec/delta-support.json) (21 entries) with PROTOCOL.md at
`7197686` (its "Valid Feature Names" appendix lists 20 names; three more are defined only in their sections):

| Feature | PROTOCOL.md | `delta-support.json` | Match |
|---|---|---|---|
| `appendOnly`, `invariants`, `checkConstraints`, `generatedColumns`, `allowColumnDefaults`, `changeDataFeed`, `identityColumns`, `rowTracking`, `domainMetadata`, `clustering`, `inCommitTimestamp` | writer | writer, planned | ✅ |
| `columnMapping`, `deletionVectors`, `timestampNtz`, `v2Checkpoint`, `vacuumProtocolCheck` | reader-writer | reader-writer, planned | ✅ |
| `icebergCompatV1`, `icebergCompatV2` | writer | writer, refused | ✅ |
| `typeWidening`, `variantType` | reader-writer (sections only; missing from the appendix table) | reader-writer, planned | ✅ names; PROTOCOL.md is internally inconsistent |
| `checkpointProtection` | **absent** from PROTOCOL.md | writer, planned | **No:** declared but not in the protocol (impl: delta-spark, unverified) |
| `catalogManaged` | reader-writer | **missing** | **No:** must at least be refused |
| `materializePartitionColumns` | writer | **missing** | **No** |
| `variantShredding` | reader-writer (section only), depends on `variantType` | **missing** | **No** |

Writer dependencies the declaration does not state: `rowTracking` and `clustering` require `domainMetadata`;
`variantShredding` requires `variantType`. `SupportDeclarationTests` checks only that each entry has a status, kind
and obligation, and that nine named writer features are present; it does not compare against the protocol.

## 4. Log replay, checkpoints and time travel

**Snapshot at version `v`** = one `protocol`, one `metaData`, `txn` per `appId`, live `domainMetadata` per domain,
live `add` per logical file, unexpired `remove` tombstones, and the `commitInfo` of `v.json` only.

| Reconciliation rule (Action Reconciliation) |
|---|
| Latest `protocol` wins; latest `metaData` wins |
| `txn`: latest `version` per `appId` |
| `domainMetadata`: latest per `domain`; `removed=true` is a tombstone and is not returned |
| File actions: newest reference per `(path, deletionVector.uniqueId)`; `add` = live, `remove` = tombstone; the two key sets are disjoint |
| `commitInfo`: only from the snapshot version's JSON (checkpoints and compactions drop it) |
| `sidecar`, `checkpointMetadata` never appear in commits and do not take part in replay |

**Building a snapshot:** read `_last_checkpoint` (optional hint: `version`, `size`, `parts`, `sizeInBytes`,
`numOfAddFiles`, `checkpointSchema`, `tags`, MD5 `checksum` over canonicalized JSON; may embed a V2 checkpoint), list
`_delta_log` from that version, take the newest **complete** checkpoint ≤ `v`, apply each `N.json` up to `v`.
Readers must not assume checkpoints exist or are frequent. A log compaction `x.y.compacted.json` may replace commits
`x..y` (optional for readers and writers).

| Checkpoint kind | Name | Spec | Rules |
|---|---|---|---|
| Classic | `n.checkpoint.parquet` | V1 or V2 | Always allowed (V1) |
| Multi-part (deprecated) | `n.checkpoint.o.p.parquet` (`o`,`p` 10-digit) | V1 only | Readers ignore one with a missing part; forbidden with `v2Checkpoint` |
| UUID-named | `n.checkpoint.<uuid>.json` or `.parquet` | V2 | Needs `v2Checkpoint`; one `checkpointMetadata`; file actions either all inline or all in sidecars |

Checkpoint content: protocol, metadata, live adds, unexpired removes, txns, live domain metadata (and V2 actions);
never `commitInfo` or `cdc`. Each action is a struct column; missing columns read as null. `add.stats` is the JSON
string unless `delta.checkpoint.writeStatsAsJson=false`; `stats_parsed` and `partitionValues_parsed` are written
when `delta.checkpoint.writeStatsAsStruct=true`. Two checkpoints of one version may coexist; either may be used.

**Version checksum** `n.crc` (optional, written after `n.json`, never overwritten): `tableSizeBytes`, `numFiles`,
`numMetadata`=1, `numProtocol`=1, `metadata`, `protocol`, optional `setTransactions`, `domainMetadata`,
`allFiles`, histograms, `inCommitTimestampOpt`. Readers may validate; must work without it.

**Time travel.** By version: replay to `v` with a checkpoint not newer than `v`. By timestamp: the protocol defines
only the ICT rules — with `inCommitTimestamp` enabled, a commit's time is `commitInfo.inCommitTimestamp`; before
`delta.inCommitTimestampEnablementVersion`, the log file's modification time; a query time
≥ `delta.inCommitTimestampEnablementTimestamp` considers only versions ≥ the enablement version. Choosing "the
latest version whose time ≤ t" and correcting non-monotonic file times are **(impl)**. Time travel is bounded by log
and data retention (§9).

## 5. Column mapping, partition values and statistics

**Column mapping** (`delta.columnMapping.mode` = `none` | `name` | `id`; honoured only at reader 2 / writer 5–6, or
with the `columnMapping` feature): every field, nested included, gets `delta.columnMapping.id` (32-bit) and
`delta.columnMapping.physicalName` in its schema metadata. `delta.columnMapping.maxColumnId` only increases.

| Mode | Reader resolves data columns by | Stats, partition values, clustering names keyed by | Missing in file |
|---|---|---|---|
| `none` | display name | display name | null |
| `name` | physical name | physical name | null |
| `id` | Parquet `field_id` = `delta.columnMapping.id` | physical name | null; a file with **no** field IDs: refuse or nulls |

Writers under column mapping write physical names **and** the ID as Parquet `field_id`, so a Lake.Data writer that
already writes field IDs (Iceberg, DuckLake) serves `id` mode directly; `name` mode adds name-based resolution (as
DuckLake's `map_by_name`). Field paths through arrays and maps use `element`, `key`, `value`.

**Partition values** are strings in `partitionValues`, keyed by (physical) column name; `""` means null:

| Type | Encoding |
|---|---|
| string | as is |
| numeric | the number's string form |
| date | `{year}-{month}-{day}` |
| timestamp | `YYYY-MM-DD HH:MM:SS[.ffffff]` in the writer's time zone, **or** ISO 8601 UTC (`1970-01-01T00:00:00.123456Z`), the recommended form |
| timestamp without time zone | `YYYY-MM-DD HH:MM:SS[.ffffff]` (needs `timestampNtz`) |
| boolean | `true` / `false` |
| binary | escaped string, e.g. `"\u0001\u0002"` |

Partition values come from the log, not the path; directory naming is convention. Writing partition columns into
data files is optional, required under `materializePartitionColumns` or IcebergCompat, and steered by
`delta.writePartitionColumnsToParquet` otherwise (decision table in the protocol); readers always take the log's value.

**Per-file statistics** (`add.stats`, a JSON string; optional):

| Field | Meaning |
|---|---|
| `numRecords` | Physical rows in the file. **Required and exact when the file has a DV** (rows in the file, not valid rows) |
| `tightBounds` | `true` (default when absent): bounds are exact over valid rows. `false`: wide bounds, allowed after DV deletes |
| `minValues`, `maxValues` | Nested like the schema; strings truncated to a prefix, timestamps truncated to milliseconds; with `tightBounds=false` only bounds |
| `nullCount` | Exact when tight; when wide, only `0` and `= numRecords` carry information |

Variant columns: min/max are Z85-encoded binary variants keyed by normalized JSON path (variant section).

## 6. Deletion vectors

Feature `deletionVectors` (reader 3, writer 7); writers create new DVs only when `delta.enableDeletionVectors=true`,
readers apply them always. A logical file = data file + optional DV.

| Descriptor field | Meaning |
|---|---|
| `storageType` | `u`: relative path from a UUID; `i`: inline; `p`: absolute path |
| `pathOrInlineDv` | `u`: optional random prefix + 20-char Z85 UUID → `<table>/<prefix>/deletion_vector_<uuid>.bin`; `i`: Z85 of the serialized bitmap; `p`: absolute URI |
| `offset` | Byte offset of this DV in its file (absent for `i`; absent means 0) |
| `sizeInBytes` | Serialized DV size (before Z85 when inline) |
| `cardinality` | Rows deleted |
| `uniqueId` (derived) | `<storageType><pathOrInlineDv>[@<offset>]`: part of the logical-file key |

**Bitmap bytes:** little-endian `magicNumber` = 1681511377 (= `0x6439D3D1`, bytes `D1 D3 39 64`), then a 64-bit
RoaringBitmap in the portable format (8-byte bucket count, then per bucket a 4-byte key and a 32-bit Roaring bitmap).
Every key's most significant bit must be 0.

**DV file** (big-endian): byte 0 `version` = 1; then per DV: `dataSize` (4 bytes), `bitmapData` (the bytes above),
CRC-32 of `bitmapData` (4 bytes).

**Relation to Iceberg.** The protocol does not mention Iceberg. Iceberg v3's `deletion-vector-v1` blob is
`length (4, BE, = 4 + |vector|) | D1 D3 39 64 | portable Roaring64 | CRC-32 (4, BE, over magic + vector)`
(`Iceberg.Net.Puffin` `DeletionVector.Encode`/`Decode`, `RoaringPositionBitmap`, which already refuses keys above
`int.MaxValue`). Read side by side, one Delta file record (`dataSize | bitmapData | checksum`) has the same bytes as
one Iceberg blob, and an inline DV would be the blob minus its length and CRC. **This is a claim to be verified by a
test**: decode Delta's own DV fixtures (DAT, delta-rs test data) with the Iceberg codec, and have delta-rs read DVs we
write. Also (unverified): whether `offset` points at `dataSize` or at `bitmapData`, and whether an inline DV carries
the magic number.

## 7. Concurrency

| Concern | Protocol mandates | Implementation behaviour **(impl)** |
|---|---|---|
| Atomicity | Never overwrite a log entry; use atomic primitives; one serial history of contiguous versions | Put-if-absent (S3 `If-None-Match: *`, local `O_EXCL`/link, Azure/GCS conditional create); a lock-based log store (e.g. DynamoDB) where the store lacks it |
| Optimistic concurrency | "Serializable ACID writes" (Overview); MVCC; writers write files first, then commit | Loser of version `v` reads `v..latest`, checks conflicts, retries at the new version **without rewriting files** when none |
| Isolation levels | **None defined.** `WriteSerializable` / `Serializable` are not in PROTOCOL.md | delta-spark `delta.isolationLevel`, default `WriteSerializable`: blind appends never conflict; `Serializable` also orders appends against reads (unverified detail) |
| Conflict detection | Same `domainMetadata` domain in two overlapping commits conflicts. Under row tracking, a retried commit must reassign `baseRowId` (so commit files can't be reused). ICT must stay monotonic across a retry | delta-spark `ConflictChecker` (unverified name): metadata/protocol change vs anything; concurrent `remove` of a file we read or remove; concurrent `add` matching our read predicate (by partition / file); `txn` of the same `appId` |
| Idempotency | `txn(appId, version)` recorded atomically with the data; latest version per app in the snapshot | Writer checks `appId`'s version ≥ its batch before writing; this is the exactly-once answer to a lost commit ack (gap D4 pattern) |
| Catalog-managed | Catalog ratifies `v` at most once and only after `v-1`; may report the latest ratified commit on conflict | — |

**Lake.NET hazard for the commit path.** `S3FileIO.WriteAsync(…, overwrite: false)` (`src/Iceberg.Net.IO/FileIO.cs`)
sends `If-None-Match: *` but treats a 412/409 as **success** when the existing object has identical bytes (it assumes
"locations are unique per write"). A Delta commit file's location is *not* unique per writer: two writers that
produce byte-identical `N.json` would both believe they committed.

**Decision (2026-10-31):** Lake.Delta keeps the shared create and makes every commit file unique instead. Each commit
attempt writes `commitInfo.txnId` = a fresh UUID (delta-spark and delta-rs write a `txnId` too), so two writers never
produce the same bytes. Byte equality then proves the object is this attempt's own, which is exactly the case the
recovery exists for: a retried `PUT` whose first attempt landed. This keeps one create path for every format and
needs no new FileIO API. Writer invariant 11 below records it, and the first Lake.Delta commit test must race two
writers whose actions are otherwise identical. An outcome that stays unknown after retries (no response, object
absent on read) is surfaced as `CommitStateUnknownException`, as for Iceberg.

## 8. Writer obligations for row-level features

| Feature | Active when | Writer must | Else |
|---|---|---|---|
| `appendOnly` | `delta.appendOnly=true` | Never remove or change data; `dataChange=false` rearrangement allowed | Refuse |
| `invariants` | column metadata `delta.invariants` = `{"expression":{"expression":"<SQL>"}}` | Abort if any new row evaluates false or null | Refuse (needs a SQL evaluator) |
| `checkConstraints` | `configuration` `delta.constraints.<name>` = SQL | Every new row satisfies every constraint; adding one validates existing rows first | Refuse |
| `generatedColumns` | `delta.generationExpression` | Enforce `(value <=> expr) IS TRUE` | Refuse |
| `allowColumnDefaults` | `CURRENT_DEFAULT` | Fill omitted columns (and `DEFAULT`) with the expression; variant defaults to null; `ADD COLUMN … DEFAULT` not allowed in one command | Refuse inserts that omit such columns |
| `identityColumns` | `delta.identity.start/step/highWaterMark/allowExplicitInsert` | Generate `start + k·step` beyond the high-water mark, detect overflow, update the mark at commit; refuse explicit values unless allowed | Refuse |
| `changeDataFeed` | `delta.enableChangeDataFeed=true` (feature listed at writer 7) | Write `cdc` files with `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`) for operations that change existing rows; pure appends/removes need none | Refuse UPDATE/DELETE/MERGE |
| `rowTracking` | listed (and not `delta.rowTrackingSuspended`) | `baseRowId` on every `add` above `rowIdHighWaterMark` in domain `delta.rowTracking`; `defaultRowCommitVersion` = commit version; carry both through recommits and removes. When enabled (`delta.enableRowTracking`): preserve stable IDs via materialized columns named in `delta.rowTracking.materialized*ColumnName`; set `commitInfo.tags["delta.rowTracking.preserved"]` | Refuse writes |
| `domainMetadata` | listed | Preserve every domain, even unknown; never let users change `delta.` domains | Refuse writes |
| `clustering` | listed | Keep clustering columns in domain `delta.clustering`; write stats for them; set `clusteringProvider` only when clustering | Preserve, don't cluster |
| `inCommitTimestamp` | `delta.enableInCommitTimestamps=true` | `commitInfo` first, with `inCommitTimestamp` = max(now, previous + 1 ms); enablement version/timestamp properties | Refuse writes |
| `typeWidening` | `delta.enableTypeWidening=true` | Only listed widenings; record `delta.typeChanges` in field metadata; Iceberg-incompatible widenings refused under IcebergCompat | Refuse schema change |
| `deletionVectors` | `delta.enableDeletionVectors=true` | `numRecords` exact on any file with a DV; keep `tightBounds` honest | Rewrite files instead of DVs (allowed while reading DVs) |
| `void` type | schema | Omit `void` columns from data files; refuse writes when `void` sits in an array/map or makes a struct or table all-void | — |

Gap review §8.1 requires, for each writer feature, a test that honours it or a test that refuses the write, before
anything is written.

## 9. Maintenance

| Operation | Protocol | Implementation **(impl)** |
|---|---|---|
| VACUUM | Data files no longer in the latest version may be deleted lazily after a retention period ("default 7 days"); a tombstone expires when the cleaning node's current time exceeds `deletionTimestamp` + threshold. With `vacuumProtocolCheck`, vacuum must run the **writer** protocol check first | Property `delta.deletedFileRetentionDuration` (default 1 week, unverified); deletes unreferenced files under the table root, skipping `_`-prefixed directories other than `_change_data` (unverified detail) |
| Log (metadata) cleanup | Cutoff = midnight UTC of the retention threshold; keep the newest checkpoint not newer than the cutoff commit, its JSON, and everything after; delete older commits, checkpoints, `.crc`, compactions starting at or before it, staged commits ≤ it; keep referenced sidecars and sidecars younger than a day | Property `delta.logRetentionDuration` (default 30 days, unverified); runs after checkpointing |
| Checkpointing | Any time, for any committed version; prefer V2 UUID-named when `v2Checkpoint`; never multi-part with it | `delta.checkpointInterval` (default 10 commits, unverified) |
| `checkpointProtection` | Not in PROTOCOL.md | delta-spark feature guarding history needed to drop a reader feature (unverified) |
| OPTIMIZE / compaction | `remove` + `add` with `dataChange=false`; allowed on append-only tables; keep `baseRowId`/`defaultRowCommitVersion` rules; recluster only files of the same `clusteringProvider` | Bin-packing, Z-order, liquid clustering |
| Catalog-managed tables | Maintenance goes through the catalog (data-layout changes such as OPTIMIZE, REORG) | — |

## 10. Writer invariants

| # | Invariant | Source |
|---|---|---|
| 1 | Never overwrite a log entry or a `.crc`; data files and sidecars uniquely named and never overwritten | Writer requirements, Version Checksum, Sidecar Files |
| 2 | Refuse to write unless every `readerFeatures` and `writerFeatures` entry is implemented, checked **before** any file is written | Table Features |
| 3 | Version 0 has `protocol` and `metaData`; at most one of each per commit; one `txn` per `appId`; one action per `(path, dvId)` per type | Delta Log Entries |
| 4 | Every data-file column in the schema has the schema's type (except recorded widenings); every partition column has a value for every file | Consistency |
| 5 | `add.partitionValues` and stats use physical names under column mapping; data files carry physical names and `field_id`; `maxColumnId` only grows; physical paths never reused | Column Mapping |
| 6 | A file with a DV has exact `numRecords`; `tightBounds=false` when stats are not exact over valid rows | Per-file Statistics |
| 7 | Never write a DV unless `delta.enableDeletionVectors=true` | Deletion Vectors |
| 8 | Respect `appendOnly`, invariants, CHECK, generated, identity, defaults and CDF when active (§8) | Writer Version Requirements |
| 9 | Row tracking: fresh row IDs unique per version and above the high-water mark; reassigned on retry | Row Tracking |
| 10 | ICT: `commitInfo` first, timestamps strictly increasing | In-Commit Timestamps |
| 11 | Every commit attempt's `commitInfo` carries a fresh `txnId`, so no two attempts write the same bytes to `_delta_log/N.json` (Lake.NET's create recovery relies on it, §7) | Lake.NET decision |
| 11 | Preserve unknown `domainMetadata`; never modify `delta.` domains you don't understand | Domain Metadata |
| 12 | Checkpoint only after its commit exists; no multi-part with `v2Checkpoint`; a V2 checkpoint puts all file actions inline or all in sidecars | Checkpoints |
| 13 | Metadata cleanup keeps a checkpoint at the oldest kept version and that version's JSON | Metadata Cleanup |
| 14 | Vacuum runs the writer protocol check; deletes only tombstoned files past retention | VACUUM Protocol Check |
| 15 | Never add a feature only to `readerFeatures`; never remove a supported feature in an ordinary commit | Supported Features |

## 11. How Lake.Delta will be verified

Order of work (goal W8; gap review §8, §9):

1. ✅ **Writer-feature table**: `delta-support.json` + `SupportDeclarationTests`. Next: add the three missing
   features (§3), resolve `checkpointProtection`, and have CI link each `supported`/`refused` entry to a test.
2. ⏳ **Shared harness first**: `IFormatDriver`, a format-neutral `TableFingerprint`/`ShadowCompare` and the common
   BDD feature set (gap review §9), so Delta is born on them.
3. ⏳ **Reader on DAT**: the Delta Acceptance Tests (`delta-incubator/dat`) reader cases as committed fixtures
   (regenerated, never hand-edited), every case's expected data compared; a `PROTOCOL.md` section map for
   `tools/spec_trace.py` and tests tagged by section; decoders (JSON log, checkpoint Parquet, DV files, Z85) under the
   `Corruption` contract with fuzz targets; DV byte-compatibility test against Iceberg's codec (§6).
4. ⏳ **Writes**: append, overwrite, delete (DVs or rewrite), then row tracking and checkpoints; each writer feature
   honoured or refused (§8).

| Evidence | What it proves | State |
|---|---|---|
| DAT reader cases | Log replay, checkpoints, column mapping, DVs, time travel, partition values, as other engines wrote them | ⏳ |
| delta-rs (`deltalake`, arrow-rs Parquet) oracle | Reads every table we write; we read its tables; `.history()` agrees with our log | ⏳ |
| Spark + Delta oracle (parquet-java) | Same both ways; `DESCRIBE HISTORY` agrees; concurrent writers race us | ⏳ |
| DuckDB `delta` extension (Delta Kernel, `duckdb/duckdb-delta`) | A cheap third reader of our tables | ⏳ |
| Put-if-absent fidelity | `If-None-Match` on every backend (moto, MinIO, AWS) in `EmulatorFidelityTests` style; refusal or a lock-based log store where unsupported; the identical-bytes hazard in §7 | ⏳ |
| Conflict matrix | `WriteSerializable` and `Serializable` × {append, delete, update, merge, optimize, metadata, protocol}, plus `txn` idempotency, against Spark and delta-rs writing at the same time | ⏳ |
| Checkpoint and log faults | Stale/missing `_last_checkpoint`, checkpoint without its commit, missing multi-part part, missing sidecar, cleanup racing time travel, vacuum within retention | ⏳ |
| Serialization properties | `partitionValues` (nulls, `""`, escaping, time zones, decimals), stats truncation and `tightBounds`, inline Z85 DVs, column-mapping names after rename/drop/re-add — property-tested against delta-rs parsing | ⏳ |
| ICT time travel | `inCommitTimestamp` vs S3 modification time, with an injectable clock (gap D6) | ⏳ |

## 12. References

| What | Where |
|---|---|
| Delta protocol | https://github.com/delta-io/delta/blob/719768686369f1d52e6891aec1f7fa1be903975c/PROTOCOL.md |
| Delta Acceptance Tests | https://github.com/delta-incubator/dat |
| delta-rs, Delta Kernel (Rust), DuckDB `delta` | https://github.com/delta-io/delta-rs, https://github.com/delta-io/delta-kernel-rs, https://github.com/duckdb/duckdb-delta |
| RoaringBitmap 64-bit portable format | https://github.com/RoaringBitmap/RoaringFormatSpec |
| Z85 | https://rfc.zeromq.org/spec/32/ |
| Iceberg Puffin `deletion-vector-v1` | `docs/puffin.md`; `src/Iceberg.Net.Puffin/Blobs/DeletionVector.cs` |
| Lake.NET declaration and test | `docs/spec/delta-support.json`; `tests/Iceberg.Net.Spec.Tests/SupportDeclarationTests.cs` |
| Plan | [goal.md](../../goal.md) (W8), [gap review §8–9](../../verification/gap-review-2026-10.md), [formats-and-engines.md](../formats-and-engines.md) |
