# Iceberg v4: the proposals and what they mean here

Iceberg format **v4 is under active development and has not been adopted**. The spec says so in as many words:
"Version 4 is under active development and has not been formally adopted". This page follows the v4 work and records
what adopting each part would mean for this library, so the work can be planned before engines ship it.

- **Source:** `format/spec.md` on `apache/iceberg` `main` at `0e4a68c` (2026-10-23), plus Java release notes and
  community write-ups (listed at the end).
- **How we follow it:** `python3 tools/spec_trace.py drift` compares `main` with the sections pinned at 1.12.0 and
  lists every section that changed. The v4 sections show up there: `manifests`, `manifest-schema`,
  `content-stats-in-manifests`, `manifest-deletion-vectors`, `file-type`, `snapshot-root-file-first-row-id-assignment`
  and others. Re-read this page whenever the drift report changes, and update it.

## Status

| Proposal | Where it stands | Confidence |
|---|---|---|
| Relative paths in metadata | In the spec text (it was the only v4 item at the 1.12.0 tag); reportedly voted in May 2026 | High |
| Equality deletes banned for new writes | In the spec text | High |
| `file` data type | In the spec text, with Parquet (`FILE` logical type), Avro and JSON mappings | High |
| Snapshot `timestamp-ms` strictly increasing per branch | In the spec text | High |
| Adaptive metadata tree: root manifest, Parquet manifests, `tracked_file` schema, manifest deletion vectors | In the spec text on `main`; the spec PR (#16025) is merged. Design syncs continue | Medium: the shape is settled, details may still move |
| Deletion vector stored in the data file's entry (one-phase planning) | In the spec text | Medium |
| Typed column statistics (`content_stats`) | In the spec text | Medium |
| Column files (column updates without rewriting rows) | The fields are in the spec text; the full column-update proposal was still a draft PR | Low |
| Mumbling bitmap (the encoding for manifest deletion vectors) | Its own spec, `format/mumbling-spec.md`, version 1 | Medium |

**Implementations.**
- Java 1.11 added v4 foundational types (`TrackedFile`, content-stats classes).
- Java 1.12 added a v4 manifest reader, relative-path resolution, relativization utilities, and writing Parquet and
  Avro manifests in the v4 layout. It is not a complete v4 writer.
- No query engine we use reads v4 today: Spark, Trino, Athena, PyIceberg and DuckDB.

**For adopters this is a planning horizon, not a migration.** Athena and Glue have to support v4 before a production table
can use it. Until then producers stay on v2 (Athena-read tables) or v3.

## What each change entails here

| Change | What the spec says | What it means for this library | Size |
|---|---|---|---|
| **Root manifest replaces the manifest list** | One root manifest per snapshot. It can reference leaf manifests, v1–v3 manifests and **data files directly** | Planning becomes a tree walk with the root as entry point. A small commit can be **one file**: the root with its changes inlined ("single-file commits"). A new maintenance operation is needed to fold inlined entries into leaf manifests later | L |
| **Manifests are Parquet** | Root and leaf manifests are Parquet files with a new `tracked_file` schema, not compatible with v1–v3's `manifest_entry`. A manifest may mix partition specs | **Abstraction done.** Every manifest and manifest-list read and write in the library (17 call sites across 9 files) goes through `ManifestStore` (Iceberg.Net.Catalog). It picks an `IManifestFormat` by the file's magic bytes when reading, so an upgraded table keeps reading its Avro manifests, and by format version when writing. `AvroManifestFormat` wraps `ManifestCodec`; a Parquet manifest is refused by name for now. A v4 `ParquetManifestFormat` (ParquetSharp, already the data-file engine) slots in beside it, and the `tracked_file` model is the rest of the work | M |
| **Tracking statuses** | Live is ADDED, EXISTING or MODIFIED. Changes are DELETED or REPLACED. A leaf entry can be marked deleted or replaced in place through the **manifest deletion vector** stored on its root entry | Every place that interprets manifest status changes. That includes the serializable-commit validation (`ValidateNoConflictingDataAsync`), expiry reachability and `TableValidator`. "Modified" is a new concept: an entry changes without the file being re-added | M |
| **Manifest deletion vectors (Mumbling bitmaps)** | Positions of a leaf manifest that are no longer live, as a Mumbling bitmap. Three bitmaps: `dv`, `deleted_positions`, `replaced_positions` | **Codec done.** `MumblingBitmap` and `MumblingPfor` in the `Blobs/` folder of `Iceberg.Net.Puffin` (the Iceberg-shape side of the Puffin assembly), from `format/mumbling-spec.md` v1 (fetched from `main` on 2026-10-24). The encoder reproduces every PFOR example in the spec byte for byte, and the container examples are pinned as whole bitmaps (the spec has no whole-bitmap vectors, so those are assembled by its layout rules). The decoder follows the `Corruption` contract with explicit checks: `CorruptionTests.Mumbling_bitmap`, `MumblingBitmapTests.Corrupted_input_is_rejected_by_explicit_checks` (never reaches the guard's fallback), and a `mumbling` SharpFuzz target. Wiring it into manifests waits for the v4 manifest reader | S–M |
| **Deletion vector in the data file's entry** | A data file's DV lives in its own `deletion_vector` field, and its offset and size must match the Puffin footer. No separate delete manifests for new deletes | Planning is one phase: the DV comes with the file. `DeleteIndex` reads it from the entry. Writing a DV marks the data file's entry MODIFIED, with a REPLACED entry for change detection | M |
| **Only deletion vectors for new row-level deletes** | "Row-level deletes may only be written in v4 as deletion vectors." **No new equality deletes, and no new position-delete files.** Existing ones in upgraded tables are still applied | On v4, `DeleteStrategy.EqualityDeletes` and v2-style position-delete files must be refused. Copy-on-write and deletion vectors remain. Upserts that relied on equality deletes need a key lookup, then a DV, which our position-delete path already does. A converter like Flink's `ConvertEqualityDeletes` (equality deletes → DVs) belongs in maintenance before an upgrade | S (rules), M (converter) |
| **Typed column statistics** (`content_stats`) | Per-field structs of typed bounds, counts, `tight_bounds` and `total_bytes`, at field IDs `10_000 + 200 × field-id`, resolved by ID | **Model and layout done.** `ColumnStats` / `ContentStats` (Iceberg.Net.Core, `Manifests`) hold typed bounds (row-value representation; `GeoPoint` for geo, opaque `VariantBound` for variant), counts, `ColumnSize`, `TightBounds` and `TotalBytes`. `ContentStats.FromDataFile` / `ApplyTo` convert from and to the v1–v3 maps losslessly: bytes a typed value cannot reproduce (a 4-byte bound on a promoted `long`, a non-minimal decimal, bad UTF-8, a field the schema lacks) are kept verbatim. `BoundSerialization` adds the spec's geo-point and variant bound forms to Appendix D. `MetricsEvaluator` and Parquet row-group pruning now run on `ColumnStats` (it replaced `ColumnMetrics`), with unchanged results. `ContentStatsLayout` maps the model to the v4 struct (base IDs, reserved `_row_id` / `_last_updated_sequence_number` ranges, member offsets, geo structs, promotion on read). Tests: `ColumnStatsTests`, `ContentStatsLayoutTests`. Left: reading and writing `content_stats` in v4 manifests, and answering min/max from `tight_bounds` (the LINQ layer has no metadata-only min/max path today; its count path uses record counts only) | M |
| **Column files** (column updates) | A data file can have `column_files`, each holding newer values for some field IDs. Adding one changes the data sequence number | The biggest payoff for us. `UpdateAsync` and `MergeAsync` that change a few columns could write a column file instead of rewriting rows or adding a DV and re-inserting. Reads stitch base file and column files together by field ID. This touches the read path, `UpdateAsync` and compaction (folding column files back). Wait for the proposal to settle | L |
| **Relative paths** | Metadata paths may be relative to the table location, which a catalog may supply. `location` becomes optional in table metadata | One path resolver that every metadata read goes through, and an option to write relative paths. `TableMetadata.Location` becomes nullable and is supplied by the catalog. Moving a table no longer means rewriting its metadata; `RegisterTableAsync` and the Glue/S3 copy cases get simpler | S–M |
| **Strictly increasing snapshot timestamps** | On v4, a snapshot's `timestamp-ms` must be greater than its parent's on the same branch | **Done.** `SnapshotProducer` writes `max(now, parent + 1)` on every format version (the clock is injectable: `WithClock(TimeProvider)`). `TableValidator` reports `META-TIMESTAMP-ORDER` as an error on format-version ≥ 4 metadata only. On v1–v3 it reports nothing, not even a warning, because other writers legitimately break the rule there. Tests: `TableValidatorTests.Snapshot_timestamps_strictly_increase_…` (fixed clock, standing still and behind the parent; branches) and `Non_increasing_snapshot_timestamps_are_an_error_only_on_v4_metadata` | S |
| **`file` type** | A struct with implicit sub-fields (`uri`, `offset`, `size`, `content_type`, `checksum`, `inline`) that reserves 7 field IDs | **Schema part done.** `FileType` (`TypeId.File`) round-trips as JSON `"file"`. Its seven IDs are reserved in the schema index (clashes rejected, `HighestFieldId` and `last-column-id` cover them, fresh-ID assignment and `SchemaUpdate` skip them), and content stats are per sub-field. It is neither a primitive (no identifier, partition or sort use) nor a struct. It needs format v4, so every table this library can create or evolve refuses it, and metadata below v4 that contains it is rejected on read (`FileTypeTests`). Left: Parquet (`FILE` logical type) and Avro data mappings, values and expressions, and a `FileReference` CLR type in the LINQ layer. Useful for tables that point at blobs (audio files, documents) | S–M |
| **Row lineage in the root manifest** | Data files in the root manifest get `first_row_id` the way manifests do | The v3 row-ID assignment code extends to entries inlined in the root | S |
| **File-system-table commits removed** | The rename-based `v<N>.metadata.json` scheme goes in v4 | Nothing to do: we only commit through catalogs | — |

These neighbouring specs are not tied to v4, but are new and relevant to the same work:
- `format/expressions-spec.md`: a JSON form of expressions, which REST scan planning already uses.
- `format/udf-spec.md`: SQL UDFs.

## Plan

1. **Now, because it pays off on v3 as well:**
   - ~~**Strictly increasing snapshot timestamps**~~, enforced by `SnapshotProducer` and checked by `TableValidator` (done).
   - ~~**A manifest abstraction**~~ (done): `ManifestStore` and `IManifestFormat`, so a v4 Parquet implementation
     slots in beside the Avro one.
   - ~~**A typed column-statistics model**~~ (done): `ColumnStats` / `ContentStats` with the v1–v3 byte maps as one
     serialization and the v4 `content_stats` layout (`ContentStatsLayout`) as the other. File-metrics and row-group
     pruning work against it. The LINQ count path needs nothing: it sums record counts and has no bound-based path.
   - ~~**The `file` type in schemas**~~ (done): `FileType`, refused below v4.
   - ~~**The Mumbling bitmap codec**~~ (done, moved up from step 2 because the spec has test vectors):
     `MumblingBitmap` under the `Corruption` contract.
   - **Producer guidance**: prefer deletion vectors (or copy-on-write) to equality deletes on new v3 tables, so
     there is nothing to convert at upgrade time.
2. **When Java writes complete v4 tables**, which is the earliest point there is anything to check against:
   - **Read v4 first**, from Java-written fixtures committed like the Spark ones: root manifests, Parquet manifests,
     colocated DVs, manifest DVs, relative paths, typed stats.
   - Then **write v4**, with Spark reading the result, as v3 was proven.
3. **When engines read v4:** an upgrade path (`upgrade-format-version` to 4) with the deletion-vector-only write rules,
   an equality-delete-to-DV converter, the tree maintenance operation, and the single-file commit path.
4. **When column updates settle:** column files in `UpdateAsync` and `MergeAsync`, and stitching in the read path.

## Decisions for adopters

- **Equality deletes.** Any producer upserting with equality deletes must move to deletion vectors (key lookup, then
  DV) or copy-on-write before its tables can go to v4. Ours default to copy-on-write.
- **Single-file commits.** Fast-changing tables gain the most. They also need the tree maintenance operation scheduled,
  or the root manifest keeps growing. That is a new maintenance duty alongside compaction and expiry.
- **Relative paths.** These make it cheaper to copy tables between buckets (environments, regions). Write them only once
  every reader of a table supports v4.

## Sources

- Spec on `main`: <https://github.com/apache/iceberg/blob/main/format/spec.md>; Mumbling bitmap:
  <https://github.com/apache/iceberg/blob/main/format/mumbling-spec.md>
- Adaptive metadata tree spec PR: <https://github.com/apache/iceberg/pull/16025>
- Java release notes (v4 items in 1.11 and 1.12): <https://github.com/apache/iceberg/blob/main/site/docs/releases.md>
- Iceberg Summit 2026 recap (Snowflake): <https://www.snowflake.com/en/blog/engineering/iceberg-summit-2026-recap-v4-spec/>
- "The State of Apache Iceberg v4 in July 2026": <https://iceberglakehouse.com/posts/iceberg-v4-state-july-2026/>
- "Apache Iceberg v4: the current state, the proposals": <https://dev.to/alexmercedcoder/apache-iceberg-v4-the-current-state-the-proposals-and-why-they-matter-3e07>
- Earlier research, October 2026: [research/ecosystem-2026-10.md](research/ecosystem-2026-10.md)
