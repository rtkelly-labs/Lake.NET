# Apache Iceberg: the spec and how Lake.NET implements it

**Status:** reference, checked 2026-10-31, spec pinned at apache-iceberg-1.12.0. One page for an engineer who must
implement or review a format feature: what the pinned spec says, where Lake.NET (still `Iceberg.Net.*`) implements it,
and which test pins it. The per-row tracker is [spec-coverage.md](../../../spec-coverage.md); this page explains the
model behind it and does not replace it. Anything not confirmed in the pinned spec text or in the repo is marked
**(unverified)**.

✅ done and tested · 🟡 partial · ⏳ not started · **refused** = rejected with an explicit error · — not applicable

Spec sources (pinned): [`format/spec.md`][spec], [`format/view-spec.md`][view], [`format/puffin-spec.md`][puffin], REST
OpenAPI vendored at `docs/spec/open-api/rest-catalog-open-api.json`. Section keys in `docs/spec/sections.json` are the
spec's heading anchors; tests tag the heading title (`[Spec("Scan Planning")]`, `[Spec("puffin: deletion-vector-v1
blob type")]`), which `SpecAttribute.Slug` (`tests/Iceberg.Net.TestSupport/SpecAttribute.cs`) and
`tools/spec_trace.py` normalise to the key.

## 1. What the spec defines

### Object model

```
 catalog pointer  (Glue param / JDBC row / REST server store / InMemoryCatalog)
      │  CAS-swapped on every commit
      ▼
 metadata/00042-<uuid>.metadata.json      ← TableMetadata: schemas, specs, sort orders, properties,
      │                                      snapshots[], refs{main, branches, tags}, logs, statistics,
      │                                      last-sequence-number, next-row-id (v3)
      │  refs ──► snapshot-id ──► Snapshot { sequence-number, timestamp-ms, summary.operation,
      │                                      manifest-list, first-row-id, added-rows }
      ▼
 snap-<id>-<uuid>.avro  (manifest list: one manifest_file row per manifest; partition summaries,
      │                  counts, sequence_number, min_sequence_number, first_row_id)
      ├──► data manifest (Avro, one partition spec)   entries: status, snapshot_id, seq numbers, data_file{…}
      │        └──► data files (Parquet / Avro / ORC)
      └──► delete manifest (content = deletes)
               ├──► position delete files (v2)  rows (file_path, pos)
               ├──► equality delete files (v2+) rows of equality_ids columns
               └──► Puffin files (v3)           deletion-vector-v1 blobs at content_offset/size
 statistics/ (Puffin, theta NDV) and partition-statistics files are referenced from metadata, not manifests
```

Every file below the pointer is immutable once written ([File System Operations][s-fso]). A snapshot is the union of
live entries across its manifests; each content file may appear in at most one live entry
([Content file uniqueness][s-uniq]).

### Commit model

| Concept | Spec rule | Lake.NET |
|---|---|---|
| Atomic swap | New metadata file per change; swap the pointer from base to new ([Optimistic Concurrency][s-occ], [Metastore Tables][s-meta]) | `MetastoreCatalog.CommitTableAsync` (`src/Iceberg.Net.Catalog/Catalog.cs`); REST server swaps in `ICatalogStore` ([rest-server.md](../../../rest-server.md)) |
| Requirements | Not in the table spec: the REST `updateTable` payload (`assert-table-uuid`, `assert-ref-snapshot-id`, …) | `UpdateRequirement` / `MetadataUpdate` (`src/Iceberg.Net.Core/Metadata/MetadataUpdate.cs`), applied by `TableMetadataBuilder` on both client and server ([architecture.md](../../../architecture.md)) |
| Retry | Failed commit re-applies on the new base when its conditions still hold ([Commit Conflict Resolution and Retry][s-retry]) | `SnapshotProducer` + `CommitRetryPolicy` (`commit.retry.*`), `ValidateNoConflictingDataAsync` for overwrites/deletes |
| Sequence numbers | Every commit gets the next number; new entries write `null` and inherit from the manifest list, so manifests are reusable across retries ([Sequence Numbers][s-seq]) | New manifests written once with null snapshot ID and sequence numbers; only the manifest list is rebuilt per attempt |
| File-system tables | `v<V>.metadata.json` + atomic rename: **deprecated, unsafe on object stores, removed in v4** ([File System Tables][s-fst]) | Not implemented; commits go through catalogs only |
| Metadata naming | `<V>-<uuid>.metadata.json` for metastore tables | `MetastoreCatalog.NewMetadataLocation`: `{version:05d}-{uuid}.metadata.json` |

## 2. Format versions

Lake.NET reads v1–v3 (`TableMetadata.SupportedFormatVersion = 3`; `MetadataJson.FromJson` throws on anything higher),
writes v2 and v3, and creates v2 tables by default (`TableMetadata.DefaultFormatVersion = 2`, because Athena reads v2).

| Version | What it adds (pinned spec, [Format Versioning][s-fv], [Appendix E][s-appe]) | Lake.NET |
|---|---|---|
| v1 | Immutable-file analytic tables; legacy `schema`/`partition-spec` fields; optional manifest list (snapshot may embed `manifests`); partition field IDs implied from 1000 | ✅ read metadata and v1 manifests/lists; **refused**: writes to v1 tables (`SnapshotProducer`: "upgrade the table to v2") and reading v1 snapshots with embedded `manifests` (`TableScan.ManifestsAsync`) |
| v2 | Delete files (position, equality); `content` on manifests and files; data + file sequence numbers with inheritance; `snapshot_id` optional in entries; required: `table-uuid`, `last-sequence-number`, `schemas`, `current-schema-id`, `partition-specs`, `default-spec-id`, `last-partition-id`, `sort-orders`, `default-sort-order-id`, `manifest-list`, manifest counts; `block_size_in_bytes`, `file_ordinal`, `sort_columns` removed; partition field IDs tracked across specs | ✅ default write version; `MetadataJson.FromJson` rejects v2 metadata missing a required field |
| v3 | Types `timestamp_ns`, `timestamptz_ns`, `unknown`, `variant`, `geometry`, `geography`; `initial-default`/`write-default`; multi-argument transforms (`source-ids`); row lineage (`next-row-id`, `first-row-id`, `added-rows`, `first_row_id`, `_row_id`, `_last_updated_sequence_number`); deletion vectors (Puffin `deletion-vector-v1`, `referenced_data_file`, `content_offset`, `content_size_in_bytes`); **no new position delete files**; readers must tolerate unknown transforms; `encryption-keys` + snapshot `key-id`; promotions `date→timestamp(_ns)` and `unknown→any` | ✅ lineage, DVs, defaults, `timestamp_ns`; 🟡 `variant`/`geometry`/`geography`/`unknown` model + JSON + stats only (Parquet writer throws, `ParquetFiles.cs`); 🟡 multi-arg transforms JSON only (sort orders lossy, §13); 🟡 encryption keys metadata only |
| v4 (pinned) | "Under active development, not formally adopted". At 1.12.0 Appendix E lists only **relative paths** (`location` optional, [Paths in Metadata][s-paths]); the `content_stats` struct (field 146, [Content Stats][s-cstats]) is also already in the pinned text | **refused** on read (format > 3); 🟡 `ContentStatsLayout` (layout and promotion only); ⏳ relative paths |
| v4 (on `main`, not pinned) | Root manifest replacing the manifest list; Parquet manifests (`tracked_file`); DV in the data file's entry; manifest DVs (Mumbling bitmap); DV-only row deletes (no new equality or position files); `file` type; snapshot `timestamp-ms` strictly increasing per branch | Tracked in [iceberg-v4.md](../../../iceberg-v4.md): ✅ `ManifestStore`/`IManifestFormat` seam, ✅ `MumblingBitmap` codec, 🟡 `FileType` (refused below v4), ✅ timestamps written as `max(now, parent+1)` on every version, ⏳ root/Parquet manifests, DV-in-entry, relative paths |

## 3. Metadata structures

### Table metadata ([Table Metadata Fields][s-tmf])

| Field | v1/v2/v3 | Must hold | Lake.NET |
|---|---|---|---|
| `format-version` | req | Reader throws if higher than supported | `MetadataJson.FromJson` |
| `table-uuid` | opt/req/req | Throw if it changes after refresh | `AssertTableUuid` requirement |
| `location` | req (v4: opt) | Base for data/manifests/metadata | `TableLocations` confines writes under it |
| `last-sequence-number` | —/req/req | Monotonic; ≥ every snapshot's | `TableValidator` `META-SEQ-MAX`, `META-SEQ-ORDER` |
| `last-column-id`, `last-partition-id` | req | ≥ every assigned ID; IDs never reused | `META-LAST-COLUMN`, `META-LAST-PARTITION`; `SchemaUpdateTests.Ids_of_deleted_columns_are_not_reused` |
| `schemas`+`current-schema-id`, `partition-specs`+`default-spec-id`, `sort-orders`+`default-sort-order-id` | opt/req/req | Lists with IDs; v1 legacy singletons | ✅ both forms read and written (`MetadataSpecTests`) |
| `current-snapshot-id` | opt | Equals `main` in `refs`; Java writes `-1` ≡ null for v1/v2 ([Appendix F][s-snapid]) | `-1` for v1/v2, `null` for v3 (`MetadataSpecTests.Empty_table_current_snapshot_id_matches_java`) |
| `refs` | —/opt/opt | Always an implied `main` | ✅ |
| `snapshot-log`, `metadata-log` | opt | Appended per change | Trimmed by `write.metadata.previous-versions-max` |
| `statistics`, `partition-statistics` | opt | Puffin / partition stats files per snapshot | ✅ metadata; NDV files written (`PuffinStatistics`) |
| `next-row-id` | v3 req | > every assigned row ID | `META-ROW-ID`, `META-ROW-ID-OVERLAP` |
| `encryption-keys` | v3 opt | KMS-wrapped keys | 🟡 metadata only (`AddEncryptionKey`, `RemoveEncryptionKey`) |

### Snapshot ([Snapshots][s-snap])

`snapshot-id` (req), `parent-snapshot-id`, `sequence-number` (v2+ req), `timestamp-ms` (req), `manifest-list` (v2+
req), `summary` with required `operation` ∈ {`append`, `replace`, `overwrite`, `delete`}, `schema-id`, `first-row-id`
and `added-rows` (v3 req), `key-id` (v3 opt). Summary metric keys (`added-data-files`, `total-records`, `added-dvs`, …)
are Appendix F recommendations, not requirements. Lake.NET: `Snapshot` (`src/Iceberg.Net.Core/Metadata/Snapshot.cs`);
totals checked by `TableValidator` `SUMMARY-TOTAL` / `SUMMARY-BOUNDS` / `SUMMARY-NULL`.

### Manifest list `manifest_file` ([Manifest Lists][s-ml])

| ID | Field | v2+ | Note |
|---|---|---|---|
| 500–502 | `manifest_path`, `manifest_length`, `partition_spec_id` | req | `MANIFEST-LENGTH` checked |
| 517 | `content` | req | 0 data, 1 deletes |
| 515, 516 | `sequence_number`, `min_sequence_number` | req | 0 when reading v1 |
| 503 | `added_snapshot_id` | req | |
| 504–506, 512–514 | added/existing/deleted file and row counts | req | `MANIFEST-COUNTS`; null in v1 ⇒ "assume non-zero" |
| 507 | `partitions: list<field_summary>` | opt | `contains_null` 509, `contains_nan` 518, `lower_bound` 510, `upper_bound` 511 (Appendix D bytes) → `ManifestEvaluator` |
| 519 | `key_metadata` | opt | |
| 520 | `first_row_id` | v3 opt | Assigned to data manifests lacking one; delete manifests always null |

### Manifest entry and `data_file` ([Manifest Entry Fields][s-mef], [Data File Fields][s-dff])

Avro key-value metadata: `schema`, `schema-id`, `partition-spec`, `partition-spec-id`, `format-version`, `content`
(v2+ required; `PyIcebergFixtureTests.Manifest_list_and_manifest_metadata_keys_are_present`).

| ID | Field | Rule |
|---|---|---|
| 0 | `status` | 0 EXISTING, 1 ADDED, 2 DELETED; DELETED is informational, never scanned |
| 1 | `snapshot_id` | Optional from v2; inherited when null |
| 3 / 4 | `sequence_number` / `file_sequence_number` | Inherited **only for ADDED**; EXISTING/DELETED must carry both explicitly; never change after commit |
| 134 | `content` | 0 data, 1 position deletes, 2 equality deletes |
| 100–104 | `file_path`, `file_format`, `partition`, `record_count`, `file_size_in_bytes` | `partition` struct uses partition field IDs; `record_count` is DV cardinality for DVs |
| 108 / 109 / 110 / 137 | `column_sizes` / `value_counts` / `null_value_counts` / `nan_value_counts` | `map<int,long>`; value counts include nulls and NaNs |
| 125 / 128 | `lower_bounds` / `upper_bounds` | `map<int,binary>`, Appendix D bytes; NaN never a bound; −0.0 precedes +0.0 |
| 131, 132 | `key_metadata`, `split_offsets` | split offsets ascending |
| 135 | `equality_ids` | Required for content 2, null otherwise |
| 140 | `sort_order_id` | Null for position deletes |
| 142 | `first_row_id` | v3; null on new files (inherited), written for EXISTING/DELETED; always null for delete files |
| 143 | `referenced_data_file` | v2 opt, required for DVs |
| 144 / 145 | `content_offset` / `content_size_in_bytes` | v3 DVs; must equal the Puffin footer's offset/length |
| 146 | `content_stats` | v4 only |
| 105–107, 111 | deprecated | Do not write in v2+ (105 must be written in v1) |
| 141 | reserved | |

Delete-file metrics describe the deleted rows (all of them, or none). Lake.NET: `ManifestEntry`, `DataFile`
(`src/Iceberg.Net.Core/Manifests/ManifestModel.cs`), encoded by `ManifestCodec` behind `ManifestStore`; metrics as typed
`ColumnStats`/`ContentStats` (`src/Iceberg.Net.Core/Manifests/ColumnStats.cs`), lossless to and from the byte maps.

### Reserved field IDs ([Reserved Field IDs][s-res])

User field IDs must be ≤ 2147483447 (`Integer.MAX_VALUE − 200`).

| ID | Name | Lake.NET |
|---|---|---|
| 2147483646 / 2147483645 | `_file` / `_pos` | ⏳ not exposed as columns; excluded from metrics |
| 2147483644 / 2147483643 / 2147483642 | `_deleted` / `_spec_id` / `_partition` | ⏳ |
| 2147483546 / 2147483545 / 2147483544 | `file_path` / `pos` / `row` (position delete files) | `PositionDeleteFiles.FilePathFieldId` / `PosFieldId` (`src/Iceberg.Net.Data/DeleteFiles.cs`); `row` never written (Appendix F: no implementation writes it) |
| 2147483543–2147483541 | `_change_type`, `_change_ordinal`, `_commit_snapshot_id` | ⏳ as columns; `ChangeFeed` yields `ChangeRow` instead |
| 2147483540 / 2147483539 | `_row_id` / `_last_updated_sequence_number` | `TableScan.RowIdFieldId` / `LastUpdatedSequenceNumberFieldId`; `ContentStatsLayout` maps them to stats bases 9200 / 9000 |

## 4. Schemas and types

### Types and their Parquet mapping ([Primitive Types][s-prim], [Appendix A Parquet][s-parquet])

| Iceberg type | Since | Parquet physical / logical | Single-value bytes (Appendix D) | Lake.NET |
|---|---|---|---|---|
| `boolean` | v1 | `boolean` | `0x00` / non-zero | ✅ |
| `int` / `long` | v1 | `int32` / `int64` | 4 / 8 bytes LE | ✅ |
| `float` / `double` | v1 | `float` / `double` | 4 / 8 bytes LE | ✅ NaN counted, never a bound (`LocalTableTests.Metrics_include_nan_counts_and_exclude_nan_from_bounds`) |
| `decimal(P,S)`, P ≤ 38 | v1 | `int32` (P≤9), `int64` (P≤18), else minimal `fixed`; `DECIMAL(P,S)` | unscaled two's-complement BE, minimal bytes | ✅ (`WideTypesTests`) |
| `date` | v1 | `int32` `DATE` | days, 4 bytes LE | ✅ |
| `time` | v1 | `int64` `TIME_MICROS` adjustToUtc=false | micros, 8 bytes LE | ✅ |
| `timestamp` / `timestamptz` | v1 | `int64` `TIMESTAMP_MICROS` adjustToUtc false / true | micros, 8 bytes LE | ✅ |
| `timestamp_ns` / `timestamptz_ns` | v3 | `int64` `TIMESTAMP_NANOS` false / true | nanos, 8 bytes LE | ✅ (gap review §5: `timestamptz_ns` has no Parquet round-trip test) |
| `string` | v1 | `binary` `UTF8` | UTF-8, no length | ✅ bounds by code point |
| `uuid` | v1 | `fixed_len_byte_array[16]` `UUID` | 16 bytes BE | ✅ (`Guid` byte order handled: `TransformSpecTests.Guid_default_byte_order_would_break_uuid_buckets`) |
| `fixed(L)` / `binary` | v1 | `fixed_len_byte_array[L]` / `binary` | raw bytes | ✅ |
| `struct` / `list` / `map` | v1 | group / 3-level `LIST` / 3-level `MAP` | not supported | ✅ (2-level lists read) |
| `unknown` | v3 | omitted from data files; read as null | not supported | 🟡 model, JSON, promotion |
| `variant` | v3 | group `metadata`+`value` (no field IDs), `VARIANT` | not supported; bounds per Bound serialization | 🟡 model, JSON, opaque `VariantBound` |
| `geometry(C)` / `geography(C,A)` | v3 | `binary` WKB, `GEOMETRY` / `GEOGRAPHY` | WKB; bounds are x:y[:z][:m] points | 🟡 model, JSON, `GeoPoint` bounds (never prune) |

Column IDs must be written as Parquet field IDs ([Parquet][s-parquet]); `ParquetFiles` writes them on every node
including groups (`DuckDbParquetReaderTests` in `Iceberg.Net.DuckDbNet.Tests` checks an independent reader sees them).

### Field-ID resolution ([Column Projection][s-proj])

Columns are selected by field ID, never by name or position. For an ID missing from a file, in order: identity
partition value from the manifest → `schema.name-mapping.default` → `initial-default` → `null`. Lake.NET:
`ParquetFiles.ReadAsync` projects by ID; name fallback only when the file has no IDs and a mapping exists
(`NameMapping`, `src/Iceberg.Net.Core/Types/NameMapping.cs`; `ParquetImport` refuses ID-less files without one).
Tests: `ReadPathTests.Projection_returns_the_same_values_as_a_full_read_for_every_type_nested_column_and_default`,
`NameMappingAndImportTests.Files_without_ids_resolve_through_the_name_mapping_including_renames`.

### Schema evolution ([Schema Evolution][s-evo])

| Change | Allowed | Lake.NET (`SchemaUpdate`, `TypeUtil.IsPromotionAllowed`) |
|---|---|---|
| Add field | New ID from `last-column-id`, nested IDs too; required needs non-null `initial-default` + `write-default` (v3) | ✅ `AddColumn`, `AddRequiredColumn(…, initialDefault)` |
| Drop / rename / reorder | Rename keeps ID; IDs never reused | ✅ `DeleteColumn`, `RenameColumn`, `Move…` |
| `int→long`, `float→double`, `decimal(P,S)→decimal(P',S)` P'>P | v1+ | ✅ |
| `date→timestamp`/`timestamp_ns` (not tz) | v3+ | ✅ |
| `unknown→any` | v3+ | ✅ |
| Promotion of a partition source whose transform result would change | forbidden (e.g. `date→timestamp` under some transforms) | ✅ (`MetadataSpecTests.Type_promotion_rules`) |
| Group into / out of nested struct; primitive ↔ struct | forbidden | ✅ refused |

Bounds are never rewritten on promotion; decode by byte length (4-byte bound on `long` ⇒ `int`, on `timestamp` ⇒
`date`). Lake.NET: `ContentStats.FromDataFile` keeps undecodable bytes verbatim;
`ContentStatsLayoutTests.Bounds_written_with_an_older_type_are_promoted_by_the_read_schema`.

### Default values ([Default values][s-def])

`initial-default` fills rows written before the field existed, is set once and never changes; `write-default` is used
when a writer omits the field and may change. Writers must never omit a known field; missing `write-default` on a
required field ⇒ writer fails. Struct defaults are `null` or `{}` (sub-field defaults live on sub-fields). `unknown`,
`variant`, `geometry`, `geography` must default to null. Lake.NET: ✅ JSON single-value form (`SingleValueJson`),
reader applies `initial-default`; 🟡 typed writes do not apply unmapped `write-default` (baseline F13 in
spec-coverage).

### Identifier fields

`identifier-field-ids`: required primitive fields, not float/double, not inside map/list or an optional struct.
Uniqueness is not enforced by Iceberg. Lake.NET uses them as the default upsert key (`TableMutations.UpsertAsync`;
[write-modes.md](../../../write-modes.md)).

## 5. Partitioning and sort orders

### Transforms ([Partition Transforms][s-pt])

| Transform | Sources | Result | Rule | Lake.NET |
|---|---|---|---|---|
| `identity` | any primitive except geo | source | | ✅ `IdentityTransform` |
| `bucket[N]` | int, long, decimal, date, time, timestamp(tz)(_ns), string, uuid, fixed, binary | int | `(murmur3_x86_32(x) & Integer.MAX_VALUE) % N`, seed 0 | ✅ `BucketTransform`, `Murmur3` |
| `truncate[W]` | int, long, decimal, string, binary | source | `v - (((v % W) + W) % W)`; decimal W in the column's scale; string ≤ W **code points**; binary W bytes | ✅ `TruncateTransform` |
| `year`/`month`/`day`/`hour` | date/timestamp variants (hour: timestamps only) | int (`day` → date; readers accept int) | floor from epoch | ✅ `TimeTransform` (`Temporal_transforms_floor_towards_negative_infinity`) |
| `void` | any | null | v1's way to drop a field | ✅ `VoidTransform` |
| unknown | — | — | readers ignore for filtering (required in v3), still compare for partition equality; **writers must not commit with one** | ✅ `UnknownTransform` round-trips; `TableWriter` refuses |

All transforms map null → null. Code: `src/Iceberg.Net.Core/Transforms/Transform.cs`, `Murmur3.cs`.

**Appendix B hashing** ([Appendix B][s-appb]): int hashed as `hashLong(long(v))` so `34` and `34L` both give
`2017239379`; decimal over minimal BE unscaled bytes; date `hashInt(days)`; time/timestamps `hashLong(micros)`;
`_ns` timestamps converted to **micros** first; string UTF-8; uuid 16 BE bytes. Tests use the spec vectors verbatim:
`TransformSpecTests.Murmur3_matches_spec_vectors`, `Nanosecond_timestamps_bucket_like_their_microsecond_value`,
`Bucket_masks_sign_bit_before_mod`, `Truncate_matches_spec_examples`, `Truncate_counts_code_points_not_utf16_units`,
and `TargetTests.Bucket_hashes_match_the_spec_vectors` on netstandard2.0.

**Appendix D single-value serialization** ([Appendix D][s-appd]): the byte column in §4's table; bounds use it except
geo (x:y:z:m points of 8-byte LE doubles) and variant (metadata + object keyed by normalized JSON paths) per
[Bound serialization][s-bound]. JSON single values (defaults, REST) per Appendix D's JSON table. Lake.NET:
`Conversions.ToBytes`/`FromBytes`, `BoundSerialization`, `SingleValueJson`; tests `AvroSpecTests`, `ColumnStatsTests`,
`SingleValueJsonTests`.

### Partition specs and evolution ([Partitioning][s-part], [Partition Evolution][s-pevo])

- A spec field: `source-id` (or v3 `source-ids` for multi-arg), `field-id` (unique across specs from v2), transform,
  name. Sources are primitives, may sit in structs, never in lists/maps.
- Equivalent specs (same sources, transforms, names) must not be re-created; equivalent fields reuse their IDs. New
  IDs come from `last-partition-id` (v1 reference impl starts at 1000).
- v1 evolution: don't reorder, replace dropped fields with `void`, only append.
- A manifest has one spec; scans project predicates with the manifest's spec, not the current one.

Lake.NET: `PartitionSpec`, `PartitionSpecUpdate` (`src/Iceberg.Net.Core/Partitioning/`), `Table.UpdateSpecAsync`;
tests `StateManagementTests`, `MetadataBuilderTests`, `SparkCrossEngineTests` (`spec_evolved_v2`).

### Sort orders ([Sorting][s-sort])

Order ID 0 = unsorted. Fields: source(s), transform, `asc`/`desc`, `nulls-first`/`nulls-last`. Float order
`-NaN < -Inf < … < -0 < 0 < … < Inf < NaN`. Files record `sort_order_id`; position delete files set it null. Lake.NET:
`SortOrder` (in `PartitionSpec.cs`), `RowSorter` sorts each written file by the default order;
`SortOrderTests.Sorter_follows_direction_null_order_and_nan_placement`.

## 6. Row-level deletes

| Kind | Since | Encoding | Lake.NET |
|---|---|---|---|
| Position delete file | v2 (**deprecated v3**: no new ones in v3 tables) | Rows `(file_path 2147483546, pos 2147483545[, row 2147483544])` sorted by path then pos | ✅ read + write on v2, one file per data file (`PositionDeleteFiles`); on v3 the same API writes DVs |
| Equality delete file | v2 | Rows of the `equality_ids` columns; null matches null; a dropped delete column still applies | ✅ read + write (`EqualityDeleteFiles`, `DeleteStrategy.EqualityDeletes`); PyIceberg 0.12 refuses them |
| Deletion vector | v3 | Puffin `deletion-vector-v1`: 4-byte BE length, magic `D1 D3 39 64`, Roaring64 portable bitmap (32-bit key + 32-bit Roaring per key), 4-byte BE CRC-32; uncompressed; blob `snapshot-id`/`sequence-number` = −1 | ✅ `DeletionVector`, `RoaringPositionBitmap`, `IcebergPuffinFile` (`src/Iceberg.Net.Puffin/Blobs/`); `SnapshotProducer.DeleteRows` |

### Applicability ([Scan Planning][s-scan])

| Delete | Applies to a data file when | Partition |
|---|---|---|
| DV | `referenced_data_file == file_path` **and** data seq(data) ≤ seq(DV) | same spec + values |
| Position file | `referenced_data_file` (if set) matches, seq(data) ≤ seq(delete), **and no DV applies** | same spec + values |
| Equality file | seq(data) **<** seq(delete) | same spec + values, or delete spec unpartitioned (global) |

Rules a writer keeps: at most one DV per data file per snapshot, merged with every existing DV and applicable position
file ([Deletion Vectors][s-dv]); removing a data file removes its DVs; `content_offset`/`content_size_in_bytes` equal
the Puffin footer. Lake.NET: `DeleteIndex` (`src/Iceberg.Net.Data/DeleteFiles.cs`);
`SnapshotProducer.ConvertPositionDeletesToDeletionVectors` folds v2 files after an upgrade. Tests:
`RowDeleteTests.Deletion_vector_applies_only_to_older_data`,
`RowDeleteTests.Concurrent_deletes_on_one_file_merge_into_one_deletion_vector`,
`PartialCompactionTests.On_v3_position_deletes_become_deletion_vectors`,
`TableMutationTests.Copy_on_write_removes_deletion_vectors_of_rewritten_files`,
`ReadPathTests.Equality_deletes_still_apply_after_their_column_is_dropped`,
`DecoderLimitTests.A_deletion_vector_must_hold_exactly_the_positions_its_manifest_entry_records`,
`PuffinEdgeTests.Deletion_vectors_must_not_be_compressed`; validator codes `DV-MULTIPLE`, `DV-REFERENCE`,
`DV-DANGLING`, `DV-CARDINALITY`, `DV-BLOB`, warning `V3-POSITION-FILES`.

**v4 direction (on `main`, not pinned):** DVs only for new row deletes; the DV moves into the data file's entry
(one-phase planning); equality deletes stay readable but cannot be added. `DeleteStrategy.EqualityDeletes` will have to
be refused on v4 ([iceberg-v4.md](../../../iceberg-v4.md)).

## 7. Snapshot management

| Spec item | Rule | Lake.NET |
|---|---|---|
| Refs ([Snapshot References][s-refs]) | `branch` (mutable, committed to with retry) or `tag`; `min-snapshots-to-keep`, `max-snapshot-age-ms` (branches), `max-ref-age-ms` (not `main`; `main` never expires) | `table.ManageSnapshots()` → `SnapshotManager` (`src/Iceberg.Net.Data/SnapshotManagement.cs`): create/replace/rename/remove, rollback, set-current, fast-forward, cherry-pick (`CherryPicker`); branch writes and WAP ([snapshot-management.md](../../../snapshot-management.md)) |
| Retention ([Snapshot Retention Policy][s-ret]) | Drop aged refs → keep ref heads → keep branch ancestors until older than max age **and** beyond min count → expire the rest | `Maintenance.ExpireAsync` + `ExpireOptions`, per-ref `RefRetention`; files deleted by reachability **after** the commit; `gc.enabled=false` refuses; files outside the table skipped |
| Time travel ([Appendix F][s-tt]) | Use `snapshot-log`, not parent lineage, for "as of time" | `TimeTravel` / `ScanRequest` (by ID, ref, instant), with the snapshot's own schema |
| Snapshot IDs ([Appendix F][s-snapid]) | Positive, collision-resistant, not timestamp-only | `SnapshotProducer.NewSnapshotId`: Java's scheme (random UUID, high XOR low 8 bytes, `& long.MaxValue`); no collision check against existing IDs was found (unverified) |
| Statistics ([Table Statistics][s-stats], [Partition Statistics][s-pstats]) | Puffin files per snapshot, informational; partition stats sorted by partition, NULL first | ✅ `set-statistics`/`remove-statistics`; theta NDV blobs (`PuffinStatistics`); 🟡 partition statistics: metadata entries only (`TableMetadata.PartitionStatistics`), no file writer in `src/` |

Tests: `SnapshotManagementTests.Rollback_moves_main_to_an_ancestor_only_and_set_current_to_any_snapshot`,
`Cherry_pick_*`, `ExpiryRetentionTests`, `MaintenanceTests.Expiry_deletes_only_files_no_retained_snapshot_references`,
`MaintenanceTests.Expiry_skips_files_outside_the_table_location`, `BranchWriteTests`; oracle
`SparkCrossEngineTests.We_read_Spark_branches_tags_rollback_and_cherry_pick`.

## 8. Row lineage (v3)

| Step | Spec ([Row Lineage][s-rl], [First Row ID Assignment][s-fria]) | Lake.NET |
|---|---|---|
| Snapshot | `first-row-id = next-row-id` on **every attempt** (reassigned on retry); `added-rows` ≥ rows given IDs | `SnapshotProducer` (v3 branch) |
| Manifest list | Each data manifest without `first_row_id` gets one ≥ snapshot's, stepping by `added_rows_count + existing_rows_count`; existing values preserved; delete manifests null | `ManifestStore.WriteListAsync` path (via `SnapshotProducer`) |
| Manifest | New data files null `first_row_id`; reader assigns manifest's value + running `record_count` sum of null-ID files; EXISTING/DELETED entries write the inherited value | `ManifestCodec`, `TableScan` |
| Table | `next-row-id += added-rows` | `TableMetadataBuilder` |
| Rows | `_row_id = first_row_id + _pos` when null; `_last_updated_sequence_number = data seq` when null; file with null `first_row_id` ⇒ both null | `RowLineage`, `TableScan.ReadWithLineageAsync` |
| Moved rows | Copy non-null `_row_id`; null `_last_updated_sequence_number` if modified, else copy | Copy-on-write, `Compaction`, `UpdateMerge` write both explicitly |
| Upgrade to v3 | `next-row-id = 0`; old snapshots unchanged; first new snapshot assigns IDs to every data file | `MetadataBuilderTests.Format_upgrades_go_up_only_and_start_row_ids_at_v3` |

Equality-delete updates do not preserve lineage (treated as delete + insert). Tests:
`UpdateMergeTests.Updated_rows_keep_their_row_id_and_take_the_new_sequence_number`,
`MergeAppendTests.Merging_preserves_sequence_numbers_lineage_and_delete_applicability`,
`SparkFixtureTests.Row_lineage_matches_spark`, `SparkFixtureTests.Copy_on_write_preserves_spark_row_ids_for_untouched_rows`,
`ModelBasedTests`; validator `FILE-ROW-ID-OVERLAP`.

## 9. Views and the REST catalog

**View spec v1** ([view-spec.md][view]): `view-uuid`, `format-version` = 1, `location`, `schemas`,
`current-version-id`, `versions` (each: `version-id`, `schema-id`, `timestamp-ms`, `summary`, `representations` of type
`sql` with one per `dialect`, `default-catalog`, `default-namespace`), `version-log`, `properties`. Versions are
immutable; commits swap the metadata pointer like tables. Lake.NET: `ViewMetadata`, `ViewJson`
(`src/Iceberg.Net.Core/Metadata/ViewMetadata.cs`), `IViewCatalog` on `RestCatalog` and `InMemoryCatalog`
(`src/Iceberg.Net.Catalog/Views.cs`); `ViewTests`, `ViewConformanceTests` (Java REST server, Polaris, Trino).

**REST catalog** (OpenAPI pinned with the tag; client `RestCatalog`, server `Iceberg.Net.RestServer`, see
[rest-server.md](../../../rest-server.md) and spec-coverage's REST table):

| Area | Spec | Lake.NET |
|---|---|---|
| Config | `GET /v1/config`: defaults, overrides, `endpoints`, prefix | ✅ endpoint list gates transactions and scan planning |
| Namespaces, tables, views | CRUD, paging, rename, register | ✅; 🟡 namespace `updateProperties` client, `register-view`, functions ⏳ |
| Commit | `POST …/tables/{t}` with requirements + updates; 409 = failed, 5xx = unknown | ✅ 409 → `CommitFailedException`; 500/502/503/504 → `CommitStateUnknownException`; `Idempotency-Key` (UUIDv7) |
| Multi-table transactions | `POST /v1/{prefix}/transactions/commit`, all or nothing | ✅ `ICatalog.CommitTransactionAsync`, `StagedCommit` |
| Credentials | `config`/`storage-credentials` on load; `loadCredentials` | ✅ longest-prefix selection; server vends S3/Azure/GCS; 🟡 no client refresh |
| Remote signing | `…/sign` for S3 requests | ✅ both sides (`RemoteSigningTests`) |
| Scan planning | `plan`, `plan/{id}`, `tasks`, cancel | ✅ client (`ScanPlanningProtocol`, `ServerSideScan`) and server (`ScanPlanningTests`, `IncrementalScanPlanningTests`) |
| Auth | OAuth2 client credentials (`/v1/oauth/tokens` deprecated), bearer, SigV4 | ✅ `SigV4Handler` |

## 10. Writer invariants

Each rule: where it comes from, where Lake.NET enforces it, and the test that would fail if it broke.

1. **Metadata files are never overwritten.** Spec: immutable files, linear history. Enforced: `IFileIO.WriteAsync(…,
   overwrite: false)` (S3 `If-None-Match: *`). Tests: `LocalTableTests.Metadata_files_are_never_overwritten`,
   `EmulatorFidelityTests.S3_conditional_create_rejects_existing_objects`.
2. **The pointer swap is a real compare-and-swap from the base version.** Enforced: `MetastoreCatalog`, Glue
   `VersionId`, JDBC row update, REST server store. Tests: `CommitProtocolTests`,
   `EmulatorFidelityTests.Glue_update_table_enforces_version_id`,
   `ConcurrencyTests.A_lost_compare_and_swap_is_a_409_and_its_file_is_removed` (RestServer.Tests).
3. **Nothing is deleted after an unknown commit outcome; a lost race cleans up only its own unreferenced files.**
   Source: Java reference behaviour (`CommitStateUnknownException`), not spec text. Enforced: `SnapshotProducer` (no
   blind retry; resolves by reloading when the server answered). Tests:
   `CommitProtocolTests.Lost_race_deletes_the_orphaned_metadata_file`,
   `CommitProtocolTests.Snapshot_producer_does_not_retry_unknown_outcomes`,
   `FaultInjectionTests.Ambiguous_catalog_swap_never_deletes_the_new_metadata`,
   `ClientStatusMappingTests.A_lost_commit_response_is_unknown_not_failed`.
4. **Resolve every column by field ID; never reuse an ID.** Spec: Column Projection, `last-column-id`. Enforced:
   `ParquetFiles`, `SchemaUpdate`, `TableValidator` `META-LAST-COLUMN`. Tests:
   `ReadPathTests.Equality_deletes_apply_across_a_rename_and_a_type_promotion`,
   `Ids_of_deleted_columns_are_not_reused`.
5. **Sequence numbers increase strictly; an entry's data and file sequence numbers never change after commit.**
   Enforced: `TableMetadataBuilder` rejects a stale `add-snapshot`; EXISTING/DELETED entries written explicitly.
   Tests: `StateManagementTests.Stale_sequence_number_is_rejected`,
   `AvroSpecTests.Deleted_entries_do_not_inherit_sequence_numbers`; validator `META-SEQ-ORDER`, `ENTRY-SEQ`,
   `MANIFEST-SEQ`.
6. **New entries leave snapshot ID, sequence numbers and `first_row_id` null so manifests survive retries; the manifest
   list and `first-row-id` are rebuilt per attempt.** Enforced: `SnapshotProducer` steps 2–4
   ([architecture.md](../../../architecture.md)). Tests: `LocalTableTests.Concurrent_writers_both_land_via_refresh_and_retry`,
   `StagedCommitTests.When_retries_run_out_neither_table_changes_and_the_staged_files_are_removed`.
7. **Snapshot `timestamp-ms` strictly increases along a branch.** Spec: v4 only, on `main` (**not in the pinned
   text**). Enforced on every version: `SnapshotProducer` writes `max(now, parent + 1)`; `TableValidator`
   `META-TIMESTAMP-ORDER` errors on v4 only. Tests:
   `TableValidatorTests.Snapshot_timestamps_strictly_increase_along_a_branch_whatever_the_clock_says`,
   `Non_increasing_snapshot_timestamps_are_an_error_only_on_v4_metadata`.
8. **Bounds bound: lower ≤ every non-null non-NaN value ≤ upper; strings compare by code point; NaN never a bound.**
   The spec defines no metrics truncation; Java's `truncate(16)` default is a table property. Lake.NET omits bounds
   for long strings instead of truncating (`ParquetFiles`, comment at the metrics code). Tests:
   `NestedFieldFilterTests.String_bounds_follow_code_point_order`,
   `EvaluatorEdgeTests.Uuids_compare_as_unsigned_big_endian_and_strings_by_code_point`,
   `ColumnStatsTests.Unknown_columns_carry_counts_but_no_bounds`, `ReadPathTests`/`ScanPruningTests` property tests.
9. **Partition `truncate` counts code points and never splits a surrogate pair.** `TruncateTransform`. Tests:
   `TransformSpecTests.Truncate_counts_code_points_not_utf16_units`, `TargetTests.Truncation_never_splits_a_surrogate_pair`
   (netstandard2.0).
10. **Never commit with an unknown transform.** `TableWriter` refuses. Test: round-trip only, in
    `TransformSpecTests.Transform_strings_round_trip`; no test found for the write refusal.
11. **At most one DV per data file; DVs never compressed; `referenced_data_file` always set.** `SnapshotProducer`,
    `IcebergPuffinFile`. Tests: §6 list; validator `DV-MULTIPLE`, `DV-REFERENCE`.
12. **No new position delete files on v3.** `SnapshotProducer.DeleteRows` writes DVs on v3. Tests:
    `PartialCompactionTests.On_v3_position_deletes_become_deletion_vectors`; validator warning `V3-POSITION-FILES`.
13. **Each content file is live at most once per snapshot.** Spec: Content file uniqueness (writers need not
    validate). Lake.NET validates after the fact: `TableValidator` `DATA-DUPLICATE`.
14. **`next-row-id` covers every assigned row ID; ranges never overlap.** `TableMetadataBuilder`, `SnapshotProducer`.
    Validator `META-ROW-ID`, `META-ROW-ID-OVERLAP`, `FILE-ROW-ID-OVERLAP`; `ModelBasedTests`.
15. **Overwrites and deletes validate against concurrent commits (serializable).** `ValidateNoConflictingDataAsync`.
    Tests: `OverwriteValidationTests`, `InterleavingTests.Overwrite_is_serializable_against_every_interferer`.
16. **Expiry deletes only files no retained snapshot references, only after the commit, and never outside the
    table.** `Maintenance.ExpireAsync`. Tests: `MaintenanceTests.Expiry_deletes_only_files_no_retained_snapshot_references`,
    `MaintenanceTests.Expiry_skips_files_outside_the_table_location`.
17. **Format version only goes up.** `UpgradeFormatVersion` in `TableMetadataBuilder`. Test:
    `StateManagementTests.Format_version_can_upgrade_not_downgrade`.

## 11. Interop pitfalls found in practice

From [roadmap.md "Bugs these paths found"](../../../roadmap.md#bugs-these-paths-found) and
[docs/upstream](../../../upstream/README.md):

| Pitfall | Spec angle | Fix / pin |
|---|---|---|
| String bounds chosen by UTF-16 order, not code point: files mixing U+E000–U+FFFF with supplementary characters pruned wrongly | Bounds must bound under Iceberg's ordering | `ValueComparer.CompareCodePoints`; `String_bounds_follow_code_point_order` |
| Hadoop-style `file:/path` (one slash) written into manifests by Java | Paths are URIs; v3 and earlier fully qualified | `LocalFileIO` accepts every form; `LocalTableTests.Local_file_io_accepts_every_file_uri_form_engines_write` |
| Gzipped metadata named `*.gz.metadata.json` (Java) or `*.metadata.json.gz` (older) | [Appendix F naming][s-gz] | `MetastoreCatalog.DecodeMetadata` sniffs gzip magic, version regex accepts both; `LocalTableTests.Gzipped_metadata_is_read_whatever_it_is_called_and_numbering_continues` |
| DVs sharing one Puffin file attributed to the wrong data files by Java ≤ 1.11 / Gravitino plan serializers | DV identity is path **+ offset**; `referenced_data_file` is required | Client matches DVs by `referenced_data_file`; `JavaMisreferencedDvTests`; [gravitino-dv-references.md](../../../upstream/gravitino-dv-references.md) |
| PyIceberg 0.12 reads a null `list<struct>` as `[]` | Not a spec issue: reader bug | Oracle avoids the case, Spark checks it; [pyiceberg-null-list-of-struct.md](../../../upstream/pyiceberg-null-list-of-struct.md) |
| Copy-on-write gave surviving rows new `_row_id`s | Row lineage: moved rows keep their ID | Rewrites store lineage explicitly; `Copy_on_write_preserves_spark_row_ids_for_untouched_rows` |
| `System.Guid` byte order is not big-endian | uuid hashes/bounds are 16 BE bytes | `Guid_default_byte_order_would_break_uuid_buckets` |
| Equality deletes on tables with several specs refused | Applicability is per (spec, partition) | Written per (spec, partition) since the Spark evolved fixture |
| HTTP 5xx after the server may have applied a commit | Unknown outcome ≠ failure | Resolved by reload when the server answered (soak) |
| Parquet.Net cannot write field IDs on group fields | Field IDs on every node | ParquetSharp is the engine; [parquet-net-group-field-ids.md](../../../upstream/parquet-net-group-field-ids.md) |
| Trino 478 cannot read DVs; PyIceberg refuses equality deletes | Engine limits | Pinned as expected failures (`TrinoCrossEngineTests`, PyIceberg cases) |

## 12. Implementation map

| Spec area | Source (`src/`) | Tests (`tests/`) | Oracle(s) |
|---|---|---|---|
| Types, schemas, JSON (Appendix C) | `Iceberg.Net.Core/Types/IcebergType.cs`, `Schema.cs`, `SchemaUpdate.cs`; `Json/TypeJson.cs`, `MetadataJson.cs` | `MetadataSpecTests`, `SchemaUpdateTests`, `NestedTypesTests`, `FileTypeTests` | Spark, PyIceberg fixtures |
| Single values, bounds (Appendix D) | `Types/Conversions.cs`, `Manifests/ColumnStats.cs` (`BoundSerialization`), `Json/SingleValueJson.cs` | `AvroSpecTests`, `ColumnStatsTests`, `SingleValueJsonTests` | Spec vectors |
| Transforms, hashing (Appendix B) | `Transforms/Transform.cs`, `Transforms/Murmur3.cs` | `TransformSpecTests`, `NetStandard.Tests/TargetTests` | Spec vectors; PyIceberg (`pyiceberg-core`) partition values |
| Partition specs, sort orders | `Partitioning/PartitionSpec.cs`, `PartitionSpecUpdate.cs`; `Iceberg.Net.Data/RowSorter.cs` | `StateManagementTests`, `MetadataBuilderTests`, `SortOrderTests` | Spark `spec_evolved_v2` |
| Table metadata, updates, requirements | `Metadata/TableMetadata.cs`, `TableMetadataBuilder.cs`, `MetadataUpdate.cs`, `Json/UpdateJson.cs` | `MetadataBuilderTests`, `StateManagementTests` | Java REST server (`CatalogConformanceTests`) |
| Manifests, manifest lists | `Iceberg.Net.Avro/ManifestCodec.cs`, `ManifestFormat.cs`, `AvroCodec.cs`; `Iceberg.Net.Catalog/ManifestStore.cs`; `Core/Manifests/ManifestModel.cs`, `ContentStatsLayout.cs` | `AvroSpecTests`, `ManifestStoreTests`, `MergeAppendTests`, `ContentStatsLayoutTests` | Spark, PyIceberg fixtures |
| Data files (Parquet, Avro) | `Iceberg.Net.Data/ParquetFiles.cs`, `NestedColumns.cs`, `AvroDataFiles.cs`, `ParquetImport.cs`, `TableWriter.cs` | `LocalTableTests`, `AvroDataFileTests`, `WideTypesTests`, `NameMappingAndImportTests` | DuckDB.NET Parquet reader, Spark, PyIceberg |
| Scan planning, expressions | `Core/Expressions/Expression.cs`, `Evaluators.cs`; `Data/TableScan.cs`, `Scan.cs` | `ExpressionTests`, `ScanPruningTests`, `ReadPathTests`, `ScanContractTests`, `EvaluatorEdgeTests` | `FilterCrossEngineTests` (PyIceberg), Spark |
| Deletes, DVs, Puffin | `Data/DeleteFiles.cs`, `TableMutations.cs`, `SnapshotProducer.cs`; `Iceberg.Net.Puffin/Format/PuffinFile.cs`, `Blobs/*` | `RowDeleteTests`, `PuffinSpecTests`, `PuffinEdgeTests`, `TableMutationTests`, `CorruptionTests` | pyroaring, PyIceberg (`PuffinCrossEngineTests`), Spark, Trino, Athena |
| Commits, retries | `Catalog/Catalog.cs`, `GlueCatalog.cs`, `JdbcCatalog.cs`, `RestCatalog.cs`; `Data/SnapshotProducer.cs`, `StagedCommit.cs` | `CommitProtocolTests`, `FaultInjectionTests`, `InterleavingTests`, `ModelBasedTests`, `StagedCommitTests`, `SoakTests` | Glue on moto (`EmulatorFidelityTests`), Java REST, Polaris |
| Row lineage | `Data/TableScan.cs` (`RowLineage`), `SnapshotProducer.cs`, `Compaction.cs`, `UpdateMerge.cs` | `StateManagementTests`, `UpdateMergeTests`, `MergeAppendTests` | Spark (`SparkFixtureTests`, `SparkCrossEngineTests`) |
| Snapshot management, maintenance | `Data/SnapshotManagement.cs`, `Maintenance.cs`, `Compaction.cs`, `MetadataTables.cs`, `ChangeFeed.cs`, `Catalog/TableFiles.cs` | `SnapshotManagementTests`, `ExpiryRetentionTests`, `MaintenanceTests`, `CompactionTests`, `BranchWriteTests` | Spark procedures |
| Statistics | `Data/PuffinStatistics.cs`, `Puffin/Blobs/ThetaSketch.cs` | `PuffinSpecTests` | Apache DataSketches |
| Validation of whole tables | `Data/TableValidator.cs`, `TableFingerprint.cs` | `TableValidatorTests`, every scenario | `ShadowCompareTests` (PyIceberg fingerprints) |
| Views | `Core/Metadata/ViewMetadata.cs`, `Catalog/Views.cs`, `RestServer/ViewUpdates.cs` | `ViewTests` | `ViewConformanceTests` (Java REST, Polaris, Trino) |
| REST protocol | `Catalog/RestCatalog.cs`, `ScanPlanning.cs`, `SigV4Handler.cs`; `Iceberg.Net.RestServer/*` | `RestProtocol.Tests`, `RestServer.Tests`, `IdempotencyTests`, `SigV4Tests` | Java REST fixture, Polaris, Gravitino, Spark 4, PyIceberg 0.12 |
| Locations, IO | `Catalog/TableLocations.cs`, `Iceberg.Net.IO/FileIO.cs` (`LocalFileIO`, `S3FileIO`, `ResolvingFileIO`) | `TableLocationsTests`, `FileIORoutingTests`, `LocationPolicyTests` | moto |

## 13. Open gaps

Not restated here; see [gap-review-2026-10.md](../../verification/gap-review-2026-10.md) and spec-coverage.

| Gap | Where |
|---|---|
| Fail-closed CI, skipped integrations, no own-version fixtures, coverage not checked by `matrix --check` | §1 E1, E2, E3, E5 |
| Snapshot/statistics paths confined (closed); JWT negatives, authorization holes, Parquet decoder unfuzzed | §2 S1–S2 (closed), S4–S9 |
| No `FileIOConformanceTests<T>`; listing `LastModified` default | §3 ST1 |
| Maintenance outside fault/model/soak; faults on whole writes only; commit faults only on InMemoryCatalog | §4 D1–D3 |
| v3 types not writable to Parquet; multi-arg sort-order JSON lossy (`TypeJson.SortOrderFromJson` keeps `source-ids[0]`); `timestamptz_ns` round-trip untested; `ValueEquality` hashes `byte[]` by length | §5 bullets |
| Scale envelope; non-streaming S3 reads, single PUT, no file rolling | §6 |
| Spec sections with no `[Spec]`-tagged test (33 of 104 at 1.12.0), including Paths in Metadata, Encryption Keys, Partition Statistics, Delete File Stats, Bounds for Geometry/Variant, Appendix G | `python3 tools/spec_trace.py` (computed for this page from `sections.json` vs test tags) |
| Typed writes ignore unmapped `write-default`; `_file`/`_pos`/changelog metadata columns not exposed; encryption metadata only; partition statistics not written | spec-coverage rows |

**Doc drift found while writing this page** (repo docs vs pinned spec or code; not fixed here):

- [gap-review §5](../../verification/gap-review-2026-10.md) says `.gz.metadata.json` is not decompressed; the code now
  sniffs the gzip magic (`MetastoreCatalog.DecodeMetadata`) and `LocalTableTests.Gzipped_metadata_is_read_whatever_it_is_called_and_numbering_continues` pins it.
- [spec-coverage.md](../../../spec-coverage.md) and `DeleteStrategy` describe v4 as banning equality deletes and adding
  a `file` type; that is `main`, not the pinned 1.12.0 text, whose only v4 change is relative paths (plus
  `content_stats`).
- [iceberg-v4.md](../../../iceberg-v4.md) says relative paths were "the only v4 item at the 1.12.0 tag"; the pinned
  text also has the `content_stats` field (146) and its sections.
- [state-management.md](../../../state-management.md) still marks DVs ⏳ ("no DVs yet"); [puffin.md](../../../puffin.md)
  says scans refuse equality deletes; [architecture.md](../../../architecture.md) says there is no expression engine
  and serializable overwrite validation is roadmap M2. Code and spec-coverage say all three are done.

## 14. References

Pinned at `apache-iceberg-1.12.0` (anchor = `sections.json` key):

- Table spec: [Format Versioning][s-fv] · [Optimistic Concurrency][s-occ] · [Sequence Numbers][s-seq] ·
  [File System Operations][s-fso] · [Paths in Metadata][s-paths] · [Primitive Types][s-prim] ·
  [Default values][s-def] · [Schema Evolution][s-evo] · [Column Projection][s-proj] · [Reserved Field IDs][s-res] ·
  [Row Lineage][s-rl] · [Partitioning][s-part] · [Partition Transforms][s-pt] · [Partition Evolution][s-pevo] ·
  [Sorting][s-sort] · [Content file uniqueness][s-uniq] · [Manifest Entry Fields][s-mef] · [Data File Fields][s-dff] ·
  [Content Stats][s-cstats] · [Snapshots][s-snap] · [Manifest Lists][s-ml] · [First Row ID Assignment][s-fria] ·
  [Scan Planning][s-scan] · [Snapshot References][s-refs] · [Snapshot Retention Policy][s-ret] ·
  [Table Metadata Fields][s-tmf] · [Table Statistics][s-stats] · [Partition Statistics][s-pstats] ·
  [Commit Conflict Resolution and Retry][s-retry] · [File System Tables][s-fst] · [Metastore Tables][s-meta] ·
  [Deletion Vectors][s-dv] · [Appendix A: Parquet][s-parquet] · [Appendix B][s-appb] · [Appendix D][s-appd] ·
  [Bound serialization][s-bound] · [Appendix E][s-appe] · [Time travel][s-tt] · [Snapshot IDs][s-snapid] ·
  [Gzip naming][s-gz]
- [Puffin spec][puffin] (`deletion-vector-v1`, `apache-datasketches-theta-v1`) · [View spec][view] · REST OpenAPI:
  `docs/spec/open-api/rest-catalog-open-api.json`, deviations in `docs/spec/openapi-deviations.json`
- In-repo: [spec-coverage.md](../../../spec-coverage.md) · [spec-traceability.md](../../../spec-traceability.md) ·
  [iceberg-v4.md](../../../iceberg-v4.md) · [puffin.md](../../../puffin.md) · [read-path.md](../../../read-path.md) ·
  [write-modes.md](../../../write-modes.md) · [update-merge.md](../../../update-merge.md) ·
  [snapshot-management.md](../../../snapshot-management.md) · [catalogs.md](../../../catalogs.md) ·
  [rest-server.md](../../../rest-server.md) · [formats-and-engines.md](../formats-and-engines.md)

[spec]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md
[view]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/view-spec.md
[puffin]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/puffin-spec.md
[s-fv]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#format-versioning
[s-occ]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#optimistic-concurrency
[s-seq]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#sequence-numbers
[s-fso]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#file-system-operations
[s-paths]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#paths-in-metadata
[s-prim]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#primitive-types
[s-def]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#default-values
[s-evo]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#schema-evolution
[s-proj]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#column-projection
[s-res]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#reserved-field-ids
[s-rl]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#row-lineage
[s-part]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#partitioning
[s-pt]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#partition-transforms
[s-pevo]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#partition-evolution
[s-sort]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#sorting
[s-uniq]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#content-file-uniqueness
[s-mef]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#manifest-entry-fields
[s-dff]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#data-file-fields
[s-cstats]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#content-stats
[s-snap]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#snapshots
[s-ml]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#manifest-lists
[s-fria]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#first-row-id-assignment
[s-scan]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#scan-planning
[s-refs]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#snapshot-references
[s-ret]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#snapshot-retention-policy
[s-tmf]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#table-metadata-fields
[s-stats]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#table-statistics
[s-pstats]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#partition-statistics
[s-retry]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#commit-conflict-resolution-and-retry
[s-fst]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#file-system-tables
[s-meta]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#metastore-tables
[s-dv]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#deletion-vectors
[s-parquet]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#parquet
[s-appb]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#appendix-b-32-bit-hash-requirements
[s-appd]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#appendix-d-single-value-serialization
[s-bound]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#bound-serialization
[s-appe]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#appendix-e-format-version-changes
[s-tt]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#point-in-time-reads-time-travel
[s-snapid]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#assignment-of-snapshot-ids-and-current-snapshot-id
[s-gz]: https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/format/spec.md#naming-for-gzip-compressed-metadata-json-files
