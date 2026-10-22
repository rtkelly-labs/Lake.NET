# Spec coverage

The Iceberg specifications are the foundation of this port: every behaviour is traced to a spec section and,
where the spec gives test vectors, pinned by them. This page is the contract tracker.

Specs tracked (all in `apache/iceberg` `format/` and `open-api/`):

| Spec | Version tracked | Notes |
|---|---|---|
| Table spec (`format/spec.md`) | v1, v2, v3 (v4 in development, not adopted) | v4 adds an adaptive metadata tree, relative paths, a `file` type and bans new equality deletes ([research](research/ecosystem-2026-10.md#11-table-format-versions)) |
| REST catalog (`open-api/rest-catalog-open-api.yaml`) | as of Java 1.12.0 (`info.version` still 0.0.1) | capability negotiation via `GET /v1/config` `endpoints` |
| Puffin (`format/puffin-spec.md`) | v1 | needed for v3 deletion vectors and statistics |
| View spec (`format/view-spec.md`) | v1 | |

Legend: ✅ implemented and tested · 🟡 partial · ⬜ not started · — not applicable.
"Tests" names the class in `tests/` that pins the behaviour.

## Table spec

### Schemas and data types

| Section | v1 | v2 | v3 | Status | Tests |
|---|---|---|---|---|---|
| Primitive types (boolean … binary) | ✓ | ✓ | ✓ | ✅ model + JSON + Parquet I/O incl. decimal (write P≤18) and uuid; 🟡 fixed not in Parquet I/O | `MetadataSpecTests`, `NameMappingAndImportTests` |
| `timestamp_ns`, `timestamptz_ns` | | | ✓ | ✅ model, JSON, transforms, Parquet I/O | `TransformSpecTests` |
| `variant` | | | ✓ | 🟡 model + JSON only (no shredding/encoding) | `MetadataSpecTests` |
| `geometry`, `geography` (CRS, algorithm) | | | ✓ | 🟡 model + JSON only (WKB I/O and bounds not done) | `MetadataSpecTests` |
| `unknown` | | | ✓ | 🟡 model + JSON + promotion rule | `MetadataSpecTests` |
| struct / list / map with field IDs | ✓ | ✓ | ✓ | ✅ model + JSON; ⬜ Parquet I/O for nested | `MetadataSpecTests` |
| `initial-default` / `write-default` | | | ✓ | 🟡 JSON round-trip; reader applies primitive initial defaults | `MetadataSpecTests` |
| Identifier field IDs (required, non-float) | ✓ | ✓ | ✓ | ✅ | `MetadataSpecTests` |
| Schema evolution & type promotion (int→long, float→double, decimal widening, v3 date→timestamp, unknown→any) | ✓ | ✓ | ✓ | ✅ rules; 🟡 no `UpdateSchema` builder API yet (callers build the new schema) | `MetadataSpecTests`, `LocalTableTests` |
| Column projection by field ID | ✓ | ✓ | ✓ | ✅ (name fallback only when a file has no IDs) | `PyIcebergFixtureTests` |
| Name mapping (`schema.name-mapping.default`) | ✓ | ✓ | ✓ | ✅ top-level columns, aliases for renames | `NameMappingAndImportTests` |
| Reserved field IDs / metadata columns (`_file`, `_pos`, `_row_id`, …) | ✓ | ✓ | ✓ | ⬜ | |

### Partitioning and sorting

| Section | Status | Tests |
|---|---|---|
| Partition specs, field IDs from 1000, `last-partition-id` | ✅ | `TransformSpecTests`, `StateManagementTests` |
| v1 specs without `field-id` (sequential assignment) | ✅ | `MetadataSpecTests` |
| identity / void | ✅ | `TransformSpecTests` |
| bucket[N] with Appendix B hashing (all spec vectors incl. `timestamp_ns`) | ✅ | `TransformSpecTests` |
| truncate[W] (ints, longs, decimals, code-point strings, binary) | ✅ | `TransformSpecTests` |
| year / month / day / hour (floor semantics before epoch) | ✅ | `TransformSpecTests` |
| Unknown transforms round-trip unchanged; writers refuse them | ✅ | `TransformSpecTests` |
| v3 multi-argument transforms (`source-ids`) | 🟡 JSON round-trip only | |
| Partition evolution (new spec IDs, reuse of compatible specs) | ✅ | `StateManagementTests` |
| Hive-style partition paths (`name=human-value`) | ✅ | `TransformSpecTests` |
| Sort orders (JSON, reuse, default) | ✅ metadata; ⬜ writers don't sort | `MetadataSpecTests` |
| Cross-implementation agreement on partition values | ✅ vs PyIceberg (Rust `pyiceberg-core`) | `PyIcebergFixtureTests`, `CrossEngineTests` |

### Manifests and snapshots

| Section | Status | Tests |
|---|---|---|
| Manifest list schema (v1 read, v2/v3 read+write), partition field summaries | ✅ | `AvroSpecTests`, `PyIcebergFixtureTests` |
| Manifest entry schema, `data_file` struct, metrics maps (`k117_v118`, …) | ✅ | `AvroSpecTests` |
| Manifest Avro metadata keys (`schema`, `partition-spec`, `format-version`, `content`) | ✅ | `PyIcebergFixtureTests` |
| Entry status ADDED / EXISTING / DELETED semantics | ✅ | `LocalTableTests`, `PyIcebergFixtureTests` |
| Snapshot ID and sequence-number inheritance (ADDED only) | ✅ | `AvroSpecTests` |
| Sequence numbers (`last-sequence-number`, monotonic, `file_sequence_number`) | ✅ | `StateManagementTests` |
| Snapshot summary (operation + totals) | ✅ append/overwrite/delete totals | `LocalTableTests` |
| Single-value binary serialisation (Appendix D) for bounds | ✅ incl. promoted-width reads | `AvroSpecTests` |
| v1 snapshots with embedded `manifests` list | ⬜ (refused explicitly) | |
| Manifest compaction / merge-append | ⬜ (fast-append only: one manifest per commit) | |
| Column metrics modes (`write.metadata.metrics.*`, truncate(16)) | 🟡 full metrics; long strings omit bounds instead of truncating | `LocalTableTests` |

### Row-level deletes and v3 capabilities

| Section | Status | Notes |
|---|---|---|
| Position delete files (v2) | ✅ read + write (one file per data file) | `RowDeleteTests`, `PuffinCrossEngineTests` (PyIceberg applies ours), `AthenaTests` (we apply Athena's) |
| Equality delete files (v2) | ⬜ | scans refuse them explicitly; v4 bans writing new ones, read support still required |
| Deletion vectors (v3, Puffin `deletion-vector-v1`) | ✅ read + write; one DV per data file (merged on later deletes and concurrent retries); v2 → v3 converter | `PuffinSpecTests`, `RowDeleteTests`, `PuffinCrossEngineTests` — see [puffin.md](puffin.md) |
| Delete applicability by sequence number; DV supersedes position deletes | ✅ | `RowDeleteTests.Deletion_vector_applies_only_to_older_data` |
| Row lineage: `next-row-id`, snapshot `first-row-id`/`added-rows`, manifest and file `first_row_id` | ✅ assignment and inheritance | `StateManagementTests`, `LocalTableTests`, `CatalogConformanceTests` |
| Row lineage metadata columns `_row_id`, `_last_updated_sequence_number` | ⬜ | |
| Table encryption keys, snapshot `key-id` | 🟡 metadata only | |

### Table metadata

| Section | Status | Tests |
|---|---|---|
| All v1/v2/v3 fields, legacy v1 `schema`/`partition-spec` read and write | ✅ | `MetadataSpecTests` |
| `current-snapshot-id`: -1 for v1/v2, `null` for v3 (as Java ≥1.9) | ✅ | `MetadataSpecTests` |
| Refs (branches, tags, retention fields); implied `main` when `refs` absent | ✅ | `MetadataSpecTests`, `StateManagementTests` |
| Snapshot log / metadata log (trimmed by `write.metadata.previous-versions-max`) | ✅ | `StateManagementTests` |
| Statistics / partition statistics file entries | ✅ metadata; NDV statistics files written (`PuffinStatistics`) | `MetadataSpecTests`, `RowDeleteTests` |
| Metadata file naming `{version:05d}-{uuid}.metadata.json`, gzip read | ✅ | `LocalTableTests` |
| Commit protocol: atomic pointer swap, never overwrite metadata | ✅ | `CommitProtocolTests`, `EmulatorFidelityTests` |
| Format version upgrade (no downgrade) | ✅ | `StateManagementTests` |

## REST catalog spec

| Endpoint / feature | Status | Notes |
|---|---|---|
| `GET /v1/config` (defaults/overrides/endpoints/prefix) | ✅ | endpoints list stored, not yet used to gate calls |
| OAuth2 client credentials, bearer token | ✅ | `/v1/oauth/tokens` is deprecated upstream; configurable token URI |
| SigV4 (Glue / S3 Tables IRC endpoints) | ⬜ | hook exists: `RestCatalogOptions.Handler` |
| Namespaces: list (paged), create, exists, load, drop | ✅ | multi-level encoded with `0x1F` |
| Namespace `updateProperties` | ⬜ | |
| Tables: list (paged), create, load, exists, drop (purge), rename, register | ✅ | `CatalogConformanceTests` against the Java reference server |
| `updateTable` (requirements + updates) | ✅ | 409 → `CommitFailedException`; 500/502/503/504 → `CommitStateUnknownException` |
| `Idempotency-Key` header | 🟡 sent on mutations; no automatic retry yet | |
| Storage credentials / per-table `config` | ✅ longest-prefix credential selection | |
| `loadCredentials` refresh, remote signing | ⬜ | |
| Multi-table `transactions/commit` | ⬜ | |
| Scan planning (`plan`, `tasks`) | ⬜ | |
| Views | ⬜ | |
| Metrics reporting | ⬜ | |
| ETag / freshness-aware loading | ⬜ | |

## Puffin and views

| Item | Status |
|---|---|
| Puffin container (magic, footer JSON, blobs) | ✅ uncompressed + zstd blobs; LZ4 footers rejected |
| `apache-datasketches-theta-v1` | ✅ compact serial v3, verified against Apache DataSketches; NDV statistics files via `set-statistics` |
| `deletion-vector-v1` | ✅ Roaring64 portable (array/bitmap written, runs read), CRC-32 |
| View metadata v1 | ⬜ |

## How to extend this page

When a behaviour lands, flip its row and name the test that pins it. If the spec offers vectors (Appendix
B hashes, truncate examples, Appendix D encodings), use them verbatim before writing your own.
