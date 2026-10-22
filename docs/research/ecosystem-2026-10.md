# Apache Iceberg Ecosystem: State as of 2026-10-22 (research for a C#/.NET port)

> Research snapshot taken 2026-10-22 to seed the design docs. Every claim carries a source URL; items marked
> **unverified** or *(search summary)* were not confirmed against a primary source. Re-check before relying on them.

Method: primary sources wherever possible. The spec, OpenAPI and docs text comes from `raw.githubusercontent.com/apache/iceberg` at `main` and at the release tags. Release dates come from Maven Central, PyPI, crates.io, the Go module proxy and NuGet registration APIs. GitHub release pages and web search fill the remaining gaps. `iceberg.apache.org`, `py.iceberg.apache.org`, `duckdb.org` and `docs.aws.amazon.com` were **not reachable** from this environment (proxy refused). Claims about those sites come from search-engine summaries and are marked *(search summary)*. Anything not confirmed is marked **unverified**.

Abbreviations: DV = deletion vector; MoR = merge-on-read; CoW = copy-on-write; IRC = Iceberg REST Catalog.

---

## 1. Specification state

### 1.1 Table format versions

Source: https://raw.githubusercontent.com/apache/iceberg/main/format/spec.md (sections "Format Versioning" and "Appendix E").

- "Versions 1, 2 and 3 of the Iceberg spec are complete and adopted by the community. **Version 4 is under active development and has not been formally adopted.**" The same sentence also appears at tags 1.10.0, 1.11.0 and 1.12.0.
- **v1: Analytic Data Tables.** Immutable Parquet, Avro or ORC files tracked by manifests.
- **v2: Row-level Deletes.**
  - Adds position and equality delete files.
  - Makes some writer requirements stricter (Appendix E, "Version 2").
- **v3: Extended Types and Capabilities.** Spec list, quoted:
  - New types: `timestamp_ns`, `timestamptz_ns`, `unknown`, `variant`, `geometry`, `geography`.
  - Column default values: `initial-default` and `write-default` on struct fields.
    - `write-default` is forward-compatible.
    - Older readers fail on required fields that are filled by `initial-default`.
  - Multi-argument transforms. Partition and sort fields write `source-ids` for a multi-argument transform and `source-id` for a single-argument transform. All readers must tolerate unknown transforms by ignoring those fields when filtering.
  - Row lineage:
    - Table `next-row-id`.
    - Manifest-list `first_row_id` per data manifest.
    - Data-file `first_row_id` (null when written, inherited at read time).
    - Reserved columns `_row_id` and `_last_updated_sequence_number`.
    - The spec gives exact rules for incrementing these and for carrying them across rewrites. When upgrading to v3, `next-row-id` starts at 0.
  - Binary deletion vectors:
    - Stored as the Puffin `deletion-vector-v1` blob type.
    - Manifests gain `referenced_data_file`, `content_offset` and `content_size_in_bytes`.
    - **At most one DV per data file.** Writers must merge DVs and older position deletes.
    - **Writers may not add new position delete files to v3 tables.** Existing v2 position delete files remain valid and must be folded into the DV when one is created.
  - Table encryption keys: table metadata `encryption-keys`, and a snapshot `key-id`.
- Since 1.9.0, Java writes `null` for `current-snapshot-id` on v3+ tables (release notes, PR #12335). See https://raw.githubusercontent.com/apache/iceberg/main/site/docs/releases.md.
- **v4 (in development, not adopted).** On `main` today the spec lists:
  - An "adaptive metadata tree" for manifests, to support efficient small commits, column updates and columnar stats.
  - Data file and DV stored in one combined entry, so planning no longer needs two phases.
  - **Relative locations** in metadata. `location` becomes optional; paths are resolved against the table location.
  - **Writing new equality deletes is prohibited.** Readers must still apply carried-over equality deletes.
  - A new `file` type: a struct with implicit sub-fields `uri`, `offset`, `size`, `content_type`, `checksum` and `inline`, reserving 7 field IDs.
  - A snapshot `timestamp-ms` that must strictly increase over its parent on the same branch.
- At the `apache-iceberg-1.12.0` tag, the v4 section lists only "relative locations". The adaptive tree, `file` type and equality-delete ban were added to `main` after 1.12.0 (compared directly: https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.12.0/format/spec.md).
- **v4 status in the community** *(secondary sources)*:
  - Adaptive metadata tree and single-file commits are "heavily developed proposals with active design syncs".
  - Relative paths and content stats are further along.
  - Column families / column updates are still open.
  - Sources: https://datalakehousehub.com/blog/iceberg-v4-state-july-2026/ and https://www.snowflake.com/en/blog/engineering/iceberg-summit-2026-recap-v4-spec/
- **Java v4 implementation progress** (1.11 and 1.12 release notes):
  - 1.11: v4 foundational types, `TrackedFile`, content stats classes.
  - 1.12: v4 manifest reader, writing Parquet and Avro manifests in the v4 layout, location relativization utilities, a co-located DV exposed through `DataFile`.
  - Source: https://raw.githubusercontent.com/apache/iceberg/main/site/docs/releases.md
- **Design implication for .NET:** target v1, v2 and v3 read/write. Keep manifest I/O behind an abstraction, because v4 may store manifests as Parquet or Avro in a tree layout.

### 1.2 Puffin spec

Source: https://raw.githubusercontent.com/apache/iceberg/main/format/puffin-spec.md

- File structure:
  - A single Puffin version.
  - Layout: magic `PFA1`, then blobs, then a footer. The footer holds a JSON `FileMetadata` with `blobs[]` (type, fields, snapshot-id, sequence-number, offset, length, compression-codec, properties).
- Blob types:
  - `apache-datasketches-theta-v1`: an NDV sketch.
  - `deletion-vector-v1`.
- DV blob layout (exact wire format matters for the port):
  - 4-byte **big-endian** length.
  - Magic bytes `D1 D3 39 64`.
  - A Roaring64 "portable" serialization:
    - Count of 32-bit bitmaps as 8 bytes, little-endian.
    - Then, per key: 4-byte LE key, followed by a standard 32-bit Roaring bitmap.
  - A 4-byte **big-endian CRC-32** over the magic and the vector.
  - The format is compatible with Delta DVs.
- Required DV blob properties:
  - `referenced-data-file` and `cardinality` must be present.
  - The DV blob must not be compressed.
  - `snapshot-id` and `sequence-number` must be -1.
- Allowed codecs for other blobs: `lz4` (single frame with content size) and `zstd` (single frame).

### 1.3 View spec

Source: https://raw.githubusercontent.com/apache/iceberg/main/format/view-spec.md

- View metadata `format-version` must be 1.
- Each version has `representations` (SQL with a dialect), a schema, a default namespace and catalog, and a summary.
- Views are supported over IRC (create, load, replace, drop, rename, exists, and `register-view` since 1.11).

### 1.4 REST Catalog OpenAPI

Sources:

- https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml
- Tagged copies at `apache-iceberg-1.8.0` through `1.12.0`, diffed locally.

General:

- `info.version` is still **0.0.1** ("first version of the REST Catalog API"). The paths are versioned as `/v1/...`. Compatibility is negotiated with the `endpoints` list returned by `GET /v1/config`. Without that list, a default endpoint set is assumed.
- `oauth/tokens` is **deprecated for removal** (deprecated since Java 1.6.0). Use an external IdP or the AuthManager API.

Endpoints on `main` today (operationIds):

- `getConfig`, `getToken`(deprecated)
- Namespaces: `listNamespaces`, `createNamespace`, `loadNamespaceMetadata`, `namespaceExists`, `dropNamespace`, `updateProperties`
- Tables: `listTables`, `createTable`, `registerTable`, `loadTable`, `updateTable`, `dropTable`, `tableExists`, **`unregisterTable`** (new in 1.12), `renameTable`, `reportMetrics`
- **Functions:** `listFunctions` and `loadFunction` (new in 1.12)
- **Scan planning:** `planTableScan` (`POST .../tables/{t}/plan`), `fetchPlanningResult` (`GET .../plan/{plan-id}`), `cancelPlanning`, `fetchScanTasks` (`POST .../tasks`)
- **Credential vending:** `loadCredentials` (`GET .../tables/{t}/credentials`). Delegation is requested with the `X-Iceberg-Access-Delegation: vended-credentials,remote-signing` header.
- **Remote signing:** `signRequest` (`POST .../tables/{t}/sign`), added to the spec in 1.11
- **Multi-table commit:** `commitTransaction` (`POST /v1/{prefix}/transactions/commit`)
- **Views:** `listViews`, `createView`, `loadView`, `replaceView`, `dropView`, `viewExists`, `renameView`, **`registerView`** (1.11)

Feature arrival by release (counted per tag):

| Feature | 1.10.0 | 1.11.0 | 1.12.0 |
|---|---|---|---|
| ETag / `If-None-Match` → 304 on loadTable ("freshness-aware loading") | yes | yes | yes |
| `Idempotency-Key` header on mutating operations | no | yes | yes |
| `signRequest`, `registerView`, `scan-planning-mode` | no | yes | yes |
| Catalog **labels** (`CatalogObjectLabels`, `FieldLabels`) on load responses | no | no | yes |
| Functions endpoints, `unregisterTable` | no | no | yes |

Scan-planning endpoints were already in the spec at 1.8.0. Java client support landed in 1.11 ("REST Scan Planning Task Implementation" #13400; "Spark: Enable remote scan planning" #14963).

Client behaviour notes:

- From the Java 1.12 notes: the REST client retries POSTs that carry an `Idempotency-Key` on 408, 500, 502, 503 and 504 (#17947).
- From 1.10:
  - "Avoid table corruption by stop retrying on 502 and 504" (#13352).
  - Treat 503 on commit as an **unknown commit state** (#13619).
- A .NET commit path must model the outcomes success, retryable conflict (409) and CommitStateUnknown (5xx).

Events endpoint:

- **Not in the spec on `main`.** No `/events` path exists.
- PR apache/iceberg#12584 proposed `/v1/{prefix}/events`, and implementation issues #13581 and #13582 track it. Merge status: **unverified (appears not merged)**.
- Source *(search summary)*: https://goodfirstissue.org/apache/iceberg/issues/13581

---

## 2. Releases

### 2.1 apache/iceberg (Java)

Dates from https://raw.githubusercontent.com/apache/iceberg/main/site/docs/releases.md, cross-checked with Maven Central https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/maven-metadata.xml. Latest is **1.12.0**.

| Version | Released | Headlines (state, commit and v3 focus) |
|---|---|---|
| 1.8.0 | 2025-02-13 | DV read and write (#11481, #11476); Spark writes DVs for v3 tables; row-lineage table metadata fields (#11948); Variant type defined; UnknownType; default-value reads for Parquet and Avro; expire snapshots can remove unused partition specs; RewriteTablePath and ComputeTableStats procedures; REST AuthManager refactor |
| 1.8.1 | 2025-02-28 | patch |
| 1.9.0 | 2025-04-28 | Row lineage enabled for all v3 tables (#12593); default values in UpdateSchema; variant readers and writers (Avro, Parquet); nanosecond timestamps and unknown; geometry and geography types (#12346); partition stats reader and writer; AuthManager API; FileRewritePlanner split; `current-snapshot-id` written as null for v3; HEAD endpoints removed from REST defaults; Spark 3.3 and Hadoop 2 removed |
| 1.9.1 / 1.9.2 | 2025-05-27 / 2025-07-16 | patches |
| 1.10.0 | 2025-09-11 | **Spark 4.0** and **Flink 2.0** support; row lineage in Avro and Arrow readers and preserved on compaction; variant read in Spark; encryption metadata keys; `RowDelta.deleteFile`; `cleanExpiredMetadata` in ExpireSnapshots; dangling-DV propagation and cleanup on rewrite; REST scan-planning models; REST retry hardening (502/504 not retried; 503 on commit means unknown state) |
| 1.10.1 / 1.10.2 | 2025-12-22 / 2026-05-18 | patches |
| 1.11.0 | 2026-05-19 | **Java 11 dropped** (#14400); Spark 4.1 and Flink 2.1; REST scan planning in client and Spark; freshness-aware table loading; registerView; Idempotency-Key; File Format API (#12774) plus Parquet, ORC and Arrow FormatModels; v4 foundational types; **duplicate DVs for one data file detected and merged before commit** (#15006); sequence-number conflicts retryable under concurrent commits (#15126); v2 deletes validated against a concurrent format upgrade (#16146); "Load snapshot after it has been committed to prevent accidental cleanup of files" (#15511); expire snapshots `cleanupMode`; encryption `kms-type` aws/azure/gcp; Flink writes DVs; Spark writes shredded variant |
| 1.12.0 | 2026-09-29 | Spark 3.4 and Flink 2.0 removed; **Flink 2.2 and 2.3** added; **position delete files with row data removed** (#17706); Parquet and Avro geometry/geography; variant shredding fixes; v4 manifest reader and writer layout; fixes "commit validation when a data file has multiple DVs across snapshots" (#17764); "Read delete manifests correctly during snapshot expiration" (#17763); "Fail scans when position deletes or DVs don't match the data file partition" (#16957); `RepairTable` action interface; a scan-based action to remove dangling delete files; Flink `ConvertEqualityDeletes` (equality deletes to DVs); Spark Hilbert-curve rewrite; Flink view support; catalog labels; REST POST retries with Idempotency-Key |

Key table-property defaults that a port should mirror (https://raw.githubusercontent.com/apache/iceberg/main/docs/docs/configuration.md):

- `format-version` = 2. This has been the default since Java 1.4.0.
- `write.delete.mode`, `write.update.mode` and `write.merge.mode` = `copy-on-write`. `merge-on-read` is allowed on v2 and later.
- `write.*.isolation-level` = `serializable` (alternative: `snapshot`).
- `write.delete.granularity` = `partition`.
- `write.target-file-size-bytes` = 512 MB.
- `write.distribution-mode` is not set at table level; engine defaults apply.
- `commit.retry.num-retries` = 4, `min-wait-ms` = 100, `max-wait-ms` = 60000, `total-timeout-ms` = 1800000.

### 2.2 PyIceberg (apache/iceberg-python)

- Latest is **0.12.0**, published 2026-09-01. The previous release was 0.11.1 on 2026-03-03, and 0.11.0 on 2026-02-10 (https://pypi.org/pypi/pyiceberg/json).
- 0.12.0 highlights (https://github.com/apache/iceberg-python/releases/tag/pyiceberg-0.12.0):
  - Reads DVs from Puffin, including a Spark interop test. **Writing DVs is not mentioned.**
  - REST view support: `create_view`, `load_view`, `register_view`.
  - REST pagination; `loadCredentials` and storage-credential support.
  - Glue support for S3 Tables federated databases.
  - Commit retries with concurrency validation.
  - Geometry and geography types.
  - Incremental append scan.
  - Config rename from `rest-scan-planning-enabled` to `scan-planning-mode`.
  - Python 3.14.
- Capabilities from the docs (https://raw.githubusercontent.com/apache/iceberg-python/main/mkdocs/docs/api.md and https://raw.githubusercontent.com/apache/iceberg-python/main/mkdocs/docs/configuration.md):
  - Native catalogs: REST, SQL, Hive, Glue, DynamoDB, and in-memory.
  - Write operations: append, overwrite (including `overwrite_filter`), `delete(delete_filter)` (rewrites or drops whole files), upsert, `add_files`, schema, partition and sort evolution, branches and tags, `maintenance.expire_snapshots()`, views, and statistics.
  - Isolation levels: `write.delete.isolation-level` / `write.update.isolation-level` (serializable or snapshot).
  - Arrow-based I/O. Parquet only for data.
- Delete-file writes: the implementation-status matrix shows PyIceberg **does not write position or equality deletes** and does not read equality deletes (https://raw.githubusercontent.com/apache/iceberg/main/site/docs/status.md). That matrix may be stale. In practice PyIceberg deletes are copy-on-write.

### 2.3 iceberg-rust

- Latest is **0.10.1**, published 2026-08-01 on crates.io. Earlier releases: 0.10.0 on 2026-07-21, 0.9.1 on 2026-05-06, 0.9.0 on 2026-03-19, 0.8.0 on 2026-01-19 (https://crates.io/api/v1/crates/iceberg). The CHANGELOG gives slightly earlier tag dates.
- Changelog highlights (https://raw.githubusercontent.com/apache/iceberg-rust/main/CHANGELOG.md):
  - "Support for V3 Metadata"; v2 to v3 upgrade.
  - Reads position and equality deletes; DV bulk insertion.
  - Transactions with retry: FastAppend, UpdateStatistics, ReplaceSortOrder, UpgradeFormat, location and properties updates.
  - **ExpireSnapshotsAction** (0.10).
  - `purge_table`; AES-GCM encryption with KMS (0.10).
  - DataFusion integration: CREATE and DROP TABLE, INSERT, and pushdowns. Non-append inserts are rejected.
  - Unreleased: time travel by timestamp.
- Crates: catalogs REST, Glue, HMS, SQL and S3Tables, plus `catalog-loader`; OpenDAL storage (https://raw.githubusercontent.com/apache/iceberg-rust/main/README.md).
- The status matrix says: no rewrite, overwrite or delete-files operations; no position-delete writes; writes equality deletes (https://raw.githubusercontent.com/apache/iceberg/main/site/docs/status.md).
- Users include Lakekeeper, RisingWave, Databend and DataFusion Comet.

### 2.4 iceberg-go

- Latest is **v0.7.0**, tagged 2026-09-28 (https://proxy.golang.org/github.com/apache/iceberg-go/@v/v0.7.0.info). v0.6.0 was 2026-05-21.
- v0.7.0 (https://github.com/apache/iceberg-go/releases):
  - Reads DVs and writes DVs for partitioned v3 tables; DV cross-client compliance tests.
  - Row lineage preserved through CoW rewrites.
  - PurgeTable; REST endpoint negotiation.
- v0.6.0:
  - Write-default and initial-default; nanosecond timestamps.
  - Equality delete read and write; **RowDelta API**.
  - Bin-pack compaction; orphan-file cleanup.
  - **REST multi-table transactional commit**; vended-credential refresh; RegisterView.
- README (https://raw.githubusercontent.com/apache/iceberg-go/main/README.md):
  - Write operations: append, rewrite, overwrite, CoW delete, position delete, equality delete, row delta.
  - Catalogs: REST, Hive, Glue, SQL, Hadoop.
  - REST scan planning in local, remote and auto modes. Idempotency keys are UUIDv7 with retries on 408, 429 and 5xx.
  - Views on REST, Hive and SQL.
  - **Probably the most complete non-Java implementation on the write and state-management side, and a good structural model for a .NET port.**

### 2.5 iceberg-cpp

- Latest is **0.4.0**, released 26 Sep (year not shown; 2026 inferred from order). Earlier: 0.3.0 on 10 Jun, 0.2.0 on 26 Jan, 0.1.0 on 15 Sep (2025) (https://github.com/apache/iceberg-cpp/releases).
- 0.4.0:
  - Merge append, row delta, OverwriteFiles, RewriteFiles, DeleteFiles.
  - Parallel manifest writes.
  - SigV4; per-table FileIO from vended credentials.
  - v3 type definitions, default values, row-lineage snapshot fields.
- 0.3.0: DataWriter, position and equality delete writers, a roaring-based position delete index, transaction retry, incremental and changelog scans.
- Requirements: C++23 (with a C++20 compatibility layer) and an Arrow dependency. Catalogs: REST, plus optional HMS and SQL (SQLite, Postgres, MySQL) (https://raw.githubusercontent.com/apache/iceberg-cpp/main/README.md).
- The status matrix still shows C++ without nanosecond timestamps, variant or uuid (may be stale).

### 2.6 Cross-implementation matrix (official, possibly lagging)

https://raw.githubusercontent.com/apache/iceberg/main/site/docs/status.md shows:

- Only Java supports ORC and Puffin as file formats, and geometry and geography.
- Go and Java support expire and manage snapshots.
- Only Java and Go write position deletes.

Treat it as a lower bound; the 2.2–2.5 release notes show newer progress (for example Rust ExpireSnapshots, and DVs in Go and PyIceberg).

---

## 3. Existing .NET / C# Iceberg efforts

| Effort | Status | Source |
|---|---|---|
| **EngineeredWood.Iceberg** (NuGet 0.3.0, 2026-08-04; 0.1.0 on 2026-07-20) | Experimental, pre-1.0. Parses table metadata v1–v3, reads manifests and manifest lists (Avro), scan planning with predicate pruning, file-system and in-memory catalogs. Manifest writing is incomplete: no bounds and no partition tuples. Transforms are declarative only. README: "cannot yet round-trip a table that other Iceberg engines will accept… Do not use it as a writer." No delete-file support mentioned. Targets net8.0, net10.0 and netstandard2.0. Apache-2.0. Repo has 2 stars. Sibling packages cover Parquet, ORC, Avro, Delta, Lance and Vortex, plus AWS, Azure and GCS I/O; the README says they are built with Claude Code. | https://www.nuget.org/packages/EngineeredWood.Iceberg ; https://github.com/clast-project/engineered-wood |
| NuGet "iceberg" search (26 hits) | Apart from EngineeredWood, no Apache Iceberg table-format client. Other hits are unrelated: IcebergDb (vector DB), Iceberg.Subzero (tokenization), and PipelineZ (`pz`, a DuckDB-based ETL engine that may use the DuckDB iceberg extension; **unverified**). | https://www.nuget.org/packages?q=iceberg |
| "IcebergSharp", "Iceberg.Net" | No matching NuGet packages or repos found. | web search; NuGet search |
| GitHub search "Apache Iceberg .NET" | Only forks of apache/iceberg. `david-acker/Iceberg` is an unrelated C# dependency mapper. | web search |
| **DuckDB.NET** (`DuckDB.NET.Data.Full` 1.5.6.1, 2026-10-15) | ADO.NET binding for DuckDB 1.5.x. The iceberg extension is loaded through SQL (`INSTALL/LOAD iceberg`), which gives .NET a working Iceberg read/write path through DuckDB (see 4.4). Extension availability within DuckDB.NET is **unverified** but follows from normal DuckDB extension loading. | NuGet registration API |
| **.NET for Apache Spark** (dotnet/spark) | Reported archived in 2024 *(secondary)*; upstream status **unverified**. Not a viable base. | https://www.red-gate.com/simple-talk/wp-json/wp/v2/posts/85005 |
| Delta analogue: `DeltaLake.Net` 0.33.0 | Rust (delta-rs) FFI wrapper. A precedent for an "FFI over iceberg-rust" design option. | NuGet flat-container index |
| Oracle OCI .NET SDK `UpdateRestIcebergCatalogDetails` | GoldenGate configuration model, not an Iceberg client. | https://docs.oracle.com/en-us/iaas/tools/dotnet/latest/api/Oci.GoldengateService.Models.UpdateRestIcebergCatalogDetails.html |

**Conclusion:** no production-grade native .NET Iceberg library exists as of 2026-10-22. The gap is real.

---

## 4. Engines and services as behavioural references

### 4.1 Spark (Iceberg runtime 1.12.0; Spark 3.5, 4.0 and 4.1)

Sources:

- https://raw.githubusercontent.com/apache/iceberg/main/docs/docs/spark-procedures.md
- https://raw.githubusercontent.com/apache/iceberg/main/docs/docs/spark-writes.md
- Maven: `iceberg-spark-runtime-4.1_2.13` 1.12.0

Procedures:

- Snapshot management: `rollback_to_snapshot`, `rollback_to_timestamp`, `set_current_snapshot`, `cherrypick_snapshot`, `publish_changes` (WAP), `fast_forward`.
- Maintenance: `expire_snapshots`, `remove_orphan_files`, `rewrite_data_files`, `rewrite_manifests`, `rewrite_position_delete_files`.
- Migration: `snapshot`, `migrate`, `add_files`, `register_table`.
- Other: `ancestors_of`, `create_changelog_view`, `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path`.

`expire_snapshots`:

- `older_than` defaults to 5 days ago; `retain_last` defaults to 1.
- Other arguments: `snapshot_ids`, `clean_expired_metadata`, `max_concurrent_deletes`, `stream_results`.
- Outputs: counts of deleted data, position-delete, equality-delete, manifest, manifest-list and statistics files.

`rewrite_data_files`:

- Strategies: `binpack` (default), `sort`, `zorder(...)`, and `hilbert(...)` (1.12, Spark 4.1).
- Key options (defaults):

| Option | Default |
|---|---|
| `target-file-size-bytes` | 512 MB |
| `min-file-size-bytes` | 75% of target |
| `max-file-size-bytes` | 180% of target |
| `min-input-files` | 5 |
| `max-file-group-size-bytes` | 100 GB |
| `max-file-group-input-files` | (no default stated) |
| `max-concurrent-file-group-rewrites` | 5 |
| `partial-progress.enabled` | false |
| `partial-progress.max-commits` | 10 |
| `use-starting-sequence-number` | true (critical for not conflicting with concurrent deletes) |
| `delete-file-threshold` | (no default stated) |
| `delete-ratio-threshold` | 0.3 |
| `remove-dangling-deletes` | false |
| `rewrite-job-order` | (no default stated) |
| `output-spec-id` | (no default stated) |
| `max-files-to-rewrite` | (no default stated) |
| `cache-delete-files` | (no default stated) |

Distribution, write modes and isolation:

- `write.distribution-mode`: none, hash or range. Since 1.2.0 Iceberg asks Spark for `hash` by default. Spark ignores it for CTAS/RTAS before 3.5.
- MoR vs CoW: per-operation `write.{delete,update,merge}.mode`; the default is CoW. On v3, MoR writes DVs (1.8+).
- Isolation: `serializable` (default) or `snapshot`, per operation.

### 4.2 Flink (Iceberg 1.12.0 supports Flink 2.1, 2.2 and 2.3)

Release notes: https://raw.githubusercontent.com/apache/iceberg/main/site/docs/releases.md

- IcebergSink and DynamicIcebergSink. Writes DVs (1.11).
- Equality-delete upserts, plus the `ConvertEqualityDeletes` maintenance task that converts them into DVs (1.12). This matters because v4 bans new equality deletes.
- TableMaintenance with a coordinator lock: expire snapshots, rewrite data files.
- Views in Flink SQL (1.12).

### 4.3 Trino (latest Maven release 484, 2026-10-16)

Source: https://raw.githubusercontent.com/trinodb/trino/master/docs/src/main/sphinx/connector/iceberg.md

- "Supports Apache Iceberg table spec versions 1 and 2. Support for format version 3 is experimental."
- `format_version` can be 1, 2 or 3; the default is 2.
- On v3, row-level UPDATE and DELETE use **deletion vectors**. `VARIANT` requires v3.
- `add_files` and `add_files_from_table` are not supported on v3. Writing to encrypted tables is not supported.
- v2 row-level deletes write **position delete files** (merge-on-read).
- Commit retries: `max_commit_retry`, default 4.

### 4.4 DuckDB iceberg extension

Sources:

- https://raw.githubusercontent.com/duckdb/duckdb-iceberg/main/README.md
- Search summaries of https://duckdb.org/2026/05/29/new-iceberg-features and https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg

Behaviour:

- Marked experimental. Reads and writes Iceberg tables and attaches REST catalogs.
- CI tests against the REST fixture, Gravitino, Lakekeeper, Nessie and Polaris 1.4.x.
- Writes: INSERT; UPDATE, DELETE and MERGE INTO through a REST catalog *(search summary)*.
  - **Merge-on-read only; copy-on-write not supported.**
  - v2 writes positional delete Parquet files; v3 writes Puffin DVs.
  - Row lineage on v3 (v1.5.3, 2026-05-29).
- PR duckdb/duckdb-iceberg#1584 (merged 2026-10-14 per search) allows multiple matched actions in a MERGE and dedupes DVs per data file (https://github.com/duckdb/duckdb-iceberg/pull/1584).
- README note on v3 lineage: rows inserted inside a transaction have NULL `_row_id` until commit.

### 4.5 AWS

**Athena:** **does not support v3** *(search summary of AWS Prescriptive Guidance)*. The v3 matrix lists "Amazon Athena (Trino): No". A third-party source says Athena rejects v3 tables on `format-version` alone.

- Sources: https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/table-spec-v3.html and https://hidekazu-konishi.com/entry/apache_iceberg_v3_on_aws.html
- Athena supports v2 MERGE, UPDATE and DELETE (position deletes), OPTIMIZE and VACUUM. This is from **prior knowledge, unverified here**; docs.aws.amazon.com was unreachable.
- **Implication: default new tables to v2 for Athena compatibility.**

**AWS v3 support across other services:**

- AWS announced v3 DV and row-lineage support in Nov 2025 for EMR 7.12 Spark, Glue, SageMaker notebooks, S3 Tables and the Glue Data Catalog (https://aws.amazon.com/about-aws/whats-new/2025/11/aws-apache-iceberg-v3-deletion-vectors-row-lineage).
- S3 Tables then added **all v3 data types** (~Sep/Oct 2026; Parquet only, upgrade is one-way) (https://aws.amazon.com/blogs/aws/amazon-s3-tables-now-support-all-apache-iceberg-v3-data-types/).
- Redshift v3 support: **unverified**.

**S3 Tables IRC endpoint:**

- Endpoint: `https://s3tables.<region>.amazonaws.com/iceberg`. Uses SigV4 with signing name `s3tables`.
- AWS recommends this endpoint for single-table-bucket access, and the **Glue IRC endpoint** for unified governance (Lake Formation).
- Source *(search summary)*: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html
- The Glue endpoint (`https://glue.<region>.amazonaws.com/iceberg`, SigV4 name `glue`) is from **prior knowledge, unverified here**.
- Java 1.12 changelog item: "AWS: Use assumed-role credentials for REST SigV4 signing".

### 4.6 Snowflake / Apache Polaris

Snowflake:

- **Iceberg v3 GA on 2026-05-07.** Covers geography, geometry, nanosecond timestamps, variant, default values, DVs and row lineage.
- External engines can read v3 through the Horizon IRC. **External writes to v3 are not yet supported.**
- External-engine writes to Snowflake-managed (non-v3) tables are in preview as of 2026-03-16.
- Polaris is integrated into Horizon (GA 2026-02-06).
- Sources: https://docs.snowflake.com/release-notes/2026/other/2026-05-07-iceberg-v3-ga ; https://docs.snowflake.com/en/release-notes/2026/other/2026-03-16-tables-iceberg-query-using-external-query-engine-snowflake-horizon-writes-feature

Apache Polaris:

- Latest Maven artifact `polaris-core` **1.8.0**, published ~2026-09-28 (https://repo1.maven.org/maven2/org/apache/polaris/polaris-core/maven-metadata.xml).
- Graduation to an Apache top-level project (Feb 2026): **unverified**; secondary source only.
- 1.8 feature set: **unverified**.

### 4.7 Unity Catalog (Databricks and OSS)

- IRC endpoint `/api/2.1/unity-catalog/iceberg-rest`. External engines can create and write managed Iceberg tables.
- Managed Iceberg GA. v3 status is inconsistent across sources (GA announcement vs. "Public Preview DBR 18.0+" vs. "Beta DBR 17.3+"): **unverified**.
- Sources: https://community.databricks.com/t5/announcements/unity-catalog-and-the-next-era-of-apache-iceberg/td-p/157927 ; https://docs.databricks.com/gcp/en/iceberg ; https://docs.azure.cn/en-us/databricks/external-access/iceberg
- OSS `io.unitycatalog:unitycatalog-server` **0.7.0** (Maven, ~2026-10-11).

### 4.8 Nessie

- `nessie-client` **0.109.0** (Maven, ~2026-10-08), so still releasing.
- Supports the IRC with git-like branches. Java Iceberg 1.12 bumped Nessie to 0.108.8.
- No official "maintenance mode" statement found.
- Sources: https://repo1.maven.org/maven2/org/projectnessie/nessie/nessie-client/maven-metadata.xml ; https://developer.dremio.com/using-nessies-rest-catalog-support-for-working-with-apache-iceberg-tables

### 4.9 Lakekeeper

- Rust IRC built on iceberg-rust.
- Latest **v0.14.1**, 2026-10-16 (also v0.13.7 the same day). Fixes S3 remote signing for Iceberg 1.12+ clients (bulk deletes, ranged reads).
- v0.14.0 (2026-10-07): authorization overhaul (OpenFGA grants, governance tags), namespace move/rename, and `scan-planning-mode` advertised on table responses.
- Source: https://github.com/lakekeeper/lakekeeper/releases

### 4.10 Apache Gravitino

- `gravitino-api` **1.3.1** (Maven, ~2026-09-30). 1.3.0 was released 2026-06-24.
- 1.3.0 IRC features: federated IRC, nested multi-level namespaces, vended-credential refresh, freshness-aware loading, async cleanup. Ships a separate `gravitino-iceberg-rest-server` binary.
- The 1.0.x docs listed multi-table transactions, pagination, scan planning and load-credentials as not implemented. Status in 1.3: **unverified**.
- Sources: https://gravitino.apache.org/blog/gravitino-1-3-0-release-notes ; https://gravitino.apache.org/docs/1.0.0/iceberg-rest-service

**Interop test targets for a .NET port:**

- `apache/iceberg-rest-fixture` Docker image, as used by iceberg-go and DuckDB CI.
- Spark 4.x for v3 cross-checks.
- Lakekeeper, Polaris, Nessie and Gravitino, as DuckDB CI does.

---

## 5. Relevant .NET libraries

Versions and dates come from the NuGet registration and flat-container APIs (https://api.nuget.org/v3/registration5-gz-semver2/{id}/index.json).

| Library | Latest | Date | Notes |
|---|---|---|---|
| **Parquet.Net** (aloneguid/parquet-dotnet) | 6.1.0 (6.1.1-pre.1) | 2026-08-11 | Fully managed read and write; targets net8.0 and net10.0 (https://www.nuget.org/packages/Parquet.Net). **Field-id:** `DataField.FieldId` exists and is written to the Thrift `SchemaElement.field_id` (`SchemaEncoder.cs` line ~454). **`StructField`, `ListField` and `MapField` have no `FieldId`** at master. Iceberg needs IDs on struct, list-element and map key/value groups, so nested schemas would need a patch or fork. Verified by grepping https://raw.githubusercontent.com/aloneguid/parquet-dotnet/master/src/Parquet/Schema/DataField.cs and `StructField.cs`/`ListField.cs`/`MapField.cs`. Variant, geometry and encryption support: **unverified**. |
| **ParquetSharp** (G-Research) | 25.0.1.1 | 2026-10-21 | P/Invoke wrapper over Arrow's parquet-cpp. Targets net8.0, netstandard2.1 and net471, and depends on Apache.Arrow ≥ 18 (https://www.nuget.org/packages/ParquetSharp). **Field-id:** full support. `Column(…, fieldId)`, `PrimitiveNode`/`GroupNode` constructors take `fieldId`, and `Node.FieldId` is readable (https://raw.githubusercontent.com/G-Research/ParquetSharp/master/csharp/Schema/GroupNode.cs). The major version tracks Arrow C++ (v25 likely means Arrow 25; **unverified**). Native binaries: platform matrix **unverified**. |
| **Apache.Avro** (C#, in apache/avro `lang/csharp`) | 1.12.2 | 2026-08-18 | Maintained alongside Java Avro; Iceberg 1.12 bumped to Avro 1.12.2 too. Targets netstandard2.0/2.1 and net8/9/10 (https://raw.githubusercontent.com/apache/avro/main/lang/csharp/README.md). Previous release 1.12.1 on 2025-10-15. Support for Iceberg's manifest needs (field-id attributes via schema properties, logical types, zstd/snappy codecs): needs a spike, **unverified**. |
| **EngineeredWood.Avro / .Orc / .Parquet** | 0.3.0 | 2026-08-04 | Managed Arrow-native readers and writers; experimental (https://www.nuget.org/packages?q=orc+apache). |
| ORC: **ApacheOrcDotNet** | 1.0.0 | 2020-11-06 | Stale. EngineeredWood.Orc is the only recent managed ORC option. **There is no mature .NET ORC library**, so treat ORC as out of scope or read-only. |
| **ZstdSharp.Port** | 0.8.8 | 2026-04-29 | Managed zstd port. |
| **Snappier** | 1.3.1 | 2026-04-28 | Managed Snappy. Iceberg's default Avro manifest codec is deflate/gzip; Parquet data defaults to zstd in recent Java (**unverified** for the Java default; PyIceberg defaults to zstd per its config doc). |
| **Roaring.Net** | 1.4.1 | 2026-05-27 | CRoaring wrapper (native). |
| **Equativ.RoaringBitmaps** | 0.1.6 | 2026-09-24 | Managed implementation; portable-format compatibility **unverified**. |
| **RoaringBitmap** | 0.0.9 | 2017 | Stale. |
| **CRoaring.Net.CrossPlatform** | 0.1.2-dev | 2026-07-19 | Prerelease. |
| DataSketches (Theta): **Clast.Sketches** | 0.1.0 | 2026-08-03 | Claims datasketches-java-compatible serialization for Theta and HLL (https://www.nuget.org/packages?q=datasketches). Very new and low adoption. No official Apache DataSketches .NET port was found. |
| **Apache.Arrow** (C#, now in apache/arrow-dotnet) | 23.0.0 | 2026-05-06 | Previous 22.1.0 on 2025-10-17. The repo was split out of apache/arrow (https://raw.githubusercontent.com/apache/arrow-dotnet/main/README.md). |
| **DuckDB.NET.Data.Full** | 1.5.6.1 | 2026-10-15 | See §3. |

Notes on the Roaring choice:

- The DV wire format is Roaring64 "portable" (§1.2): an 8-byte LE bitmap count, then per-key 32-bit portable bitmaps.
- Writing a small managed serializer for only that format, run containers included, may be simpler than taking a native dependency.
- Reference implementations: Java `RoaringPositionBitmap` and iceberg-cpp's roaring-based position bitmap.

---

## 6. Takeaways for the .NET design docs

1. **Scope:** v1, v2 and v3 read; v2 write first (Athena has no v3); v3 write (DVs and row lineage) second. v4 is not adopted; keep the manifest layer pluggable.
2. **Commit protocol:** follow Java and Go.
   - Requirements and updates sent to IRC `updateTable` and `transactions/commit`.
   - `Idempotency-Key` on POSTs.
   - 409 means conflict, so retry from refreshed metadata.
   - 5xx (502/503/504) means **CommitStateUnknown**: do not clean up files.
   - Retry property defaults: 4 retries, 100 ms – 60 s backoff, 30 min total.
   - Validate concurrent DV and duplicate-DV cases (Java 1.11 #15006, 1.12 #17764).
3. **Parquet:** ParquetSharp is the safer choice for spec-correct field IDs on nested types. Parquet.Net needs group-level field-id support added.
4. **Reference implementations to copy structure from:** iceberg-go (most complete non-JVM write path: RowDelta, DVs, multi-table commit, scan planning) and Java 1.12. Use the REST fixture and Spark 4 for interop tests.
5. **Existing .NET work** is limited to EngineeredWood.Iceberg (metadata-only, experimental), plus DuckDB.NET plus the iceberg extension as an indirect path.
