# Other implementations and reference engines

Two kinds of prior art: **ports** (libraries implementing the spec in another language — what we are building)
and **engines** (Spark, Trino, Flink, DuckDB, Athena — whose *behaviour* defines what users expect and what
our files must survive). Versions are as of 2026-10-22; sources in
[research/ecosystem-2026-10.md](research/ecosystem-2026-10.md).

## Language ports

| Port | Latest | Strengths | Gaps | What we take from it |
|---|---|---|---|---|
| **Java** (`apache/iceberg`) | 1.12.0 (2026-09-29) | The reference. Everything: v3 DVs, row lineage, REST client and server, all catalogs, ORC, Puffin, views, v4 prototypes | JVM | Semantics of record: `TableMetadata.Builder`, `SnapshotProducer`, `MergingSnapshotProducer` validation, `GlueTableOperations`, retry/unknown-state rules. Ships the **REST fixture** we test against |
| **PyIceberg** | 0.12.0 (2026-09-01) | Pragmatic, Arrow-native; REST/Glue/SQL/Hive/DynamoDB catalogs; append/overwrite/delete/upsert; reads DVs | Deletes are copy-on-write only; no position/equality delete writes | Our **oracle**: same team, independent code. Also the precedent for an in-house Avro codec. Its bucket transform delegates to Rust `pyiceberg-core`, so agreeing with PyIceberg means agreeing with iceberg-rust too |
| **iceberg-go** | v0.7.0 (2026-09-28) | Most complete non-JVM write path: RowDelta, position + equality + DV writes, multi-table commit, scan planning, compaction, orphan cleanup | Smaller catalog set than Java | **Structural model** for the .NET write path (a statically-typed, non-JVM port that has already solved RowDelta and DVs) |
| **iceberg-rust** | 0.10.1 (2026-08-01) | v3 metadata, reads all deletes, FastAppend, ExpireSnapshots, DataFusion integration; REST/Glue/HMS/SQL/S3Tables catalogs | Limited write operations (no overwrite/delete-files) | Option: an FFI wrapper (precedent: `DeltaLake.Net` over delta-rs). Rejected for now — native packaging per RID, and the write path we need most is the one Rust lacks |
| **iceberg-cpp** | 0.4.0 (2026-09) | Row delta, overwrite, rewrite, delete; roaring position index; SigV4 | C++23, Arrow dependency; young | Reference for a roaring-based position-delete index |
| **.NET** | — | — | **No production-grade library exists.** EngineeredWood.Iceberg 0.3.0 is experimental, metadata-only, and its README says not to use it as a writer | Confirms the gap; worth re-checking before each milestone |

Indirect .NET options that exist today, and why they are not the answer on their own:

| Option | Works for | Why not sufficient |
|---|---|---|
| DuckDB.NET + DuckDB `iceberg` extension | reads; MoR writes through REST catalogs | Merge-on-read only; REST-catalog-centric (Glue via REST endpoint only); embeds a native engine in every service; experimental |
| Athena from C# (`StartQueryExecution`) | any SQL Athena supports | Per-query latency and cost, Athena-side concurrency limits, no file-level control |
| Spark .NET | — | Archived upstream (reported; unverified) |

## Engines as behavioural references

The user-facing contract of Iceberg is defined as much by engines as by the spec. Our files must be
readable by all of them and our commits must not violate assumptions they make.

### Spark (Iceberg Spark runtime) — the behavioural reference

Spark's runtime is where most Iceberg semantics were first defined, so it is the reference whenever the spec
is silent on *operational* behaviour:

| Area | Spark behaviour | Implication for the port |
|---|---|---|
| Row-level operations | `write.{delete,update,merge}.mode` = copy-on-write by default; merge-on-read writes position deletes (v2) or DVs (v3) | Implement CoW first (done); MoR must produce DVs on v3, never new position-delete files |
| Isolation | `serializable` by default, `snapshot` optional, per operation | Overwrite/delete validation must default to serializable (M2) |
| Distribution | `write.distribution-mode` none/hash/range; Iceberg asks for `hash` | Writers should cluster by partition before writing to avoid one-file-per-row-group explosions — `TableWriter` groups by partition tuple |
| `expire_snapshots` | `older_than` default now − 5 days, `retain_last` 1, refs always kept | Mirrored in `Maintenance.ExpireSnapshotsAsync` |
| `rewrite_data_files` | bin-pack default, target 512 MB, `use-starting-sequence-number=true` | When we compact, commit with the *starting* sequence number so concurrent deletes still apply |
| `rewrite_manifests`, `remove_orphan_files`, `rewrite_position_delete_files` | | Maintenance roadmap; Athena `OPTIMIZE`/`VACUUM` cover Glue tables meanwhile |
| Snapshot procedures | `rollback_to_snapshot`, `cherrypick_snapshot`, `fast_forward`, WAP `publish_changes` | All expressible as `set-snapshot-ref` updates on our state machine |
| Migration | `add_files`, `register_table`, `migrate`, `snapshot` | `RegisterTableAsync` done; `add_files` (adopt existing Parquet) is cheap to add and useful for backfills |

Spark 4.x is the cross-check target for v3 features we cannot verify with PyIceberg (DV writes, variant).
It is not in the emulation stack yet because of image size; see [regression-suite.md](regression-suite.md#future-tiers).

### Others

| Engine | Relevant behaviour |
|---|---|
| **Athena** (Trino-based, engine v3) | **Our production reader.** Iceberg v2 only — rejects v3 tables. Writes position deletes for DELETE/UPDATE/MERGE (MoR). `OPTIMIZE`/`VACUUM` for maintenance. Reads Glue `metadata_location`. ⇒ default to v2; MoR read support is required before we can read tables Athena has mutated (`AthenaTests` pins the current refusal) |
| **Trino** (484) | v1/v2 supported, v3 experimental; v3 row-level ops use DVs; commit retries default 4 |
| **Flink** (Iceberg 1.12: Flink 2.1–2.3) | Streaming upserts via equality deletes; `ConvertEqualityDeletes` turns them into DVs (because v4 bans new equality deletes) |
| **DuckDB** `iceberg` | MoR-only writes (position deletes on v2, DVs on v3), row lineage on v3; CI-tests against REST fixture, Polaris, Nessie, Lakekeeper, Gravitino — the same matrix we should grow into |
| **Snowflake** | v3 GA (2026-05); external engines can read v3 via Horizon IRC but cannot write v3 yet |

## Strategy this implies

1. **Spec semantics from Java, structure from iceberg-go, verification against PyIceberg + the Java REST
   fixture + Athena.** Every behaviour visible to another engine gets a cross-engine test.
2. **v2 first** (Athena), v3 next (lineage done; DVs next), keep manifest I/O pluggable for v4.
3. **Copy-on-write before merge-on-read** for writes; **merge-on-read reads before MoR writes**, because
   Athena already produces position deletes on our tables.
