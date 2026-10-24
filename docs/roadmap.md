# Roadmap

Two tracks: **delivery** (what C# producers can do with Iceberg tables) and **verification** (how we know it is
correct, and stays correct). Delivery is ordered by what unblocks production use cases soonest; each milestone ends with
its rows in [spec-coverage.md](spec-coverage.md) flipped and tests in the tier that proves them. Verification is the
13 paths agreed as "how do we make sure the capabilities are correct", with what is built, what is left, and what
needs action outside this repository.

## Delivery milestones

| Milestone | Outcome | Contents | Proven by |
|---|---|---|---|
| **M0 — foundations** ✅ | C# can create, append to, overwrite and read flat v2/v3 tables in Glue and REST catalogs, interoperably | spec model, transforms, state machine, Avro, Parquet (flat), Glue/REST/in-memory, emulation + regression suite | L0, L1/L2 |
| **M1 — production-grade append** | A C# producer can safely own appends to an Athena-queried Glue table | ✅ `UpdateSchema` API, merge-append, `DropTable(purge)` deleting data, SigV4 (Glue/S3 Tables REST), JDBC catalog, `add_files`-style adoption (`ParquetImport`), S3 throttling retries, serializable `Overwrite`/`DeleteFiles` validation, decimals up to precision 38, `fixed(L)`, NuGet packages (smoke-tested in CI); Idempotency-Key retries (when the server advertises a key lifetime); nested types (struct/list/map Parquet I/O and schema evolution, checked against Spark both ways and PyIceberg); netstandard2.0 builds of Core and Avro (tested by `Iceberg.Net.NetStandard.Tests`, consumed by a netstandard2.0 library in the package smoke test); remaining: typed producers via Parquet.SourceGenerator once its Iceberg profile lands ([gaps](parquet-sourcegenerator.md)) | Athena L3 green in a sandbox account (needs AWS, below); soak against Glue, REST, Polaris and JDBC ✅ in emulation |
| **M2 — merge-on-read reads** ✅ | Read any table Athena/Spark/Trino have mutated | position deletes, DVs, Puffin, delete applicability, v2→v3 DV converter, NDV statistics, Spark- and Trino-written MoR tables, Spark v1 and schema/partition-evolved tables, v3 row lineage read, filter expressions with manifest/partition/metrics pruning | Spark + Trino + PyIceberg ✅; Athena-written DELETE/MERGE tables (AWS run pending) |
| **M3 — row-level writes** ✅ | C# upserts / deletes without Athena | insert, upsert by key, group replacement, delete by key/predicate/expression with copy-on-write, position/DV and equality deletes (also across partition specs); single-snapshot RowDelta; re-plan-on-conflict serializability ([write-modes.md](write-modes.md)); `_row_id` preserved on v3 rewrites; key- and filter-based file pruning; Arrow `RecordBatch` API (`Iceberg.Net.Arrow`) | Spark, Trino and PyIceberg read our output ✅ (PyIceberg also our Arrow output); Athena (AWS pending) |
| **M4 — maintenance & breadth** | Self-sufficient operations | ✅ snapshot expiry with file deletion, orphan-file cleanup, bin-pack and sort compaction (lineage kept), sorted writes (`ReplaceSortOrderAsync`; files record `sort_order_id`), merge-append and manifest rewrite, multi-table transactions (REST, JDBC), views (REST, JDBC V1 layout, in-memory; Trino reads ours and we read its); server-side scan planning (`RestCatalog.PlanTableScanAsync` + `ServerSideScan`; checked end to end against Gravitino and against plans Iceberg Java makes of Spark's tables and ours) | Spark reads compacted, merged, evolved and expired tables ✅ |


### Next-phase design (phases A–G)

These were designed together and built in parallel on one shared read contract (`ScanRequest`) and one UPDATE/MERGE
shape.

| Phase | Scope | State |
|---|---|---|
| **A. Conflict hardening** | Serializable overwrites against compaction, concurrent deletes and expiry; interleaving suite | ✅ |
| **B. UPDATE and MERGE** | `UpdateAsync`, `MergeAsync` (all clauses, cardinality, v3 lineage), Spark MERGE equivalence | ✅ [update-merge.md](update-merge.md) |
| **C. Read path** | Projection by field ID, row-group pruning, streaming, time travel with the snapshot's schema, metadata counts | ✅ [read-path.md](read-path.md) |
| **D. Snapshot management** | Branches/tags, rollback, cherry-pick, fast-forward, branch writes (WAP), per-ref retention, metadata tables, change feed, partial-progress compaction, position-delete rewrite | ✅ [snapshot-management.md](snapshot-management.md) |
| **E. Typed layer** | `Iceberg.Net.Linq`: mapping by field ID, LINQ provider, typed writes in EF Core's bulk shape | ✅ [linq.md](linq.md) |
| **F. EF Core provider** | Non-relational provider over phase E: model, queries, `ExecuteUpdate`/`ExecuteDelete`, `SaveChanges` | ✅ [efcore.md](efcore.md) (`SaveChanges` across tables is one catalog transaction where the catalog has them; table by table on Glue, or refused with `RequireAtomicSaveChanges`) |
| **G. Ecosystem** | DuckDB oracle, OpenTelemetry, Spark procedure helpers and job submission, Athena helpers, a REST catalog server | 🟡 see below |

Phase G, piece by piece:

| Piece | State | Where |
|---|---|---|
| OpenTelemetry traces and metrics (scans, commits with attempts/retries/outcome, catalog calls) | ✅ | [telemetry.md](telemetry.md), `IcebergTelemetry`, `TelemetryTests` |
| Spark procedure SQL (rewrite data / position deletes / manifests, expire, orphans, snapshot and WAP procedures, changelog view, …) | ✅ run through Spark 4 + Iceberg 1.12 | [spark-and-athena.md](spark-and-athena.md), `Iceberg.Net.Spark`, `SparkProcedureOracleTests` |
| Job submission: EMR Serverless and Glue `StartJobRun` of a generated script | 🟡 against faked SDK clients; a real run needs AWS | `EmrServerlessSqlJobRunner`, `GlueSqlJobRunner` |
| Spark Connect | ⬜ not planned (no .NET client) | [spark-and-athena.md](spark-and-athena.md#running-a-script-on-emr-serverless-or-glue) |
| Athena SQL (CREATE TABLE … `table_type`=ICEBERG, OPTIMIZE, VACUUM, properties, time travel) and runner | 🟡 SQL unit-tested; real Athena with the AWS run (#4) | `Iceberg.Net.Athena` |
| DuckDB oracle | ✅ reads our CoW, position-delete, equality-delete and DV tables; skips where the extension cannot load | `interop/duckdb_oracle.py`, `DuckDbCrossEngineTests` |
| REST catalog server (ASP.NET Core, SQL/in-memory stores, bearer/OAuth, multi-table transactions, server-side scan planning, role grants, STS credential vending, `Idempotency-Key`) | ✅ our client, PyIceberg and Spark against it (both plan scans through it); vending against moto's STS | [rest-server.md](rest-server.md), `Iceberg.Net.RestServer` |

Phase G progress: the REST catalog server is built ([rest-server.md](rest-server.md)). It runs as the `dotnet-rest`
conformance kind and is checked against PyIceberg and Spark.

Gaps the phases left open:
- The EMR Serverless, Glue and Athena runners are tested against faked SDK clients; a real run needs the AWS sandbox (#4).
- The REST server has no remote signing or incremental scan planning. Its idempotency keys and plan IDs are per
  process. Credential vending covers S3 through STS only.
- The change feed is Spark-checked only for tables without delete files (Java's changelog scan refuses them); our
  replay test covers the rest.

Closed in the second build-out: nested UPDATE, ranged Parquet reads, delete files after a schema change, cherry-pick
across partition specs, strictly increasing snapshot timestamps, the manifest abstraction, and the three LINQ gaps
(pruned residual deletes, kept field IDs, time-travel pushdown on renamed columns). Since then: nested UPDATE/MERGE
and cherry-pick across partition specs are Spark-checked (one difference fixed: a replayed overwrite no longer records
`replace-partitions`), and Avro data and delete files are streamed in ranged chunks with read-ahead instead of
fetched whole.

Closed since: atomic `SaveChanges` across tables. `TableMutations.StageMergeAsync` / `StageUpdateAsync` stage a
mutation, and `StagedCommit.CommitAllAsync` commits several in one `CommitTransactionAsync`, re-planning all of them
after a conflict ([update-merge.md](update-merge.md#staged-commits-across-tables)).

### Upstream changes to plan for (from the spec-drift check)

`python tools/spec_trace.py drift` compares apache/iceberg `main` with the sections pinned at the 1.12.0 release.
Today it reports the in-progress **v4** work. Two items affect our design:

What each v4 proposal would mean for this library, its status, and the plan are in [iceberg-v4.md](iceberg-v4.md).
In short:

- **v4 allows only deletion vectors for new row-level deletes**: no new equality deletes *and* no new position-delete
  files (existing v2/v3 ones must still be applied). When v4 support lands (we read and write up to v3 today), `DeleteStrategy.EqualityDeletes`
  must refuse v4 tables, and producers that rely on
  equality deletes for cheap upserts need a v4 answer: deletion vectors after a key lookup, which our
  `PositionDeletes` strategy already does.
- **Manifests are being restructured**: a root manifest replaces the manifest list, manifests become Parquet with a
  new `tracked_file` schema, and there are manifest deletion vectors, typed content stats and relative paths. None of
  this affects v1–v3 tables, but it is the biggest change to plan for if we adopt v4.
- **Done ahead of v4** (each pays off or is testable now): strictly increasing snapshot timestamps, the manifest
  abstraction (`ManifestStore`), the typed column-statistics model with the v4 `content_stats` layout (pruning runs on
  it), the `file` type in schemas (refused below v4), and the Mumbling bitmap codec for manifest deletion vectors.

## Verification roadmap

Status: ✅ built and running in CI · 🟡 built, the rest needs something external · ⏳ not started.

| # | Path | Status | What is built | What is left | Needs from outside this repo |
|---|---|---|---|---|---|
| 1 | **Table invariant checker ("fsck")** | ✅ | `TableValidator` (`src/Iceberg.Net.Data/TableValidator.cs`): 40+ checks on metadata, manifests, files, DVs, row-ID ranges, summaries and bounds; deep mode re-reads data. Runs after every BDD scenario, every model-based case, every fault and soak run, on Spark/Trino-written tables, and inside the shadow tool | — | Running it read-only against **production** tables happens through #4 |
| 2 | **Model-based property testing** | ✅ | `ModelBasedTests` (FsCheck): random insert / upsert / group-replace / delete / drop / expire / stats / upgrade+convert sequences vs an in-memory model, with shrinking (`ICEBERG_PBT_RUNS`, `ICEBERG_PBT_SEED`); checks time travel, fsck and v3 row-lineage invariants after every step | Concurrent interleavings are covered by #6 rather than by the model | — |
| 3 | **Spark and Trino as oracles** | ✅ | **Spark 4.0.1 + Iceberg 1.12** (`interop/spark_oracle.py`, `SparkOracle`): committed Spark-written fixtures in L0 (rows, fsck, row lineage, our writes on top), and a live run that reads every table we write: rows, snapshots, delete-file kinds and lineage. **Trino 478** (`--trino`): reads our writes, we read its MoR DELETE/UPDATE/MERGE, mixed histories agree | Trino 478 cannot read deletion vectors (pinned as a characterization test that will flag the upgrade); Spark never writes equality deletes (Flink does), so equality-delete *reading* is cross-checked by Spark and Trino reading ours, not by a third-party writer | — |
| 4 | **Nightly AWS run + read-only shadowing** | 🟡 | `.github/workflows/iceberg-dotnet-aws.yml` (nightly, dormant until configured); `tools/ShadowCompare`: reads a table with this library and with PyIceberg from the **same metadata file**, compares order-independent per-column and per-row fingerprints (`TableFingerprint`), and runs fsck. Proven locally against every type and delete mechanism, and against Spark tables | Athena as a second shadow reference (aggregates only), once a workgroup exists | **An AWS OIDC role and repository variables** (`ICEBERG_AWS_ROLE_ARN`, `ICEBERG_AWS_WAREHOUSE`, `ICEBERG_ATHENA_WORKGROUP`, optional `ICEBERG_SHADOW_TABLES`); an S3 lifecycle rule on the regression prefix. See [regression-suite.md](regression-suite.md#aws-mode) |
| 5 | **Crash and fault injection** | ✅ | `FaultInjectingFileIO` + `FaultInjectionTests`: every write fault before and after it lands, ambiguous catalog swaps, unknown commit outcomes (`CommitStateUnknownException.ServerResponded`): the table is always before-or-after and fsck-clean, and nothing that may have committed is deleted. `ThrottlingTests`: `503 SlowDown` injected under the AWS SDK against moto, cleanly and after the request was applied | — | — |
| 6 | **Concurrency soak** | ✅ | `SoakTests`: 4 writers × random modes against memory v2/v3, Glue, the Java REST fixture and Polaris; per-writer models plus hot keys contended by all writers; `ICEBERG_SOAK_SECONDS` for long runs. It found that the REST fixture's SQLite returned HTTP 500 under contention, and that unknown commit outcomes were never resolved; both are fixed | Hours-long runs on a schedule | AWS soak once #4 is configured |
| 7 | **Reference files from other writers** | ✅ | Committed fixtures from PyIceberg (`fixtures/pyiceberg`) and Spark (`fixtures/spark`: v2 position deletes, v3 DVs + row lineage, a format v1 table, and a table evolved through add/rename/widen/drop/re-add columns and partition evolution), read in the network-free L0 tier, with our writes on top | — | Athena-written fixtures (the L3 test that reads one exists; it needs AWS) |
| 8 | **Decoder fuzzing** | ✅ | `CorruptionTests`: bit flips, truncation, insertion and hostile length fields against metadata JSON, manifest lists, manifests, Puffin, DVs, Roaring and Theta, plus structural JSON damage and required-field removal (`ICEBERG_FUZZ_RUNS`, `ICEBERG_FUZZ_SEED`). Contract (`Corruption`): fail with a format error, never a crash or a silent wrong answer. Found and fixed: unchecked lengths sizing allocations (Avro, Roaring), DV length overflow, negative Theta counts accepted, unchecked Avro union branches and varints, v2/v3 metadata silently defaulting required fields | — (coverage-guided fuzzing added: `tools/fuzz.sh`, SharpFuzz + libFuzzer, 8 targets, weekly in CI; first sweep 2026-10-23, 2 minutes per target, 0.27–2.6M inputs each: no findings) | — |
| 9 | **Mutation testing** | ✅ | Stryker.NET 5 (`dotnet-tools.json`, `mutation/`); Puffin baseline 66.2%, raised by `PuffinEdgeTests` written from the survivors: see [Mutation testing](#mutation-testing) | Extend to the metadata state machine and delete applicability (transforms and expressions: `stryker-core.json`; scheduled weekly) | — |
| 10 | **Spec traceability + drift** | ✅ | `[Spec("…")]` (C#, also an xunit trait: `--filter Spec=row-lineage`) and `@spec:` (Gherkin) tags; `tools/spec_trace.py` builds [spec-traceability.md](spec-traceability.md) from sections pinned at apache-iceberg-1.12.0 (CI fails if it is stale or links to unknown sections); the weekly `drift` job diffs upstream `main` and fails on changes nobody has reviewed yet (`drift --acknowledge` records a review in `docs/spec/acknowledged-drift.json`; the v4 changes above are acknowledged as of 2026-10-23). Today 73/88 implementable table + Puffin sections and 22/23 implemented REST operations have linked tests | The ⬜ rows of the matrix: untagged tests, or real gaps such as default values and path relativization | — |
| 11 | **More REST servers** | ✅ | **Apache Polaris 1.8** (`--polaris`) and **Apache Gravitino 1.3's Iceberg REST service** (`--gravitino`, Docker Hub) run the full catalog conformance suite, views, the soak and the PyIceberg cross-engine tests. Polaris caught a test that registered two live tables at one location, which it rightly refuses. Gravitino is the first emulated server with **server-side scan planning**: it plans tables we write under every delete mechanism (`ServerScanPlanningLiveTests`), and exposed the upstream DV-reference bug below; it does not advertise multi-table transactions, which the client now checks. **Nessie 0.109** (`--nessie`, the Maven Central runner jar on a JDK image) runs the same suites: it rejected schema-ID reuse (the client now sends the next free ID, as Java does), serves only the current snapshot (history is in Nessie commits; history assertions are gated) and has no v3 (create silently returns v2). The **JDBC catalog** runs the same suites over SQLite, shared with PyIceberg's `SqlCatalog`  **Lakekeeper 0.14.1** (`--lakekeeper`), the one server not built on Iceberg Java (Rust, Postgres-backed), passes every suite unchanged, v3 included; CI pulls its quay.io image and `emulation/lakekeeper_image.sh` builds the same release from source where quay.io is blocked | — | — |
| 12 | **BDD across engines** | ✅ | `Then Spark reads the same rows` (rows, and row lineage on v3), `Then Trino reads the same rows` (after `Given the tables live in the shared REST catalog`) and the PyIceberg steps; `CrossEngine.feature` runs upsert + delete under every mechanism past Spark, and past Trino for the ones it reads (not v3 deletion vectors in Trino 478). Also asserted in xunit (`SparkCrossEngineTests`, `TrinoCrossEngineTests`, `ViewConformanceTests`) | — | Athena step needs #4 |
| 13 | **Emulator fidelity upkeep** | ✅ | `.github/workflows/iceberg-dotnet-upkeep.yml` (weekly): the whole regression suite against `:latest` moto, REST fixture, Polaris and Trino, and the spec-drift check; image tags are pinned and overridable (`MOTO_TAG`, `REST_FIXTURE_TAG`, `POLARIS_TAG`, `TRINO_TAG`) | — | Fidelity tests against real AWS on the same schedule (#4) |

### Bugs these paths found

Each of these reproduced with the tests listed, and was fixed:

| Found by | Defect |
|---|---|
| Soak (#6) | Commits with an unknown outcome (HTTP 5xx after the server may have applied them) always failed the mutation; now resolved by reloading the table when the server answered |
| Soak (#6) | The Java REST fixture's default in-memory SQLite fails concurrent commits (`SQLITE_BUSY` → 500); now file SQLite with WAL |
| Spark (#3, #7) | Copy-on-write rewrites gave surviving rows new `_row_id`s; Java and Spark preserve them |
| Shadow tool smoke test (#4) | `LocalFileIO` rejected Hadoop-style `file:/path` locations, which Java writes into manifests |
| Fuzzing (#8) | The nine decoder defects listed in row 8 |
| Polaris (#11) | A conformance test relied on registering a second live table over an existing table's location |
| Throttling tests (#5) | The AWS SDK's client-wide retry quota drains under sustained throttling and then fails requests mid-commit without retrying; now off by default (retries are still capped per request) |
| Throttling tests (#5) | A conditional `PUT` whose first attempt landed but whose response was lost fails its own retry with `412`, failing a commit that had written its file; now recognised as ours by comparing bytes |
| Pruning property test (#2) | (planted) an unsound temporal projection for `>` is caught within one run, with a replay seed |
| Mutation testing (#9) | Theta merge semantics (`min` vs `max` theta, trimming to k), estimation-mode filtering and Puffin error types were untested |
| Spark evolved fixture (#7) | Equality deletes refused tables with more than one partition spec; they are now written per (spec, partition) |
| Arrow cross-check | UUID columns needed Arrow's `arrow.uuid` extension type to match PyIceberg's output |
| Nested-field filter tests | String bounds were chosen by UTF-16 order, not Iceberg's code-point order, so files mixing U+E000–U+FFFF with supplementary characters could be pruned wrongly |
| PyIceberg fingerprints (nested types) | PyIceberg 0.12 reads a null `list<struct>` as empty (upstream; [drafted](upstream/pyiceberg-null-list-of-struct.md)) |
| Java scan-planning oracle | Iceberg Java 1.10's `PlanTableScanResponse` serializer resolves delete-file references by path, so deletion vectors sharing one Puffin file are attributed to the wrong data files (and to files with none). Fixed upstream by 1.12, whose builder derives delete files from the tasks; our client reads both forms, and the fixtures come from 1.12 |
| Mutation testing, metadata (#9) | Setting a ref to the value it already had was recorded as a change (bumping `last-updated-ms`, sending a redundant update); and with no partition-evolution API, a spec built with `PartitionSpec.BuilderFor` reused field IDs of different fields from earlier specs — now `PartitionSpecUpdate` |
| Gravitino (#11) | Its scan-planning responses (Iceberg 1.11 underneath) attribute deletion vectors that share a Puffin file to the wrong data files, so deleted rows reappear; the client now matches DVs to tasks by `referenced_data_file`, which every DV carries (`JavaMisreferencedDvTests`) |
| Gravitino (#11) | PyIceberg 0.12 sends `Authorization: Bearer None` without a token, which Gravitino rejects; the oracle uses PyIceberg's noop auth manager |
| netstandard2.0 tests | A `DateOnly` literal passed to the netstandard2.0 build (from a .NET 6+ app) was rejected; now recognised by type name |

## Mutation testing

`cd mutation && dotnet stryker --config-file stryker-puffin.json` (about half an hour; nothing else may build the
solution while it runs, because Stryker swaps the mutated assembly into the test output). The score is the share of
injected bugs (flipped conditions, changed arithmetic and constants, removed statements) that at least one L0 test
catches.

| Project | Mutants | Killed | Survived | No coverage | Score | Date |
|---|---|---|---|---|---|---|
| Iceberg.Net.Puffin | 526 | 346 + 2 timeouts | 128 | 50 | 66.2% | 2026-10-23 (baseline) |
| Iceberg.Net.Puffin | 526 | 375 + 10 timeouts | 97 | 44 | 73.2% | 2026-10-23, after `PuffinEdgeTests` |
| Iceberg.Net.Core (transforms, expressions) | 414 | 286 + 1 timeout | 58 | 69 | 69.3% | 2026-10-23 (baseline; most survivors and uncovered mutants in `Expression.cs` and `Evaluators.cs`) |
| Iceberg.Net.Core (metadata state machine) | 246 | 130 + 3 timeouts | 54 | 59 | 54.1% | 2026-10-23 (baseline; `MetadataBuilderTests` written from its survivors since) |
| Iceberg.Net.Core (metadata state machine) | 247 | 186 | 42 | 19 | 75.3% | 2026-10-23, after `MetadataBuilderTests` (most survivors left are error-message text) |
| Iceberg.Net.Data (delete applicability, `DeleteFiles.cs`) | 97 | 67 + 16 timeouts | 3 | 11 | 85.6% | 2026-10-23 |

Runs weekly in `.github/workflows/iceberg-dotnet-mutation.yml` (Puffin, and Core's transforms and expressions via
`stryker-core.json`), uploading the reports.

## Needs action outside this repository

1. **AWS sandbox for CI** (#4, #6, #7, #13, M1): an OIDC role trusted by this repository, with S3 on the regression
   prefix, Glue on `regr_*` databases and Athena on one workgroup; set the repository variables listed in row 4.
   The nightly workflow then starts on its own.
2. **S3 lifecycle rule** on the regression prefix (expire after 3 days) as a backstop for runs that die before their
   cleanup (`DropTable(purge)` deletes every reachable file).
3. **Shadow tables**: choose the production tables to shadow-read nightly (`ICEBERG_SHADOW_TABLES`). The tool only
   reads.
4. **Upstream issues**: Parquet.Net group field IDs ([drafted](upstream/parquet-net-group-field-ids.md), patch
   verified), PyIceberg reading a null `list<struct>` as empty ([drafted](upstream/pyiceberg-null-list-of-struct.md)), Gravitino
   misattributing deletion vectors in scan plans ([drafted](upstream/gravitino-dv-references.md))
   (the Parquet.SourceGenerator Iceberg columns, field IDs, local timestamps and INT32 dates, were merged as
   [#712](https://github.com/rtkelly-labs/Parquet.SourceGenerator/pull/712); bump the test pin from 0.0.3 once a
   release carries them; [gaps](parquet-sourcegenerator.md)).

## Breaking out

The folder is designed to be extracted into its own repository (`rtkelly-labs/Iceberg.NET`) without changes:
no references outside `iceberg-dotnet/`, its own `global.json`, `Directory.*.props`, `.gitignore`, docs, tools and
CI workflows (move `.github/workflows/iceberg-dotnet*.yml` and drop the `iceberg-dotnet/` path prefixes). Suggested
moment: when M1 starts and it needs NuGet publishing.

## Open questions

1. **Package boundary for non-.NET 8 consumers.** Some consumer services are `net472`. `Core` and `Avro` now also
   build for `netstandard2.0` (metadata, manifests, expressions, transforms on .NET Framework 4.6.2+), with
   PolySharp and a small `Compat`/`Polyfills` layer; `DateOnly`/`TimeOnly` overloads exist only on .NET 6+. Going
   further (Data, Catalog: writing tables from net472) needs ParquetSharp's `net471` build and the AWS SDK's
   `net472` one; decide when a net472 producer is in scope.
2. **Arrow as the public row API** (M3) vs keeping `object?[]` for simple producers — likely both. Both exist now,
   for every type including struct/list/map (Arrow output matches PyIceberg's `to_arrow()`).
3. **Upstream?** An Apache `iceberg-dotnet` does not exist; if this matures, proposing it upstream (or contributing
   to an existing community effort) would share the maintenance cost. Decide after M2.
4. **v4 adoption**: whether producers that use equality deletes move to deletion vectors before v4 tables
   appear in our estate (see "Upstream changes to plan for").
