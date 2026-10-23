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
| **M1 — production-grade append** | A C# producer can safely own appends to an Athena-queried Glue table | ✅ `UpdateSchema` API, merge-append, `DropTable(purge)` deleting data, SigV4 (Glue/S3 Tables REST), JDBC catalog, `add_files`-style adoption (`ParquetImport`), S3 throttling retries, serializable `Overwrite`/`DeleteFiles` validation, decimals up to precision 38, `fixed(L)`, NuGet packages (smoke-tested in CI); remaining: nested types, Idempotency-Key retries, a netstandard2.0 build of Core/Avro, typed producers via Parquet.SourceGenerator once its Iceberg profile lands ([gaps](parquet-sourcegenerator.md)) | Athena L3 green in a sandbox account (needs AWS, below); soak against Glue, REST, Polaris and JDBC ✅ in emulation |
| **M2 — merge-on-read reads** ✅ | Read any table Athena/Spark/Trino have mutated | position deletes, DVs, Puffin, delete applicability, v2→v3 DV converter, NDV statistics, Spark- and Trino-written MoR tables, Spark v1 and schema/partition-evolved tables, v3 row lineage read, filter expressions with manifest/partition/metrics pruning | Spark + Trino + PyIceberg ✅; Athena-written DELETE/MERGE tables (AWS run pending) |
| **M3 — row-level writes** ✅ | C# upserts / deletes without Athena | insert, upsert by key, group replacement, delete by key/predicate/expression with copy-on-write, position/DV and equality deletes (also across partition specs); single-snapshot RowDelta; re-plan-on-conflict serializability ([write-modes.md](write-modes.md)); `_row_id` preserved on v3 rewrites; key- and filter-based file pruning; Arrow `RecordBatch` API (`Iceberg.Net.Arrow`) | Spark, Trino and PyIceberg read our output ✅ (PyIceberg also our Arrow output); Athena (AWS pending) |
| **M4 — maintenance & breadth** | Self-sufficient operations | ✅ snapshot expiry with file deletion, orphan-file cleanup, bin-pack compaction (lineage kept), merge-append and manifest rewrite, multi-table transactions (REST, JDBC), views (REST + in-memory; Trino reads ours and we read its); remaining: sort compaction, server-side scan planning, views in the JDBC catalog | Spark reads compacted, merged, evolved and expired tables ✅ |

### Upstream changes to plan for (from the spec-drift check)

`python tools/spec_trace.py drift` compares apache/iceberg `main` with the sections pinned at the 1.12.0 release.
Today it reports the in-progress **v4** work. Two items affect our design:

- **v4 forbids new equality deletes** ("Equality delete files must not be added to v4 tables"; existing v2/v3 ones
  must still be applied). When v4 support lands (we read and write up to v3 today), `DeleteStrategy.EqualityDeletes`
  must refuse v4 tables, and producers that rely on
  equality deletes for cheap upserts need a v4 answer: deletion vectors after a key lookup, which our
  `PositionDeletes` strategy already does.
- **Manifests are being restructured** (root manifests, content stats, manifest deletion vectors, relative paths).
  This is additive for v1–v3 readers, but it is the biggest change to plan for if we adopt v4.

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
| 8 | **Decoder fuzzing** | ✅ | `CorruptionTests`: bit flips, truncation, insertion and hostile length fields against metadata JSON, manifest lists, manifests, Puffin, DVs, Roaring and Theta, plus structural JSON damage and required-field removal (`ICEBERG_FUZZ_RUNS`, `ICEBERG_FUZZ_SEED`). Contract (`Corruption`): fail with a format error, never a crash or a silent wrong answer. Found and fixed: unchecked lengths sizing allocations (Avro, Roaring), DV length overflow, negative Theta counts accepted, unchecked Avro union branches and varints, v2/v3 metadata silently defaulting required fields | Coverage-guided fuzzing (SharpFuzz + libFuzzer) for long unattended runs | — |
| 9 | **Mutation testing** | ✅ | Stryker.NET 5 (`dotnet-tools.json`, `mutation/`); Puffin baseline 66.2%, raised by `PuffinEdgeTests` written from the survivors: see [Mutation testing](#mutation-testing) | Extend to transforms, the metadata state machine and delete applicability; schedule it (too slow for PR CI) | — |
| 10 | **Spec traceability + drift** | ✅ | `[Spec("…")]` (C#, also an xunit trait: `--filter Spec=row-lineage`) and `@spec:` (Gherkin) tags; `tools/spec_trace.py` builds [spec-traceability.md](spec-traceability.md) from sections pinned at apache-iceberg-1.12.0 (CI fails if it is stale or links to unknown sections); the weekly `drift` job diffs upstream `main` and fails on changes nobody has reviewed yet (`drift --acknowledge` records a review in `docs/spec/acknowledged-drift.json`; the v4 changes above are acknowledged as of 2026-10-23). Today 67/88 implementable table + Puffin sections and 14/15 implemented REST operations have linked tests | The ⬜ rows of the matrix: untagged tests, or real gaps such as sort orders, default values and path relativization | — |
| 11 | **More REST servers** | 🟡 | **Apache Polaris 1.8** (`--polaris`) runs the full catalog conformance suite, the soak and the PyIceberg cross-engine tests. It caught a test that registered two live tables at one location, which Polaris rightly refuses. The new **JDBC catalog** runs the same suites over SQLite, shared with PyIceberg's `SqlCatalog` | Nessie, Lakekeeper | Their current images are on quay.io/ghcr.io, which this sandbox's proxy blocks; Docker Hub's `projectnessie/nessie:latest` is from January 2024, before Nessie served the Iceberg REST API. GitHub Actions can reach them: add them as compose profiles from a machine with access |
| 12 | **BDD across engines** | 🟡 | `Then Spark reads the same rows` (rows, and row lineage on v3) and the PyIceberg steps; `CrossEngine.feature` runs upsert + delete under every mechanism past Spark. Trino and Spark are also asserted in xunit (`SparkCrossEngineTests`, `TrinoCrossEngineTests`, `ViewConformanceTests`) | Trino step (needs the scenario's table in the REST catalog) | Athena step needs #4 |
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

## Mutation testing

`cd mutation && dotnet stryker --config-file stryker-puffin.json` (about half an hour; nothing else may build the
solution while it runs, because Stryker swaps the mutated assembly into the test output). The score is the share of
injected bugs (flipped conditions, changed arithmetic and constants, removed statements) that at least one L0 test
catches.

| Project | Mutants | Killed | Survived | No coverage | Score | Date |
|---|---|---|---|---|---|---|
| Iceberg.Net.Puffin | — | — | — | — | — | pending first full run |

## Needs action outside this repository

1. **AWS sandbox for CI** (#4, #6, #7, #13, M1): an OIDC role trusted by this repository, with S3 on the regression
   prefix, Glue on `regr_*` databases and Athena on one workgroup; set the repository variables listed in row 4.
   The nightly workflow then starts on its own.
2. **S3 lifecycle rule** on the regression prefix (expire after 3 days), because `DropTable(purge)` does not delete
   data files yet.
3. **Shadow tables**: choose the production tables to shadow-read nightly (`ICEBERG_SHADOW_TABLES`). The tool only
   reads.
4. **Registry access for Nessie/Lakekeeper** (#11), or add them from a machine that can pull from quay.io/ghcr.io.
5. **Upstream issues**: Parquet.Net group field IDs ([drafted](upstream/parquet-net-group-field-ids.md), patch
   verified) and the Parquet.SourceGenerator Iceberg profile ([gaps](parquet-sourcegenerator.md)).

## Breaking out

The folder is designed to be extracted into its own repository (`rtkelly-labs/Iceberg.NET`) without changes:
no references outside `iceberg-dotnet/`, its own `global.json`, `Directory.*.props`, `.gitignore`, docs, tools and
CI workflows (move `.github/workflows/iceberg-dotnet*.yml` and drop the `iceberg-dotnet/` path prefixes). Suggested
moment: when M1 starts and it needs NuGet publishing.

## Open questions

1. **Package boundary for non-.NET 8 consumers.** Some consumer services are `net472`. `Core` and `Avro` can target
   `netstandard2.0` with modest changes (no `Rune`, `BinaryPrimitives` via `System.Memory`); ParquetSharp supports
   `net471`. Worth doing in M1 if a net472 producer is in scope.
2. **Arrow as the public row API** (M3) vs keeping `object?[]` for simple producers — likely both.
3. **Upstream?** An Apache `iceberg-dotnet` does not exist; if this matures, proposing it upstream (or contributing
   to an existing community effort) would share the maintenance cost. Decide after M2.
4. **v4 adoption**: whether producers that use equality deletes move to deletion vectors before v4 tables
   appear in our estate (see "Upstream changes to plan for").
