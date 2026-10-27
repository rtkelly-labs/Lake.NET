# Regression suite

One suite, two targets. The **same test binaries** run in full emulation (default, local and CI) and against AWS
(nightly once configured), selected by environment variables. Tests that need something the target cannot provide
**skip with a reason**; with `ICEBERG_REQUIRE_TARGET=1` (set by `run-tests.sh` and CI) an unreachable target is
a failure instead. Every test is linked to the spec section it verifies: `python3 tools/spec_trace.py matrix` writes
`docs/spec-traceability.md` (generated, not committed; each CI run uploads it as the `spec-traceability` artifact).

## Tiers

| Tier | Project / class | Needs | What it proves |
|---|---|---|---|
| **L0 spec** | `Iceberg.Net.Spec.Tests` | nothing | Spec vectors (Appendix B/D, truncate), metadata v1/v2/v3 JSON, state machine, Avro/manifest codecs, local end-to-end, commit protocol, Puffin/DV/Theta against DataSketches + pyroaring bytes, row deletes and the v2→v3 converter, name mapping and import, write modes |
| L0 · other writers | `PyIcebergFixtureTests`, `SparkFixtureTests` | nothing (committed fixtures) | We read PyIceberg- and Spark-written tables (v2 position deletes, v3 DVs, row lineage) exactly, they pass fsck, and our writes on top of them stay consistent |
| L0 · invariants | `TableValidatorTests`, `ModelBasedTests`, `FaultInjectionTests`, `CorruptionTests` | nothing | fsck detects planted corruption; random operation sequences always match an in-memory model; every write fault leaves the table before-or-after; corrupt bytes fail with a format error |
| **L1 catalog conformance** | `CatalogConformanceTests` | S3 + Glue (moto or AWS); REST fixture; Polaris | Identical scenarios against Glue (client-side commit), the Java REST fixture and Apache Polaris (server-side commit): lifecycle, round trip, concurrent writers, stale requirements, evolution + time travel, rename/register/drop, v3 |
| L1 fidelity | `EmulatorFidelityTests` | S3 + Glue | The storage/catalog guarantees commits rely on (conditional create, Glue VersionId); same assertions in both modes |
| L1 soak | `SoakTests` | per target | 4 concurrent writers × random modes for `ICEBERG_SOAK_SECONDS` against memory v2/v3, Glue, REST fixture and Polaris: no lost or duplicated rows, deletes stay deleted, fsck clean |
| **L2 cross-engine** | `CrossEngineTests`, `WriteModeCrossEngineTests`, `PuffinCrossEngineTests`, `SourceGeneratorInteropTests`, `ShadowCompareTests` | PyIceberg oracle | Both directions with PyIceberg (rows, values, partitions, summaries, position deletes, DVs, NDV sketches, generator files); PyIceberg refuses equality deletes rather than misreading them; shadow fingerprints agree for every type |
| L2 · Spark | `SparkCrossEngineTests` | `emulation/up.sh --spark` | Spark + Iceberg Java reads every write path (CoW, position deletes, DVs, equality deletes, v2→v3 upgrade, multi-transform partitioning) with the same rows, snapshots, delete kinds and row lineage |
| L2 · Java scan planning | `ServerScanPlanningTests` (L0, committed plans), `SparkCrossEngineTests.Java_plans_our_tables_and_we_read_its_plans` | `emulation/up.sh --spark` for the live half | Iceberg Java plans scans (interop/java/ScanPlanOracle.java, the Spark image's JDK + Iceberg 1.12) and serializes the REST `PlanTableScanResponse`; we read those plans, of Spark's tables and of every table we write, exactly as our own planner does. No emulated REST server implements planning, so this is the server side |
| L2 · Trino | `TrinoCrossEngineTests` | `emulation/up.sh --trino` | Trino reads our writes; we read Trino's MoR DELETE/UPDATE/MERGE; mixed histories agree in both |
| L2 · managed services | `ManagedSparkJobTests`, `AthenaRunnerEmulationTests`, `AthenaSqlOnTrinoTests` | moto; `--spark` for the job runs, `--trino` for the Athena SQL | The EMR Serverless, Glue and Athena runners with the real SDK clients against moto (staging, job creation, submission, polling through moto's state manager, cancel/stop, Athena results and paging); the staged job run from moto S3 by Spark on a Glue table in moto, as the service would (`SparkJobServiceEmulator`); Athena's query SQL run by Trino, Athena-only statements' Trino dialect ([spark-and-athena.md](spark-and-athena.md)) |
| **REST protocol** | `Iceberg.Net.RestProtocol.Tests` | nothing (in-process server); emulators for the cross-server class | Every REST OpenAPI operation's success and documented errors (error model, status, type) against this repository's server; the client's mapping of each status, including 419 and 5xx on commits (scripted server); the error contract (missing vs taken names, stale commits, non-empty drops) against the Java reference server, Polaris, Nessie, Lakekeeper and Gravitino |
| **BDD · REST catalog** | `Iceberg.Net.RestCatalogBehaviour.Tests` (Reqnroll) | nothing (in-process server) | Gherkin specification of the REST catalog's behaviour: namespaces, tables, commits and racing writers, multi-table transactions, views, authentication and the role matrix, and protocol details (ETag, config, the error model) |
| **BDD** | `Iceberg.Net.Behaviour.Tests` (Reqnroll) | PyIceberg oracle for the cross-engine steps | Gherkin specification of write modes × delete mechanisms × format versions, concurrency and conversion; deep fsck after every scenario |
| **L3 AWS-only** | `AthenaTests` | AWS + Athena workgroup | Athena reads our tables (counts, sums, partition pruning on our transforms); we read Athena-written tables including its position deletes |

Current results (emulation, everything up, 2026-10-24): **L0 752/752**, **L1+L2 267/270** (2 Athena tests skip
without AWS; Nessie skips the v3 conformance case; the Athena paging fidelity case needs a moto started after shim 3), **Ecosystem 55/55**. The full inventory is in [handover.md](handover.md#test-inventory).

## Environment variables

| Variable | Default (emulation) | AWS | Meaning |
|---|---|---|---|
| `ICEBERG_TEST_TARGET` | `emulation` | `aws` | target selection |
| `ICEBERG_REQUIRE_TARGET` | unset | `1` | fail instead of skip if the target is unreachable |
| `AWS_REGION` | `eu-west-1` | your region | |
| `MOTO_PORT` | `5000` | — | moto's host port, for `emulation/up.sh` and the tests' default endpoint (5000 is taken by AirPlay on macOS) |
| `ICEBERG_S3_ENDPOINT` | `http://localhost:$MOTO_PORT` | unset | S3 endpoint override |
| `ICEBERG_GLUE_ENDPOINT` | = S3 endpoint | unset | Glue endpoint override |
| `ICEBERG_REST_URI` | `http://localhost:8181` | optional | REST catalog under test |
| `ICEBERG_POLARIS_URI` | `http://localhost:8182/api/catalog` | — | Polaris (emulation only) |
| `ICEBERG_TRINO_URI` | `http://localhost:8080` | — | Trino (emulation only) |
| `ICEBERG_SPARK` / `ICEBERG_SPARK_IMAGE` / `ICEBERG_SPARK_JAR` | on / `apache/spark:4.0.1` / `emulation/.cache/…jar` | — | Spark oracle; `ICEBERG_SPARK=0` disables it, `ICEBERG_SPARK_KEEP=1` keeps its tables for debugging. The managed-job emulation also needs the AWS jars `up.sh --spark` puts next to the runtime jar |
| `ICEBERG_DUCKDB_PYTHON` / `ICEBERG_DUCKDB` / `ICEBERG_DUCKDB_REQUIRED` | `ICEBERG_PYTHON` / on / off | — | DuckDB oracles (`Iceberg.Net.Ecosystem.Tests`'s `iceberg` extension, `Iceberg.Net.DuckLake.Tests`'s `ducklake` extension): the Python with the interop `duckdb` group; `ICEBERG_DUCKDB=0` disables it; `ICEBERG_DUCKDB_REQUIRED=1` (CI) fails instead of skipping when the extension cannot load ([spark-and-athena.md](spark-and-athena.md#duckdb-oracle)) |
| `ICEBERG_DUCKLAKE_POSTGRES` / `ICEBERG_DUCKLAKE_POSTGRES_REQUIRED` | unset (emulation mode of `run-tests.sh`: the stack's PostgreSQL on 5434) / off | — | DuckLake's PostgreSQL catalogs: an Npgsql connection string the tests may create and drop databases with; `_REQUIRED=1` (CI) fails instead of skipping ([ducklake.md](ducklake.md#tests)) |
| `ICEBERG_DUCKLAKE11_PYTHON` / `ICEBERG_DUCKLAKE11_REQUIRED` | unset / off | — | The DuckLake 1.1 oracle: a Python with `duckdb==2.0.0` (the extension downloads from extensions.duckdb.org) |
| `ICEBERG_WAREHOUSE` | `s3://iceberg-regression/warehouse` | **required** `s3://bucket/prefix` | where test tables live (Glue tables under `/glue`) |
| `ICEBERG_ATHENA_WORKGROUP` | — | e.g. `primary` | enables L3 |
| `ICEBERG_ATHENA_OUTPUT` | — | optional `s3://…` | Athena result location if the workgroup has none |
| `ICEBERG_PYTHON` | — | — | interpreter with `interop/` deps; otherwise `uv run --project interop` is used |
| `ICEBERG_SOAK_SECONDS` | `15` | | soak duration per target |
| `ICEBERG_PBT_RUNS` / `ICEBERG_PBT_SEED` | `40` / random | | model-based test cases; a failure prints the seed to replay |
| `ICEBERG_FUZZ_RUNS` / `ICEBERG_FUZZ_SEED` | `500` / random | | corruption mutations per decoder sample |
| AWS credentials | `test`/`test` (set automatically) | standard SDK chain | |

## Running

```bash
./run-tests.sh spec                                   # L0
./emulation/up.sh --spark --trino --polaris --gravitino --nessie --lakekeeper   # everything CI runs; plain ./emulation/up.sh for moto + REST
./run-tests.sh emulation                              # L0 + BDD + L1 + L2
dotnet test tests/Iceberg.Net.Spec.Tests --filter Spec=deletion-vectors   # every test of one spec section
ICEBERG_PBT_RUNS=2000 ICEBERG_FUZZ_RUNS=20000 ./run-tests.sh spec               # longer randomized runs
```

### AWS mode

```bash
# Use a sandbox account/bucket. Every run creates Glue databases named regr_<purpose>_<yyyyMMddHHmm>_<id>
# and removes them (tables purged) at the end of each test.
export AWS_PROFILE=data-sandbox ICEBERG_WAREHOUSE=s3://iceberg-net-regression/runs ICEBERG_ATHENA_WORKGROUP=primary
./run-tests.sh aws
```

AWS permissions needed: `s3:GetObject/PutObject/DeleteObject/ListBucket` on the warehouse prefix;
`glue:*Database*`, `glue:*Table*` on `regr_*`; `athena:StartQueryExecution/GetQueryExecution/GetQueryResults`
on the workgroup (and its output bucket). Cost per run: a handful of Athena queries over KB-sized tables (cents).
For the nightly shadow read, add read-only S3/Glue access to the production tables listed in `ICEBERG_SHADOW_TABLES`.

Leftovers: if a run is killed, delete Glue databases matching `regr_*` older than a day and the corresponding
`$ICEBERG_WAREHOUSE` prefixes. Data files are not deleted by `DropTable(purge)` yet (metadata only) — set an S3
lifecycle rule on the regression prefix (e.g. expire after 3 days).

### Shadow comparison

`tools/ShadowCompare` reads one table with this library and with PyIceberg, both from the same metadata file, and
compares order-independent content fingerprints (row count; per column the null count and a sum of value hashes;
a sum of whole-row hashes), then runs the read-only table checker. It moves no rows and writes nothing.

```bash
dotnet run --project tools/ShadowCompare -- --catalog glue --table analytics.plays   # exit 0 identical, 1 different, 2 error
dotnet run --project tools/ShadowCompare -- --metadata s3://bucket/path/metadata/00042-….metadata.json
```

### Fuzzing

`CorruptionTests` (L0) mutates fixtures at random; `tools/fuzz.sh` adds coverage-guided fuzzing with SharpFuzz and
libFuzzer for long unattended runs, one decoder at a time:

```bash
sudo apt-get install clang libclang-rt-18-dev     # libFuzzer runtime
tools/fuzz.sh manifest 600                         # metadata view-metadata manifest-list manifest puffin
                                                   # deletion-vector roaring mumbling theta
FUZZ_STRICT=1 tools/fuzz.sh roaring 600            # also flag low-level exceptions a Corruption.Guard converted
```

The harness (`tests/Iceberg.Net.Fuzz`) holds each decoder to the `Corruption` contract: a format error or
success, nothing else. Crashes, hangs (10 s) and out-of-memory (4 GB) inputs are saved under
`fuzz/<target>/findings/`; turn each into a `CorruptionTests` case when fixing it. The corpus is seeded from the
committed fixtures and kept between runs in `fuzz/<target>/corpus` (git-ignored). The fuzz workflow
(`.github/workflows/fuzz.yml`) runs every target for 15 minutes when dispatched.

### Size limits

Random mutation rarely produces inputs whose fields agree with each other, such as a schema and a count, or a header
and its runs. So the limits that stop a decoder allocating or looping on a claimed size have hand-built cases in
`DecoderLimitTests`:

| Limit | Value | Where |
|---|---|---|
| Avro block, compressed or inflated | 128 MiB (`AvroBinary.MaxBlockBytes`) | container and streamed data files |
| Avro records per block | the block's bytes; 2^20 for a schema that encodes in zero bytes | `AvroBinary.CheckRecordCount` |
| Avro `fixed` size | the bytes left | `AvroBinary.Read` |
| Puffin zstd blob, decompressed | 256 MiB (`PuffinFile.MaxDecompressedBlobBytes`) | `PuffinFile.Contents.Payload` |
| Deletion vector positions | the manifest entry's `record_count`; runs must match each container's declared cardinality | `DeleteFiles.ReadDeletionVectorAsync`, `RoaringPositionBitmap` |
| Bound lengths | the type's width | `Conversions.FromBytes` |

## CI

| Workflow | When | What |
|---|---|---|
| `ci.yml` | every PR, merge queue entry and push to `main` (its `ci` job is the required check, and fails unless every job below succeeds) | Three parallel jobs, one per `<CiSuite>` (`tools/test-projects.sh`): `unit` (L0, netstandard2.0, LINQ, EF Core, REST catalog behaviour, package smoke test, spec traceability matrix as an artifact; no Docker), `cross-engine` (all emulators + Spark + DuckDB: ecosystem, BDD, REST server) and `regression` (all emulators, `ICEBERG_REQUIRE_TARGET=1`: REST protocol, L1/L2) |
| `upkeep.yml` | when dispatched | spec drift vs apache/iceberg `main` (red only for upstream changes not yet reviewed with `tools/spec_trace.py drift --acknowledge`); the regression suite against `:latest` emulator images |
| `aws.yml` | when dispatched, **dormant until `ICEBERG_AWS_ROLE_ARN` is set** | L1–L3 against AWS; shadow comparison of `ICEBERG_SHADOW_TABLES` |
| `release.yml` | when dispatched (a dry run unless unticked) | Release build, the `unit` suite, pack, package smoke test, the commit's `ci` check; publishes the `CHANGELOG.md` version to NuGet (trusted publishing) |

All are path-filtered or scheduled, so they do not affect the platform's CI time budget.

## Adding tests

- Tag every test with the spec section it verifies: `[Spec("Row Lineage")]`, `[Spec("puffin: deletion-vector-v1 blob
  type")]`, `[Spec("rest: loadTable")]`, or `@spec:row-lineage` in Gherkin; `python3 tools/spec_trace.py matrix --check` shows the result locally.
- Spec behaviour → L0, preferring the spec's own vectors.
- Anything that touches a catalog → add to `CatalogConformanceTests` (it runs for every catalog), not a new class.
- Anything another engine observes → add a case to the cross-engine classes (extend `interop/pyiceberg_oracle.py` or
  `interop/spark_oracle.py`; Spark tables go in the shared fixture so one JVM reads them all).
- An AWS guarantee the emulator lacks → shim + `EmulatorFidelityTests` ([emulation.md](emulation.md)).
- A new decoder → add its sample to `CorruptionTests`; a new operation → add it to `ModelBasedTests`.
- Regenerate committed fixtures with the oracles' `fixtures` commands when their scenarios change (Java plans:
  `interop/java/make-fixtures.sh`); L0 stays network-free.

Planned tiers and their status are tracked in [roadmap.md](roadmap.md#verification-roadmap).
