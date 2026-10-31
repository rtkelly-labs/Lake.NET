# Gap review, October 2026: test design against system completeness

**Status:** review, 2026-10-31. Measured against [../goal.md](../goal.md). The review itself only read the code; the
items it found that have since been fixed are listed first, so this page stays the index of what is open.

## Closed since the review

| Item | Fixed by | Tests |
|---|---|---|
| S1 IAM wildcard injection in STS vending | `fix(rest-server): close IAM wildcard injection and out-of-table file references` | `LocationPolicyTests`, `CredentialVendingTests` (21 cases fail on the old code) |
| S2 Snapshot, statistics and partition-statistics files outside the table | same commit | `LocationPolicyTests.A_snapshot_cannot_point_at_a_manifest_list_outside_its_table` and siblings |
| S3 Unsupported schemes written to local disk | `fix(io): refuse storage schemes with no FileIO; …` | `FileIORoutingTests` |
| §5 Gzip metadata named the Java way | same commit, plus the REST server's reader | `LocalTableTests.Gzipped_metadata_is_read_whatever_it_is_called_and_numbering_continues` |
| §9 No dependency-direction test | `test: DuckDB in-process as a second Parquet reader; …` | `DependencyDirectionTests` |
| Manifests named by an in-table manifest list (found while fixing S2) | `fix(rest-server): scan planning reads only the planned table's files` | `LocationPolicyTests.Planning_refuses_manifests_outside_the_table_named_by_a_manifest_list_inside_it` (fails on the old code) |
| E4 Fuzz, mutation and upkeep never scheduled; `mumbling` missing from the fuzz matrix | `ci: run fuzzing, mutation testing and upkeep weekly; …` | — |
| ST1 (part) No storage conformance suite | `test(io): one conformance suite for every FileIO; …` | `FileIOConformanceTests` (local, S3 on moto). Found and fixed: short S3 ranged reads past the end; `DirectoryNotFoundException` for missing local files. MinIO and remote-signed S3 fixtures still to add |
| PQ1 (tests) No hostile-input testing of the managed Parquet decoder | fork `lake(0026): Malformed-file tests for the decoder` (branch `ccr-a26a1234-riyup4` of `rtkelly-labs/parquet-dotnet`) | 1,909 cases (parquet-testing `bad_data/` plus seeded truncations, bit flips and header edits of 11 fixtures): 1,177 pass, **731 known failures and 1 hang**, grouped as findings F1–F12 in the fork's change record. The decoder fixes (bounded allocations, wrapped exceptions, the varint hang, stale pooled bytes decoded as values) are still open, and the Parquet.Net backend must not become a default until they land |
| PQ2 (part) No independent Parquet reader in the unit suite | same commit | `Iceberg.Net.DuckDbNet.Tests` (DuckDB's reader: values, field IDs, statistics, both directions) |

**Still open from that work:** existing tables or namespaces whose location already contains a wildcard now fail to vend (a 500, not a
clean error); re-validate them at load.

## What was reviewed

| Source | Where it lives |
|---|---|
| `VISION.md`, `docs/lake/**` | PR #46 (`rtkelly13/refactor-rename-library`). Not on `main`. |
| GitHub plan | Issues #27–#45, labelled `lake-plan` |
| Iceberg | `main`: 13 src projects, 12 test projects |
| DuckLake | 7-PR stack #49–#56. Top of stack: `lake/recover-ducklake` |
| Parquet.Net backend | `ccr-87fadcf9-bxi9wr` |
| Open readiness PRs | #26 (full-stack readiness), #48 (formal and chaos checks) |
| Delta | Not started. Only `docs/lake/delta.md` exists. |

Line references are to `main` unless a branch is named. Items marked **(bug?)** are probable defects found by
reading the code. None of them has been reproduced.

## Verdict

The verification design for Iceberg is unusually strong:
- spec traceability with a drift job;
- a table validator after every scenario;
- model-based tests;
- fault injection;
- 6 REST servers;
- Spark, Trino, PyIceberg and DuckDB as oracles;
- fuzzing and mutation testing.

The docs in `docs/lake/verification/` also list their own gaps honestly: no coverage measurement, no
schedules, no benchmarks.

What they miss sits *between* the pillars. Each pillar is deep, but it is applied to a narrow slice of the system:
- Fault, model and soak testing cover writes, but not maintenance.
- Storage is exercised only through local disk and S3.
- The security tests prove the documented controls work, but not that they compose. Wildcards, hostile snapshot
  paths and cross-issuer identities slip between them.
- CI can report green without the cross-engine assertions ever having run.

So the test system is pillar-complete but not system-complete.

---

## 1. Evidence integrity: can CI be green without proving the claim? (P0)

| # | Gap | Evidence | Design |
|---|---|---|---|
| E1 | **The fail-closed switch covers moto only.** Spark, Trino, PyIceberg, Polaris, Gravitino, Nessie, Lakekeeper, Azurite and GCS all call `Skip.If` silently: about 70 `Require.*` calls and about 25 direct skips. The `cross-engine` job sets no `REQUIRE_TARGET`, and its comment says a missing engine "is a skip, not a failure". The engine probe is a single 2-second TCP connect, cached for the whole process. | `TestSupport/TestEnvironment.cs:104-117,224-233`, `ci.yml:88` | Land the `ICEBERG_REQUIRED_INTEGRATIONS` from PR #26 and make every `Require.*` throw when its integration is listed. Collect TRX, and fail when the skip count exceeds a by-name allowlist (Nessie v3, AWS-only). Publish executed and skipped counts per engine in the step summary. |
| E2 | **Some claimed integrations never run.** Azurite SAS vending: the image is never pulled. Athena L3 has never run. `aws.yml` runs `Regression.Tests` only and skips Athena when the workgroup is unset. M1's exit criterion is "Athena L3 green". | `AzureCredentialVendingTests.cs:109,168`, `aws.yml:55`, `roadmap.md:14,56` | Apply the same gate. Mark those capabilities "experimental" in `capability-baseline.md` until a run is green. |
| E3 | **No compatibility tests for our own output.** 0.0.1-alpha.1 has shipped, but every committed fixture comes from a third party. Nothing checks that tables written by version N are readable by N+1, and there is no binary API check. | `tests/**/fixtures/*` | Add `fixtures/lake-net/<version>/` generated by a `tools/WriteGolden` per format: v2 and v3, every delete mode, evolved schema and spec, views. Add an L0 test that reads every golden file, runs a deep validation, compares fingerprints, appends, and validates again. Fail the release when the golden for that version is missing. Turn on `EnablePackageValidation` with the alpha.1 baseline. |
| E4 | **"Nightly" and "weekly" jobs have no `schedule:`.** This affects fuzz, mutation, upkeep (including spec drift) and AWS. Mutation runs with `break=0`, so it can never fail. Release re-runs only the unit suite. | `fuzz.yml`, `mutation.yml:5`, `upkeep.yml`, `release.yml:94` | Add cron triggers and open an issue on failure. Ratchet the mutation `break` threshold. Release should require a green `ci` run on the same SHA. |
| E5 | **The spec matrix checks link validity, not coverage.** `matrix --check` fails on unknown sections but never on uncovered ones. DuckLake drift is not wired in: there is no `--ducklake` in upkeep and no acknowledged-drift file. | `spec_trace.py:46,175`, `upkeep.yml:25` | Add the vision's `<format>-support.json` (supported, refused or planned) and fail when a `supported` section has no *executed* test. |

## 2. Security (threat model, then tests)

The tests cover the remote signer, basic location policy, static grants, per-principal credential vending,
idempotent replay and secret redaction well. The gaps are where these controls interact.

### P0

| # | Gap | Evidence | Test design |
|---|---|---|---|
| S1 | **(bug?) IAM wildcard injection in STS vending.** `SessionPolicy` pastes the location into `Resource` and `s3:prefix` unescaped. `RequireEntryName` rejects `/` and `..` but allows `*` and `?`, and namespace levels are never checked. An admin of one namespace can set its location to `wh/*` and get vended read-write on the whole warehouse. | `RestServer/CredentialVending.cs:78-93`, `CatalogService.Locations.cs:20-25`, `CatalogService.cs:128,160,380` | Unit: the policy rejects or escapes `*`, `?` and `$`. HTTP: a namespace location containing `*`, a namespace level `*`, and tables named `t*` and `t?` each get 400. Property test: no vended `Resource` or prefix contains a wildcard other than the trailing `/*`. |
| S2 | **(bug?) Snapshot files are not confined.** Commit checks table directories, not the manifest-list, statistics or partition-statistics paths inside `add-snapshot`. Server-side planning then reads them with the server's own credentials. A writer can learn another table's files and column min/max values. | `CatalogService.cs:254,264` | Commit and register with each of those paths pointing at (i) another table, (ii) another bucket, (iii) `file:///etc/passwd` or `gs://…`. Each must get 400. Planning must never return another table's entries. |
| S3 | **(bug?) Unsupported schemes fall through to local disk.** `ResolvingFileIO` routes `StartsWith("s3")` to S3 and everything else to `LocalFileIO`, which creates directories as needed. A `gs://`, `abfss://` or `hdfs://` location is written to, or read from, the local filesystem, and this is RestCatalog's default IO. On the server side this turns into a local-file read or SSRF. | `IO/FileIO.cs:349`, `RestCatalog.cs:198,485` | Theory over {gs, abfss, wasbs, hdfs, oss, http}: every method throws `NotSupportedException`. Routing: a relative path `s3data/x` stays local. A fake REST server that vends `gs://` gets a refusal before any write. |

### P1

| # | Gap | Evidence | Design |
|---|---|---|---|
| S4 | JWT negatives are missing: expired token, `alg=none`, wrong issuer or audience. Grants key on `sub` alone, so an external IdP user with `sub=root` gets local root's grants **(bug?)**. | `Authorization.cs:109`, `Auth.cs:115-121` | Stub JWKS. Expect 401 for each negative. Key principals by `(iss, sub)`. |
| S5 | Authorization holes: rename *into* a namespace without write; register with no write, or with `overwrite` and no admin; view create, replace and drop; a plan ID reused by another principal. | `Endpoints.cs:249`, `AuthorizationTests.cs:96,150` | A table-driven authorization matrix: principal × operation × resource → expected status, generated from the grant model so new endpoints fail until they are listed. |
| S6 | **(bug?)** The 16 MiB body limit applies only after the idempotency middleware has read and hashed the whole body. There is no JSON depth limit, no rate limit on the token endpoint, and every create reads all tables' metadata. | `Endpoints.cs:97,122-126`, `CatalogService.Locations.cs:96-121` | Run on real Kestrel (TestServer ignores limits): 17 MiB gets 413 with and without an `Idempotency-Key`, deep JSON gets 400, and metadata reads per create are bounded. |
| S7 | **(bug?)** 400 messages name other tables and their URIs, which leaks tables in namespaces the caller cannot read. | `CatalogService.Locations.cs:139,178` | Assert that error bodies never contain identifiers outside the caller's grants. |
| S8 | **(bug?)** The client accepts any server-pushed `s3.endpoint` that ends in `amazonaws.com`, which includes API Gateway, ELB and `attacker.s3.amazonaws.com` hosts. Not tested: redirects carrying the bearer token, a server-pushed `prefix` of `../x`, and server-chosen table locations. | `RestCatalog.cs:281,200,237` | Each of these hosts and paths is refused, and redirects drop `Authorization`. |
| S9 | **The Parquet decoder is the largest untested attack surface.** It is native (ParquetSharp, Arrow C++), with no corruption samples, no fuzz target and no bomb tests. Avro data files have no fuzz target, `mumbling` is missing from the `fuzz.yml` matrix, and REST JSON in both directions has nothing. | `Fuzz/Program.cs`, `fuzz.yml` | A process-isolated Parquet fuzz target (it is native, so a crash must not kill the runner). Footer and page-header bit flips, RLE and dictionary bombs, huge value counts. Add `avro-data`, `mumbling` and `rest-json` targets. Run the same samples against both Parquet backends. |
| S10 | **DuckLake paths (bug?).** Paths from catalog rows are joined unconfined, cleanup deletes any absolute path a row names, and a schema named `..` writes outside `data_path`. | DuckLake branch: `DuckLakeCatalog.cs:411`, `DuckLakeMaintenance.cs:210-221`, `DuckLakeTransaction.cs:414` | Names `..`, `.` and `a/b` are refused. A row with `../../x` is refused. Cleanup leaves a sentinel file outside `data_path` intact. |
| S11 | **DuckLake SQL (bug?).** Inlined-table names read from the catalog are dropped and deleted from without a pattern check. `DuckDbAttach` builds the PostgreSQL connection string unquoted. | `DuckLakeMaintenance.cs:153-162`, `DuckLakeStore.cs:57-66` | A row naming `ducklake_snapshot` is never dropped. A password containing `'` and `;` round-trips. End-to-end names containing `"`, `'`, `;` and Unicode. |
| S12 | **DuckLake encryption.** Only the happy path is tested. An encrypted lake with a NULL per-file key falls back to a plaintext read. Untested: wrong key, wrong key length, flipped byte, truncation, swapped files (no AAD prefix), and keys leaking into telemetry or exception text. | `DuckLakeCatalog.cs:502`, `EncryptionTests.cs:49-78` | One test per case, each expecting a format error. Add a telemetry and exception scrubber assertion. |

### P2

- **Signer:** it uses a header deny-list. Switch to an allow-list, and refuse `x-amz-tagging`, a KMS key id and the `PATCH` method.
- **HTTP-level location tests:** add `%2e%2e`, `S3://`, `s3n://` and symlink cases.
- **Static-token comparison:** make it constant-time.
- **Supply chain:** add an explicit `--vulnerable --include-transitive` gate, lock files with locked restore, a CycloneDX SBOM, provenance attestation and CodeQL.
- **`SECURITY.md`:** add a threat model covering the server being open by default, DuckLake catalog writers being fully trusted, keys living in the catalog, and the Azure container-wide scope.

## 3. Storage and IO plane (P1)

The vision describes one IO layer shared by every format. Today it has two implementations, local and S3, and
no conformance suite.

- **ST1: no `FileIOConformanceTests<T>`.** S3 is checked only for conditional create and range reads
  (`EmulatorFidelityTests.cs:23,35`). Listing returns `LastModified ?? MinValue`, so an object without a timestamp
  looks ancient and becomes an immediate orphan candidate (`FileIO.cs:163-176`).
  - Fixtures: Local, moto, MinIO and remote-signed S3.
  - Cases:
    - N writers race a create-if-absent and exactly one wins;
    - every read method on a missing object throws `FileNotFoundException`;
    - range reads at and past EOF;
    - a zero-byte object;
    - listing 2,500 keys;
    - `t/` vs `t2/` prefix isolation;
    - deleting twice is idempotent;
    - cancellation is honoured.
  - Then run the maintenance suites (orphans, expiry, purge) on each fixture.
- **ST2: DuckLake never touches object storage.** `TestLake.cs:45` hard-codes `LocalFileIO`, and there is no L3
  setup with S3 data and a hosted PostgreSQL. Add a storage dimension to `TestLake` and run the cross-engine,
  maintenance and BDD suites on S3 (moto). DuckDB reads through httpfs with a secret.
- **ST3: Delta's whole commit protocol is put-if-absent on `_delta_log/N.json`.** The vision assigns this to
  `EmulatorFidelityTests`. It needs the ST1 race case on every backend and an explicit refusal on stores without
  conditional writes. See §8.

## 4. Durability: the fault model is narrower than the invariant (P0/P1)

The chaos page states four invariants: before-or-after, validator passes, nothing possibly-committed is deleted,
and retries converge. The harness enforces them for *writes on the in-memory catalog*.

| # | Gap | Evidence | Design |
|---|---|---|---|
| D1 | **Maintenance is outside the fault, model and soak harnesses.** This covers expiry, orphan cleanup, compaction, rewrite-position-deletes, rewrite-manifests, merge-append, branches, UPDATE/MERGE and multi-table transactions. | `FaultInjectionTests.cs:34-56`, `ModelBasedTests.cs:71-80`, `SoakTests.cs:77-100` | Add all of these to `FaultInjectionTests.Operations`. Add Compact, RewriteManifests, EvolveSpec and AddColumn steps to the model. Add a soak worker that loops compaction and expiry against the 4 writers. |
| D2 | **Faults are injected on whole writes only.** Reads, deletes, torn writes and slow reads are never faulted. `TableFiles.DeleteAllAsync` aborts on the first delete error *after* expiry has committed, so the result is lost. | `FaultInjectingFileIO.cs:17-65`, `TableFiles.cs:49`, `Maintenance.cs:93-117` | Add `FailRead`, `FailDelete`, `TornWrite` and `SlowRead`. Set a contract for a partial delete, such as `FailedDeletes` on the result. |
| D3 | **Commit-protocol faults run only on InMemoryCatalog.** | — | Rerun the before-or-after theory against JDBC and Glue on moto, and against one REST server. |
| D4 | **DuckLake has no fault injection on its catalog database.** The unknown-outcome, DbException-retry and cancel-at-COMMIT paths are unreached. PostgreSQL `40001` and SQLite BUSY fail hard instead of retrying. There is no idempotency token, so retrying after a lost ack duplicates rows. | `DuckLakeTransaction.cs:638-690` | An EF Core `DbCommandInterceptor` and `DbTransactionInterceptor` that throw before or after command N, lose the commit ack, or raise `40001`. Run it as a theory over operations × fault point × database. Nightly: Toxiproxy in front of PostgreSQL, and a primary/standby promotion. |
| D5 | **DuckDB never writes *concurrently* with us**, so DuckDB never parses our `changes_made` under contention. A mis-encoded change could let DuckDB commit over a real conflict without anyone noticing. | `CrossEngineTests.cs:984-1009`, `DuckLakeChanges.cs:218-242` | A long-lived oracle session that stages, waits for a signal, then commits. Run every row of the conflict matrix with DuckDB staging, both ways round. |
| D6 | **Clock skew cannot be tested.** Orphan cleanup compares the store's `LastModified` against the local `DateTime.UtcNow`. | `Maintenance.cs:220`, `DuckLakeMaintenance.cs:255` | Thread a `TimeProvider` through. A concurrent uncommitted file must survive skew up to the minimum age. |
| D7 | DuckLake: the conflict rule table covers only "ours" ∈ {schema, DDL, insert, delete}. `ConflictRuleTests.cs:81` points at a `DeleteConflictTests` class that doesn't exist. Commit-attempt exhaustion and multi-snapshot retries are untested. `CleanupOldFilesAsync(olderThan:null)` deletes everything scheduled, and its grace boundary is untested. | — | Generate the full ours × theirs table. Add boundary tests for the `olderThan` argument. Add a test where expiry runs under a writer's base snapshot. |
| D8 | **Process death.** No `kill -9` in the middle of a commit for any format. | — | The vision's `tools/ChaosWriter`. Recover, then validate. |

## 5. Interop, fixtures and correctness breadth (P1)

- **Gzip metadata ✅ is unsubstantiated.** Only names ending in `.gz` are decompressed, so Java's
  `…​.gz.metadata.json` is not. `VersionRe` resets numbering after register (`Catalog.cs:372,382`). Add fixtures
  in both forms and a Spark case with `write.metadata.compression-codec=gzip`.
- **Other ✅ claims are thin:**
  - `timestamptz_ns` Parquet I/O has no round-trip test.
  - Rewrite-manifests has one test.
  - Purge is tested on memory and local only.
  - Orphan cleanup is tested on local only.
  - The multi-argument transform JSON round-trip is lossy: `SortOrderFromJson` keeps only `source-ids[0]` (`TypeJson.cs:214`).
- **v3 types:** variant, geometry, geography and unknown are tested in metadata and stats only. The Parquet
  writer throws (`ParquetFiles.cs:56`). Pin the failure contract: refuse before any file is written, and allow a
  projection without the column. `unknown` should read as null.
- **Type × operation matrix:**
  - Mutation and maintenance keys use only long, int, string, double and date.
  - `ValueEquality` hashes `byte[]` by length only (`TableMutations.cs:395`).
  - The pruning oracle omits uuid, binary, fixed, time, timestamp_ns and decimal38, and the hour and year transforms.
  - Add a partition-evolution oracle: drop the old source column, then scan, compact, expire and purge.
- **The catalog conformance matrix leaves out InMemoryCatalog.** Not tested:
  - purge removing every file, checked by listing before and after;
  - rename errors and a rename racing a commit (Glue rename is create-then-delete, `GlueCatalog.cs:134`);
  - dropping a non-empty namespace;
  - list paging;
  - transactions carrying schema or spec updates.
- **DuckLake directions:**
  - DuckDB→us is not tested for column rename, drop or widen; nested field changes; SET DEFAULT; epoch and bucket
    partitions; partition evolution; or the 1.0→1.1 migration done by DuckDB 2.0.
  - Time travel by timestamp is untested in both directions.
  - There are no committed DuckDB-written fixtures; it is live-only, and the 1.1 extension is downloaded unpinned
    at test time.
  - The model-based generator has no DDL.
- **Parquet backends:**
  - The Parquet.Net job runs only the unit and DuckLake suites, on older DuckLake code.
  - No external engine reads Iceberg files written by Parquet.Net.
  - Each backend reading the other's files *is* tested on that branch (`ParquetBackendTests`), but not on `main`.
  - Add a backend axis to cross-engine (nightly at least) and assert fingerprint equality between backends. See §10.
- **One version manifest:** engine and image pins are spread over about 8 files. The vision's
  `interop/engines.json` doesn't exist.

## 6. Scale, resources and operability (P1/P2)

- Several paths are not streaming or bounded:
  - `S3FileIO.OpenReadAsync` buffers whole objects.
  - Writes are single PUTs, so 5 GB is the cap.
  - Parquet files are built in a `MemoryStream`.
  - `TableWriter` writes one file per partition and doesn't roll files at the target size.

  No test has a large file, many manifests or many snapshots.
- **Design:** a nightly `Scale` suite. Synthesise 5k manifests, 200k files and 10k snapshots through the codecs,
  then assert plan time, FileIO request counts (from telemetry) and allocated bytes against a committed
  `budgets.json`. This is the vision's "resource envelope".
- **Telemetry:** 7 span assertions, and no metric or request-count contract. The vision wants request counts as
  the deterministic PR performance gate, so they need a contract test first.
- **Soak:** 15 seconds on PRs (`SoakTests.cs:35`). DuckLake has none. Run 30–60 minutes nightly per format, with
  faults on.

## 7. Platform matrix (P1)

- **OS:** CI runs on Ubuntu only. Orphan cleanup's ordinal path comparison (`Maintenance.cs:197-214`,
  `TableLocations.cs:76`) is untested on Windows or on a case-insensitive filesystem. That is a direct data-loss
  path. Widen the platform job from PR #26 to cover maintenance, FileIO and the package smoke test. Add an
  orphan dry-run on a table at a `C:\` path that must find zero candidates.
- **net472 / netstandard2.0:** these run on .NET 10, and the legacy consumer is only built. Run them on Windows
  as net472.
- **Native assets:** ParquetSharp needs a per-RID package smoke test.
- **Hygiene:**
  - Add per-test timeouts and `--blame-hang`.
  - Seed `CorruptionTests` from `GITHUB_RUN_ID`, not `TickCount`.
  - Give each run its own JDBC SQLite file.
  - Clean up the `regr_*` namespaces on AWS.

## 8. Delta: design the test system *before* the code (P1)

`docs/lake/delta.md` lists DAT, delta-rs, Spark, traceability, corruption, DV bytes and put-if-absent. That is
good for a *reader*. For a writer that is safe to share with Spark, Databricks and delta-rs, it misses these:

1. **Writer-side table-feature enforcement.** This is where a Delta writer corrupts tables silently. For each
   writer feature (`appendOnly`, `invariants`, `checkConstraints`, `generatedColumns`, `identityColumns`,
   `columnDefaults`, `changeDataFeed`, `rowTracking`, `deletionVectors`, `timestampNtz`, `domainMetadata`,
   `v2Checkpoint`, `typeWidening`, `variantType`, `inCommitTimestamp`, `clustering`, `vacuumProtocolCheck`,
   `checkpointProtection`), there must be either a test that *honours* it or a test that *refuses the write*.
   Make it a `delta-support.json` that CI checks, as in §1 E5. DAT does not cover this because it is reader-only.
2. **Writer oracles in both directions.**
   - Spark+Delta and delta-rs read *every* table we write.
   - Add DuckDB's `delta` extension (delta-kernel-rs) as a third, cheap reader.
   - For every write mode, Spark's `DESCRIBE HISTORY` and `delta-rs` `.history()` must agree with our log.
3. **Conflict detection matrix.**
   - Delta's `WriteSerializable` vs `Serializable` isolation, across append, delete, update, merge, optimize and
     metadata/protocol changes.
   - `SetTransaction` (`txn` app-id/version) idempotency, which gives producers exactly-once writes and is the
     answer to the D4-style lost-ack problem.
   - Concurrency races against Spark and delta-rs writing at the same time (the §4 D5 pattern).
4. **Log-store semantics.**
   - S3 conditional `If-None-Match` on every backend.
   - A clear refusal, or a pluggable lock-based log store, for stores without conditional writes (older MinIO,
     some S3-compatibles).
   - Azure and GCS once IO supports them.
5. **Checkpoint and log faults.**
   - A stale or missing `_last_checkpoint`.
   - A checkpoint written without its commit.
   - A multi-part checkpoint with a missing part.
   - V2 sidecars that are referenced but missing.
   - Log cleanup racing time travel within `logRetentionDuration`.
   - Vacuum within `deletedFileRetentionDuration`, honouring `checkpointProtection`.
6. **Serialisation interop.** These are notorious cross-engine bug sources:
   - `partitionValues` as strings: nulls, empty strings, escaping, timestamps with time zones, decimals;
   - `stats` JSON truncation and `tightBounds` once deletion vectors exist;
   - inline Z85 deletion vectors;
   - column-mapping physical names after rename, drop and re-add.

   Property-test these against delta-rs parsing.
7. **Timestamp time travel** with `inCommitTimestamp`, compared with file modification time on S3. This needs the
   §4 D6 clock injection.

## 9. Cross-format harness (P2, but it gates Delta)

- There is no `IFormatDriver`. Iceberg and DuckLake have separate step libraries, and `TestSupport` uses Iceberg
  types.
- There is no dependency-direction test, which is a "how we know it's working" criterion in the vision.
- There is no format-neutral `TableFingerprint` or `ShadowCompare`.

Build these before Delta's first PR so that Delta is born on the common BDD feature set, rather than retrofitted
like DuckLake.

---

## 10. The Parquet.Net fork (`rtkelly-labs/parquet-dotnet`, Lake.Parquet)

**Where it stands.**
- The fork is upstream's history with 25 `lake(NNNN)` changes on top, each with a record in `docs/changes/`. It is
  rebased onto upstream daily (`lake-sync.yml`) and published as `Lake.Parquet` from a renamed copy.
- Lake.NET consumes it as the `external/parquet-dotnet` submodule on `ccr-87fadcf9-bxi9wr`, behind
  `IParquetBackend`.

**What it already has.**
- Each change's evidence is recorded against upstream.
- Upstream's suite runs against the *renamed* assembly, and `VerifyLakeAssembly` checks the rename.
- Interop with parquet-cpp (ParquetSharp) in all five encryption modes, and pyarrow through upstream's tests.
- 10 encryption tests that include wrong key, missing key, tampering and key size.
- Lake-side, `ParquetBackendTests` checks that each backend reads the other's files, and that they declare the same
  schema and statistics and prune the same row groups.
- `lake-stack` enforces the stack rules.

**Gaps, in the context of the lake.**

This fork is the *managed data plane*: the code that reads every data file in every format, including files other
engines wrote. It should therefore meet the strictest bar in the lake, and today it meets the loosest one.

| # | Gap | Evidence | Test design |
|---|---|---|---|
| PQ1 (P0) | **No hostile-input testing of a managed decoder of untrusted bytes.** The fork's own vision names "allocation amplification from untrusted footer counts" and "page checksums never verified". The only malformed-file sample is upstream's `641_rle_pagesize_oob.parquet`. There is no fuzz target, and `apache/parquet-testing`'s `bad_data/` is not used. The lake's `Corruption` contract (format error, never a crash, OOM or silent wrong answer) doesn't reach this layer. | `src/Parquet.Test` (no corrupt or fuzz tests), `docs/vision.md` | A SharpFuzz/libFuzzer target over `ParquetReader`. It is managed, so unlike ParquetSharp it can run in-process. Seed it with `data/**` and the encryption fixtures. Vendor `parquet-testing` `data/` (each file read equal to pyarrow's JSON) and `bad_data/` (each file must raise `ParquetException`). Add a bit-flip, truncation and hostile-length suite for the footer, page headers, RLE/bit-packed runs, dictionary and DELTA_* encodings, with an allocation cap asserted via `GC.GetAllocatedBytesForCurrentThread`. Back it with a `lake(NNNN)` change that bounds allocations from footer and header counts, and another that verifies page CRCs. Register the target in Lake.NET's `fuzz.yml`. |
| PQ2 (P0) | **No external engine except DuckDB reads Parquet.Net output.** Lake.NET's `parquet-net` job runs only the unit and DuckLake suites. Spark (parquet-java), Trino (its own reader), PyIceberg (Arrow C++) and, for Delta, delta-rs (arrow-rs) never read a data file this backend wrote. Each engine has its *own* Parquet reader, so parquet-cpp interop alone proves nothing for the other three. | Lake.NET `ccr` `ci.yml:160-200`, `docs/parquet-net-gaps.md` | Run the `cross-engine` and BDD suites with `ICEBERG_PARQUET_BACKEND=parquet-net`, nightly or as a PR matrix axis. In the fork, add a writer-matrix test: one file per type, nesting shape, encoding, page version and codec, read by pyarrow, parquet-java (through `parquet-cli` on a JDK image), DuckDB and arrow-rs (through `deltalake` or `polars`). |
| PQ3 (P1) | **The daily rebase is promoted without Lake.NET's contract.** `lake-sync` runs `lake.yml`, which is Ubuntu-only and the fork's own suite. Upstream's 3-OS `build.yml` runs only *after* `master` moves, and does not run against the renamed assembly. Nothing Lake.NET relies on (field IDs on groups, `RawDataField` levels, physical values, typed statistics, encryption) is re-checked before promotion. When upstream supersedes a change and it is retired, its tests may leave with it. | `.github/workflows/lake-sync.yml`, `lake.yml:51` | Add a Lake-owned **contract suite** in the fork (`eng/lake/Lake.Contract.Tests`) that covers exactly what Lake.NET calls, independent of the change records, so retiring a change cannot remove coverage. Make it, plus a 3-OS run of the renamed build, gates on `promote`. Optionally use `repository_dispatch` to run Lake.NET's backend suites on `lake/next` before `master` moves. |
| PQ4 (P1) | **The statistics have no oracle beyond fixed examples.** Wrong min/max values make pruning silently drop rows. Tests are example-based: NaN is covered, but -0.0/+0.0, code-point vs UTF-16 order, unsigned, FLBA, decimal, INT96 and truncated binary bounds are not. | `StatisticsSortOrderTest.cs`, `TypedStatisticsTest.cs` | A property test (FsCheck): random columns × type × encoding × page size. Footer and page statistics must equal (a) the min/max computed by brute force in the column's sort order, and (b) what parquet-cpp writes for the same data. Then add a *pruning soundness* property: no row-group skip ever drops a matching row. Lake.NET's planted unsound-projection test shows this kind of test works. |
| PQ5 (P1) | **Encryption is only spot-checked for the properties that matter.** | `EncryptionTest.cs`, change 0024 review notes | (a) Nonce uniqueness over many thousands of modules, plus a check that no code path reuses a nonce. (b) AAD binding: swap pages, column chunks or row groups between two files encrypted with the same key, and the read must fail. (c) A tamper theory over *every* module type (footer, column metadata, page header, dictionary page, data page), not one byte. (d) GCM_CTR pages are unauthenticated by design: assert headers still are. (e) Interop with **parquet-java**'s encryption (Spark, Iceberg Java), not only parquet-cpp. (f) Keys never appear in exception text or `ToString()`. Have an expert review this, as the change record itself asks. |
| PQ6 (P2) | **Breadth for the formats to come.** Iceberg v3 and Delta `variantType` need the variant encoding, which is only on `feature/variant-encoding`. Geo types, bloom filters and page indexes are not written. There is no spec map of `parquet-format`. | branches, change 0024 notes | The variant vectors from `parquet-testing`, shredded and unshredded. A `parquet-format` section map for `spec_trace.py`, as the lake's spec-alignment page already plans. Pin refusal tests for features the fork doesn't write. |
| PQ7 (P2) | **The side-by-side promise is untested.** The vision says `Lake.Parquet` must load beside `Parquet.Net` in one process, and D10 had it moving to Parquet.SourceGenerator's build, which the later `ccr` commit reverses. | `docs/vision.md`, Lake.NET `decisions.md#d10` | A smoke test that references both packages, reads and writes with each, and checks that `Lake.Parquet`-only APIs bind to the right assembly. Settle D10 in the docs. |
| PQ8 (P2) | **No performance gate.** `perf/` and `Parquet.PerfRunner` exist, but nothing compares against upstream or fails on a regression. The vision is driven by allocation amplification. | `perf/` | BenchmarkDotNet allocation runs, fork vs upstream, in the sync PR report. Fail on an allocation increase over 10% for the decode paths Lake.NET uses. |

## Suggested order

Items marked closed above are done; the order of the rest stands.

1. **Merge PR #26** for required integrations, TRX output, the Azurite pull, the Windows and macOS job and the API
   snapshot. Then extend the gate to every engine (E1, E2) and add schedules (E4). This makes every later claim
   trustworthy.
2. **Security P0s S1–S3**, each with a regression test. Then the JWT, authorization-matrix and Parquet-fuzz work
   (S4, S5, S9).
3. **Durability:**
   - maintenance under faults (D1, D2);
   - DuckLake catalog-database faults with an idempotency token (D4);
   - concurrent DuckDB writers (D5).
4. **Storage:** the `FileIOConformanceTests` suite, DuckLake on S3, and golden fixtures of our own output (ST1,
   ST2, E3).
5. **Parquet fork:**
   - PQ1, hostile input with fuzzing and `bad_data`, before Parquet.Net becomes a default or packed backend;
   - PQ2, external engines reading its output;
   - PQ3, a Lake contract suite gating the daily promotion.
6. **Before any Delta code:** `IFormatDriver`, the dependency-direction test, `delta-support.json` with the
   writer-feature table, and DAT plus delta-rs fixtures (§8, §9).
7. **Then:** scale and budgets, the OS matrix, coverage and pathway tracing (§6, §7).

## Corrections to reviewer findings after spot-checks

- The DuckLake orphan `Normalise` collapses `//` → `/` on *both* sides of the comparison, so S3 paths still match.
  That is not a bug.
- The DuckLake orphan grace period defaults to 3 days; 1 hour is the minimum a caller can choose. So the risk of
  a slow writer losing its staged files only arises with an explicit short `olderThan`. It still deserves a test.
