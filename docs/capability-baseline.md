# Capability baseline before 0.0.1

Assessment date: 26 October 2026. Source baseline: `5582a0b` (main after #20), plus the handover, documentation and Spark test-harness corrections in PR #21. The owner selected **the full stack, including the REST server**, as the minimum release use case. This is a
repository capability assessment and proposed release contract, not a
release announcement. No `0.0.1` version is cut. The existing `0.0.1-alpha.1` is a prerelease.

## Assessment

The repository already implements a native Iceberg table engine, catalog clients, typed access, and a REST catalog
server. Calling all of it “M0 foundations” understates the implementation. Calling it complete Iceberg v1–v3 or
production-verified would overstate the evidence. The main readiness problem is an unsettled support contract:
which exact APIs, engines, runtimes and guarantees will a full-stack `0.0.1` promise? The server is part of
that scope; it cannot be treated as an optional future component.

The strongest existing evidence is the metadata/commit state machine, supported Parquet and Avro I/O, row-level
mutations, delete applicability, local model/fault tests, and emulator/reference-engine integration suites. The
largest qualification is that real cloud execution is outstanding. Some broad public claims also hide narrower
boundaries: v1 is read-only, several v3 types are metadata-only, LINQ includes client execution, generic SQL-provider
support is not proven beyond SQLite, and cloud credential vending is distinct from native .NET cloud FileIO.

This assessment inventories all 12 source projects, the solution's test projects and harnesses, packaging, workflows,
oracles, emulation, and operational tooling. It checks public entry points, explicit refusals and representative
implementation paths against their tests. It is not an exhaustive line-by-line correctness, security, performance or
upstream-spec audit. Existing test names below identify evidence in the repository; only the separately recorded
execution results are fresh verification from this assessment.

## How to read the inventory

- **Implemented**: a usable public implementation and tests exist; limits still apply.
- **Partial**: part of the named capability exists; do not advertise the whole capability.
- **Absent**: no implementation for the stated scope.
- **External verification pending**: the implementation exists, but the named real service/provider has not been
  demonstrated by the evidence reviewed here.

A test linked to a spec heading is traceability, not proof that every requirement or combination is covered.
A green run with skipped integration cases is not verification of those integrations.

## Source and package boundaries

The current seven-package selection does not distribute the complete selected full-stack scope. A delivery path
for the additional libraries and server must be agreed before `0.0.1`. All project paths below are
relative to [`src/`](../src).

| Component | Purpose and public surface | Current distribution / runtime |
|---|---|---|
| [Core](../src/Iceberg.Net.Core) | Types, schemas, transforms, metadata, requirements/updates, expressions, statistics model, telemetry | Packable; `net10.0` and `netstandard2.0` |
| [Avro](../src/Iceberg.Net.Avro) | Iceberg Avro container, manifest and manifest-list codecs | Packable; `net10.0` and `netstandard2.0` |
| [IO](../src/Iceberg.Net.IO) | `IFileIO`, listing, local/S3 access, ranged streams | Packable; `net10.0` |
| [Catalog](../src/Iceberg.Net.Catalog) | `ICatalog`, `Table`, catalog implementations, views, REST plans/signing | Packable; `net10.0` |
| [Puffin](../src/Iceberg.Net.Puffin) | Puffin, deletion vectors, Roaring, Theta, preparatory Mumbling codec | Packable; `net10.0` |
| [Data](../src/Iceberg.Net.Data) | File I/O, scans, mutations, staged commits, maintenance, validation | Packable; `net10.0`; ParquetSharp native dependency |
| [Linq](../src/Iceberg.Net.Linq) | Typed schema/mapping, queries and writes | Packable; `net10.0` |
| [Arrow](../src/Iceberg.Net.Arrow) | Arrow schemas, batches and table reads/writes | Implemented; `IsPackable=false`; `net10.0` |
| [EntityFrameworkCore](../src/Iceberg.Net.EntityFrameworkCore) | EF Core 10 provider over the typed layer | Implemented; `IsPackable=false`; `net10.0` |
| [Spark](../src/Iceberg.Net.Spark) | Procedure SQL and EMR Serverless/Glue job runners | Implemented; `IsPackable=false`; `net10.0` |
| [Athena](../src/Iceberg.Net.Athena) | Athena/Trino SQL generation and Athena query runner | Implemented; `IsPackable=false`; `net10.0` |
| [RestServer](../src/Iceberg.Net.RestServer) | ASP.NET Core executable and hosting APIs | `IsPackable=false` by choice; run/publish from source; `net10.0` |

`netstandard2.0` does not extend the complete read/write engine to .NET Framework: only Core and Avro target it.
Its tests execute those assemblies on the current .NET runtime, not on .NET Framework. CI uses Ubuntu; this is not
a Windows/macOS/architecture compatibility matrix. Local macOS runs add evidence for this workstation only.

## Table engine capabilities

The IDs are stable discussion references for agreeing release scope. They do not imply that every row must be in
`0.0.1`. Test classes are in [Spec.Tests](../tests/Iceberg.Net.Spec.Tests) unless another suite is named.

| ID | Capability / entry point | Assessment and boundaries | Evidence |
|---|---|---|---|
| C01 | Types and schemas: `Schema`, `IcebergType`, JSON codecs | Implemented for ordinary primitive and nested models; extended-type data I/O is qualified below | `MetadataSpecTests`, `SingleValueJsonTests`, `NestedTypesTests` |
| C02 | Table metadata and evolution: `TableMetadataBuilder`, `Table.UpdateSchemaAsync`, `UpdateSpecAsync`, sort-order replacement | Implemented requirements/updates, schema/partition evolution and promotion; nested schema changes exist despite an old top-level-only tracker description | `StateManagementTests`, `SchemaUpdateTests`, `MetadataBuilderTests`, `SortOrderTests` |
| C03 | Partition transforms and sorting | Implemented identity, bucket, truncate, temporal and void transforms; unknown transforms round-trip but cannot write; multi-source transforms are metadata-only; row sorting currently requires top-level sources | `TransformSpecTests`; [RowSorter](../src/Iceberg.Net.Data/RowSorter.cs), [TableWriter](../src/Iceberg.Net.Data/TableWriter.cs) |
| C04 | Manifest and manifest-list I/O | v1 read, v2/v3 read/write; resolve by field ID; inherit sequence and row IDs; v1 embedded-manifest-list snapshots explicitly refused | `AvroSpecTests`, `PyIcebergFixtureTests`, `ManifestStoreTests`; [ManifestCodec](../src/Iceberg.Net.Avro/ManifestCodec.cs) |
| C05 | Parquet data I/O: `ParquetFiles`, `TableWriter` | Implemented supported primitives, decimals through precision 38, UUID/fixed and struct/list/map; default table writes group rows in memory and produce a file per partition | `WideTypesTests`, `NestedTypesTests`, `ReadPathTests`; [writer](../src/Iceberg.Net.Data/TableWriter.cs) |
| C06 | Avro data/delete I/O: `AvroDataFiles`, `DataFiles` | Implemented read/write and field-ID projection; null/deflate codecs only; streamed blocks with a known size, whole-file fallback otherwise | `AvroDataFileTests`, `SparkFixtureTests`, `ReadPathTests`; [codec dispatch](../src/Iceberg.Net.Data/AvroDataFiles.cs) |
| C07 | Pruned/projected scans: `ScanRequest`, `TableScan.ReadRowsAsync` | Implemented manifest/partition/metrics/row-group pruning, field-ID projection, row-group/block iteration and count fast paths; filters through structs, not collections | `ScanContractTests`, `ScanPruningTests`, `ReadPathTests`, `NestedFieldFilterTests` |
| C08 | Time travel: `SnapshotSelector` | Snapshot/ref/instant with historical schema through `ScanRequest`; legacy list/Arrow overloads intentionally use the current schema | `ReadPathTests`, `ScanContractTests`; [read contract](read-path.md#time-travel-semantics) |
| C09 | Append/overwrite/file delete: `SnapshotProducer` | Atomic single-table snapshots with optimistic retries; v1 table writes refused until upgrade | `LocalTableTests`, `OverwriteValidationTests`, `InterleavingTests` |
| C10 | Insert/upsert/group replacement/key and predicate delete: `TableMutations` | Implemented serializable re-planning and pruning; equality deletes require key/partition constraints; a predicate cannot use equality-delete strategy | `TableMutationTests`; [BDD features](../tests/Iceberg.Net.Behaviour.Tests/Features), [write modes](write-modes.md) |
| C11 | UPDATE/MERGE: `UpdateMerge`, `RowUpdate`, `MergeBuilder` | Implemented nested struct assignments and ordered clauses; MERGE joins by top-level equality keys, not arbitrary SQL predicates; duplicate non-null source keys are refused even if none match targets | `UpdateMergeTests`, `InterleavingTests`; [cross-engine cases](../tests/Iceberg.Net.Regression.Tests/UpdateMergeCrossEngineTests.cs), [semantics](update-merge.md) |
| C12 | Copy-on-write, position/equality deletes, v3 DVs | Implemented read/write, delete applicability and v2 position-delete to v3 DV conversion; equality-delete fields currently top-level; engine interoperability differs | `RowDeleteTests`, `TableMutationTests`, `SparkFixtureTests`; [DeleteFiles](../src/Iceberg.Net.Data/DeleteFiles.cs) |
| C13 | v3 row lineage | Implemented inherited/stored IDs, preservation during rewrites, update sequence tracking and fresh IDs for inserts | `BranchWriteTests`, `SparkFixtureTests`, `ModelBasedTests`; `SparkCrossEngineTests` |
| C14 | Atomic staging across tables: `StagedCommit.CommitAllAsync` | Implemented on one catalog instance with multi-table transactions; retries re-plan every table; Glue cannot provide this atomicity | `StagedCommitTests`, `JdbcCatalogTests`, regression `CatalogConformanceTests` |
| C15 | Branches/tags/rollback/cherry-pick/fast-forward/WAP | Implemented through `ManageSnapshots()` and branch writes; operation-specific ancestry and conflict rules apply | `SnapshotManagementTests`, `BranchWriteTests`; [snapshot contract](snapshot-management.md) |
| C16 | Expiry/purge/orphan cleanup | Implemented reachability, table-location confinement, GC flag, per-ref retention and minimum orphan age; orphan deletion needs listing support | `MaintenanceTests`, `ExpiryRetentionTests`, `TableLocationsTests`; [Maintenance](../src/Iceberg.Net.Data/Maintenance.cs) |
| C17 | Compaction and manifest/delete rewrites | Implemented bin-pack/sort behavior, lineage preservation and optional partial progress; partial failures are reported; output size is not a universal hard size guarantee | `CompactionTests`, `PartialCompactionTests`, `MergeAppendTests`; [Compaction](../src/Iceberg.Net.Data/Compaction.cs) |
| C18 | Metadata tables | Snapshots, history, refs, metadata log, manifests, files/data/delete files, entries and partitions; no `all_*` tables or `readable_metrics` | `MetadataTablesTests`, `SparkCrossEngineTests`; [MetadataTables](../src/Iceberg.Net.Data/MetadataTables.cs) |
| C19 | Change feed | Snapshot ancestry, inserts/deletes, optional carryover removal, skips replace snapshots; current schema; expired ancestry may make a range unreadable; no UPDATE before/after classification | `ChangeFeedTests`; [ChangeFeed](../src/Iceberg.Net.Data/ChangeFeed.cs); Spark comparison excludes histories with delete files |
| C20 | Foreign Parquet adoption: `ParquetImport` | Implemented validation/name mapping/metrics without rewriting; importer rejects nested columns and reads full data to derive metrics/partitions | `NameMappingAndImportTests`; regression `SourceGeneratorInteropTests`; [importer](../src/Iceberg.Net.Data/ParquetImport.cs) |
| C21 | Puffin/statistics/sketches | Implemented uncompressed/zstd blobs, v3 DVs, portable Roaring (run containers read), compact Theta v3 and NDV statistics; no LZ4 footer or general sketch families | `PuffinSpecTests`, `PuffinEdgeTests`, `RowDeleteTests`; [Puffin](../src/Iceberg.Net.Puffin) |
| C22 | Table validation and comparison | Read-only `TableValidator` (optional deep reads), fingerprints and `ShadowCompare` tool; reference comparison uses PyIceberg against the same metadata file, not an Athena shadow oracle | `TableValidatorTests`; regression `ShadowCompareTests`; [tool](../tools/ShadowCompare/Program.cs) |

### Format and type boundaries

| Area | Actual scope |
|---|---|
| v1 | Metadata/manifests and supported data reads; snapshot writes refused; embedded `manifests` snapshots unsupported |
| v2 / v3 | Principal read/write formats, subject to type, codec, operation and engine limitations in this baseline |
| v4 | Preparatory `file` type, content-statistics model/layout and Mumbling codec; metadata reader accepts only 1–3; no complete v4 table read/write |
| Ordinary primitives | Boolean/numerics, date/time, micro/nanosecond timestamps, string/binary, UUID, fixed, decimal through precision 38 have supported Parquet mappings |
| Struct/list/map | Data I/O and typed mapping exist; filters/partition sources can traverse structs, not list/map elements; import and EF Core have narrower boundaries |
| Variant / geometry / geography / unknown | Model/JSON capability does not imply complete data I/O; the Parquet writer has no cases for these types |
| Defaults and encryption | Initial defaults are applied for missing read columns; typed-write defaults for absent members are implemented, preserving explicit nulls (F13 fix). Encryption keys are represented in metadata, not an implemented end-to-end encryption service |
| ORC | No data-file implementation |

Sources: [Parquet mappings](../src/Iceberg.Net.Data/ParquetFiles.cs), [Avro mappings](../src/Iceberg.Net.Avro/ManifestCodec.cs),
[metadata version check](../src/Iceberg.Net.Core/Json/MetadataJson.cs), [v4 plan](iceberg-v4.md).

## Catalogs, storage and integrations

| ID | Capability | Implemented scope and evidence boundary |
|---|---|---|
| C23 | Native catalog clients | In-memory, Glue, JDBC/SQL and REST. Shared CRUD/commit contract: [ICatalog](../src/Iceberg.Net.Catalog/Catalog.cs), regression `CatalogConformanceTests`. Namespace load/update-properties and credential-refresh endpoints are not exposed as a complete .NET client API |
| C24 | Glue and AWS REST clients | Glue VersionId locking and single-level namespaces; SigV4 helpers for Glue REST/S3 Tables. SDK/emulator and signing-vector tests exist. Real AWS catalog/storage/workgroup verification remains pending |
| C25 | SQL catalog | ADO.NET factory, V0/V1 JDBC layouts, optional views and multi-table transactions; tests prove SQLite. Treat PostgreSQL/MySQL/SQL Server portability as unverified; generic ADO.NET plumbing does not establish SQL-dialect compatibility |
| C26 | REST client | Config/auth, paging, tables/views, idempotent retries, selected endpoint negotiation, remote signing and server-planned scans. Namespace properties, credential refresh, metrics submission and freshness-aware table loading remain client gaps |
| C27 | Views | v1 metadata, dialect SQL representations, create/load/replace/list/exists/rename/drop on in-memory/REST/JDBC V1; Glue lacks `IViewCatalog`. View SQL storage does not provide a .NET SQL query engine. `ViewTests` and regression `ViewConformanceTests` |
| C28 | Native FileIO | Local and S3, including ranges, listing, conditional create, credentials and request signing; `ResolvingFileIO` dispatches to those. No native Azure/GCS data FileIO is supplied; server vending for external clients is separate. [FileIO](../src/Iceberg.Net.IO/FileIO.cs), `ThrottlingTests`, `EmulatorFidelityTests` |
| C29 | Engine interoperability | Spark/Java, PyIceberg, Trino and DuckDB oracles exist. Support is a set of tested combinations, not every engine × format × delete mode: pinned PyIceberg refuses equality deletes; pinned Trino cannot read DVs; Spark changelog comparison cannot validate histories with delete files. [Regression tests](../tests/Iceberg.Net.Regression.Tests), [oracles](../interop) |

The REST compatibility suite exercises Java reference, Polaris, Gravitino, Nessie, Lakekeeper, JDBC and the in-repo
server. Known server differences are characterised (for example Nessie format/history behavior); this does not prove
every commercial REST endpoint. The pinned versions and table-format support must accompany any compatibility claim.

## Consumer APIs and server

| ID | Capability / public entry point | Assessment and limitations | Evidence |
|---|---|---|---|
| C30 | Typed schema and mapping: `IcebergSchema`, `ReflectionRowMapper<T>` | Implemented classes/records, explicit IDs, naming, nested/collection mappings and checked conversions. Name binding is convenient initially; explicit IDs are what survive renames | [mapping](../src/Iceberg.Net.Linq/Mapping), `MappingTests`, `SchemaFromTypeTests` |
| C31 | Typed queries: `AsQueryable<T>()` | Filters/projection/time travel push into scans where supported; residual predicates and relational-looking operators can execute in .NET; ordering/grouping/aggregates are not a server query engine. Strict mode rejects residual filtering, not all client computation; one table per query | [query implementation](../src/Iceberg.Net.Linq/Query), `QueryTranslationTests`, `TimeTravelTests` |
| C32 | Typed mutations | Insert/upsert/delete/update/MERGE wrappers; updates require exact filter translation, deletes may refine a pushed superset; bulk mutations require current-table queries with only `Where` beforehand | [typed writes](../src/Iceberg.Net.Linq/Writes/IcebergWrites.cs), `TypedWriteTests` |
| C33 | Arrow batches | Schema/field-ID conversion, supported primitive and nested arrays, reads/writes and UUID metadata; not every extended Iceberg type | [Arrow](../src/Iceberg.Net.Arrow), `ArrowTests`, `ArrowNestedTests`, regression `ArrowCrossEngineTests` |
| C34 | EF Core 10 | Model creation, queries, time travel, bulk writes, tracked `SaveChanges`, concurrency tokens and atomic saves where catalog supports them. Without transactions, multi-table saves are sequential unless `RequireAtomicSaveChanges` is set. No migrations, explicit transactions, compiled queries, relationships, inheritance, complex/owned or collection columns, converters or global query filters | [provider](../src/Iceberg.Net.EntityFrameworkCore), [tests](../tests/Iceberg.Net.EntityFrameworkCore.Tests), REST server `EfCoreTransactionTests` |
| C35 | REST catalog server | Tables/namespaces/views, staged creates, transactions, plans/tasks/incremental scans, ETags, unregister, metrics endpoint and generated OpenAPI. Metrics reports are accepted/discarded; not stored. Functions and register-view absent; one warehouse per server | [routes](../src/Iceberg.Net.RestServer/Endpoints.cs), [protocol tests](../tests/Iceberg.Net.RestProtocol.Tests), [behavior specs](../tests/Iceberg.Net.RestCatalogBehaviour.Tests/Features) |
| C36 | Server auth and storage delegation | Bearer/client credentials/token exchange, grants, table-location constraints, S3 signing, STS/Azure SAS/GCS tokens. S3 signing only; real GCS downscoping and ADLS directory-SAS validation remain outstanding. Do not infer native .NET Azure/GCS data access | [server](../src/Iceberg.Net.RestServer), `AuthorizationTests`, `LocationPolicyTests`, credential-vending and remote-signing tests |
| C37 | Server shared state | SQL/in-memory pointer stores, shared idempotency and plan state; response retention/size limit and credential stripping. No per-principal retained-response quota; audit logging and metrics-report storage absent | `SharedStateTests`, `IdempotencyTests`, `ServerDefaultsTests`; [state](../src/Iceberg.Net.RestServer/ServerState.cs) |
| C38 | Spark helpers / managed jobs | Procedure SQL, generated driver, staging, EMR Serverless/Glue submission/polling/cancel; SQL oracle and SDK emulation tests. Not a Spark Connect client; real service execution pending | [Spark](../src/Iceberg.Net.Spark), ecosystem `SparkProcedureOracleTests`, `SparkJobRunnerTests`; regression `ManagedSparkJobTests` |
| C39 | Athena helpers / query runner | DDL/maintenance/time-travel SQL and SDK submission/polling/paging/cancel; SQL unit tests, Trino query checks and moto runner. Real Athena DDL/OPTIMIZE/VACUUM and AWS execution remain pending | [Athena](../src/Iceberg.Net.Athena), ecosystem `AthenaSqlTests`; regression `AthenaRunnerEmulationTests`, `AthenaSqlOnTrinoTests` |
| C40 | Telemetry | `IcebergTelemetry` ActivitySource/Meter for planning/scans/commits/catalog calls, outcomes/retries and counters; optional listener integration, not a bundled monitoring service | [instrumentation](../src/Iceberg.Net.Core/Diagnostics/IcebergTelemetry.cs), ecosystem `TelemetryTests` |

The server OpenAPI comparison has **25 explicitly reviewed deviations**, including three missing operations;
see the [deviation ledger](spec/openapi-deviations.json) and `OpenApiConformanceTests`. An OpenAPI shape check and an
operation-level protocol test are different evidence; neither alone establishes full server compliance.

## Verification and operations

| Mechanism | What is present | Qualification |
|---|---|---|
| Unit contract suites | Spec, netstandard assemblies, LINQ-to-Objects predicate matrix, EF Core, REST behavior | Run through solution/CiSuite registry; counts and results below |
| Cross-engine / regression | Python, Docker reference servers, Spark, Trino, DuckDB, moto shims, managed-job emulation | Availability checks can skip capabilities; the required-target flag only hard-fails the primary target |
| Fault / concurrency | Deterministic interleavings, injected IO and commit faults, random mutation models, contention soak | Strong correctness evidence; not production-duration load testing |
| Decoder robustness | `Corruption.Guard`, size/count limits, mutation samples, SharpFuzz/libFuzzer harness | Harness has nine targets; fuzz workflow matrix runs eight and omits `mumbling` |
| Mutation testing | Four Stryker configurations | Threshold `break=0`; measures survivors but does not enforce a minimum score |
| Spec traceability | Generated matrix against pinned Apache 1.12.0 sections | 73/88 table+Puffin sections and 32/32 implemented REST operations linked; links are not execution or completeness coverage |
| Upstream drift / upkeep | Pinned-spec comparison and latest-emulator workflow | Manual dispatch; not automatically weekly |
| Cloud verification | AWS workflow with OIDC and variables; read-only shadow tool | Manual dispatch, AWS role-gated; no nightly schedule; real cloud evidence remains outstanding |
| Release machinery | Changelog parser, exact package-count smoke test, CI-commit check, dry-run default, trusted publishing | Dry-run CI verdict can warn rather than fail; current latest cut section remains the already released alpha |
| API / performance / portability | Public XML docs, integration tests and native dependency | No checked-in public API compatibility baseline or benchmark suite found; Ubuntu CI alone is not cross-platform certification |

See [CI](../.github/workflows/ci.yml), [fuzz](../.github/workflows/fuzz.yml),
[mutation](../.github/workflows/mutation.yml), [AWS](../.github/workflows/aws.yml),
[upkeep](../.github/workflows/upkeep.yml), [test target selection](../tests/Iceberg.Net.TestSupport/TestEnvironment.cs),
and [run-tests.sh](../run-tests.sh).

## Findings to settle before calling the capability set formal

| Priority / ID | Finding and consequence | Concrete completion criterion |
|---|---|---|
| P0 F01 | Full-stack scope is selected, but the exact support contract and distribution are not agreed. Only seven of twelve projects currently pack | Define success scenarios and included/experimental/excluded behavior for each C-ID; deliver the selected additional libraries and server; no version cut before that agreement |
| P1 F02 | Current docs contain materially different claims (M0 vs broad features; metadata types vs real I/O; client vs server capabilities) | README, package README, coverage tracker and roadmap use this same set of boundaries; historical changelog entries remain historical |
| P1 F03 | A green CI check can omit expected integrations. Recent run 40239398233 skipped four server Spark cases and one Azurite case | Declare required integrations, fail if unavailable, and record exact skips with reasons. Explicit Spark path in PR #21 enabled the cases; CI run 40384002348 exposed a root-owned-file permission failure. The test harness now applies the shared oracle permission handoff; rerun required |
| P1 F04 | `run-tests.sh emulation` runs the cross-engine suite before it starts the emulators | On a clean machine, bring up dependencies before dependent suites; prove the documented command executes the selected integrations |
| P1 F05 | Native .NET Azure/GCS FileIO and generic JDBC-provider guarantees can be inferred from broader vendor/provider wording | Explicitly limit shipped storage to local/S3 and proven SQL provider to SQLite, or add real implementations/provider conformance runs |
| P1 F06 | Cloud semantics are emulator-verified, not service-verified | For the full stack, declare which cloud integrations are supported vs experimental; obtain real AWS and cloud-delegation evidence for every supported path |
| P1 F07 | Public API surface has no checked-in compatibility baseline; XML-doc warnings are suppressed | Review and capture supported APIs for included packages (especially mutations/staging, reads, mapping and EF options); agree compatibility expectations and examples |
| P1 F08 | Memory/size claims need a workload contract | State bounded batch assumptions and measure representative data. `TableWriter` groups all rows, Parquet writes buffer a full file, default row group can span that file, and change-feed snapshots buffer changes; scan streaming alone is not a total-memory bound |
| P2 F09 | Scheduled-verification wording disagrees with dispatch-only workflows; Mumbling fuzz target absent from CI matrix; mutation threshold is zero | Decide required cadence and targets, then configure them or describe checks as manual/informational |
| P1 F10 | The selected full stack includes a server whose operating contract is not formal | Define quotas, audit logging/metrics expectations, backup/recovery, deployment artifacts, credential lifecycle and multi-instance behavior. A protocol test alone cannot satisfy these operator scenarios |
| P2 F11 | Package copy and local default version still describe `0.1.0-preview.*` while target release is undecided | Align package README, version defaults, smoke consumers and release notes when scope/version is agreed; do not imply a release now |
| P2 F12 | Runtime/OS support and performance targets are not formal | List supported TFMs, OS/architectures/native requirements and data sizes; add only the corresponding build/runtime and benchmark checks |
| P1 F13 | Confirmed write-default defect: unmapped typed columns are left null; optional fields silently lose the declared default, required fields fail | Apply declared defaults when a typed member is absent (while preserving explicit-null semantics); cover optional/required fields, supported formats and nested/default behavior, including a cross-engine scenario |

P0/P1/P2 here rank work for formalising the contract. They are not vulnerability severities and do not mean every
missing optional feature is a release blocker. ORC, full v4, a SQL query engine, all EF features and every cloud
provider need not be built for a useful `0.0.1`; exclusions must be explicit.

## Proposed 0.0.1 acceptance record

The owner has selected the full stack including the REST server. Complete the detailed support contract below
before release preparation; implementation breadth alone does not satisfy these gates.

| Decision / gate | Proposed evidence to retain | Current state |
|---|---|---|
| Supported users and workflows | Example: native .NET producer/reader; typed consumer; server operator, each with concrete success scenarios | **Full stack including REST server selected**; concrete scenarios pending |
| Included capabilities | C01–C40 marked included/experimental/excluded, with their exact limitations | Inventory ready; selection pending |
| Packages and platforms | Approved package list, TFMs, OS/architectures and native dependencies | Seven currently packable; full-stack distribution unresolved |
| Compatibility claims | Engine/catalog/version × table format × delete strategy × data type combinations | Existing tests provide a base; assemble an approved matrix |
| Commit/data safety | Required fault, corruption, concurrency, lineage and GC cases; unknown outcomes never cleaned up | Implemented tests; retain fresh required-suite results |
| Verification completeness | Successful checks at exact release SHA, with no unexplained required-case skips | CI running; cloud/optional integrations remain qualified |
| API and examples | Reviewed API snapshot and runnable examples from the packed artifacts | Package consumers exist; API agreement pending |
| Resource envelope | Agreed row/file sizes, memory and latency expectations for scans, writes and maintenance | No formal benchmark baseline found |
| Operational scope | Catalog/server permissions, storage location policy, recovery and observability expectations | Server included; deployment/recovery/credential-lifecycle contract pending |
| Exclusions and documentation | User-facing support statement matching the capability decisions | This baseline is the starting point |
| Release execution | Version cut only after agreement; dry-run packages and CI verified; owner confirms real publish | Deliberately deferred |

## Proposed full-stack acceptance scenarios

These turn the capability inventory into observable release promises. They are proposed acceptance scenarios, not
claims that each has passed. Supported providers, cloud scope and the distribution format must be named for each.

| Scenario | Required observable result | Current evidence / missing work |
|---|---|---|
| S01 Create, load and restart | Create namespaces and v2/v3 tables through the server; insert/read with raw, typed and selected EF/Arrow APIs; restart a SQL-backed server and retain tables/views/history | Catalog/behavior/SQL tests exist; agree the full-stack runnable distribution and an example combining consumers |
| S02 Interoperable mutation | Append/upsert/group-replace/delete/UPDATE/MERGE across supported partition and delete modes; named external engines return the same rows | BDD/regression matrix exists; publish exact supported combinations rather than “all engines” |
| S03 Evolution and historical reads | Rename/add/widen/nested evolution preserve field identity; old snapshot/ref/instant reads use documented schemas and defaults | Strong existing read/evolution tests; F13 blocks declaring complete typed write-default semantics |
| S04 Concurrency and atomic saves | Two writers either serializably succeed or return a documented conflict; multi-table REST/EF commits leave all-or-none state under failure | Interleaving/staging/EF transaction tests; Glue must remain explicitly non-atomic across tables |
| S05 Authorization and confinement | Anonymous/reader/writer/admin behavior is explicit; grants, storage roots and signing/vending never authorize outside their table scope | Authorization/location/signing/vending tests; choose supported identity-provider and deployment configuration |
| S06 Delegation and credential lifetime | Supported clients can create/load/read/write, refresh expired credentials, and recover after a replayed response has credentials removed | Server refresh route exists; .NET credential-refresh API missing. This cross-capability flow needs explicit qualification or completion |
| S07 Server-planned reads | Our client and supported external clients plan/read inline and async/task plans, with each supported delete mode and cancellation | Existing plan tests; newly enabled Spark flow needs green CI after ownership correction |
| S08 Lifecycle and retention | Ref/WAP operations, maintenance, purge and dry-run orphan cleanup obey lineage, reachability, age and `gc.enabled`; feed replay agrees with snapshots | Local/model/fault/engine cases exist; specify partial-progress and expired-history behavior |
| S09 Multi-instance recovery | Shared SQL state handles racing commits, duplicate requests and plan IDs across instances; restart/backup/restore procedure preserves recoverable state | Shared-state/concurrency tests cover portions; quota and operator recovery/backup expectations remain to define |
| S10 Consumption and platforms | Each included library is installable from the release feed; server has a runnable/publishable artifact and example configuration; declared native dependencies work on supported platforms | Seven-package smoke test exists; four libraries currently excluded; no approved OS/architecture matrix or release server artifact |
| S11 Query and resource behavior | Typed/EF semantics match documented LINQ behavior; query diagnostics expose client work; representative scan/write/maintenance sizes stay inside declared resource targets | Translation matrix exists; no benchmark/resource envelope yet; default row groups and writes can be large |
| S12 Real-service integrations | Supported AWS catalog/storage/Athena/managed-job and GCS/ADLS delegation paths run against real services with appropriate isolation | Emulated/scripted evidence only; either obtain real-service results or explicitly mark these paths experimental |

## Execution evidence from this assessment

Fresh local verification completed on the assessment checkout:

| Command / check | Result |
|---|---|
| `tools/test-projects.sh test unit` | 1,131 passed, zero failed/skipped: Spec 816, NetStandard 109, LINQ 84, EF Core 33, REST catalog behavior 89 |
| `MOTO_PORT=5050 dotnet test tests/Iceberg.Net.RestServer.Tests` | 141 passed, 14 skipped, zero failed; skips cover unavailable Python/Docker/emulator paths |
| Typed-default public API probe | Reproduced F13 for optional and required unmapped columns |
| `python3 tools/spec_trace.py matrix --check` | 110 linked sections, zero unknown; matrix summary: 73/88 table+Puffin, 32/32 implemented REST operations |
| `dotnet run scripts/ParseChangelog.cs` | Valid unchanged alpha section; no new release cut |
| Relative documentation file links / `git diff --check` | Passed |

The first local REST server run used default port 5000 and reported 141 passed, 13 skipped and one failed moto STS
case. This Mac's port-5000 service satisfied the reachability probe without being moto. Repeating with the documented
`MOTO_PORT=5050` correctly skipped that unavailable target and passed the remaining tests; no assertion was weakened.
This also illustrates why a reachability check alone does not prove an emulator's identity.

Docker is unavailable on this workstation, so fresh Spark/Trino/reference-server/cloud verification cannot be
claimed from local runs. The cited CI run is separate evidence: Behaviour 65 and Ecosystem 58 passed; server Spark
execution exposed the ownership failure described below. The harness correction requires another CI run.
Earlier package smoke testing in this work packed and consumed exactly seven packages at `0.1.0-preview.1`;
that tested package plumbing, not approval or verification of a `0.0.1` release.

## Confirmed behavior and implementation follow-up

### Typed write-default defect and correction (F13)

A local probe creates a v3 table with `Id` and an unmapped integer `Extra` whose `write-default` is 7, then inserts a
model that has only `Id` through the public typed insert API. Observed results:

```text
required=False: extra=null; expected write default=7
required=True: ArgumentException: Required column Extra contains nulls
```

This is consistent with [typed write validation](../src/Iceberg.Net.Linq/Writes/IcebergWrites.cs) allowing a required
unmapped field when a write default exists, while [StructBinding.ToRow](../src/Iceberg.Net.Linq/Mapping/ReflectionRowMapper.cs)
allocates a null-filled row and populates only mapped members. No writer application of `WriteDefault` was found.
The probe and output are retained under gitignored `temp/capability-audit/default-probe/` and
`temp/capability-audit/default-probe.log`. This is a confirmed implementation gap, not just a missing test tag.
The mapper now fills only unmapped members, including nested structs, from independently decoded write defaults.
New unit cases cover both file formats, explicit nulls/values, nested mutable defaults and preservation of existing
unmapped values in MERGE. The BDD scenario compares Parquet with PyIceberg and Avro with Spark; CI evidence is pending.

### Enabling the server Spark tests exposed a Docker ownership issue (F03)

[CI run 40384002348](https://github.com/rtkelly-labs/Iceberg.NET/actions/runs/40384002348) ran the formerly skipped
Spark cases after the jar-path correction. `Spark_and_the_dotnet_client_share_tables_through_the_server` failed with
`UnauthorizedAccessException` when the host read a Spark-written position-delete file. Spark ran as root inside
Docker. The other three Spark cases no longer skipped. The server result was 153 passed, one failed and one
Azurite skip; Behaviour and Ecosystem passed 65 and 58 cases respectively.

The harness now uses the same post-process shared-mount permission handoff as `SparkOracle.ReadTablesAsync`, preserving
the Spark exit code. This repairs the test environment; [CI run 40385929772](https://github.com/rtkelly-labs/Iceberg.NET/actions/runs/40385929772) passed the unit,
cross-engine and regression jobs after the correction, verifying that flow.
