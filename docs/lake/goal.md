# Lake.NET: the goal

**Status:** agreed direction, partly built. This page defines what "done" means for the whole project, so that every
pull request can say which part of the goal it moves. The vision (why) is [VISION.md](../../VISION.md); the plan
(how) is this folder's [README](README.md); the gaps measured against this goal are in
[verification/gap-review-2026-10.md](verification/gap-review-2026-10.md).

## The goal

> **Lake.NET 1.0 is a managed .NET library that reads, writes and maintains Apache Iceberg, DuckLake and Delta Lake
> tables with the same guarantees as each format's reference implementation, safely under concurrency, faults and
> hostile input, and proves every capability it claims with a test that actually ran against that format's
> reference engines.**

Every word is load-bearing:

| Phrase | Means | Measured by |
|---|---|---|
| *managed .NET* | No FFI wrapper of another table-format implementation; runs wherever .NET runs once the managed Parquet backend is the default (WASM, mobile, AOT) | `IsAotCompatible` + trimming analyzers clean on every packed project; a NativeAOT smoke app per format |
| *reads, writes and maintains* | Read, every write mode, schema/partition evolution, time travel, compaction, expiry/vacuum, orphan cleanup | Per-format `<format>-support.json` with every spec section `supported`, `refused` or `planned` |
| *same guarantees as the reference* | Spec-exact behaviour where engines can observe it | Spec traceability: every `supported` section has an executed test |
| *safely under concurrency* | Serializable commits, conflicts detected or retried, no lost or duplicated rows | Soak and model-based tests per format; two-writer races against the reference engine |
| *faults* | Before-or-after state, validator clean, nothing possibly committed deleted, retries converge | Deterministic fault injection of every commit **and maintenance** path, per format |
| *hostile input* | Untrusted bytes fail with a format error, never a crash, hang, unbounded allocation or wrong answer; a hostile catalog client cannot escape its grants | Corruption contract + fuzz targets for every decoder (Parquet included); REST authorization matrix; threat model in SECURITY.md |
| *proves … with a test that actually ran* | A skipped test proves nothing | CI fails closed: every required integration must execute; pathway coverage from TRX, not from attributes |
| *reference engines* | Iceberg: Spark, PyIceberg, Trino, DuckDB; DuckLake: DuckDB 1.5 (1.0) and 2.0 (1.1); Delta: delta-rs, Spark, DuckDB `delta` | Cross-engine suites in both directions, versions pinned in one manifest |

## Definition of done

Lake.NET 1.0 ships when every row is ✅ for every format. Status columns are updated by the PR that changes them.

✅ done · 🟡 partial · ⏳ not started · — not applicable

| # | Criterion | Iceberg | DuckLake | Delta | Shared |
|---|---|---|---|---|---|
| G1 | Read, write, maintain: every spec section `supported`, `refused` or `planned`, CI-checked | 🟡 73/88 linked, no support file | 🟡 65/66 linked, branch only | ⏳ | — |
| G2 | Reference-engine interop in both directions, every write mode | ✅ Spark, PyIceberg, Trino, DuckDB (read) | 🟡 DuckDB, serial only | ⏳ | — |
| G3 | Committed fixtures from every reference writer **and from our own released versions** | 🟡 third-party + our own (`OwnGoldenFixtureTests`; release-gated) | ⏳ live-only | ⏳ DAT | — |
| G4 | Validator after every scenario, model step, fault and soak run | ✅ | ✅ | ⏳ | — |
| G5 | Deterministic faults on every commit **and maintenance** path | 🟡 writes only, in-memory catalog | ⏳ | ⏳ | 🟡 `FaultInjectingFileIO` writes only |
| G6 | Concurrency: soak and two-writer races against the reference engine | 🟡 15 s soak | ⏳ | ⏳ | — |
| G7 | Every decoder of untrusted bytes fuzzed and corruption-tested | 🟡 no Parquet, Avro data or REST JSON | 🟡 two targets | ⏳ | ⏳ Parquet (both backends) |
| G8 | Security: threat model, authorization matrix, credential scoping, storage confinement | 🟡 see gap review §2 | 🟡 path and SQL confinement open | ⏳ | 🟡 scheme routing ✅ |
| G9 | Storage conformance suite on every FileIO; object stores beyond S3 or explicit refusal | 🟡 local + S3; others refused ✅ | ⏳ local only | ⏳ | ⏳ |
| G10 | CI fails closed; scheduled heavy suites; code and pathway coverage with ratchets | 🟡 schedules ✅, unit-suite coverage ratchet ✅; fail-closed engines in #26, pathway coverage ⏳ | ⏳ | ⏳ | 🟡 |
| G11 | Resource envelope: memory, request counts and time budgets at scale | ⏳ | 🟡 bench tool | ⏳ | ⏳ |
| G12 | Platforms: Linux, Windows, macOS; net10 + netstandard2.0 consumers executed; both Parquet backends | 🟡 Linux only | 🟡 both backends on branch | ⏳ | ⏳ |
| G13 | Dependency direction enforced; one format never pulls in another | ✅ `DependencyDirectionTests` | ⏳ needs phase 3 | ⏳ | ✅ |
| G14 | Per-package versions, API files, package validation against the last release | 🟡 package validation on every pack | ⏳ | ⏳ | 🟡 |

## Workstreams

Each workstream owns criteria above, and maps to issues labelled `lake-plan`.

| Workstream | Owns | First deliverables |
|---|---|---|
| **W1 Evidence** | G10 | Land PR #26's required integrations; extend fail-closed to every engine; cron on fuzz/mutation/upkeep; TRX skip gate |
| **W2 Security** | G8 | ✅ IAM wildcard and out-of-table snapshot files; ✅ scheme routing; JWT negatives; authorization matrix; planner refuses out-of-table manifests; SECURITY.md threat model |
| **W3 Durability** | G5, G6 | Maintenance in `FaultInjectionTests`; read/delete/torn-write faults; DuckLake `DbCommandInterceptor` faults + idempotency token; DuckDB concurrent-writer oracle |
| **W4 Storage** | G9 | `FileIOConformanceTests<T>` (local, moto, MinIO, remote-signed); DuckLake on S3; Azure/GCS FileIO or permanent refusal |
| **W5 Interop and fixtures** | G2, G3 | Golden fixtures of our own releases; DuckDB-written DuckLake fixtures; one engine-version manifest (`interop/engines.json`); ✅ DuckDB in-process Parquet reader in the unit suite |
| **W6 Data plane** | G7, G12 (backends) | ✅ malformed-file suite in the fork (`lake(0026)`: 732 failing cases, F1–F12); next, one fix change per finding, starting with the varint hang (F1) and stale pooled bytes (F7). Parquet.Net fork: hostile-input suite, external readers, contract suite gating the daily rebase ([reference/forks.md](reference/forks.md)) |
| **W7 Shared layers** | G13 | Phase 3 extraction; `IFormatDriver` and `Lake.TestSupport` before any Delta code |
| **W8 Delta** | all Delta cells | ✅ Writer-feature table ([`docs/spec/delta-support.json`](../spec/delta-support.json), checked by `SupportDeclarationTests`); then the DAT reader, then writes ([delta.md](delta.md), gap review §8) |
| **W9 Performance** | G11 | Micro and scenario benchmarks; request-count gates; resource envelope |
| **W10 Release** | G14, G12 | Package validation, API files, Windows/macOS jobs, AOT smoke apps |

## How the .NET ecosystem is used

The rule is **reuse the ecosystem below the table format, own the table format itself** (VISION.md, "Not in
scope"). What we take, what we fork and why: [reference/dotnet-ecosystem.md](reference/dotnet-ecosystem.md).
How forks are run so that changes go back upstream: [reference/forks.md](reference/forks.md). DuckDB, DuckDB.NET and
the DuckDB 2.0 transition: [reference/duckdb.md](reference/duckdb.md).
