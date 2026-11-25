# The .NET ecosystem around Lake.NET

**Status:** reference, checked 2026-10-31. The rule: **reuse the ecosystem below the table format, own the table
format** (VISION.md, "Not in scope"). This page lists every library in play: what it does for us, whether it is used
today, and what to adopt next. Versions live in `Directory.Packages.props`; this page doesn't repeat them except
where the version is the point.

Legend: **Use** (as is) · **Wrap** (behind our interface) · **Fork** ([forks.md](forks.md)) · **Adopt** (not used
yet, should be) · **Avoid** (considered, rejected).

## Data plane

| Library | Role | Verdict | Notes |
|---|---|---|---|
| ParquetSharp | Default Parquet backend (Arrow C++ native) | **Wrap** (`IParquetBackend`) | Field IDs on groups and modular encryption work; native, so no WASM/AOT-trimmed/mobile, and it can't be fuzzed in-process |
| Parquet.Net → Lake.Parquet | Managed Parquet backend | **Fork** (rebased stack) | The road to a fully managed, AOT-friendly lake; needs the hostile-input, external-reader and contract work in gap review §10 before it can be the default |
| Apache.Arrow | Columnar in/out (`Iceberg.Net.Arrow`) | **Use** | The interchange with DuckDB.NET (`ExecuteArrowStream`), ML.NET and Polars bindings; the planned `Lake.Arrow` for every format |
| ZstdSharp.Port | Zstd for Puffin (and Parquet through the fork) | **Use** | Managed |
| In-house Avro codec | Iceberg manifests and Avro data files | **Own** | Apache.Avro can't parse logical types on `fixed` (docs/libraries.md) |
| In-house Roaring64 / Puffin | Deletion vectors (Iceberg v3, DuckLake 1.1, later Delta) | **Own** | Moves to `Lake.Core` when Delta needs it; to be checked against Delta's DV test files |
| ORC | Iceberg's second file format | **Own (planned port)** | No usable .NET ORC library; refused today (decision D8) |

## Catalogs and storage

| Library | Role | Verdict | Notes |
|---|---|---|---|
| AWSSDK.S3 / Glue / STS / Athena / EMR Serverless | S3 FileIO, Glue catalog, credential vending, job runners | **Use** | Client-wide retry quota off; conditional-PUT retry recognised as ours (`ThrottlingTests`) |
| Azure.Storage.Files.DataLake / Blobs | ADLS FileIO; the REST server's SAS vending already uses the SDK | **Adopt** for a FileIO, or keep `abfss` refused | Today `abfss://` is refused by `ResolvingFileIO` |
| AWSSDK.S3 against Cloudflare R2 | R2 as an S3-compatible store | **Use** via `S3FileIOOptions.ForR2` | Region `auto`, checksums only when required; Basin Catalog via `RestCatalogOptions.ForBasinCatalog` ([cloudflare.md](cloudflare.md)) |
| Google.Cloud.Storage.V1 | GCS FileIO | **Adopt** for a FileIO, or keep `gs` refused | The REST server already vends GCS tokens to other engines |
| Microsoft.Data.Sqlite, Npgsql (+ EF Core providers) | JDBC-style Iceberg catalog; DuckLake catalogs | **Use** | DuckLake's commit protocol depends on PostgreSQL's isolation and unique-violation behaviour: needs fidelity tests (gap review §4 D4) |
| Entity Framework Core | DuckLake catalog model; Iceberg EF Core provider | **Use** | `DbCommandInterceptor` / `DbTransactionInterceptor` are the fault-injection seam for DuckLake |
| ASP.NET Core | Iceberg REST catalog server | **Use** | Run limit tests on Kestrel, not `TestServer` (which ignores body limits) |
| Microsoft.IdentityModel.JsonWebTokens | Server tokens | **Use** | Negative-token tests missing (gap review S4) |

## Engines and interop

| Library | Role | Verdict | Notes |
|---|---|---|---|
| **DuckDB.NET** (`DuckDB.NET.Data.Full`) | In-process DuckDB: independent Parquet reader in the unit suite; next, the DuckLake and Iceberg oracles without Python | **Use**, upstream-first | 2.0 on `develop`, not yet on NuGet ([duckdb.md](duckdb.md)) |
| Python oracles (PyIceberg, pyarrow, duckdb) | Cross-engine | **Use** under `interop/` | Keep for engines with no .NET binding |
| Spark, Trino, Polaris, Gravitino, Nessie, Lakekeeper | Cross-engine and catalog conformance | **Use** in Docker | Start them as test fixtures (Testcontainers, below) so a missing engine fails instead of skipping |

## Test, verification and quality

| Library | Role | Verdict | Notes |
|---|---|---|---|
| xUnit, Xunit.SkippableFact | Test runner | **Use** | Skips must be counted and gated (gap review E1) |
| Reqnroll | BDD | **Use** | One step library for every format through `IFormatDriver` (verification/bdd.md) |
| FsCheck | Model-based and property tests | **Use** | Add DDL, maintenance and two-writer models |
| SharpFuzz + libFuzzer | Coverage-guided fuzzing | **Use** | Add Parquet (managed backend in-process), Avro data and REST JSON targets |
| Stryker.NET | Mutation testing | **Use** | Ratchet `break`; add DuckLake and commit-protocol configs |
| **Testcontainers for .NET** | Start moto, PostgreSQL, MinIO, Toxiproxy, Spark, Trino and the REST servers from test fixtures | **Adopt** | Replaces shell-started emulators for local runs; a fixture that can't start fails the test, which closes most silent skips |
| **Toxiproxy** (Testcontainers module) | Network faults: latency, resets, timeouts | **Adopt** | Chaos tier for catalogs, S3 and PostgreSQL (verification/chaos.md) |
| **Microsoft.Extensions.TimeProvider.Testing** (`FakeTimeProvider`) | Clock control | **Adopt** | Pass `TimeProvider` through expiry, orphan cleanup and commit timestamps; tests for clock skew (gap review §4 D6) |
| **Verify** (snapshot testing) | Golden metadata JSON, manifests rendered as JSON, OpenAPI | **Adopt** | Golden fixtures of our own output per release (gap review E3) |
| **coverlet + ReportGenerator** | Code coverage with per-package ratchets | **Adopt** | verification/integration-coverage.md |
| **BenchmarkDotNet** | Micro benchmarks, allocations | **Adopt** | verification/benchmarks.md; replaces the ad-hoc bench tools |
| **Microsoft.DotNet.ApiCompat / package validation**, PublicApiAnalyzers | API compatibility per package | **Adopt** | `EnablePackageValidation` against `0.0.1-alpha.1` (gap review E3, releases.md) |
| ArchUnitNET | Architecture rules | **Avoid** for now | `DependencyDirectionTests` reads the csproj graph directly and needs no dependency |
| .NET Aspire | Local orchestration of the emulator estate with a dashboard | **Consider** | Useful for developers; CI stays on Testcontainers |

## Runtime concerns

| Library | Role | Verdict | Notes |
|---|---|---|---|
| OpenTelemetry (`System.Diagnostics` ActivitySource/Meter) | Traces and metrics | **Use** | Request and command counts are the deterministic performance gate (verification/benchmarks.md) |
| Microsoft.Extensions.Resilience / Polly | Retries | **Avoid** in the core | Commit retries are protocol logic (re-plan on conflict, unknown-outcome resolution), not generic resilience; keep them explicit |
| System.Security.Cryptography (AES-GCM, AES-CTR over ECB) | Parquet modular encryption in the fork | **Use** | BCL only; needs the encryption properties tested in gap review PQ5 |
| Trimming / NativeAOT analyzers | Run anywhere | **Adopt** | `IsAotCompatible=true` on packed projects once the managed backend is default; one AOT smoke app per format |
| PolySharp, System.Memory, System.Text.Json | netstandard2.0 builds of Core and Avro | **Use** | Execute on .NET Framework on Windows, not only build (gap review §7) |
