# Formats, specs and reference engines

**Status:** reference, checked 2026-10-31.

## Specs

| Spec | Version(s) we target | Pinned at | Section map | Drift check |
|---|---|---|---|---|
| Apache Iceberg table spec | v1 (read), v2, v3; v4 watched | `apache-iceberg-1.12.0` | `docs/spec/sections.json` | `tools/spec_trace.py drift` (manual dispatch) |
| Iceberg REST catalog OpenAPI | same tag | same | `docs/spec/open-api/` | `tools/openapi_reference.py` |
| Iceberg view spec, Puffin | same tag | same | `sections.json` | same |
| DuckLake | 1.0, 1.1 | `docs/stable` (1.1), on the DuckLake branch | `docs/spec/ducklake-sections.json` | none yet |
| Delta Lake protocol | to decide (reader/writer versions + table features) | to pin: a tagged `PROTOCOL.md` | ⏳ | ⏳ |
| Apache Parquet format | the parts `Lake.Data` relies on (field IDs, logical types, statistics sort orders, modular encryption, variant) | to pin: a `parquet-format` tag | ⏳ | ⏳ |

## Reference engines and the Parquet reader each one uses

Interop only proves something about the reader that ran. Each engine below has its **own** Parquet implementation, so
"Spark reads our files" says nothing about "Trino reads our files". This table is why the cross-engine suites must run
on both of our Parquet backends.

| Engine | Formats it is our oracle for | Its Parquet implementation | Where it runs in our tests |
|---|---|---|---|
| Apache Spark 4 + Iceberg 1.12 | Iceberg (read and write), Delta (planned, with Delta Spark) | parquet-java (parquet-mr), plus Spark's vectorized reader | `cross-engine` CI job, Docker (`interop/spark_oracle.py`) |
| PyIceberg 0.12 | Iceberg | Arrow C++ (pyarrow) | `cross-engine`, Python |
| Trino 478 | Iceberg (and Athena's SQL dialect) | Trino's own reader and writer | `cross-engine`, Docker |
| DuckDB 1.5 / 2.0 | Iceberg (read, `iceberg` extension), DuckLake 1.0 / 1.1 (read and write), Delta (planned, `delta` extension) | DuckDB's own Parquet reader and writer | Python oracles in `cross-engine`; **in-process through DuckDB.NET in the unit suite** (`Iceberg.Net.DuckDbNet.Tests`) |
| delta-rs (`deltalake`) | Delta (planned) | arrow-rs `parquet` | planned |
| Delta Kernel (Rust, via DuckDB `delta`) | Delta (planned, read) | arrow-rs via DuckDB | planned |
| ParquetSharp (Arrow C++) | our default Parquet backend | Arrow C++ | everywhere |
| Lake.Parquet (Parquet.Net fork) | our managed Parquet backend | Parquet.Net | DuckLake branch, `parquet-net` job |

**Consequence for the Parquet.Net backend:** files it writes are today read externally only by DuckDB. Spark
(parquet-java), Trino and delta-rs (arrow-rs) must read them before it can be a default (gap review §10, PQ2).

## Catalogs and stores per format

| Format | Catalogs | Commit atomicity | Stores |
|---|---|---|---|
| Iceberg | in-memory, Glue, JDBC/SQL, REST (Polaris, Gravitino, Nessie, Lakekeeper, Java fixture, our server) | compare-and-swap of the metadata pointer | local, S3 (`s3`, `s3a`, `s3n`); others refused |
| DuckLake | SQLite, PostgreSQL (EF Core) | one SQL transaction | local (S3 untested) |
| Delta | none: storage only | put-if-absent of `_delta_log/NNN.json` | needs conditional create on every store; S3 `If-None-Match` |

## Versions in use (to be consolidated into `interop/engines.json`)

| Engine | Version | Pinned in |
|---|---|---|
| Spark / Iceberg runtime | 4.0.1 / 1.12.0 | `emulation/up.sh`, `SparkOracle.cs` |
| Trino | 478 | `emulation/docker-compose.yml` |
| PyIceberg | 0.12 | `interop/pyproject.toml` |
| DuckDB (Python oracles) | 1.5.5 (Iceberg, DuckLake 1.0), 2.0.0 (DuckLake 1.1) | `interop/pyproject.toml`, `ci.yml` on the DuckLake branch |
| DuckDB (in-process) | 1.5.6 via DuckDB.NET 1.5.6.1 | `Directory.Packages.props` |
| Polaris / Gravitino / Nessie / Lakekeeper | 1.8 / 1.3 / 0.109 / 0.14.1 | `emulation/` |
| moto | pinned per service | `emulation/docker-compose.yml` |
