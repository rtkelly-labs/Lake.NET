# iceberg-dotnet documentation

| Doc | Read it for |
|---|---|
| [spec-coverage.md](spec-coverage.md) | **The foundation.** Section-by-section status against the table spec (v1–v3), REST, Puffin and view specs, with the test that pins each item |
| [architecture.md](architecture.md) | Layers, the metadata-as-value model, the metastore commit protocol, retries, reading |
| [state-management.md](state-management.md) | What Iceberg's release history taught about table state (sequence numbers, row lineage, unknown commits, idempotency, isolation, maintenance) and where we stand |
| [ports-and-engines.md](ports-and-engines.md) | Other implementations (Java, PyIceberg, Go, Rust, C++, .NET attempts) and reference engines (Spark runtime, Athena, Trino, Flink, DuckDB, Snowflake) |
| [catalogs.md](catalogs.md) | Integration challenges for each catalog provider: Glue, Glue/S3 Tables REST, Polaris, Unity, Nessie, Lakekeeper, Gravitino, Hive, JDBC, DynamoDB |
| [write-modes.md](write-modes.md) | Insert, upsert by key, group key replacement, deletes — and the copy-on-write / position / equality delete mechanisms |
| [upstream/parquet-net-group-field-ids.md](upstream/parquet-net-group-field-ids.md) | Draft Parquet.Net issue + verified minimal patch for field IDs on struct/list/map |
| [puffin.md](puffin.md) | Puffin files, deletion vectors, Theta sketches, the v2→v3 position-delete converter, NDV statistics |
| [parquet-sourcegenerator.md](parquet-sourcegenerator.md) | Using Parquet.SourceGenerator as a typed Iceberg write path: measured gaps and where to fix each |
| [libraries.md](libraries.md) | Libraries under the hood (Avro, Parquet, ORC, compression, Roaring/DVs, Puffin, sketches, Arrow, SigV4) and CLR type pitfalls |
| [emulation.md](emulation.md) | The local AWS + REST emulation stack, its fidelity shims, and known gaps |
| [regression-suite.md](regression-suite.md) | Test tiers, running in emulation and AWS, environment variables, CI |
| [roadmap.md](roadmap.md) | Milestones M0–M4, breaking out into its own repo, open questions |
| [research/ecosystem-2026-10.md](research/ecosystem-2026-10.md) | Sourced research snapshot (specs, releases, ports, engines, catalogs, .NET libraries) |
