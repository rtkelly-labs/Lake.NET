# iceberg-dotnet documentation

| Doc | Read it for |
|---|---|
| [dependencies.md](dependencies.md) | Dependency update PRs (Renovate): what it manages, which updates merge themselves, how to run it locally without a hosted service |
| [capability-baseline.md](capability-baseline.md) | **Scope before 0.0.1:** all 12 projects, 40 capabilities, evidence boundaries, assessment findings and proposed acceptance criteria |
| [handover.md](handover.md) | **Start here when picking the project up:** release and contribution workflow, test inventory, local setup, open items and next steps |
| [spec-coverage.md](spec-coverage.md) | **The foundation.** Section-by-section status against the table spec (v1–v3), REST, Puffin and view specs, with the test that pins each item |
| [architecture.md](architecture.md) | Layers, the metadata-as-value model, the metastore commit protocol, retries, reading |
| [state-management.md](state-management.md) | What Iceberg's release history taught about table state (sequence numbers, row lineage, unknown commits, idempotency, isolation, maintenance) and where we stand |
| [ports-and-engines.md](ports-and-engines.md) | Other implementations (Java, PyIceberg, Go, Rust, C++, .NET attempts) and reference engines (Spark runtime, Athena, Trino, Flink, DuckDB, Snowflake) |
| [catalogs.md](catalogs.md) | Integration challenges for each catalog provider: Glue, Glue/S3 Tables REST, Polaris, Unity, Nessie, Lakekeeper, Gravitino, Hive, JDBC, DynamoDB |
| [rest-server.md](rest-server.md) | The REST catalog server (`Iceberg.Net.RestServer`): running it, storage, authentication and role grants, server-side and incremental scan planning, credential vending (S3, Azure, GCS), remote signing, idempotency keys shared across instances, endpoints, what is not implemented |
| [update-merge.md](update-merge.md) | UPDATE and MERGE: clause semantics, strategies, cardinality, row lineage, concurrency |
| [read-path.md](read-path.md) | Scans: pruning levels (manifests, files, row groups), projection by field ID, streaming, time travel semantics |
| [snapshot-management.md](snapshot-management.md) | Branches, tags, rollback, cherry-pick, write-audit-publish, per-ref retention, metadata tables, change feed |
| [linq.md](linq.md) | The typed layer: mapping rules, what LINQ pushes down vs evaluates in memory, strict mode, typed writes |
| [efcore.md](efcore.md) | The EF Core provider: model mapping, query delegation to the LINQ layer, `SaveChanges` as one MERGE per table, concurrency, atomicity across tables |
| [write-modes.md](write-modes.md) | Insert, upsert by key, group key replacement, deletes — and the copy-on-write / position / equality delete mechanisms |
| [upstream/README.md](upstream/README.md) | Proposed upstream tickets (Parquet.Net, PyIceberg, Gravitino) and the Parquet.SourceGenerator patch, ready to file; none filed |
| [puffin.md](puffin.md) | Puffin files, deletion vectors, Theta sketches, the v2→v3 position-delete converter, NDV statistics |
| [ducklake.md](ducklake.md) | DuckLake 1.0 on the same data plane (`Iceberg.Net.DuckLake`): what Iceberg.NET contributed and where it got in the way, usage, how the repository's rules apply, where DuckDB and the DuckLake spec differ, what is not implemented |
| [lake-parquet/](lake-parquet/README.md) | **The Parquet.Net fork, Lake.Parquet:** its decisions, vision, stack rules and record format, rebaselining and history, testing and benchmarks, upstreaming. Decisions are made here; the fork carries code and per-change records |
| [parquet-sourcegenerator.md](parquet-sourcegenerator.md) | Using Parquet.SourceGenerator as a typed Iceberg write path: measured gaps and where to fix each |
| [libraries.md](libraries.md) | Libraries under the hood (Avro, Parquet, ORC, compression, Roaring/DVs, Puffin, sketches, Arrow, SigV4) and CLR type pitfalls |
| [emulation.md](emulation.md) | The local AWS + REST emulation stack, its fidelity shims, and known gaps |
| [regression-suite.md](regression-suite.md) | Test tiers, running in emulation and AWS, environment variables, CI, shadow comparison |
| `spec-traceability.md` | Generated, not committed (`python3 tools/spec_trace.py matrix`, or the `spec-traceability` artifact of a CI run): every upstream spec section (pinned at 1.12.0) and the tests linked to it |
| [iceberg-v4.md](iceberg-v4.md) | Following the Iceberg v4 proposals: status of each, what adopting it would mean here, and the plan |
| [telemetry.md](telemetry.md) | Traces and metrics (`ActivitySource`/`Meter` "Iceberg.Net"): every span, tag and instrument, and wiring into OpenTelemetry |
| [spark-and-athena.md](spark-and-athena.md) | Spark procedure SQL and EMR Serverless / Glue job submission (run against moto and Spark), Athena SQL, its Trino dialect and runner (run on Trino and moto), the DuckDB oracle |
| [roadmap.md](roadmap.md) | Delivery milestones M0–M4, the 13 verification paths (built / left / needs action), upstream v4 changes, repository split, open questions |
| [research/ecosystem-2026-10.md](research/ecosystem-2026-10.md) | Sourced research snapshot (specs, releases, ports, engines, catalogs, .NET libraries) |
