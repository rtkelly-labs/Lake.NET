# Roadmap

Ordered by what unblocks production use cases soonest, not by spec order. Each milestone ends with its rows in
[spec-coverage.md](spec-coverage.md) flipped and tests in the tier that proves them.

| Milestone | Outcome | Contents | Proven by |
|---|---|---|---|
| **M0 — foundations** ✅ | C# can create, append to, overwrite and read flat v2/v3 tables in Glue and REST catalogs, interoperably | spec model, transforms, state machine, Avro, Parquet (flat), Glue/REST/in-memory, emulation + regression suite | L0 90, L1/L2 21 |
| **M1 — production-grade append** | A C# producer can safely own appends to an Athena-queried Glue table | nested types, decimal/uuid/fixed columns, `UpdateSchema` builder API, merge-append (manifest compaction on commit), serializable `Overwrite` validation, `DropTable(purge)` deleting data, SigV4 handler (Glue/S3 Tables REST), JDBC catalog, Idempotency-Key retries, `add_files`, packaging (NuGet, net8.0 + netstandard2.0 core) | Athena L3 green in a sandbox account; soak test with concurrent writers |
| **M2 — merge-on-read reads** | Read any table Athena/Spark/Trino have mutated | expressions + partition/metrics pruning, position deletes, equality deletes, delete applicability by sequence number, Puffin, DV reads (Roaring64 portable) | Athena-written DELETE/MERGE tables read correctly; Spark DV fixtures |
| **M3 — row-level writes** | C# upserts / deletes without Athena | RowDelta, CoW delete/update by predicate, DV writes on v3 (one DV per file, merge with existing), position deletes on v2, conflict validation (Java `MergingSnapshotProducer` semantics), Arrow `RecordBatch` API | PyIceberg + Spark + Athena read our MoR output |
| **M4 — maintenance & breadth** | Self-sufficient operations | expire with file deletion, orphan cleanup, rewrite data files / manifests, views, multi-table transactions, scan planning endpoint, more REST servers in CI | |

## Breaking out

The folder is designed to be extracted into its own repository (`rtkelly-labs/Iceberg.NET`) without changes:
no references outside `iceberg-dotnet/`, its own `global.json`, `Directory.*.props`, `.gitignore`, docs, CI
workflow (move `.github/workflows/iceberg-dotnet.yml` and drop the `iceberg-dotnet/` path prefixes). Suggested
moment: when M1 starts and it needs NuGet publishing.

## Open questions

1. **Package boundary for non-.NET 8 consumers.** Some consumer services are `net472`. `Core` and `Avro` can target
   `netstandard2.0` with modest changes (no `Rune`, `BinaryPrimitives` via `System.Memory`); ParquetSharp supports
   `net471`. Worth doing in M1 if a net472 producer is in scope.
2. **Arrow as the public row API** (M3) vs keeping `object?[]` for simple producers — likely both.
3. **Upstream?** An Apache `iceberg-dotnet` does not exist; if this matures, proposing it upstream (or contributing
   to an existing community effort) would share the maintenance cost. Decide after M2.
