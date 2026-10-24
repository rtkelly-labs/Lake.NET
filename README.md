[![AI Assisted](https://img.shields.io/badge/development-AI%20assisted-792ee5.svg?logo=openai&logoColor=white)](AI_POLICY.md)

# Iceberg.NET — a C# port of Apache Iceberg

**Status:** M0 (foundations) — exploratory, **not for production writes yet**.

## Why this exists

Writing to Iceberg tables from C# today means handing the write to Python, Spark or a SQL engine. This
project is **a native .NET Iceberg library**, so a C# producer can commit to a table
directly through Glue (or any REST catalog) with the same guarantees PyIceberg and Java give.

No production-grade .NET Iceberg library exists today (see [docs/ports-and-engines.md](docs/ports-and-engines.md)).

## What works now (M0)

| Area | State |
|---|---|
| Spec model | Types incl. v3 (`timestamp_ns`, `variant`, `geometry`, `geography`, `unknown`, defaults), schemas, field-ID indexing, type promotion rules |
| Transforms | identity, bucket (Murmur3, all spec vectors), truncate, year/month/day/hour, void, unknown-transform passthrough |
| Table metadata | v1 read, v2/v3 read+write, full update/requirement state machine (REST-shaped), metadata & snapshot logs, refs, row lineage |
| Manifests | In-house Avro codec; manifest + manifest list read (v1–v3) and write (v2–v3), field-ID resolution, sequence-number and `first_row_id` inheritance |
| Data | ParquetSharp data files with field IDs and column metrics, every primitive type plus struct/list/map columns (Dremel levels over the physical API); Avro data and delete files read, and written when `write.format.default=avro`; expression-based scan pruning; append, overwrite, file delete; sorted writes |
| Write modes | Pure insert, upsert by key, group key replacement, delete by key / predicate / whole files, UPDATE and MERGE — each one atomic snapshot, re-planned on conflict ([docs/write-modes.md](docs/write-modes.md), [docs/update-merge.md](docs/update-merge.md)) |
| Reads | Streamed one Parquet row group at a time; only the footer and needed column chunks fetched; only projected columns decoded (by field ID); row groups skipped on footer statistics; time travel by snapshot, ref or instant, with the snapshot's own schema; metadata-only counts ([docs/read-path.md](docs/read-path.md)) |
| Snapshot management | Branches and tags, rollback, cherry-pick, fast-forward, write-audit-publish branch writes, per-ref retention, metadata tables, change feed ([docs/snapshot-management.md](docs/snapshot-management.md)) |
| Typed layer | `Iceberg.Net.Linq`: row mapping by field ID, schema from a model, a LINQ provider that pushes filters, projection and time travel into one scan (residual filters in memory, strict mode), typed insert/upsert/`ExecuteDelete`/`ExecuteUpdate`/MERGE ([docs/linq.md](docs/linq.md)) |
| EF Core | `Iceberg.Net.EntityFrameworkCore`: a non-relational EF Core 8 provider over the typed layer, with pushdown, time travel, `ExecuteUpdate`/`ExecuteDelete` and `SaveChanges` (one snapshot per table, concurrency tokens) ([docs/efcore.md](docs/efcore.md)) |
| REST catalog server | `Iceberg.Net.RestServer`: an ASP.NET Core Iceberg REST catalog over a SQL or in-memory store, with bearer/OAuth auth and multi-table transactions; PyIceberg and Spark use it ([docs/rest-server.md](docs/rest-server.md)) |
| Ecosystem | Traces and metrics (`Iceberg.Net` ActivitySource/Meter, [docs/telemetry.md](docs/telemetry.md)); Spark procedure SQL with EMR Serverless and Glue job runners, and Athena SQL with a query runner ([docs/spark-and-athena.md](docs/spark-and-athena.md)) |
| Delete mechanisms | Copy-on-write, v2 position delete files, v3 deletion vectors (one per data file, merged), equality deletes; v2→v3 converter |
| Puffin | Container, `deletion-vector-v1` (Roaring64), `apache-datasketches-theta-v1`; NDV statistics files |
| Foreign Parquet | `ParquetImport` adopts files from other writers (e.g. Parquet.SourceGenerator) via name mapping and type validation |
| Catalogs | In-memory, AWS Glue (VersionId optimistic lock), JDBC/SQL (Java/PyIceberg layout, views in V1), Iceberg REST (OAuth2/bearer, SigV4 for Glue/S3 Tables, config merge, vended credentials, idempotent retries, views, multi-table transactions) |
| Storage | Local and S3 FileIO (conditional create via `If-None-Match`) |
| Maintenance | Snapshot expiry with file deletion and per-ref retention, orphan cleanup, bin-pack and sort compaction with partial progress, position-delete rewrite, merge-append, manifest rewrite |
| Proven interop | Spark + Iceberg Java, PyIceberg, Trino and DuckDB read our tables (deletes, DVs, lineage, nested types) and we read theirs; Java REST, Polaris and JDBC catalogs accept our commits; DataSketches reads our sketches |

Not yet: ORC data files, filters or partitions through lists and maps. See [docs/roadmap.md](docs/roadmap.md).

## Layout

```
Iceberg.NET/
├── src/
│   ├── Iceberg.Net.Core      spec model, transforms, metadata state machine, JSON
│   ├── Iceberg.Net.Avro      Avro container codec + manifest/manifest-list encoding
│   ├── Iceberg.Net.IO        FileIO: local, S3
│   ├── Iceberg.Net.Catalog   ICatalog, metastore commit protocol, in-memory, Glue, REST
│   ├── Iceberg.Net.Puffin    Puffin container, deletion vectors (Roaring64), Theta sketches
│   ├── Iceberg.Net.Data      Parquet files, scans, deletes, snapshot producers, import, statistics
│   ├── Iceberg.Net.Arrow     Arrow record batches in and out
│   ├── Iceberg.Net.Linq      typed mapping, LINQ provider, typed writes
│   ├── Iceberg.Net.EntityFrameworkCore  EF Core 8 provider over the typed layer
│   ├── Iceberg.Net.RestServer           ASP.NET Core Iceberg REST catalog server
│   ├── Iceberg.Net.Spark     Spark procedure SQL, EMR Serverless / Glue job runners
│   └── Iceberg.Net.Athena    Athena SQL and query runner
├── tests/
│   ├── Iceberg.Net.Spec.Tests        L0: spec vectors, state machine, PyIceberg fixtures (no network)
│   ├── Iceberg.Net.Regression.Tests  L1–L3: emulation or AWS, same code
│   ├── Iceberg.Net.Behaviour.Tests   BDD: Gherkin (Reqnroll) specification of write modes and deletes
│   ├── Iceberg.Net.Linq.Tests        typed layer: query matrix against LINQ to Objects
│   ├── Iceberg.Net.EntityFrameworkCore.Tests  EF Core provider
│   ├── Iceberg.Net.RestServer.Tests  REST server: contract, protocol, auth, PyIceberg and Spark against it
│   ├── Iceberg.Net.Ecosystem.Tests   telemetry, Spark/Athena helpers, Spark and DuckDB oracles
│   └── Iceberg.Net.TestSupport       shared target selection and PyIceberg oracle
├── interop/        PyIceberg oracle used by the cross-engine tests
├── emulation/      docker compose: moto (+ fidelity shims) and the Iceberg REST fixture
└── docs/           design, research and runbooks (start at docs/README.md)
```

## Quick start

```bash
cd iceberg-dotnet
./run-tests.sh spec        # L0 only: ~1 s, no network
./run-tests.sh emulation   # starts moto + REST fixture, runs everything that doesn't need AWS
ICEBERG_WAREHOUSE=s3://my-bucket/iceberg-regression ICEBERG_ATHENA_WORKGROUP=primary \
  ./run-tests.sh aws       # real Glue, S3 and Athena (see docs/regression-suite.md first)
```

```csharp
using var catalog = GlueCatalog.Create(new S3FileIO(new S3FileIOOptions()), new GlueCatalogOptions { Warehouse = "s3://bucket/wh" });
var table = await catalog.CreateTableAsync(new CreateTableRequest(
    TableIdentifier.Of("analytics", "events"), schema,
    PartitionSpec.BuilderFor(schema).Day("event_ts").Bucket("id", 16).Build()));

var files = await TableWriter.WriteAsync(table, rows);                 // Parquet, one file per partition
await SnapshotProducer.Append(table).AddFiles(files).CommitAsync();   // optimistic commit with retry
var all = await TableScan.ReadAllAsync(table);
```

Requires the .NET 8 SDK (newer SDKs roll forward), Docker for emulation, and `uv` (or `ICEBERG_PYTHON`)
for the PyIceberg oracle.

---

## 🤖 AI Utilization & Transparency

This project leverages AI-assisted software engineering and agentic workflows for rapid development, test scaffolding, and documentation.

* **Human Accountability:** The maintainer reviews all architecture, code changes, and pull requests, assuming 100% responsibility for the correctness, stability, and security of all merged code.
* **Automated Verification:** All features and contributions are validated by automated test suites (.NET 8 & .NET 9), strict type checking, and automated CI pipelines.
* **Community Policy:** For contributor guidelines regarding generative AI tooling, see [**`AI_POLICY.md`**](AI_POLICY.md).
