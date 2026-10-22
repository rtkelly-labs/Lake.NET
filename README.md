# iceberg-dotnet — a C# port of Apache Iceberg

**Status:** M0 (foundations) — exploratory, on branch only, **not for production writes yet**.
**Scope of this folder:** fully self-contained. Nothing outside `iceberg-dotnet/` depends on it and it
depends on nothing outside itself (apart from the path-filtered workflow
`.github/workflows/iceberg-dotnet.yml`), so it can be lifted into its own repository unchanged.

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
| Data | ParquetSharp data files with field IDs and column metrics (incl. decimal, uuid); scan planning; append, copy-on-write overwrite, file delete; metadata-only snapshot expiry |
| Row-level deletes | Merge-on-read reads and writes: v2 position delete files, v3 deletion vectors (one per data file, merged), v2→v3 converter |
| Puffin | Container, `deletion-vector-v1` (Roaring64), `apache-datasketches-theta-v1`; NDV statistics files |
| Foreign Parquet | `ParquetImport` adopts files from other writers (e.g. Parquet.SourceGenerator) via name mapping and type validation |
| Catalogs | In-memory, AWS Glue (VersionId optimistic lock), Iceberg REST (OAuth2/bearer, config merge, vended credentials, idempotency keys) |
| Storage | Local and S3 FileIO (conditional create via `If-None-Match`) |
| Proven interop | Reads PyIceberg-written tables; PyIceberg reads ours (incl. our deletes and DVs); Java REST reference server accepts our commits; DataSketches reads our sketches |

Not yet: equality deletes, expressions & pruning (so deletes are by position), nested Parquet columns, ORC,
views, SigV4 REST. See [docs/roadmap.md](docs/roadmap.md).

## Layout

```
iceberg-dotnet/
├── src/
│   ├── Iceberg.Net.Core      spec model, transforms, metadata state machine, JSON
│   ├── Iceberg.Net.Avro      Avro container codec + manifest/manifest-list encoding
│   ├── Iceberg.Net.IO        FileIO: local, S3
│   ├── Iceberg.Net.Catalog   ICatalog, metastore commit protocol, in-memory, Glue, REST
│   ├── Iceberg.Net.Puffin    Puffin container, deletion vectors (Roaring64), Theta sketches
│   └── Iceberg.Net.Data      Parquet files, scans, deletes, snapshot producers, import, statistics
├── tests/
│   ├── Iceberg.Net.Spec.Tests        L0: spec vectors, state machine, PyIceberg fixtures (no network)
│   └── Iceberg.Net.Regression.Tests  L1–L3: emulation or AWS, same code
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
