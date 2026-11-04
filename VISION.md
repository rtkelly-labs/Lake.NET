# Lake.NET — vision

**Status:** target state, agreed direction, not yet started. The repository, packages and namespaces are still
`Iceberg.Net.*`; the plan to get from here to there is [docs/lake/](docs/lake/README.md).

## What Lake.NET is

A native .NET library for open table formats: **Apache Iceberg, DuckLake and Delta Lake**, read and written from
C# with the same guarantees as each format's reference implementation, without handing the write to Spark,
Python or a SQL engine.

It starts as Iceberg.NET. Building DuckLake on it showed that most of the hard work (Parquet with field IDs
on every node, nested columns, pruning, ranged reads, write-once storage, deletion-vector bitmaps, the oracle
test setup) is the same for every format. One library for all three keeps that shared work in one place.

## Shared below, specific above

The formats agree on the data plane and disagree on metadata. The library is layered to match:

```
 ┌───────────────┬───────────────┬───────────────┐
 │ Lake.Iceberg  │ Lake.DuckLake │ Lake.Delta    │  each format's full model and API: metadata,
 │ catalogs, REST│ SQL catalog   │ _delta_log    │  commits, snapshots, maintenance
 ├───────────────┴───────────────┴───────────────┤
 │ Lake.Data      Parquet data plane (field IDs, nested columns, pruning, statistics)
 │ Lake.Puffin    Puffin container           Lake.Arrow   columnar in/out
 │ Lake.Orc       ORC files (port of Apache ORC Java; Iceberg's second file format)
 ├───────────────────────────────────────────────┤
 │ Lake.Core      types and physical values, expressions, Roaring64, Corruption, telemetry
 │ Lake.IO        local and S3 storage, conditional create, ranged reads
 └───────────────────────────────────────────────┘
```

- **Formats sit side by side and never depend on each other.** `Lake.DuckLake` does not pull in Iceberg's Avro
  manifests or catalogs; `Lake.Iceberg` does not know DuckLake exists.
- **Each format keeps its full API.** There is no lowest-common-denominator `ITable` that hides what a format
  can do. Iceberg branches, DuckLake inlining and Delta table features are exposed as themselves.
- **Code is shared only once two formats use it.** It moves down into `Lake.Core` or `Lake.Data` when a second
  format needs it, never in advance.
- **Typed access (LINQ, EF Core) comes later and is opt-in**, over the shared scan and row model, and only where
  the semantics really are the same.

## Formats

| Format | Today | Target |
|---|---|---|
| **Apache Iceberg** v1–v3 (v4 tracked) | Read and write, Glue / REST / JDBC / in-memory catalogs, REST server, LINQ, EF Core, interop proven with Spark, PyIceberg, Trino, DuckDB | `Lake.Iceberg.*`; the most complete format and the reference for the shared layers |
| **DuckLake** 1.0, 1.1 | Evaluation build, not packed: SQLite and PostgreSQL catalogs through EF Core, read and write in both directions with DuckDB | `Lake.DuckLake`; depends only on the shared layers |
| **Delta Lake** | Not started | `Lake.Delta`: log replay, checkpoints, column mapping, deletion vectors, then writes |

## Rules every format follows

These are the rules Iceberg.NET already follows ([AGENTS.md](AGENTS.md)). They apply to every format:

1. **The spec wins.** Tests are tagged with the spec section they verify, and the traceability matrix covers
   every format's spec.
2. **Resolve columns by field ID**, never by name or position, wherever the format provides one.
3. **Files are written once.** Nothing is overwritten. After an unknown commit outcome, nothing is deleted.
4. **Behaviour other engines can observe gets a cross-engine test** against the format's reference
   implementation (Iceberg: Spark/PyIceberg; DuckLake: DuckDB; Delta: delta-rs/Spark).
5. **Decoders of untrusted bytes follow the `Corruption` contract** and are fuzzed.
6. **Self-contained.** No references to code outside the repository; the only fork we depend on
   (Parquet.Net, `rtkelly-labs/parquet-dotnet`) carries behavioural changes in its own repository, and this
   repository's build gives it a Lake-owned identity, so it never clashes with Parquet.Net in an application.

## Packages and naming

- Product and repository: **Lake.NET** (`rtkelly-labs/Lake.NET`).
- Package IDs and namespaces: **`Lake.*`**, without `.Net`, as for the Parquet.Net fork's optional `Lake.Parquet` build. `Lake.Net` itself
  is not a package.
- Format packages: `Lake.<Format>[.<Part>]`, e.g. `Lake.Iceberg.Catalog`, `Lake.DuckLake`.
- Each package has its own version, changelog and recorded public API, so it is released when it changes
  and not otherwise ([docs/lake/releases.md](docs/lake/releases.md)).
- Licence: **Apache-2.0**, matching the rest of the ecosystem, with a `NOTICE` file crediting Apache Iceberg, whose Java
  implementation parts of the code follow, and Apache ORC once the port lands. The Parquet.Net fork stays MIT, as
  Parquet.Net is.
- The `Iceberg.Net.*` packages (`0.0.1-alpha.1`, published only to test the release lifecycle) are deprecated in
  favour of their `Lake.*` successors, then unlisted.

## Not in scope

- **A query engine.** Lake.NET reads and writes tables; DuckDB, Spark, Trino and Athena query them.
- **Wrapping a native implementation** (delta-rs, iceberg-rust, ORC C++) over FFI. Everything is managed C#, apart from
  ParquetSharp's native Parquet library, which the managed Parquet.Net backend can replace.
- **Converting between formats** (as Delta UniForm does) until each format stands on its own.

## How we know it's working

- Every format passes its reference implementation's reads and writes, in both directions, in CI.
- Every spec section is mapped to its tests in the traceability matrix, for all three formats.
- A dependency test fails the build if one format package references another, or if a shared layer references a
  format package.
- An application that uses only one format pulls in only that format and the shared layers.
