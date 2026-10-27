# Changelog

All notable changes to **Iceberg.NET** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). `CHANGELOG.md` is the release authority:
`.github/workflows/release.yml` publishes the version of the first cut section below `[Unreleased]`.

---

## [Unreleased]

### Added
- `Iceberg.Net.Data.ParquetNet` (not yet packed): Parquet.Net, fully managed (no native binaries), as the Parquet
  library of `Iceberg.Net.Data`, from the `rtkelly-labs/parquet-dotnet` fork at `external/parquet-dotnet` (a git
  submodule). `ParquetNet.UseAsDefault()`, or `ICEBERG_PARQUET_BACKEND=parquet-net`, switches every read and write;
  ParquetSharp stays the default. Both backends write the same files (schema, field IDs, annotations, statistics,
  Zstd, encryption) and CI runs the unit and DuckLake suites (with DuckDB's oracles) on Parquet.Net too.
  `ParquetFiles.BackendName` reports the one in use; `tools/ParquetBackendBench` compares them.
- `Iceberg.Net.Data`: `ParquetWriteOptions.Annotations` writes columns narrower than their Iceberg type (unsigned and
  narrow integers, millisecond timestamps, UTC times, INTERVAL); the reader decodes unsigned and millisecond columns
  (values and footer statistics) from any writer's annotations. `ParquetInspection.InspectAsync` reads an existing
  file's footer (fields, sizes, name-matched statistics and type compatibility); `DataFileReadOptions.MatchByName`
  reads a file by column names, ignoring its field IDs. `ParquetWriteOptions.EncryptionKey` /
  `DataFileReadOptions.DecryptionKey`: Parquet modular encryption (AES-GCM, encrypted footer).
- `Iceberg.Net.DuckLake` (not yet packed): the DuckLake 1.0 and 1.1 table formats on the Iceberg data plane — an
  EF Core catalog on SQLite or PostgreSQL (the databases DuckDB attaches), nested schemas and the `null` type (1.1), snapshots and time travel, schema/table DDL, inserts with file and table statistics,
  merge-on-read deletes, UPDATE, schema evolution, partitioned writes, DuckDB's inlined rows and partial files on
  read, the spec's conflict resolution on commit, `DuckLakeValidator` for whole-lake consistency checks, and maintenance
  (merging and rewriting files, snapshot expiry, scheduled and orphaned file cleanup) interoperable with DuckDB's.
  Data inlining (small inserts and deletes kept in the catalog database, as DuckDB writes them) and flushing it to Parquet.
  Tags and comments, table renames, views, macros and sort orders, as DuckDB writes them. The unsigned integers,
  `timestamp_s`/`timestamp_ms`, `timetz` and `interval`, stored with DuckDB's Parquet annotations and inlining encodings.
  Registering existing Parquet files under a name mapping (`AddDataFilesAsync`, as `ducklake_add_data_files`), and
  reading files DuckDB registered that way. Typed reads: `ReadClrAsync` (natural CLR values) and `ReadAsync<T>` (rows bound to
  records by column name). Format 1.1's Puffin deletion vectors, read and (under `write_deletion_vectors`) written. Encrypted lakes (Parquet
  modular encryption with a key per write in `encryption_key`, as DuckDB's `ENCRYPTED`). DuckDB's `ducklake` extension is the cross-engine oracle in both
  directions on both databases, with a BDD specification and model-based property tests; DuckLake spec sections are
  pinned and traced like Iceberg's ([docs/ducklake.md](docs/ducklake.md)).

### Fixed
- `ParquetFiles`: a top-level decimal column given `System.Decimal` values wrote them as nulls; they are written as
  nested decimal fields already were (at the column's scale), and anything else is refused.

## [0.0.1-alpha.1] - 2026-10-25

The first published build: an early alpha of `Iceberg.Net.Core`, `Avro`, `Puffin`, `IO`, `Catalog`, `Data` and `Linq`,
ahead of the first preview, `0.1.0-preview.1`. It exercises the release path and nuget.org trusted publishing from
`rtkelly-labs`; it is not a supported API release. `EntityFrameworkCore`, `Spark`, `Athena` and `Arrow` follow in a later
preview. The REST catalog server is not published until its storage-location checks land.

### Added
- Apache Iceberg table spec v1–v3 for .NET: types (including v3 `timestamp_ns`, `variant`, `geometry`, `unknown`,
  defaults), schemas, transforms, partitioning, snapshots, refs and row lineage, metadata JSON.
- Manifests and manifest lists (in-house Avro codec), Parquet data files (ParquetSharp), Avro data files, Puffin
  deletion vectors and theta sketches.
- Catalogs: in-memory, AWS Glue, Iceberg REST (OAuth2, SigV4, vended credentials, remote signing, idempotent retries,
  views, multi-table transactions) and JDBC/SQL.
- Writes: append, overwrite, insert, upsert by key, group replacement, delete by key, predicate or file, UPDATE and
  MERGE, each one atomic snapshot re-planned on conflict; copy-on-write, position deletes, deletion vectors and
  equality deletes.
- Reads: pruning by partition and column metrics, projection by field ID, streaming, time travel, metadata tables,
  change feed.
- Maintenance: snapshot expiry, orphan-file cleanup, compaction, manifest rewrite.
- `Iceberg.Net.Linq`: typed rows and a LINQ provider that pushes filters and projections into scans.
- Targets .NET 10; `Core` and `Avro` also target `netstandard2.0`.

### Security
- Expiry, purge and orphan cleanup delete only under the table's own directories, honour `gc.enabled=false`, and
  orphan cleanup needs files at least 24 hours old.
- Decoders bound every size and count a file declares (Avro blocks and records, deflate and zstd output, `fixed`,
  deletion-vector cardinality, Roaring containers).
- The REST client sends credentials only over https or to loopback, and does not send its own AWS signatures to a
  storage endpoint the server chose.
- Dependencies clear GHSA-2m69-gcr7-jv3q (SQLite) and GHSA-9cvc-h2w8-phrp (AWS SDK).
