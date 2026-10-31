# Changelog

All notable changes to **Iceberg.NET** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). `CHANGELOG.md` is the release authority:
`.github/workflows/release.yml` publishes the version of the first cut section below `[Unreleased]`.

---

## [Unreleased]

### Security
- REST catalog server: namespace levels, table and view names (on create and rename) and every location it accepts
  refuse `*`, `?` and `$`, and the S3 vendor refuses to build a session policy containing them, so no name or location
  can widen vended STS credentials to other tables. Commits, staged creates and registers refuse manifest lists,
  statistics and partition statistics outside the table's own directories, which server-side scan planning would
  otherwise read with the server's access. Server-side scan planning reads only files inside the planned table's
  directories, so a manifest list inside a table cannot name another table's manifests.
- `ResolvingFileIO` and `LocalFileIO` refuse storage schemes they have no FileIO for (`gs`, `abfss`, `hdfs`, …)
  instead of reading and writing them as local paths; a relative path starting with `s3` stays local.

### Fixed
- `S3FileIO.ReadRangeAsync` throws `EndOfStreamException` when S3 returns fewer bytes than asked (a range past the end)
  instead of returning the short buffer as if it were the range.
- `LocalFileIO` reports a file whose directory is missing as `FileNotFoundException` (it threw
  `DirectoryNotFoundException`, which callers that handle missing files don't catch), and deleting one is a no-op.
- Gzipped metadata is recognised by its content, so Java's `NNNNN-uuid.gz.metadata.json` is read (by catalogs and the
  REST server) and the next version number continues from it.

### Added
- Cloudflare: `S3FileIOOptions.ForR2` (R2's S3 endpoint, region `auto`, jurisdictions, temporary credentials) and
  `RestCatalogOptions.ForBasinCatalog` (Basin Catalog, formerly R2 Data Catalog, with vended credentials); a live
  `cloudflare` workflow running the FileIO contract on R2 and Iceberg tables through Basin Catalog.
- `S3FileIOOptions.ChecksumsOnlyWhenRequired`: off for AWS, on by default for other endpoints, whose S3 implementations
  don't all accept the AWS SDK's default checksum trailers.
- `Iceberg.Net.DuckDbNet.Tests`: DuckDB's Parquet reader, in-process through DuckDB.NET, checks the values, field IDs
  and statistics of every file type we write, in the unit suite.
- Golden tables written by this library (`tests/Iceberg.Net.Spec.Tests/fixtures/lake-net/`), read, validated and
  written on top by every later version (`OwnGoldenFixtureTests`); a release requires its own set.
- Code coverage for every test project (coverlet); CI merges the unit suite's reports and fails when a package's line
  coverage drops more than 0.5 points below `docs/lake/verification/coverage-baseline.json` (`tools/coverage_ratchet.py`).
- `DeltaDeletionVectorBytesTests`: a Spark/Delta-written deletion vector (delta-rs test data) is byte for byte an Iceberg
  v3 deletion-vector blob, so one Roaring64 codec serves both formats.
- `interop/engines.json`: every reference engine and emulator version in one manifest, enforced by
  `EngineVersionManifestTests` against the compose file, scripts, Python and C# pins.
- Package validation: every pack compares each published package's API with `0.0.1-alpha.1`.
- `FileIOConformanceTests`: one contract for every FileIO (local and S3), covering concurrent create-if-absent,
  overwrite, missing objects, exact ranged reads, empty objects, idempotent deletes, listings that page, stay inside
  their prefix and spell keys exactly as written, and cancellation.
- `DependencyDirectionTests`: every `src` project has a layer and a role, and references may only point down, stay in
  the shared layers, and never cross formats.

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
