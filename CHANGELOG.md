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
- REST catalog server, authentication: principals are keyed by issuer and subject, so an external IdP user whose `sub`
  is `root` is `external/root`, never the server's own client `root` (**breaking:** grants for external users must name
  `external/{sub}`); the IdP's key cannot sign a token in the server's name nor the server's key one in the IdP's; a
  principal claim inside a token is ignored; with `Auth:Audience` set the server's own tokens are accepted again (they
  carry no audience); static tokens are compared in constant time. Expired, unsigned (`alg=none`), wrong-issuer,
  wrong-audience and `exp`-less JWTs are tested to get 401.
- REST catalog server: a scan plan ID belongs to the principal that planned it; another principal's fetch or cancel is
  a 404. An authorization matrix test covers every mapped route and fails for a new route until it is listed.
- REST catalog server: the 16 MiB body limit applies before the idempotency middleware reads and hashes the body (a
  17 MiB create with an `Idempotency-Key` used to succeed, and without one was a 500); it is now a 413 either way. JSON
  depth is capped at 64 explicitly. `POST /v1/oauth/tokens` is rate limited per client address
  (`Auth:TokenRequestsPerMinute`, default 60; 429 with `Retry-After`).
- REST catalog server: a 400 for an overlapping location no longer names the other table, view or namespace or its
  location (they may be outside the caller's grants); the details go to the server log.
- REST catalog server: the remote signer signs an allow-list of headers (no `x-amz-tagging`, no client-chosen KMS key
  or encryption context, nothing S3 adds later) and only `GET`, `HEAD`, `PUT`, `POST` and `DELETE`.
- REST catalog server: credentials for a table whose stored location contains `*`, `?` or `$` are refused with an
  explicit 422 before any vendor is asked (it was a misleading 400 from the STS vendor, and other vendors vended).
- `RestCatalog`: a server-pushed `s3.endpoint` is trusted only if it is an S3 service endpoint over https with no path
  (`RestCatalog.IsS3ServiceEndpoint`), not any `*.amazonaws.com` host (API Gateway, ELB, bucket and access-point hosts);
  redirects are no longer followed (a 307 from the token endpoint re-sent the client secret to its target), also by
  `SigV4Handler`'s default handler; a server `prefix` that would change the request path (`..`, encoded or not, `?`,
  `#`, `\`) is refused.
- `SECURITY.md` has a threat model: the server is open by default, trust boundaries, the scope of vended credentials
  (Azure container SAS, Cloudflare Basin Catalog's bucket-wide credentials), DuckLake catalog writers fully trusted,
  and keys in the catalog.

### Fixed
- The REST server no longer reads every table's metadata on every create: table directories are cached by (immutable)
  metadata file, so a create's overlap check reads only tables committed since the last one.
- Orphan cleanup on Windows took every live file for an orphan: metadata builds local paths with `/` while a directory
  listing returns `\`, and `TableLocations.Normalise` compared them as written. Local paths now normalise to `/` and an
  upper-case drive letter on Windows (found by the new Windows CI job before any release ran it).
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
- `RequestBudgetTests`: exact storage-request budgets per operation (append, scans, upserts under each delete
  mechanism, expiry) in `docs/lake/verification/request-budgets.json`, measured with `MeteredFileIO`.
- `benchmarks/Lake.Benchmarks.Micro` (BenchmarkDotNet: deletion vectors, metadata JSON, Parquet write and scan) and a
  weekly `benchmarks` workflow.
- Package validation: every pack compares each published package's API with `0.0.1-alpha.1`.
- `FileIOConformanceTests`: one contract for every FileIO (local and S3), covering concurrent create-if-absent,
  overwrite, missing objects, exact ranged reads, empty objects, idempotent deletes, listings that page, stay inside
  their prefix and spell keys exactly as written, and cancellation.
- `tests/Lake.TestSupport` (`IFormatDriver` and the common scenarios every format must pass) and
  `tests/Lake.Common.Tests`, running them for Iceberg v2 and v3 under copy-on-write, position deletes, equality deletes
  and deletion vectors.
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
