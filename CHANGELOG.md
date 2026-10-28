# Changelog

All notable changes to **Iceberg.NET** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). `CHANGELOG.md` is the release authority:
`.github/workflows/release.yml` publishes the version of the first cut section below `[Unreleased]`.

---

## [Unreleased]

### Added
- Full-stack development packages: Arrow, EntityFrameworkCore, Spark and Athena join the seven existing libraries;
  the REST server has a publishable executable archive with package-consumer and restart checks.
- Public API snapshots/checker, resource diagnostic probe, server operating/recovery contract and explicit
  capability/platform/experimental-cloud boundaries.
- Required integration-dependency checks, retained test/skip results, Windows/macOS qualification jobs, the Mumbling
  fuzz target and Dependabot version-update configuration.
- Explicit REST client storage-credential refresh and fresh table loading after replayed create/register responses.
- Per-principal idempotency claim budgets shared across server instances; at capacity new keys receive 429 while
  existing keys remain replayable. Local/default development packs use `0.0.1-dev.1`; no release is cut.

### Fixed
- Typed writes apply write defaults only to absent members, including nested structs; explicit nulls remain null
  and unmapped values of existing rows survive typed MERGE/EF updates.
- The test runner starts dependencies before dependent suites and rejects invalid/misconfigured modes before tests.

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
