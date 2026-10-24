# Changelog

All notable changes to **Iceberg.NET** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). `CHANGELOG.md` is the release authority:
`.github/workflows/release.yml` publishes the version of the first cut section below `[Unreleased]`.

---

## [Unreleased]

The first preview, `0.1.0-preview.1`, will publish `Iceberg.Net.Core`, `Avro`, `Puffin`, `IO`, `Catalog`, `Data` and
`Linq`. `EntityFrameworkCore`, `Spark`, `Athena` and `Arrow` follow in a later preview. The REST catalog server is not
published until its storage-location checks land.

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
