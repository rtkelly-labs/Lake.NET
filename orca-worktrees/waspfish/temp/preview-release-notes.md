The first preview of `Iceberg.Net.Core`, `Avro`, `Puffin`, `IO`, `Catalog`, `Data` and `Linq`, following the
`0.0.1-alpha.1` release that exercised NuGet trusted publishing. The API remains in preview.
`EntityFrameworkCore`, `Spark`, `Athena` and `Arrow` follow in a later preview. The REST catalog server remains
unpublished by choice; its storage-location checks are implemented.

S3, Glue and the Athena runner have been tested against emulators only; a real AWS run remains outstanding.
Athena SQL has unit and Trino checks, not verification against real Athena.

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
