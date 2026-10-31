# Table specifications

**Status:** reference, checked 2026-10-31. One page per table format, each with the same structure: what the spec
defines, its versions, its structures, the invariants a writer must keep, interop pitfalls, where Lake.NET implements
it, and how it is verified. File formats (Parquet, Avro, Puffin and deletion vectors, ORC) are in
[../formats/](../formats/README.md).

| Format | Page | Spec pinned at | Lake.NET today |
|---|---|---|---|
| Apache Iceberg (v1–v3; v4 watched), views, REST catalog | [iceberg.md](iceberg.md) | `apache-iceberg-1.12.0` | Read, write and maintain on `main` |
| DuckLake (1.0, 1.1) | [ducklake.md](ducklake.md) | DuckLake `docs/stable` (1.1) | Read, write and maintain on the DuckLake branch (PRs #49–#56) |
| Delta Lake | [delta.md](delta.md) | to pin: a tagged `PROTOCOL.md` | Not started; [`docs/spec/delta-support.json`](../../../spec/delta-support.json) declares the table features |

## The three formats side by side

The formats agree on the data plane (Parquet files, column statistics, deletion bitmaps) and disagree on where
metadata lives and how a commit becomes atomic. That split is the layering in VISION.md: shared `Lake.Core` /
`Lake.Data` below, one package per format above.

| Concern | Iceberg | DuckLake | Delta |
|---|---|---|---|
| Where metadata lives | Immutable files in storage (metadata JSON, Avro manifest lists and manifests), plus a catalog pointer | Rows in a SQL database (`ducklake_*` tables) | An append-only log in storage (`_delta_log/NNNNNNNNNNNNNNNNNNNN.json`) plus Parquet checkpoints |
| What makes a commit atomic | Compare-and-swap of the catalog's metadata pointer | One SQL transaction | Put-if-absent of the next log file |
| Catalog needed | Yes (any Iceberg catalog: REST, Glue, JDBC, …) | Yes: the database *is* the catalog | No; a catalog is optional |
| Snapshot identity | 64-bit snapshot ID, plus a sequence number | Snapshot ID (row in `ducklake_snapshot`) | Log version number |
| Conflict detection | Writer re-validates against the new base (requirements + re-plan) | Snapshot changes compared by rule; retry | Writer checks the actions committed since its read version, by isolation level |
| Column identity | Field ID on every column, nested included | Column ID = Parquet `field_id` | Logical name; physical name or field ID under column mapping |
| Schema evolution | Add, drop, rename, reorder, widen; by field ID | Add, drop, rename, widen; by column ID | Add; rename and drop need column mapping; widening needs `typeWidening` |
| Partitioning | Hidden partitioning with transforms; spec evolution | Transforms, recorded per file | Identity partition columns, values as strings; liquid clustering as a hint |
| File statistics | Manifest metrics (binary single-value bounds, counts) | `ducklake_file_column_stats` (text) | `add.stats` JSON (`minValues`, `maxValues`, `nullCount`, `numRecords`, `tightBounds`) |
| Row-level deletes | Position deletes, equality deletes, deletion vectors (v3) | Position delete files, deletion vectors (1.1), inlined deletes | Deletion vectors (inline Z85, relative or absolute file) |
| Row lineage | v3 `_row_id`, `_last_updated_sequence_number` | Row IDs kept through UPDATE | Row tracking (`baseRowId`, `defaultRowCommitVersion`) |
| Small writes | Each one is a snapshot with files | Inlined into the catalog database, flushed later | Each one is a log entry with files |
| Time travel | By snapshot ID, timestamp, branch or tag | By snapshot ID or timestamp | By version or timestamp (in-commit timestamps when enabled) |
| Multi-table transactions | REST and JDBC catalogs | Natural: one database transaction | Not in the protocol |
| Maintenance | Expire snapshots, orphan cleanup, compaction, rewrite manifests | Expire snapshots, cleanup old files, merge files, rewrite deletes, flush inlined data | VACUUM, log cleanup, checkpointing, OPTIMIZE |
| Encryption | Spec'd in v3 (`encryption-keys`, snapshot `key-id`); not implemented | Encrypted lakes: Parquet modular encryption, keys in the catalog | Not in the protocol (storage-level) |
| Reference engines (our oracles) | Spark, PyIceberg, Trino, DuckDB | DuckDB (1.5 writes 1.0, 2.0 writes 1.1) | delta-rs, Spark, DuckDB `delta` (planned) |

## How these pages are kept true

- Every page states the spec version it was checked against and the date.
- Claims about a spec link to the pinned section. Claims about Lake.NET name a file or type that exists and, where
  possible, the test that proves it. Anything not yet checked says so.
- When a spec is re-pinned (the drift check), the page is re-read in the same pull request.
