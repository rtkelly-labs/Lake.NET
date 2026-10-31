# delta-rs test tables

Copied unchanged from [delta-io/delta-rs](https://github.com/delta-io/delta-rs) `crates/test/tests/data/` at commit
`b7c38792431a645c26d69d3d741f9cf1e7d8d6c9` (Apache License 2.0). Written by Spark/Delta, not by this library: refresh
them by copying again, never by editing.

| Table | Used by | What it holds |
|---|---|---|
| `table-with-dv-small` | `DeltaDeletionVectorBytesTests` | 10 rows (`value` 0–9); `DELETE … WHERE value IN (0, 9)` stored as a deletion vector in `deletion_vector_61d16c75-….bin` (`storageType` `u`, `offset` 1, `sizeInBytes` 36) |
