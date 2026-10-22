# Puffin

Puffin (`format/puffin-spec.md`) is Iceberg's container for blobs that don't fit in manifests. Two blob types matter:

| Blob | Used for | Spec version |
|---|---|---|
| `deletion-vector-v1` | v3 row-level deletes: one bitmap of deleted positions per data file | v3 |
| `apache-datasketches-theta-v1` | per-column NDV statistics for cost-based optimisers (Spark, Trino) | v2+ |

`Iceberg.Net.Puffin` implements the container and both blobs from scratch (no native dependencies), and
`Iceberg.Net.Data` uses them for merge-on-read deletes and table statistics.

## What it does

| Capability | API | Notes |
|---|---|---|
| Read/write Puffin files | `PuffinFile.Write` / `PuffinFile.Read` | blobs uncompressed or `zstd`; LZ4-compressed **footers** are rejected on read (none of the engines we read write them by default) |
| Deletion vectors | `DeletionVector.Encode/Decode`, `RoaringPositionBitmap` | Roaring64 portable format; array + bitmap containers written, run containers also read; CRC-32 verified |
| Theta sketches | `ThetaSketch` | DataSketches compact/ordered serial v3, seed 9001, MurmurHash3_x64_128 over Iceberg single-value bytes |
| Row deletes (v2 → position delete files, v3 → DVs) | `SnapshotProducer.DeleteRows` | at most one DV per data file: an existing DV (or applicable v2 position deletes) is folded in and the old DV removed |
| **Converter: v2 position deletes → v3 DVs** | `SnapshotProducer.ConvertPositionDeletesToDeletionVectors` | after `upgrade-format-version 3`; folds every position delete file into one DV per data file, drops dangling deletes, operation `replace` |
| NDV statistics | `PuffinStatistics.ComputeNdvAsync` / `ReadNdvAsync` | one theta blob per column, `ndv` property, registered via `set-statistics` |
| Scan applies deletes | `TableScan.ReadAllAsync`, `DeleteIndex` | sequence-number applicability; DV supersedes position deletes for its file; equality deletes refused |

## Wire formats (what the tests pin)

```
Puffin      = "PFA1" Blob* "PFA1" FooterJSON FooterSize(int32 LE) Flags(4 bytes, bit0 = LZ4 footer) "PFA1"
DV blob     = Length(int32 BE, = 4 + |vector|) D1 D3 39 64 Vector CRC32(BE, over magic+vector)
Vector      = Count(int64 LE) { Key(uint32 LE, ≤ int.MaxValue) RoaringBitmap32(portable) }*
Theta       = preLongs(1|2|3) serVer=3 family=3 0 0 flags(0x1A, 0x1E empty) seedHash(LE, 0x93CC for 9001)
              [count int32, 4 zero bytes]          if preLongs ≥ 2
              [theta int64]                       if preLongs = 3 (estimation mode)
              hash int64 × count (ascending; hash = MurmurHash3_x64_128(bytes, 9001).h1 >>> 1)
```

## Verification

| Check | Oracle | Test |
|---|---|---|
| Exact-mode sketches byte-identical; hash of `"iceberg"` = `0x032E42766A92E119` | Apache DataSketches (C++ via Python) | `PuffinSpecTests` (committed fixtures) |
| Estimation-mode sketch from DataSketches deserialises with the same theta/retained/estimate | DataSketches | `PuffinSpecTests` |
| DVs written by pyroaring (incl. run containers) decode to the same positions | pyroaring (CRoaring) + zlib | `PuffinSpecTests` |
| PyIceberg applies our v2 position deletes and v3 DVs, and decodes our DVs (CRC verified with zlib) | PyIceberg 0.12 scan + `deletion_vectors_from_puffin_file` | `PuffinCrossEngineTests` |
| Converted (v2 → v3) table reads identically in PyIceberg | PyIceberg | `PuffinCrossEngineTests` |
| DataSketches reads our NDV blobs with identical estimates | DataSketches | `PuffinCrossEngineTests` |
| We read Athena's position deletes, add our own on top, Athena still reads | Athena | `AthenaTests` (AWS only) |

Regenerate the L0 fixtures with `interop/pyiceberg_oracle.py puffin-fixtures tests/Iceberg.Net.Spec.Tests/fixtures/puffin`.

## Known differences and limits

- **Theta update algorithm**: ours is K-minimum-values (keeps exactly k=4096 hashes); DataSketches' QuickSelect
  keeps between k and ~2k. Serialized form and estimator are standard, so sketches interoperate and merge, but in
  estimation mode the bytes (and the estimate, within the ±1.6% RSE) differ from Java's for the same input.
- **Athena cannot read v3**, so DVs are for tables read by Spark/Trino/PyIceberg/S3 Tables; on Glue tables queried
  by Athena use v2 (position delete files).
- Row-delete validation re-checks that every target data file is still live on each retry; deletes that a concurrent
  commit made redundant are merged, not rejected (serializable conflict detection for overlapping *data* changes is M2).
- DV Puffin files and position delete files live under `write.data.path` (default `{location}/data`), like Java.
