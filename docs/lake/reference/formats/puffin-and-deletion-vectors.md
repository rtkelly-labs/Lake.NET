# Puffin and deletion vectors

**Status:** reference, checked 2026-10-31, spec pinned at `apache-iceberg-1.12.0` (`c234952`: `format/puffin-spec.md`,
`format/spec.md` "Deletion Vectors", `format/mumbling-spec.md` v1), `delta-io/delta` `master` `7197687`
(`PROTOCOL.md` "Deletion Vectors", "Deletion Vector Format") and DuckLake `duckdb/ducklake-web` `9d09594`
(`ducklake_delete_file`). Lake.NET code cited from `main` at `d949a44` and the DuckLake branch
`origin/ccr-87fadcf9-bxi9wr` at `91f4d31` (marked **DL**).

Deletion vectors (DVs) are the row-level delete mechanism all three formats converge on: a bitmap of deleted row
positions per data file, serialized as a 64-bit Roaring bitmap. Iceberg and DuckLake 1.1 put it in a Puffin blob;
Delta uses its own file (or inlines it in the log). The bitmap bytes are the same; the framing around them differs.
Iceberg v4 adds a second, unrelated bitmap (Mumbling) for manifest deletion vectors.

## 1. Puffin container

```
"PFA1"                                    magic 50 46 41 31 at offset 0
Blob₁ … Blobₙ                             raw bytes, located only by the footer
Footer:
  "PFA1"
  FooterPayload                           UTF-8 JSON FileMetadata, optionally one LZ4 frame (content size present)
  FooterPayloadSize   int32 LE            after compression
  Flags               4 bytes             byte 0 bit 0 = payload compressed; all other bits reserved, 0 on write
  "PFA1"
```

| `FileMetadata` / `BlobMetadata` field | Required | Lake.NET (`PuffinFile`) |
|---|---|---|
| `blobs[]` | yes | read and written |
| `properties` (e.g. `created-by`) | no | read; written when given |
| `type` | yes | opaque string; rules per type are supplied by the caller (`PuffinBlobType`) |
| `fields` (int list) | yes | DVs: `[2147483545]` (`_pos`, as Java writes) |
| `snapshot-id`, `sequence-number` | yes | DVs write `-1` (inherited from the manifest entry, as the spec requires) |
| `offset`, `length` | yes | written; used to slice blobs on read |
| `compression-codec` | no ⇒ uncompressed | `zstd` read/write; `lz4` **refused** (`NotSupportedException`) |
| `properties` | no | read and written |

| Codec | Spec | Read | Write |
|---|---|---|---|
| none | default | ✅ | ✅ |
| `zstd` (single frame, content size present) | allowed | ✅ ZstdSharp, output capped at 256 MiB (`MaxDecompressedBlobBytes`) | ✅ |
| `lz4` (single frame, content size present) | allowed | refused | no |
| LZ4 footer payload (flag bit 0) | allowed | refused: `NotSupportedException` | never written |

`lz4` blobs and LZ4 footers are valid Puffin that this library refuses (`docs/libraries.md`: "LZ4 not needed yet");
no engine we test against writes them by default (unverified for Trino's statistics writer).

## 2. Blob types

| Type | Payload | Compression | Required properties | Lake.NET |
|---|---|---|---|---|
| `apache-datasketches-theta-v1` | DataSketches compact Theta sketch (Alpha family, default seed 9001) of values in Iceberg single-value serialization | allowed | `ndv` (optional, decimal digits) | `ThetaSketch`: serial v3, `preLongs` 1/2/3, flags `0x1A` (`0x1E` empty), seed hash `0x93CC`; hash = MurmurHash3_x64_128(bytes, 9001).h1 >>> 1. Exact mode byte-identical to DataSketches; estimation mode keeps exactly k = 4096 (KMV) where QuickSelect keeps k…~2k, so bytes differ, estimates agree within RSE |
| `deletion-vector-v1` | §3 | **must be omitted** | `referenced-data-file` (= the data file's `location`), `cardinality` | `DeletionVector`, `RoaringPositionBitmap`; `IcebergPuffinFile.Write` applies `Compressible: false` |

## 3. `deletion-vector-v1` and Roaring64 "portable"

```
length    int32 BE    = 4 + |vector|           (magic + vector)
magic     D1 D3 39 64                          (= 1681511377 as int32 LE)
vector    Roaring64 portable:
            count   uint64 LE                  number of 32-bit bitmaps
            repeat, keys ascending (unsigned):
              key     uint32 LE                high 32 bits of the position; MSB must be 0
              bitmap  RoaringFormatSpec 32-bit bitmap (below)
crc       uint32 BE   CRC-32 (IEEE, reflected 0xEDB88320, = java.util.zip.CRC32) over magic + vector
```

32-bit Roaring bitmap (RoaringFormatSpec "portable"), all little-endian:

| Part | No run containers | With run containers |
|---|---|---|
| Cookie | `uint32` 12346 | `uint16` 12347, then `uint16` (containers − 1) |
| Container count | `uint32` | (in the cookie) |
| Run flags | — | ⌈n/8⌉ bytes, bit i = container i is a run container |
| Descriptive header | per container: `uint16` key (high 16 bits), `uint16` cardinality − 1 | same |
| Offsets | `uint32` per container (from the start of this bitmap) | only when n ≥ 4 |
| Containers | array (≤ 4096 values): `uint16` × card; bitmap: 8,192 bytes (65,536 bits) | run: `uint16` run count, then (start, length − 1) pairs |

| Lake.NET choice | Detail |
|---|---|
| Writes | array and bitmap containers only (switch at 4,096 values), cookie 12346 with offsets; keys ascending; empty 32-bit bitmaps dropped |
| Reads | all three container kinds; run containers must be sorted, disjoint, inside 0…65535 and sum to the declared cardinality; bitmap containers must hold exactly their declared cardinality |
| Keys | ≤ `int.MaxValue` on write and read ("Java-compatible range"; Delta requires MSB 0, Iceberg requires positive positions) and strictly ascending on read |
| Cardinality | `Decode(blob, maxCardinality: record_count)` checks declared sizes before building containers, then requires `Cardinality == record_count` (`DeleteFiles.ReadDeletionVectorAsync`) |
| Locating | reads the whole Puffin file (cached per path), finds the footer blob whose `offset`/`length` equal the manifest's `content_offset`/`content_size_in_bytes`, refuses a compressed DV |
| `referenced-data-file` | written; compared with the manifest's `referenced_data_file` by `TableValidator`, not on the read path |
| Trailing bytes after the last bitmap | not rejected (they are inside the CRC) |

Iceberg rules around the blob (`spec.md` "Deletion Vectors"): at most one DV per data file per snapshot; a DV
replaces all earlier position delete files for that file; several DVs may share one Puffin file. Lake.NET:
`SnapshotProducer.DeleteRows` folds an existing DV or applicable v2 position deletes in and removes the old DV;
`ConvertPositionDeletesToDeletionVectors` converts a v2 table after `upgrade-format-version 3`; `DeleteIndex` throws on
two DVs for one file.

## 4. Delta deletion vectors

Delta (`deletionVectors` reader and writer feature, reader 3 / writer 7) attaches a `DeletionVectorDescriptor` to
`add`/`remove`:

| Field | Meaning |
|---|---|
| `storageType` | `u`: relative file, `pathOrInlineDv` = optional random prefix + 20-char Z85 UUID → `<table>/<prefix>/deletion_vector_<uuid>.bin`; `i`: inline, `pathOrInlineDv` = Z85 of the bitmap bytes; `p`: absolute path |
| `offset` | start of this DV in the file; absent for `i`, read as 0 if absent otherwise |
| `sizeInBytes` | size of the serialized bitmap **before** Z85 (`bitmapData` only, not length or checksum) |
| `cardinality` | rows removed; `numRecords` in `stats` becomes mandatory |
| Z85 | ZeroMQ RFC 32 base85: 4 bytes ↔ 5 chars, big-endian groups |

Bitmap ("RoaringBitmapArray", all little-endian): `magicNumber` int32 = **1681511377**, then the Roaring64 portable
bitmap (8-byte bucket count, `uint32` key + 32-bit Roaring bitmap per bucket, keys ascending, key MSB 0).
On-disk file (all big-endian): `version` byte = 1, then per DV: `dataSize` int32, `bitmapData` (magic + bitmap),
`checksum` CRC-32 of `bitmapData`.

## 5. DuckLake 1.1 deletion vectors

`ducklake_delete_file.format = 'puffin'` ("Iceberg V3 deletion vector, experimental"). Two shapes:

| Shape | Contents | Snapshot of the vector | Catalog fields |
|---|---|---|---|
| Bare vector | the `deletion-vector-v1` blob bytes alone (length + magic + vector + CRC), no `PFA1` | the row's `begin_snapshot` | `footer_size = 0`, `row_group_count` NULL |
| Puffin container | several cumulative vectors, each with blob property `ducklake-snapshot-id` | readers take the greatest `ducklake-snapshot-id` ≤ the requested snapshot | `delete_count` = the latest vector's count; `footer_size` = JSON payload + 16-byte footer structure |

Puffin delete files are never encrypted, even in an encrypted lake. Lake.NET (**DL**): reads both shapes, telling
them apart by the first 4 bytes (`PAR1`/`PARE` Parquet, `PFA1` container, anything else a bare vector:
`DuckLakeDeleteFiles.VectorsAsync`); writes bare vectors only, as `ducklake-<uuidv7>-delete.puffin`, when the lake's
`write_deletion_vectors` or `DuckLakeOptions.WriteDeletionVectors` is set, never in a 1.0 lake. **DL**
`docs/ducklake.md` still says DVs are "not read" in two places (lines 23 and 155) while line 237 says they are; the
code reads them.

## 6. Byte-layout comparison

| Field | Iceberg DV blob | Delta file record | Delta inline (`i`) | DuckLake bare file |
|---|---|---|---|---|
| Container | Puffin, at `content_offset` | file starting with `01` | Z85 string in the log | whole file |
| Length prefix | int32 BE = magic + vector | `dataSize` int32 BE = magic + bitmap | none (`sizeInBytes` in JSON) | as Iceberg |
| Magic | `D1 D3 39 64` | 1681511377 LE = `D1 D3 39 64` | same, per the format section | as Iceberg |
| Bucket count | uint64 LE | 8 bytes LE | same | as Iceberg |
| Key / bucket | uint32 LE + 32-bit portable Roaring | same | same | as Iceberg |
| Checksum | CRC-32 BE over magic + vector | CRC-32 BE of `bitmapData` | none | as Iceberg |
| Size recorded in metadata | `content_size_in_bytes` = whole blob (incl. length and CRC) | `sizeInBytes` = `bitmapData` only | `sizeInBytes` = decoded bytes | `file_size_bytes` = whole file |
| Cardinality recorded | blob `cardinality`, manifest `record_count` | `cardinality` | `cardinality` | `delete_count` |

So an Iceberg DV blob is byte-for-byte one Delta DV record (`dataSize ‖ bitmapData ‖ checksum`), which is what the
Puffin spec means by "big endian values were chosen for compatibility with existing deletion vectors in Delta
tables". Converting between them needs only metadata arithmetic: Iceberg `content_offset` = Delta record start,
`content_size_in_bytes` = `sizeInBytes` + 8.

| Claim | Status | How |
|---|---|---|
| Iceberg magic bytes = Delta's magic number little-endian | ✅ verified | 1681511377 = `0x6439D3D1` → LE `D1 D3 39 64` |
| Iceberg blob = Delta record layout | ✅ verified | `DeltaDeletionVectorBytesTests`: the Spark/Delta-written DV in delta-rs's `table-with-dv-small` decodes with `DeletionVector.Decode` to {0, 9}, and `DeletionVector.Encode` of {0, 9} reproduces Delta's record byte for byte, CRC included |
| Delta `offset` points at `dataSize`, not at the magic | ✅ verified | in the same fixture `offset` is 1 (after the version byte `01`) and the 4 bytes there are `00 00 00 24` = `sizeInBytes` 36, big-endian |
| Delta `u` example decodes | ✅ verified | `ab^-aqEH.-t@S}K{vb[*k^` → prefix `ab` + UUID `d2c639aa-8816-431a-aaf6-d3fe2512ff61`, as the protocol states |
| Delta inline example is the portable format | **contradicted** | Example 3 (`wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L`, 40 bytes) decodes to `64 39 D3 D0 00000001 0000001C` + a 28-byte 32-bit Roaring bitmap {3, 4, 7, 11, 18, 29}: magic **1681511376 big-endian**, a big-endian int32 bitmap count and a big-endian per-bitmap length, i.e. a different ("native") RoaringBitmapArray layout, not the little-endian portable layout with magic 1681511377 that the format section specifies. A Delta reader must therefore expect both magics (unverified which one delta-spark writes today) |
| Inline bytes not a multiple of 4 | ⏳ unknown | Z85 needs 4-byte groups; padding rule not in the protocol |
| pyroaring/CRoaring DVs (with run containers) decode | ✅ | `PuffinSpecTests.Reads_deletion_vectors_written_by_pyroaring_including_run_containers` (committed fixture) |
| PyIceberg applies and decodes our DVs | ✅ | `PuffinCrossEngineTests.Pyiceberg_applies_our_v3_deletion_vectors_and_decodes_them` |
| Spark reads our DVs; Trino 478 refuses (`Unexpected PUFFIN file format`) | ✅ | `docs/emulation.md`; characterization test |
| DuckDB 2.0 reads our bare DuckLake DVs and we read its vectors | ✅ (**DL**) | `CrossEngineTests.Each_engine_reads_the_deletion_vectors_the_other_writes` |
| Delta DVs | ⏳ not implemented | no Delta code; Z85 and the `01`-prefixed file are unwritten |

## 7. Mumbling bitmap (Iceberg v4 manifest DVs)

`format/mumbling-spec.md` v1: positions 0 … 2,097,151 in up to 8,192 containers of 256 bits; all integers unsigned
little-endian.

```
header       01 | cardinality (3 bytes LE) | container count (2 bytes LE, ≤ 8192)
descriptors  one byte per container, PFOR-encoded (appendix A):
               000nnnnn sparse, n = 0…31 positions (= container length)
               001xxxxx dense, 32 bytes (low bits ignored)
containers   sparse: n ascending bytes (offset in container)
             dense: 32 bytes, bit 0 = MSB of byte 0
PFOR chunk   (b1 | b2 << 4) e m | primary b1 bits × n (MSB first) | exception offsets (byte × e) | high b2 bits × e
```

Containers with < 32 bits set must be sparse, ≥ 32 dense. Lake.NET `MumblingBitmap`/`MumblingPfor` (`Blobs/`) is the
codec only; nothing reads v4 manifests yet. `Deserialize` rejects a descriptor type other than sparse/dense, a
non-ascending sparse container, a dense container with < 32 bits, a cardinality that disagrees with the containers,
and trailing bytes. Verified only against the spec's own examples (no second implementation exists):
`MumblingBitmapTests`.

## 8. Hardening

| Input | Check | Where |
|---|---|---|
| Puffin magic, footer size, footer magic | both magics, `payloadSize ≥ 0`, footer magic at the computed offset | `PuffinFile.ReadUnguarded` |
| Blob slice | `Payload(blob)` inside `Corruption.Guard` | `PuffinFile.Contents.Payload` |
| zstd blob bomb | ≤ 256 MiB | `PuffinFile` decompress |
| DV length, magic, CRC | length ≥ 4 and inside the blob; magic; CRC-32 | `DeletionVector.DecodeUnguarded` |
| Roaring64 count | ≤ (bytes − 8) / 4 | `RoaringPositionBitmap.DeserializeUnguarded` |
| Container sizes | cumulative declared cardinality ≤ `maxCardinality` before allocation | `Bitmap32.Deserialize(ref budget)` |
| Unknown cookie | `FormatException` | `Bitmap32.Deserialize` |

`Iceberg.Net.Puffin` has its own copy of the `Corruption` contract (`Format/Corruption.cs`) because it deliberately
does not depend on Core.

## 9. Implementation map and tests

| Path | Role |
|---|---|
| `src/Iceberg.Net.Puffin/Format/PuffinFile.cs` | Generic container: `PuffinFile.Read`/`Write`, `PuffinBlob`, `PuffinBlobMetadata`, `PuffinBlobType` |
| `src/Iceberg.Net.Puffin/Blobs/DeletionVector.cs` | `deletion-vector-v1` framing, `Crc32` |
| `src/Iceberg.Net.Puffin/Blobs/RoaringPositionBitmap.cs` | Roaring64 portable codec (array, bitmap, run containers) |
| `src/Iceberg.Net.Puffin/Blobs/MumblingBitmap.cs` | Mumbling bitmap + PFOR |
| `src/Iceberg.Net.Puffin/Blobs/ThetaSketch.cs`, `PuffinBlobTypes.cs`, `IcebergPuffinFile.cs` | Theta sketches; Iceberg's blob-type rules |
| `src/Iceberg.Net.Data/DeleteFiles.cs`, `SnapshotProducer.cs`, `PuffinStatistics.cs`, `TableValidator.cs` | DV read/write, converter, NDV statistics, validation |
| **DL** `src/Iceberg.Net.DuckLake/DuckLakeDeleteFiles.cs`, `DuckLakeTransaction.cs` | DuckLake bare vectors and containers |
| `docs/puffin.md`, `docs/libraries.md` | Feature overview, wire formats, why built in-house |

The Roaring code lives in `Iceberg.Net.Puffin` (`Blobs/`), not in Core.

| Test | Covers |
|---|---|
| `PuffinSpecTests` | Theta byte-identity with DataSketches, Murmur3 and seed hash, pyroaring DVs with run containers, DV round trip and CRC detection, container switch at 4,096, Puffin round trip with a zstd blob |
| `PuffinEdgeTests` | malformed sketches and Puffin files fail with specific errors, properties round-trip, generic container writes unknown blob types, blob rules come from the defining format, `Deletion_vectors_must_not_be_compressed` |
| `MumblingBitmapTests` | spec PFOR and container examples, round trips, malformed bitmaps, explicit-check corruption |
| `CorruptionTests` | `Puffin_file`, `Deletion_vector`, `Roaring_bitmap`, `Mumbling_bitmap`, `Theta_sketch`, `Every_single_bit_flip_in_a_deletion_vector_is_detected` |
| `DecoderLimitTests` | run and bitmap containers hold exactly their declared values, overlapping runs refused, a DV cannot exceed or differ from its manifest's `record_count`, zstd blob limit |
| `RowDeleteTests`, `TableValidatorTests.Detects_corrupt_deletion_vector_bytes` | one DV per data file, merge, applicability by sequence number, converter |
| `PuffinCrossEngineTests` (Regression) | PyIceberg and DataSketches read ours |
| **DL** `DeletionVectorTests`, `CrossEngineTests` (DuckLake) | bare vectors at every snapshot, cumulative containers, 1.0 lakes never write vectors, DuckDB 2.0 both ways |
| Fuzz targets | `puffin`, `deletion-vector`, `roaring`, `mumbling`, `theta` (`fuzz.yml`, weekly) |
