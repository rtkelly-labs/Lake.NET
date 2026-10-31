# Apache ORC

**Status:** reference, checked 2026-10-31, spec pinned at `apache/orc` `main` `3204660` (`site/specification/ORCv1.md`)
and Iceberg `apache-iceberg-1.12.0` (`format/spec.md` appendix A "ORC"). Lake.NET code cited from `main` at `d949a44`;
decision D8 from `origin/rtkelly13/refactor-rename-library:docs/lake/decisions.md`.

ORC (Optimized Row Columnar) is Hive's columnar file format, the alternative to Parquet. Iceberg allows ORC data
files (`write.format.default=orc`) because it grew up beside Hive and Iceberg Java writes ORC through Apache ORC Java.
DuckLake and Delta are Parquet-only. **Lake.NET refuses ORC today.**

## Current status

| Path | Behaviour | Code | Test |
|---|---|---|---|
| Write, `write.format.default=orc` | `NotSupportedException("write.format.default=orc is not supported (parquet, avro)")` before any file is written | `src/Iceberg.Net.Data/TableWriter.cs` (format switch) | `AvroDataFileTests.Unsupported_formats_are_refused` |
| Read, manifest entry with `file_format = ORC` | `NotSupportedException("ORC data files are not supported (Parquet and Avro are)")` | `DataFiles.ReadBatchesAsync` in `src/Iceberg.Net.Data/AvroDataFiles.cs` | none found (unverified that a scan over an ORC entry is tested) |
| Validator | checks only `PARQUET`/`AVRO` data files and `PUFFIN` DVs | `TableValidator.cs` | — |

A table that mixes ORC files in (Iceberg Java can) therefore fails on scan rather than returning partial rows.

## Decision D8

**Decided 2026-10-28: option (d).** Port Apache ORC Java to C# as `Lake.Orc` in this repository, credited in
`NOTICE` (Apache-2.0, decision D9), and plug Iceberg ORC data files into `Lake.Data` as a second file format. Reads
first, verified against Spark-written ORC fixtures carrying `iceberg.id` attributes; writes after. **ORC stays refused
until the port passes those fixtures.** Rejected: `EngineeredWood.Orc` 0.3.0 (three releases old, "preliminary"),
`ApacheOrcDotNet` 1.0.0 (unmaintained since 2020); Arrow C# and ParquetSharp have no ORC. D8 notes that whether
either .NET library exposes ORC type attributes was never checked.

## The format in one screen

```
"ORC"                          3-byte file header
stripe*                        independent, typically ~200 MB
  index streams                ROW_INDEX (min/max etc. per 10,000-row group), BLOOM_FILTER_UTF8
  data streams                 per column: PRESENT, DATA, LENGTH, DICTIONARY_DATA, SECONDARY …
  StripeFooter (protobuf)      stream directory (kind, column, length), ColumnEncoding per column, writerTimezone
file tail
  [encrypted stripe stats]
  Metadata (protobuf)          stripe statistics
  Footer (protobuf)            stripes, types (flattened pre-order tree), row count, column statistics, rowIndexStride
  PostScript (protobuf)        footerLength, compression, compressionBlockSize, version [0,12], metadataLength, magic "ORC"
  psLen                        1 byte (< 256)
```

| Area | What a port must implement |
|---|---|
| Compression | `NONE`, `ZLIB`, `SNAPPY`, `LZO`, `LZ4`, `ZSTD`; everything but the PostScript is chunked: 3-byte LE header `compressedLength * 2 + isOriginal`, chunks ≤ `compressionBlockSize` (default 256 KiB) |
| Integer RLE v1 | runs and literals (Hive < 0.12 files) |
| Integer RLE v2 | four sub-encodings chosen per run: **short repeat**, **direct** (fixed bit width), **patched base** (base + patches for outliers), **delta**; zigzag for signed |
| Byte RLE, boolean RLE | `PRESENT` streams and booleans |
| Column encodings | `DIRECT`, `DICTIONARY` (RLE v1), `DIRECT_V2`, `DICTIONARY_V2` (RLE v2) |
| Strings | direct (DATA + LENGTH) or dictionary (DATA indexes + DICTIONARY_DATA + LENGTH) |
| Decimals | DATA = unbounded zigzag base-128 varints, SECONDARY = scale (signed RLE) |
| Timestamps | DATA = seconds since **2015-01-01** (signed RLE), SECONDARY = nanoseconds with trailing decimal zeros removed and their count in the low 3 bits; `timestamp` vs `timestamp_instant` |
| Dates | days since epoch, signed RLE |
| Nested | struct (PRESENT only), list/map (LENGTH + children), union (tag stream) |
| Statistics, indexes | column statistics in footer/stripe/row-group index; bloom filters (`BLOOM_FILTER_UTF8`) |
| Column encryption | ORCv1 defines encrypted variants and masks; out of scope for a first port |

## Iceberg's mapping and field IDs

| Iceberg | ORC type | Type attributes |
|---|---|---|
| `boolean`, `int`, `long`, `float`, `double` | same (`tinyint`/`smallint` read as `int`) | — |
| `decimal(P,S)`, `date`, `string`, `binary` | `decimal`, `date`, `string` (`varchar`/`char` read as `string`), `binary` | — |
| `time` | `long` | `iceberg.long-type=TIME` (µs) |
| `timestamp` / `timestamptz` | `timestamp` / `timestamp_instant` | `iceberg.timestamp-unit=MICROS` (assumed if absent); writers **must** truncate to µs |
| `timestamp_ns` / `timestamptz_ns` | `timestamp` / `timestamp_instant` | `iceberg.timestamp-unit=NANOS` |
| `uuid` | `binary` | `iceberg.binary-type=UUID` |
| `fixed(L)` | `binary` | `iceberg.binary-type=FIXED`, `iceberg.length=L` (adapter checks the length) |
| `struct`, `list`, `map` | `struct`, `array`, `map` | — |
| `variant` | `struct` of `metadata`, `value` (no IDs) | `iceberg.struct-type=VARIANT`; no shredding |
| `geometry`, `geography` | `binary` (WKB) | `iceberg.binary-type=GEOMETRY` / `GEOGRAPHY` |
| every column | — | **`iceberg.id`** = field ID; `iceberg.required=true` when required |

ORC resolves columns by name; Iceberg resolves by ID, so a reader must build the file's ID → column map from the
`iceberg.id` attributes, then project, rename and default exactly as for Parquet. Iceberg also notes ORC-546: a
`TimestampColumnVector` reports milliseconds twice (in `time` and `nanos`), so an adapter must take them from one.

**Discrepancy:** Iceberg depends on ORC *type attributes*, but `ORCv1.md` at `3204660` documents a `Type` message
with no `attributes` field (and no mention of attributes anywhere). The field exists in ORC's protobuf definition
(`repeated StringPair attributes`, unverified here: `orc_proto.proto` was not fetched), so a port must work from
the `.proto`, not from the spec page.

## Verification plan for the port

| Step | Oracle |
|---|---|
| Decoder primitives (RLE v1/v2, byte/boolean RLE, chunked compression, varints) | Apache ORC Java's own unit vectors, ported with the code |
| File reads | ORC files written by Spark 4 + Iceberg 1.12 (every type above, nested, nulls, `iceberg.id` attributes), committed as fixtures; rows must equal Spark's |
| Cross-check | PyArrow's ORC reader (Apache ORC's C++ library, independent of ORC Java) on the same files |
| Scan integration | an ORC entry in a manifest reads through `DataFiles.ReadBatchesAsync` with projection, renames and defaults by `iceberg.id` |
| Hostile input | the `Corruption` contract: a fuzz target and truncation/bit-flip suite like Parquet's (gap review PQ1), with allocation caps on lengths from PostScript, Footer and stream directories |
| Writes (later) | Spark and PyIceberg read our files; Iceberg metrics from values as for Parquet |

Until then `AvroDataFileTests.Unsupported_formats_are_refused` pins the write refusal; a read-refusal test is missing.
