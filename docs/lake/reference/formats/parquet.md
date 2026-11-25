# Apache Parquet

**Status:** reference, checked 2026-10-31, spec pinned at `apache-parquet-format-2.14.0` (tag object `804b577`,
commit `04d56f2`). Lake.NET code cited from `main` as checked out at `d949a44` and from the DuckLake branch
`origin/ccr-87fadcf9-bxi9wr` at `91f4d31` (marked **DL**); the Parquet.Net fork from `rtkelly-labs/parquet-dotnet`
branch `ccr-a26a1234-riyup4` at `edcd51d0`. Iceberg appendix A from `apache-iceberg-1.12.0`; Delta from
`delta-io/delta` `master` at `7197687`; DuckLake from `duckdb/ducklake-web` at `9d09594`.

Parquet is the data file format of all three table formats, the delete-file format of DuckLake and Iceberg v2, and
Delta's checkpoint format. Everything the data plane promises about values, pruning and hostile input rests on this
page. Spec files read: `README.md`, `LogicalTypes.md`, `Encodings.md`, `Compression.md`, `Encryption.md`,
`VariantEncoding.md`, `VariantShredding.md`, `BloomFilter.md`, `PageIndex.md`, `src/main/thrift/parquet.thrift`.

## 1. File layout

```
offset 0  "PAR1"                                  ("PARE" when the footer is encrypted)
          ┌ row group 0 ──────────────────────────────────────────────────────────────┐
          │ column chunk, leaf 0                                                       │
          │   [dictionary page] PageHeader(thrift compact) + PLAIN values   (optional, first)
          │   data page v1      PageHeader + compressed{ rep levels, def levels, values }
          │   data page v2      PageHeader + rep levels + def levels (uncompressed) + compressed values
          │   …                                                                        │
          │ column chunk, leaf 1 …   (one chunk per leaf, in schema depth-first order)  │
          └────────────────────────────────────────────────────────────────────────────┘
          row group 1 …
          [page index: ColumnIndex, OffsetIndex]   located by ColumnChunk.*_index_offset/length
          [bloom filters]                          located by ColumnMetaData.bloom_filter_offset/length
          FileMetaData (thrift compact)            PARE: FileCryptoMetaData + encrypted FileMetaData
                                                   plaintext-footer encryption: FileMetaData + nonce(12) + tag(16)
          footer length (int32 LE)
          "PAR1" / "PARE"
```

| Item | Rule | Lake.NET |
|---|---|---|
| Footer length | 4-byte LE before the trailing magic; covers `FileMetaData` (or crypto metadata + encrypted footer) | `ParquetWriteResult.FooterSize` reads it back (`ParquetFiles.WriteFileAsync`); DuckLake stores it in `footer_size` |
| Leading magic | Readers need not read it | Parquet.Net backend sets `ValidateHeaderMagic = false` for ranged reads (fork `lake(0023)`); parquet-cpp skips it too |
| `split_offsets` | Iceberg: row-group start offsets | Lake.NET always writes `[4]` (start of row group 0), even for multi-row-group files (**DL** `ParquetFiles.cs`) — under-reports splits, harmless for correctness |
| `column_sizes` | Iceberg metric | Summed `total_compressed_size` of each leaf's chunks, read back from the written bytes |

## 2. Schema

The schema is a depth-first list of `SchemaElement`s: groups (`num_children`) and primitives (`type`), each with a
repetition (`REQUIRED`, `OPTIONAL`, `REPEATED`), an optional `field_id`, a `LogicalType` and the legacy
`ConvertedType`.

| Concept | Rule |
|---|---|
| Max definition level of a leaf | Number of `OPTIONAL` + `REPEATED` ancestors (itself included) |
| Max repetition level | Number of `REPEATED` ancestors (itself included) |
| Null at depth d | Definition level < max: the level says which ancestor is null; values are stored for defined leaves only |
| Empty list vs null list | `optional group l (LIST) { repeated group list { optional int32 element } }`: def 0 = `l` null, 1 = empty list, 2 = null element, 3 = value |
| Records | Rep level 0 starts a record; > 0 continues the list at that depth |

### LIST and MAP (3-level)

```
<rep> group <name> (LIST) {           <rep> group <name> (MAP) {
  repeated group list {                 repeated group key_value {
    <rep> <type> element;                 required <type> key;
  }                                       <rep> <type> value;
}                                       }
                                      }
```

Iceberg requires the 3-level form for lists (appendix A, "Parquet") and 3-level maps. Readers must accept the
legacy shapes (`LogicalTypes.md`, backward-compatibility rules):

| Rule (2.14 numbering) | Shape | Element |
|---|---|---|
| 1 | `repeated` primitive directly under `LIST` | the primitive, required |
| 2 | `repeated group` with several fields | the group (struct), required |
| 3 | `repeated group` whose one field is itself `repeated` | the group, required |
| 4 | `repeated group` named `array` or `<list>_tuple` | the group, required |
| 5 | otherwise | the repeated group's one field, with its repetition |
| maps | `MAP_KEY_VALUE` in place of `MAP`; `key`/`value` identified by position | |

Lake.NET reads rules 1, 2, 4 and 5 and both map shapes in `NestedColumns.FromFile` (**DL** `NestedColumns.cs`,
`repeated.Name is not "array" && !…EndsWith("_tuple")`). Rule 3 (one `repeated` child) is not special-cased there
(unverified whether it reads). The fork's change `lake(0018)` calls its fix "rules 2 and 3"; in the 2.14 numbering
those are rules 2 and 4.

### Field IDs

`SchemaElement.field_id` (thrift field 9) is how Iceberg, DuckLake (`column_id`) and Delta column mapping `id` mode
resolve columns. **Groups need IDs too**: an Iceberg struct, list or map is itself a field with an ID, and projection,
rename and nested `element-id`/`key-id`/`value-id` resolution all go through the group. Exceptions: the synthetic
`repeated` groups (`list`, `key_value`) carry no ID, and Iceberg forbids IDs on a variant's `metadata`/`value`.

| Writer | Where the ID goes |
|---|---|
| Top-level primitive | `field_id = NestedField.Id` (**DL** `ParquetFiles.ToPq`) |
| Struct / list / map group | the field's ID (`NestedColumns.ToPq`: `FieldId = id` on the `LIST`/`MAP` group) |
| `element`, `key`, `value` | `ListType.ElementId`, `MapType.KeyId`, `MapType.ValueId` |
| `list` / `key_value` repeated groups | none (−1) |

Reading: if any leaf has an ID, columns are matched by ID; otherwise by name through the table's name mapping
(`ParquetFiles` layout, `byFieldId`/`byName`, `matchByName`). Upstream Parquet.Net could not read IDs at all, nor
write them on groups (gaps 1 and 2, fixed by fork `lake(0014)`).

### Logical vs converted types

`LogicalType` (thrift union, 2.x) replaces `ConvertedType`. Writers **should** also write the matching converted type;
2.14 says writers **must** annotate local (not UTC-adjusted) `TIME`/`TIMESTAMP` `MILLIS`/`MICROS` with
`TIME_*`/`TIMESTAMP_*` as well. The Parquet.Net backend writes a converted type for those only when
`isAdjustedToUTC = true` (**DL** `ParquetNetBackend.ToThrift`); parquet-cpp does the same by default (unverified).
On read, a converted type alone means UTC-adjusted (`TIMESTAMP_MICROS` → `TIMESTAMP(true, MICROS)`), which both
backends follow (`ParquetNetBackend.ToPq(SchemaElement)`).

## 3. Type mapping

Lake.NET's value model is **physical**: a row is `object?[]` holding the CLR value below, never `DateTime` or
`decimal` (`ParquetFiles` summary; `IcebergDecimal` and `IcebergTime` in `src/Iceberg.Net.Core/Types/Values.cs`).
DuckLake types map onto Iceberg types plus a `ParquetAnnotation` (**DL** `DuckLakeTypes.ToIceberg`, `.Annotation`).

| Parquet physical + logical | Iceberg | DuckLake | Delta | Arrow (`ArrowInterop`) | CLR physical value |
|---|---|---|---|---|---|
| `BOOLEAN` | `boolean` | `boolean` | `boolean` | `bool` | `bool` |
| `INT32` | `int` | `int32` | `integer` | `int32` | `int` |
| `INT32` `INT(8,true)` / `INT(16,true)` | `int` + `Int8`/`Int16` | `int8` / `int16` | `byte` / `short` | `int32` | `int` |
| `INT32` `INT(8,false)` / `INT(16,false)` | `int` + `UInt8`/`UInt16` | `uint8` / `uint16` | — | `int32` | `int` |
| `INT32` `INT(32,false)` | `long` + `UInt32` | `uint32` | — | `int64` | `long` |
| `INT64` | `long` | `int64` | `long` | `int64` | `long` |
| `INT64` `INT(64,false)` | `decimal(20,0)` + `UInt64` | `uint64` | — | `decimal128(20,0)` | `IcebergDecimal` (scale 0) |
| `FLOAT` / `DOUBLE` | `float` / `double` | `float32` / `float64` | `float` / `double` | `float` / `double` | `float` / `double` |
| `INT32` `DATE` | `date` | `date` | `date` | `date32` | `int` days since 1970-01-01 |
| `INT64` `TIME(false, MICROS)` | `time` | `time` | — | `time64[us]` | `long` µs since midnight |
| `INT64` `TIME(true, MICROS)` | `time` + `TimeUtc` | `timetz` | — | `time64[us]` | `long` µs (UTC) |
| `INT64` `TIMESTAMP(false, MICROS)` | `timestamp` | `timestamp`, `timestamp_s` (truncated) | `timestamp_ntz` | `timestamp[us]` | `long` µs |
| `INT64` `TIMESTAMP(false, MILLIS)` | `timestamp` + `TimestampMillis` | `timestamp_ms` | — | `timestamp[us]` | `long` µs (×1000 on read, floor ÷1000 on write) |
| `INT64` `TIMESTAMP(true, MICROS)` | `timestamptz` | `timestamptz` | `timestamp` | `timestamp[us, UTC]` | `long` µs |
| `INT64` `TIMESTAMP(false, NANOS)` | `timestamp_ns` | `timestamp_ns` | — | `timestamp[ns]` | `long` ns |
| `INT64` `TIMESTAMP(true, NANOS)` | `timestamptz_ns` | `timestamptz_ns` (refused by Lake.NET DuckLake) | — | `timestamp[ns, UTC]` | `long` ns |
| `INT32`/`INT64`/`FLBA(n)` `DECIMAL(P,S)` | `decimal(P,S)` | `decimal(P,S)` | `decimal(P,S)` | `decimal128(P,S)` | `IcebergDecimal(BigInteger unscaled, scale)` |
| `BYTE_ARRAY` `STRING` | `string` | `varchar`, `json` | `string` | `utf8` | `string` |
| `BYTE_ARRAY` | `binary` | `blob` | `binary` | `binary` | `byte[]` |
| `FLBA(16)` `UUID` | `uuid` | `uuid` | — | `fixed_size_binary(16)` + `arrow.uuid` | `Guid` (bytes big-endian, `ToByteArray(bigEndian: true)`) |
| `FLBA(L)` | `fixed(L)` | — | — | `fixed_size_binary(L)` | `byte[]` |
| `FLBA(12)` `INTERVAL` (converted only) | `fixed(12)` + `Interval` | `interval` | — | — | `byte[]` (LE months, days, millis) |
| group `VARIANT` | `variant` | `variant` | `variant` (`variantType`) | — | writer refuses |
| `BYTE_ARRAY` `GEOMETRY`/`GEOGRAPHY` | `geometry`/`geography` | `geometry` | — | — | writer refuses |
| `UNKNOWN` / omitted | `unknown` (omitted from files) | `null` | `void` | — | `null` |
| `INT96` | — (Iceberg writers must not) | — | legacy Spark timestamps | — | refused on import; no stats |

Notes. Lake.NET's DuckLake also refuses `int128`, `uint128`, `time_ns`, `variant` and `geometry`
(`DuckLakeTypes.ToIceberg` summary). `json` is written as `STRING` with no `JSON` annotation. The writer rejects any
type without a mapping (`NotSupportedException` "is not supported by the Parquet writer"; gap review: refuse before
any file is written). Delta is not implemented; its column is the protocol's type list (`PROTOCOL.md`,
"Primitive Types": `timestamp` must be `isAdjustedToUTC=true`, `timestamp_ntz` `false`).

## 4. Encodings and compression

| Encoding (id) | Spec status | ParquetSharp reads | Lake.Parquet reads | What Lake.NET writes |
|---|---|---|---|---|
| `PLAIN` (0) | all types | ✅ | ✅ | both backends, non-dictionary pages |
| `PLAIN_DICTIONARY` (2) | deprecated | ✅ | ✅ | — |
| `RLE` (3) | levels, booleans | ✅ | ✅ | levels (both) |
| `BIT_PACKED` (4) | deprecated | ✅ | ✅ | — |
| `DELTA_BINARY_PACKED` (5) | INT32/INT64 | ✅ | ✅ | — |
| `DELTA_LENGTH_BYTE_ARRAY` (6) | BYTE_ARRAY | ✅ | ✅ | — |
| `DELTA_BYTE_ARRAY` (7) | BYTE_ARRAY, FLBA | ✅ | ✅ | — |
| `RLE_DICTIONARY` (8) | indexes | ✅ | ✅ | ParquetSharp: every column (parquet-cpp default) unless `Dictionary = false`; Parquet.Net: string columns only (`EncodingHint.Dictionary`) |
| `BYTE_STREAM_SPLIT` (9) | fixed-width | ✅ | ✅ | — |

ParquetSharp's read column is parquet-cpp's (unverified per encoding); Lake.Parquet's is
`File/DataColumnReader.cs` (`case Encoding.*`).

| Codec | Spec | ParquetSharp | Lake.Parquet | Lake.NET writes |
|---|---|---|---|---|
| `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD`, `BROTLI` | current | ✅ (unverified for `BROTLI`) | ✅ (ZstdSharp, Snappier, BCL) | **`ZSTD`** only: parquet-cpp default level; Parquet.Net `CompressionLevel.Fastest` (≈ parquet-cpp level 1) |
| `LZ4` (5) | deprecated (Hadoop framing ambiguity) | unverified | decoded as raw LZ4 block, same as `LZ4_RAW` (unverified against Hadoop-framed files) | — |
| `LZ4_RAW` (7) | current | ✅ | ✅ (K4os) | — |
| `LZO` (3) | current | unverified | refused: `NotImplementedException` (outside the F1–F12 exception family) | — |

| Page | Spec | Writes | Reads |
|---|---|---|---|
| `DATA_PAGE` (v1) | levels and values compressed together | both backends (Parquet.Net only writes v1: `ThriftFooter.CreateDataPage`; parquet-cpp default v1, unverified) | both |
| `DATA_PAGE_V2` | levels uncompressed, `is_compressed`, null/row counts | — | both (fork `lake(0005)` fixed repeated columns) |
| `DICTIONARY_PAGE` | one per chunk, first | both | both; `dictionary_page_offset` written since fork `lake(0006)` |
| `INDEX_PAGE` | never used | — | Lake.Parquet: `NotSupportedException` (F10) |

## 5. Statistics

`Statistics` (thrift): `null_count`, `distinct_count`, `min_value`/`max_value` (5/6), `is_min/max_value_exact`
(7/8), `nan_count` (9), and deprecated `min`/`max` (1/2, signed comparison only).
`FileMetaData.column_orders` says how `min_value`/`max_value` are ordered.

| `ColumnOrder` | Meaning | Rules |
|---|---|---|
| `TYPE_ORDER` | logical type's order, else physical | `UTF8`/`JSON`/`BSON`/`ENUM`/`BYTE_ARRAY`/`FLBA`: unsigned bytes; `INT(…,false)`: unsigned; `INT*`, `DATE`, `TIME`, `TIMESTAMP`, `DECIMAL`: signed; `INTERVAL`, `LIST`, `MAP`, `VARIANT`, geo, `FILE`: undefined |
| `TYPE_ORDER` on floats | ambiguous | Writers: `nan_count` **must** be set (even 0); min/max from non-NaN only; none if all NaN; min zero written `-0.0`, max zero `+0.0`. Readers: ignore NaN bounds; min `+0` may hide `-0`, max `-0` may hide `+0` |
| `IEEE_754_TOTAL_ORDER` (2.14) | floats, total order | `nan_count` mandatory; if all values NaN, min/max **are** NaNs |
| `INT96_TIMESTAMP_ORDER` (2.14) | INT96 chronological | with `TYPE_ORDER`, readers ignore INT96 stats |
| absent | legacy file | only deprecated signed `min`/`max`; parquet-cpp and parquet-mr trust unsigned-order bounds only from known writers |
| unknown union member | | ignore min/max |

`null_count` absent ≠ 0 (readers **must not** assume 0). Bounds may be loose (truncated) unless marked exact; a
truncated `max` must still be ≥ every value (writers round up).

### How Lake.NET uses them

| Use | Source | Rule | Code |
|---|---|---|---|
| Row-group pruning on read | footer stats via `IPqReader.Chunk(rg, leaf).Statistics` | `RowGroupStatistics.For`: bounds only when the leaf's `PqNode.SortOrder` is the one Iceberg compares in (`Comparable`: signed numerics, unsigned `BYTE_ARRAY`/`FLBA` for `string`/`binary`/`fixed`); none for `uuid`, `FLBA`/binary decimals, `INT96`; null count trusted only alongside min/max; a string bound that is not valid UTF-8 is dropped; NaN bounds ignored by the evaluator | **DL** `ParquetFiles.cs` `RowGroupStatistics`; main `ParquetFiles.cs` |
| Iceberg manifest metrics on write | **the values written**, not Parquet stats | `Metrics.Compute`: value/null/NaN counts; bounds skip NaN, omitted for `binary`/`uuid`/`fixed` and for strings over 16 code points (no truncation); strings compared by code point | `ParquetFiles.cs` `Metrics` |
| Iceberg metrics for imported files | footer stats | `ParquetImport` (`NullCount`: unknown ⇒ assume nulls); **DL** `ParquetInspection.Statistics`: a group without bounds (unless all null) drops the file's bounds | `ParquetImport.cs`, **DL** `ParquetInspection.cs` |
| DuckLake `ducklake_file_column_stats` | values written (`DuckLakeColumnStats`, text) | min/max in `ValueComparer` order, NaN → `contains_nan`, `value_count` = non-null (as DuckDB) ; no string truncation | **DL** `DuckLakeValues.cs` |
| DuckLake `AddDataFilesAsync` | footer stats through `ParquetInspection` | as import | **DL** `DuckLakeTransaction.AddFiles.cs` |
| Delta `add.stats` | — | not implemented ⏳ | — |

`PqNode.SortOrder` is derived from the type only; whether min/max exist at all (`HasMinMax`, honouring
`column_orders` and the deprecated fields) is the library's decision: ParquetSharp's `Statistics.HasMinMax`, the
fork's `DataColumnStatistics.HasMinMax`/`PhysicalMin` (`lake(0019)`, `lake(0021)`). Neither backend's handling of
`IEEE_754_TOTAL_ORDER` is tested (unverified); an all-NaN chunk under that order would surface NaN bounds, which the
evaluator ignores.

### UTF-16 vs code-point order (bug history)

.NET `string.CompareOrdinal` orders by UTF-16 code unit, which puts U+E000–U+FFFF **after** supplementary characters
(surrogates U+D800–U+DFFF); UTF-8 byte order (Parquet `TYPE_ORDER`) and Iceberg's order are by code point.

| Where | Bug | Fix | Test |
|---|---|---|---|
| Lake.NET manifest metrics | string bounds chosen by UTF-16 order (found by the nested-field filter tests, `docs/roadmap.md`) | `ValueComparer.CompareCodePoints` (`Expressions/Evaluators.cs`) used by `Metrics.Compute` and `ValueComparer.Compare` | `ExpressionTests.String_order_is_code_point_order`, `EvaluatorEdgeTests.Uuids_compare_as_unsigned_big_endian_and_strings_by_code_point` |
| Lake.NET truncate transform | length counted in UTF-16 units | rune enumeration | `TransformSpecTests.Truncate_counts_code_points_not_utf16_units` |
| Parquet.Net footer stats | strings compared by UTF-16 code unit; also unsigned compared signed, UUIDs by `Guid.CompareTo`, NaN bounds, no `column_orders` | fork `lake(0008)` | fork `StatisticsSortOrderTest` |
| Oracle | | DuckDB reads each row group and checks it lies within its bounds | `DuckDbParquetReaderTests.Every_row_group_statistic_bounds_the_values_DuckDB_reads_from_that_group("s")` (main) |

Other upstream statistics bugs fixed in the fork: timestamp min/max always in millis (`lake(0002)`, DuckDB returned
no rows), UUID stats little-endian (`lake(0007)`), decimal/unsigned decoding (`lake(0010)`), no stats for
`BYTE_STREAM_SPLIT` (`lake(0009)`). Open: PQ4 (no property-test oracle for -0.0/+0.0, unsigned, FLBA, decimal,
INT96, truncated binary).

## 6. Modular encryption

| Item | Spec (`Encryption.md`) |
|---|---|
| Algorithms | `AES_GCM_V1`: every module GCM. `AES_GCM_CTR_V1`: pages CTR (unauthenticated), all other modules GCM |
| Module framing | `length(4 LE) ‖ nonce(12) ‖ ciphertext ‖ tag(16)` (GCM); `length ‖ nonce(12) ‖ ciphertext` (CTR, IV = nonce ‖ 0x00000001) |
| Nonce | 12 random bytes per module (NIST SP 800-38D RBG); ≤ 2³² GCM invocations per key |
| Encrypted footer | magic `PARE`; `FileCryptoMetaData{encryption_algorithm, key_metadata}` + encrypted `FileMetaData`; hides schema, stats, KV metadata |
| Plaintext footer | magic `PAR1`; `FileMetaData.encryption_algorithm` set; footer signed: nonce(12) + tag(16) appended; stats of encrypted columns stripped from plaintext `meta_data` |
| Keys | footer key; per-column keys with `encrypted_column_metadata`; `key_metadata` (opaque bytes for a KMS) |
| AAD | `aad_prefix` (optional, stored or supplied) ‖ suffix = `aad_file_unique` ‖ module type (1 byte) ‖ row group (int16 LE) ‖ column (int16 LE) ‖ page (int16 LE, data pages and headers only). Module types: footer 0, column metadata 1, data page 2, dictionary page 3, data page header 4, dictionary page header 5, ColumnIndex 6, OffsetIndex 7, bloom header 8, bloom bitset 9 |

| Capability | ParquetSharp (parquet-cpp) | Lake.Parquet (`lake(0024)`) | What Lake.NET uses |
|---|---|---|---|
| `AES_GCM_V1` / `AES_GCM_CTR_V1` | ✅ / ✅ | ✅ / ✅ (CTR over AES-ECB, `AesModules.Ctr`) | GCM, both libraries' default |
| Encrypted / plaintext footer | ✅ / ✅ | ✅ / ✅ (`FixedTimeEquals` on the signature) | encrypted footer (`PARE`) |
| Footer key / column keys | ✅ / ✅ | ✅ / ✅ | footer key only (`ParquetWriteOptions.EncryptionKey`, 16/24/32 bytes) |
| `key_metadata`, key retriever, AAD prefix | ✅ | ✅ (stored or supplied) | none (no prefix, no key metadata) |
| Page index / bloom modules | ✅ | no (neither is written) | n/a |
| Append to encrypted file, key rotation | n/a | refused / no | n/a |
| Interop | reference | both ways with parquet-cpp, all five modes; DuckDB 1.5.5 full scans of GCM files | `ParquetBackendTests.Annotated_and_encrypted_files_cross_backends` (**DL**) |

**DuckLake encrypted lakes** (**DL** `DuckLakeOptions.Encrypted`, `DuckLakeCatalog.cs`): every data and Parquet
delete file is written AES-GCM with an encrypted footer under a fresh 256-bit key per write
(`RandomNumberGenerator.GetBytes(32)`), stored Base64 in `ducklake_data_file.encryption_key` /
`ducklake_delete_file.encryption_key`, as DuckDB does. Puffin delete files are written unencrypted even when the column
is set (DuckLake spec). Known: DuckDB 1.5.5/2.0 fail filtered reads of parquet-cpp-encrypted files ("Computed AES tag
differs from read AES tag"), so DuckDB cannot filter, delete from or update files Lake.NET encrypts
(`DuckDB_reads_encrypted_parquet_written_here_but_not_with_a_filter`, **DL** `docs/ducklake.md`). Iceberg table
encryption (`encryption-keys`, snapshot `key-id`) is not implemented.

Open security test items (gap review **PQ5**, none done): (a) nonce uniqueness over many thousands of modules and no
reuse path; (b) AAD binding: swapping pages, chunks or row groups between two files under one key must fail; (c)
tamper tests over every module type; (d) GCM_CTR: headers still authenticated though pages are not; (e) interop with
parquet-java encryption (Spark, Iceberg Java); (f) keys never in exception text or `ToString()`; plus an expert review,
which `lake(0024)`'s own notes ask for.

## 7. Variant, bloom filters, page index

| Feature | Spec | ParquetSharp backend | Lake.Parquet backend | Status |
|---|---|---|---|---|
| `VARIANT` (`VariantEncoding.md`, `VariantShredding.md`) | group `VARIANT(1)` with `required binary metadata` and `value` (`optional` when shredded) plus `typed_value`; metadata header `version=1`, value header `basic_type` (2 bits) + 6-bit header | no factory in ParquetSharp 25.0.1.1 | thrift types only; values are plain binary columns (upstream branch `feature/variant-encoding` has a decoder) | ⏳ writer refuses `variant` (Iceberg v3, DuckLake, Delta `variantType`); PQ6 |
| `GEOMETRY` / `GEOGRAPHY` + `GeospatialStatistics` | WKB in `BYTE_ARRAY`; bbox stats | no factory | thrift types only | ⏳ refused |
| Bloom filters (`BloomFilter.md`) | split-block (256-bit blocks of 8 × 32-bit words, 8 fixed salts), `XXH64` seed 0, `BloomFilterHeader` at `bloom_filter_offset` | not enabled, not read | not written, not read | ⏳ |
| Page index (`PageIndex.md`) | `ColumnIndex` (per-page min/max, `null_pages`, `boundary_order`, null counts) and `OffsetIndex` (page locations), after the row groups | not enabled, not read | not written, not read | ⏳ (pruning is per row group only) |
| `SizeStatistics` | unencoded byte sizes, level histograms | not used | not used | — |

## 8. The two backends

`IParquetBackend` (**DL** `src/Iceberg.Net.Data/ParquetModel.cs`) is the seam: `OpenAsync(stream, decryptionKey)`
returns an `IPqReader` (schema tree of `PqNode`, row groups, `PqColumnChunk` with `PqStatistics`, and
`ReadLeafAsync` → `PqLeafData(levels, defs, reps, typed values)`); `Create(schema, PqWriteSettings(Statistics,
Dictionary, EncryptionKey))` returns an `IPqWriter`. Everything else (`ParquetFiles`, `NestedColumns`,
`ParquetInspection`, `ParquetImport`) is written once over it. Selection: ParquetSharp unless
`ParquetNet.UseAsDefault()`, `ParquetNet.Scope()` or `ICEBERG_PARQUET_BACKEND=parquet-net` (`ParquetFiles.Backend`).
On main (`d949a44`) `ParquetFiles` is still ParquetSharp-only; the seam is on the DuckLake branch.

| Capability | ParquetSharp 25.0.1.1 (`ParquetSharpBackend`) | Lake.Parquet fork (`Iceberg.Net.Data.ParquetNet`) |
|---|---|---|
| Implementation | Arrow C++ parquet-cpp via P/Invoke | managed C# (Parquet.Net + `lake(0001…0026)`) |
| Native binaries / WASM, mobile | per RID / no | none / possible, untested |
| NativeAOT, trimming | no (native) | not declared: no `IsAotCompatible` in any csproj; no AOT smoke app (goal G12) |
| Packed | ✅ | no: `IsPackable=false` until Lake.Parquet is on NuGet |
| Field IDs on groups, read back | ✅ | ✅ `lake(0014)` |
| Physical values (`long` ns, unscaled decimals) | `LogicalReaderOverride` | `PhysicalClrType` `lake(0017)`, `RawDataField` `lake(0022)` |
| `FLBA(n)` for `fixed[n]` | ✅ | ✅ `lake(0015)` |
| Statistics off | `DisableStatistics()` | `WriteStatistics=false` `lake(0016)` |
| Typed stats + sort order | `Statistics<T>`, `HasMinMax` | `lake(0019)`, `lake(0021)`; INT96/INTERVAL stats no longer decode |
| Per-leaf descriptor, footer length | `ColumnDescriptor` | `ParquetReader.Columns`, `FooterLength` `lake(0020)` |
| Legacy lists | ✅ + `NestedColumns` | `lake(0018)` |
| Encryption | ✅ | ✅ `lake(0024)` |
| Hostile input | native decoder; a crash is a process crash; not fuzzable in-process | 1,909 malformed cases: 1,177 pass, **731 known findings + 1 hang** (`lake(0026)`) |
| Performance (`tools/ParquetBackendBench`, 1M flat rows, Zstd) | write 2,489 ms, read 1,227 ms, 33.6 MB | write 2,300 ms, read 1,347 ms, 29.0 MB |

### Hostile-input findings (fork `lake(0026)`)

Case counts per group as the record gives them (a case can appear under more than one frame). Allowed errors: `ParquetException`, `IOException`/`EndOfStreamException`, `InvalidDataException`; anything else, a
10 s timeout, or allocation over 64 × file size + 64 MB is a finding. Record:
[`docs/changes/0026-malformed-file-tests.md`](https://github.com/rtkelly-labs/parquet-dotnet/blob/ccr-a26a1234-riyup4/docs/changes/0026-malformed-file-tests.md);
list `data/malformed/known.tsv`; test `src/Parquet.Test/MalformedFileTest.cs`.

| # | Finding | Cases |
|---|---|---|
| F1 | **Hang**: truncated Thrift varint never ends (`ReadByte` −1 → `0xFF`) | 1 (skipped) |
| F2 | Footer length not checked against the file (AOORE / `OverflowException`) | 33 |
| F3 | `ThriftProtocolException` is not a `ParquetException` | 59 |
| F4 | Schema not validated (empty, `num_children` overrun, scale > precision, unknown type) | 113 |
| F5 | Column chunk `num_values`, offsets trusted (AOORE, OOM) | 39 |
| F6 | **Page sizes size allocations**: `uncompressed_page_size = 2^28` makes an 800-byte file allocate 268 MB | 124 |
| F7 | **Stale pooled memory decoded** as levels/values (`ArrayPool` not cleared; other files' bytes can surface) | 37 (83 `any`) |
| F8 | Dictionary and data page `num_values` trusted | 57 |
| F9 | RLE/bit-packed runs, dictionary indexes, ULEB128, delta blocks not bounds-checked | 65 |
| F10 | Unknown page type, second dictionary page, missing sub-header (`NotSupported`, `InvalidOperation`, `NullReference`) | 94 |
| F11 | Unknown codec / mismatched encoding → `NotSupportedException` | 46 |
| F12 | `ZstdException` and junk-statistics `ArgumentException` not wrapped | 18 |

Consequence: the Parquet.Net backend must not become a default or packed backend until F1, F7, F2/F4–F6/F8 and F9
are fixed (gap review PQ1); no Lake.NET fuzz target covers Parquet yet (`fuzz.yml` targets: metadata, view-metadata,
manifest-list, manifest, puffin, deletion-vector, roaring, theta, mumbling).

## 9. Writer invariants for Lake files

| Invariant | Why | Enforced in |
|---|---|---|
| `field_id` on every primitive and every struct/list/map group; none on `list`/`key_value` | ID projection (Iceberg, DuckLake, Delta `id` mode) | `ParquetFiles.ToPq`, `NestedColumns.ToPq`; tested `DuckDbParquetReaderTests.Every_column_and_nested_field_carries_its_field_id` |
| 3-level lists (`list`/`element`), maps `key_value`/`key` (required)/`value` | Iceberg appendix A | `NestedColumns.ToPq` |
| No `INT96` | deprecated; Iceberg forbids | no writer mapping; `ParquetImport.CheckType` refuses on import |
| Timestamps `INT64` with `TIMESTAMP(isAdjustedToUTC, MICROS\|NANOS)` matching `timestamp`/`timestamptz`/`_ns`; `MILLIS` only via DuckLake's `TimestampMillis` annotation | unit and zone are part of the type | `ParquetFiles.ToPq`, `Annotated`; import checks unit and UTC flag |
| Decimals by precision: ≤ 9 `INT32`, ≤ 18 `INT64`, else minimal `FLBA` (`DecimalBytes`), big-endian two's complement; value scale must equal column scale | Iceberg appendix A, Parquet `DECIMAL` | `ParquetFiles.ToPq`, `FixedDecimal`, `Unscaled` (throws on scale mismatch) |
| `uuid` = `FLBA(16)` `UUID`, RFC 4122 byte order | `Guid.ToByteArray()` is mixed-endian | `ParquetFiles` value writers; `docs/libraries.md` pitfalls |
| Statistics only in `TYPE_ORDER`, `column_orders` written | readers ignore unsigned bounds otherwise | parquet-cpp; fork `lake(0008)`; `ParquetBackendTests.Statistics_prune_row_groups_alike` |
| Statistics off for files where bounds are useless | delete files | `ParquetWriteOptions.Statistics`, `PqWriteSettings.Statistics` |
| Manifest metrics from values, not footer stats | footer stats are optional and writer-dependent | `Metrics.Compute` |
| Unsupported Iceberg types refused (variant, geometry, geography) | no silent wrong file | `ParquetFiles.ToPq` default arm; gap review asks to refuse before any byte is written |
| Zstd compression | size; both backends agree | `ParquetSharpBackend.Writer`, `ParquetNetBackend.Writer` |
| Write once (`overwrite: false`) | Iceberg file immutability | `io.WriteAsync(location, bytes, overwrite: false)` |

## 10. Implementation map and tests

| Path | Role |
|---|---|
| `src/Iceberg.Net.Data/ParquetFiles.cs` | Write (schema, levels, metrics) and read (ID projection, physical values, row-group pruning, ranged reads) |
| `src/Iceberg.Net.Data/NestedColumns.cs` | Shred/assemble lists, maps, structs; legacy shapes on read |
| `src/Iceberg.Net.Data/ParquetImport.cs` | `add_files` import; `CheckType` per Iceberg type |
| **DL** `src/Iceberg.Net.Data/ParquetModel.cs` | `IParquetBackend`, `PqNode`, `PqStatistics`, `PqSortOrder` |
| **DL** `src/Iceberg.Net.Data/ParquetSharpBackend.cs`, `src/Iceberg.Net.Data.ParquetNet/ParquetNetBackend.cs` | The two backends |
| **DL** `src/Iceberg.Net.Data/ParquetInspection.cs` | Footer-only inspection for DuckLake `AddDataFilesAsync` |
| `src/Iceberg.Net.Core/Types/Values.cs`, `Expressions/Evaluators.cs` | `IcebergDecimal`, `IcebergTime`, `ValueComparer` |
| **DL** `docs/parquet-net-gaps.md` | Gap list, fork status, upstream bugs, benchmarks |

| Test | Proves | Where |
|---|---|---|
| `DuckDbParquetReaderTests` (`Iceberg.Net.DuckDbNet.Tests`) | DuckDB's reader: every type, field IDs on every node, footer stats vs manifest metrics, every row group within its bounds, DuckDB-written files import | main |
| `ParquetBackendTests` (Spec suite) | each backend reads the other's files; same schema and metrics; same pruning; annotated and encrypted files across backends | **DL** |
| `NestedTypesTests`, `WideTypesTests`, `ReadPathTests`, `NameMappingAndImportTests` | nesting, decimals/timestamps, projection, name mapping, import | main |
| `SparkFixtureTests`, `PyIcebergFixtureTests`, `SparkCrossEngineTests` | files from parquet-java and Arrow C++ | main |
| `SourceGeneratorInteropTests.Gap_default_datetime_mapping_is_int96` | Parquet.SourceGenerator's INT96 default is refused | main |
| `parquet-net` CI job | unit + DuckLake suites on the managed backend | **DL** |
| Fork: `FieldIdTest`, `PhysicalValuesTest`, `TypedStatisticsTest`, `StatisticsSortOrderTest`, `LegacyListTest`, `EncryptionTest`, `ParquetCppInteropTest`, `MalformedFileTest` | the `lake(NNNN)` changes | fork |

Gaps (gap review): PQ1 fuzz/hostile input, PQ2 only DuckDB reads Parquet.Net output (Spark, Trino, arrow-rs never
do), PQ3 contract suite on fork promotion, PQ4 statistics oracle, PQ5 encryption, PQ6 variant/geo/bloom/page index
and a `parquet-format` section map, PQ7 side-by-side load, PQ8 performance gate.
