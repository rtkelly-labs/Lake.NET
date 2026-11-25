# Apache Avro

**Status:** reference, checked 2026-10-31, spec pinned at `apache/avro` `main` `3a207fa`
(`doc/content/en/docs/++version++/Specification/_index.md`: object container files, binary encoding, schema
resolution, logical types) and Iceberg `apache-iceberg-1.12.0` (`format/spec.md` appendix A "Avro"). Lake.NET code
cited from `main` as checked out at `d949a44`.

Avro is Iceberg's metadata container: every manifest list and manifest of v1–v3 is an Avro object container file
(OCF). Iceberg also allows Avro data and delete files (`write.format.default=avro`). DuckLake and Delta do not use
Avro. Lake.NET reads and writes all of it with its own codec, `Iceberg.Net.Avro`.

| Use | Read | Write | Code |
|---|---|---|---|
| Manifest lists | v1, v2, v3 | v2, v3 (v1 refused: `NotSupportedException`) | `ManifestCodec.ReadManifestList` / `WriteManifestList` |
| Manifests (data and delete) | v1, v2, v3 | v2, v3 | `ManifestCodec.ReadManifest` / `WriteManifest` |
| Data and delete files | ✅ streamed in 1 MiB ranges when the size is known | ✅ when `write.format.default=avro` | `src/Iceberg.Net.Data/AvroDataFiles.cs` |
| v4 manifests | — | — | Parquet in v4 (`docs/iceberg-v4.md`), not Avro |

## 1. Object container file layout

```
"O" "b" "j" 0x01                                   magic, 4 bytes
meta : map<string, bytes>                          Avro map encoding (blocks of key/value, count 0 ends)
         "avro.schema" → writer schema JSON        required
         "avro.codec"  → "null" | "deflate" | …    absent ⇒ "null"
         Iceberg keys  → schema, schema-id, partition-spec, partition-spec-id, format-version, content,
                         snapshot-id, parent-snapshot-id, sequence-number, first-row-id (v3), iceberg.schema (data files)
sync : 16 random bytes
block*:
  count : long (zigzag varint)                     objects in the block
  size  : long                                     bytes after the codec
  data  : size bytes                               concatenated binary-encoded objects, codec applied
  sync  : the same 16 bytes                        a mismatch is corruption
```

| Rule | Spec | Lake.NET |
|---|---|---|
| Metadata keys starting `avro.` | reserved | written: `avro.schema`, `avro.codec`; read: same |
| Sync marker | random per file, repeated after each block | `Compat.RandomBytes(16)`; every block's marker compared (`FormatException` "Avro sync marker mismatch") |
| Block size | writer's choice | flush at 1,000 records or 16 MiB encoded (`WriterFlushBytes`); one record over 128 MiB refused on write |
| Empty file | header only | written when there are no records; reads as zero records |

## 2. Binary encoding

| Type | Encoding |
|---|---|
| `null` | 0 bytes |
| `boolean` | 1 byte, 0 or 1 |
| `int`, `long` | zigzag (`(n << 1) ^ (n >> 63)`) then LEB128 varint; 0→`00`, −1→`01`, 1→`02`, 64→`80 01` |
| `float`, `double` | IEEE 754 bits, little-endian (`floatToRawIntBits`) |
| `bytes`, `string` | long length + bytes (string = UTF-8) |
| `record` | fields in declaration order, no framing |
| `enum` | int index of the symbol |
| `array`, `map` | blocks: long count + items (map: string key + value); count 0 ends; negative count ⇒ abs(count) followed by a long byte size (allows skipping) |
| `union` | long branch index + value of that branch |
| `fixed` | exactly `size` bytes, no length |

Iceberg narrows this: the only union allowed in data files is `["null", T]` (optional fields, list elements and map
values); optional fields without an Iceberg default must have Avro `"default": null`.

## 3. Logical types

| Avro logical type | Avro spec base | Iceberg use | Lake.NET read → CLR | Lake.NET write |
|---|---|---|---|---|
| `decimal` (precision, scale) | `bytes` or `fixed`, big-endian two's complement | `fixed` of `minBytesRequired(P)` | `IcebergDecimal` (from bytes or fixed) | `fixed` named `decimal_<id>`, size `ManifestCodec.DecimalRequiredBytes(P)` |
| `uuid` | `string` or `fixed(16)`, RFC 4122 | `fixed(16)` | `Guid` (fixed: big-endian; string: `Guid.Parse`) | `fixed` `uuid_fixed_<id>`, 16 |
| `date` | `int` days | `int` | `int` | `int` + `date` |
| `time-micros` | `long` µs | `time` | `long` µs | `time-micros` |
| `time-millis`, `timestamp-millis` | `int` ms / `long` ms | not used | not converted to µs (unverified) | — |
| `timestamp-micros` | `long` µs, instant | with `adjust-to-utc` (Iceberg property, default `false`) for `timestamp` / `timestamptz` | `long` µs | `adjust-to-utc` false/true |
| `timestamp-nanos` | `long` ns, instant (in the Avro spec at `3a207fa`) | `timestamp_ns` / `timestamptz_ns` with `adjust-to-utc` | `long` ns | as Iceberg |
| `local-timestamp-*` | local time | not used: Iceberg uses `adjust-to-utc: false` instead | not recognised (unverified) | — |
| `duration` | `fixed(12)` LE months/days/ms | not used | — | — |
| `map` (Iceberg-only) | — | `array` of 2-field records `{key, value}` for non-string keys | `Dictionary<object, object?>` | record named `k<keyId>_v<valueId>` |

Promotions on read: `int` → `long`/`time`/timestamps, `float` → `double` (`AvroDataFiles.Convert`). Iceberg's
`variant` (record `metadata`/`value` without field IDs) and `geometry`/`geography` (`bytes`, WKB) are not written.

**Discrepancy:** Iceberg 1.12 appendix A note 2 says "Avro logical type `timestamp-nanos` is an Iceberg convention;
the Avro specification does not define this type". The Avro spec at `3a207fa` does define `timestamp-nanos` (and
`local-timestamp-nanos`). The bytes agree (`long` ns since epoch); only Iceberg's note is stale.

## 4. Iceberg field IDs on Avro schemas

| ID of | Where | Property | Lake.NET |
|---|---|---|---|
| Struct field | record field object | `field-id` | `AvroField.FieldId`; parsed from `f["field-id"]` (`AvroSchemaJson.ReadRecord`) |
| List element | array schema | `element-id` | kept in `AvroSchema.Props`, written verbatim |
| String-keyed map key / value | map schema | `key-id`, `value-id` | same |
| Non-string map key / value | `field-id` on the `key`/`value` fields of the element record | `field-id` | same |

Generic Avro libraries drop or mishandle these, which is the first reason for the in-house codec.

## 5. Schema resolution

Avro's own resolution is **by name** (reader vs writer schema: fields matched by name, `aliases`, defaults for
missing fields, promotions `int→long→float→double`, `string↔bytes`). Iceberg resolves **by field ID** instead, so
Lake.NET decodes every record with the **writer's** schema from `avro.schema` and then maps by ID:

| Reader | Resolution | Code |
|---|---|---|
| Manifests, manifest lists | `ById` accessor: field index by `field-id`; missing optional ⇒ null; missing required ⇒ `FormatException("Manifest is missing required field N")`. Covers every writer's names (v1 `added_data_files_count` vs v2 `added_files_count`) | `ManifestCodec.ById` |
| Data files | Projection by `field-id` when any field has one; else by name through the table's `NameMapping`, then the field name; absent columns get the Iceberg initial default (`Defaults.ToValue`); unprojected fields skipped without materialising (`Skip`) | `AvroDataFiles.ReadBatchesAsync`, `ReadBlock` |

The schema parser (`AvroSchemaJson`) keeps: records (`record`/`error`), arrays, maps, `fixed`, unions, primitives,
`logicalType`, and every non-reserved property. Limits (unverified against other writers' files, none seen to need
them): `enum` is refused (`NotSupportedException`, "not used by Iceberg"); named types are registered by `name` only,
so a reference by full name (`namespace.Name`) fails with `FormatException`; `aliases` are ignored; only a `null`
field default is retained (`AvroField.DefaultNull`), so a non-null Iceberg default is not written as an Avro default,
which appendix A asks of writers ("must convert the default to an equivalent Avro default").

## 6. Codecs

| `avro.codec` | Avro spec | Lake.NET read | Lake.NET write |
|---|---|---|---|
| `null` | required | ✅ | ✅ (`AvroCodecKind.Null`) |
| `deflate` | required; raw RFC 1951, no zlib header or checksum | ✅ `DeflateStream`, output capped at 128 MiB | ✅ default (`CompressionLevel.Optimal`) |
| `snappy` | optional; each block followed by 4-byte big-endian CRC32 of the uncompressed data | refused: `NotSupportedException` | — |
| `zstandard` | optional | refused: `NotSupportedException` | — |
| `bzip2`, `xz` | optional | refused: `NotSupportedException` | — |

Iceberg Java's `write.avro.compression-codec` also offers `zstd` and `snappy` (unverified here: Iceberg's configuration
page was not re-read); a table configured that way produces manifests this library refuses. Adding `zstandard`
needs only ZstdSharp (already a dependency of Puffin); `snappy` needs Snappier and the trailing CRC check.

## 7. Why in-house

From `docs/libraries.md` ("Avro: why in-house"): Iceberg needs arbitrary properties on fields and arrays
(`field-id`, `element-id`, `key-id`, `value-id`) preserved verbatim; logical types on `fixed` (Apache.Avro 1.12.0
fails to parse `{"type":"fixed","logicalType":"uuid",…}` inside a union: `SchemaParseException: Undefined name:
fixed`); the non-standard `logicalType: map` on arrays; and ID-based reads across every writer's schema variant. The
OCF and binary encoding are small and stable (`src/Iceberg.Net.Avro` is 1,096 lines with the manifest schemas;
`docs/libraries.md` still says ~500), and PyIceberg made the
same choice. `Iceberg.Net.Avro` targets `net10.0` and `netstandard2.0`.

## 8. Corruption and limit hardening

Every public decode entry is wrapped in `Corruption.Guard` (`src/Iceberg.Net.Core/Corruption.cs`): bad bytes surface as
`FormatException`, `InvalidDataException`, `EndOfStreamException`, `NotSupportedException` or `JsonException`, never a
cast or index exception, and lengths are checked before they size an allocation.

| Hostile input | Limit / check | Where |
|---|---|---|
| Block size claim | ≤ remaining bytes and ≤ `MaxBlockBytes` (128 MiB) | `AvroContainer.ReadUnguarded`, `AvroDataFiles.ReadBlockPrefix` |
| Deflate bomb | inflated output ≤ 128 MiB (`Corruption.ReadAtMost`) | `AvroBinary.Inflate` |
| Record count claim | ≤ `MaxRecordsPerBlock` (2²²); ≤ block bytes when the schema's minimum encoding is > 0 (`MinEncodedBytes`, linear over shared named types); ≤ `MaxZeroWidthRecords` (2²⁰) otherwise | `AvroBinary.CheckRecordCount` |
| Array/map block count | ≤ remaining bytes; `long.MinValue` refused | `AvroBinary.ReadBlockCount` |
| `bytes`/`string` length | 0 ≤ length ≤ `int.MaxValue` and ≤ remaining | `AvroBinary.ReadLength` |
| `fixed` size from the schema | ≤ 128 MiB and ≤ remaining (when seekable) | `AvroBinary.Read` |
| Varint | ≤ 10 bytes; `int` range checked | `AvroBinary.ReadLong`, `ReadPrimitive` |
| Union branch | 0 ≤ branch < branches | `AvroBinary.Read` |
| Sync marker | must equal the header's | both readers |
| Missing `avro.schema` | `InvalidDataException` | both readers |

| Test | Covers |
|---|---|
| `DecoderLimitTests` (Spec) | `An_avro_block_cannot_claim_billions_of_zero_byte_records`, `…_more_records_than_its_bytes_hold`, `…_millions_of_records_even_with_the_bytes_for_them`, `A_deflate_block_cannot_inflate_past_the_block_limit`, `A_fixed_size_from_the_file_cannot_size_an_allocation`, `A_huge_fixed_is_refused_on_a_stream_that_cannot_report_its_length`, `Measuring_a_schema_that_reuses_named_records_stays_linear`, `The_writer_splits_blocks_so_its_own_files_stay_readable` |
| `CorruptionTests` (Spec) | bit flips and truncations of `Manifest_list_avro`, `Manifest_avro`, `Avro_data_file(streamed)` must fail with a format error |
| `AvroSpecTests` (Spec) | `Zigzag_varints_match_avro_spec`, `Schema_json_keeps_iceberg_properties`, `Decimal_fixed_sizes_match_java`, manifest and manifest-list round trips, `Container_handles_multiple_blocks`, appendix D single-value serialisation |
| `AvroDataFileTests` (Spec) | every primitive and nested type round-trips (v2, v3); deletes, metrics and lineage on Avro files; `Unsupported_formats_are_refused` |
| `SparkFixtureTests`, `PyIcebergFixtureTests` | manifests written by Iceberg Java and PyIceberg |
| Fuzz (`tests/Iceberg.Net.Fuzz`, `fuzz.yml` weekly) | targets `manifest-list`, `manifest`; **no target for Avro data files** (goal G7, gap review) |

## 9. Implementation map

| Path | Role |
|---|---|
| `src/Iceberg.Net.Avro/AvroCodec.cs` | `AvroBinary` (encoding, limits), `AvroContainer` (OCF read/write), `AvroCodecKind` |
| `src/Iceberg.Net.Avro/AvroSchema.cs` | `AvroSchema` model (`Props`, `LogicalType`, `FieldId`), `AvroSchemaJson` parser/writer |
| `src/Iceberg.Net.Avro/ManifestCodec.cs` | Iceberg manifest and manifest-list schemas per format version; `ById` reads; `ToAvro(IcebergType)` |
| `src/Iceberg.Net.Avro/ManifestFormat.cs` | Format-version facts for manifests |
| `src/Iceberg.Net.Data/AvroDataFiles.cs` | Avro data files: `SchemaFor` (Java's naming), streamed reads, projection, `Skip` |
| `src/Iceberg.Net.Core/Corruption.cs` | The decoder contract |
| `src/Iceberg.Net.Data/TableWriter.cs` | `write.format.default`: `parquet` (default) or `avro`; anything else refused |
