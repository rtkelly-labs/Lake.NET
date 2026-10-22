# Libraries under the hood

What a complete Iceberg implementation needs beneath the spec layer, what .NET offers for each, and what we chose
(or will have to build). Versions as of 2026-10; sources in [research/ecosystem-2026-10.md](research/ecosystem-2026-10.md#5-relevant-net-libraries).

| Need | Used for | .NET options | Decision |
|---|---|---|---|
| **Avro** (object container files) | manifests, manifest lists (v1–v3) | Apache.Avro 1.12.x | **In-house codec** (`Iceberg.Net.Avro`, ~500 lines) |
| **Parquet** | data files, v4 manifests | ParquetSharp 25 (Arrow C++), Parquet.Net 6 (managed) | **ParquetSharp** |
| **ORC** | data files (optional) | none mature (ApacheOrcDotNet 2020, EngineeredWood.Orc experimental) | Out of scope; refuse ORC files explicitly |
| **Compression** | Avro deflate; Parquet zstd/snappy/gzip; Puffin lz4/zstd | BCL `DeflateStream`/`GZipStream`; ZstdSharp.Port; Snappier; K4os LZ4 | BCL for Avro deflate (Iceberg default); Parquet codecs come with ParquetSharp; ZstdSharp for Puffin later |
| **Roaring bitmaps** | v3 deletion vectors, position-delete indexes | Roaring.Net (native CRoaring), Equativ.RoaringBitmaps (managed, young) | Build a small managed **Roaring64 portable** serializer (DV wire format is narrow) |
| **Puffin** | DVs, table statistics | none | Build (simple container: magic, blobs, JSON footer) |
| **Theta sketches** | NDV statistics blobs | Clast.Sketches 0.1 (very new) | Defer; statistics are optional |
| **Murmur3 x86 32** | bucket transform | various | In-house, pinned by spec vectors |
| **JSON** | metadata, REST | System.Text.Json | `JsonNode` for exact field control and forward-compatible round-trips |
| **AWS** | S3, Glue, Athena | AWSSDK v4 | Used directly |
| **SigV4 for HttpClient** | Glue/S3 Tables REST endpoints | AWSSDK internals | Small `DelegatingHandler` (M1) |
| **Arrow** | public columnar API, zero-copy into engines | Apache.Arrow 23 (now `apache/arrow-dotnet`) | Planned public surface (M3) |
| **Expressions** | predicate pushdown, partition pruning, metrics evaluation | none | Build (port Java's `Expressions`/`ManifestEvaluator`/`InclusiveMetricsEvaluator`) |
| **Variant / geo encodings** | v3 `variant` binary encoding & shredding, WKB | none for variant; NetTopologySuite for WKB | Later |

## Avro: why in-house

Iceberg needs three things generic Avro libraries handle poorly:

1. Arbitrary properties on fields and arrays (`field-id`, `element-id`, `key-id`, `value-id`) preserved on read and written verbatim.
2. Logical types on `fixed` (`uuid`, `decimal`) — **Apache.Avro 1.12.0 fails to parse** `{"type":"fixed","logicalType":"uuid",…}`
   inside a union (`SchemaParseException: Undefined name: fixed`; reproduced while building M0).
3. The non-standard `logicalType: map` on arrays of `k/v` records (Iceberg's int-keyed maps).

Reading must also resolve columns **by field ID** across every writer's schema variant (v1 names like
`added_data_files_count` vs v2 `added_files_count`). The Avro binary encoding and container format are small and
stable, so owning them is cheaper than working around a library. PyIceberg made the same call.

Supported codecs: `null`, `deflate` (Iceberg's default). `snappy`/`zstd` Avro codecs are easy additions if a writer
we must read uses them.

## Parquet: why ParquetSharp

Iceberg requires Parquet `field_id` on **every** node, including struct groups, list elements and map key/value
groups, because readers project by ID. Parquet.Net exposes `FieldId` on leaf `DataField`s only (not on
`StructField`/`ListField`/`MapField`), which makes nested Iceberg columns impossible without a fork. ParquetSharp
(G-Research; wraps Arrow's parquet-cpp, the same writer pyarrow uses) supports field IDs on all nodes — verified by
writing a nested `list<string>` with IDs and reading the IDs back with pyarrow.

Costs: native binaries per runtime identifier (linux-x64/arm64, win-x64, osx) — fine for Lambda/containers, a
consideration for exotic targets; `net472` consumers are supported by ParquetSharp but not by this solution yet.

Gotchas found:
- Read timestamps/dates as raw `long`/`int` with `LogicalReaderOverride<long?>()` — the default logical reader
  converts to `DateTime` and loses nanoseconds.
- Column chunk sizes for `column_sizes` metrics come from the written file's metadata.
- Parquet statistics are not trusted for Iceberg metrics; we compute bounds from the values written
  (NaN excluded, NaN counted separately; strings longer than 16 code points omit bounds rather than truncate).

## Roaring bitmaps and deletion vectors

The v3 DV blob is: 4-byte big-endian length, magic `D1 D3 39 64`, a Roaring64 *portable* serialization (8-byte LE
count of 32-bit bitmaps, then per key a 4-byte LE key and a standard portable 32-bit Roaring bitmap), then a
4-byte big-endian CRC-32 over magic + vector. It must be uncompressed and carry `referenced-data-file` and
`cardinality` properties. Only *serialization compatibility* matters, so a focused managed implementation of the
portable format (array/bitmap/run containers) with Java/Delta test vectors is lower risk than a native dependency.

## CLR type pitfalls

These caused or would cause silent cross-engine disagreement; each has a test.

| Pitfall | Consequence | Mitigation |
|---|---|---|
| `Guid.ToByteArray()` is mixed-endian | wrong uuid bucket and bounds | always `ToByteArray(bigEndian: true)` (`TransformSpecTests.Guid_default_byte_order_would_break_uuid_buckets`) |
| `System.Decimal` is 96-bit (28–29 digits) | Iceberg allows precision 38 | `IcebergDecimal` = BigInteger + scale |
| `DateTime` ticks are 100 ns | `timestamp_ns` loses 2 digits | physical `long` nanos; helpers take extra nanos explicitly |
| `string` length is UTF-16 units | `truncate[W]` must count code points | rune enumeration (`Truncate_counts_code_points_not_utf16_units`) |
| `%` on negatives | truncate/time transforms must floor | explicit floor-mod/floor-div (`Temporal_transforms_floor_towards_negative_infinity`) |
| Boxed `int` vs `long` after promotion | equality checks fail on promoted columns | widen on read (`Conversions.FromBytes`, Parquet INT32→long) |
| `DateTimeKind.Unspecified` | ambiguous timestamps | treated as UTC; `timestamp` (no tz) values are wall-clock micros |
