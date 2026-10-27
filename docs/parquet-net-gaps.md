# Parquet.Net gaps: why the data plane uses ParquetSharp

Iceberg.NET reads and writes Parquet through **ParquetSharp** (a wrapper over Apache Arrow's C++ `parquet-cpp`). A
pure-managed **Parquet.Net** would remove the native dependency: no per-platform native binaries, and it would work
wherever .NET runs (WASM, mobile, trimmed or AOT apps). This page lists what Iceberg.NET and DuckLake need that
Parquet.Net cannot do yet. Each item is a candidate to upstream, or to carry in the fork
([rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet)) until it lands.

Checked against Parquet.Net `master` at `658e1ca` (2026-10-26). "Confirmed" means a program run against that source
showed it; the others come from reading the source (paths are relative to `src/Parquet/`).

> **Upstream policy.** Since `658e1ca`, Parquet.Net does not accept issues, pull requests or comments written by an
> LLM. Such submissions are treated as spam and the account is blocked (`docs/README.md`, issue templates). This page
> is notes for a person to work from. Issues and PRs upstream have to be written by hand. The policy does not cover a
> fork: the fork can carry machine-assisted changes, which is not true of contributions sent upstream.

## Where ParquetSharp is used

Everything is in `Iceberg.Net.Data`:

| File | What it does with ParquetSharp |
|---|---|
| `ParquetFiles.cs` | Writes data files (schema with field IDs, logical annotations, Zstd, row groups, optional statistics, encryption) and reads them in batches (field-ID projection, physical-value readers, footer statistics for row-group pruning, ranged reads) |
| `NestedColumns.cs` | Shreds and assembles lists, maps and structs with explicit definition and repetition levels (physical column readers and writers) |
| `ParquetInspection.cs` | Reads only the footer of an existing file: fields, sizes, per-column statistics and type compatibility (DuckLake `AddDataFilesAsync`) |
| `ParquetImport.cs` | Iceberg `add_files`-style import of existing files (footer metrics) |

## Gaps

Priority: **P0** blocks switching, **P1** is needed for full feature parity, **P2** is a convenience or an optimisation.
The table describes upstream `658e1ca`; the [fork status](#fork-status) section says which of these the fork now
covers.

| # | Need | ParquetSharp API used | Parquet.Net today | Priority |
|---|---|---|---|---|
| 1 | **Read field IDs.** Iceberg and DuckLake map columns by Parquet `field_id`, not by name | `Node.FieldId` | **Confirmed gap.** `DataField.FieldId` is written (`Encodings/SchemaEncoder.cs`) but never set on read: a file written with `FieldId = 7` reads back `-1`. It is only reachable in the raw thrift `FileMetaData.Schema` | P0 |
| 2 | **Field IDs on group nodes**: structs, lists and maps carry IDs too (Iceberg schemas give every field an ID) | `GroupNode(..., fieldId)` | **Gap.** Only `DataField` has `FieldId`; `StructField`, `ListField` and `MapField` do not | P0 |
| 3 | **Read and write physical values without logical conversion**: `long` microseconds or nanoseconds for timestamps and times, `int` days for dates, unscaled `long`/`byte[]` for decimals. Iceberg's value model is physical, and conversions cost allocations | `LogicalReaderOverride<T>`, `LogicalWriterOverride<T>` | **Confirmed gap.** `ReadRawAsync<T>` requires `T` to be the field's CLR type (`DataField.IsCompatibleWith`): reading a `TIMESTAMP(NANOS)` column as `long` throws. `DateTime` holds 100 ns ticks, so `timestamp_ns` loses its last two digits | P0 |
| 4 | **Parquet modular encryption**: AES-GCM, encrypted footer (`PARE`), a footer key, read and write (DuckLake encrypted lakes; Iceberg's coming table encryption) | `FileEncryptionPropertiesBuilder`, `FileDecryptionPropertiesBuilder` | **Gap.** No encryption support. `ParquetActor.ValidateFileAsync` rejects any file not starting and ending with `PAR1` | P1 |
| 5 | **`FIXED_LEN_BYTE_ARRAY(n)` for arbitrary byte values** (Iceberg `fixed[n]`) | `PrimitiveNode(..., FixedLenByteArray, n)` | **Gap.** `byte[]` always maps to `BYTE_ARRAY`; FLBA is only written for `decimal`, `Guid` and `Interval` (`SchemaEncoder.Encode`) | P1 |
| 6 | **Typed footer statistics with the column's sort order**: min/max decoded to physical values, null counts, whether min/max are present, and unsigned or unknown sort orders refused for pruning | `Statistics<T>`, `HasMinMax`, `ColumnDescriptor.SortOrder` | **Partial.** `ParquetRowGroupReader.GetStatistics` returns min/max as CLR values (subject to item 3), and `ColumnOrders` is only in the raw thrift. Usable, but each caller has to re-derive the sort-order rules | P1 |
| 7 | **Read legacy list and map shapes**: two-level lists, repeated fields named `array` or `*_tuple`. The Iceberg spec requires these backward-compatibility rules | `NestedColumns.FromFile` handles them over ParquetSharp's schema tree | **To verify.** `Schema/ListField.cs` notes "assuming non-legacy lists"; not tested against files with these shapes | P1 |
| 8 | **Turn statistics off per file** (delete and position files where bounds are useless) | `WriterPropertiesBuilder.DisableStatistics()` | **Gap**, minor: `ParquetOptions` has no switch | P2 |
| 9 | **Physical schema facts per leaf**: physical type, type length, definition and repetition levels, unsigned/millis annotations, footer length | `ColumnDescriptor`, `LogicalType` subclasses | **Mostly there**, through the raw thrift `FileMetaData` and `SchemaElement`, but as wire structs rather than a typed descriptor | P2 |

Already covered by Parquet.Net, so not a blocker:

- Every logical annotation written today: `INT(bw, signed)` via `sbyte`/`byte`/…/`ulong`, `TIMESTAMP` and `TIME` with
  unit and `isAdjustedToUTC` (`DateTimeDataField`, `TimeDataField`), `DECIMAL` on `INT32`/`INT64`/FLBA
  (`DecimalDataField`, `BigDecimal`), `UUID`, `DATE`, `INTERVAL` (`Interval`), `LIST`/`MAP`.
- Zstd (`CompressionMethod.Zstd`, via ZstdSharp), row groups under the writer's control, custom key-value metadata.
- Low-level writes with explicit definition and repetition levels (`ParquetRowGroupWriter.WriteAllPartsAsync`), and
  raw reads of values plus levels (`ReadRawAsync`), once item 3 allows physical types.
- Column chunk metadata (`ParquetRowGroupReader.GetMetadata`: compressed sizes, value counts, offsets) and the whole
  thrift `FileMetaData` (`ParquetReader.Metadata`).
- Reading from any seekable `Stream`, which is enough for Iceberg.NET's `RangeReadStream` (ranged object-store
  reads).
- **Ahead of ParquetSharp:** Parquet.Net's thrift model already has the `VARIANT`, `GEOMETRY` and `GEOGRAPHY`
  logical types (`Meta/Parquet.cs`); ParquetSharp 25.0.1.1 exposes no factory for them. Iceberg v3 and DuckLake both
  use these types, and DuckLake's `variant` and `geometry` are unimplemented partly for this reason.

## Fork status

Branch `ccr-87fadcf9-bxi9wr` of [rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet), each
change with its own tests (fork suite: 750 passed; the one failure needs `pyarrow`, which the environment lacks).

| Gap | Fork commit | What changed |
|---|---|---|
| 1, 2 field IDs | `927bf99` | `FieldId` moved from `DataField` to `Field`; decoded from `SchemaElement.field_id` for every field, and written on list, map and struct groups |
| 3 physical values | `7ecfa00` | `DataField.PhysicalClrType` (`bool`, `int`, `long`, `float`, `double`, `ReadOnlyMemory<byte>`): `ReadRawAsync<T>`/`WriteAsync<T>` accept it for any column, with the logical annotation kept, so `TIMESTAMP(NANOS)` reads as `long` nanoseconds |
| 5 `fixed[n]` | `54efd9a` | `DataField.FixedLength` writes `byte[]`/`ReadOnlyMemory<byte>` as `FIXED_LEN_BYTE_ARRAY(n)` (and is set on read) |
| 8 no statistics | `54efd9a` | `ParquetOptions.WriteStatistics = false` leaves statistics out of column chunks and page headers |
| 4 encryption | `344675a` | `ParquetOptions.Encryption`/`Decryption`: AES_GCM_V1 and AES_GCM_CTR_V1, encrypted (`PARE`) or signed plaintext footers, the footer key for every column or separate column keys, key metadata and a key retriever, AAD prefixes stored or supplied. AES from the BCL, no new dependency. Interop both ways with parquet-cpp in all five modes (fixtures written by ParquetSharp are in the fork's tests); DuckDB 1.5.5 reads the GCM files in full scans. 2M rows: write 477 ms plain / 482 ms GCM, read 781 / 921 ms |
| 6 typed statistics | `35bd5c1` | `DataColumnStatistics.SortOrder` (signed, unsigned, unknown, from the type and the file's column orders), `HasMinMax`, `MinBytes`/`MaxBytes`, `PhysicalMin`/`PhysicalMax`, exactness, `NanCount`; the deprecated `min`/`max` used only for signed numeric columns, nothing for unknown orders |
| 7 legacy lists | `ff03dcc` | Backward-compatibility rules 2 and 3 (a repeated group with several fields, or named `array` or `<list>_tuple`, is a struct element); rules 1 and the bare repeated field already worked. Tested on files written by parquet-cpp. Legacy maps (`MAP_KEY_VALUE`, any group name) already worked |

Bugs found on the way, also fixed in the fork. These are worth reporting upstream on their own, by hand:

- **Timestamp statistics in the wrong unit** (`7ecfa00`). Min/max of an `INT64` timestamp column were always encoded
  as milliseconds, whatever the column's unit. A `TIMESTAMP(MICROS)` file written by Parquet.Net has min/max 1000×
  too small, so engines that prune on statistics drop row groups they should keep: DuckDB returns **no rows** for
  `WHERE ts > TIMESTAMP '1970-01-01 00:00:01'` on such a file, and the right rows once the statistics are fixed.
- **UUID statistics in the wrong byte order** (`7ecfa00`). Min/max of a `UUID` column were `Guid.ToByteArray()`
  (little-endian fields) rather than the big-endian RFC 4122 bytes the values themselves use.
- **`List<T?>` deserialization loses data** (`ff03dcc`). `ParquetSerializer` read `[1, null, 2]` back as `[1, 2]` and
  `[]` as `null`, for lists and arrays of nullable values, including files it wrote itself. The levels on disk were
  right; the assembler created a list only at the item's definition level, one too deep for nullable items.
- **Decimal and unsigned statistics decode wrongly** (`35bd5c1`). Min/max of decimals stored as `INT32`/`INT64` came
  back as raw unscaled integers (the decimal branch was unreachable), and a `uint64` max of 2^64−1 as −1.
- **The writer leaves `dictionary_page_offset` unset** (`344675a`), pointing `data_page_offset` at the dictionary
  page instead. Readers tolerate it, but encrypted readers need the dictionary page's offset to pick its AAD.
- **`DATA_PAGE_V2` pages of repeated columns throw** `NotImplementedException` (`bfd86da`), and the values section of
  compressed v2 pages was decompressed to the whole page's size. Tested on v2 files written by parquet-cpp.
- **Legacy-named list groups deserialize as null** (`ff03dcc`). When a file's list groups are named differently from
  the class schema's (`array`, `bag`, `<name>_tuple`), the serializer found no column for the property and silently
  left it null. It now falls back to matching on field names.

Still open in the fork: a typed physical-schema descriptor (9; the raw thrift covers it). With the P0 and P1 items
done, the next step is a Parquet.Net backend for `ParquetFiles` behind its existing API, run against the Spec, DuckLake
and cross-engine suites alongside ParquetSharp. The fork's GitHub Actions workflow is not enabled (dispatching it
returns 404, the default for forks), so its suite has only been run locally; enabling Actions on the fork would let
it run on Linux, Windows and macOS.

## Suggested order

1. **Field IDs on read, and on group nodes** (1, 2). Small and self-contained: decode `SchemaElement.field_id` into
   every `Field`, and add `FieldId` to `Field` rather than only `DataField`. This alone would let the read path map
   columns the Iceberg way.
2. **A physical-value mode** (3). For example, an option or a `DataField` flag that keeps `ClrType` at the physical
   type (`long`/`int`/`byte[]`) and still writes the logical annotation. This is the biggest change and the one the
   performance case rests on (no `DateTime` round trip, nanosecond precision).
3. **`fixed[n]`** (5) and **no-statistics** (8): small writer options.
4. **Legacy list/map reading** (7): write regression files with Spark/parquet-mr shapes first, then fix what fails.
5. **Encryption** (4): the largest piece. AES-GCM is in the BCL (`System.Security.Cryptography.AesGcm`), so it can
   stay dependency-free; the spec's AAD construction (module types, ordinals) is the work.
6. **Typed statistics with sort order** (6).

With 1–3 done, `ParquetFiles` could gain a Parquet.Net backend behind its existing API, and the Spec, DuckLake and
cross-engine suites would compare the two. ParquetSharp could then stay only for encryption until item 5 lands.
