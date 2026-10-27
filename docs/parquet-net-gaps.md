# Parquet backends, and what upstream Parquet.Net lacks

Iceberg.NET reads and writes Parquet through one of two libraries:

- **ParquetSharp** (a wrapper over Apache Arrow's C++ `parquet-cpp`), the default;
- **Parquet.Net**, fully managed (no per-platform native binaries, so it runs wherever .NET does: WASM, mobile, trimmed
  or AOT apps), from the fork [rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet) carried
  as the `external/parquet-dotnet` git submodule ([Backends](#backends)).

Upstream Parquet.Net could not do what Iceberg.NET and DuckLake need. This page lists those gaps, what the fork does
about each, and the upstream bugs found on the way: each is a candidate to upstream, by hand.

Checked against Parquet.Net `master` at `658e1ca` (2026-10-26). "Confirmed" means a program run against that source
showed it; the others come from reading the source (paths are relative to `src/Parquet/`).

> **Upstream policy.** Since `658e1ca`, Parquet.Net does not accept issues, pull requests or comments written by an
> LLM. Such submissions are treated as spam and the account is blocked (`docs/README.md`, issue templates). This page
> is notes for a person to work from. Issues and PRs upstream have to be written by hand. The policy does not cover a
> fork: the fork can carry machine-assisted changes, which is not true of contributions sent upstream.

## Backends

`Iceberg.Net.Data` talks to Parquet through a small interface (`IParquetBackend`, in `ParquetModel.cs`): a file's
schema tree (physical types, annotations, field IDs), its column chunks (sizes, value counts, statistics as physical
values), and each leaf column of a row group as its definition and repetition levels plus its defined values, typed.
Everything else is written once over it:

| File | What it does |
|---|---|
| `ParquetFiles.cs` | Writes data files (schema with field IDs, logical annotations, Zstd, row groups, optional statistics, encryption) and reads them in batches (field-ID projection, physical-value readers, footer statistics for row-group pruning, ranged reads) |
| `NestedColumns.cs` | Shreds and assembles lists, maps and structs with explicit definition and repetition levels (physical column readers and writers) |
| `ParquetInspection.cs` | Reads only the footer of an existing file: fields, sizes, per-column statistics and type compatibility (DuckLake `AddDataFilesAsync`) |
| `ParquetImport.cs` | Iceberg `add_files`-style import of existing files (footer metrics) |
| `ParquetSharpBackend.cs` | The interface over ParquetSharp's physical column readers and writers |
| `Iceberg.Net.Data.ParquetNet` | The interface over the Parquet.Net fork: `RawDataField` columns declared exactly as stored, raw level reads and writes, `ParquetOptions.Encryption` (not packed until the fork is published) |

Which backend is used:

- ParquetSharp, unless one of the below.
- `ParquetNet.UseAsDefault()` (process-wide), or `using (ParquetNet.Scope()) { … }` for the current async flow only.
- `ICEBERG_PARQUET_BACKEND=parquet-net`, read once; the `Iceberg.Net.Data.ParquetNet` assembly must be present (the
  test projects reference it, so any suite runs on Parquet.Net this way).

Both backends write the same files: the same schema (types, annotations and converted types, field IDs, repetition),
Zstd, footer statistics both read alike, and Parquet modular encryption. `ParquetBackendTests` (Spec suite) checks
that each reads the other's files (every Iceberg type, nested shapes, DuckLake annotations, encryption), that both
declare the same schema and statistics, and that their statistics prune the same row groups. CI's `parquet-net` job
runs the unit suite and the DuckLake suites (DuckDB 1.5 and 2.0 as oracles, SQLite and PostgreSQL catalogs) on
Parquet.Net; locally, on each backend: Spec 826 passed, EF Core 33, LINQ 84, REST catalog behaviour 89, DuckLake 544
passed / 50 skipped and the DuckLake BDD specification 95 / 16 (DuckDB 1.5 oracle, SQLite and PostgreSQL catalogs).

Through `ParquetFiles` (`tools/ParquetBackendBench`, Release, best of three after a warm-up; the flat table is `long`,
`timestamptz`, a string of 1,000 distinct values with 10% nulls, `decimal(12,2)`, `double` and `uuid`, the nested one a
list of strings, a map and a struct; row groups of 250,000 rows; Zstd):

| Backend | Table | Write | Read | Size |
|---|---|---|---|---|
| ParquetSharp | flat, 1,000,000 rows | 2,489 ms | 1,227 ms | 33.6 MB |
| Parquet.Net | flat, 1,000,000 rows | 2,300 ms | 1,347 ms | 29.0 MB |
| ParquetSharp | nested, 250,000 rows | 726 ms | 853 ms | 4.5 MB |
| Parquet.Net | nested, 250,000 rows | 718 ms | 941 ms | 3.5 MB |

Parquet.Net writes a little faster and smaller (its dictionary and Zstd settings differ) and reads within about 10%
(run to run, the benchmark varies by about as much). Reading the flat table was 26% slower at first; profiling each
column on its own (forced GC, best of seven) put all of it on two columns:

- **strings**: Parquet.Net returned each value in its own `char[]`, which the backend then copied into a string. The
  fork now decodes a page's text into one array (`003c77f`), and the backend keeps one string per distinct value (up
  to 4,096) rather than a new one per row: the string column went from 311 to 174 ms (ParquetSharp 159);
- **UUIDs**: a `byte[]` per value, then a `Guid` from it. The fork now slices one array per page, and the backend
  decodes `Guid`s straight from the bytes (`PqValues.Uuids`): from 319 to 164 ms (ParquetSharp 185).

Per column (same file, Parquet.Net's): id 139 / 137, ts 145 / 137, name 174 / 159, amount 162 / 151, score 154 / 135,
key 164 / 185, all six 979 / 956 ms (Parquet.Net / ParquetSharp).

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
change with its own tests (fork suite: 772 passed, 3 skipped on net8.0 and net10.0, with `pyarrow` and `pandas` installed for its Arrow interop test;
CI runs it on Linux, Windows and macOS). For upstreaming, the fork's `docs/upstream-review.md` splits it into review
units, and `docs/upstream/` has every unit as a verified patch (its tests failing or not compiling without it, the
suite passing with it), on its own and as an ordered series that applies to upstream `master` without conflicts.

| Gap | Fork commit | What changed |
|---|---|---|
| 1, 2 field IDs | `927bf99` | `FieldId` moved from `DataField` to `Field`; decoded from `SchemaElement.field_id` for every field, and written on list, map and struct groups |
| 3 physical values | `7ecfa00` | `DataField.PhysicalClrType` (`bool`, `int`, `long`, `float`, `double`, `ReadOnlyMemory<byte>`): `ReadRawAsync<T>`/`WriteAsync<T>` accept it for any column, with the logical annotation kept, so `TIMESTAMP(NANOS)` reads as `long` nanoseconds |
| 5 `fixed[n]` | `54efd9a` | `DataField.FixedLength` writes `byte[]`/`ReadOnlyMemory<byte>` as `FIXED_LEN_BYTE_ARRAY(n)` (and is set on read) |
| 8 no statistics | `54efd9a` | `ParquetOptions.WriteStatistics = false` leaves statistics out of column chunks and page headers |
| 4 encryption | `344675a` | `ParquetOptions.Encryption`/`Decryption`: AES_GCM_V1 and AES_GCM_CTR_V1, encrypted (`PARE`) or signed plaintext footers, the footer key for every column or separate column keys, key metadata and a key retriever, AAD prefixes stored or supplied. AES from the BCL, no new dependency. Interop both ways with parquet-cpp in all five modes (fixtures written by ParquetSharp are in the fork's tests); DuckDB 1.5.5 reads the GCM files in full scans. 2M rows: write 477 ms plain / 482 ms GCM, read 781 / 921 ms |
| 6 typed statistics | `35bd5c1` | `DataColumnStatistics.SortOrder` (signed, unsigned, unknown, from the type and the file's column orders), `HasMinMax`, `MinBytes`/`MaxBytes`, `PhysicalMin`/`PhysicalMax`, exactness, `NanCount`; the deprecated `min`/`max` used only for signed numeric columns, nothing for unknown orders |
| 9 physical schema | `da1574b` | `ParquetReader.Columns`: a descriptor per leaf column (path, ordinal, physical type, FLBA length, logical and converted types, field ID, max levels, sort order), and `ParquetReader.FooterLength` |
| (backend) raw columns | `f12c75b`, `02925d8`, `326b529` | `RawDataField`: a column declared by its exact physical type, logical and converted type and length, read and written as physical values (text columns also as strings, so dictionary encoding applies) |
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
- **No `column_orders` in the footer** (`792c737`). Without them, parquet-cpp (so pyarrow and DuckDB) and parquet-mr
  trust the min/max of unsigned-order columns only from writers they know: every string and binary bound Parquet.Net
  wrote was ignored for pruning. pyarrow reports `has_min_max: False` for such a file, and `True` once the footer says
  `TYPE_ORDER`.
- **Statistics in the wrong order** (`f12c75b`): unsigned columns written as their signed bits compared signed; UUIDs
  compared with `Guid.CompareTo` (mixed-endian fields) rather than as bytes; strings compared by UTF-16 code unit,
  which puts U+E000–U+FFFF after supplementary characters, unlike UTF-8 byte order; float bounds could be NaN, and
  zero bounds were not written as -0.0/+0.0. Byte-array columns (binary, fixed) got no bounds at all, and boolean
  columns none either (`229f9d7`).
- **Statistics not what `TYPE_ORDER` requires** (`a28da59`): no `nan_count` on floating point columns (required once
  the footer declares `TYPE_ORDER`), and the deprecated signed `min`/`max` written for strings and byte arrays too.
  Columns written `BYTE_STREAM_SPLIT` got no min/max at all.
- **Required maps are written `OPTIONAL`** (`02925d8`): the levels are computed as required, so the file's levels
  disagree with its own schema.
- **Legacy-named list groups deserialize as null** (`ff03dcc`). When a file's list groups are named differently from
  the class schema's (`array`, `bag`, `<name>_tuple`), the serializer found no column for the property and silently
  left it null. It now falls back to matching on field names.

### Iceberg files, both ways

One schema covering every shape Iceberg.NET writes (required `long`; `timestamp` and `timestamp_ns` as physical
`long`s; `date` as days; `decimal(10,2)`; `uuid`; `fixed[4]`; `string`; `list<int>`; `map<string,long>`; a struct),
three rows with nulls, empty and null collections and a struct with a null field:

| Written by | Read by | Result |
|---|---|---|
| `ParquetFiles` (ParquetSharp) | `ParquetFiles` | the rows written |
| the fork, with Iceberg's layout and field IDs | `ParquetFiles` (field-ID projection) | the same rows |
| `ParquetFiles` | the fork | field IDs on every leaf and group, nanoseconds as `long`, `fixed[4]` length, timestamp and UUID statistics as written |
| the fork | DuckDB 1.5.5 | the same rows; `ts >` and `u =` filters (pruned by statistics) return the right rows |
| the fork | pyarrow 26 | the same rows; `timestamp[ns]` kept |

The fork's suite carries the parquet-cpp half of this as tests (`ParquetCppInteropTest`, with ParquetSharp as a
test-only dependency), plain and encrypted.

### Performance against ParquetSharp

2M rows of Iceberg-shaped physical columns (`long` id, `timestamp` as `long` microseconds, a string with 1,000
distinct values, `double`), Zstd, one row group, best of 3 after a warm-up, Release:

| Library | Write | Read | Size |
|---|---|---|---|
| Parquet.Net fork | 369–376 ms | 169–187 ms | 7.4 MB |
| ParquetSharp 25.0.1.1 | 401–406 ms | 177–190 ms | 8.2 MB |

Two things got it there. Parquet.Net dictionary-encodes a string column only when asked
(`ParquetOptions.ColumnEncodingHints[path] = EncodingHint.Dictionary`); without it the string column alone took
212–374 ms to read, so a backend should set the hint the way ParquetSharp's default dictionary encoding does. And Zstd
decompression copied each page twice (fixed in `cbcc43d`): reads went from 399 to about 180 ms. Without
compression the fork is ahead (write 173 vs 360 ms, read 81 vs 111 ms).

Every gap in the table is closed in the fork, and the [Parquet.Net backend](#backends) runs on it. A bug of the fork's
own, not upstream's, was found by the backend's parallel reads and fixed in `12b2856`: the Zstd decompressor cache
(`cbcc43d`) was taken before an `await`, so concurrent reads could share one decompressor ("Data corruption
detected"). `12b2856` also adds `ParquetOptions.ValidateHeaderMagic`, which the backend turns off over ranged storage
(parquet-cpp does not read the first four bytes either).

The fork's own GitHub Actions workflow is not enabled (dispatching it returns 404, the default for forks), so its suite
runs locally and through Iceberg.NET's CI, which builds it from the submodule and runs the backend's suites on Linux.
Enabling Actions on the fork would add its Windows and macOS matrix.

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
