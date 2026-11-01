# .NET types and Parquet types

How Lake.Parquet maps .NET types to Parquet's physical types and annotations, in both directions, through the core
API (`DataField`, `ParquetRowGroupWriter.WriteAsync`, `ParquetRowGroupReader.ReadAsync`). The rule behind it is
[decision 9](decisions.md): where the format has a type matching a .NET type, the type is supported, losslessly by
default; a mapping with more than one representation, or one that can lose precision, is asked for.

Every row here was checked by writing the type, opening the file with pyarrow, and reading it back
(1 November 2026, `master` at `8eff9f2d`). Interop with other writers is tested by the conformance harness against
parquet-cpp in both directions ([testing.md](testing.md)).

**Legend**

| Mark | Meaning |
|---|---|
| ✓ | Maps cleanly: lossless both ways, by default, and other readers see the right type |
| ⚠ | Works, with an exception (an option, a choice of representation, a limit or a known bug): follow the link |
| ✗ | Not supported: the link says why and what to use instead |

## From .NET: what a type is written as

| .NET type | Declare with | Parquet physical type | Annotation | Write | Read back | Page |
|---|---|---|---|---|---|---|
| `bool` | `DataField<bool>` | BOOLEAN | none | ✓ | ✓ | |
| `sbyte`, `byte` | `DataField<sbyte>` | INT32 | INT(8, signed / unsigned) | ✓ | ✓ | |
| `short`, `ushort` | `DataField<short>` | INT32 | INT(16, signed / unsigned) | ✓ | ✓ | |
| `int`, `uint` | `DataField<int>` | INT32 | INT(32, signed / unsigned) | ✓ | ✓ | |
| `long`, `ulong` | `DataField<long>` | INT64 | INT(64, signed / unsigned) | ✓ | ✓ | |
| `float` | `DataField<float>` | FLOAT | none | ✓ | ✓ | |
| `double` | `DataField<double>` | DOUBLE | none | ✓ | ✓ | |
| `System.Half` | `DataField<Half>` | FIXED_LEN_BYTE_ARRAY(2) | FLOAT16 | ✓ | ✓ | [half](types/half.md) |
| `string` | `DataField<string>` | BYTE_ARRAY | STRING | ✓ | ✓ | [strings and binary](types/strings-and-binary.md) |
| `byte[]` | `DataField<byte[]>` | BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY(n) with a fixed length | none | ✓ | ✓ | [strings and binary](types/strings-and-binary.md) |
| `Guid` | `DataField<Guid>` | FIXED_LEN_BYTE_ARRAY(16) | UUID | ✓ | ✓ | [guid](types/guid.md) |
| `decimal` | `DecimalDataField(name, precision, scale)` | INT32 (p ≤ 9), INT64 (p ≤ 18), FIXED_LEN_BYTE_ARRAY | DECIMAL(p, s) | ⚠ | ⚠ | [decimal](types/decimal.md) |
| `Parquet.Data.BigDecimal` | `BigDecimalDataField`, or `ParquetOptions.UseBigDecimal` | FIXED_LEN_BYTE_ARRAY | DECIMAL(p, s) | ✓ | ⚠ | [decimal](types/decimal.md) |
| `DateTime` | `DataField<DateTime>`, or `DateTimeDataField(name, format, unit, isAdjustedToUTC)` | INT96 (default), INT64, or INT32 for a date | none, TIMESTAMP(unit, UTC), TIMESTAMP_MILLIS/MICROS, DATE | ⚠ | ⚠ | [datetime](types/datetime.md) |
| `DateOnly` | `DataField<DateOnly>` | INT32 | DATE | ✓ | ⚠ | [dateonly](types/dateonly.md) |
| `TimeOnly` | `TimeDataField(name, unit, typeof(TimeOnly))` | INT32 (millis), INT64 (micros, nanos) | TIME(unit) | ⚠ | ⚠ | [time](types/time.md) |
| `TimeSpan` | `TimeDataField(name, unit, typeof(TimeSpan))` | INT32 (millis), INT64 (micros, nanos) | TIME(unit) | ⚠ | ⚠ | [time](types/time.md) |
| `Parquet.File.Values.Primitives.Interval` | `DataField<Interval>` | FIXED_LEN_BYTE_ARRAY(12) | INTERVAL (converted type only) | ✓ | ✓ | [interval](types/interval.md) |
| `System.Numerics.BigInteger` | `DataField<BigInteger>` | INT96 | none | ⚠ | ⚠ | [int96](types/int96.md) |
| Arrays, lists, structs, maps | `ListField`, `StructField`, `MapField` | the element's type, with levels | LIST, MAP | ⚠ | ⚠ | [nested](types/nested.md) |
| `DateTimeOffset` | | | | ✗ | ✗ | [datetimeoffset](types/datetimeoffset.md) |
| `Int128`, `UInt128` | | | | ✗ | ✗ | [int128](types/int128.md) |
| enums | | | | ✗ | ✗ | [enum](types/enum.md) |
| `char`, `nint`, `nuint`, `Memory<T>`, `object` | | | | ✗ | ✗ | [other types](types/unsupported.md) |

## The matrix

Which physical type each .NET type can be stored in. ● is what a plain `DataField<T>` writes; ○ is another
representation, asked for by a field type or an option; a blank cell is not possible. The mark after the type is its
status from the table above.

| .NET type | BOOLEAN | INT32 | INT64 | INT96 | FLOAT | DOUBLE | BYTE_ARRAY | FIXED_LEN_BYTE_ARRAY |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| `bool` ✓ | ● | | | | | | | |
| `sbyte` `byte` `short` `ushort` `int` `uint` ✓ | | ● | | | | | | |
| `long` `ulong` ✓ | | | ● | | | | | |
| `float` ✓ | | | | | ● | | | |
| `double` ✓ | | | | | | ● | | |
| `Half` ✓ | | | | | | | | ● (2) |
| `string` ✓ | | | | | | | ● | |
| `byte[]` ✓ | | | | | | | ● | ○ (n) |
| `Guid` ✓ | | | | | | | | ● (16) |
| [`decimal`](types/decimal.md) ⚠ | | ○ p ≤ 9 | ○ p ≤ 18 | | | | | ● p ≤ 38 |
| [`BigDecimal`](types/decimal.md) ✓ | | | | | | | | ● |
| [`DateTime`](types/datetime.md) ⚠ | | ○ DATE | ○ TIMESTAMP | ● | | | | |
| [`DateOnly`](types/dateonly.md) ⚠ | | ● DATE | | | | | | |
| [`TimeOnly`, `TimeSpan`](types/time.md) ⚠ | | ○ TIME(ms) | ○ TIME(µs, ns) | | | | | |
| [`Interval`](types/interval.md) ✓ | | | | | | | | ● (12) |
| [`BigInteger`](types/int96.md) ⚠ | | | | ● | | | | |
| [`DateTimeOffset`](types/datetimeoffset.md) ✗ | | | | | | | | |
| [`Int128` `UInt128`](types/int128.md) ✗ | | | | | | | | |
| [enums](types/enum.md) ✗ | | | | | | | | |

## From a file: what another writer's column reads as

What a column written by Spark, pyarrow, parquet-cpp, DuckDB or parquet-mr reads as, by its annotation, with the
default options.

| Parquet type | Reads as | Options | Status | Page |
|---|---|---|---|---|
| BOOLEAN, INT32, INT64, FLOAT, DOUBLE, unannotated | `bool`, `int`, `long`, `float`, `double` | | ✓ | |
| INT(8/16/32/64, signed or not), or INT_8 … UINT_64 | the matching .NET integer | | ✓ | |
| FLOAT16 | `Half` | | ✓ | [half](types/half.md) |
| STRING, ENUM, UTF8 | `string` | | ✓ | [strings and binary](types/strings-and-binary.md) |
| BYTE_ARRAY, unannotated | `byte[]` | `TreatByteArrayAsString` reads it as `string` | ✓ | [strings and binary](types/strings-and-binary.md) |
| JSON, BSON | `byte[]` (the UTF-8 text, the BSON document) | | ⚠ | [strings and binary](types/strings-and-binary.md) |
| GEOMETRY, GEOGRAPHY | `byte[]` (WKB); bounding box statistics are read | | ⚠ | [strings and binary](types/strings-and-binary.md) |
| FIXED_LEN_BYTE_ARRAY, unannotated | `byte[]` | | ✓ | [strings and binary](types/strings-and-binary.md) |
| UUID | `Guid` | | ✓ | [guid](types/guid.md) |
| DECIMAL on INT32, INT64, FIXED_LEN_BYTE_ARRAY or BYTE_ARRAY | `decimal`, exact, or refused if it does not fit | `UseBigDecimal` reads `BigDecimal` | ⚠ | [decimal](types/decimal.md) |
| DATE | `DateTime` (midnight, UTC) | `UseDateOnlyTypeForDates` reads `DateOnly` | ⚠ | [dateonly](types/dateonly.md) |
| TIME(ms), TIME_MILLIS | `int` milliseconds | `TimeMapping.TimeOnly` or `TimeSpan` | ⚠ | [time](types/time.md) |
| TIME(µs, ns), TIME_MICROS | `long` in the unit | `TimeMapping.TimeOnly` or `TimeSpan` | ⚠ | [time](types/time.md) |
| TIMESTAMP(ms, µs), UTC-adjusted; TIMESTAMP_MILLIS, TIMESTAMP_MICROS | `DateTime`, `Kind = Utc` | | ✓ | [datetime](types/datetime.md) |
| TIMESTAMP(ns) | `DateTime`, rounded down to 100 ns | | ⚠ | [datetime](types/datetime.md) |
| TIMESTAMP, not UTC-adjusted | `DateTime`, `Kind = Local` | `UnadjustedTimestampsAsWallClock` reads `Unspecified` wall-clock values | ⚠ | [datetime](types/datetime.md) |
| INT96 | `DateTime`, `Kind = Unspecified`, rounded down to 100 ns | `TreatBigIntegersAsDates = false` reads `BigInteger` | ⚠ | [int96](types/int96.md) |
| INTERVAL | `Interval` | | ✓ | [interval](types/interval.md) |
| LIST, MAP, groups | each leaf column, with definition and repetition levels; `NestedLayout` gives Arrow's offsets and validity | | ⚠ | [nested](types/nested.md) |
| VARIANT | its `metadata` and `value` leaves as `byte[]`, shredded leaves as their types | | ⚠ | [nested](types/nested.md) |
| NULL (UNKNOWN) | `int`, every value null | | ✓ | |
| An INT64 that another library meant as a duration (pyarrow `duration`) | `long` | | ✓ | [time](types/time.md) |

## Known issues

Each broke decision 9 (a value silently changed) and is fixed as a change with a failing test, or waits on a decision.

| Issue | Page | Status |
|---|---|---|
| Digits beyond a decimal column's scale were truncated on write | [decimal](types/decimal.md) | Fixed, `lake(0072)`: refused unless `RoundDecimalsToScale` |
| A TIMESTAMP that is not UTC-adjusted (a wall-clock time) converts a `Local` value to UTC on write, and reads as `Local` | [datetime](types/datetime.md#wall-clock-timestamps) | Option, `lake(0073)`: `UnadjustedTimestampsAsWallClock` |
| A DECIMAL that does not fit `System.Decimal` was rounded on read | [decimal](types/decimal.md) | Fixed, `lake(0061)` |
| Columns in optional structs and lists failed through the typed API | [nested](types/nested.md) | Fixed, `lake(0062)` |
| A `DateTime` outside 1677–2262 written as TIMESTAMP(ns) wrapped to another date | [datetime](types/datetime.md) | Fixed, `lake(0064)` |
| A `decimal` wider than its column's precision was written, breaking the file's schema | [decimal](types/decimal.md) | Fixed, `lake(0065)` |
| Dictionary indexes of 2^24 and more were written corrupted (RLE's top byte) | | Fixed, `lake(0076)` |
| A chunk of nulls only wrote min = max = 0 in its statistics (integers, decimals, INT96) | | Fixed, `lake(0069)` |
