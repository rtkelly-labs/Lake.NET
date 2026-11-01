# DateTime

[Back to the type matrix](../types.md) · Status: write ⚠, read ⚠

`DateTime` has four Parquet representations. Which one is written is chosen by the field, and the default is
upstream's, which is the legacy one.

## Mapping

| Declare with | Physical type | Annotation | Precision kept | Notes |
|---|---|---|---|---|
| `DataField<DateTime>` | INT96 | none | 100 ns (a tick) | The default. INT96 is deprecated in the format; Spark and Impala still read it. See [int96](int96.md) |
| `DateTimeDataField(name, DateTimeFormat.Timestamp, unit: Millis/Micros/Nanos)` | INT64 | TIMESTAMP(unit, isAdjustedToUTC) | the unit; nanoseconds hold ticks × 100 | What other writers write today. Recommended |
| `DateTimeDataField(name, DateTimeFormat.DateAndTime)` | INT64 | TIMESTAMP_MILLIS (converted type) | 1 ms | Legacy |
| `DateTimeDataField(name, DateTimeFormat.DateAndTimeMicros)` | INT64 | TIMESTAMP_MICROS (converted type) | 1 µs | Legacy |
| `DateTimeDataField(name, DateTimeFormat.Date)` | INT32 | DATE (converted type) | 1 day | Prefer `DateOnly`, see [dateonly](dateonly.md) |
| `DateTimeDataField(name, DateTimeFormat.Impala)` | INT96 | none | 100 ns | Same as the default |

## Time zones and `Kind`

- **On write**, a `DateTime` with `Kind = Local` is converted to UTC first; `Utc` and `Unspecified` are written as
  they are, `Unspecified` taken to be UTC. This holds for INT96, TIMESTAMP and the legacy timestamps.
- **On read**, a UTC-adjusted TIMESTAMP and the legacy timestamps read with `Kind = Utc`; a DATE reads as midnight,
  `Kind = Utc`; INT96 reads with `Kind = Unspecified` (the instant is UTC).

## Precision

Each representation keeps what its unit holds, so the field's unit is the choice of precision:

- writing a `DateTime` with sub-millisecond ticks to a millisecond column drops them; that is the unit asked for;
- reading TIMESTAMP(ns) or INT96 rounds down to 100 ns, the smallest `DateTime` step. The integer nanoseconds stay
  available through the physical type (`DataField.PhysicalClrType`, `lake(0017)`: read the column as `long`).

## Range

| Representation | Range |
|---|---|
| TIMESTAMP(ms), TIMESTAMP(µs), INT96, DATE | all of `DateTime`, 0001-01-01 to 9999-12-31 |
| TIMESTAMP(ns) | 1677-09-21 00:12:43 to 2262-04-11 23:47:16 UTC (a signed 64-bit count of nanoseconds since 1970); a value outside is refused with `ArgumentOutOfRangeException` (`lake(0064)`; upstream wrapped it, so 1600-01-01 read back as 2184-07-20) |

## Wall-clock timestamps

- **Timestamps that are not UTC-adjusted** (`isAdjustedToUTC: false`) are wall-clock times with no zone. By
  default, as upstream, a `Local` value is converted to UTC on write (12:00 in New York is stored as 17:00) and values
  read back as `Local`. `ParquetOptions.UnadjustedTimestampsAsWallClock = true` (`lake(0073)`) gives wall-clock
  semantics: every value is stored as it reads, whatever its `Kind`, and reads back `Unspecified`. Statistics, page
  indexes and bloom filters hold the same wall-clock value.

## Tests

`Types/EndToEndTypeTest`, `Types/DateBeforeEpochTest`, `Types/TimestampNanosRangeTest`, `Types/WallClockTimestampTest`, the conformance harness's timestamp kinds
(millis, micros, nanos, UTC-adjusted or not, legacy TIMESTAMP_MILLIS) against parquet-cpp in both directions, and the
native AOT suite (`api`: every `DateTimeFormat`).
