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
| TIMESTAMP(ns) | 1677-09-21 00:12:43 to 2262-04-11 23:47:16 UTC (a signed 64-bit count of nanoseconds since 1970) |

## Known issues

- **TIMESTAMP(ns) out of range overflows silently.** A `DateTime` outside 1677–2262 is written as a wrapped 64-bit
  integer: 1600-01-01 reads back as 2184-07-20. To be fixed: refuse the value, naming the column and the range.
- **Timestamps that are not UTC-adjusted.** `isAdjustedToUTC: false` means a wall-clock time with no zone. Today a
  `Local` value is still converted to UTC on write (12:00 in New York is stored as 17:00), and every value reads back
  with `Kind = Local`. For wall-clock semantics the value should be stored as it is and read back as `Unspecified`.
  That changes what existing code reads, so under decision 9 it needs a decision first: fix it by default, or behind an
  option.

## Tests

`Types/EndToEndTypeTest`, `Types/DateBeforeEpochTest`, the conformance harness's timestamp kinds
(millis, micros, nanos, UTC-adjusted or not, legacy TIMESTAMP_MILLIS) against parquet-cpp in both directions, and the
native AOT suite (`api`: every `DateTimeFormat`).
