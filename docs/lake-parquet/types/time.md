# TimeOnly and TimeSpan

[Back to the type matrix](../types.md) · Status: write ⚠, read ⚠

Parquet's TIME is a time of day in milliseconds (INT32), microseconds or nanoseconds (INT64). .NET has two types that
can hold one, and upstream used neither: it reads TIME as the unit's integer. Both mappings are asked for
(`lake(0059)`).

## Writing

```csharp
var t = new TimeDataField("t", TimeUnitPrecision.Micros, typeof(TimeOnly));   // or typeof(TimeSpan)
```

| Unit | Physical type | Annotation | Precision |
|---|---|---|---|
| `Millis` | INT32 | TIME(MILLIS), TIME_MILLIS | 1 ms; finer ticks are dropped |
| `Micros` | INT64 | TIME(MICROS) | 1 µs |
| `Nanos` | INT64 | TIME(NANOS) | 100 ns, written as ticks × 100 |

A `TimeSpan` must be a time of day, 0 up to but not including 24 hours; anything else is refused with
`ArgumentOutOfRangeException`. TIME cannot hold a duration.

`DataField<TimeOnly>` is upstream's, unchanged: its type is `long`, and it takes `long` values. `DataField<TimeSpan>`
is refused. Use `TimeDataField` with the type.

## Reading

| `ParquetOptions.TimeMapping` | TIME(ms), TIME_MILLIS | TIME(µs), TIME_MICROS | TIME(ns) |
|---|---|---|---|
| `Integer` (default, upstream's) | `int` milliseconds | `long` microseconds | `long` nanoseconds |
| `TimeOnly` | `TimeOnly` | `TimeOnly` | `TimeOnly`, rounded down to 100 ns |
| `TimeSpan` | `TimeSpan` | `TimeSpan` | `TimeSpan`, rounded down to 100 ns |

Statistics decode the same way. Reading nanoseconds as `TimeOnly`/`TimeSpan` is the one lossy step, and it happens
only when asked for.

## Durations

A duration has no Parquet logical type. pyarrow writes `duration` as an unannotated INT64 (plus Arrow schema
metadata); DuckDB writes INTERVAL. Store a duration as `long` ticks or as [`Interval`](interval.md); neither reads back
as `TimeSpan` automatically.

## Tests

`Types/NativeTypesTest` (each unit both ways, the 24-hour limit, legacy TIME_MILLIS, pyarrow's time32/time64 file),
the conformance harness's TimeOnly and TimeSpan kinds against parquet-cpp in both directions (`lake(0060)`), and the
native AOT suite.
