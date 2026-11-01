# Interval

[Back to the type matrix](../types.md) · Status: write ✓, read ✓

| Declare with | Physical type | Annotation |
|---|---|---|
| `DataField<Interval>` (`Parquet.File.Values.Primitives.Interval`) | FIXED_LEN_BYTE_ARRAY(12) | INTERVAL (converted type; the format has no INTERVAL logical type) |

`Interval` is the library's own struct, not a .NET type: three unsigned little-endian 32-bit counts of months, days
and milliseconds, kept apart because a month and a day have no fixed length. It round-trips exactly. DuckDB writes
`INTERVAL` columns this way, and they read as `Interval`.

There is no conversion to `TimeSpan`, which cannot represent months. For a duration in a single unit, store `long`
(see [time](time.md#durations)).
