# DateTimeOffset

[Back to the type matrix](../types.md) · Status: ✗

`new DataField<DateTimeOffset>(...)` throws `NotSupportedException`: upstream removed it "due to numerous ambiguity
issues".

## Why

Parquet's TIMESTAMP stores an instant (UTC-adjusted) or a wall-clock time (not adjusted). It has nowhere to store an
offset. A `DateTimeOffset` written as a UTC-adjusted timestamp keeps the instant but loses its offset: `12:00+02:00`
reads back as `10:00+00:00`. That is a lossy step, so under decision 9 it cannot be the default. DuckDB and Polars
write it that way without saying so.

## What to use

- For the instant: write `value.UtcDateTime` to a `DateTimeDataField` with `DateTimeFormat.Timestamp`,
  `isAdjustedToUTC: true`.
- To keep the offset too: a second column with `value.Offset.TotalMinutes` as `short`.

## Proposal

An opt-in `DateTimeOffset` mapping to TIMESTAMP(UTC-adjusted), reading back with offset zero, declared on the field
like `TimeDataField`. Not decided.
