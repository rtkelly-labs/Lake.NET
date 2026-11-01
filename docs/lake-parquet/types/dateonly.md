# DateOnly

[Back to the type matrix](../types.md) · Status: write ✓, read ⚠

## Mapping

| Declare with | Physical type | Annotation |
|---|---|---|
| `DataField<DateOnly>` | INT32, days since 1970-01-01 | DATE, and the DATE converted type |

Every `DateOnly`, 0001-01-01 to 9999-12-31, fits.

## The exception: what DATE reads as

Upstream reads every DATE column as `DateTime` (midnight, `Kind = Utc`), including one the library wrote from
`DateOnly`. To read `DateOnly`, ask for it:

```csharp
var options = new ParquetOptions { UseDateOnlyTypeForDates = true };
```

Without the option, reading a DATE column into a `DateOnly` buffer throws, because the field's type is `DateTime`.
The default stays upstream's (decision 9). Making `DateOnly` the default would change what existing callers read.

## Tests

The conformance harness's `DateOnly` kind against parquet-cpp in both directions, and the native AOT suite.
