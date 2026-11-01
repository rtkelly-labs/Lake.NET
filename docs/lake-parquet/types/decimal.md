# decimal and BigDecimal

[Back to the type matrix](../types.md) · Status: write ⚠ (scale truncated), read ⚠ (exact or refused)

## Mapping

| .NET | Declare with | Physical type | Annotation |
|---|---|---|---|
| `decimal` | `DecimalDataField(name, precision, scale)` | INT32 when precision ≤ 9, INT64 when ≤ 18, otherwise FIXED_LEN_BYTE_ARRAY of the smallest width that holds the precision | DECIMAL(precision, scale), and the DECIMAL converted type |
| `decimal` | `DataField<decimal>` | FIXED_LEN_BYTE_ARRAY(16) | DECIMAL(38, 18) |
| `decimal` | `DecimalDataField(..., forceByteArrayEncoding: true)` | FIXED_LEN_BYTE_ARRAY at any precision | DECIMAL(p, s) |
| `Parquet.Data.BigDecimal` | `BigDecimalDataField(name, precision, scale)` | FIXED_LEN_BYTE_ARRAY | DECIMAL(p, s) |

The value stored is the unscaled integer (value × 10^scale), little-endian for INT32 and INT64, big-endian two's
complement in the byte arrays, as the format says.

## Why it is not a clean mapping

`System.Decimal` is a 96-bit integer with a scale of 0 to 28, so about 28 significant digits. A Parquet DECIMAL goes to
precision 38 (and further in byte arrays). The two overlap but neither contains the other:

- **Reading.** A DECIMAL whose value fits `decimal` exactly reads exactly. One that would lose digits (its integer is
  wider than 96 bits, or its scale is above 28 once trailing zeros are dropped) is refused with an
  `OverflowException` that names `ParquetOptions.UseBigDecimal` (`lake(0061)`). Upstream rounded it silently.
- **Reading wider values.** `ParquetOptions.UseBigDecimal = true` reads every DECIMAL as `BigDecimal`: the unscaled
  `BigInteger` with its precision and scale, exact at any width. `BigDecimal` is a container, not a number type; it
  does no arithmetic.
- **Writing.** The column's precision and scale are yours to choose; pick them to cover your values (see the known
  issues below for what happens if a value does not fit).

A FIXED_LEN_BYTE_ARRAY decimal comes back without trailing fractional zeros (`1.5`, not `1.500000000`); an INT32 or
INT64 decimal keeps the column's scale (`1.50`). The values are equal.

Statistics: an INT32 or INT64 decimal's bounds decode as `decimal`; a byte-array decimal's decode as `BigDecimal`.

## Precision and scale on write

- **A value wider than the column's precision is refused** (`ParquetException`), on INT32, INT64 and
  FIXED_LEN_BYTE_ARRAY alike (`lake(0065)`). Upstream wrote it, making a file that breaks its own schema:
  `DecimalDataField("x", 5, 2)` took `12345.6m`.
- **Digits beyond the scale are truncated**, as upstream does and tests: `DecimalDataField("x", 9, 2)` writes `1.239m`
  as 1.23. Decision 9 keeps upstream's default where changing it would change existing results, so this stays.

## Known issues

- **Truncation beyond the scale is silent.** Whether to refuse it by default, or offer a strict option, is open.

## Tests

`Types/DecimalPrecisionTest` (exact or refused on read, the `decimal` edges, 38 digits through `BigDecimal`),
`Types/EndToEndTypeTest` (upstream's decimal round trips), the conformance harness's decimal kinds on INT32, INT64 and
FIXED_LEN_BYTE_ARRAY against parquet-cpp, and the native AOT suite (`api`: decimal(38,9) from pyarrow, exact and
refused).
