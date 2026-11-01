# INT96 and BigInteger

[Back to the type matrix](../types.md) · Status: write ⚠, read ⚠

INT96 is a deprecated 12-byte physical type. In practice it holds one thing, an Impala/Spark timestamp: 8 bytes of
nanoseconds within the day, then 4 bytes of Julian day, little-endian.

## As DateTime (the default)

`DataField<DateTime>` writes INT96, upstream's default (see [datetime](datetime.md)). INT96 columns read as
`DateTime` with `Kind = Unspecified` (the instant is UTC), rounded down to 100 ns.

## As BigInteger

```csharp
var options = new ParquetOptions { TreatBigIntegersAsDates = false };
```

With the option, INT96 reads as `System.Numerics.BigInteger`: the 12 bytes as one little-endian two's-complement
integer. `DataField<BigInteger>` writes that, and must fit in 96 bits (`lake(0053)`). It is a way to get at the raw
bytes; no other library writes integers in INT96. For a wide integer, see [int128](int128.md).

Tests: `Types/Int96BigIntegerTest`, the native AOT suite (`api`: INT96 as BigInteger), and
`int96_timestamp_order.parquet` from parquet-testing.
