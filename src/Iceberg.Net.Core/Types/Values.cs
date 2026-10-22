using System.Globalization;
using System.Numerics;

namespace Iceberg.Net.Types;

/// <summary>
/// An Iceberg decimal. System.Decimal tops out at 28-29 significant digits, but Iceberg allows precision 38,
/// so values are carried as an unscaled BigInteger plus scale.
/// </summary>
public readonly record struct IcebergDecimal(BigInteger Unscaled, int Scale)
{
    public static IcebergDecimal FromDecimal(decimal value, int scale)
    {
        var scaled = decimal.Round(value * Pow10Decimal(scale), 0, MidpointRounding.ToEven);
        if (scaled != decimal.Truncate(scaled)) throw new ArgumentException($"{value} does not fit scale {scale}");
        return new IcebergDecimal(new BigInteger(scaled), scale);
    }

    public static IcebergDecimal Parse(string text)
    {
        var dot = text.IndexOf('.');
        var scale = dot < 0 ? 0 : text.Length - dot - 1;
        return new IcebergDecimal(BigInteger.Parse(text.Replace(".", ""), CultureInfo.InvariantCulture), scale);
    }

    public decimal ToDecimal() => (decimal)Unscaled / Pow10Decimal(Scale);

    public override string ToString()
    {
        if (Scale == 0) return Unscaled.ToString(CultureInfo.InvariantCulture);
        var neg = Unscaled.Sign < 0;
        var digits = BigInteger.Abs(Unscaled).ToString(CultureInfo.InvariantCulture).PadLeft(Scale + 1, '0');
        return (neg ? "-" : "") + digits[..^Scale] + "." + digits[^Scale..];
    }

    private static decimal Pow10Decimal(int n)
    {
        decimal r = 1;
        for (var i = 0; i < n; i++) r *= 10;
        return r;
    }
}

/// <summary>
/// Conversions between CLR values and Iceberg's physical representations: date = days from epoch (int),
/// time/timestamp = microseconds (long), timestamp_ns = nanoseconds (long).
/// </summary>
public static class IcebergTime
{
    public static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    public static readonly DateOnly EpochDate = new(1970, 1, 1);

    public static int DaysFromDate(DateOnly date) => date.DayNumber - EpochDate.DayNumber;
    public static DateOnly DateFromDays(int days) => EpochDate.AddDays(days);

    public static long MicrosFromTime(TimeOnly time) => time.Ticks / 10;
    public static TimeOnly TimeFromMicros(long micros) => new(micros * 10);

    public static long MicrosFromTimestamp(DateTime ts) => (ToUtc(ts).Ticks - Epoch.Ticks) / 10;
    public static long MicrosFromTimestamp(DateTimeOffset ts) => (ts.UtcTicks - Epoch.Ticks) / 10;
    public static DateTime TimestampFromMicros(long micros) => Epoch.AddTicks(micros * 10);

    /// <summary>.NET ticks are 100ns, so the last two nanosecond digits must be supplied separately.</summary>
    public static long NanosFromTimestamp(DateTime ts, int extraNanos = 0) => (ToUtc(ts).Ticks - Epoch.Ticks) * 100 + extraNanos;

    public static long FloorDiv(long a, long b)
    {
        var q = a / b;
        if ((a % b != 0) && ((a < 0) ^ (b < 0))) q--;
        return q;
    }

    private static DateTime ToUtc(DateTime ts) => ts.Kind switch
    {
        DateTimeKind.Local => ts.ToUniversalTime(),
        DateTimeKind.Unspecified => DateTime.SpecifyKind(ts, DateTimeKind.Utc),
        _ => ts,
    };
}
