using System.Globalization;
using System.Text;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// A <c>timetz</c> value: a time of day in microseconds (0 to 24:00:00) at a UTC offset in seconds (±15:59:59), as DuckDB
/// holds it. Parquet stores only the UTC instant (<see cref="UtcMicros"/>), so a value read from a data file has offset 0;
/// inlined rows keep the offset.
/// </summary>
public readonly record struct DuckLakeTimeTz
{
    public const long MicrosPerDay = 86_400_000_000L;
    public const int MaxOffsetSeconds = 15 * 3600 + 59 * 60 + 59;

    public DuckLakeTimeTz(long micros, int offsetSeconds)
    {
        if (micros is < 0 or > MicrosPerDay) throw new ArgumentOutOfRangeException(nameof(micros), micros, "a time of day is 0 to 24:00:00");
        if (Math.Abs(offsetSeconds) > MaxOffsetSeconds) throw new ArgumentOutOfRangeException(nameof(offsetSeconds), offsetSeconds, "offsets are within ±15:59:59");
        Micros = micros;
        OffsetSeconds = offsetSeconds;
    }

    public long Micros { get; }
    public int OffsetSeconds { get; }

    /// <summary>The same instant at offset 0, wrapped into one day, as DuckDB writes it to Parquet.</summary>
    public long UtcMicros => ((Micros - OffsetSeconds * 1_000_000L) % MicrosPerDay + MicrosPerDay) % MicrosPerDay;

    public static DuckLakeTimeTz FromUtcMicros(long micros) => new(micros, 0);

    /// <summary>DuckDB's text: <c>HH:MM:SS[.ffffff]±HH[:MM[:SS]]</c>.</summary>
    public override string ToString()
    {
        var sb = new StringBuilder(DuckLakeTemporal.FormatClock(Micros));
        var offset = Math.Abs(OffsetSeconds);
        sb.Append(OffsetSeconds < 0 ? '-' : '+').Append((offset / 3600).ToString("00", CultureInfo.InvariantCulture));
        if (offset % 3600 != 0) sb.Append(':').Append((offset / 60 % 60).ToString("00", CultureInfo.InvariantCulture));
        if (offset % 60 != 0) sb.Append(':').Append((offset % 60).ToString("00", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    /// <summary>Parse DuckDB's or PostgreSQL's text (an offset is required; <c>Z</c> is offset 0).</summary>
    public static DuckLakeTimeTz Parse(string text)
    {
        text = text.Trim();
        var sign = text.IndexOfAny(['+', '-', 'Z', 'z'], 1);
        if (sign < 0) throw new FormatException($"'{text}' has no UTC offset");
        var micros = DuckLakeTemporal.ParseClock(text[..sign]) ?? throw new FormatException($"'{text}' is not a time with time zone");
        var offset = 0;
        if (text[sign] is not ('Z' or 'z'))
        {
            var parts = text[(sign + 1)..].Split(':');
            if (parts.Length > 3 || parts.Any(p => p.Length is 0 or > 2 || !p.All(char.IsAsciiDigit))) throw new FormatException($"'{text}' has a malformed offset");
            offset = int.Parse(parts[0], CultureInfo.InvariantCulture) * 3600
                     + (parts.Length > 1 ? int.Parse(parts[1], CultureInfo.InvariantCulture) * 60 : 0)
                     + (parts.Length > 2 ? int.Parse(parts[2], CultureInfo.InvariantCulture) : 0);
            if (text[sign] == '-') offset = -offset;
        }
        return new DuckLakeTimeTz(micros, offset);
    }
}

/// <summary>
/// An <c>interval</c>: months, days and microseconds, each signed and kept apart (one month is not 30 days), as DuckDB
/// holds it. Parquet's INTERVAL stores unsigned months, days and milliseconds, so writing a data file drops
/// sub-millisecond digits and rejects negative parts, as DuckDB does; inlined rows keep the exact value.
/// </summary>
public readonly record struct DuckLakeInterval(int Months, int Days, long Micros)
{
    public static DuckLakeInterval FromTimeSpan(TimeSpan span) => new(0, 0, span.Ticks / 10);

    /// <summary>DuckDB's text: <c>1 year 2 months 3 days 04:05:06.000007</c>, <c>00:00:00</c> when zero.</summary>
    public override string ToString()
    {
        var parts = new List<string>();
        static string Unit(long n, string unit) => $"{n.ToString(CultureInfo.InvariantCulture)} {unit}{(Math.Abs(n) == 1 ? "" : "s")}";
        var years = Months / 12;
        var months = Months % 12;
        if (years != 0) parts.Add(Unit(years, "year"));
        if (months != 0) parts.Add(Unit(months, "month"));
        if (Days != 0) parts.Add(Unit(Days, "day"));
        if (Micros != 0 || parts.Count == 0)
        {
            var magnitude = Micros == long.MinValue ? (ulong)long.MaxValue + 1 : (ulong)Math.Abs(Micros);
            var hours = magnitude / 3_600_000_000UL;
            var rest = (long)(magnitude % 3_600_000_000UL);
            var clock = (Micros < 0 ? "-" : "") + hours.ToString("00", CultureInfo.InvariantCulture) + DuckLakeTemporal.FormatClock(rest)[2..];
            parts.Add(clock);
        }
        return string.Join(' ', parts);
    }

    /// <summary>
    /// Parse DuckDB's interval text, PostgreSQL's (<c>1 mon 2 days 00:00:03</c>) and the unit forms DuckDB accepts
    /// (<c>3 hours 4 minutes 5.5 seconds</c>, <c>2 weeks</c>, <c>1 millisecond</c>).
    /// </summary>
    public static DuckLakeInterval Parse(string text)
    {
        var tokens = text.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        long months = 0, days = 0;
        decimal micros = 0;
        for (var i = 0; i < tokens.Length; i++)
        {
            var token = tokens[i];
            if (token.Contains(':'))
            {
                var negative = token.StartsWith('-');
                var clock = token.TrimStart('-', '+').Split(':');
                if (clock.Length is < 2 or > 3 || !long.TryParse(clock[0], NumberStyles.None, CultureInfo.InvariantCulture, out var h)
                    || !long.TryParse(clock[1], NumberStyles.None, CultureInfo.InvariantCulture, out var m)
                    || clock.Length == 3 && !decimal.TryParse(clock[2], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out _))
                    throw new FormatException($"'{text}' is not an interval");
                var s = clock.Length == 3 ? decimal.Parse(clock[2], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture) : 0;
                var value = (h * 3600m + m * 60m + s) * 1_000_000m;
                micros += negative ? -value : value;
                continue;
            }
            if (!decimal.TryParse(token, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var n) || i + 1 >= tokens.Length)
                throw new FormatException($"'{text}' is not an interval");
            var unit = tokens[++i].ToLowerInvariant().TrimEnd(',');
            switch (unit)
            {
                case "year" or "years" or "y" or "yr" or "yrs": months += Whole(n, text) * 12; break;
                case "month" or "months" or "mon" or "mons": months += Whole(n, text); break;
                case "week" or "weeks" or "w": days += Whole(n, text) * 7; break;
                case "day" or "days" or "d": days += Whole(n, text); break;
                case "hour" or "hours" or "h" or "hr" or "hrs": micros += n * 3_600_000_000m; break;
                case "minute" or "minutes" or "min" or "mins" or "m": micros += n * 60_000_000m; break;
                case "second" or "seconds" or "sec" or "secs" or "s": micros += n * 1_000_000m; break;
                case "millisecond" or "milliseconds" or "ms" or "msec" or "msecs": micros += n * 1000m; break;
                case "microsecond" or "microseconds" or "us" or "usec" or "usecs": micros += n; break;
                default: throw new FormatException($"'{text}': unknown interval unit '{unit}'");
            }
        }
        if (micros != decimal.Truncate(micros)) throw new FormatException($"'{text}' has digits below a microsecond");
        return new DuckLakeInterval(checked((int)months), checked((int)days), checked((long)micros));
    }

    private static long Whole(decimal n, string text) =>
        n == decimal.Truncate(n) ? (long)n : throw new FormatException($"'{text}': fractional months, weeks or days are not supported");

    /// <summary>Parquet's INTERVAL bytes (little-endian unsigned months, days, milliseconds; sub-millisecond digits dropped).</summary>
    public byte[] ToParquet()
    {
        if (Months < 0 || Days < 0 || Micros < 0) throw new ArgumentException($"Parquet files do not support negative intervals ({this})");
        var millis = Micros / 1000;
        if (millis > uint.MaxValue) throw new ArgumentException($"Interval {this} has more milliseconds than Parquet's INTERVAL holds");
        var bytes = new byte[12];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(bytes, (uint)Months);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(4), (uint)Days);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), (uint)millis);
        return bytes;
    }

    public static DuckLakeInterval FromParquet(byte[] bytes)
    {
        if (bytes.Length != 12) throw new InvalidDataException($"An INTERVAL value has {bytes.Length} bytes, not 12");
        return new DuckLakeInterval(
            checked((int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes)),
            checked((int)System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(4))),
            System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8)) * 1000L);
    }
}

internal static class DuckLakeTemporal
{
    /// <summary><c>HH:MM:SS</c> plus the fraction without trailing zeros, as DuckDB prints times.</summary>
    internal static string FormatClock(long micros)
    {
        var seconds = micros / 1_000_000;
        var frac = micros % 1_000_000;
        var s = $"{(seconds / 3600).ToString("00", CultureInfo.InvariantCulture)}:{(seconds / 60 % 60).ToString("00", CultureInfo.InvariantCulture)}:{(seconds % 60).ToString("00", CultureInfo.InvariantCulture)}";
        return frac == 0 ? s : s + "." + frac.ToString("000000", CultureInfo.InvariantCulture).TrimEnd('0');
    }

    /// <summary><c>HH:MM[:SS[.f…]]</c> (up to 24:00:00) as microseconds; null when malformed.</summary>
    internal static long? ParseClock(string text)
    {
        var parts = text.Split(':');
        if (parts.Length is < 2 or > 3 || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var h)
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var m) || h > 24 || m > 59) return null;
        decimal s = 0;
        if (parts.Length == 3 && (!decimal.TryParse(parts[2], NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out s) || s >= 60)) return null;
        var micros = (h * 3600L + m * 60L) * 1_000_000L + s * 1_000_000m;
        if (micros != decimal.Truncate(micros) || micros > DuckLakeTimeTz.MicrosPerDay) return null;
        return (long)micros;
    }
}
