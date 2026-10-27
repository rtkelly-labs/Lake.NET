using System.Collections;
using System.Globalization;
using System.Numerics;
using System.Text;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Values of DuckLake columns. In memory they are Iceberg's physical values (see <see cref="Data.ParquetFiles"/>):
/// <c>int</c> for int8/16/32, <c>long</c>, <c>float</c>, <c>double</c>, <c>bool</c>, <c>string</c> for varchar/json,
/// <c>byte[]</c>, <see cref="Guid"/>, <see cref="IcebergDecimal"/>, <c>int</c> days for date, <c>long</c> microseconds for
/// time/timestamp/timestamptz and nanoseconds for timestamp_ns; <c>object?[]</c> structs, <c>List&lt;object?&gt;</c>
/// lists and <c>Dictionary&lt;object, object?&gt;</c> maps. As text (statistics, defaults and inlined rows) they use the
/// spec's "Type Encoding for Statistics".
/// </summary>
public static class DuckLakeValues
{
    /// <summary>
    /// Convert a CLR value to the physical value of <paramref name="type"/>: besides the physical forms, accepts any
    /// integral type (range-checked against int8/int16/int32), <see cref="decimal"/>, <see cref="DateOnly"/>,
    /// <see cref="DateTime"/>, <see cref="DateTimeOffset"/>, <see cref="TimeOnly"/>, <see cref="TimeSpan"/> and
    /// <see cref="string"/> for uuid.
    /// </summary>
    public static object? Normalize(DuckLakeType type, object? value, string column = "?")
    {
        if (value is null) return null;
        try
        {
            return type switch
            {
                DuckLakePrimitive p => NormalizePrimitive(p, value),
                DuckLakeList l => value is IEnumerable e and not string
                    ? e.Cast<object?>().Select(x => Normalize(l.Element, x, column)).ToList()
                    : throw new ArgumentException("expected a list"),
                DuckLakeMap m => value is IDictionary d
                    ? d.Keys.Cast<object>().ToDictionary(k => Normalize(m.Key, k, column) ?? throw new ArgumentException("map keys cannot be null"), k => Normalize(m.Value, d[k], column))
                    : throw new ArgumentException("expected a dictionary"),
                DuckLakeStruct s => value is IList fields && fields.Count == s.Fields.Count
                    ? s.Fields.Select((f, i) => Normalize(f.Type, fields[i], $"{column}.{f.Name}")).ToArray()
                    : throw new ArgumentException($"expected object?[{s.Fields.Count}]"),
                _ => throw new NotSupportedException(type.ToString()),
            };
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException or FormatException)
        {
            throw new ArgumentException($"Column {column}: {value} ({value.GetType().Name}) is not a {type}: {e.Message}", e);
        }
    }

    private static object NormalizePrimitive(DuckLakePrimitive p, object v) => p.Name switch
    {
        "boolean" => (bool)v,
        "int8" => (int)checked((sbyte)Convert.ToInt64(v, CultureInfo.InvariantCulture)),
        "int16" => (int)checked((short)Convert.ToInt64(v, CultureInfo.InvariantCulture)),
        "int32" => checked((int)Convert.ToInt64(v, CultureInfo.InvariantCulture)),
        "int64" => Convert.ToInt64(v, CultureInfo.InvariantCulture),
        "float32" => Convert.ToSingle(v, CultureInfo.InvariantCulture),
        "float64" => Convert.ToDouble(v, CultureInfo.InvariantCulture),
        "date" => v switch
        {
            int days => days,
            DateOnly d => IcebergTime.DaysFromDate(d),
            DateTime dt => IcebergTime.DaysFromDate(dt),
            _ => throw new InvalidCastException(),
        },
        "time" => v switch
        {
            long micros => micros,
            TimeOnly t => IcebergTime.MicrosFromTime(t),
            TimeSpan ts => ts.Ticks / 10,
            _ => throw new InvalidCastException(),
        },
        "timestamp" or "timestamptz" => v switch
        {
            long micros => micros,
            DateTime dt => IcebergTime.MicrosFromTimestamp(dt),
            DateTimeOffset dto => IcebergTime.MicrosFromTimestamp(dto),
            _ => throw new InvalidCastException(),
        },
        "timestamp_ns" => v switch
        {
            long nanos => nanos,
            DateTime dt => IcebergTime.NanosFromTimestamp(dt),
            DateTimeOffset dto => (dto.UtcTicks - IcebergTime.Epoch.Ticks) * 100,
            _ => throw new InvalidCastException(),
        },
        "varchar" or "json" => (string)v,
        "blob" => (byte[])v,
        "uuid" => v is string s ? Guid.Parse(s) : (Guid)v,
        _ when DuckLakeTypes.ToIceberg(p) is DecimalType d => v switch
        {
            IcebergDecimal x when x.Scale == d.Scale => x,
            IcebergDecimal x => Rescale(x, d.Scale) ?? throw new FormatException($"{x} does not fit scale {d.Scale}"),
            _ => IcebergDecimal.FromDecimal(Convert.ToDecimal(v, CultureInfo.InvariantCulture), d.Scale),
        },
        _ => throw new NotSupportedException(p.Name),
    };

    /// <summary>A value as statistics text (spec "Type Encoding for Statistics"); null for types without min/max.</summary>
    public static string? Format(DuckLakePrimitive type, object value) => type.Name switch
    {
        "boolean" => (bool)value ? "1" : "0",
        "int8" or "int16" or "int32" => ((int)value).ToString(CultureInfo.InvariantCulture),
        "int64" => ((long)value).ToString(CultureInfo.InvariantCulture),
        "float32" => FormatFloat((float)value),
        "float64" => FormatDouble((double)value),
        "date" => FormatDate((int)value),
        "time" => FormatTime((long)value),
        "timestamp" => FormatTimestamp((long)value, 6),
        "timestamptz" => FormatTimestamp((long)value, 6) is { } t ? t + "+00" : null,
        "timestamp_ns" => FormatTimestamp((long)value, 9),
        "varchar" or "json" => (string)value,
        "uuid" => ((Guid)value).ToString("D"),
        // blob: DuckDB itself writes blob bounds inconsistently (escaped text in some places, hex in others); none is a
        // valid choice under "bounds may be absent".
        "blob" => null,
        _ when value is IcebergDecimal d => d.ToString(),
        _ => null,
    };

    private static string? FormatFloat(float f) => float.IsNaN(f) ? null
        : float.IsPositiveInfinity(f) ? "inf" : float.IsNegativeInfinity(f) ? "-inf" : f.ToString("R", CultureInfo.InvariantCulture);

    private static string? FormatDouble(double d) => double.IsNaN(d) ? null
        : double.IsPositiveInfinity(d) ? "inf" : double.IsNegativeInfinity(d) ? "-inf" : d.ToString("R", CultureInfo.InvariantCulture);

    private static string? FormatDate(int days)
    {
        var d = IcebergTime.EpochDate.DayNumber + (long)days;
        return d < DateOnly.MinValue.DayNumber || d > DateOnly.MaxValue.DayNumber ? null
            : DateOnly.FromDayNumber((int)d).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    private static string FormatTime(long micros)
    {
        var t = new TimeSpan(micros * 10);
        var s = $"{t.Hours:00}:{t.Minutes:00}:{t.Seconds:00}";
        var frac = micros % 1_000_000;
        return frac == 0 ? s : $"{s}.{frac:000000}";
    }

    private static string? FormatTimestamp(long value, int digits)
    {
        var perSecond = digits == 6 ? 1_000_000L : 1_000_000_000L;
        var seconds = IcebergTime.FloorDiv(value, perSecond);
        var frac = value - seconds * perSecond;
        if (seconds < -62135596800L || seconds > 253402300799L) return null; // outside years 1–9999
        var s = IcebergTime.Epoch.AddSeconds(seconds).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        return frac == 0 ? s : s + "." + frac.ToString(new string('0', digits), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Parse statistics, default or inlined-row text as a value of <paramref name="type"/>. False for text this reader
    /// does not understand (special values such as <c>infinity</c> on dates, other writers' encodings): a bound that
    /// cannot be parsed is treated as absent, which never prunes.
    /// </summary>
    public static bool TryParse(DuckLakePrimitive type, string text, out object? value)
    {
        value = null;
        try
        {
            value = Parse(type, text.Trim());
            return value is not null;
        }
        catch (Exception e) when (e is FormatException or OverflowException or ArgumentException)
        {
            return false;
        }
    }

    private static object? Parse(DuckLakePrimitive type, string text)
    {
        var inv = CultureInfo.InvariantCulture;
        switch (type.Name)
        {
            case "boolean":
                return text.ToLowerInvariant() switch
                {
                    "1" or "true" or "t" => true,
                    "0" or "false" or "f" => false,
                    _ => null,
                };
            case "int8": return (int)sbyte.Parse(text, NumberStyles.Integer, inv);
            case "int16": return (int)short.Parse(text, NumberStyles.Integer, inv);
            case "int32": return int.Parse(text, NumberStyles.Integer, inv);
            case "int64": return long.Parse(text, NumberStyles.Integer, inv);
            case "float32": return (float?)ParseDouble(text);
            case "float64": return ParseDouble(text);
            case "date": return IcebergTime.DaysFromDate(DateOnly.ParseExact(text, "yyyy-MM-dd", inv));
            case "time": return ParseTime(text, out var micros) ? micros : null;
            case "timestamp": return ParseTimestamp(text, 6, allowOffset: false);
            case "timestamptz": return ParseTimestamp(text, 6, allowOffset: true);
            case "timestamp_ns": return ParseTimestamp(text, 9, allowOffset: false);
            case "varchar" or "json": return text;
            case "uuid": return Guid.Parse(text);
            case "blob": return ParseBlob(text);
        }
        if (DuckLakeTypes.ToIceberg(type) is DecimalType d) return Rescale(IcebergDecimal.Parse(text), d.Scale);
        return null;
    }

    private static double? ParseDouble(string text) => text.ToLowerInvariant() switch
    {
        "inf" or "+inf" or "infinity" or "+infinity" => double.PositiveInfinity,
        "-inf" or "-infinity" => double.NegativeInfinity,
        "nan" or "-nan" => double.NaN,
        _ => double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture),
    };

    /// <summary>Exact rescale (trailing zeros only); null when it would lose digits.</summary>
    private static IcebergDecimal? Rescale(IcebergDecimal d, int scale)
    {
        if (d.Scale == scale) return d;
        if (d.Scale < scale) return new IcebergDecimal(d.Unscaled * BigInteger.Pow(10, scale - d.Scale), scale);
        var divisor = BigInteger.Pow(10, d.Scale - scale);
        return BigInteger.Remainder(d.Unscaled, divisor).IsZero ? new IcebergDecimal(d.Unscaled / divisor, scale) : null;
    }

    private static bool ParseTime(string text, out long micros)
    {
        micros = 0;
        var parts = text.Split(':');
        if (parts.Length != 3 || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var h)
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var m) || h > 24 || m > 59) return false;
        if (!ParseSeconds(parts[2], 6, out var sec, out var frac)) return false;
        micros = ((h * 60L + m) * 60 + sec) * 1_000_000 + frac;
        return micros <= 86_400_000_000L;
    }

    private static bool ParseSeconds(string text, int digits, out long seconds, out long fraction)
    {
        seconds = fraction = 0;
        var dot = text.IndexOf('.');
        var whole = dot < 0 ? text : text[..dot];
        if (!long.TryParse(whole, NumberStyles.None, CultureInfo.InvariantCulture, out seconds) || seconds > 60) return false;
        if (dot < 0) return true;
        var f = text[(dot + 1)..];
        if (f.Length == 0 || f.Length > 9 || !f.All(char.IsAsciiDigit)) return false;
        // Digits beyond the unit are accepted only when zero: dropping others would move an upper bound below the max.
        if (f.Length > digits)
        {
            if (f[digits..].Any(c => c != '0')) return false;
            f = f[..digits];
        }
        fraction = long.Parse(f.PadRight(digits, '0'), CultureInfo.InvariantCulture);
        return true;
    }

    private static long? ParseTimestamp(string text, int digits, bool allowOffset)
    {
        var sep = text.IndexOfAny([' ', 'T']);
        if (sep < 0) return null;
        var date = DateOnly.ParseExact(text[..sep], "yyyy-MM-dd", CultureInfo.InvariantCulture);
        var rest = text[(sep + 1)..];
        long offsetSeconds = 0;
        var z = rest.IndexOfAny(['+', '-', 'Z']);
        if (z >= 0)
        {
            if (!allowOffset) return null;
            offsetSeconds = rest[z] == 'Z' ? 0 : ParseOffset(rest[z..]) ?? throw new FormatException(rest);
            rest = rest[..z];
        }
        var parts = rest.Split(':');
        if (parts.Length != 3 || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var h)
            || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var m) || h > 23 || m > 59) return null;
        if (!ParseSeconds(parts[2], digits, out var sec, out var frac)) return null;
        var perSecond = digits == 6 ? 1_000_000L : 1_000_000_000L;
        var epochSeconds = (long)IcebergTime.DaysFromDate(date) * 86_400 + h * 3600L + m * 60L + sec - offsetSeconds;
        return checked(epochSeconds * perSecond + frac);
    }

    private static long? ParseOffset(string text)
    {
        var sign = text[0] == '-' ? -1 : 1;
        var body = text[1..].Replace(":", "");
        if (body.Length is not (2 or 4 or 6) || !body.All(char.IsAsciiDigit)) return null;
        var h = int.Parse(body[..2], CultureInfo.InvariantCulture);
        var m = body.Length >= 4 ? int.Parse(body[2..4], CultureInfo.InvariantCulture) : 0;
        var s = body.Length == 6 ? int.Parse(body[4..6], CultureInfo.InvariantCulture) : 0;
        return sign * (h * 3600L + m * 60L + s);
    }

    /// <summary>Hex (the spec's encoding) or DuckDB's escaped form (<c>\x00\xFF</c>, other bytes literal).</summary>
    private static byte[]? ParseBlob(string text)
    {
        if (text.Length % 2 == 0 && text.All(char.IsAsciiHexDigit)) return Convert.FromHexString(text);
        var bytes = new List<byte>();
        for (var i = 0; i < text.Length; i++)
        {
            if (text[i] == '\\' && i + 3 < text.Length && text[i + 1] == 'x'
                && char.IsAsciiHexDigit(text[i + 2]) && char.IsAsciiHexDigit(text[i + 3]))
            {
                bytes.Add(Convert.ToByte(text.Substring(i + 2, 2), 16));
                i += 3;
            }
            else if (text[i] < 0x80) bytes.Add((byte)text[i]);
            else bytes.AddRange(Encoding.UTF8.GetBytes(text[i].ToString()));
        }
        return [.. bytes];
    }

    /// <summary>Order used for min/max: Iceberg's (code points for strings, big-endian bytes for UUIDs).</summary>
    internal static int Compare(object a, object b) => ValueComparer.Compare(a, b);
}

/// <summary>Per-column statistics of one data file, in <c>ducklake_file_column_stats</c> terms.</summary>
public sealed record DuckLakeColumnStats(long ColumnId, long ValueCount, long NullCount, string? Min, string? Max, bool? ContainsNan)
{
    /// <summary>Bytes of the column's chunks in the file, when known.</summary>
    public long? ColumnSizeBytes { get; init; }
}

/// <summary>
/// Collects <see cref="DuckLakeColumnStats"/> for every primitive (leaf) column of the rows written to a file.
/// <c>value_count</c> counts non-null values, as DuckDB writes and reads it; the spec's prose says "including NULL
/// values", which DuckDB does not do (docs/ducklake.md, "Where the reference implementation and the spec differ").
/// A null parent (or an empty list or map) counts as one null of each leaf beneath it, as Parquet's definition levels do.
/// </summary>
internal sealed class StatsCollector
{
    private sealed class Acc
    {
        public long Values, Nulls;
        public bool Nan;
        public object? Min, Max;
    }

    private readonly Dictionary<long, (DuckLakePrimitive Type, Acc Acc)> _leaves = [];
    private readonly IReadOnlyList<DuckLakeColumn> _columns;

    public StatsCollector(IReadOnlyList<DuckLakeColumn> columns)
    {
        _columns = columns;
        foreach (var leaf in columns.SelectMany(c => c.SelfAndDescendants()).Where(c => c.Type is DuckLakePrimitive))
            _leaves[leaf.Id] = ((DuckLakePrimitive)leaf.Type, new Acc());
    }

    public void Add(object?[] row)
    {
        for (var i = 0; i < _columns.Count; i++) Visit(_columns[i], row[i]);
    }

    private void Visit(DuckLakeColumn column, object? value)
    {
        switch (column.Type)
        {
            case DuckLakePrimitive:
                var acc = _leaves[column.Id].Acc;
                if (value is null) { acc.Nulls++; return; }
                acc.Values++;
                if (ValueComparer.IsNaN(value)) { acc.Nan = true; return; }
                if (acc.Min is null || DuckLakeValues.Compare(value, acc.Min) < 0) acc.Min = value;
                if (acc.Max is null || DuckLakeValues.Compare(value, acc.Max) > 0) acc.Max = value;
                return;
            case DuckLakeStruct:
                var fields = value as object?[];
                for (var i = 0; i < column.Children.Count; i++) Visit(column.Children[i], fields?[i]);
                return;
            case DuckLakeList:
                var items = (value as IList)?.Cast<object?>().ToList();
                if (items is null || items.Count == 0) Visit(column.Children[0], null);
                else foreach (var item in items) Visit(column.Children[0], item);
                return;
            case DuckLakeMap:
                var map = value as IDictionary;
                if (map is null || map.Count == 0)
                {
                    Visit(column.Children[0], null);
                    Visit(column.Children[1], null);
                }
                else
                {
                    foreach (DictionaryEntry e in map)
                    {
                        Visit(column.Children[0], e.Key);
                        Visit(column.Children[1], e.Value);
                    }
                }
                return;
        }
    }

    public IReadOnlyList<DuckLakeColumnStats> Result(IReadOnlyDictionary<int, long>? columnSizes) =>
        _leaves.Select(kv =>
        {
            var (type, acc) = kv.Value;
            var isFloat = type.Name is "float32" or "float64";
            var min = acc.Min is null ? null : DuckLakeValues.Format(type, acc.Min);
            var max = acc.Max is null ? null : DuckLakeValues.Format(type, acc.Max);
            return new DuckLakeColumnStats(kv.Key, acc.Values, acc.Nulls, min is null || max is null ? null : min, min is null || max is null ? null : max,
                isFloat ? acc.Nan : null)
            {
                ColumnSizeBytes = columnSizes is not null && columnSizes.TryGetValue((int)kv.Key, out var size) ? size : null,
            };
        }).OrderBy(s => s.ColumnId).ToList();
}
