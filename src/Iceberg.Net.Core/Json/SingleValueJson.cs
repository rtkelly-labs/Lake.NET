using System.Globalization;
using System.Text.Json.Nodes;
using Iceberg.Net.Types;

namespace Iceberg.Net.Json;

/// <summary>
/// Spec Appendix D, "JSON single-value serialization" (used for column <c>initial-default</c>/<c>write-default</c>),
/// over the physical value representation: days for dates, micros (or nanos) for times and timestamps,
/// <see cref="IcebergDecimal"/>, <see cref="Guid"/>, <c>byte[]</c>; structs as <c>object?[]</c> in field order, lists as
/// <c>IReadOnlyList&lt;object?&gt;</c>, maps as key/value pair lists.
/// </summary>
public static class SingleValueJson
{
    public static JsonNode? ToJson(IcebergType type, object? value)
    {
        if (value is null) return null;
        return (type, value) switch
        {
            (PrimitiveType { TypeId: TypeId.Boolean }, bool b) => b,
            (PrimitiveType { TypeId: TypeId.Int }, int i) => i,
            (PrimitiveType { TypeId: TypeId.Long }, long l) => l,
            (PrimitiveType { TypeId: TypeId.Float }, float f) when Compat.IsFinite(f) => f,
            (PrimitiveType { TypeId: TypeId.Double }, double d) when Compat.IsFinite(d) => d,
            (DecimalType, IcebergDecimal m) => m.ToString(),
            (PrimitiveType { TypeId: TypeId.Date }, int days) => IcebergTime.Epoch.AddDays(days).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            (PrimitiveType { TypeId: TypeId.Time }, long us) => $"{TimeSpan.FromTicks(us * 10):hh\\:mm\\:ss}.{us % 1_000_000:D6}",
            (PrimitiveType { TypeId: TypeId.Timestamp }, long us) => Timestamp(us, 1_000_000, 6, ""),
            (PrimitiveType { TypeId: TypeId.TimestampTz }, long us) => Timestamp(us, 1_000_000, 6, "+00:00"),
            (PrimitiveType { TypeId: TypeId.TimestampNs }, long ns) => Timestamp(ns, 1_000_000_000, 9, ""),
            (PrimitiveType { TypeId: TypeId.TimestampTzNs }, long ns) => Timestamp(ns, 1_000_000_000, 9, "+00:00"),
            (PrimitiveType { TypeId: TypeId.String }, string s) => s,
            (PrimitiveType { TypeId: TypeId.Uuid }, Guid g) => g.ToString("D"),
            (_, byte[] bytes) when type.TypeId is TypeId.Binary or TypeId.Fixed => Compat.ToHex(bytes).ToLowerInvariant(),
            (StructType s, object?[] fields) => new JsonObject(s.Fields.Select((f, i) =>
                KeyValuePair.Create(f.Id.ToString(CultureInfo.InvariantCulture), ToJson(f.Type, fields[i])))),
            (ListType l, IEnumerable<object?> items) => new JsonArray(items.Select(v => ToJson(l.ElementType, v)).ToArray()),
            (MapType m, IEnumerable<KeyValuePair<object, object?>> entries) => new JsonObject
            {
                ["keys"] = new JsonArray(entries.Select(e => ToJson(m.KeyType, e.Key)).ToArray()),
                ["values"] = new JsonArray(entries.Select(e => ToJson(m.ValueType, e.Value)).ToArray()),
            },
            _ => throw new ArgumentException($"Cannot serialize {value.GetType().Name} '{value}' as a {type} single value"),
        };
    }

    public static object? FromJson(IcebergType type, JsonNode? node)
    {
        if (node is null) return null;
        string Text() => node.GetValue<string>();
        return type switch
        {
            PrimitiveType { TypeId: TypeId.Boolean } => node.GetValue<bool>(),
            PrimitiveType { TypeId: TypeId.Int } => node.GetValue<int>(),
            PrimitiveType { TypeId: TypeId.Long } => node.GetValue<long>(),
            PrimitiveType { TypeId: TypeId.Float } => node.GetValue<float>(),
            PrimitiveType { TypeId: TypeId.Double } => node.GetValue<double>(),
            DecimalType => IcebergDecimal.Parse(Text()),
            PrimitiveType { TypeId: TypeId.Date } => IcebergTime.DaysFromIsoDate(Text()),
            PrimitiveType { TypeId: TypeId.Time } => ParseTimeOfDay(Text(), 6),
            PrimitiveType { TypeId: TypeId.Timestamp } => ParseTimestamp(Text(), 6, zoned: false),
            PrimitiveType { TypeId: TypeId.TimestampTz } => ParseTimestamp(Text(), 6, zoned: true),
            PrimitiveType { TypeId: TypeId.TimestampNs } => ParseTimestamp(Text(), 9, zoned: false),
            PrimitiveType { TypeId: TypeId.TimestampTzNs } => ParseTimestamp(Text(), 9, zoned: true),
            PrimitiveType { TypeId: TypeId.String } => Text(),
            PrimitiveType { TypeId: TypeId.Uuid } => Guid.Parse(Text()),
            _ when type.TypeId is TypeId.Binary or TypeId.Fixed => Compat.FromHex(Text()),
            StructType s => s.Fields.Select(f => FromJson(f.Type, node[f.Id.ToString(CultureInfo.InvariantCulture)])).ToArray(),
            ListType l => node.AsArray().Select(v => FromJson(l.ElementType, v)).ToList(),
            MapType m => node["keys"]!.AsArray().Zip(node["values"]!.AsArray())
                .Select(kv => KeyValuePair.Create(FromJson(m.KeyType, kv.First)!, FromJson(m.ValueType, kv.Second))).ToList(),
            _ => throw new NotSupportedException($"No JSON single-value form for {type}"),
        };
    }

    private static string Timestamp(long value, long perSecond, int digits, string zone)
    {
        var seconds = IcebergTime.FloorDiv(value, perSecond);
        var fraction = value - seconds * perSecond;
        var dt = IcebergTime.Epoch.AddSeconds(seconds);
        return $"{dt:yyyy-MM-dd'T'HH:mm:ss}.{fraction.ToString(new string('0', digits), CultureInfo.InvariantCulture)}{zone}";
    }

    private static long ParseTimeOfDay(string text, int digits)
    {
        var (whole, fraction) = SplitFraction(text, digits);
        var t = TimeSpan.ParseExact(whole, @"hh\:mm\:ss", CultureInfo.InvariantCulture);
        return t.Ticks / TimeSpan.TicksPerSecond * Pow10(digits) + fraction;
    }

    private static long ParseTimestamp(string text, int digits, bool zoned)
    {
        if (zoned)
        {
            if (!text.EndsWith("+00:00", StringComparison.Ordinal)) throw new FormatException($"timestamptz single value must end with +00:00: '{text}'");
            text = text[..^6];
        }
        else if (text.EndsWith('Z') || text.Length > 19 && text[^6] is '+' or '-' && text[^3] == ':')
            throw new FormatException($"timestamp single value must not include a zone offset: '{text}'");
        var (whole, fraction) = SplitFraction(text, digits);
        var dt = DateTime.ParseExact(whole, "yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        return (dt.Ticks - IcebergTime.Epoch.Ticks) / TimeSpan.TicksPerSecond * Pow10(digits) + fraction;
    }

    private static (string Whole, long Fraction) SplitFraction(string text, int digits)
    {
        var dot = text.IndexOf('.');
        if (dot < 0) return (text, 0);
        var f = text[(dot + 1)..];
        if (f.Length > digits || !f.All(c => c is >= '0' and <= '9')) throw new FormatException($"Bad fractional seconds in '{text}'");
        return (text[..dot], long.Parse(f.PadRight(digits, '0'), CultureInfo.InvariantCulture));
    }

    private static long Pow10(int n) => n == 6 ? 1_000_000 : 1_000_000_000;
}
