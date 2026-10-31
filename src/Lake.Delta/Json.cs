using System.Text.Json;
using System.Text.Json.Nodes;

namespace Lake.Delta;

/// <summary>Typed, checked access to log JSON: a field of the wrong kind is a <see cref="InvalidDataException"/>.</summary>
internal static class Json
{
    /// <summary>Bounded nesting, so a hostile log cannot recurse the parser deep.</summary>
    public static readonly JsonDocumentOptions DocumentOptions = new() { MaxDepth = 64 };

    public static bool TryGetLong(JsonValue v, out long value)
    {
        if (v.TryGetValue(out value)) return true;
        if (v.TryGetValue<int>(out var i)) { value = i; return true; }
        if (v.TryGetValue<short>(out var s)) { value = s; return true; }
        if (v.TryGetValue<sbyte>(out var b)) { value = b; return true; }
        if (v.TryGetValue<JsonElement>(out var e) && e.ValueKind == JsonValueKind.Number && e.TryGetInt64(out value)) return true;
        value = 0;
        return false;
    }

    private static JsonNode? Field(JsonObject o, string name) => o.TryGetPropertyValue(name, out var n) ? n : null;

    public static string? Str(JsonObject o, string name, string where)
    {
        var n = Field(o, name);
        if (n is null) return null;
        if (n is JsonValue v && v.TryGetValue<string>(out var s)) return s;
        if (n is JsonValue e && e.TryGetValue<JsonElement>(out var el) && el.ValueKind == JsonValueKind.String) return el.GetString();
        throw new InvalidDataException($"{where}.{name} is not a string");
    }

    public static string ReqStr(JsonObject o, string name, string where) =>
        Str(o, name, where) ?? throw new InvalidDataException($"{where}.{name} is required");

    public static long? Long(JsonObject o, string name, string where)
    {
        var n = Field(o, name);
        if (n is null) return null;
        if (n is JsonValue v && TryGetLong(v, out var l)) return l;
        throw new InvalidDataException($"{where}.{name} is not an integer");
    }

    public static long ReqLong(JsonObject o, string name, string where) =>
        Long(o, name, where) ?? throw new InvalidDataException($"{where}.{name} is required");

    public static int? Int(JsonObject o, string name, string where)
    {
        var l = Long(o, name, where);
        if (l is null) return null;
        if (l is < int.MinValue or > int.MaxValue) throw new InvalidDataException($"{where}.{name} = {l} is out of the 32-bit range");
        return (int)l;
    }

    public static bool? Bool(JsonObject o, string name, string where)
    {
        var n = Field(o, name);
        if (n is null) return null;
        if (n is JsonValue v && v.TryGetValue<bool>(out var b)) return b;
        if (n is JsonValue e && e.TryGetValue<JsonElement>(out var el) && el.ValueKind is JsonValueKind.True or JsonValueKind.False) return el.GetBoolean();
        throw new InvalidDataException($"{where}.{name} is not a boolean");
    }

    public static JsonObject? Obj(JsonObject o, string name, string where) => Field(o, name) switch
    {
        null => null,
        JsonObject child => child,
        _ => throw new InvalidDataException($"{where}.{name} is not an object"),
    };

    /// <summary>A <c>Map[String, String]</c>; values may be null (a null partition value).</summary>
    public static Dictionary<string, string?>? StrMap(JsonObject o, string name, string where)
    {
        var map = Obj(o, name, where);
        if (map is null) return null;
        var result = new Dictionary<string, string?>(map.Count, StringComparer.Ordinal);
        foreach (var (k, v) in map)
            result[k] = v switch
            {
                null => null,
                JsonValue jv when jv.TryGetValue<string>(out var s) => s,
                JsonValue jv when jv.TryGetValue<JsonElement>(out var el) && el.ValueKind == JsonValueKind.String => el.GetString(),
                _ => throw new InvalidDataException($"{where}.{name}['{k}'] is not a string"),
            };
        return result;
    }

    public static List<string>? StrList(JsonObject o, string name, string where)
    {
        var n = Field(o, name);
        if (n is null) return null;
        if (n is not JsonArray a) throw new InvalidDataException($"{where}.{name} is not an array");
        var result = new List<string>(a.Count);
        foreach (var item in a)
            result.Add(item is JsonValue v && v.TryGetValue<string>(out var s) ? s
                : item is JsonValue e && e.TryGetValue<JsonElement>(out var el) && el.ValueKind == JsonValueKind.String ? el.GetString()!
                : throw new InvalidDataException($"{where}.{name} has a non-string element"));
        return result;
    }
}
