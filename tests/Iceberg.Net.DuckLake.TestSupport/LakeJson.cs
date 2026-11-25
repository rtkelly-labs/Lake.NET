using System.Collections;
using System.Globalization;
using System.Text.Json.Nodes;
using Iceberg.Net.Types;
using Xunit;

namespace Iceberg.Net.DuckLake.TestSupport;

/// <summary>Physical values as the JSON interop/ducklake_oracle.py makes of DuckDB's Python values, for comparing rows across engines.</summary>
public static class LakeJson
{
    public static List<string> Rows(IReadOnlyList<DuckLakeColumn> columns, IEnumerable<object?[]> rows) =>
        rows.Select(r => Canonical(new JsonArray(columns.Select((c, i) => Value(c.Type, r[i])).ToArray()))!.ToJsonString()).Order(StringComparer.Ordinal).ToList();

    /// <summary>Equal row sets, reporting the first differing row in full.</summary>
    public static void AssertSameRows(IReadOnlyList<string> expected, IReadOnlyList<string> actual)
    {
        var missing = expected.Except(actual).FirstOrDefault();
        var extra = actual.Except(expected).FirstOrDefault();
        Assert.True(missing is null && extra is null && expected.Count == actual.Count,
            $"{expected.Count} expected rows, {actual.Count} actual.\nFirst expected row not found: {missing}\nFirst actual row not expected: {extra}");
    }

    /// <summary>Numbers in one spelling (Python writes <c>4.0</c> where .NET writes <c>4</c>), everything else as is.</summary>
    public static JsonNode? Canonical(JsonNode? node) => node switch
    {
        JsonArray a => new JsonArray(a.Select(Canonical).ToArray()),
        JsonObject o => new JsonObject(o.Select(kv => KeyValuePair.Create(kv.Key, Canonical(kv.Value)))),
        JsonValue v when v.GetValueKind() == System.Text.Json.JsonValueKind.Number =>
            JsonValue.Create(double.Parse(v.ToJsonString(), CultureInfo.InvariantCulture)),
        JsonValue v => JsonNode.Parse(v.ToJsonString()),
        _ => null,
    };

    public static JsonNode? Value(DuckLakeType type, object? v)
    {
        if (v is null) return null;
        switch (type)
        {
            case DuckLakeList l:
                return new JsonArray(((IEnumerable)v).Cast<object?>().Select(x => Value(l.Element, x)).ToArray());
            case DuckLakeStruct s:
                var fields = (object?[])v;
                var o = new JsonObject();
                for (var i = 0; i < s.Fields.Count; i++) o[s.Fields[i].Name] = Value(s.Fields[i].Type, fields[i]);
                return o;
            case DuckLakeMap m:
                var map = new JsonObject();
                foreach (DictionaryEntry e in (IDictionary)v) map[e.Key as string ?? Value(m.Key, e.Key)!.ToJsonString()] = Value(m.Value, e.Value);
                return map;
        }
        var p = (DuckLakePrimitive)type;
        return p.Name switch
        {
            "date" => IcebergTime.DateFromDays((int)v).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            "time" => TimeOnly.FromTimeSpan(TimeSpan.FromTicks((long)v * 10)).ToString(((long)v % 1_000_000) == 0 ? "HH:mm:ss" : "HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
            "blob" => Convert.ToHexString((byte[])v).ToLowerInvariant(),
            "uuid" => ((Guid)v).ToString("D"),
            "float32" when float.IsNaN((float)v) => "NaN",
            "float64" when double.IsNaN((double)v) => "NaN",
            "float32" when float.IsInfinity((float)v) => (float)v > 0 ? "Infinity" : "-Infinity",
            "float64" when double.IsInfinity((double)v) => (double)v > 0 ? "Infinity" : "-Infinity",
            "float32" => JsonValue.Create((double)(float)v),
            _ when v is IcebergDecimal d => d.ToString(),
            _ => JsonValue.Create(v),
        };
    }
}
