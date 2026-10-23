using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.TestSupport;

/// <summary>
/// Values as <c>spark_oracle.py</c> prints them (structs as objects by name, timestamps as micros, dates ISO, decimals
/// and UUIDs as text, NaN as "NaN") rendered in <see cref="TableFingerprint.Canonical"/>'s form, so a Spark row and one
/// of ours compare as strings, nested values included.
/// </summary>
public static class SparkJson
{
    private const string Null = "\u0000";

    public static string Ours(IcebergType type, object? value) => value is null ? Null : TableFingerprint.Canonical(type, value);

    public static string Canonical(IcebergType type, JsonNode? v)
    {
        if (v is null) return Null;
        switch (type)
        {
            case StructType st:
                return "(" + string.Join(",", st.Fields.Select(f => Canonical(f.Type, v[f.Name]))) + ")";
            case ListType lt:
                return "[" + string.Join(",", v.AsArray().Select(e => Canonical(lt.ElementType, e))) + "]";
            case MapType mt:
                return "{" + string.Join(",", v.AsObject()
                    .Select(kv => (K: Canonical(mt.KeyType, JsonValue.Create(kv.Key)), V: Canonical(mt.ValueType, kv.Value)))
                    .OrderBy(x => x.K, StringComparer.Ordinal).Select(x => $"{x.K}={x.V}")) + "}";
        }
        var text = v.GetValueKind() == JsonValueKind.String ? v.GetValue<string>() : v.ToJsonString();
        object value = type.TypeId switch
        {
            TypeId.Boolean => v.GetValue<bool>(),
            TypeId.Int => v.GetValue<int>(),
            TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs => v.GetValue<long>(),
            TypeId.Date => IcebergTime.DaysFromDate(DateOnly.Parse(text, CultureInfo.InvariantCulture)),
            TypeId.Float => text == "NaN" ? float.NaN : v.GetValue<float>(),
            TypeId.Double => text == "NaN" ? double.NaN : v.GetValue<double>(),
            TypeId.Decimal => IcebergDecimal.Parse(text),
            TypeId.Uuid => Guid.Parse(text),
            TypeId.String => text,
            _ => throw new NotSupportedException($"No Spark JSON reading for {type}"),
        };
        return TableFingerprint.Canonical(type, value);
    }

    /// <summary>Each row as one string, keyed by its first column (a long ID).</summary>
    public static Dictionary<long, string> Rows(Schema schema, JsonArray rows) =>
        rows.ToDictionary(r => r![schema.Columns[0].Name]!.GetValue<long>(), r => string.Join("|", schema.Columns.Select(c => Canonical(c.Type, r![c.Name]))));

    public static Dictionary<long, string> Rows(Schema schema, IEnumerable<object?[]> rows) =>
        rows.ToDictionary(r => (long)r[0]!, r => string.Join("|", schema.Columns.Select((c, i) => Ours(c.Type, r[i]))));
}
