using System.Globalization;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Lake.Delta;

/// <summary>
/// A Delta data type, from the Spark-style JSON schema in <c>metaData.schemaString</c> (PROTOCOL.md "Schema
/// Serialization Format"). Values read from a table use these CLR types:
/// <list type="table">
/// <item><term>string / binary / boolean</term><description><see cref="string"/> / <c>byte[]</c> / <see cref="bool"/></description></item>
/// <item><term>byte / short / integer / long</term><description><see cref="sbyte"/> / <see cref="short"/> / <see cref="int"/> / <see cref="long"/></description></item>
/// <item><term>float / double / decimal(p,s)</term><description><see cref="float"/> / <see cref="double"/> / <see cref="decimal"/></description></item>
/// <item><term>date</term><description><see cref="DateOnly"/></description></item>
/// <item><term>timestamp</term><description><see cref="DateTimeOffset"/> in UTC</description></item>
/// <item><term>timestamp_ntz</term><description><see cref="DateTime"/> with <see cref="DateTimeKind.Unspecified"/></description></item>
/// <item><term>struct / array / map</term><description><c>object?[]</c> (one slot per field) / <c>List&lt;object?&gt;</c> / <c>Dictionary&lt;object, object?&gt;</c></description></item>
/// <item><term>void</term><description>always null</description></item>
/// </list>
/// </summary>
public abstract record DeltaType
{
    public abstract string ToJsonName();
    public sealed override string ToString() => ToJsonName();
}

public sealed record DeltaPrimitiveType : DeltaType
{
    public static readonly DeltaPrimitiveType String = new("string"), Long = new("long"), Integer = new("integer"),
        Short = new("short"), Byte = new("byte"), Float = new("float"), Double = new("double"), Boolean = new("boolean"),
        Binary = new("binary"), Date = new("date"), Timestamp = new("timestamp"), TimestampNtz = new("timestamp_ntz"),
        Void = new("void");

    private static readonly Dictionary<string, DeltaPrimitiveType> ByName = new[]
    {
        String, Long, Integer, Short, Byte, Float, Double, Boolean, Binary, Date, Timestamp, TimestampNtz, Void,
    }.ToDictionary(t => t.Name);

    private DeltaPrimitiveType(string name) => Name = name;

    public string Name { get; }

    internal static DeltaPrimitiveType? Find(string name) => ByName.GetValueOrDefault(name);

    public override string ToJsonName() => Name;
}

public sealed record DeltaDecimalType(int Precision, int Scale) : DeltaType
{
    public override string ToJsonName() => $"decimal({Precision},{Scale})";
}

public sealed record DeltaArrayType(DeltaType ElementType, bool ContainsNull) : DeltaType
{
    public override string ToJsonName() => $"array<{ElementType.ToJsonName()}>";
}

public sealed record DeltaMapType(DeltaType KeyType, DeltaType ValueType, bool ValueContainsNull) : DeltaType
{
    public override string ToJsonName() => $"map<{KeyType.ToJsonName()},{ValueType.ToJsonName()}>";
}

public sealed record DeltaStructType(IReadOnlyList<DeltaField> Fields) : DeltaType
{
    public override string ToJsonName() => $"struct<{string.Join(",", Fields.Select(f => f.Name + ":" + f.Type.ToJsonName()))}>";

    public int IndexOf(string name)
    {
        for (var i = 0; i < Fields.Count; i++)
            if (Fields[i].Name == name) return i;
        return -1;
    }
}

/// <summary>A struct field. Column-mapping metadata (<c>delta.columnMapping.*</c>) is read from <see cref="Metadata"/>.</summary>
public sealed record DeltaField(string Name, DeltaType Type, bool Nullable, JsonObject Metadata)
{
    public const string ColumnMappingIdKey = "delta.columnMapping.id";
    public const string PhysicalNameKey = "delta.columnMapping.physicalName";

    /// <summary><c>delta.columnMapping.id</c>, the Parquet field ID in <c>id</c> mode.</summary>
    public int? ColumnMappingId => Metadata[ColumnMappingIdKey] is JsonValue v && Json.TryGetLong(v, out var id) ? checked((int)id) : null;

    /// <summary><c>delta.columnMapping.physicalName</c>.</summary>
    public string? PhysicalNameMetadata => Metadata[PhysicalNameKey] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

    /// <summary>The column's name in data files, partition values and statistics under <paramref name="mode"/>.</summary>
    public string PhysicalName(ColumnMappingMode mode) => mode == ColumnMappingMode.None
        ? Name
        : PhysicalNameMetadata ?? throw new InvalidDataException($"Field '{Name}' has no {PhysicalNameKey} although the table uses column mapping mode {mode.ToString().ToLowerInvariant()}");
}

/// <summary><c>delta.columnMapping.mode</c>.</summary>
public enum ColumnMappingMode
{
    None,
    Name,
    Id,
}

/// <summary>Parses <c>schemaString</c>.</summary>
public static partial class DeltaSchema
{
    [GeneratedRegex(@"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")]
    private static partial Regex DecimalPattern();

    public static DeltaStructType Parse(string schemaString) =>
        Corruption.Guard("Delta schema", () =>
        {
            var node = JsonNode.Parse(schemaString, documentOptions: Json.DocumentOptions)
                ?? throw new InvalidDataException("The Delta schema is JSON null");
            return ParseType(node, "schema") as DeltaStructType
                ?? throw new InvalidDataException("The Delta schema's top level is not a struct");
        });

    internal static DeltaType ParseType(JsonNode node, string path)
    {
        if (node is JsonValue v)
        {
            var name = v.TryGetValue<string>(out var s) ? s : throw new InvalidDataException($"{path}: type is not a string or an object");
            if (DeltaPrimitiveType.Find(name) is { } primitive) return primitive;
            if (name == "decimal") return new DeltaDecimalType(10, 0); // Spark's default
            var m = DecimalPattern().Match(name);
            if (m.Success)
            {
                var precision = int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture);
                var scale = int.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
                if (precision is < 1 or > 38 || scale > precision)
                    throw new InvalidDataException($"{path}: invalid {name}");
                return new DeltaDecimalType(precision, scale);
            }
            if (name == "variant")
                throw new DeltaUnsupportedFeatureException($"{path}: the variant type is not supported by this reader", ["variantType"]);
            throw new InvalidDataException($"{path}: unknown type '{name}'");
        }
        if (node is not JsonObject o) throw new InvalidDataException($"{path}: type is not a string or an object");
        var kind = Json.Str(o, "type", path) ?? throw new InvalidDataException($"{path}: complex type has no 'type'");
        switch (kind)
        {
            case "struct":
            {
                if (o["fields"] is not JsonArray fields) throw new InvalidDataException($"{path}: struct has no 'fields' array");
                var result = new List<DeltaField>(fields.Count);
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var f in fields)
                {
                    if (f is not JsonObject field) throw new InvalidDataException($"{path}: struct field is not an object");
                    var name = Json.Str(field, "name", path) ?? throw new InvalidDataException($"{path}: struct field has no name");
                    if (!seen.Add(name)) throw new InvalidDataException($"{path}: duplicate field name '{name}' (names are unique regardless of case)");
                    var type = field["type"] ?? throw new InvalidDataException($"{path}.{name}: no type");
                    var metadata = field["metadata"] switch
                    {
                        null => new JsonObject(),
                        JsonObject md => (JsonObject)md.DeepClone(),
                        _ => throw new InvalidDataException($"{path}.{name}: metadata is not an object"),
                    };
                    result.Add(new DeltaField(name, ParseType(type, $"{path}.{name}"), Json.Bool(field, "nullable", path) ?? true, metadata));
                }
                return new DeltaStructType(result);
            }
            case "array":
                return new DeltaArrayType(
                    ParseType(o["elementType"] ?? throw new InvalidDataException($"{path}: array has no elementType"), path + ".element"),
                    Json.Bool(o, "containsNull", path) ?? true);
            case "map":
                return new DeltaMapType(
                    ParseType(o["keyType"] ?? throw new InvalidDataException($"{path}: map has no keyType"), path + ".key"),
                    ParseType(o["valueType"] ?? throw new InvalidDataException($"{path}: map has no valueType"), path + ".value"),
                    Json.Bool(o, "valueContainsNull", path) ?? true);
            default:
                throw new InvalidDataException($"{path}: unknown complex type '{kind}'");
        }
    }

    /// <summary>Every field, nested ones included, with its dotted logical path.</summary>
    internal static IEnumerable<(string Path, DeltaField Field)> AllFields(DeltaStructType type, string prefix = "")
    {
        foreach (var f in type.Fields)
        {
            var path = prefix.Length == 0 ? f.Name : prefix + "." + f.Name;
            yield return (path, f);
            foreach (var nested in Nested(f.Type, path)) yield return nested;
        }

        static IEnumerable<(string, DeltaField)> Nested(DeltaType t, string path) => t switch
        {
            DeltaStructType s => AllFields(s, path),
            DeltaArrayType a => Nested(a.ElementType, path + ".element"),
            DeltaMapType m => Nested(m.KeyType, path + ".key").Concat(Nested(m.ValueType, path + ".value")),
            _ => [],
        };
    }
}
