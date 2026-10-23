using System.Text.Json.Nodes;

namespace Iceberg.Net.Avro;

/// <summary>
/// Minimal Avro schema model. Iceberg relies on per-field <c>field-id</c>, <c>element-id</c>,
/// <c>key-id</c>/<c>value-id</c> properties and on logical types over <c>fixed</c> (uuid, decimal) and over
/// arrays (<c>map</c>), which general-purpose Avro libraries handle poorly, so this keeps them first-class.
/// </summary>
public abstract record AvroSchema
{
    public string? LogicalType { get; init; }
    /// <summary>Extra properties written verbatim (precision, scale, adjust-to-utc, element-id, ...).</summary>
    public IReadOnlyDictionary<string, JsonNode> Props { get; init; } = new Dictionary<string, JsonNode>();

    public static AvroPrimitive Null => new(AvroKind.Null);
    public static AvroPrimitive Boolean => new(AvroKind.Boolean);
    public static AvroPrimitive Int => new(AvroKind.Int);
    public static AvroPrimitive Long => new(AvroKind.Long);
    public static AvroPrimitive Float => new(AvroKind.Float);
    public static AvroPrimitive Double => new(AvroKind.Double);
    public static AvroPrimitive Bytes => new(AvroKind.Bytes);
    public static AvroPrimitive String => new(AvroKind.String);

    public static AvroUnion Optional(AvroSchema s) => new([Null, s]);

    public AvroSchema WithLogical(string logicalType, params (string Key, JsonNode Value)[] props) => this with
    {
        LogicalType = logicalType,
        Props = Props.Concat(props.Select(p => KeyValuePair.Create(p.Key, p.Value))).ToDictionary(p => p.Key, p => p.Value),
    };

    public AvroSchema WithProp(string key, JsonNode value) => this with
    {
        Props = Props.Where(p => p.Key != key).Append(KeyValuePair.Create(key, value)).ToDictionary(p => p.Key, p => p.Value),
    };

    public bool IsNullable => this is AvroUnion u && u.Branches.Any(b => b is AvroPrimitive { Kind: AvroKind.Null });

    /// <summary>The non-null branch of an optional union, or this schema.</summary>
    public AvroSchema NonNull => this is AvroUnion u && u.Branches.Count == 2 && IsNullable ? u.Branches.First(b => b is not AvroPrimitive { Kind: AvroKind.Null }) : this;

    public int? IntProp(string key) => Props.TryGetValue(key, out var v) ? v.GetValue<int>() : null;
}

public enum AvroKind { Null, Boolean, Int, Long, Float, Double, Bytes, String }

public sealed record AvroPrimitive(AvroKind Kind) : AvroSchema;

public sealed record AvroField(string Name, AvroSchema Schema, int? FieldId = null, string? Doc = null, bool DefaultNull = false);

public sealed record AvroRecord(string Name, IReadOnlyList<AvroField> Fields) : AvroSchema
{
    public AvroField? ByFieldId(int id) => Fields.FirstOrDefault(f => f.FieldId == id);
    public int IndexOf(string name)
    {
        for (var i = 0; i < Fields.Count; i++) if (Fields[i].Name == name) return i;
        return -1;
    }
}

public sealed record AvroArray(AvroSchema Items) : AvroSchema;

/// <summary>Avro map (string keys only).</summary>
public sealed record AvroMap(AvroSchema Values) : AvroSchema;

public sealed record AvroFixed(string Name, int Size) : AvroSchema;

public sealed record AvroUnion(IReadOnlyList<AvroSchema> Branches) : AvroSchema;

public static class AvroSchemaJson
{
    public static JsonNode ToJson(AvroSchema schema)
    {
        var seen = new HashSet<string>();
        return Write(schema, seen);
    }

    private static JsonNode Write(AvroSchema s, HashSet<string> seen)
    {
        JsonNode node;
        switch (s)
        {
            case AvroPrimitive p:
                var name = p.Kind.ToString().ToLowerInvariant();
                if (p.LogicalType is null && p.Props.Count == 0) return JsonValue.Create(name)!;
                node = new JsonObject { ["type"] = name };
                break;
            case AvroRecord r:
                if (!seen.Add(r.Name)) return JsonValue.Create(r.Name)!;
                node = new JsonObject
                {
                    ["type"] = "record",
                    ["name"] = r.Name,
                    ["fields"] = new JsonArray(r.Fields.Select(f =>
                    {
                        var o = new JsonObject { ["name"] = f.Name, ["type"] = Write(f.Schema, seen) };
                        if (f.Doc is not null) o["doc"] = f.Doc;
                        if (f.DefaultNull) o["default"] = null;
                        if (f.FieldId is { } id) o["field-id"] = id;
                        return (JsonNode)o;
                    }).ToArray()),
                };
                break;
            case AvroArray a:
                node = new JsonObject { ["type"] = "array", ["items"] = Write(a.Items, seen) };
                break;
            case AvroMap m:
                node = new JsonObject { ["type"] = "map", ["values"] = Write(m.Values, seen) };
                break;
            case AvroFixed f:
                if (!seen.Add(f.Name)) return JsonValue.Create(f.Name)!;
                node = new JsonObject { ["type"] = "fixed", ["name"] = f.Name, ["size"] = f.Size };
                break;
            case AvroUnion u:
                return new JsonArray(u.Branches.Select(b => (JsonNode?)Write(b, seen)).ToArray());
            default:
                throw new NotSupportedException(s.GetType().Name);
        }
        var obj = node.AsObject();
        if (s.LogicalType is not null) obj["logicalType"] = s.LogicalType;
        foreach (var (k, v) in s.Props) obj[k] = v.DeepClone();
        return obj;
    }

    public static AvroSchema Parse(string json) => Read(JsonNode.Parse(json)!, new Dictionary<string, AvroSchema>());

    private static readonly HashSet<string> Reserved = ["type", "name", "namespace", "fields", "items", "values", "size", "logicalType", "doc", "aliases", "symbols", "default"];

    private static AvroSchema Read(JsonNode n, Dictionary<string, AvroSchema> named)
    {
        switch (n)
        {
            case JsonArray arr:
                return new AvroUnion(arr.Select(b => Read(b!, named)).ToList());
            case JsonValue v:
            {
                var t = v.GetValue<string>();
                return Primitive(t) ?? (named.TryGetValue(t, out var ns) ? ns : throw new FormatException($"Unknown Avro type {t}"));
            }
        }

        var o = n.AsObject();
        var type = o["type"]!;
        if (type is not JsonValue) return Read(type, named);
        var typeName = type.GetValue<string>();
        AvroSchema result = typeName switch
        {
            "record" or "error" => ReadRecord(o, named),
            "array" => new AvroArray(Read(o["items"]!, named)),
            "map" => new AvroMap(Read(o["values"]!, named)),
            "fixed" => Register(named, new AvroFixed(o["name"]!.GetValue<string>(), o["size"]!.GetValue<int>())),
            "enum" => throw new NotSupportedException("Avro enums are not used by Iceberg"),
            _ => Primitive(typeName) ?? (named.TryGetValue(typeName, out var ns) ? ns : throw new FormatException($"Unknown Avro type {typeName}")),
        };
        var props = o.Where(kv => !Reserved.Contains(kv.Key) && kv.Value is not null).ToDictionary(kv => kv.Key, kv => kv.Value!.DeepClone());
        return result with { LogicalType = o["logicalType"]?.GetValue<string>(), Props = props };
    }

    private static AvroSchema Register(Dictionary<string, AvroSchema> named, AvroSchema s)
    {
        var name = s switch { AvroFixed f => f.Name, AvroRecord r => r.Name, _ => throw new InvalidOperationException() };
        named[name] = s;
        return s;
    }

    private static AvroRecord ReadRecord(JsonObject o, Dictionary<string, AvroSchema> named)
    {
        var name = o["name"]!.GetValue<string>();
        var fields = o["fields"]!.AsArray().Select(f => new AvroField(
            f!["name"]!.GetValue<string>(),
            Read(f["type"]!, named),
            f["field-id"]?.GetValue<int>(),
            f["doc"]?.GetValue<string>(),
            f.AsObject().ContainsKey("default") && f["default"] is null)).ToList();
        var rec = new AvroRecord(name, fields);
        named[name] = rec;
        return rec;
    }

    private static AvroPrimitive? Primitive(string t) => t switch
    {
        "null" => AvroSchema.Null,
        "boolean" => AvroSchema.Boolean,
        "int" => AvroSchema.Int,
        "long" => AvroSchema.Long,
        "float" => AvroSchema.Float,
        "double" => AvroSchema.Double,
        "bytes" => AvroSchema.Bytes,
        "string" => AvroSchema.String,
        _ => null,
    };
}
