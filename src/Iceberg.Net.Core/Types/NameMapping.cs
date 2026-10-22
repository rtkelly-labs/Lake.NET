using System.Text.Json.Nodes;

namespace Iceberg.Net.Types;

/// <summary>A name-mapping entry: the field ID a set of file column names maps to ("Column Projection" in the spec).</summary>
public sealed record MappedField(int? FieldId, IReadOnlyList<string> Names, IReadOnlyList<MappedField>? Fields = null);

/// <summary>
/// The table's default name mapping (<c>schema.name-mapping.default</c>): how readers resolve columns in data
/// files written without Parquet field IDs — files from Hive migrations, <c>add_files</c>, or writers such as
/// Parquet.Net-based generators that do not emit IDs.
/// </summary>
public sealed class NameMapping(IReadOnlyList<MappedField> fields)
{
    public const string Property = "schema.name-mapping.default";

    public IReadOnlyList<MappedField> Fields { get; } = fields;

    /// <summary>Map every field's current name to its ID (what Java's <c>MappingUtil.create</c> does).</summary>
    public static NameMapping FromSchema(Schema schema) => new(MapStruct(schema.Struct));

    private static List<MappedField> MapStruct(StructType s) => s.Fields.Select(f => new MappedField(f.Id, [f.Name], Children(f.Type))).ToList();

    private static List<MappedField>? Children(IcebergType t) => t switch
    {
        StructType s => MapStruct(s),
        ListType l => [new MappedField(l.ElementId, ["element"], Children(l.ElementType))],
        MapType m => [new MappedField(m.KeyId, ["key"], Children(m.KeyType)), new MappedField(m.ValueId, ["value"], Children(m.ValueType))],
        _ => null,
    };

    /// <summary>Top-level file column names that map to <paramref name="fieldId"/>.</summary>
    public IReadOnlyList<string> NamesFor(int fieldId) => Fields.FirstOrDefault(f => f.FieldId == fieldId)?.Names ?? [];

    /// <summary>Add a historical name for a field (e.g. after a rename), so older files still resolve.</summary>
    public NameMapping WithAlias(int fieldId, string name) => new(Fields.Select(f =>
        f.FieldId == fieldId && !f.Names.Contains(name) ? f with { Names = [.. f.Names, name] } : f).ToList());

    public string ToJson() => ToNode(Fields).ToJsonString();

    private static JsonArray ToNode(IEnumerable<MappedField> fields) => new(fields.Select(f =>
    {
        var o = new JsonObject();
        if (f.FieldId is { } id) o["field-id"] = id;
        o["names"] = new JsonArray(f.Names.Select(n => (JsonNode)n).ToArray());
        if (f.Fields is { Count: > 0 }) o["fields"] = ToNode(f.Fields);
        return (JsonNode)o;
    }).ToArray());

    public static NameMapping Parse(string json) => new(FromNode(JsonNode.Parse(json)!.AsArray()));

    private static List<MappedField> FromNode(JsonArray a) => a.Select(n => new MappedField(
        n!["field-id"]?.GetValue<int>(),
        n["names"]!.AsArray().Select(x => x!.GetValue<string>()).ToList(),
        n["fields"] is JsonArray children ? FromNode(children) : null)).ToList();

    public static NameMapping? ForTable(IReadOnlyDictionary<string, string> properties) =>
        properties.TryGetValue(Property, out var json) ? Parse(json) : null;
}
