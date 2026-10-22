using System.Globalization;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Json;

/// <summary>JSON for types, schemas, partition specs and sort orders (spec Appendix C).</summary>
public static partial class TypeJson
{
    [GeneratedRegex(@"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")]
    private static partial Regex DecimalRe();

    [GeneratedRegex(@"^fixed\[\s*(\d+)\s*\]$")]
    private static partial Regex FixedRe();

    [GeneratedRegex(@"^geometry\s*\(\s*([^)]*)\s*\)$")]
    private static partial Regex GeometryRe();

    [GeneratedRegex(@"^geography\s*\(\s*([^,)]*)\s*(?:,\s*([^)]*))?\)$")]
    private static partial Regex GeographyRe();

    public static JsonNode ToJson(IcebergType type) => type switch
    {
        PrimitiveType p => JsonValue.Create(p.ToString())!,
        StructType s => StructToJson(s),
        ListType l => new JsonObject
        {
            ["type"] = "list",
            ["element-id"] = l.ElementId,
            ["element"] = ToJson(l.ElementType),
            ["element-required"] = l.ElementRequired,
        },
        MapType m => new JsonObject
        {
            ["type"] = "map",
            ["key-id"] = m.KeyId,
            ["key"] = ToJson(m.KeyType),
            ["value-id"] = m.ValueId,
            ["value"] = ToJson(m.ValueType),
            ["value-required"] = m.ValueRequired,
        },
        _ => throw new NotSupportedException(type.ToString()),
    };

    private static JsonObject StructToJson(StructType s) => new()
    {
        ["type"] = "struct",
        ["fields"] = new JsonArray(s.Fields.Select(FieldToJson).ToArray<JsonNode?>()),
    };

    private static JsonObject FieldToJson(NestedField f)
    {
        var o = new JsonObject
        {
            ["id"] = f.Id,
            ["name"] = f.Name,
            ["required"] = f.Required,
            ["type"] = ToJson(f.Type),
        };
        if (f.Doc is not null) o["doc"] = f.Doc;
        if (f.InitialDefault is JsonNode i) o["initial-default"] = i.DeepClone();
        if (f.WriteDefault is JsonNode w) o["write-default"] = w.DeepClone();
        return o;
    }

    public static IcebergType TypeFromJson(JsonNode node)
    {
        if (node is JsonValue v)
        {
            var s = v.GetValue<string>().Trim();
            return s switch
            {
                "boolean" => PrimitiveType.Boolean,
                "int" => PrimitiveType.Int,
                "long" => PrimitiveType.Long,
                "float" => PrimitiveType.Float,
                "double" => PrimitiveType.Double,
                "date" => PrimitiveType.Date,
                "time" => PrimitiveType.Time,
                "timestamp" => PrimitiveType.Timestamp,
                "timestamptz" => PrimitiveType.TimestampTz,
                "timestamp_ns" => PrimitiveType.TimestampNs,
                "timestamptz_ns" => PrimitiveType.TimestampTzNs,
                "string" => PrimitiveType.String,
                "uuid" => PrimitiveType.Uuid,
                "binary" => PrimitiveType.Binary,
                "variant" => PrimitiveType.Variant,
                "unknown" => PrimitiveType.Unknown,
                "geometry" => new GeometryType(),
                "geography" => new GeographyType(),
                _ => ParseParameterised(s),
            };
        }

        var o = node.AsObject();
        return o["type"]!.GetValue<string>() switch
        {
            "struct" => new StructType(o["fields"]!.AsArray().Select(f => FieldFromJson(f!))),
            "list" => new ListType(o["element-id"]!.GetValue<int>(), TypeFromJson(o["element"]!), o["element-required"]!.GetValue<bool>()),
            "map" => new MapType(o["key-id"]!.GetValue<int>(), TypeFromJson(o["key"]!), o["value-id"]!.GetValue<int>(), TypeFromJson(o["value"]!), o["value-required"]!.GetValue<bool>()),
            var t => throw new NotSupportedException($"Unknown nested type {t}"),
        };
    }

    private static IcebergType ParseParameterised(string s)
    {
        var m = DecimalRe().Match(s);
        if (m.Success) return new DecimalType(int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture), int.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture));
        m = FixedRe().Match(s);
        if (m.Success) return new FixedType(int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture));
        m = GeometryRe().Match(s);
        if (m.Success) return new GeometryType(m.Groups[1].Value.Trim());
        m = GeographyRe().Match(s);
        if (m.Success) return new GeographyType(m.Groups[1].Value.Trim(), m.Groups[2].Success ? m.Groups[2].Value.Trim() : null);
        throw new NotSupportedException($"Unknown type '{s}'");
    }

    private static NestedField FieldFromJson(JsonNode n) => new(
        n["id"]!.GetValue<int>(),
        n["name"]!.GetValue<string>(),
        TypeFromJson(n["type"]!),
        n["required"]!.GetValue<bool>(),
        n["doc"]?.GetValue<string>(),
        n["initial-default"]?.DeepClone(),
        n["write-default"]?.DeepClone());

    public static JsonObject SchemaToJson(Schema schema)
    {
        var o = StructToJson(schema.Struct);
        var result = new JsonObject { ["type"] = "struct", ["schema-id"] = schema.SchemaId };
        if (schema.IdentifierFieldIds.Count > 0)
            result["identifier-field-ids"] = new JsonArray(schema.IdentifierFieldIds.Select(i => (JsonNode)i).ToArray());
        result["fields"] = o["fields"]!.DeepClone();
        return result;
    }

    public static Schema SchemaFromJson(JsonNode n)
    {
        var s = (StructType)TypeFromJson(n);
        var id = n["schema-id"]?.GetValue<int>() ?? 0;
        var identifiers = n["identifier-field-ids"]?.AsArray().Select(x => x!.GetValue<int>());
        return new Schema(id, s.Fields, identifiers);
    }

    public static JsonObject SpecToJson(PartitionSpec spec) => new()
    {
        ["spec-id"] = spec.SpecId,
        ["fields"] = SpecFieldsToJson(spec),
    };

    public static JsonArray SpecFieldsToJson(PartitionSpec spec) => new(spec.Fields.Select(f =>
    {
        var o = new JsonObject { ["name"] = f.Name, ["transform"] = f.Transform.ToString() };
        if (f.SourceIds is { Count: > 1 }) o["source-ids"] = new JsonArray(f.SourceIds.Select(i => (JsonNode)i).ToArray());
        else o["source-id"] = f.SourceId;
        o["field-id"] = f.FieldId;
        return (JsonNode)o;
    }).ToArray());

    public static PartitionSpec SpecFromJson(JsonNode n) =>
        new(n["spec-id"]?.GetValue<int>() ?? 0, SpecFieldsFromJson(n["fields"]!.AsArray()));

    /// <summary>v1 specs may omit field-id; IDs are then assigned sequentially from 1000 (spec requirement).</summary>
    public static IEnumerable<PartitionField> SpecFieldsFromJson(JsonArray fields)
    {
        var next = PartitionSpec.PartitionDataIdStart;
        foreach (var f in fields)
        {
            var sourceIds = f!["source-ids"]?.AsArray().Select(x => x!.GetValue<int>()).ToList();
            var sourceId = f["source-id"]?.GetValue<int>() ?? sourceIds![0];
            var fieldId = f["field-id"]?.GetValue<int>() ?? next;
            next = fieldId + 1;
            yield return new PartitionField(sourceId, fieldId, f["name"]!.GetValue<string>(), Transform.Parse(f["transform"]!.GetValue<string>()), sourceIds);
        }
    }

    public static JsonObject SortOrderToJson(SortOrder order) => new()
    {
        ["order-id"] = order.OrderId,
        ["fields"] = new JsonArray(order.Fields.Select(f => (JsonNode)new JsonObject
        {
            ["transform"] = f.Transform.ToString(),
            ["source-id"] = f.SourceId,
            ["direction"] = f.Direction == SortDirection.Asc ? "asc" : "desc",
            ["null-order"] = f.NullOrder == NullOrder.NullsFirst ? "nulls-first" : "nulls-last",
        }).ToArray()),
    };

    public static SortOrder SortOrderFromJson(JsonNode n) => new(
        n["order-id"]!.GetValue<int>(),
        n["fields"]!.AsArray().Select(f => new SortField(
            Transform.Parse(f!["transform"]!.GetValue<string>()),
            f["source-id"]?.GetValue<int>() ?? f["source-ids"]!.AsArray()[0]!.GetValue<int>(),
            f["direction"]!.GetValue<string>() == "asc" ? SortDirection.Asc : SortDirection.Desc,
            f["null-order"]!.GetValue<string>() == "nulls-first" ? NullOrder.NullsFirst : NullOrder.NullsLast)));
}
