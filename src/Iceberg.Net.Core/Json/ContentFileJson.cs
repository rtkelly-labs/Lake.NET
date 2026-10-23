using System.Globalization;
using System.Text.Json.Nodes;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Json;

/// <summary>
/// Data and delete files in the REST spec's JSON form (<c>ContentFile</c>, Java's <c>ContentFileParser</c>), as
/// server-side scan planning returns them. Accepted as the spec writes them and as Java 1.10 does: <c>content</c>
/// as <c>data</c> / <c>position-deletes</c> / <c>equality-deletes</c> or the enum names (<c>POSITION_DELETES</c>);
/// <c>partition</c> as an array in spec order or an object keyed by partition field ID, each value in the
/// single-value form; metric maps as <c>{"keys":[…],"values":[…]}</c>, bounds and key metadata as hex.
/// </summary>
public static class ContentFileJson
{
    public static DataFile FromJson(JsonNode node, Func<int, PartitionSpec> specs, Schema schema) =>
        Corruption.Guard("content file JSON", () => Parse(node.AsObject(), specs, schema));

    private static DataFile Parse(JsonObject o, Func<int, PartitionSpec> specs, Schema schema)
    {
        var specId = o["spec-id"]?.GetValue<int>() ?? throw new InvalidDataException("Content file without 'spec-id'");
        var spec = specs(specId);
        var partitionType = spec.PartitionType(schema);
        return new DataFile
        {
            Content = Content(Required(o, "content").GetValue<string>()),
            FilePath = Required(o, "file-path").GetValue<string>(),
            FileFormat = Required(o, "file-format").GetValue<string>().ToUpperInvariant(),
            SpecId = specId,
            Partition = Partition(o["partition"], spec, partitionType),
            RecordCount = Required(o, "record-count").GetValue<long>(),
            FileSizeInBytes = Required(o, "file-size-in-bytes").GetValue<long>(),
            ColumnSizes = Map(o["column-sizes"], v => v.GetValue<long>()),
            ValueCounts = Map(o["value-counts"], v => v.GetValue<long>()),
            NullValueCounts = Map(o["null-value-counts"], v => v.GetValue<long>()),
            NanValueCounts = Map(o["nan-value-counts"], v => v.GetValue<long>()),
            LowerBounds = Map(o["lower-bounds"], v => Compat.FromHex(v.GetValue<string>())),
            UpperBounds = Map(o["upper-bounds"], v => Compat.FromHex(v.GetValue<string>())),
            KeyMetadata = o["key-metadata"] is { } k ? Compat.FromHex(k.GetValue<string>()) : null,
            SplitOffsets = o["split-offsets"]?.AsArray().Select(x => x!.GetValue<long>()).ToList(),
            EqualityIds = o["equality-ids"]?.AsArray().Select(x => x!.GetValue<int>()).ToList(),
            SortOrderId = o["sort-order-id"]?.GetValue<int>(),
            FirstRowId = o["first-row-id"]?.GetValue<long>(),
            ReferencedDataFile = o["referenced-data-file"]?.GetValue<string>(),
            ContentOffset = o["content-offset"]?.GetValue<long>(),
            ContentSizeInBytes = o["content-size-in-bytes"]?.GetValue<long>(),
        };
    }

    private static JsonNode Required(JsonObject o, string key) => o[key] ?? throw new InvalidDataException($"Content file without '{key}'");

    private static FileContent Content(string text) => text.ToLowerInvariant().Replace('_', '-') switch
    {
        "data" => FileContent.Data,
        "position-deletes" => FileContent.PositionDeletes,
        "equality-deletes" => FileContent.EqualityDeletes,
        _ => throw new InvalidDataException($"Unknown content '{text}'"),
    };

    private static IReadOnlyList<object?> Partition(JsonNode? node, PartitionSpec spec, StructType type)
    {
        if (spec.Fields.Count == 0) return [];
        switch (node)
        {
            case JsonArray a:
                if (a.Count != spec.Fields.Count) throw new InvalidDataException($"Partition has {a.Count} values; spec {spec.SpecId} has {spec.Fields.Count} fields");
                return a.Select((v, i) => SingleValueJson.FromJson(type.Fields[i].Type, v)).ToList();
            case JsonObject byId:
                return spec.Fields.Select((f, i) => SingleValueJson.FromJson(type.Fields[i].Type, byId[f.FieldId.ToString(CultureInfo.InvariantCulture)])).ToList();
            default:
                throw new InvalidDataException($"Partitioned content file without 'partition' (spec {spec.SpecId})");
        }
    }

    private static IReadOnlyDictionary<int, T>? Map<T>(JsonNode? node, Func<JsonNode, T> value)
    {
        if (node is null) return null;
        var keys = node["keys"]?.AsArray() ?? throw new InvalidDataException("Metric map without 'keys'");
        var values = node["values"]?.AsArray() ?? throw new InvalidDataException("Metric map without 'values'");
        if (keys.Count != values.Count) throw new InvalidDataException("Metric map keys and values differ in length");
        var map = new Dictionary<int, T>();
        for (var i = 0; i < keys.Count; i++) map[keys[i]!.GetValue<int>()] = value(values[i]!);
        return map;
    }
}
