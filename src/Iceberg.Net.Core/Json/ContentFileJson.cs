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
    /// <summary>
    /// The spec form of <paramref name="file"/>: <c>content</c> as <c>data</c> / <c>position-deletes</c> /
    /// <c>equality-deletes</c>, <c>partition</c> as an array in spec order (single-value form, typed by
    /// <paramref name="spec"/> over <paramref name="schema"/>), lower-case <c>file-format</c>, metric maps as
    /// <c>{"keys","values"}</c> and bounds as hex. <paramref name="statsFields"/> limits the column metrics written
    /// (null: all; empty: none), as servers trim data-file stats to the <c>stats-fields</c> a planning request asks for.
    /// </summary>
    public static JsonObject ToJson(DataFile file, PartitionSpec spec, Schema schema, ISet<int>? statsFields = null)
    {
        var o = new JsonObject
        {
            ["spec-id"] = file.SpecId,
            ["content"] = file.Content switch
            {
                FileContent.Data => "data",
                FileContent.PositionDeletes => "position-deletes",
                FileContent.EqualityDeletes => "equality-deletes",
                var c => throw new ArgumentException($"Unknown content {c}"),
            },
            ["file-path"] = file.FilePath,
            ["file-format"] = file.FileFormat.ToLowerInvariant(),
        };
        var partitionType = spec.PartitionType(schema);
        if (file.Partition.Count != spec.Fields.Count)
            throw new ArgumentException($"{file.FilePath} has {file.Partition.Count} partition values; spec {spec.SpecId} has {spec.Fields.Count} fields");
        o["partition"] = new JsonArray(file.Partition.Select((v, i) => SingleValueJson.ToJson(partitionType.Fields[i].Type, v)).ToArray());
        o["file-size-in-bytes"] = file.FileSizeInBytes;
        o["record-count"] = file.RecordCount;
        Put(o, "column-sizes", file.ColumnSizes, statsFields, v => JsonValue.Create(v));
        Put(o, "value-counts", file.ValueCounts, statsFields, v => JsonValue.Create(v));
        Put(o, "null-value-counts", file.NullValueCounts, statsFields, v => JsonValue.Create(v));
        Put(o, "nan-value-counts", file.NanValueCounts, statsFields, v => JsonValue.Create(v));
        Put(o, "lower-bounds", file.LowerBounds, statsFields, v => JsonValue.Create(Compat.ToHex(v)));
        Put(o, "upper-bounds", file.UpperBounds, statsFields, v => JsonValue.Create(Compat.ToHex(v)));
        if (file.KeyMetadata is { } key) o["key-metadata"] = Compat.ToHex(key);
        if (file.SplitOffsets is { } splits) o["split-offsets"] = new JsonArray(splits.Select(x => (JsonNode)x).ToArray());
        if (file.EqualityIds is { } eq) o["equality-ids"] = new JsonArray(eq.Select(x => (JsonNode)x).ToArray());
        if (file.SortOrderId is { } sort) o["sort-order-id"] = sort;
        if (file.FirstRowId is { } first) o["first-row-id"] = first;
        if (file.ReferencedDataFile is { } referenced) o["referenced-data-file"] = referenced;
        if (file.ContentOffset is { } offset) o["content-offset"] = offset;
        if (file.ContentSizeInBytes is { } size) o["content-size-in-bytes"] = size;
        return o;
    }

    private static void Put<T>(JsonObject o, string key, IReadOnlyDictionary<int, T>? map, ISet<int>? only, Func<T, JsonNode?> value)
    {
        if (map is null) return;
        var keys = map.Keys.Where(k => only is null || only.Contains(k)).OrderBy(k => k).ToList();
        if (keys.Count == 0) return;
        o[key] = new JsonObject
        {
            ["keys"] = new JsonArray(keys.Select(k => (JsonNode)k).ToArray()),
            ["values"] = new JsonArray(keys.Select(k => value(map[k])).ToArray()),
        };
    }

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
