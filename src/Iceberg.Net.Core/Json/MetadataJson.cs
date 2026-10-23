using System.Text.Json;
using System.Text.Json.Nodes;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;

namespace Iceberg.Net.Json;

/// <summary>Table metadata file JSON ("Table Metadata Fields" in the spec), covering v1, v2 and v3.</summary>
public static class MetadataJson
{
    private static readonly JsonSerializerOptions Pretty = new() { WriteIndented = true };

    public static string Write(TableMetadata m, bool indented = false) => ToJson(m).ToJsonString(indented ? Pretty : null);

    public static TableMetadata Read(string json, string? location = null) =>
        Corruption.Guard("table metadata", () => FromJson(JsonNode.Parse(json) ?? throw new InvalidDataException("Table metadata is JSON null")) with { MetadataFileLocation = location });

    public static JsonObject ToJson(TableMetadata m)
    {
        var o = new JsonObject
        {
            ["format-version"] = m.FormatVersion,
            ["table-uuid"] = m.TableUuid.ToString(),
            ["location"] = m.Location,
        };
        if (m.FormatVersion > 1) o["last-sequence-number"] = m.LastSequenceNumber;
        o["last-updated-ms"] = m.LastUpdatedMs;
        o["last-column-id"] = m.LastColumnId;
        if (m.FormatVersion == 1) o["schema"] = TypeJson.SchemaToJson(m.CurrentSchema); // legacy field, still required for v1 readers
        o["current-schema-id"] = m.CurrentSchemaId;
        o["schemas"] = Array(m.Schemas, TypeJson.SchemaToJson);
        if (m.FormatVersion == 1) o["partition-spec"] = TypeJson.SpecFieldsToJson(m.DefaultSpec);
        o["default-spec-id"] = m.DefaultSpecId;
        o["partition-specs"] = Array(m.PartitionSpecs, TypeJson.SpecToJson);
        o["last-partition-id"] = m.LastPartitionId;
        o["default-sort-order-id"] = m.DefaultSortOrderId;
        o["sort-orders"] = Array(m.SortOrders, TypeJson.SortOrderToJson);
        o["properties"] = StringMap(m.Properties);
        // v1/v2 readers expect -1 for "no snapshot"; Java writes null for v3+ (since 1.9, apache/iceberg#12335).
        o["current-snapshot-id"] = m.CurrentSnapshotId is { } current ? current : m.FormatVersion >= 3 ? null : -1;
        o["refs"] = new JsonObject(m.Refs.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)RefToJson(kv.Value))));
        o["snapshots"] = Array(m.Snapshots, s => SnapshotToJson(s, m.FormatVersion));
        o["statistics"] = Array(m.Statistics, StatisticsToJson);
        o["partition-statistics"] = Array(m.PartitionStatistics, p => new JsonObject
        {
            ["snapshot-id"] = p.SnapshotId,
            ["statistics-path"] = p.StatisticsPath,
            ["file-size-in-bytes"] = p.FileSizeInBytes,
        });
        o["snapshot-log"] = Array(m.SnapshotLog, e => new JsonObject { ["timestamp-ms"] = e.TimestampMs, ["snapshot-id"] = e.SnapshotId });
        o["metadata-log"] = Array(m.MetadataLog, e => new JsonObject { ["timestamp-ms"] = e.TimestampMs, ["metadata-file"] = e.MetadataFile });
        if (m.FormatVersion >= 3)
        {
            o["next-row-id"] = m.NextRowId ?? 0;
            if (m.EncryptionKeys.Count > 0) o["encryption-keys"] = Array(m.EncryptionKeys, EncryptionKeyToJson);
        }
        return o;
    }

    private static readonly string[] V2Required =
    [
        "table-uuid", "location", "last-sequence-number", "last-updated-ms", "last-column-id", "schemas", "current-schema-id",
        "partition-specs", "default-spec-id", "last-partition-id", "sort-orders", "default-sort-order-id",
    ];

    private static readonly string[] V3Required = [.. V2Required, "next-row-id"];

    public static TableMetadata FromJson(JsonNode n)
    {
        var formatVersion = n["format-version"]!.GetValue<int>();
        if (formatVersion is < 1 or > TableMetadata.SupportedFormatVersion)
            throw new NotSupportedException($"Unsupported table format version {formatVersion}");
        // v1 fallbacks (schema, partition-spec, implied ids) are only legal in v1; from v2 the spec requires these.
        foreach (var field in formatVersion >= 3 ? V3Required : formatVersion == 2 ? V2Required : [])
            if (n[field] is null) throw new InvalidDataException($"Format v{formatVersion} table metadata is missing required field '{field}'");

        var schemas = n["schemas"] is JsonArray sa
            ? sa.Select(s => TypeJson.SchemaFromJson(s!)).ToList()
            : [TypeJson.SchemaFromJson(n["schema"]!)];
        var currentSchemaId = n["current-schema-id"]?.GetValue<int>() ?? schemas[0].SchemaId;

        var specs = n["partition-specs"] is JsonArray pa
            ? pa.Select(p => TypeJson.SpecFromJson(p!)).ToList()
            : [new PartitionSpec(0, TypeJson.SpecFieldsFromJson(n["partition-spec"]!.AsArray()))];
        var defaultSpecId = n["default-spec-id"]?.GetValue<int>() ?? specs[0].SpecId;
        var lastPartitionId = n["last-partition-id"]?.GetValue<int>() ?? specs.Max(s => s.LastAssignedFieldId);

        var sortOrders = n["sort-orders"] is JsonArray so ? so.Select(o => TypeJson.SortOrderFromJson(o!)).ToList() : [SortOrder.Unsorted];
        var currentSnapshotId = n["current-snapshot-id"]?.GetValue<long>() is { } cs and not -1 ? cs : (long?)null;

        var refs = n["refs"]?.AsObject().ToDictionary(kv => kv.Key, kv => RefFromJson(kv.Value!)) ?? [];
        // Spec: if refs is absent, main is implied by current-snapshot-id.
        if (n["refs"] is null && currentSnapshotId is { } cur) refs[SnapshotRef.Main] = SnapshotRef.Branch(cur);

        return new TableMetadata
        {
            FormatVersion = formatVersion,
            TableUuid = n["table-uuid"] is { } u ? Guid.Parse(u.GetValue<string>()) : Guid.Empty,
            Location = n["location"]!.GetValue<string>(),
            LastSequenceNumber = n["last-sequence-number"]?.GetValue<long>() ?? 0,
            LastUpdatedMs = n["last-updated-ms"]!.GetValue<long>(),
            LastColumnId = n["last-column-id"]!.GetValue<int>(),
            Schemas = schemas,
            CurrentSchemaId = currentSchemaId,
            PartitionSpecs = specs,
            DefaultSpecId = defaultSpecId,
            LastPartitionId = lastPartitionId,
            SortOrders = sortOrders,
            DefaultSortOrderId = n["default-sort-order-id"]?.GetValue<int>() ?? sortOrders[0].OrderId,
            Properties = StringMapFrom(n["properties"]),
            CurrentSnapshotId = currentSnapshotId,
            Snapshots = n["snapshots"]?.AsArray().Select(s => SnapshotFromJson(s!)).ToList() ?? [],
            SnapshotLog = n["snapshot-log"]?.AsArray().Select(e => new SnapshotLogEntry(e!["timestamp-ms"]!.GetValue<long>(), e["snapshot-id"]!.GetValue<long>())).ToList() ?? [],
            MetadataLog = n["metadata-log"]?.AsArray().Select(e => new MetadataLogEntry(e!["timestamp-ms"]!.GetValue<long>(), e["metadata-file"]!.GetValue<string>())).ToList() ?? [],
            Refs = refs,
            Statistics = n["statistics"]?.AsArray().Select(s => StatisticsFromJson(s!)).ToList() ?? [],
            PartitionStatistics = n["partition-statistics"]?.AsArray().Select(p => new PartitionStatisticsFile(
                p!["snapshot-id"]!.GetValue<long>(), p["statistics-path"]!.GetValue<string>(), p["file-size-in-bytes"]!.GetValue<long>())).ToList() ?? [],
            NextRowId = formatVersion >= 3 ? n["next-row-id"]?.GetValue<long>() ?? 0 : null,
            EncryptionKeys = n["encryption-keys"]?.AsArray().Select(k => EncryptionKeyFromJson(k!)).ToList() ?? [],
        };
    }

    public static JsonObject SnapshotToJson(Snapshot s, int formatVersion = 2)
    {
        var o = new JsonObject { ["snapshot-id"] = s.SnapshotId };
        if (s.ParentSnapshotId is { } p) o["parent-snapshot-id"] = p;
        if (formatVersion > 1) o["sequence-number"] = s.SequenceNumber;
        o["timestamp-ms"] = s.TimestampMs;
        if (s.ManifestList is not null) o["manifest-list"] = s.ManifestList;
        if (s.Manifests is not null) o["manifests"] = new JsonArray(s.Manifests.Select(x => (JsonNode)x).ToArray());
        o["summary"] = StringMap(s.Summary);
        if (s.SchemaId is { } sid) o["schema-id"] = sid;
        if (s.FirstRowId is { } fr) o["first-row-id"] = fr;
        if (s.AddedRows is { } ar) o["added-rows"] = ar;
        if (s.KeyId is not null) o["key-id"] = s.KeyId;
        return o;
    }

    public static Snapshot SnapshotFromJson(JsonNode n) => new()
    {
        SnapshotId = n["snapshot-id"]!.GetValue<long>(),
        ParentSnapshotId = n["parent-snapshot-id"]?.GetValue<long>(),
        SequenceNumber = n["sequence-number"]?.GetValue<long>() ?? 0,
        TimestampMs = n["timestamp-ms"]!.GetValue<long>(),
        ManifestList = n["manifest-list"]?.GetValue<string>(),
        Manifests = n["manifests"]?.AsArray().Select(x => x!.GetValue<string>()).ToList(),
        Summary = StringMapFrom(n["summary"]),
        SchemaId = n["schema-id"]?.GetValue<int>(),
        FirstRowId = n["first-row-id"]?.GetValue<long>(),
        AddedRows = n["added-rows"]?.GetValue<long>(),
        KeyId = n["key-id"]?.GetValue<string>(),
    };

    public static JsonObject RefToJson(SnapshotRef r)
    {
        var o = new JsonObject { ["snapshot-id"] = r.SnapshotId, ["type"] = r.Type == SnapshotRefType.Branch ? "branch" : "tag" };
        if (r.MinSnapshotsToKeep is { } a) o["min-snapshots-to-keep"] = a;
        if (r.MaxSnapshotAgeMs is { } b) o["max-snapshot-age-ms"] = b;
        if (r.MaxRefAgeMs is { } c) o["max-ref-age-ms"] = c;
        return o;
    }

    public static SnapshotRef RefFromJson(JsonNode n) => new(
        n["snapshot-id"]!.GetValue<long>(),
        n["type"]!.GetValue<string>() == "tag" ? SnapshotRefType.Tag : SnapshotRefType.Branch,
        n["min-snapshots-to-keep"]?.GetValue<int>(),
        n["max-snapshot-age-ms"]?.GetValue<long>(),
        n["max-ref-age-ms"]?.GetValue<long>());

    public static JsonObject StatisticsToJson(StatisticsFile s)
    {
        var o = new JsonObject
        {
            ["snapshot-id"] = s.SnapshotId,
            ["statistics-path"] = s.StatisticsPath,
            ["file-size-in-bytes"] = s.FileSizeInBytes,
            ["file-footer-size-in-bytes"] = s.FileFooterSizeInBytes,
            ["blob-metadata"] = Array(s.BlobMetadata, b =>
            {
                var bo = new JsonObject
                {
                    ["type"] = b.Type,
                    ["snapshot-id"] = b.SnapshotId,
                    ["sequence-number"] = b.SequenceNumber,
                    ["fields"] = new JsonArray(b.Fields.Select(f => (JsonNode)f).ToArray()),
                };
                if (b.Properties is not null) bo["properties"] = StringMap(b.Properties);
                return bo;
            }),
        };
        if (s.KeyMetadata is not null) o["key-metadata"] = s.KeyMetadata;
        return o;
    }

    public static StatisticsFile StatisticsFromJson(JsonNode n) => new(
        n["snapshot-id"]!.GetValue<long>(),
        n["statistics-path"]!.GetValue<string>(),
        n["file-size-in-bytes"]!.GetValue<long>(),
        n["file-footer-size-in-bytes"]!.GetValue<long>(),
        n["blob-metadata"]?.AsArray().Select(b => new BlobMetadata(
            b!["type"]!.GetValue<string>(),
            b["snapshot-id"]!.GetValue<long>(),
            b["sequence-number"]!.GetValue<long>(),
            b["fields"]!.AsArray().Select(f => f!.GetValue<int>()).ToList(),
            b["properties"] is { } p ? StringMapFrom(p) : null)).ToList() ?? [],
        n["key-metadata"]?.GetValue<string>());

    public static JsonObject EncryptionKeyToJson(EncryptedKey k)
    {
        var o = new JsonObject { ["key-id"] = k.KeyId, ["encrypted-key-metadata"] = k.EncryptedKeyMetadata };
        if (k.EncryptedById is not null) o["encrypted-by-id"] = k.EncryptedById;
        if (k.Properties is not null) o["properties"] = StringMap(k.Properties);
        return o;
    }

    public static EncryptedKey EncryptionKeyFromJson(JsonNode n) => new(
        n["key-id"]!.GetValue<string>(),
        n["encrypted-key-metadata"]!.GetValue<string>(),
        n["encrypted-by-id"]?.GetValue<string>(),
        n["properties"] is { } p ? StringMapFrom(p) : null);

    public static JsonArray Array<T>(IEnumerable<T> items, Func<T, JsonNode> f) => new(items.Select(i => (JsonNode?)f(i)).ToArray());

    public static JsonObject StringMap(IReadOnlyDictionary<string, string> map) =>
        new(map.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)JsonValue.Create(kv.Value))));

    public static Dictionary<string, string> StringMapFrom(JsonNode? n) =>
        n?.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value is JsonValue v && v.TryGetValue<string>(out var s) ? s : kv.Value?.ToJsonString() ?? "") ?? [];
}
