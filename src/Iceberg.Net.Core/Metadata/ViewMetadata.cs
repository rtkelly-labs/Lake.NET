using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Iceberg.Net.Types;

namespace Iceberg.Net.Metadata;

/// <summary>A view definition in one SQL dialect (view spec "Representations").</summary>
public sealed record ViewRepresentation(string Sql, string Dialect)
{
    public string Type => "sql";
}

/// <summary>One version of a view (view spec "Versions").</summary>
public sealed record ViewVersion(
    int VersionId,
    long TimestampMs,
    int SchemaId,
    IReadOnlyDictionary<string, string> Summary,
    IReadOnlyList<ViewRepresentation> Representations,
    IReadOnlyList<string> DefaultNamespace,
    string? DefaultCatalog = null)
{
    public ViewRepresentation? For(string dialect) =>
        Representations.FirstOrDefault(r => string.Equals(r.Dialect, dialect, StringComparison.OrdinalIgnoreCase));
}

public sealed record ViewLogEntry(long TimestampMs, int VersionId);

/// <summary>
/// View metadata (Iceberg view spec, format version 1): the view's schemas, its versions (SQL representations per
/// dialect, default catalog/namespace) and the history of which version was current.
/// </summary>
public sealed record ViewMetadata
{
    public const int FormatVersion = 1;

    public required Guid ViewUuid { get; init; }
    public required string Location { get; init; }
    public required IReadOnlyList<Schema> Schemas { get; init; }
    public required int CurrentVersionId { get; init; }
    public required IReadOnlyList<ViewVersion> Versions { get; init; }
    public required IReadOnlyList<ViewLogEntry> VersionLog { get; init; }
    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();
    public string? MetadataFileLocation { get; init; }

    public ViewVersion CurrentVersion => Versions.First(v => v.VersionId == CurrentVersionId);
    public Schema CurrentSchema => Schemas.First(s => s.SchemaId == CurrentVersion.SchemaId);

    /// <summary>A new view with its first version.</summary>
    public static ViewMetadata Create(string location, Schema schema, ViewVersion version, IReadOnlyDictionary<string, string>? properties, long nowMs) => new()
    {
        ViewUuid = Guid.NewGuid(),
        Location = location.TrimEnd('/'),
        Schemas = [schema.WithId(0)],
        CurrentVersionId = 1,
        Versions = [version with { VersionId = 1, SchemaId = 0, TimestampMs = nowMs }],
        VersionLog = [new ViewLogEntry(nowMs, 1)],
        Properties = properties ?? new Dictionary<string, string>(),
    };

    /// <summary>
    /// Replace the definition: a new version (reusing an identical schema) becomes current. Older versions stay, as the
    /// spec intends (they are expired by <c>version.history.num-entries</c>, default 10).
    /// </summary>
    public ViewMetadata Replace(Schema schema, ViewVersion version, long nowMs)
    {
        var existing = Schemas.FirstOrDefault(s => s.SameSchema(schema));
        var schemas = existing is null ? [.. Schemas, schema.WithId(Schemas.Max(s => s.SchemaId) + 1)] : Schemas;
        var schemaId = existing?.SchemaId ?? schemas[^1].SchemaId;
        var id = Versions.Max(v => v.VersionId) + 1;
        var keep = int.TryParse(Properties.GetValueOrDefault("version.history.num-entries"), out var n) ? Math.Max(1, n) : 10;
        var versions = Versions.Append(version with { VersionId = id, SchemaId = schemaId, TimestampMs = nowMs }).TakeLast(keep).ToList();
        return this with
        {
            Schemas = schemas.Where(s => versions.Any(v => v.SchemaId == s.SchemaId)).ToList(),
            Versions = versions,
            CurrentVersionId = id,
            VersionLog = [.. VersionLog, new ViewLogEntry(nowMs, id)],
            MetadataFileLocation = null,
        };
    }
}

/// <summary>View metadata JSON (view spec "View Metadata").</summary>
public static class ViewJson
{
    public static string Write(ViewMetadata m) => ToJson(m).ToJsonString();

    public static ViewMetadata Read(string json, string? location = null) =>
        Corruption.Guard("view metadata", () => FromJson(JsonNode.Parse(json) ?? throw new InvalidDataException("View metadata is JSON null")) with { MetadataFileLocation = location });

    public static JsonObject ToJson(ViewMetadata m) => new()
    {
        ["view-uuid"] = m.ViewUuid.ToString(),
        ["format-version"] = ViewMetadata.FormatVersion,
        ["location"] = m.Location,
        ["schemas"] = new JsonArray(m.Schemas.Select(s => (JsonNode)TypeJson.SchemaToJson(s)).ToArray()),
        ["current-version-id"] = m.CurrentVersionId,
        ["versions"] = new JsonArray(m.Versions.Select(v => (JsonNode)VersionToJson(v)).ToArray()),
        ["version-log"] = new JsonArray(m.VersionLog.Select(e => (JsonNode)new JsonObject { ["timestamp-ms"] = e.TimestampMs, ["version-id"] = e.VersionId }).ToArray()),
        ["properties"] = new JsonObject(m.Properties.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))),
    };

    public static JsonObject VersionToJson(ViewVersion v)
    {
        var o = new JsonObject
        {
            ["version-id"] = v.VersionId,
            ["timestamp-ms"] = v.TimestampMs,
            ["schema-id"] = v.SchemaId,
            ["summary"] = new JsonObject(v.Summary.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value))),
            ["representations"] = new JsonArray(v.Representations.Select(r => (JsonNode)new JsonObject { ["type"] = r.Type, ["sql"] = r.Sql, ["dialect"] = r.Dialect }).ToArray()),
            ["default-namespace"] = new JsonArray(v.DefaultNamespace.Select(n => (JsonNode)n).ToArray()),
        };
        if (v.DefaultCatalog is not null) o["default-catalog"] = v.DefaultCatalog;
        return o;
    }

    public static ViewMetadata FromJson(JsonNode n)
    {
        var version = n["format-version"]!.GetValue<int>();
        if (version != ViewMetadata.FormatVersion) throw new NotSupportedException($"Unsupported view format version {version}");
        foreach (var field in new[] { "view-uuid", "location", "schemas", "current-version-id", "versions", "version-log" })
            if (n[field] is null) throw new InvalidDataException($"View metadata is missing required field '{field}'");
        return new ViewMetadata
        {
            ViewUuid = Guid.Parse(n["view-uuid"]!.GetValue<string>()),
            Location = n["location"]!.GetValue<string>(),
            Schemas = n["schemas"]!.AsArray().Select(s => TypeJson.SchemaFromJson(s!)).ToList(),
            CurrentVersionId = n["current-version-id"]!.GetValue<int>(),
            Versions = n["versions"]!.AsArray().Select(v => VersionFromJson(v!)).ToList(),
            VersionLog = n["version-log"]!.AsArray().Select(e => new ViewLogEntry(e!["timestamp-ms"]!.GetValue<long>(), e["version-id"]!.GetValue<int>())).ToList(),
            Properties = MetadataJson.StringMapFrom(n["properties"]),
        };
    }

    public static ViewVersion VersionFromJson(JsonNode v) => new(
        v["version-id"]!.GetValue<int>(),
        v["timestamp-ms"]!.GetValue<long>(),
        v["schema-id"]!.GetValue<int>(),
        MetadataJson.StringMapFrom(v["summary"]),
        v["representations"]!.AsArray()
            .Where(r => r!["type"]?.GetValue<string>() == "sql") // unknown representation types are skipped, per spec
            .Select(r => new ViewRepresentation(r!["sql"]!.GetValue<string>(), r["dialect"]!.GetValue<string>())).ToList(),
        v["default-namespace"]!.AsArray().Select(x => x!.GetValue<string>()).ToList(),
        v["default-catalog"]?.GetValue<string>());
}
