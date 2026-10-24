using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer;

/// <summary>REST <c>ViewRequirement</c>: only <c>assert-view-uuid</c> exists.</summary>
public sealed record ViewRequirement(Guid Uuid)
{
    public void Validate(ViewMetadata current)
    {
        if (current.ViewUuid != Uuid) throw new CommitFailedException($"Requirement failed: view UUID changed (expected {Uuid}, found {current.ViewUuid})");
    }

    public static ViewRequirement FromJson(JsonNode n) => n["type"]!.GetValue<string>() switch
    {
        "assert-view-uuid" => new ViewRequirement(Guid.Parse(n["uuid"]!.GetValue<string>())),
        var t => throw new BadRequestException($"Unknown view requirement '{t}'"),
    };
}

/// <summary>
/// REST <c>ViewUpdate</c>s applied to view metadata, following Java's <c>ViewMetadata.Builder</c>: an added schema
/// identical to an existing one is reused, an added version identical (but for ID and timestamp) to an existing one is
/// reused, and <c>-1</c> means "the one added in this commit". Old versions are expired past
/// <c>version.history.num-entries</c> (default 10).
/// </summary>
public abstract record ViewUpdate
{
    public sealed record AssignUuid(Guid Uuid) : ViewUpdate;
    public sealed record UpgradeFormatVersion(int FormatVersion) : ViewUpdate;
    public sealed record AddSchema(Schema Schema) : ViewUpdate;
    public sealed record SetLocation(string Location) : ViewUpdate;
    public sealed record SetProperties(IReadOnlyDictionary<string, string> Updates) : ViewUpdate;
    public sealed record RemoveProperties(IReadOnlyList<string> Removals) : ViewUpdate;
    public sealed record AddViewVersion(ViewVersion Version) : ViewUpdate;
    public sealed record SetCurrentViewVersion(int VersionId) : ViewUpdate;

    public static ViewUpdate FromJson(JsonNode n) => n["action"]!.GetValue<string>() switch
    {
        "assign-uuid" => new AssignUuid(Guid.Parse(n["uuid"]!.GetValue<string>())),
        "upgrade-format-version" => new UpgradeFormatVersion(n["format-version"]!.GetValue<int>()),
        "add-schema" => new AddSchema(TypeJson.SchemaFromJson(n["schema"]!)),
        "set-location" => new SetLocation(n["location"]!.GetValue<string>()),
        "set-properties" => new SetProperties(MetadataJson.StringMapFrom(n["updates"])),
        "remove-properties" => new RemoveProperties(n["removals"]!.AsArray().Select(r => r!.GetValue<string>()).ToList()),
        "add-view-version" => new AddViewVersion(ViewJson.VersionFromJson(n["view-version"]!)),
        "set-current-view-version" => new SetCurrentViewVersion(n["view-version-id"]!.GetValue<int>()),
        var a => throw new BadRequestException($"Unsupported view update '{a}'"),
    };

    public static ViewMetadata ApplyAll(ViewMetadata m, IEnumerable<ViewUpdate> updates, long nowMs)
    {
        int? lastSchema = null, lastVersion = null;
        var log = m.VersionLog.ToList();
        foreach (var update in updates)
        {
            switch (update)
            {
                case AssignUuid u:
                    if (u.Uuid != m.ViewUuid) throw new ArgumentException("Cannot reassign the view UUID");
                    break;
                case UpgradeFormatVersion u:
                    if (u.FormatVersion != ViewMetadata.FormatVersion) throw new ArgumentException($"Unsupported view format version {u.FormatVersion}");
                    break;
                case AddSchema u:
                {
                    var existing = m.Schemas.FirstOrDefault(s => s.SameSchema(u.Schema));
                    if (existing is not null) { lastSchema = existing.SchemaId; break; }
                    var id = m.Schemas.Count == 0 ? 0 : m.Schemas.Max(s => s.SchemaId) + 1;
                    m = m with { Schemas = [.. m.Schemas, u.Schema.WithId(id)] };
                    lastSchema = id;
                    break;
                }
                case SetLocation u:
                    m = m with { Location = u.Location.TrimEnd('/') };
                    break;
                case SetProperties u:
                {
                    var props = new Dictionary<string, string>(m.Properties);
                    foreach (var (k, v) in u.Updates) props[k] = v;
                    m = m with { Properties = props };
                    break;
                }
                case RemoveProperties u:
                {
                    var props = new Dictionary<string, string>(m.Properties);
                    foreach (var k in u.Removals) props.Remove(k);
                    m = m with { Properties = props };
                    break;
                }
                case AddViewVersion u:
                {
                    var schemaId = u.Version.SchemaId == -1 ? lastSchema ?? throw new ArgumentException("No schema added in this commit") : u.Version.SchemaId;
                    if (m.Schemas.All(s => s.SchemaId != schemaId)) throw new ArgumentException($"Unknown schema {schemaId}");
                    if (u.Version.Representations.Select(r => r.Dialect.ToLowerInvariant()).Distinct().Count() != u.Version.Representations.Count)
                        throw new ArgumentException("A view version may have one representation per dialect");
                    var candidate = u.Version with { SchemaId = schemaId };
                    var same = m.Versions.FirstOrDefault(v => SameVersion(v, candidate));
                    if (same is not null) { lastVersion = same.VersionId; break; }
                    var id = m.Versions.Count == 0 ? 1 : m.Versions.Max(v => v.VersionId) + 1;
                    m = m with { Versions = [.. m.Versions, candidate with { VersionId = id, TimestampMs = candidate.TimestampMs > 0 ? candidate.TimestampMs : nowMs }] };
                    lastVersion = id;
                    break;
                }
                case SetCurrentViewVersion u:
                {
                    var id = u.VersionId == -1 ? lastVersion ?? throw new ArgumentException("No view version added in this commit") : u.VersionId;
                    if (m.Versions.All(v => v.VersionId != id)) throw new ArgumentException($"Unknown view version {id}");
                    if (id != m.CurrentVersionId) log.Add(new ViewLogEntry(nowMs, id));
                    m = m with { CurrentVersionId = id };
                    break;
                }
            }
        }

        var keep = int.TryParse(m.Properties.GetValueOrDefault("version.history.num-entries"), out var n) ? Math.Max(1, n) : 10;
        var versions = m.Versions.OrderBy(v => v.VersionId).ToList();
        if (versions.Count > keep)
        {
            var current = versions.First(v => v.VersionId == m.CurrentVersionId);
            versions = versions.Where(v => v != current).TakeLast(keep - 1).Append(current).OrderBy(v => v.VersionId).ToList();
        }
        var kept = versions.Select(v => v.VersionId).ToHashSet();
        return m with
        {
            Versions = versions,
            Schemas = m.Schemas.Where(s => versions.Any(v => v.SchemaId == s.SchemaId)).ToList(),
            VersionLog = log.Where(e => kept.Contains(e.VersionId)).ToList(),
            MetadataFileLocation = null,
        };
    }

    private static bool SameVersion(ViewVersion a, ViewVersion b) =>
        a.SchemaId == b.SchemaId
        && a.DefaultCatalog == b.DefaultCatalog
        && a.DefaultNamespace.SequenceEqual(b.DefaultNamespace)
        && a.Representations.SequenceEqual(b.Representations);
}
