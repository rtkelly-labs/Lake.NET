using System.Text.Json.Nodes;
using Iceberg.Net.Metadata;
using U = Iceberg.Net.Metadata.MetadataUpdate;
using R = Iceberg.Net.Metadata.UpdateRequirement;

namespace Iceberg.Net.Json;

/// <summary>REST catalog <c>TableUpdate</c> / <c>TableRequirement</c> JSON (open-api/rest-catalog-open-api.yaml).</summary>
public static class UpdateJson
{
    public static JsonObject ToJson(MetadataUpdate update)
    {
        var o = new JsonObject { ["action"] = update.Action };
        switch (update)
        {
            case U.AssignUuid u: o["uuid"] = u.Uuid.ToString(); break;
            case U.UpgradeFormatVersion u: o["format-version"] = u.FormatVersion; break;
            case U.AddSchema u: o["schema"] = TypeJson.SchemaToJson(u.Schema); break;
            case U.SetCurrentSchema u: o["schema-id"] = u.SchemaId; break;
            case U.AddPartitionSpec u: o["spec"] = TypeJson.SpecToJson(u.Spec); break;
            case U.SetDefaultPartitionSpec u: o["spec-id"] = u.SpecId; break;
            case U.AddSortOrder u: o["sort-order"] = TypeJson.SortOrderToJson(u.SortOrder); break;
            case U.SetDefaultSortOrder u: o["sort-order-id"] = u.SortOrderId; break;
            case U.AddSnapshot u: o["snapshot"] = MetadataJson.SnapshotToJson(u.Snapshot); break;
            case U.SetSnapshotRef u:
                o["ref-name"] = u.RefName;
                foreach (var (k, v) in MetadataJson.RefToJson(u.Ref)) o[k] = v!.DeepClone();
                break;
            case U.RemoveSnapshotRef u: o["ref-name"] = u.RefName; break;
            case U.RemoveSnapshots u: o["snapshot-ids"] = new JsonArray(u.SnapshotIds.Select(i => (JsonNode)i).ToArray()); break;
            case U.SetLocation u: o["location"] = u.Location; break;
            case U.SetProperties u: o["updates"] = MetadataJson.StringMap(u.Updates); break;
            case U.RemoveProperties u: o["removals"] = new JsonArray(u.Removals.Select(r => (JsonNode)r).ToArray()); break;
            case U.SetStatistics u:
                o["snapshot-id"] = u.Statistics.SnapshotId; // deprecated duplicate, still expected by older servers
                o["statistics"] = MetadataJson.StatisticsToJson(u.Statistics);
                break;
            case U.RemoveStatistics u: o["snapshot-id"] = u.SnapshotId; break;
            case U.RemovePartitionSpecs u: o["spec-ids"] = new JsonArray(u.SpecIds.Select(i => (JsonNode)i).ToArray()); break;
            case U.RemoveSchemas u: o["schema-ids"] = new JsonArray(u.SchemaIds.Select(i => (JsonNode)i).ToArray()); break;
            case U.AddEncryptionKey u: o["encryption-key"] = MetadataJson.EncryptionKeyToJson(u.Key); break;
            case U.RemoveEncryptionKey u: o["key-id"] = u.KeyId; break;
            default: throw new NotSupportedException(update.Action);
        }
        return o;
    }

    public static MetadataUpdate UpdateFromJson(JsonNode n) => n["action"]!.GetValue<string>() switch
    {
        "assign-uuid" => new U.AssignUuid(Guid.Parse(n["uuid"]!.GetValue<string>())),
        "upgrade-format-version" => new U.UpgradeFormatVersion(n["format-version"]!.GetValue<int>()),
        "add-schema" => new U.AddSchema(TypeJson.SchemaFromJson(n["schema"]!)),
        "set-current-schema" => new U.SetCurrentSchema(n["schema-id"]!.GetValue<int>()),
        "add-spec" => new U.AddPartitionSpec(TypeJson.SpecFromJson(n["spec"]!)),
        "set-default-spec" => new U.SetDefaultPartitionSpec(n["spec-id"]!.GetValue<int>()),
        "add-sort-order" => new U.AddSortOrder(TypeJson.SortOrderFromJson(n["sort-order"]!)),
        "set-default-sort-order" => new U.SetDefaultSortOrder(n["sort-order-id"]!.GetValue<int>()),
        "add-snapshot" => new U.AddSnapshot(MetadataJson.SnapshotFromJson(n["snapshot"]!)),
        "set-snapshot-ref" => new U.SetSnapshotRef(n["ref-name"]!.GetValue<string>(), MetadataJson.RefFromJson(n)),
        "remove-snapshot-ref" => new U.RemoveSnapshotRef(n["ref-name"]!.GetValue<string>()),
        "remove-snapshots" => new U.RemoveSnapshots(n["snapshot-ids"]!.AsArray().Select(i => i!.GetValue<long>()).ToList()),
        "set-location" => new U.SetLocation(n["location"]!.GetValue<string>()),
        "set-properties" => new U.SetProperties(MetadataJson.StringMapFrom(n["updates"])),
        "remove-properties" => new U.RemoveProperties(n["removals"]!.AsArray().Select(r => r!.GetValue<string>()).ToList()),
        "set-statistics" => new U.SetStatistics(MetadataJson.StatisticsFromJson(n["statistics"]!)),
        "remove-statistics" => new U.RemoveStatistics(n["snapshot-id"]!.GetValue<long>()),
        "remove-partition-specs" => new U.RemovePartitionSpecs(n["spec-ids"]!.AsArray().Select(i => i!.GetValue<int>()).ToList()),
        "remove-schemas" => new U.RemoveSchemas(n["schema-ids"]!.AsArray().Select(i => i!.GetValue<int>()).ToList()),
        "add-encryption-key" => new U.AddEncryptionKey(MetadataJson.EncryptionKeyFromJson(n["encryption-key"]!)),
        "remove-encryption-key" => new U.RemoveEncryptionKey(n["key-id"]!.GetValue<string>()),
        var a => throw new NotSupportedException($"Unknown table update '{a}'"),
    };

    public static JsonObject ToJson(UpdateRequirement r)
    {
        var o = new JsonObject { ["type"] = r.Type };
        switch (r)
        {
            case R.AssertCreate: break;
            case R.AssertTableUuid x: o["uuid"] = x.Uuid.ToString(); break;
            case R.AssertRefSnapshotId x: o["ref"] = x.Ref; o["snapshot-id"] = x.SnapshotId; break;
            case R.AssertLastAssignedFieldId x: o["last-assigned-field-id"] = x.LastAssignedFieldId; break;
            case R.AssertCurrentSchemaId x: o["current-schema-id"] = x.CurrentSchemaId; break;
            case R.AssertLastAssignedPartitionId x: o["last-assigned-partition-id"] = x.LastAssignedPartitionId; break;
            case R.AssertDefaultSpecId x: o["default-spec-id"] = x.DefaultSpecId; break;
            case R.AssertDefaultSortOrderId x: o["default-sort-order-id"] = x.DefaultSortOrderId; break;
            default: throw new NotSupportedException(r.Type);
        }
        return o;
    }

    public static UpdateRequirement RequirementFromJson(JsonNode n) => n["type"]!.GetValue<string>() switch
    {
        "assert-create" => new R.AssertCreate(),
        "assert-table-uuid" => new R.AssertTableUuid(Guid.Parse(n["uuid"]!.GetValue<string>())),
        "assert-ref-snapshot-id" => new R.AssertRefSnapshotId(n["ref"]!.GetValue<string>(), n["snapshot-id"]?.GetValue<long>()),
        "assert-last-assigned-field-id" => new R.AssertLastAssignedFieldId(n["last-assigned-field-id"]!.GetValue<int>()),
        "assert-current-schema-id" => new R.AssertCurrentSchemaId(n["current-schema-id"]!.GetValue<int>()),
        "assert-last-assigned-partition-id" => new R.AssertLastAssignedPartitionId(n["last-assigned-partition-id"]!.GetValue<int>()),
        "assert-default-spec-id" => new R.AssertDefaultSpecId(n["default-spec-id"]!.GetValue<int>()),
        "assert-default-sort-order-id" => new R.AssertDefaultSortOrderId(n["default-sort-order-id"]!.GetValue<int>()),
        var t => throw new NotSupportedException($"Unknown requirement '{t}'"),
    };
}
