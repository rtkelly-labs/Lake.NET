using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Metadata;

/// <summary>
/// A change to table metadata. Names and payloads follow the REST catalog spec's <c>TableUpdate</c>
/// union (the JSON <c>action</c> is <see cref="Action"/>).
/// </summary>
public abstract record MetadataUpdate(string Action)
{
    /// <summary>Used by set-current-schema / set-default-spec / set-default-sort-order to mean "the one added in this commit".</summary>
    public const int LastAdded = -1;

    public sealed record AssignUuid(Guid Uuid) : MetadataUpdate("assign-uuid");
    public sealed record UpgradeFormatVersion(int FormatVersion) : MetadataUpdate("upgrade-format-version");
    public sealed record AddSchema(Schema Schema) : MetadataUpdate("add-schema");
    public sealed record SetCurrentSchema(int SchemaId) : MetadataUpdate("set-current-schema");
    public sealed record AddPartitionSpec(PartitionSpec Spec) : MetadataUpdate("add-spec");
    public sealed record SetDefaultPartitionSpec(int SpecId) : MetadataUpdate("set-default-spec");
    public sealed record AddSortOrder(SortOrder SortOrder) : MetadataUpdate("add-sort-order");
    public sealed record SetDefaultSortOrder(int SortOrderId) : MetadataUpdate("set-default-sort-order");
    public sealed record AddSnapshot(Snapshot Snapshot) : MetadataUpdate("add-snapshot");
    public sealed record SetSnapshotRef(string RefName, SnapshotRef Ref) : MetadataUpdate("set-snapshot-ref");
    public sealed record RemoveSnapshotRef(string RefName) : MetadataUpdate("remove-snapshot-ref");
    public sealed record RemoveSnapshots(IReadOnlyList<long> SnapshotIds) : MetadataUpdate("remove-snapshots");
    public sealed record SetLocation(string Location) : MetadataUpdate("set-location");
    public sealed record SetProperties(IReadOnlyDictionary<string, string> Updates) : MetadataUpdate("set-properties");
    public sealed record RemoveProperties(IReadOnlyList<string> Removals) : MetadataUpdate("remove-properties");
    public sealed record SetStatistics(StatisticsFile Statistics) : MetadataUpdate("set-statistics");
    public sealed record RemoveStatistics(long SnapshotId) : MetadataUpdate("remove-statistics");
    public sealed record RemovePartitionSpecs(IReadOnlyList<int> SpecIds) : MetadataUpdate("remove-partition-specs");
    public sealed record RemoveSchemas(IReadOnlyList<int> SchemaIds) : MetadataUpdate("remove-schemas");
    public sealed record AddEncryptionKey(EncryptedKey Key) : MetadataUpdate("add-encryption-key");
    public sealed record RemoveEncryptionKey(string KeyId) : MetadataUpdate("remove-encryption-key");
}

/// <summary>
/// An optimistic-concurrency precondition checked against the catalog's current metadata before an
/// update is applied (REST <c>TableRequirement</c>). A failed requirement is a <see cref="CommitFailedException"/>:
/// the client should refresh, re-apply its change, and retry.
/// </summary>
public abstract record UpdateRequirement(string Type)
{
    public abstract void Validate(TableMetadata? current);

    protected static TableMetadata Exists(TableMetadata? m) => m ?? throw new CommitFailedException("Requirement failed: table does not exist");

    public sealed record AssertCreate() : UpdateRequirement("assert-create")
    {
        public override void Validate(TableMetadata? current)
        {
            if (current is not null) throw new CommitFailedException("Requirement failed: table already exists");
        }
    }

    public sealed record AssertTableUuid(Guid Uuid) : UpdateRequirement("assert-table-uuid")
    {
        public override void Validate(TableMetadata? current)
        {
            if (Exists(current).TableUuid != Uuid) throw new CommitFailedException($"Requirement failed: table UUID changed (expected {Uuid}, found {current!.TableUuid})");
        }
    }

    /// <summary><paramref name="SnapshotId"/> null asserts the ref must not exist.</summary>
    public sealed record AssertRefSnapshotId(string Ref, long? SnapshotId) : UpdateRequirement("assert-ref-snapshot-id")
    {
        public override void Validate(TableMetadata? current)
        {
            var actual = Exists(current).Refs.TryGetValue(Ref, out var r) ? r.SnapshotId : (long?)null;
            if (actual != SnapshotId) throw new CommitFailedException($"Requirement failed: branch {Ref} has changed (expected {SnapshotId?.ToString() ?? "none"}, found {actual?.ToString() ?? "none"})");
        }
    }

    public sealed record AssertLastAssignedFieldId(int LastAssignedFieldId) : UpdateRequirement("assert-last-assigned-field-id")
    {
        public override void Validate(TableMetadata? current)
        {
            if (Exists(current).LastColumnId != LastAssignedFieldId) throw new CommitFailedException("Requirement failed: last assigned field id changed");
        }
    }

    public sealed record AssertCurrentSchemaId(int CurrentSchemaId) : UpdateRequirement("assert-current-schema-id")
    {
        public override void Validate(TableMetadata? current)
        {
            if (Exists(current).CurrentSchemaId != CurrentSchemaId) throw new CommitFailedException("Requirement failed: current schema changed");
        }
    }

    public sealed record AssertLastAssignedPartitionId(int LastAssignedPartitionId) : UpdateRequirement("assert-last-assigned-partition-id")
    {
        public override void Validate(TableMetadata? current)
        {
            if (Exists(current).LastPartitionId != LastAssignedPartitionId) throw new CommitFailedException("Requirement failed: last assigned partition id changed");
        }
    }

    public sealed record AssertDefaultSpecId(int DefaultSpecId) : UpdateRequirement("assert-default-spec-id")
    {
        public override void Validate(TableMetadata? current)
        {
            if (Exists(current).DefaultSpecId != DefaultSpecId) throw new CommitFailedException("Requirement failed: default partition spec changed");
        }
    }

    public sealed record AssertDefaultSortOrderId(int DefaultSortOrderId) : UpdateRequirement("assert-default-sort-order-id")
    {
        public override void Validate(TableMetadata? current)
        {
            if (Exists(current).DefaultSortOrderId != DefaultSortOrderId) throw new CommitFailedException("Requirement failed: default sort order changed");
        }
    }
}

/// <summary>Optimistic concurrency conflict: refresh and retry.</summary>
public class CommitFailedException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>The commit outcome is unknown (e.g. a timeout after the catalog may have applied it). Never retry blindly and never delete written files.</summary>
public class CommitStateUnknownException(string message, Exception? inner = null) : Exception(message, inner);
