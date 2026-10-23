using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Metadata;

/// <summary>
/// Immutable table metadata (the content of a <c>vN.metadata.json</c> file). Every change goes through
/// <see cref="TableMetadataBuilder"/> by applying <see cref="MetadataUpdate"/>s, which is also the unit
/// exchanged with REST catalogs — so local catalogs (Glue, in-memory, SQL) and REST share one code path.
/// </summary>
public sealed record TableMetadata
{
    public const int SupportedFormatVersion = 3;
    public const int DefaultFormatVersion = 2;
    public const long InitialSequenceNumber = 0;
    public const long InitialRowId = 0;

    public required int FormatVersion { get; init; }
    public required Guid TableUuid { get; init; }
    public required string Location { get; init; }
    public long LastSequenceNumber { get; init; }
    public required long LastUpdatedMs { get; init; }
    public required int LastColumnId { get; init; }
    public required IReadOnlyList<Schema> Schemas { get; init; }
    public required int CurrentSchemaId { get; init; }
    public required IReadOnlyList<PartitionSpec> PartitionSpecs { get; init; }
    public required int DefaultSpecId { get; init; }
    public required int LastPartitionId { get; init; }
    public required IReadOnlyList<SortOrder> SortOrders { get; init; }
    public required int DefaultSortOrderId { get; init; }
    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();
    public long? CurrentSnapshotId { get; init; }
    public IReadOnlyList<Snapshot> Snapshots { get; init; } = [];
    public IReadOnlyList<SnapshotLogEntry> SnapshotLog { get; init; } = [];
    public IReadOnlyList<MetadataLogEntry> MetadataLog { get; init; } = [];
    public IReadOnlyDictionary<string, SnapshotRef> Refs { get; init; } = new Dictionary<string, SnapshotRef>();
    public IReadOnlyList<StatisticsFile> Statistics { get; init; } = [];
    public IReadOnlyList<PartitionStatisticsFile> PartitionStatistics { get; init; } = [];
    /// <summary>v3 row lineage: the next row ID to assign.</summary>
    public long? NextRowId { get; init; }
    public IReadOnlyList<EncryptedKey> EncryptionKeys { get; init; } = [];

    /// <summary>Where this metadata was read from / written to. Not serialised.</summary>
    public string? MetadataFileLocation { get; init; }

    public Schema CurrentSchema => SchemaById(CurrentSchemaId);
    public PartitionSpec DefaultSpec => SpecById(DefaultSpecId);
    public SortOrder DefaultSortOrder => SortOrders.First(o => o.OrderId == DefaultSortOrderId);
    public Snapshot? CurrentSnapshot => CurrentSnapshotId is { } id ? SnapshotById(id) : null;

    public Schema SchemaById(int id) => Schemas.FirstOrDefault(s => s.SchemaId == id) ?? throw new KeyNotFoundException($"Schema {id}");
    public PartitionSpec SpecById(int id) => PartitionSpecs.FirstOrDefault(s => s.SpecId == id) ?? throw new KeyNotFoundException($"Partition spec {id}");
    public Snapshot? SnapshotById(long id) => Snapshots.FirstOrDefault(s => s.SnapshotId == id);
    public Snapshot? SnapshotForRef(string name) => Refs.TryGetValue(name, out var r) ? SnapshotById(r.SnapshotId) : null;

    public long NextSequenceNumber => FormatVersion > 1 ? LastSequenceNumber + 1 : InitialSequenceNumber;

    public string Property(string key, string defaultValue) => Properties.GetValueOrDefault(key, defaultValue);

    /// <summary>Walk the parent chain from <paramref name="snapshotId"/> (inclusive), newest first.</summary>
    public IEnumerable<Snapshot> Ancestors(long? snapshotId)
    {
        var current = snapshotId is { } id ? SnapshotById(id) : null;
        while (current is not null)
        {
            yield return current;
            current = current.ParentSnapshotId is { } p ? SnapshotById(p) : null;
        }
    }

    /// <summary>
    /// Metadata for a new table. Field IDs, partition field IDs and spec/order IDs are freshly assigned,
    /// as Java and PyIceberg do, so callers can pass schemas with placeholder IDs.
    /// </summary>
    public static TableMetadata NewTable(
        Schema schema,
        PartitionSpec? spec,
        SortOrder? sortOrder,
        string location,
        IReadOnlyDictionary<string, string>? properties = null,
        int formatVersion = DefaultFormatVersion,
        Func<long>? clock = null)
    {
        if (formatVersion is < 1 or > SupportedFormatVersion) throw new ArgumentOutOfRangeException(nameof(formatVersion));
        var lastId = 0;
        var fresh = TypeUtil.AssignFreshIds(schema, () => ++lastId);
        if (fresh.MinFormatVersion > formatVersion)
            throw new ArgumentException($"Schema needs format version {fresh.MinFormatVersion} but table is v{formatVersion}");

        string NameOf(int oldId) => schema.FindColumnName(oldId) ?? throw new ArgumentException($"Partition/sort source {oldId} not in schema");
        int FreshId(int oldId) => fresh.FindField(NameOf(oldId))!.Id;

        var partitionId = PartitionSpec.PartitionDataIdStart - 1;
        var freshSpec = new PartitionSpec(0, (spec ?? PartitionSpec.Unpartitioned).Fields
            .Select(f => f with { SourceId = FreshId(f.SourceId), FieldId = ++partitionId, SourceIds = f.SourceIds?.Select(FreshId).ToList() }));

        var freshOrder = sortOrder is null || sortOrder.IsUnsorted
            ? SortOrder.Unsorted
            : new SortOrder(1, sortOrder.Fields.Select(f => f with { SourceId = FreshId(f.SourceId) }));

        var now = (clock ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()))();
        return new TableMetadata
        {
            FormatVersion = formatVersion,
            TableUuid = Guid.NewGuid(),
            Location = location.TrimEnd('/'),
            LastSequenceNumber = InitialSequenceNumber,
            LastUpdatedMs = now,
            LastColumnId = lastId,
            Schemas = [fresh],
            CurrentSchemaId = fresh.SchemaId,
            PartitionSpecs = [freshSpec],
            DefaultSpecId = 0,
            LastPartitionId = partitionId,
            SortOrders = freshOrder.IsUnsorted ? [SortOrder.Unsorted] : [freshOrder],
            DefaultSortOrderId = freshOrder.OrderId,
            Properties = Compat.Copy(properties ?? new Dictionary<string, string>()),
            NextRowId = formatVersion >= 3 ? InitialRowId : null,
        };
    }
}
