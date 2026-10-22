namespace Iceberg.Net.Metadata;

/// <summary>Snapshot summary operation values from the spec.</summary>
public static class Operations
{
    public const string Append = "append";
    public const string Replace = "replace";
    public const string Overwrite = "overwrite";
    public const string Delete = "delete";
}

/// <summary>
/// A snapshot: the state of a table at a point in time, identified by its manifest list.
/// v1 tables could instead embed a <see cref="Manifests"/> list; this library reads that but never writes it.
/// </summary>
public sealed record Snapshot
{
    public required long SnapshotId { get; init; }
    public long? ParentSnapshotId { get; init; }
    /// <summary>v2+: required; v1: absent (treated as 0).</summary>
    public long SequenceNumber { get; init; }
    public required long TimestampMs { get; init; }
    public string? ManifestList { get; init; }
    public IReadOnlyList<string>? Manifests { get; init; }
    public IReadOnlyDictionary<string, string> Summary { get; init; } = new Dictionary<string, string>();
    public int? SchemaId { get; init; }
    /// <summary>v3 row lineage: first row ID assigned to rows added in this snapshot.</summary>
    public long? FirstRowId { get; init; }
    /// <summary>v3 row lineage: upper bound of row IDs assigned by this snapshot.</summary>
    public long? AddedRows { get; init; }
    /// <summary>v3: ID of the encryption key that encrypts the manifest list key metadata.</summary>
    public string? KeyId { get; init; }

    public string? Operation => Summary.GetValueOrDefault("operation");
}

public enum SnapshotRefType { Branch, Tag }

/// <summary>A named reference (branch or tag). <c>main</c> is the branch that tracks <c>current-snapshot-id</c>.</summary>
public sealed record SnapshotRef(
    long SnapshotId,
    SnapshotRefType Type,
    int? MinSnapshotsToKeep = null,
    long? MaxSnapshotAgeMs = null,
    long? MaxRefAgeMs = null)
{
    public const string Main = "main";

    public static SnapshotRef Branch(long id) => new(id, SnapshotRefType.Branch);
    public static SnapshotRef Tag(long id) => new(id, SnapshotRefType.Tag);
}

public sealed record SnapshotLogEntry(long TimestampMs, long SnapshotId);

public sealed record MetadataLogEntry(long TimestampMs, string MetadataFile);

/// <summary>Table-level statistics file (Puffin) registered for a snapshot.</summary>
public sealed record StatisticsFile(
    long SnapshotId,
    string StatisticsPath,
    long FileSizeInBytes,
    long FileFooterSizeInBytes,
    IReadOnlyList<BlobMetadata> BlobMetadata,
    string? KeyMetadata = null);

public sealed record BlobMetadata(string Type, long SnapshotId, long SequenceNumber, IReadOnlyList<int> Fields, IReadOnlyDictionary<string, string>? Properties = null);

public sealed record PartitionStatisticsFile(long SnapshotId, string StatisticsPath, long FileSizeInBytes);

/// <summary>v3 table encryption key entry.</summary>
public sealed record EncryptedKey(string KeyId, string EncryptedKeyMetadata, string? EncryptedById = null, IReadOnlyDictionary<string, string>? Properties = null);
