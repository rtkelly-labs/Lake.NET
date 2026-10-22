namespace Iceberg.Net.Manifests;

public enum FileContent { Data = 0, PositionDeletes = 1, EqualityDeletes = 2 }

public enum ManifestContent { Data = 0, Deletes = 1 }

public enum ManifestEntryStatus { Existing = 0, Added = 1, Deleted = 2 }

/// <summary>
/// A data or delete file tracked by a manifest (<c>data_file</c>, field 2). Column metrics are keyed by
/// field ID; bounds use single-value binary serialisation (<see cref="Types.Conversions"/>).
/// </summary>
public sealed record DataFile
{
    public FileContent Content { get; init; } = FileContent.Data;
    public required string FilePath { get; init; }
    public string FileFormat { get; init; } = "PARQUET";
    public int SpecId { get; init; }
    /// <summary>Partition tuple in partition-spec field order, using physical values.</summary>
    public IReadOnlyList<object?> Partition { get; init; } = [];
    public required long RecordCount { get; init; }
    public required long FileSizeInBytes { get; init; }
    public IReadOnlyDictionary<int, long>? ColumnSizes { get; init; }
    public IReadOnlyDictionary<int, long>? ValueCounts { get; init; }
    public IReadOnlyDictionary<int, long>? NullValueCounts { get; init; }
    public IReadOnlyDictionary<int, long>? NanValueCounts { get; init; }
    public IReadOnlyDictionary<int, byte[]>? LowerBounds { get; init; }
    public IReadOnlyDictionary<int, byte[]>? UpperBounds { get; init; }
    public byte[]? KeyMetadata { get; init; }
    public IReadOnlyList<long>? SplitOffsets { get; init; }
    public IReadOnlyList<int>? EqualityIds { get; init; }
    public int? SortOrderId { get; init; }
    /// <summary>v3 row lineage.</summary>
    public long? FirstRowId { get; init; }
    /// <summary>v3: data file a position-delete file or deletion vector applies to.</summary>
    public string? ReferencedDataFile { get; init; }
    /// <summary>v3 deletion vectors: offset/length of the blob inside the Puffin file.</summary>
    public long? ContentOffset { get; init; }
    public long? ContentSizeInBytes { get; init; }
}

/// <summary>
/// A manifest entry. Null <see cref="SnapshotId"/>/<see cref="SequenceNumber"/>/<see cref="FileSequenceNumber"/>
/// are inherited from the manifest list on read (only valid for ADDED entries).
/// </summary>
public sealed record ManifestEntry(
    ManifestEntryStatus Status,
    long? SnapshotId,
    long? SequenceNumber,
    long? FileSequenceNumber,
    DataFile DataFile);

public sealed record FieldSummary(bool ContainsNull, bool? ContainsNan, byte[]? LowerBound, byte[]? UpperBound);

/// <summary>A manifest list entry (<c>manifest_file</c>).</summary>
public sealed record ManifestFile
{
    public required string ManifestPath { get; init; }
    public required long ManifestLength { get; init; }
    public required int PartitionSpecId { get; init; }
    public ManifestContent Content { get; init; } = ManifestContent.Data;
    public long SequenceNumber { get; init; }
    public long MinSequenceNumber { get; init; }
    public required long AddedSnapshotId { get; init; }
    public int AddedFilesCount { get; init; }
    public int ExistingFilesCount { get; init; }
    public int DeletedFilesCount { get; init; }
    public long AddedRowsCount { get; init; }
    public long ExistingRowsCount { get; init; }
    public long DeletedRowsCount { get; init; }
    public IReadOnlyList<FieldSummary>? Partitions { get; init; }
    public byte[]? KeyMetadata { get; init; }
    /// <summary>v3 row lineage: first row ID for ADDED data files in this manifest.</summary>
    public long? FirstRowId { get; init; }
}
