using System.Globalization;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>A row of the <c>snapshots</c> metadata table.</summary>
public sealed record SnapshotsRow(DateTimeOffset CommittedAt, long SnapshotId, long? ParentId, string? Operation, string? ManifestList, IReadOnlyDictionary<string, string> Summary)
{
    public IReadOnlyDictionary<string, object?> Columns => new Dictionary<string, object?>
    {
        ["committed_at"] = CommittedAt, ["snapshot_id"] = SnapshotId, ["parent_id"] = ParentId, ["operation"] = Operation,
        ["manifest_list"] = ManifestList, ["summary"] = Summary,
    };
}

/// <summary>A row of the <c>history</c> metadata table: when a snapshot became current on <c>main</c>.</summary>
public sealed record HistoryRow(DateTimeOffset MadeCurrentAt, long SnapshotId, long? ParentId, bool IsCurrentAncestor)
{
    public IReadOnlyDictionary<string, object?> Columns => new Dictionary<string, object?>
    {
        ["made_current_at"] = MadeCurrentAt, ["snapshot_id"] = SnapshotId, ["parent_id"] = ParentId, ["is_current_ancestor"] = IsCurrentAncestor,
    };
}

/// <summary>A row of the <c>refs</c> metadata table. <see cref="Type"/> is <c>BRANCH</c> or <c>TAG</c>.</summary>
public sealed record RefsRow(string Name, string Type, long SnapshotId, long? MaxReferenceAgeInMs, int? MinSnapshotsToKeep, long? MaxSnapshotAgeInMs)
{
    public IReadOnlyDictionary<string, object?> Columns => new Dictionary<string, object?>
    {
        ["name"] = Name, ["type"] = Type, ["snapshot_id"] = SnapshotId, ["max_reference_age_in_ms"] = MaxReferenceAgeInMs,
        ["min_snapshots_to_keep"] = MinSnapshotsToKeep, ["max_snapshot_age_in_ms"] = MaxSnapshotAgeInMs,
    };
}

/// <summary>One partition field's summary in the <c>manifests</c> table; bounds as the transform's human-readable string.</summary>
public sealed record PartitionSummaryRow(bool ContainsNull, bool? ContainsNan, string? LowerBound, string? UpperBound);

/// <summary>A row of the <c>manifests</c> metadata table (<see cref="Content"/>: 0 data, 1 deletes).</summary>
public sealed record ManifestsRow(
    int Content, string Path, long Length, int PartitionSpecId, long AddedSnapshotId,
    int AddedDataFilesCount, int ExistingDataFilesCount, int DeletedDataFilesCount,
    int AddedDeleteFilesCount, int ExistingDeleteFilesCount, int DeletedDeleteFilesCount,
    IReadOnlyList<PartitionSummaryRow> PartitionSummaries)
{
    public IReadOnlyDictionary<string, object?> Columns => new Dictionary<string, object?>
    {
        ["content"] = Content, ["path"] = Path, ["length"] = Length, ["partition_spec_id"] = PartitionSpecId, ["added_snapshot_id"] = AddedSnapshotId,
        ["added_data_files_count"] = AddedDataFilesCount, ["existing_data_files_count"] = ExistingDataFilesCount, ["deleted_data_files_count"] = DeletedDataFilesCount,
        ["added_delete_files_count"] = AddedDeleteFilesCount, ["existing_delete_files_count"] = ExistingDeleteFilesCount, ["deleted_delete_files_count"] = DeletedDeleteFilesCount,
        ["partition_summaries"] = PartitionSummaries,
    };
}

/// <summary>
/// A row of the <c>files</c>, <c>data_files</c> and <c>delete_files</c> metadata tables (<see cref="Content"/>: 0 data,
/// 1 position deletes, 2 equality deletes). <see cref="Partition"/> maps the table's unified partition fields (every
/// partition field of every spec, as Spark shows them) to the file's stored (physical) values, null for a field its spec
/// lacks; metric maps are keyed by field ID with bounds in single-value binary form, as Spark shows them.
/// </summary>
public sealed record FilesRow(
    int Content, string FilePath, string FileFormat, int SpecId, IReadOnlyDictionary<string, object?> Partition,
    long RecordCount, long FileSizeInBytes,
    IReadOnlyDictionary<int, long>? ColumnSizes, IReadOnlyDictionary<int, long>? ValueCounts, IReadOnlyDictionary<int, long>? NullValueCounts,
    IReadOnlyDictionary<int, long>? NanValueCounts, IReadOnlyDictionary<int, byte[]>? LowerBounds, IReadOnlyDictionary<int, byte[]>? UpperBounds,
    byte[]? KeyMetadata, IReadOnlyList<long>? SplitOffsets, IReadOnlyList<int>? EqualityIds, int? SortOrderId,
    long? FirstRowId, string? ReferencedDataFile, long? ContentOffset, long? ContentSizeInBytes)
{
    public IReadOnlyDictionary<string, object?> Columns => new Dictionary<string, object?>
    {
        ["content"] = Content, ["file_path"] = FilePath, ["file_format"] = FileFormat, ["spec_id"] = SpecId, ["partition"] = Partition,
        ["record_count"] = RecordCount, ["file_size_in_bytes"] = FileSizeInBytes, ["column_sizes"] = ColumnSizes, ["value_counts"] = ValueCounts,
        ["null_value_counts"] = NullValueCounts, ["nan_value_counts"] = NanValueCounts, ["lower_bounds"] = LowerBounds, ["upper_bounds"] = UpperBounds,
        ["key_metadata"] = KeyMetadata, ["split_offsets"] = SplitOffsets, ["equality_ids"] = EqualityIds, ["sort_order_id"] = SortOrderId,
        ["first_row_id"] = FirstRowId, ["referenced_data_file"] = ReferencedDataFile, ["content_offset"] = ContentOffset, ["content_size_in_bytes"] = ContentSizeInBytes,
    };
}

/// <summary>A row of the <c>entries</c> metadata table (<see cref="Status"/>: 0 existing, 1 added, 2 deleted).</summary>
public sealed record EntriesRow(int Status, long? SnapshotId, long? SequenceNumber, long? FileSequenceNumber, FilesRow DataFile)
{
    public IReadOnlyDictionary<string, object?> Columns => new Dictionary<string, object?>
    {
        ["status"] = Status, ["snapshot_id"] = SnapshotId, ["sequence_number"] = SequenceNumber, ["file_sequence_number"] = FileSequenceNumber, ["data_file"] = DataFile,
    };
}

/// <summary>
/// A row of the <c>partitions</c> metadata table. Partitions are keyed on the table's unified partition type (every
/// partition field of every spec, by field ID), as Java's <c>PartitionsTable</c>; a field a file's spec lacks is null.
/// Deletion vectors count as position delete files and also in <see cref="DvCount"/>.
/// </summary>
public sealed record PartitionsRow(
    IReadOnlyDictionary<string, object?> Partition, int SpecId, long RecordCount, int FileCount, long TotalDataFileSizeInBytes,
    long PositionDeleteRecordCount, int PositionDeleteFileCount, int DvCount, long EqualityDeleteRecordCount, int EqualityDeleteFileCount,
    DateTimeOffset? LastUpdatedAt, long? LastUpdatedSnapshotId)
{
    public IReadOnlyDictionary<string, object?> Columns => new Dictionary<string, object?>
    {
        ["partition"] = Partition, ["spec_id"] = SpecId, ["record_count"] = RecordCount, ["file_count"] = FileCount,
        ["total_data_file_size_in_bytes"] = TotalDataFileSizeInBytes, ["position_delete_record_count"] = PositionDeleteRecordCount,
        ["position_delete_file_count"] = PositionDeleteFileCount, ["dv_count"] = DvCount, ["equality_delete_record_count"] = EqualityDeleteRecordCount,
        ["equality_delete_file_count"] = EqualityDeleteFileCount, ["last_updated_at"] = LastUpdatedAt, ["last_updated_snapshot_id"] = LastUpdatedSnapshotId,
    };
}

/// <summary>A row of the <c>metadata_log_entries</c> metadata table.</summary>
public sealed record MetadataLogEntriesRow(DateTimeOffset Timestamp, string File, long? LatestSnapshotId, int? LatestSchemaId, long? LatestSequenceNumber);

/// <summary>
/// Iceberg's metadata tables (Spark's <c>table.snapshots</c>, <c>table.files</c>, …) as typed rows with the same columns
/// (<c>Columns</c> gives each row under Spark's column names). <c>snapshots</c>, <c>history</c>, <c>refs</c> and
/// <c>metadata_log_entries</c> describe the table; the others read the manifests of the snapshot a
/// <see cref="SnapshotSelector"/> picks (default: current), so <c>files</c> of a tag are the files that tag sees.
/// Not covered: the <c>readable_metrics</c> column and the <c>all_*</c> tables.
/// </summary>
public static class MetadataTables
{
    public static IReadOnlyList<SnapshotsRow> Snapshots(Table table) =>
        table.Metadata.Snapshots.OrderBy(s => s.TimestampMs)
            .Select(s => new SnapshotsRow(At(s.TimestampMs), s.SnapshotId, s.ParentSnapshotId, s.Operation, s.ManifestList, s.Summary)).ToList();

    public static IReadOnlyList<HistoryRow> History(Table table)
    {
        var m = table.Metadata;
        var ancestors = m.Ancestors(m.CurrentSnapshotId).Select(s => s.SnapshotId).ToHashSet();
        return m.SnapshotLog.Select(e => new HistoryRow(At(e.TimestampMs), e.SnapshotId, m.SnapshotById(e.SnapshotId)?.ParentSnapshotId, ancestors.Contains(e.SnapshotId))).ToList();
    }

    public static IReadOnlyList<RefsRow> Refs(Table table) =>
        table.Metadata.Refs.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => new RefsRow(kv.Key, kv.Value.Type == SnapshotRefType.Branch ? "BRANCH" : "TAG", kv.Value.SnapshotId,
                kv.Value.MaxRefAgeMs, kv.Value.MinSnapshotsToKeep, kv.Value.MaxSnapshotAgeMs)).ToList();

    public static IReadOnlyList<MetadataLogEntriesRow> MetadataLogEntries(Table table)
    {
        var m = table.Metadata;
        // Java resolves the snapshot current at each entry's time from the snapshot log.
        MetadataLogEntriesRow Row(long ts, string file)
        {
            var current = m.SnapshotLog.LastOrDefault(e => e.TimestampMs <= ts);
            var snapshot = current is null ? null : m.SnapshotById(current.SnapshotId);
            return new MetadataLogEntriesRow(At(ts), file, current?.SnapshotId, snapshot?.SchemaId, snapshot?.SequenceNumber);
        }
        var rows = m.MetadataLog.Select(e => Row(e.TimestampMs, e.MetadataFile)).ToList();
        if (m.MetadataFileLocation is { } location) rows.Add(Row(m.LastUpdatedMs, location));
        return rows;
    }

    public static async Task<IReadOnlyList<ManifestsRow>> ManifestsAsync(Table table, SnapshotSelector? at = null, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = TimeTravel.Resolve(m, at ?? SnapshotSelector.Current);
        if (snapshot is null) return [];
        return (await TableScan.ManifestsAsync(table, snapshot, ct)).Select(mf =>
        {
            var spec = m.SpecById(mf.PartitionSpecId);
            var type = spec.PartitionType(m.CurrentSchema);
            var summaries = (mf.Partitions ?? []).Select((p, i) => new PartitionSummaryRow(p.ContainsNull, p.ContainsNan,
                Human(spec.Fields[i], type.Fields[i].Type, p.LowerBound), Human(spec.Fields[i], type.Fields[i].Type, p.UpperBound))).ToList();
            var data = mf.Content == ManifestContent.Data;
            return new ManifestsRow((int)mf.Content, mf.ManifestPath, mf.ManifestLength, mf.PartitionSpecId, mf.AddedSnapshotId,
                data ? mf.AddedFilesCount : 0, data ? mf.ExistingFilesCount : 0, data ? mf.DeletedFilesCount : 0,
                data ? 0 : mf.AddedFilesCount, data ? 0 : mf.ExistingFilesCount, data ? 0 : mf.DeletedFilesCount, summaries);
        }).ToList();
    }

    /// <summary>Live data and delete files of the snapshot (<c>files</c>).</summary>
    public static async Task<IReadOnlyList<FilesRow>> FilesAsync(Table table, SnapshotSelector? at = null, CancellationToken ct = default) =>
        (await EntriesAsync(table, at, ct)).Where(e => e.Status != (int)ManifestEntryStatus.Deleted).Select(e => e.DataFile).ToList();

    public static async Task<IReadOnlyList<FilesRow>> DataFilesAsync(Table table, SnapshotSelector? at = null, CancellationToken ct = default) =>
        (await FilesAsync(table, at, ct)).Where(f => f.Content == (int)FileContent.Data).ToList();

    public static async Task<IReadOnlyList<FilesRow>> DeleteFilesAsync(Table table, SnapshotSelector? at = null, CancellationToken ct = default) =>
        (await FilesAsync(table, at, ct)).Where(f => f.Content != (int)FileContent.Data).ToList();

    /// <summary>Every entry of the snapshot's manifests, deleted ones included (<c>entries</c>).</summary>
    public static async Task<IReadOnlyList<EntriesRow>> EntriesAsync(Table table, SnapshotSelector? at = null, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = TimeTravel.Resolve(m, at ?? SnapshotSelector.Current);
        if (snapshot is null) return [];
        var rows = new List<EntriesRow>();
        var unified = UnifiedPartitionFields(m);
        foreach (var mf in await TableScan.ManifestsAsync(table, snapshot, ct))
        {
            var spec = m.SpecById(mf.PartitionSpecId);
            foreach (var e in ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(mf.ManifestPath, ct), spec, m.CurrentSchema, mf).Entries)
                rows.Add(new EntriesRow((int)e.Status, e.SnapshotId, e.SequenceNumber, e.FileSequenceNumber, File(unified, spec, e.DataFile with { SpecId = mf.PartitionSpecId })));
        }
        return rows;
    }

    public static async Task<IReadOnlyList<PartitionsRow>> PartitionsAsync(Table table, SnapshotSelector? at = null, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = TimeTravel.Resolve(m, at ?? SnapshotSelector.Current);
        if (snapshot is null) return [];
        var unified = UnifiedPartitionFields(m);
        var groups = new Dictionary<string, Acc>(StringComparer.Ordinal);
        foreach (var mf in await TableScan.ManifestsAsync(table, snapshot, ct))
        {
            var spec = m.SpecById(mf.PartitionSpecId);
            foreach (var e in ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(mf.ManifestPath, ct), spec, m.CurrentSchema, mf).Entries)
            {
                if (e.Status == ManifestEntryStatus.Deleted) continue;
                var f = e.DataFile;
                var values = UnifiedValues(unified, spec, f);
                var key = string.Join('\u001f', values.Select(v => v is byte[] b ? Convert.ToHexString(b) : Convert.ToString(v, CultureInfo.InvariantCulture) ?? "\0"));
                if (!groups.TryGetValue(key, out var acc))
                    groups[key] = acc = new Acc(unified.Select((u, i) => KeyValuePair.Create(u.Name, values[i])).ToDictionary(kv => kv.Key, kv => kv.Value));
                acc.SpecId = mf.PartitionSpecId;
                switch (f.Content)
                {
                    case FileContent.Data:
                        acc.Records += f.RecordCount; acc.Files++; acc.Bytes += f.FileSizeInBytes;
                        break;
                    case FileContent.PositionDeletes:
                        acc.PosRecords += f.RecordCount; acc.PosFiles++;
                        if (f.FileFormat == "PUFFIN") acc.Dvs++;
                        break;
                    case FileContent.EqualityDeletes:
                        acc.EqRecords += f.RecordCount; acc.EqFiles++;
                        break;
                }
                if (e.SnapshotId is { } sid && m.SnapshotById(sid) is { } s && (acc.LastUpdated is null || s.TimestampMs > acc.LastUpdated.TimestampMs)) acc.LastUpdated = s;
            }
        }
        return groups.Values.Select(a => new PartitionsRow(a.Partition, a.SpecId, a.Records, a.Files, a.Bytes, a.PosRecords, a.PosFiles, a.Dvs, a.EqRecords, a.EqFiles,
            a.LastUpdated is null ? null : At(a.LastUpdated.TimestampMs), a.LastUpdated?.SnapshotId)).ToList();
    }

    private sealed class Acc(Dictionary<string, object?> partition)
    {
        public Dictionary<string, object?> Partition { get; } = partition;
        public int SpecId;
        public long Records, Bytes, PosRecords, EqRecords;
        public int Files, PosFiles, Dvs, EqFiles;
        public Snapshot? LastUpdated;
    }

    /// <summary>Java's <c>Partitioning.partitionType</c>: every non-void partition field of every spec, by field ID, in order of first definition.</summary>
    private static List<PartitionField> UnifiedPartitionFields(TableMetadata m) =>
        m.PartitionSpecs.OrderBy(s => s.SpecId).SelectMany(s => s.Fields).Where(f => f.Transform is not VoidTransform).DistinctBy(f => f.FieldId).ToList();

    private static List<object?> UnifiedValues(List<PartitionField> unified, PartitionSpec spec, DataFile f) =>
        unified.Select(u => spec.Fields.ToList().FindIndex(sf => sf.FieldId == u.FieldId) is var i and >= 0 && i < f.Partition.Count && spec.Fields[i].Transform is not VoidTransform
            ? f.Partition[i] : null).ToList();

    private static FilesRow File(List<PartitionField> unified, PartitionSpec spec, DataFile f) => new(
        (int)f.Content, f.FilePath, f.FileFormat, f.SpecId,
        unified.Zip(UnifiedValues(unified, spec, f)).ToDictionary(p => p.First.Name, p => p.Second),
        f.RecordCount, f.FileSizeInBytes, f.ColumnSizes, f.ValueCounts, f.NullValueCounts, f.NanValueCounts, f.LowerBounds, f.UpperBounds,
        f.KeyMetadata, f.SplitOffsets, f.EqualityIds, f.SortOrderId, f.FirstRowId, f.ReferencedDataFile, f.ContentOffset, f.ContentSizeInBytes);

    private static string? Human(PartitionField field, IcebergType type, byte[]? bound) =>
        bound is null ? null : field.Transform.ToHumanString(type, Conversions.FromBytes(type, bound));

    private static DateTimeOffset At(long ms) => DateTimeOffset.FromUnixTimeMilliseconds(ms);
}
