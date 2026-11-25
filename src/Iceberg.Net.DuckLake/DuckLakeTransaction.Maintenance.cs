using Iceberg.Net.Data;
using Iceberg.Net.Types;
using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

/// <summary>Which files <see cref="DuckLakeCatalog.MergeAdjacentFilesAsync"/> considers (DuckDB's <c>ducklake_merge_adjacent_files</c> options).</summary>
public sealed record DuckLakeMergeOptions
{
    /// <summary>Merged files grow towards this size; files at or above it are left alone.</summary>
    public long TargetFileSize { get; init; } = 512L * 1024 * 1024;
    /// <summary>At most this many groups of files per table per call (merging is memory-hungry: run it in batches).</summary>
    public int? MaxCompactedFiles { get; init; }
    /// <summary>Files smaller than this are not merged.</summary>
    public long? MinFileSize { get; init; }
    /// <summary>Files at or above this size are not merged (default: <see cref="TargetFileSize"/>).</summary>
    public long? MaxFileSize { get; init; }
}

/// <summary>One table's part of a compaction: how many files went in and how many came out.</summary>
public sealed record DuckLakeCompaction(string Schema, string Table, int FilesProcessed, int FilesCreated);

public sealed partial class DuckLakeTransaction
{
    /// <summary>
    /// Small files merged into one partial data file: its rows keep their row IDs and the snapshot that wrote them
    /// (<c>_ducklake_internal_row_id</c>, <c>_ducklake_internal_snapshot_id</c>), so every snapshot reads exactly what it
    /// did before. The new file begins at the earliest input's snapshot; the inputs leave the catalog.
    /// </summary>
    private sealed record MergeOp(PendingTable Table, IReadOnlyList<long> Inputs, WrittenFile File, long BeginSnapshot, long PartialMax, long? RowIdStart,
        long? PartitionId) : Op;

    /// <summary>Files with many deleted rows rewritten without them (row IDs kept); the inputs and their delete files end here.</summary>
    private sealed record RewriteOp(PendingTable Table, IReadOnlyList<(long DataFileId, long? DeleteFileId)> Inputs, IReadOnlyList<(WrittenFile File, long? PartitionId)> Files) : Op;

    private async Task<(PendingTable Table, DuckLakeTable Loaded)> CompactableAsync(DuckLakeDbContext db, string schema, string table, CancellationToken ct)
    {
        var t = await TableAsync(db, schema, table, ct);
        if (t.Created || t.Altered || t.PartitionChanged || t.Inserted || t.Deleted || t.Compacted)
            throw new InvalidOperationException($"A compaction must be the only change to {schema}.{table} in a transaction");
        return (t, await _catalog.LoadTableAsync(db, t.Schema.Name, t.Name, _base!.Value, ct));
    }

    /// <summary>
    /// Stage the merge of adjacent small files of a table (DuckDB's <c>ducklake_merge_adjacent_files</c>): files of the
    /// same partition, in commit order, grouped towards <see cref="DuckLakeMergeOptions.TargetFileSize"/>. Files with
    /// deletes (delete files or inlined deletions) are skipped, as are files written before the table's last schema
    /// change. Returns null when nothing qualifies.
    /// </summary>
    internal async Task<DuckLakeCompaction?> MergeAdjacentFilesAsync(string schema, string table, DuckLakeMergeOptions options, CancellationToken ct)
    {
        PendingTable t;
        DuckLakeTable loaded;
        List<DataFileEntity> files;
        HashSet<long> withDeletes;
        Dictionary<long, IReadOnlyList<string?>> partitionValues;
        long schemaSince;
        await using (var db = await BeginAsync(ct))
        {
            (t, loaded) = await CompactableAsync(db, schema, table, ct);
            var tableId = loaded.TableId;
            var s = _base!.Value;
            files = await db.DataFiles.LiveAt(s).Where(f => f.TableId == tableId).ToListAsync(ct);
            withDeletes = (await db.DeleteFiles.LiveAt(s).Where(d => d.TableId == tableId).Select(d => d.DataFileId).ToListAsync(ct)).ToHashSet();
            withDeletes.UnionWith((await DuckLakeCatalog.InlinedDeletionsAsync(db, tableId, s, ct)).Keys);
            partitionValues = await PartitionValuesAsync(db, tableId, files.Select(f => f.DataFileId).ToList(), ct);
            schemaSince = await db.SchemaVersions.Where(v => v.TableId == tableId && v.BeginSnapshot <= s).MaxAsync(v => (long?)v.BeginSnapshot, ct) ?? 0;
        }
        var maxSize = options.MaxFileSize ?? options.TargetFileSize;
        var groups = files
            .Where(f => !withDeletes.Contains(f.DataFileId) && f.MappingId is null && f.BeginSnapshot >= schemaSince
                        && string.Equals(f.FileFormat ?? "parquet", "parquet", StringComparison.OrdinalIgnoreCase)
                        && f.FileSizeBytes < Math.Min(maxSize, options.TargetFileSize) && f.FileSizeBytes >= (options.MinFileSize ?? 0))
            .GroupBy(f => (f.PartitionId, Key: string.Join("\u0001", partitionValues.GetValueOrDefault(f.DataFileId, []).Select(v => v ?? "\u0000"))))
            .SelectMany(g => Adjacent(g.OrderBy(f => f.BeginSnapshot).ThenBy(f => f.FileOrder ?? f.DataFileId).ThenBy(f => f.DataFileId), options.TargetFileSize))
            .Where(g => g.Count >= 2)
            .Take(options.MaxCompactedFiles ?? int.MaxValue)
            .ToList();
        if (groups.Count == 0) return null;
        foreach (var group in groups)
        {
            var rows = new List<(object?[] Values, long RowId, long Snapshot)>();
            foreach (var f in group)
                await foreach (var (_, rowId, snapshot, values) in ReadFileAsync(loaded, f, deleted: null, ct))
                    rows.Add((values, rowId, snapshot));
            var directory = CompactedDirectory(group[0]);
            var file = await WriteCompactedAsync(t, loaded.Columns, directory, rows, keepSnapshots: true, partitionValues.GetValueOrDefault(group[0].DataFileId, []), ct);
            _ops.Add(new MergeOp(t, group.Select(f => f.DataFileId).ToList(), file, group.Min(f => f.BeginSnapshot),
                group.Max(f => f.PartialMax ?? f.BeginSnapshot), group.Any(f => f.RowIdStart is null) ? null : group.Min(f => f.RowIdStart), group[0].PartitionId));
        }
        t.Compacted = true;
        return new DuckLakeCompaction(t.Schema.Name, t.Name, groups.Sum(g => g.Count), groups.Count);

        static IEnumerable<List<DataFileEntity>> Adjacent(IEnumerable<DataFileEntity> ordered, long target)
        {
            var current = new List<DataFileEntity>();
            long size = 0;
            foreach (var f in ordered)
            {
                // Files are not combined when that would move the result further from the target.
                if (current.Count > 0 && size + f.FileSizeBytes > target)
                {
                    yield return current;
                    current = [];
                    size = 0;
                }
                current.Add(f);
                size += f.FileSizeBytes;
            }
            if (current.Count > 0) yield return current;
        }
    }

    /// <summary>
    /// Stage the rewrite of a table's files whose deleted share (delete file plus inlined deletions) is at least
    /// <paramref name="deleteThreshold"/> (DuckDB's <c>ducklake_rewrite_data_files</c>): the surviving rows of each
    /// partition go into a new file that keeps their row IDs; a file with no survivors is simply ended.
    /// </summary>
    internal async Task<DuckLakeCompaction?> RewriteDataFilesAsync(string schema, string table, double deleteThreshold, int? maxFiles, CancellationToken ct)
    {
        if (deleteThreshold is < 0 or > 1 || double.IsNaN(deleteThreshold)) throw new ArgumentOutOfRangeException(nameof(deleteThreshold), deleteThreshold, "between 0 and 1");
        PendingTable t;
        DuckLakeTable loaded;
        DuckLakeScanPlan plan;
        Dictionary<long, DeleteFileEntity> deleteRows;
        Dictionary<long, DataFileEntity> fileRows;
        Dictionary<long, IReadOnlyList<string?>> partitionValues;
        await using (var db = await BeginAsync(ct))
        {
            (t, loaded) = await CompactableAsync(db, schema, table, ct);
            plan = await _catalog.PlanScanAsync(db, loaded, null, ct);
            deleteRows = await DuckLakeCatalog.DeleteFilesAsync(db, loaded.TableId, loaded.SnapshotId, ct);
            var ids = plan.Files.Select(f => f.DataFileId).ToList();
            fileRows = await db.DataFiles.Where(f => ids.Contains(f.DataFileId)).ToDictionaryAsync(f => f.DataFileId, ct);
            partitionValues = await PartitionValuesAsync(db, loaded.TableId, ids, ct);
        }
        var candidates = new List<(DuckLakeFileTask Task, HashSet<long> Deleted)>();
        foreach (var task in plan.Files)
        {
            if (!deleteRows.ContainsKey(task.DataFileId) && !plan.InlinedDeletions.ContainsKey(task.DataFileId)) continue;
            var deleted = await _catalog.DeletedPositionsAsync(plan, task, loaded.SnapshotId, ct) ?? [];
            if (task.RecordCount > 0 && deleted.Count >= deleteThreshold * task.RecordCount) candidates.Add((task, deleted));
            if (candidates.Count >= (maxFiles ?? int.MaxValue)) break;
        }
        if (candidates.Count == 0) return null;
        var outputs = new List<(WrittenFile, long?)>();
        foreach (var group in candidates.GroupBy(c => (fileRows[c.Task.DataFileId].PartitionId,
                     Key: string.Join("\u0001", partitionValues.GetValueOrDefault(c.Task.DataFileId, []).Select(v => v ?? "\u0000")))))
        {
            var rows = new List<(object?[] Values, long RowId, long Snapshot)>();
            foreach (var (task, deleted) in group)
                await foreach (var (_, rowId, snapshot, values) in ReadFileAsync(loaded, fileRows[task.DataFileId], deleted, ct))
                    rows.Add((values, rowId, snapshot));
            if (rows.Count == 0) continue; // every row was deleted: the inputs just end
            var first = fileRows[group.First().Task.DataFileId];
            var directory = CompactedDirectory(first);
            outputs.Add((await WriteCompactedAsync(t, loaded.Columns, directory, rows, keepSnapshots: false, partitionValues.GetValueOrDefault(first.DataFileId, []), ct),
                group.Key.PartitionId));
        }
        _ops.Add(new RewriteOp(t, candidates.Select(c => (c.Task.DataFileId, deleteRows.TryGetValue(c.Task.DataFileId, out var d) ? d.DeleteFileId : (long?)null)).ToList(), outputs));
        t.Compacted = true;
        return new DuckLakeCompaction(t.Schema.Name, t.Name, candidates.Count, outputs.Count);
    }

    /// <summary>
    /// The directory (relative to the table) a compacted file goes in: its first input's, as DuckDB does (a partition's
    /// directory). An input registered in place (absolute path) has none under the table: the table's own then.
    /// </summary>
    private static string CompactedDirectory(DataFileEntity first) =>
        first.PathIsRelative != false && first.Path is { } p && p.LastIndexOf('/') is var slash and >= 0 ? p[..(slash + 1)] : "";

    private static async Task<Dictionary<long, IReadOnlyList<string?>>> PartitionValuesAsync(DuckLakeDbContext db, long tableId, List<long> fileIds, CancellationToken ct) =>
        (await db.FilePartitionValues.Where(p => p.TableId == tableId && fileIds.Contains(p.DataFileId)).ToListAsync(ct))
        .GroupBy(p => p.DataFileId)
        .ToDictionary(g => g.Key, g => (IReadOnlyList<string?>)g.OrderBy(p => p.PartitionKeyIndex).Select(p => p.PartitionValue).ToList());

    /// <summary>A data file's rows that are not in <paramref name="deleted"/>, with their row IDs and the snapshot that wrote each.</summary>
    private async IAsyncEnumerable<(long Position, long RowId, long Snapshot, object?[] Values)> ReadFileAsync(DuckLakeTable table, DataFileEntity file,
        HashSet<long>? deleted, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var location = DuckLakeCatalog.Resolve(table.Location, file.Path ?? throw new InvalidDataException($"Data file {file.DataFileId} has no path"), file.PathIsRelative ?? true);
        var width = table.Columns.Count;
        var options = new DataFileReadOptions { FileSize = file.FileSizeBytes, DecryptionKey = _catalog.Key(file.EncryptionKey, $"data file {file.DataFileId}") };
        var fromFile = DuckLakeValues.FileRowConverter(table.Columns, toFile: false);
        DuckLakeNameMap? mapping = null;
        if (file.MappingId is { } mappingId)
        {
            await using var db = _catalog.Context(_stage);
            mapping = (await DuckLakeNameMap.LoadAsync(db, [mappingId], ct))[mappingId];
        }
        await foreach (var batch in _catalog.ReadDataFileAsync(table, location, mapping, DuckLakeCatalog.FileProjection(table), options, ct))
        {
            for (var i = 0; i < batch.Rows.Count; i++)
            {
                var pos = batch.FirstPosition + i;
                if (deleted is not null && deleted.Contains(pos)) continue;
                var full = batch.Rows[i];
                var rowId = full[width] as long? ?? file.RowIdStart + pos
                    ?? throw new InvalidDataException($"Data file {file.DataFileId} has no row_id_start and no stored row IDs");
                var values = full[..width];
                yield return (pos, rowId, full[width + 1] as long? ?? file.BeginSnapshot, fromFile is null ? values : fromFile(values));
            }
        }
    }

    /// <summary>Write compacted rows with their row IDs (and, for a partial file, the snapshot of each row) in the table's current layout.</summary>
    private async Task<WrittenFile> WriteCompactedAsync(PendingTable t, IReadOnlyList<DuckLakeColumn> columns, string directory, List<(object?[] Values, long RowId, long Snapshot)> rows,
        bool keepSnapshots, IReadOnlyList<string?> partitionValues, CancellationToken ct)
    {
        var stored = columns.Select((c, i) => (c, i)).Where(x => x.c.Type is not DuckLakePrimitive { Name: "null" }).ToList();
        var fields = DuckLakeTypes.ToIceberg(stored.Select(x => x.c).ToList()).Columns
            .Append(NestedField.Optional(DuckLakeCatalog.RowIdFieldId, "_ducklake_internal_row_id", PrimitiveType.Long));
        if (keepSnapshots) fields = fields.Append(NestedField.Optional(DuckLakeCatalog.SnapshotIdFieldId, "_ducklake_internal_snapshot_id", PrimitiveType.Long));
        var storedColumns = stored.Select(x => x.c).ToList();
        var converted = storedColumns.Select((c, k) => DuckLakeTypes.HasFileForm(c.Type)).ToArray();
        var fileRows = rows.Select(r =>
        {
            var values = new object?[stored.Count + (keepSnapshots ? 2 : 1)];
            for (var k = 0; k < stored.Count; k++)
                values[k] = converted[k] ? DuckLakeValues.ToFile(stored[k].c.Type, r.Values[stored[k].i]) : r.Values[stored[k].i];
            values[stored.Count] = r.RowId;
            if (keepSnapshots) values[stored.Count + 1] = r.Snapshot;
            return values;
        }).ToList();
        var relative = $"{directory}ducklake-{Guid.CreateVersion7()}.parquet";
        var location = t.Location + relative;
        _written.Add(location);
        var (key, keyText) = _catalog.NewKey();
        var written = await ParquetFiles.WriteFileAsync(_catalog.IO, location, new Schema(0, fields), 0, [], fileRows,
            _catalog.Options.Parquet with { Metrics = false, Annotations = DuckLakeTypes.Annotations(storedColumns), EncryptionKey = key }, ct);
        var stats = new StatsCollector(columns);
        foreach (var r in rows) stats.Add(r.Values);
        return new WrittenFile(relative, rows.Count, written.File.FileSizeInBytes, written.FooterSize, written.RowGroups, stats.Result(written.File.ColumnSizes), partitionValues)
            { Key = keyText };
    }
}
