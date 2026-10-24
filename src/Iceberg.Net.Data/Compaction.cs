using System.Globalization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <param name="TargetFileSizeBytes">Files smaller than 75% of this are compaction candidates (Java's default ratio).</param>
/// <param name="MinInputFiles">A partition is rewritten only when it has at least this many candidates.</param>
/// <param name="Filter">Only consider files that may hold rows matching this filter.</param>
/// <param name="RewriteAll">Rewrite every matching file whatever its size (Java's <c>rewrite-all</c>).</param>
/// <param name="PartialProgressEnabled">
/// Java's <c>partial-progress.enabled</c>: commit the file groups (one per partition) in up to
/// <paramref name="PartialProgressMaxCommits"/> separate commits instead of one, so a concurrent writer invalidates only
/// the commit it races with — that one is re-planned on the new state and retried, the others stand. A group that still
/// fails after the retries is skipped (counted in <see cref="RewriteResult.FailedGroups"/>), not fatal.
/// </param>
/// <param name="PartialProgressMaxCommits">Java's <c>partial-progress.max-commits</c> (default 10).</param>
/// <remarks>
/// When the table has a default sort order, output is sorted by it (Java's sort strategy), and files not written in that
/// order are candidates on their own — so a newly set sort order is applied to existing data by one compaction.
/// </remarks>
public sealed record RewriteOptions(
    long TargetFileSizeBytes = 128L * 1024 * 1024,
    int MinInputFiles = 5,
    Expression? Filter = null,
    bool RewriteAll = false,
    bool PartialProgressEnabled = false,
    int PartialProgressMaxCommits = 10);

/// <param name="Snapshot">The (last) snapshot committed, or null when there was nothing to rewrite.</param>
/// <param name="Attempts">Planning attempts of the (last) commit.</param>
public sealed record RewriteResult(Snapshot? Snapshot, int FilesRewritten, int FilesAdded, long RowsRewritten, int Attempts)
{
    /// <summary>Every snapshot committed: one, or one per partial-progress commit.</summary>
    public IReadOnlyList<Snapshot> Commits { get; init; } = [];
    /// <summary>Partial progress: file groups given up on after their retries (their files are left as they were).</summary>
    public int FailedGroups { get; init; }
}

/// <param name="Snapshot">The <c>replace</c> snapshot, or null when there was nothing to rewrite.</param>
/// <param name="DeleteFilesRemoved">Position delete files (or dangling DVs) removed.</param>
/// <param name="DeleteFilesAdded">Position delete files written: one per data file that still has deleted rows.</param>
/// <param name="DanglingPositionsDropped">Deleted positions of data files that are no longer live (or that the delete no longer applies to).</param>
public sealed record RewritePositionDeletesResult(Snapshot? Snapshot, int DeleteFilesRemoved, int DeleteFilesAdded, long DanglingPositionsDropped);

/// <summary>
/// Rewrite data files (Java's <c>RewriteDataFiles</c>, bin-pack or sort): per partition, small files — with their deletes
/// applied — are merged into new files in one <c>replace</c> snapshot. v3 rows keep their <c>_row_id</c> and
/// last-updated sequence number. Any concurrent commit makes the rewrite re-plan from the new state, so a delete
/// committed meanwhile is never undone (the new files' higher sequence number would otherwise hide it).
/// </summary>
public static class Compaction
{
    private sealed record Group(string Key, IReadOnlyList<ScanTask> Tasks);

    private static string GroupKey(DataFile f) =>
        f.SpecId.ToString(CultureInfo.InvariantCulture) + "|" + string.Join('\u001f', f.Partition.Select(v => v is byte[] b ? Convert.ToHexString(b) : v?.ToString() ?? "\0"));

    private static async Task<List<Group>> PlanGroupsAsync(Table table, RewriteOptions options, IReadOnlySet<string>? onlyKeys, CancellationToken ct)
    {
        var plan = await TableScan.PlanAsync(table, options.Filter ?? Expression.True, ct: ct);
        var order = table.Metadata.DefaultSortOrder;
        bool Unsorted(DataFile f) => !order.IsUnsorted && f.SortOrderId != order.OrderId;
        return plan.Tasks
            .Where(t => options.RewriteAll || Unsorted(t.File) || t.File.FileSizeInBytes < options.TargetFileSizeBytes * 3 / 4)
            .GroupBy(t => GroupKey(t.File), StringComparer.Ordinal)
            .Where(g => onlyKeys is null || onlyKeys.Contains(g.Key))
            .Where(g => options.RewriteAll || g.Any(t => Unsorted(t.File)) || g.Count() >= Math.Max(2, options.MinInputFiles))
            .Select(g => new Group(g.Key, g.ToList()))
            .ToList();
    }

    public static async Task<RewriteResult> RewriteDataFilesAsync(Table table, RewriteOptions? options = null, int maxAttempts = 5, CancellationToken ct = default)
    {
        options ??= new RewriteOptions();
        if (options.PartialProgressEnabled) return await RewritePartialAsync(table, options, maxAttempts, ct);
        for (var attempt = 1; ; attempt++)
        {
            if (attempt > 1) await table.RefreshAsync(ct);
            if (table.Metadata.CurrentSnapshot is null) return new RewriteResult(null, 0, 0, 0, attempt);
            var groups = await PlanGroupsAsync(table, options, null, ct);
            if (groups.Count == 0) return new RewriteResult(null, 0, 0, 0, attempt);
            var (snapshot, outcome) = await CommitGroupsAsync(table, groups, ct);
            if (snapshot is not null) return new RewriteResult(snapshot, outcome.Removed, outcome.Added, outcome.Rows, attempt) { Commits = [snapshot] };
            if (attempt >= maxAttempts) throw new CommitFailedException($"Compaction lost {attempt} races in a row; giving up");
        }
    }

    /// <summary>Java's partial progress: groups split over up to max-commits commits, each re-planned and retried on its own.</summary>
    private static async Task<RewriteResult> RewritePartialAsync(Table table, RewriteOptions options, int maxAttempts, CancellationToken ct)
    {
        if (options.PartialProgressMaxCommits < 1) throw new ArgumentOutOfRangeException(nameof(options), "partial-progress.max-commits must be at least 1");
        if (table.Metadata.CurrentSnapshot is null) return new RewriteResult(null, 0, 0, 0, 1);
        var planned = await PlanGroupsAsync(table, options, null, ct);
        if (planned.Count == 0) return new RewriteResult(null, 0, 0, 0, 1);

        var perCommit = (planned.Count + options.PartialProgressMaxCommits - 1) / options.PartialProgressMaxCommits;
        var batches = planned.Chunk(perCommit).Select(b => b.Select(g => g.Key).ToHashSet(StringComparer.Ordinal)).ToList();
        var commits = new List<Snapshot>();
        int removed = 0, added = 0, failed = 0, attempts = 0;
        long rows = 0;
        foreach (var keys in batches)
        {
            for (var attempt = 1; ; attempt++)
            {
                attempts = attempt;
                if (attempt > 1) await table.RefreshAsync(ct);
                // Re-planned on the current state: a concurrent change to these partitions is read, never undone.
                var groups = await PlanGroupsAsync(table, options, keys, ct);
                if (groups.Count == 0) break;
                var (snapshot, outcome) = await CommitGroupsAsync(table, groups, ct);
                if (snapshot is not null)
                {
                    commits.Add(snapshot);
                    removed += outcome.Removed;
                    added += outcome.Added;
                    rows += outcome.Rows;
                    break;
                }
                if (attempt >= maxAttempts)
                {
                    failed += keys.Count;
                    break;
                }
            }
        }
        return new RewriteResult(commits.LastOrDefault(), removed, added, rows, attempts) { Commits = commits, FailedGroups = failed };
    }

    /// <summary>Rewrite the groups and commit them as one <c>replace</c>; null when a concurrent commit won (nothing left behind).</summary>
    private static async Task<(Snapshot? Snapshot, (int Removed, int Added, long Rows) Outcome)> CommitGroupsAsync(Table table, IReadOnlyList<Group> groups, CancellationToken ct)
    {
        var deletes = await TableScan.DeletesAsync(table, null, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var v3 = table.Metadata.FormatVersion >= 3;
        var schema = table.Schema;
        var projection = deletes.WithEqualityColumns(v3 ? RowLineage.WithLineage(schema) : schema);
        var n = schema.Columns.Count;
        var removed = new List<string>();
        var added = new List<DataFile>();
        long rows = 0;
        foreach (var group in groups)
        {
            var groupRows = new List<object?[]>();
            var lineage = new List<(long? RowId, long? Seq)>();
            foreach (var task in group.Tasks)
            {
                var fileRows = await DataFiles.ReadAsync(table.IO, task.File, projection, mapping, ct);
                var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, projection, ct);
                for (var pos = 0; pos < fileRows.Count; pos++)
                {
                    if (isDeleted?.Invoke(pos, fileRows[pos]) == true) continue;
                    var row = fileRows[pos][..n];
                    groupRows.Add(row);
                    if (v3) lineage.Add(((long?)fileRows[pos][n] ?? (task.File.FirstRowId is { } f ? f + pos : null), (long?)fileRows[pos][n + 1] ?? task.DataSequenceNumber));
                }
                removed.Add(task.File.FilePath);
            }
            rows += groupRows.Count;
            if (groupRows.Count > 0) added.AddRange(await TableWriter.WriteAsync(table, groupRows, v3 ? lineage : null, ct));
        }

        var producer = SnapshotProducer.Rewrite(table).RemoveDataFiles(removed).AddFiles(added).WithRetries(0)
            .Set("rewritten-data-files-count", removed.Count.ToString(CultureInfo.InvariantCulture));
        try
        {
            return (await producer.CommitAsync(ct), (removed.Count, added.Count, rows));
        }
        catch (CommitFailedException)
        {
            await TableFiles.DeleteAllAsync(table.IO, added.Select(f => f.FilePath), ct); // never committed: ours to remove
            return (null, default);
        }
    }

    /// <summary>
    /// Rewrite v2 position delete files (Java's <c>RewritePositionDeleteFiles</c>): every live position delete file is
    /// replaced by one file per data file holding all the positions still deleted from it, so readers open one delete file
    /// per data file; positions of data files that are gone, or that a delete no longer applies to (its data sequence
    /// number is older than the data file's), are dropped. One <c>replace</c> snapshot; the rows of the table do not
    /// change. Nothing is committed when every data file already has at most one delete file of its own and none is
    /// dangling. A concurrent commit that rewrote or removed one of the delete files fails it with
    /// <see cref="ValidationException"/>. On v3 tables, v2 position deletes are folded into deletion vectors
    /// (<see cref="SnapshotProducer.ConvertPositionDeletesToDeletionVectors"/>) and dangling deletion vectors are removed.
    /// </summary>
    public static async Task<RewritePositionDeletesResult> RewritePositionDeletesAsync(Table table, CancellationToken ct = default)
    {
        var m = table.Metadata;
        if (m.CurrentSnapshot is null) return new RewritePositionDeletesResult(null, 0, 0, 0);
        var index = await TableScan.DeletesAsync(table, null, ct);
        var live = (await TableScan.PlanFilesAsync(table, (long?)null, null, ct)).ToDictionary(t => t.File.FilePath, StringComparer.Ordinal);

        if (m.FormatVersion >= 3)
        {
            Snapshot? last = null;
            var converted = index.All.Count(e => !e.IsDeletionVector);
            if (converted > 0)
            {
                last = await SnapshotProducer.ConvertPositionDeletesToDeletionVectors(table).CommitAsync(ct);
                index = await TableScan.DeletesAsync(table, null, ct);
                live = (await TableScan.PlanFilesAsync(table, (long?)null, null, ct)).ToDictionary(t => t.File.FilePath, StringComparer.Ordinal);
            }
            var dangling = index.All.Where(e => e.IsDeletionVector && (!live.TryGetValue(e.File.ReferencedDataFile!, out var task) || e.DataSequenceNumber < task.DataSequenceNumber)).ToList();
            if (dangling.Count > 0)
                last = await SnapshotProducer.Rewrite(table).RemoveDeleteFiles(dangling.Select(d => d.File)).Set("rewritten-delete-files-count", dangling.Count.ToString(CultureInfo.InvariantCulture)).CommitAsync(ct);
            return new RewritePositionDeletesResult(last, converted + dangling.Count, 0, dangling.Sum(d => d.File.RecordCount));
        }

        var positionFiles = index.All.Where(e => !e.IsDeletionVector).ToList();
        if (positionFiles.Count == 0) return new RewritePositionDeletesResult(null, 0, 0, 0);

        var byDataFile = new Dictionary<string, RoaringPositionBitmap>(StringComparer.Ordinal);
        var filesPerDataFile = new Dictionary<string, int>(StringComparer.Ordinal);
        long dropped = 0;
        var compact = true;
        foreach (var pd in positionFiles)
        {
            var references = await index.PositionDeletesAsync(pd.File.FilePath, pd.File.FileFormat);
            if (references.Count != 1) compact = false;
            foreach (var (dataPath, positions) in references)
            {
                if (!live.TryGetValue(dataPath, out var task) || pd.DataSequenceNumber < task.DataSequenceNumber)
                {
                    dropped += positions.Cardinality;
                    compact = false;
                    continue;
                }
                filesPerDataFile[dataPath] = filesPerDataFile.GetValueOrDefault(dataPath) + 1;
                if (!byDataFile.TryGetValue(dataPath, out var bm)) byDataFile[dataPath] = bm = new RoaringPositionBitmap();
                bm.Or(positions);
            }
        }
        if (compact && filesPerDataFile.Values.All(c => c == 1)) return new RewritePositionDeletesResult(null, 0, 0, 0);

        var dataDir = m.Property("write.data.path", $"{m.Location}/data").TrimEnd('/');
        var commitId = Guid.NewGuid();
        var written = new List<DataFile>();
        foreach (var (dataPath, bm) in byDataFile.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            var data = live[dataPath].File;
            written.Add(await PositionDeleteFiles.WriteAsync(table.IO, $"{dataDir}/{commitId:N}-{written.Count:D5}-rewritten-deletes.parquet", data.SpecId, data.Partition,
                bm.Positions().Select(p => (dataPath, p)), ct));
        }
        try
        {
            var snapshot = await SnapshotProducer.Rewrite(table).RemoveDeleteFiles(positionFiles.Select(p => p.File)).AddDeleteFiles(written)
                .Set("rewritten-delete-files-count", positionFiles.Count.ToString(CultureInfo.InvariantCulture)).CommitAsync(ct);
            return new RewritePositionDeletesResult(snapshot, positionFiles.Count, written.Count, dropped);
        }
        catch (Exception e) when (e is ValidationException or CommitFailedException)
        {
            await TableFiles.DeleteAllAsync(table.IO, written.Select(f => f.FilePath), ct); // never committed: ours to remove
            throw;
        }
    }
}
