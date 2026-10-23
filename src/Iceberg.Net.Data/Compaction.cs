using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <param name="TargetFileSizeBytes">Files smaller than 75% of this are compaction candidates (Java's default ratio).</param>
/// <param name="MinInputFiles">A partition is rewritten only when it has at least this many candidates.</param>
/// <param name="Filter">Only consider files that may hold rows matching this filter.</param>
/// <param name="RewriteAll">Rewrite every matching file whatever its size (Java's <c>rewrite-all</c>).</param>
/// <remarks>
/// When the table has a default sort order, output is sorted by it (Java's sort strategy), and files not written in that
/// order are candidates on their own — so a newly set sort order is applied to existing data by one compaction.
/// </remarks>
public sealed record RewriteOptions(long TargetFileSizeBytes = 128L * 1024 * 1024, int MinInputFiles = 5, Expression? Filter = null, bool RewriteAll = false);

public sealed record RewriteResult(Snapshot? Snapshot, int FilesRewritten, int FilesAdded, long RowsRewritten, int Attempts);

/// <summary>
/// Rewrite data files (Java's <c>RewriteDataFiles</c>, bin-pack or sort): per partition, small files — with their deletes
/// applied — are merged into new files in one <c>replace</c> snapshot. v3 rows keep their <c>_row_id</c> and
/// last-updated sequence number. Any concurrent commit makes the rewrite re-plan from the new state, so a delete
/// committed meanwhile is never undone (the new files' higher sequence number would otherwise hide it).
/// </summary>
public static class Compaction
{
    public static async Task<RewriteResult> RewriteDataFilesAsync(Table table, RewriteOptions? options = null, int maxAttempts = 5, CancellationToken ct = default)
    {
        options ??= new RewriteOptions();
        for (var attempt = 1; ; attempt++)
        {
            if (attempt > 1) await table.RefreshAsync(ct);
            if (table.Metadata.CurrentSnapshot is null) return new RewriteResult(null, 0, 0, 0, attempt);

            var plan = await TableScan.PlanAsync(table, options.Filter ?? Expression.True, ct: ct);
            var order = table.Metadata.DefaultSortOrder;
            bool Unsorted(DataFile f) => !order.IsUnsorted && f.SortOrderId != order.OrderId;
            var groups = plan.Tasks
                .Where(t => options.RewriteAll || Unsorted(t.File) || t.File.FileSizeInBytes < options.TargetFileSizeBytes * 3 / 4)
                .GroupBy(t => (t.File.SpecId, Key: string.Join('\u001f', t.File.Partition.Select(v => v is byte[] b ? Convert.ToHexString(b) : v?.ToString() ?? "\0"))))
                .Where(g => options.RewriteAll || g.Any(t => Unsorted(t.File)) || g.Count() >= Math.Max(2, options.MinInputFiles))
                .ToList();
            if (groups.Count == 0) return new RewriteResult(null, 0, 0, 0, attempt);

            var deletes = await TableScan.DeletesAsync(table, null, ct);
            var mapping = NameMapping.ForTable(table.Metadata.Properties);
            var v3 = table.Metadata.FormatVersion >= 3;
            var schema = table.Schema;
            var projection = v3 ? RowLineage.WithLineage(schema) : schema;
            var n = schema.Columns.Count;
            var removed = new List<string>();
            var added = new List<DataFile>();
            long rows = 0;
            foreach (var group in groups)
            {
                var groupRows = new List<object?[]>();
                var lineage = new List<(long? RowId, long? Seq)>();
                foreach (var task in group)
                {
                    var fileRows = await DataFiles.ReadAsync(table.IO, task.File, projection, mapping, ct);
                    var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, schema, ct);
                    for (var pos = 0; pos < fileRows.Count; pos++)
                    {
                        var row = fileRows[pos][..n];
                        if (isDeleted?.Invoke(pos, row) == true) continue;
                        groupRows.Add(row);
                        if (v3) lineage.Add(((long?)fileRows[pos][n] ?? (task.File.FirstRowId is { } f ? f + pos : null), (long?)fileRows[pos][n + 1] ?? task.DataSequenceNumber));
                    }
                    removed.Add(task.File.FilePath);
                }
                rows += groupRows.Count;
                if (groupRows.Count > 0) added.AddRange(await TableWriter.WriteAsync(table, groupRows, v3 ? lineage : null, ct));
            }

            var producer = SnapshotProducer.Rewrite(table).RemoveDataFiles(removed).AddFiles(added).WithRetries(0)
                .Set("rewritten-data-files-count", removed.Count.ToString(System.Globalization.CultureInfo.InvariantCulture));
            try
            {
                var snapshot = await producer.CommitAsync(ct);
                return new RewriteResult(snapshot, removed.Count, added.Count, rows, attempt);
            }
            catch (CommitFailedException) when (attempt < maxAttempts)
            {
                await TableFiles.DeleteAllAsync(table.IO, added.Select(f => f.FilePath), ct); // never committed: ours to remove
            }
        }
    }
}
