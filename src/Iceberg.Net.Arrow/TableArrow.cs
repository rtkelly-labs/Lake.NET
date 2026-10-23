using System.Runtime.CompilerServices;
using Apache.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.Arrow;

/// <summary>Arrow record batches in and out of tables: the same scans and write modes as the row API.</summary>
public static class TableArrow
{
    /// <summary>
    /// Matching rows as record batches (one per data file, after deletes and the filter; empty batches skipped), in the
    /// current schema with field IDs.
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(Table table, Expression? filter = null, long? snapshotId = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        filter ??= Expression.True;
        var bound = Binder.Bind(table.Schema, filter);
        var matches = bound is AlwaysTrue ? null : RowEvaluator.Compile(bound);
        var deletes = await TableScan.DeletesAsync(table, snapshotId, ct);
        var mapping = Types.NameMapping.ForTable(table.Metadata.Properties);
        foreach (var task in await TableScan.PlanFilesAsync(table, filter, snapshotId, ct))
        {
            var rows = await DataFiles.ReadAsync(table.IO, task.File, table.Schema, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, table.Schema, ct);
            var kept = rows.Where((r, pos) => isDeleted?.Invoke(pos, r) != true && matches?.Invoke(r) != false).ToList();
            if (kept.Count > 0) yield return ArrowInterop.ToRecordBatch(table.Schema, kept);
        }
    }

    public static Task<MutationResult> InsertAsync(Table table, RecordBatch batch, CancellationToken ct = default) =>
        TableMutations.InsertAsync(table, ArrowInterop.ToRows(table.Schema, batch), ct);

    public static Task<MutationResult> UpsertAsync(Table table, RecordBatch batch, IReadOnlyList<string>? keyColumns = null,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default) =>
        TableMutations.UpsertAsync(table, ArrowInterop.ToRows(table.Schema, batch), keyColumns, strategy, ct);

    public static Task<MutationResult> ReplaceGroupsAsync(Table table, RecordBatch batch, IReadOnlyList<string> groupColumns,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default) =>
        TableMutations.ReplaceGroupsAsync(table, ArrowInterop.ToRows(table.Schema, batch), groupColumns, strategy, ct);
}
