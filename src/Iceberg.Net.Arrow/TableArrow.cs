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
    /// Matching rows as record batches (one per data-file row group, after deletes and the filter; empty batches skipped),
    /// in the current schema with field IDs. Memory is bounded by a row group.
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(Table table, Expression? filter = null, long? snapshotId = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var rows in TableScan.ReadBatchesAsync(table, filter ?? Expression.True, snapshotId, null, ct))
            yield return ArrowInterop.ToRecordBatch(table.Schema, rows);
    }

    /// <summary>
    /// <paramref name="request"/>'s rows as record batches (one per data-file row group), in
    /// <see cref="TableScan.SchemaOf"/> — the snapshot's schema narrowed to the requested columns — with field IDs.
    /// </summary>
    public static async IAsyncEnumerable<RecordBatch> ReadBatchesAsync(Table table, ScanRequest request, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var schema = TableScan.SchemaOf(table, request);
        await foreach (var rows in TableScan.ReadBatchesAsync(table, request, ct))
            yield return ArrowInterop.ToRecordBatch(schema, rows);
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
