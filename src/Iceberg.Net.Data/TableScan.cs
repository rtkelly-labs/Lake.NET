using System.Globalization;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;
/// <summary>
/// v3 row lineage metadata columns (spec "Row Lineage"). A row's <c>_row_id</c> is the value stored in its data file
/// when present (rows carried through a rewrite keep their identity), otherwise the file's <c>first_row_id</c> plus
/// the row's position. <c>_last_updated_sequence_number</c> is likewise stored, or inherited from the file.
/// </summary>
public static class RowLineage
{
    public const int RowIdFieldId = 2147483540;
    public const int LastUpdatedSequenceNumberFieldId = 2147483539;

    public static readonly NestedField RowId = NestedField.Optional(RowIdFieldId, "_row_id", PrimitiveType.Long);
    public static readonly NestedField LastUpdatedSequenceNumber = NestedField.Optional(LastUpdatedSequenceNumberFieldId, "_last_updated_sequence_number", PrimitiveType.Long);

    public static Schema WithLineage(Schema schema) => new(schema.SchemaId, [.. schema.Columns, RowId, LastUpdatedSequenceNumber], schema.IdentifierFieldIds);

    public static bool IsReserved(int fieldId) => fieldId >= 2147483000;
}

public sealed record LineageRow(object?[] Row, long? RowId, long? LastUpdatedSequenceNumber);

/// <summary>Live data file in a snapshot, with the sequence number used for delete applicability.</summary>
public sealed record ScanTask(DataFile File, long DataSequenceNumber, string ManifestPath);

/// <summary>Planned data files and how much the filter pruned (manifests via partition summaries, files via partitions and metrics).</summary>
public sealed record ScanPlan(IReadOnlyList<ScanTask> Tasks, int ManifestsRead, int ManifestsSkipped, int FilesSkippedByPartition, int FilesSkippedByMetrics);

public static partial class TableScan
{
    public static async Task<IReadOnlyList<ManifestFile>> ManifestsAsync(Table table, Snapshot snapshot, CancellationToken ct = default)
    {
        if (snapshot.ManifestList is null)
            throw new NotSupportedException("v1 snapshots with an embedded 'manifests' list are not supported yet");
        return ManifestCodec.ReadManifestList(await table.IO.ReadAllAsync(snapshot.ManifestList, ct)).Manifests;
    }

    private static Snapshot? Resolve(TableMetadata m, long? snapshotId) =>
        snapshotId is { } id ? m.SnapshotById(id) ?? throw new ArgumentException($"Unknown snapshot {id}") : m.CurrentSnapshot;

    /// <summary>Plan the live data files of a snapshot (default: current).</summary>
    public static async Task<IReadOnlyList<ScanTask>> PlanFilesAsync(Table table, long? snapshotId = null, Func<DataFile, bool>? filter = null, CancellationToken ct = default) =>
        (await PlanAsync(table, Expression.True, snapshotId, filter, ct)).Tasks;

    /// <summary>Data files that may hold rows matching <paramref name="filter"/> (pruning only: rows still need filtering).</summary>
    public static async Task<IReadOnlyList<ScanTask>> PlanFilesAsync(Table table, Expression filter, long? snapshotId = null, CancellationToken ct = default) =>
        (await PlanAsync(table, filter, snapshotId, null, ct)).Tasks;

    /// <summary>
    /// Plan a scan (spec "Scan Planning"): skip manifests whose partition summaries cannot match the filter's inclusive
    /// projection, then files whose partition tuple or column metrics cannot match. <paramref name="fileFilter"/> is an
    /// extra caller predicate on files.
    /// </summary>
    public static async Task<ScanPlan> PlanAsync(Table table, Expression filter, long? snapshotId = null, Func<DataFile, bool>? fileFilter = null, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = Resolve(m, snapshotId);
        if (snapshot is null) return new ScanPlan([], 0, 0, 0, 0);
        var schema = m.CurrentSchema;
        return await PlanCoreAsync(table, snapshot, await ManifestsAsync(table, snapshot, ct), schema, Binder.Bind(schema, filter), fileFilter, ct);
    }

    /// <param name="schema">The schema <paramref name="bound"/> was bound to; partition specs and manifests are read with it.</param>
    private static async Task<ScanPlan> PlanCoreAsync(Table table, Snapshot snapshot, IReadOnlyList<ManifestFile> manifests, Schema schema, Expression bound,
        Func<DataFile, bool>? fileFilter, CancellationToken ct)
    {
        var m = table.Metadata;
        var metrics = bound is AlwaysTrue ? null : new MetricsEvaluator(bound);
        var bySpec = new Dictionary<int, (ManifestEvaluator Manifest, Func<IReadOnlyList<object?>, bool> Partition)>();
        int read = 0, skipped = 0, byPartition = 0, byMetrics = 0;
        var tasks = new List<ScanTask>();
        foreach (var manifest in manifests.Where(mf => mf.Content == ManifestContent.Data))
        {
            var spec = m.SpecById(manifest.PartitionSpecId);
            if (!bySpec.TryGetValue(spec.SpecId, out var eval))
            {
                var projected = Projections.Inclusive(spec, schema, bound);
                bySpec[spec.SpecId] = eval = (new ManifestEvaluator(projected), RowEvaluator.Compile(projected));
            }
            if (!eval.Manifest.MightMatch(manifest.Partitions)) { skipped++; continue; }
            read++;
            var contents = ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(manifest.ManifestPath, ct), spec, schema, manifest);
            foreach (var e in contents.Entries)
            {
                if (e.Status == ManifestEntryStatus.Deleted) continue;
                if (fileFilter is not null && !fileFilter(e.DataFile)) continue;
                if (!eval.Partition(e.DataFile.Partition)) { byPartition++; continue; }
                if (metrics is not null && !metrics.MightMatch(e.DataFile)) { byMetrics++; continue; }
                tasks.Add(new ScanTask(e.DataFile, e.SequenceNumber ?? 0, manifest.ManifestPath));
            }
        }
        return new ScanPlan(tasks, read, skipped, byPartition, byMetrics);
    }

    /// <summary>The delete files (v2 position deletes, v3 deletion vectors) of a snapshot.</summary>
    public static async Task<DeleteIndex> DeletesAsync(Table table, long? snapshotId = null, CancellationToken ct = default)
    {
        var snapshot = Resolve(table.Metadata, snapshotId);
        return snapshot is null ? await DeleteIndex.LoadAsync(table, [], ct) : await DeleteIndex.LoadAsync(table, await ManifestsAsync(table, snapshot, ct), ct);
    }

    /// <summary>Read live rows with their v3 row lineage (<c>_row_id</c>, <c>_last_updated_sequence_number</c>).</summary>
    public static async Task<List<LineageRow>> ReadWithLineageAsync(Table table, long? snapshotId = null, CancellationToken ct = default)
    {
        var deletes = await DeletesAsync(table, snapshotId, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var schema = table.Schema;
        var projection = RowLineage.WithLineage(schema);
        var n = schema.Columns.Count;
        var result = new List<LineageRow>();
        foreach (var task in await PlanFilesAsync(table, snapshotId, null, ct))
        {
            var fileRows = await DataFiles.ReadAsync(table.IO, task.File, projection, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, schema, ct);
            for (var pos = 0; pos < fileRows.Count; pos++)
            {
                var row = fileRows[pos][..n];
                if (isDeleted?.Invoke(pos, row) == true) continue;
                var rowId = (long?)fileRows[pos][n] ?? (task.File.FirstRowId is { } first ? first + pos : null);
                var seq = (long?)fileRows[pos][n + 1] ?? (table.Metadata.FormatVersion >= 3 ? task.DataSequenceNumber : null);
                result.Add(new LineageRow(row, rowId, seq));
            }
        }
        return result;
    }

    /// <summary>
    /// Read all rows of a snapshot, projected onto the current schema (by field ID), with position deletes,
    /// deletion vectors and equality deletes applied (merge-on-read).
    /// </summary>
    /// <remarks>
    /// Deliberately the <em>current</em> schema even for an old snapshot (rows stay aligned with <see cref="Table.Schema"/>
    /// for callers that index positionally); <see cref="ReadRowsAsync"/> with a <see cref="SnapshotSelector"/> reads a
    /// snapshot in the schema it was written with, as Spark's time travel does.
    /// </remarks>
    public static Task<List<object?[]>> ReadAllAsync(Table table, long? snapshotId = null, CancellationToken ct = default) =>
        ReadAsync(table, Expression.True, snapshotId, ct);

    /// <summary>
    /// Rows of a snapshot matching <paramref name="filter"/> (bound to the current schema), in the current schema: pruned
    /// planning and row groups, deletes applied, then the filter on every row. Streams internally; only the result list is held.
    /// </summary>
    public static async Task<List<object?[]>> ReadAsync(Table table, Expression filter, long? snapshotId = null, CancellationToken ct = default)
    {
        var rows = new List<object?[]>();
        await foreach (var batch in ReadBatchesAsync(table, filter, snapshotId, null, ct)) rows.AddRange(batch);
        return rows;
    }
}
