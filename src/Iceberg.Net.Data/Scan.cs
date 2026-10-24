using System.Runtime.CompilerServices;
using Iceberg.Net.Catalog;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>Which table state a read sees: the current snapshot, one by ID, the head of a branch or tag, or the state at an instant.</summary>
public abstract record SnapshotSelector
{
    private SnapshotSelector() { }

    public static SnapshotSelector Current { get; } = new CurrentSelector();
    public static SnapshotSelector Id(long snapshotId) => new IdSelector(snapshotId);
    /// <summary>The snapshot a branch or tag points at (Spark's <c>VERSION AS OF 'name'</c>).</summary>
    public static SnapshotSelector Ref(string name) => new RefSelector(name);
    /// <summary>The snapshot that was current on <c>main</c> at <paramref name="instant"/> (Spark's <c>TIMESTAMP AS OF</c>).</summary>
    public static SnapshotSelector AsOf(DateTimeOffset instant) => new AsOfSelector(instant);

    public sealed record CurrentSelector : SnapshotSelector;
    public sealed record IdSelector(long SnapshotId) : SnapshotSelector;
    public sealed record RefSelector(string Name) : SnapshotSelector;
    public sealed record AsOfSelector(DateTimeOffset Instant) : SnapshotSelector;
}

/// <summary>Resolves <see cref="SnapshotSelector"/>s, and the schema a snapshot was written with.</summary>
public static class TimeTravel
{
    /// <summary>The selected snapshot; null only for <see cref="SnapshotSelector.Current"/> on a table with no snapshot.</summary>
    public static Snapshot? Resolve(TableMetadata m, SnapshotSelector selector) => selector switch
    {
        SnapshotSelector.CurrentSelector => m.CurrentSnapshot,
        SnapshotSelector.IdSelector s => m.SnapshotById(s.SnapshotId) ?? throw new ArgumentException($"Unknown snapshot {s.SnapshotId}"),
        SnapshotSelector.RefSelector r => m.Refs.TryGetValue(r.Name, out var reference)
            ? m.SnapshotById(reference.SnapshotId) ?? throw new ArgumentException($"Ref '{r.Name}' points at snapshot {reference.SnapshotId}, which no longer exists")
            : throw new ArgumentException($"Unknown branch or tag '{r.Name}'"),
        // The snapshot log records when each snapshot became current on main, so a rolled-back snapshot is not chosen
        // for an instant after the rollback, as Java's SnapshotUtil.snapshotIdAsOfTime.
        SnapshotSelector.AsOfSelector a => AsOf(m, a.Instant),
        _ => throw new ArgumentOutOfRangeException(nameof(selector)),
    };

    private static Snapshot AsOf(TableMetadata m, DateTimeOffset instant)
    {
        var ms = instant.ToUnixTimeMilliseconds();
        var entry = m.SnapshotLog.LastOrDefault(e => e.TimestampMs <= ms)
            ?? throw new ArgumentException($"No snapshot was current at {instant:O}: the oldest retained history entry is " +
                (m.SnapshotLog.Count > 0 ? DateTimeOffset.FromUnixTimeMilliseconds(m.SnapshotLog[0].TimestampMs).ToString("O") : "missing"));
        return m.SnapshotById(entry.SnapshotId)
            ?? throw new ArgumentException($"Snapshot {entry.SnapshotId}, current at {instant:O}, has expired");
    }

    /// <summary>
    /// The schema to read <paramref name="snapshot"/> with: the one it was written with, as Spark reads time travel, so a
    /// column renamed or dropped later reads as it was. The current schema for <c>main</c>'s head or an empty table.
    /// </summary>
    public static Schema SchemaFor(TableMetadata m, Snapshot? snapshot) =>
        snapshot is null || snapshot.SnapshotId == m.CurrentSnapshotId || snapshot.SchemaId is not { } id ? m.CurrentSchema : m.SchemaById(id);
}

/// <summary>
/// One read: which state, which rows, which columns. The shared entry point for typed queries, time travel and row-level
/// operations, so pruning and projection improvements reach all of them.
/// </summary>
public sealed record ScanRequest
{
    /// <summary>Rows must match it; pushed down to manifests and files, then applied to every row.</summary>
    public Expression Filter { get; init; } = Expression.True;
    public SnapshotSelector Snapshot { get; init; } = SnapshotSelector.Current;
    /// <summary>Top-level column names to return, in this order; null for every column of the snapshot's schema.</summary>
    public IReadOnlyList<string>? Columns { get; init; }
    /// <summary>Optional sink for what the scan pruned and read (manifests, files, row groups, rows).</summary>
    public ScanMetrics? Metrics { get; init; }
}

/// <summary>
/// Counters filled while a scan plans and reads: how much each pruning level skipped, and how much was decoded.
/// Thread-safe; one instance may be shared by several scans to sum them.
/// </summary>
public sealed class ScanMetrics
{
    private long _manifestsRead, _manifestsSkipped, _filesPlanned, _filesByPartition, _filesByMetrics;
    private long _rowGroupsRead, _rowGroupsSkipped, _rowsDecoded, _rowsInSkippedGroups, _rowsDeleted, _rowsReturned, _maxBatchRows;

    public long ManifestsRead => Interlocked.Read(ref _manifestsRead);
    public long ManifestsSkipped => Interlocked.Read(ref _manifestsSkipped);
    /// <summary>Data files left after planning (the files actually opened).</summary>
    public long FilesPlanned => Interlocked.Read(ref _filesPlanned);
    public long FilesSkippedByPartition => Interlocked.Read(ref _filesByPartition);
    public long FilesSkippedByMetrics => Interlocked.Read(ref _filesByMetrics);
    /// <summary>Parquet row groups (and Avro blocks) decoded.</summary>
    public long RowGroupsRead => Interlocked.Read(ref _rowGroupsRead);
    /// <summary>Parquet row groups skipped because their footer statistics rule the filter out.</summary>
    public long RowGroupsSkipped => Interlocked.Read(ref _rowGroupsSkipped);
    public long RowsDecoded => Interlocked.Read(ref _rowsDecoded);
    public long RowsInSkippedGroups => Interlocked.Read(ref _rowsInSkippedGroups);
    /// <summary>Decoded rows removed by position deletes, deletion vectors or equality deletes.</summary>
    public long RowsDeleted => Interlocked.Read(ref _rowsDeleted);
    public long RowsReturned => Interlocked.Read(ref _rowsReturned);
    /// <summary>The most rows decoded at once: a row group (or Avro block), never a whole file, since reads stream.</summary>
    public long MaxBatchRows => Interlocked.Read(ref _maxBatchRows);

    internal void Planned(ScanPlan plan)
    {
        Interlocked.Add(ref _manifestsRead, plan.ManifestsRead);
        Interlocked.Add(ref _manifestsSkipped, plan.ManifestsSkipped);
        Interlocked.Add(ref _filesPlanned, plan.Tasks.Count);
        Interlocked.Add(ref _filesByPartition, plan.FilesSkippedByPartition);
        Interlocked.Add(ref _filesByMetrics, plan.FilesSkippedByMetrics);
    }

    internal void RowGroupRead(int rows)
    {
        Interlocked.Increment(ref _rowGroupsRead);
        Interlocked.Add(ref _rowsDecoded, rows);
        long max;
        while (rows > (max = Interlocked.Read(ref _maxBatchRows)) && Interlocked.CompareExchange(ref _maxBatchRows, rows, max) != max) { }
    }

    internal void RowGroupSkipped(int rows)
    {
        Interlocked.Increment(ref _rowGroupsSkipped);
        Interlocked.Add(ref _rowsInSkippedGroups, rows);
    }

    internal void Deleted(int rows) => Interlocked.Add(ref _rowsDeleted, rows);
    internal void Returned(int rows) => Interlocked.Add(ref _rowsReturned, rows);

    public override string ToString() =>
        $"manifests {ManifestsRead} read/{ManifestsSkipped} skipped, files {FilesPlanned} planned/{FilesSkippedByPartition} by partition/{FilesSkippedByMetrics} by metrics, " +
        $"row groups {RowGroupsRead} read/{RowGroupsSkipped} skipped, rows {RowsDecoded} decoded/{RowsDeleted} deleted/{RowsReturned} returned";
}

public static partial class TableScan
{
    /// <summary>The schema rows from <paramref name="request"/> follow: the snapshot's schema, narrowed to the requested columns.</summary>
    public static Schema SchemaOf(Table table, ScanRequest request) =>
        Project(TimeTravel.SchemaFor(table.Metadata, TimeTravel.Resolve(table.Metadata, request.Snapshot)), request.Columns);

    private static Schema Project(Schema schema, IReadOnlyList<string>? columns)
    {
        if (columns is null) return schema;
        var fields = columns.Select(c => schema.Columns.FirstOrDefault(f => f.Name == c)
            ?? throw new ArgumentException($"No column '{c}' in schema {schema.SchemaId}")).ToList();
        return new Schema(schema.SchemaId, fields, schema.IdentifierFieldIds.Where(id => fields.Any(f => f.Id == id)));
    }

    /// <summary>
    /// Stream the live rows <paramref name="request"/> selects, with every delete applied, as arrays aligned to
    /// <see cref="SchemaOf"/>. The filter is bound to the snapshot's schema; files are planned with partition and metrics
    /// pruning, Parquet row groups are skipped on footer statistics, only the requested columns (plus those the filter and
    /// equality deletes need) are decoded, and rows are produced one row group at a time, so memory is bounded by a row
    /// group rather than a file or the result.
    /// </summary>
    public static async IAsyncEnumerable<object?[]> ReadRowsAsync(Table table, ScanRequest request, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var batch in ReadBatchesAsync(table, request, ct))
            foreach (var row in batch) yield return row;
    }

    /// <summary>As <see cref="ReadRowsAsync"/>, one batch per data-file row group (empty batches are not produced).</summary>
    public static IAsyncEnumerable<IReadOnlyList<object?[]>> ReadBatchesAsync(Table table, ScanRequest request, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = TimeTravel.Resolve(m, request.Snapshot);
        if (snapshot is null) return Empty();
        var schema = TimeTravel.SchemaFor(m, snapshot);
        var projected = Project(schema, request.Columns);
        return ScanAsync(table, snapshot, schema, projected.Columns.Select(c => c.Id).ToList(), request.Filter, request.Metrics, ct);
    }

    /// <summary>
    /// The legacy shape (<see cref="ReadAsync"/>): rows of a snapshot in the <em>current</em> schema, the filter bound to it,
    /// as row-group batches.
    /// </summary>
    public static IAsyncEnumerable<IReadOnlyList<object?[]>> ReadBatchesAsync(Table table, Expression filter, long? snapshotId = null, ScanMetrics? metrics = null,
        CancellationToken ct = default) =>
        Resolve(table.Metadata, snapshotId) is { } snapshot ? ScanAsync(table, snapshot, table.Schema, null, filter, metrics, ct) : Empty();

    /// <summary>Plan <paramref name="request"/>'s files, with its filter bound to the snapshot's schema.</summary>
    public static async Task<ScanPlan> PlanAsync(Table table, ScanRequest request, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = TimeTravel.Resolve(m, request.Snapshot);
        if (snapshot is null) return new ScanPlan([], 0, 0, 0, 0);
        var schema = TimeTravel.SchemaFor(m, snapshot);
        var plan = await PlanCoreAsync(table, snapshot, await ManifestsAsync(table, snapshot, ct), schema, Binder.Bind(schema, request.Filter), null, ct);
        request.Metrics?.Planned(plan);
        return plan;
    }

    /// <summary>
    /// Rows <paramref name="request"/> would return. With no filter and no delete files in the snapshot, the sum of
    /// the planned files' record counts from manifests, without opening a data file; otherwise a scan that decodes only
    /// the columns the filter and equality deletes need.
    /// </summary>
    public static async Task<long> CountAsync(Table table, ScanRequest request, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = TimeTravel.Resolve(m, request.Snapshot);
        if (snapshot is null) return 0;
        var schema = TimeTravel.SchemaFor(m, snapshot);
        var bound = Binder.Bind(schema, request.Filter);
        if (bound is AlwaysTrue)
        {
            var manifests = await ManifestsAsync(table, snapshot, ct);
            if ((await DeleteIndex.LoadAsync(table, manifests, ct)).IsEmpty)
            {
                var plan = await PlanCoreAsync(table, snapshot, manifests, schema, bound, null, ct);
                request.Metrics?.Planned(plan);
                return plan.Tasks.Sum(t => t.File.RecordCount);
            }
        }
        long count = 0;
        await foreach (var batch in ScanAsync(table, snapshot, schema, [], request.Filter, request.Metrics, ct)) count += batch.Count;
        return count;
    }

#pragma warning disable CS1998 // an empty async iterator
    private static async IAsyncEnumerable<IReadOnlyList<object?[]>> Empty() { yield break; }
#pragma warning restore CS1998

    /// <summary>
    /// The read path every scan shares. <paramref name="schema"/> is what the filter binds to and rows are read in;
    /// <paramref name="outputIds"/> the top-level columns returned (null: all of <paramref name="schema"/>).
    /// </summary>
    private static async IAsyncEnumerable<IReadOnlyList<object?[]>> ScanAsync(Table table, Snapshot snapshot, Schema schema, IReadOnlyList<int>? outputIds,
        Expression filter, ScanMetrics? metrics, [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var telemetry = IcebergTelemetry.StartScan(table.Identifier.ToString(), snapshot.SnapshotId);
        var bound = Binder.Bind(schema, filter);
        var manifests = await ManifestsAsync(table, snapshot, ct);
        var plan = await PlanCoreAsync(table, snapshot, manifests, schema, bound, null, ct);
        metrics?.Planned(plan);
        if (plan.Tasks.Count == 0) yield break;
        var deletes = await DeleteIndex.LoadAsync(table, manifests, ct);

        // Decode only what is returned, filtered on, or matched by equality deletes.
        var outputs = outputIds ?? schema.Columns.Select(c => c.Id).ToList();
        var needed = new HashSet<int>(outputs);
        ReferencedColumns(bound, schema, needed);
        foreach (var eq in deletes.EqualityDeletes)
            foreach (var id in eq.File.EqualityIds ?? [])
                if (schema.Columns.Any(c => c.Id == id)) needed.Add(id);
        // Plus equality-delete columns the read schema lacks (dropped since, or added after a time-travel snapshot).
        var read = deletes.WithEqualityColumns(new Schema(schema.SchemaId, schema.Columns.Where(c => needed.Contains(c.Id)), schema.IdentifierFieldIds.Where(needed.Contains)));
        var readBound = Remap(bound, schema, read);
        var matches = readBound is AlwaysTrue ? null : RowEvaluator.Compile(readBound);
        var positions = outputs.Select(id => read.Columns.Select((c, i) => (c, i)).First(x => x.c.Id == id).i).ToArray();
        var identity = positions.Length == read.Columns.Count && positions.Select((p, i) => p == i).All(x => x);

        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var options = new DataFileReadOptions { Filter = bound, Metrics = metrics };
        foreach (var task in plan.Tasks)
        {
            telemetry.FileOpened(task.File.FileSizeInBytes);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, read, ct);
            await foreach (var batch in DataFiles.ReadBatchesAsync(table.IO, task.File, read, mapping, options, ct))
            {
                var kept = new List<object?[]>(batch.Rows.Count);
                var deleted = 0;
                for (var i = 0; i < batch.Rows.Count; i++)
                {
                    var row = batch.Rows[i];
                    if (isDeleted?.Invoke(batch.FirstPosition + i, row) == true) { deleted++; continue; }
                    if (matches?.Invoke(row) == false) continue;
                    kept.Add(identity ? row : Array.ConvertAll(positions, p => row[p]));
                }
                metrics?.Deleted(deleted);
                metrics?.Returned(kept.Count);
                telemetry.Rows(kept.Count, deleted);
                if (kept.Count > 0) yield return kept;
            }
        }
    }

    /// <summary>Top-level columns a bound expression reads (a nested predicate reads its top-level struct).</summary>
    private static void ReferencedColumns(Expression bound, Schema schema, HashSet<int> into)
    {
        switch (bound)
        {
            case And a: ReferencedColumns(a.Left, schema, into); ReferencedColumns(a.Right, schema, into); break;
            case Or o: ReferencedColumns(o.Left, schema, into); ReferencedColumns(o.Right, schema, into); break;
            case BoundPredicate p: into.Add(schema.Columns[p.Position].Id); break;
        }
    }

    /// <summary>Rebind <paramref name="bound"/> from positions in <paramref name="from"/> to positions in <paramref name="to"/>, by field ID.</summary>
    private static Expression Remap(Expression bound, Schema from, Schema to) => bound switch
    {
        And a => Expr.And(Remap(a.Left, from, to), Remap(a.Right, from, to)),
        Or o => Expr.Or(Remap(o.Left, from, to), Remap(o.Right, from, to)),
        BoundPredicate p => RemapPredicate(p, from, to),
        _ => bound,
    };

    private static BoundPredicate RemapPredicate(BoundPredicate p, Schema from, Schema to)
    {
        var id = from.Columns[p.Position].Id;
        var position = to.Columns.Select((c, i) => (c, i)).First(x => x.c.Id == id).i;
        return p with { Position = position, StructPath = p.StructPath is { } path ? [position, .. path.Skip(1)] : null };
    }
}
