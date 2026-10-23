using System.Runtime.CompilerServices;
using Iceberg.Net.Catalog;
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
    /// <see cref="SchemaOf"/>. Files are planned with partition and metrics pruning; rows are produced one data file at a
    /// time, so memory is bounded by a file rather than the result.
    /// </summary>
    public static async IAsyncEnumerable<object?[]> ReadRowsAsync(Table table, ScanRequest request, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = TimeTravel.Resolve(m, request.Snapshot);
        if (snapshot is null) yield break;
        var schema = TimeTravel.SchemaFor(m, snapshot);
        var projected = Project(schema, request.Columns);
        var positions = projected.Columns.Select(f => schema.Columns.Select((c, i) => (c, i)).First(p => p.c.Id == f.Id).i).ToArray();
        var bound = Binder.Bind(schema, request.Filter);
        var matches = bound is AlwaysTrue ? null : RowEvaluator.Compile(bound);
        var deletes = await DeletesAsync(table, snapshot.SnapshotId, ct);
        var mapping = NameMapping.ForTable(m.Properties);
        foreach (var task in (await PlanAsync(table, request.Filter, snapshot.SnapshotId, ct: ct)).Tasks)
        {
            var fileRows = await DataFiles.ReadAsync(table.IO, task.File, schema, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, schema, ct);
            for (var pos = 0; pos < fileRows.Count; pos++)
            {
                var row = fileRows[pos];
                if (isDeleted?.Invoke(pos, row) == true || matches?.Invoke(row) == false) continue;
                yield return request.Columns is null ? row : Array.ConvertAll(positions, i => row[i]);
            }
        }
    }
}
