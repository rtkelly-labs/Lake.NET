using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Data;

/// <summary>Column assignments for an update: a constant, or a value computed from the row being updated.</summary>
public sealed class RowUpdate
{
    private readonly List<(string Column, Func<object?[], object?> Value)> _assignments = [];

    /// <summary>The assignments in the order they were made; each sees the row as it was before the update.</summary>
    public IReadOnlyList<(string Column, Func<object?[], object?> Value)> Assignments => _assignments;

    public RowUpdate Set(string column, object? value)
    {
        _assignments.Add((column, _ => value));
        return this;
    }

    public RowUpdate Set(string column, Func<object?[], object?> compute)
    {
        _assignments.Add((column, compute));
        return this;
    }
}

/// <summary>
/// The clauses of a MERGE, applied in order like SQL's: the first matching clause of a kind wins for a row. Conditions
/// see the target row and, for matched and not-matched rows, the source row.
/// </summary>
public sealed class MergeBuilder
{
    internal List<(Func<object?[], object?[], bool>? When, Action<RowUpdate> Set)> MatchedUpdates { get; } = [];
    internal List<Func<object?[], object?[], bool>?> MatchedDeletes { get; } = [];
    internal List<(Func<object?[], bool>? When, Func<object?[], object?[]>? Row)> NotMatchedInserts { get; } = [];
    internal List<Expression?> NotMatchedBySourceDeletes { get; } = [];

    /// <summary><c>WHEN MATCHED [AND when(target, source)] THEN UPDATE SET …</c>.</summary>
    public MergeBuilder WhenMatchedUpdate(Action<RowUpdate> set, Func<object?[], object?[], bool>? when = null)
    {
        MatchedUpdates.Add((when, set));
        return this;
    }

    /// <summary><c>WHEN MATCHED [AND when(target, source)] THEN DELETE</c>.</summary>
    public MergeBuilder WhenMatchedDelete(Func<object?[], object?[], bool>? when = null)
    {
        MatchedDeletes.Add(when);
        return this;
    }

    /// <summary><c>WHEN NOT MATCHED [AND when(source)] THEN INSERT</c>: the source row as-is, or <paramref name="row"/>(source).</summary>
    public MergeBuilder WhenNotMatchedInsert(Func<object?[], bool>? when = null, Func<object?[], object?[]>? row = null)
    {
        NotMatchedInserts.Add((when, row));
        return this;
    }

    /// <summary>
    /// <c>WHEN NOT MATCHED BY SOURCE [AND filter] THEN DELETE</c>. This clause reads every target row the filter can
    /// reach, not just the rows the source keys can.
    /// </summary>
    public MergeBuilder WhenNotMatchedBySourceDelete(Expression? filter = null)
    {
        NotMatchedBySourceDeletes.Add(filter);
        return this;
    }
}

/// <param name="Snapshot">Null when the merge changed nothing, so no snapshot was committed.</param>
public sealed record MergeResult(Snapshot? Snapshot, long RowsUpdated, long RowsDeleted, long RowsInserted, DeleteStrategy Strategy, int Attempts);

public static partial class TableMutations
{
    /// <summary>
    /// <c>UPDATE … SET … WHERE filter</c> as one snapshot: matching rows are removed with <paramref name="strategy"/>
    /// (never equality deletes, which cannot express a filter) and re-inserted with the assignments applied. On v3 they
    /// keep their <c>_row_id</c>. Re-planned on a concurrent commit, like every mutation.
    /// </summary>
    public static Task<MergeResult> UpdateAsync(Table table, Expression filter, Action<RowUpdate> set,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default) =>
        throw new NotImplementedException("UpdateAsync is being implemented (phase B)");

    /// <summary>
    /// <c>MERGE INTO table USING source ON keys</c> as one snapshot. A source key that matches more than one target row,
    /// or appears twice in the source, is an error, as in Spark. Re-planned on a concurrent commit.
    /// </summary>
    public static Task<MergeResult> MergeAsync(Table table, IReadOnlyList<object?[]> source, IReadOnlyList<string> on, Action<MergeBuilder> clauses,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default) =>
        throw new NotImplementedException("MergeAsync is being implemented (phase B)");
}
