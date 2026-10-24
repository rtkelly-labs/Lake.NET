using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Column assignments for an update: a constant, or a value computed from the row being updated. A column is a
/// top-level column or a field nested in structs, by its dotted path (<c>address.city</c>).
/// </summary>
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
/// The clauses of a MERGE, applied like SQL's: for a matched pair, the first <c>WHEN MATCHED</c> clause (update or
/// delete, in the order they were added) whose condition holds; for an unmatched source row, the first
/// <c>WHEN NOT MATCHED</c> clause whose condition holds. Conditions see the target row and, for matched and not-matched
/// rows, the source row.
/// </summary>
public sealed class MergeBuilder
{
    /// <summary>A <c>WHEN MATCHED</c> clause: delete when both <see cref="Set"/> and <see cref="Row"/> are null.</summary>
    internal sealed record MatchedClause(Func<object?[], object?[], bool>? When, Action<RowUpdate>? Set, Func<object?[], object?[], object?[]>? Row)
    {
        public bool IsDelete => Set is null && Row is null;
    }

    /// <summary>Matched updates and deletes in one list, in the order they were added: the first that applies wins.</summary>
    internal List<MatchedClause> Matched { get; } = [];
    internal List<(Func<object?[], bool>? When, Func<object?[], object?[]>? Row)> NotMatchedInserts { get; } = [];
    internal List<Expression?> NotMatchedBySourceDeletes { get; } = [];

    /// <summary><c>WHEN MATCHED [AND when(target, source)] THEN UPDATE SET …</c>; assignments see the target row as it was.</summary>
    public MergeBuilder WhenMatchedUpdate(Action<RowUpdate> set, Func<object?[], object?[], bool>? when = null)
    {
        Matched.Add(new MatchedClause(when, set ?? throw new ArgumentNullException(nameof(set)), null));
        return this;
    }

    /// <summary>
    /// <c>WHEN MATCHED [AND when(target, source)] THEN UPDATE SET *</c>, generalised: the updated row is
    /// <paramref name="row"/>(target, source), aligned to the schema's columns. <c>(t, s) =&gt; s</c> is <c>SET *</c>.
    /// </summary>
    public MergeBuilder WhenMatchedUpdate(Func<object?[], object?[], object?[]> row, Func<object?[], object?[], bool>? when = null)
    {
        Matched.Add(new MatchedClause(when, null, row ?? throw new ArgumentNullException(nameof(row))));
        return this;
    }

    /// <summary><c>WHEN MATCHED [AND when(target, source)] THEN DELETE</c>.</summary>
    public MergeBuilder WhenMatchedDelete(Func<object?[], object?[], bool>? when = null)
    {
        Matched.Add(new MatchedClause(when, null, null));
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
    /// <summary>Java's property for the row-removal mode of <c>UPDATE</c> (<c>copy-on-write</c> unless <c>merge-on-read</c>).</summary>
    public const string UpdateModeProperty = "write.update.mode";

    /// <summary>
    /// <c>UPDATE … SET … WHERE filter</c> as one snapshot: matching live rows are removed with <paramref name="strategy"/>
    /// (never equality deletes, which cannot express a filter: <see cref="ValidationException"/>) and re-inserted with
    /// the assignments applied to the row as it was. <see cref="DeleteStrategy.Default"/> follows
    /// <c>write.update.mode</c>. Only files the filter can reach are read. On v3 updated rows keep their
    /// <c>_row_id</c> and take the new snapshot's sequence number as <c>_last_updated_sequence_number</c>. Re-planned
    /// from scratch on a concurrent commit, like every mutation. Nothing matching commits nothing (null snapshot).
    /// </summary>
    public static async Task<MergeResult> UpdateAsync(Table table, Expression filter, Action<RowUpdate> set,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var (resolved, plan) = PlanUpdate(table, filter, set, strategy);
        var (snapshot, updated, attempts) = await RetryAsync(table, () => plan(ct), ct);
        return new MergeResult(snapshot, updated, 0, 0, resolved, attempts);
    }

    /// <summary>
    /// <see cref="UpdateAsync"/> planned and written but not committed: commit it, with changes to other tables, through
    /// <see cref="StagedCommit.CommitAllAsync"/>, which plans it again after a lost race. <see cref="StagedCommit{TResult}.Result"/>
    /// holds the counts of the current plan.
    /// </summary>
    public static async Task<StagedCommit<MergeResult>> StageUpdateAsync(Table table, Expression filter, Action<RowUpdate> set,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var (resolved, plan) = PlanUpdate(table, filter, set, strategy);
        var staged = new StagedCommit<MergeResult>(table, async c => { var (p, o) = await plan(c); return (p, o); },
            (snapshot, o, attempts) => new MergeResult(snapshot, (long?)o ?? 0, 0, 0, resolved, attempts));
        await staged.PlanAsync(ct);
        return staged;
    }

    /// <summary>Validates an UPDATE and returns its strategy and the plan of one attempt against the table's current state.</summary>
    private static (DeleteStrategy Strategy, Func<CancellationToken, Task<(SnapshotProducer? Producer, long Updated)>> Plan) PlanUpdate(
        Table table, Expression filter, Action<RowUpdate> set, DeleteStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentNullException.ThrowIfNull(set);
        var resolved = Resolve(table, strategy, UpdateModeProperty);
        if (resolved == DeleteStrategy.EqualityDeletes)
            throw new ValidationException("UPDATE cannot use equality deletes: they delete by key and cannot express a filter; use CopyOnWrite or PositionDeletes");
        var matches = RowEvaluator.Compile(Binder.Bind(table.Schema, filter));
        var assign = Assignments.Of(table.Schema, set);

        return (resolved, async ct =>
        {
            if (table.Metadata.CurrentSnapshot is null) return ((SnapshotProducer?)null, 0L);
            var found = await FindMatchesAsync(table, filter, row => matches(row) ? new RowChange(assign.Apply(row, null)) : null, ct);
            if (found.Count == 0) return (null, 0L);
            var producer = SnapshotProducer.RowDelta(table).WithRetries(0);
            await StageChangesAsync(table, producer, found, resolved, null, [], ct);
            return (producer, found.Sum(m => (long)m.Replacements.Count));
        });
    }

    /// <summary>
    /// <c>MERGE INTO table USING source ON keys</c> as one snapshot. Source rows are aligned to the schema's columns;
    /// <paramref name="on"/> are top-level key columns (default: the identifier fields) compared for equality, and a
    /// null key never matches. Only files the source keys can reach are read, plus, with a not-matched-by-source
    /// clause, every file its filter can reach. A source key that appears twice in the source, and so a target row
    /// matched by two source rows, is a <see cref="ValidationException"/> before anything is written (Spark's MERGE
    /// cardinality check, applied to the source alone); one source row may match several target rows.
    /// <see cref="DeleteStrategy.EqualityDeletes"/> needs a merge whose matched clauses are unconditional and that has
    /// no not-matched-by-source clause, so that every target row of a matched key is removed. On v3 updated rows keep
    /// their <c>_row_id</c>. Re-planned from scratch on a concurrent commit; a merge that changes nothing commits nothing.
    /// </summary>
    public static async Task<MergeResult> MergeAsync(Table table, IReadOnlyList<object?[]> source, IReadOnlyList<string> on, Action<MergeBuilder> clauses,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var (resolved, plan) = PlanMerge(table, source, on, clauses, strategy);
        var (snapshot, counts, attempts) = await RetryAsync(table, () => plan(ct), ct);
        return new MergeResult(snapshot, counts.Updated, counts.Deleted, counts.Inserted, resolved, attempts);
    }

    /// <summary>
    /// <see cref="MergeAsync"/> planned and written but not committed, for an atomic commit across tables with
    /// <see cref="StagedCommit.CommitAllAsync"/>. The clauses (and the checks they make) run again on every re-plan,
    /// against the state the transaction then commits on. <see cref="StagedCommit{TResult}.Result"/> holds the counts of
    /// the current plan; its snapshot is null when the merge changes nothing.
    /// </summary>
    public static async Task<StagedCommit<MergeResult>> StageMergeAsync(Table table, IReadOnlyList<object?[]> source, IReadOnlyList<string> on,
        Action<MergeBuilder> clauses, DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var (resolved, plan) = PlanMerge(table, source, on, clauses, strategy);
        var staged = new StagedCommit<MergeResult>(table, async c => { var (p, o) = await plan(c); return (p, o); },
            (snapshot, o, attempts) => o is (long u, long d, long i)
                ? new MergeResult(snapshot, u, d, i, resolved, attempts)
                : new MergeResult(snapshot, 0, 0, 0, resolved, attempts));
        await staged.PlanAsync(ct);
        return staged;
    }

    /// <summary>Validates a MERGE (cardinality included) and returns its strategy and the plan of one attempt.</summary>
    private static (DeleteStrategy Strategy, Func<CancellationToken, Task<(SnapshotProducer? Producer, (long Updated, long Deleted, long Inserted) Counts)>> Plan) PlanMerge(
        Table table, IReadOnlyList<object?[]> source, IReadOnlyList<string> on, Action<MergeBuilder> clauses, DeleteStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(clauses);
        var schema = table.Schema;
        var builder = new MergeBuilder();
        clauses(builder);
        var resolved = Resolve(table, strategy, MergeModeProperty);
        if (resolved == DeleteStrategy.EqualityDeletes && (builder.NotMatchedBySourceDeletes.Count > 0 || builder.Matched.Any(c => c.When is not null)))
            throw new ValidationException("MERGE can use equality deletes only when every WHEN MATCHED clause is unconditional and there is no WHEN NOT MATCHED BY SOURCE clause " +
                "(an equality delete removes every row of a key); use CopyOnWrite or PositionDeletes");

        var key = Key.Resolve(schema, on, requireIdentifierDefault: true);
        var matched = builder.Matched.Select(c => (c.When, c.IsDelete, Apply: c.IsDelete ? null : c.Set is { } set ? Assignments.Of(schema, set) : Assignments.FullRow(schema, c.Row!))).ToList();
        var bySourceFilters = builder.NotMatchedBySourceDeletes.Select(f => f is null ? (_ => true) : RowEvaluator.Compile(Binder.Bind(schema, f))).ToList();

        // Source rows by key; the cardinality check runs before anything is read or written.
        var width = schema.Columns.Count;
        var bySource = new Dictionary<string, object?[]>(StringComparer.Ordinal);
        var sourceKeys = new string?[source.Count];
        var tuples = new List<object?[]>();
        for (var i = 0; i < source.Count; i++)
        {
            var row = source[i] ?? throw new ArgumentException($"Source row {i} is null");
            if (row.Length != width) throw new ArgumentException($"Source row {i} has {row.Length} values; the schema has {width} columns");
            var tuple = key.Columns.Select((c, j) => row[c] is { } v ? Assignments.Conform(key.Fields[j], v) : null).ToArray();
            if (tuple.Any(v => v is null)) continue; // a null key never matches
            var encoded = key.Encode(tuple);
            if (!bySource.TryAdd(encoded, row))
                throw new ValidationException($"MERGE cardinality violation: source key [{string.Join(", ", tuple)}] appears more than once, so a target row would match several source rows");
            sourceKeys[i] = encoded;
            tuples.Add(tuple);
        }
        var prune = key.FilterFor(tuples);
        foreach (var f in builder.NotMatchedBySourceDeletes) prune = Expr.Or(prune, f ?? Expression.True);

        return (resolved, async ct =>
        {
            // Per attempt: what this attempt found in the table state it planned against.
            var hit = new HashSet<string>(StringComparer.Ordinal);
            var hitTuples = new List<object?[]>();
            long updated = 0, deleted = 0;
            RowChange? Change(object?[] target)
            {
                var tuple = key.TupleOf(target);
                if (!tuple.Any(v => v is null) && bySource.TryGetValue(key.Encode(tuple), out var src))
                {
                    if (hit.Add(key.Encode(tuple))) hitTuples.Add(tuple);
                    foreach (var (when, isDelete, apply) in matched)
                    {
                        if (when is not null && !when(target, src)) continue;
                        if (isDelete) { deleted++; return RowChange.Delete; }
                        updated++;
                        return new RowChange(apply!.Apply(target, src));
                    }
                    return null;
                }
                foreach (var f in bySourceFilters)
                    if (f(target)) { deleted++; return RowChange.Delete; }
                return null;
            }

            var found = table.Metadata.CurrentSnapshot is null || (builder.Matched.Count == 0 && builder.NotMatchedBySourceDeletes.Count == 0 && builder.NotMatchedInserts.Count == 0)
                ? [] : await FindMatchesAsync(table, prune, Change, ct);
            var inserts = new List<object?[]>();
            for (var i = 0; i < source.Count; i++)
            {
                if (sourceKeys[i] is { } k && hit.Contains(k)) continue;
                foreach (var (when, row) in builder.NotMatchedInserts)
                {
                    if (when is not null && !when(source[i])) continue;
                    inserts.Add(Assignments.ConformRow(schema, row is null ? source[i] : row(source[i]), "inserted row"));
                    break;
                }
            }
            if (found.Count == 0 && inserts.Count == 0) return ((SnapshotProducer?)null, (0L, 0L, 0L));
            var producer = SnapshotProducer.RowDelta(table).WithRetries(0);
            var equality = resolved == DeleteStrategy.EqualityDeletes && hitTuples.Count > 0
                ? new Matcher(key, new KeySet(hit, hitTuples), null, Expression.True) : null;
            await StageChangesAsync(table, producer, found, resolved, equality, inserts, ct);
            return (producer, (updated, deleted, (long)inserts.Count));
        });
    }

    /// <summary>
    /// Stage one attempt's row changes: remove the changed rows with <paramref name="strategy"/>, then write the
    /// replacement rows (keeping, on v3, the <c>_row_id</c> of the row each replaces and leaving
    /// <c>_last_updated_sequence_number</c> null so it inherits this snapshot's sequence number, as Spark does) together
    /// with the new rows. Returns the number of data files rewritten.
    /// </summary>
    private static async Task<int> StageChangesAsync(Table table, SnapshotProducer producer, List<FileMatch> found, DeleteStrategy strategy,
        Matcher? equalityKeys, List<object?[]> inserts, CancellationToken ct)
    {
        var rewritten = 0;
        switch (strategy)
        {
            case DeleteStrategy.EqualityDeletes:
                if (equalityKeys is not null) await AddEqualityDeletesAsync(table, producer, equalityKeys, ct);
                break;
            case DeleteStrategy.PositionDeletes:
                foreach (var m in found) producer.DeletePositions(m.File.FilePath, m.Positions);
                break;
            case DeleteStrategy.CopyOnWrite:
                foreach (var m in found)
                {
                    await RewriteWithoutAsync(table, producer, m, ct);
                    rewritten++;
                }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(strategy));
        }

        var rows = new List<object?[]>();
        var lineage = new List<(long? RowId, long? Seq)>();
        foreach (var m in found)
            for (var i = 0; i < m.Replacements.Count; i++)
            {
                rows.Add(m.Replacements[i]);
                lineage.Add((m.ReplacedLineage[i].RowId, null));
            }
        foreach (var row in inserts)
        {
            rows.Add(row);
            lineage.Add((null, null));
        }
        if (rows.Count > 0)
            producer.AddFiles(await TableWriter.WriteAsync(table, rows, table.Metadata.FormatVersion >= 3 ? lineage : null, ct));
        return rewritten;
    }

    /// <summary>
    /// Compiled column assignments: each value is checked against its column's type before anything is written. A
    /// target is a top-level column or, by its dotted path (<c>address.city</c>), a field nested in structs; lists and
    /// maps are assigned whole.
    /// </summary>
    private sealed class Assignments
    {
        private readonly Schema _schema;
        private readonly (int[] Path, NestedField Field, Func<object?[], object?>? Value)[] _set;
        private readonly Func<object?[], object?[], object?[]>? _row;

        private Assignments(Schema schema, (int[], NestedField, Func<object?[], object?>?)[] set, Func<object?[], object?[], object?[]>? row)
        {
            _schema = schema;
            _set = set;
            _row = row;
        }

        public static Assignments Of(Schema schema, Action<RowUpdate> configure)
        {
            var update = new RowUpdate();
            configure(update);
            var seen = new HashSet<int>();
            var set = update.Assignments.Select(a =>
            {
                var field = schema.FindField(a.Column)
                    ?? throw new ArgumentException($"Cannot assign '{a.Column}': not a column of the table");
                var path = schema.StructPath(field.Id)?.ToArray()
                    ?? throw new ArgumentException($"Cannot assign '{a.Column}': it is inside a list or map; assign the enclosing column instead");
                if (!seen.Add(field.Id)) throw new ArgumentException($"Column '{a.Column}' is assigned more than once");
                return (path, field, (Func<object?[], object?>?)a.Value);
            }).ToArray();
            if (set.Length == 0) throw new ArgumentException("An update needs at least one assignment");
            // A struct and a field inside it: which value would win is ambiguous (Spark rejects it too).
            foreach (var (path, field, _) in set)
                foreach (var (other, otherField, _) in set)
                    if (other.Length < path.Length && path.AsSpan(0, other.Length).SequenceEqual(other))
                        throw new ArgumentException($"Cannot assign both '{schema.FindColumnName(otherField.Id)}' and its field '{schema.FindColumnName(field.Id)}'");
            return new Assignments(schema, set, null);
        }

        public static Assignments FullRow(Schema schema, Func<object?[], object?[], object?[]> row) => new(schema, [], row);

        /// <summary>
        /// The updated row: assignments evaluated against the row as it was (<paramref name="source"/> for MERGE). A
        /// nested assignment copies each enclosing struct it passes through (the old row is never mutated); a null
        /// enclosing struct becomes a struct whose other fields are null, as Spark's nested assignment does, so one
        /// of its required fields left unassigned is an <see cref="ArgumentException"/>.
        /// </summary>
        public object?[] Apply(object?[] target, object?[]? source)
        {
            if (_row is not null) return ConformRow(_schema, _row(target, source!), "updated row");
            var result = (object?[])target.Clone();
            HashSet<object?[]>? copied = null;
            List<(StructType Type, object?[] Value, string Name)>? created = null;
            foreach (var (path, field, value) in _set)
            {
                // Set(column, null) binds to the computed overload with a null delegate: treat it as the constant null.
                var conformed = Conform(field, value?.Invoke(target));
                var container = result;
                IReadOnlyList<NestedField> fields = _schema.Columns;
                for (var k = 0; k < path.Length - 1; k++)
                {
                    var structField = fields[path[k]];
                    var type = (StructType)structField.Type;
                    copied ??= new HashSet<object?[]>(ReferenceEqualityComparer.Instance);
                    var child = container[path[k]] as object?[];
                    if (child is null)
                    {
                        child = new object?[type.Fields.Count];
                        (created ??= []).Add((type, child, structField.Name));
                        copied.Add(child);
                    }
                    else if (!copied.Contains(child))
                    {
                        child = (object?[])child.Clone();
                        copied.Add(child);
                    }
                    container[path[k]] = child;
                    container = child;
                    fields = type.Fields;
                }
                container[path[^1]] = conformed;
            }
            foreach (var (type, value, name) in created ?? [])
                for (var i = 0; i < type.Fields.Count; i++)
                    if (type.Fields[i].Required && value[i] is null)
                        throw new ArgumentException($"Struct '{name}' is null in the row being updated, and its required field '{type.Fields[i].Name}' is not assigned");
            return result;
        }

        /// <summary>A copy of a whole row with every value checked against its column.</summary>
        public static object?[] ConformRow(Schema schema, object?[] row, string what)
        {
            var columns = schema.Columns;
            if (row is null || row.Length != columns.Count)
                throw new ArgumentException($"The {what} has {row?.Length ?? 0} values; the schema has {columns.Count} columns");
            var result = new object?[row.Length];
            for (var i = 0; i < row.Length; i++) result[i] = Conform(columns[i], row[i]);
            return result;
        }

        /// <summary>
        /// A value converted to the column's physical representation with the conversions filter literals use (int to
        /// long, DateOnly to days, DateTime to micros, decimals rescaled without loss…); nested values are checked by
        /// the writer. Null into a required column, or a value of the wrong type, is an <see cref="ArgumentException"/>.
        /// </summary>
        public static object? Conform(NestedField field, object? value)
        {
            if (value is null)
                return field.Required ? throw new ArgumentException($"Column '{field.Name}' is required and cannot be set to null") : null;
            if (!field.Type.IsPrimitive) return value;
            if (ValueComparer.IsNaN(value))
                return field.Type.TypeId switch
                {
                    TypeId.Float => float.NaN,
                    TypeId.Double => double.NaN,
                    _ => throw new ArgumentException($"Cannot set column '{field.Name}' ({field.Type}) to NaN"),
                };
            try
            {
                return Literals.Convert(field.Type, value, field.Name);
            }
            catch (Exception e) when (e is ArgumentException or FormatException or OverflowException or InvalidCastException)
            {
                throw new ArgumentException($"Cannot set column '{field.Name}' ({field.Type}) to {value.GetType().Name} '{value}': {e.Message}", e);
            }
        }
    }
}
