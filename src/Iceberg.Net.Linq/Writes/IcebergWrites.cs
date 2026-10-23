using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Linq.Query;
using Iceberg.Net.Types;
using IcebergExpression = Iceberg.Net.Expressions.Expression;
using LinqExpression = System.Linq.Expressions.Expression;

namespace Iceberg.Net.Linq;

/// <summary>
/// Column assignments for <see cref="IcebergWrites.ExecuteUpdateAsync{T}"/>, in EF Core's <c>ExecuteUpdate</c> shape:
/// <c>s => s.SetProperty(x => x.Status, "closed").SetProperty(x => x.Qty, x => x.Qty + 1)</c>. Only ever read as an
/// expression tree; calling the methods directly throws.
/// </summary>
public sealed class SetPropertyCalls<T>
{
    private SetPropertyCalls() { }

    public SetPropertyCalls<T> SetProperty<TProperty>(Func<T, TProperty> property, TProperty value) => throw Unused();

    public SetPropertyCalls<T> SetProperty<TProperty>(Func<T, TProperty> property, Func<T, TProperty> value) => throw Unused();

    internal static InvalidOperationException Unused() => new("SetProperty is only used inside an ExecuteUpdateAsync / MergeAsync expression");
}

/// <summary>Assignments for a MERGE's matched rows: constants, values from the target row, or from the target and source rows.</summary>
public sealed class MergeSetPropertyCalls<T>
{
    private MergeSetPropertyCalls() { }

    public MergeSetPropertyCalls<T> SetProperty<TProperty>(Func<T, TProperty> property, TProperty value) => throw SetPropertyCalls<T>.Unused();

    public MergeSetPropertyCalls<T> SetProperty<TProperty>(Func<T, TProperty> property, Func<T, TProperty> value) => throw SetPropertyCalls<T>.Unused();

    /// <summary>The value from the matched target row and the source row (<c>(target, source) => …</c>).</summary>
    public MergeSetPropertyCalls<T> SetProperty<TProperty>(Func<T, TProperty> property, Func<T, T, TProperty> value) => throw SetPropertyCalls<T>.Unused();
}

/// <summary>The clauses of a typed MERGE (see <see cref="MergeBuilder"/> for the semantics).</summary>
public sealed class IcebergMergeBuilder<T>
{
    internal List<Action<MergeContext<T>, MergeBuilder>> Clauses { get; } = [];

    /// <summary><c>WHEN MATCHED [AND when(target, source)] THEN UPDATE SET …</c>.</summary>
    public IcebergMergeBuilder<T> WhenMatchedUpdate(Expression<Func<MergeSetPropertyCalls<T>, MergeSetPropertyCalls<T>>> setters, Expression<Func<T, T, bool>>? when = null)
    {
        Clauses.Add((ctx, m) => m.WhenMatchedUpdate(ctx.Setters(setters), ctx.Condition(when)));
        return this;
    }

    /// <summary><c>WHEN MATCHED [AND when(target, source)] THEN UPDATE SET *</c>: every mapped column that is not a merge key, from the source.</summary>
    public IcebergMergeBuilder<T> WhenMatchedUpdateAll(Expression<Func<T, T, bool>>? when = null)
    {
        Clauses.Add((ctx, m) => m.WhenMatchedUpdate(ctx.AllFromSource(), ctx.Condition(when)));
        return this;
    }

    /// <summary><c>WHEN MATCHED [AND when(target, source)] THEN DELETE</c>.</summary>
    public IcebergMergeBuilder<T> WhenMatchedDelete(Expression<Func<T, T, bool>>? when = null)
    {
        Clauses.Add((ctx, m) => m.WhenMatchedDelete(ctx.Condition(when)));
        return this;
    }

    /// <summary><c>WHEN NOT MATCHED [AND when(source)] THEN INSERT *</c>.</summary>
    public IcebergMergeBuilder<T> WhenNotMatchedInsert(Expression<Func<T, bool>>? when = null)
    {
        Clauses.Add((ctx, m) => m.WhenNotMatchedInsert(ctx.SourceCondition(when)));
        return this;
    }

    /// <summary><c>WHEN NOT MATCHED BY SOURCE [AND filter] THEN DELETE</c>. The filter must translate completely into an Iceberg expression.</summary>
    public IcebergMergeBuilder<T> WhenNotMatchedBySourceDelete(Expression<Func<T, bool>>? filter = null)
    {
        Clauses.Add((ctx, m) => m.WhenNotMatchedBySourceDelete(ctx.ExactFilter(filter)));
        return this;
    }
}

/// <summary>What typed merge clauses need at run time: the binding, the source rows by key, and readers.</summary>
internal sealed class MergeContext<T>(Table table, IIcebergRowBinding<T> binding, IReadOnlyList<string> keyColumns, IReadOnlyList<T> source)
{
    private readonly RowReader<T> _read = new(binding.CreateReader(table.Schema));
    private Dictionary<object?[], T>? _byKey;

    public Func<object?[], object?[], bool>? Condition(Expression<Func<T, T, bool>>? when)
    {
        if (when is null) return null;
        var f = when.Compile();
        return (target, src) => f(_read.Read(target), _read.Read(src));
    }

    public Func<object?[], bool>? SourceCondition(Expression<Func<T, bool>>? when)
    {
        if (when is null) return null;
        var f = when.Compile();
        return src => f(_read.Read(src));
    }

    public IcebergExpression? ExactFilter(Expression<Func<T, bool>>? filter)
    {
        if (filter is null) return null;
        var t = new PredicateTranslator(binding.Members, table.Schema, table.Schema).Translate(filter);
        return t.Exact ? t.Filter : throw new NotSupportedException($"WhenNotMatchedBySourceDelete needs a filter that translates completely into an Iceberg expression: {filter}");
    }

    /// <summary>The source item whose merge key equals the target row's.</summary>
    public T SourceFor(object?[] target)
    {
        if (_byKey is null)
        {
            var positions = keyColumns.Select(c => table.Schema.Columns.ToList().FindIndex(f => f.Name == c)).ToArray();
            var map = new Dictionary<object?[], T>(TupleComparer.Instance);
            foreach (var item in source)
            {
                var row = binding.ToRow(item);
                map[positions.Select(p => row[p]).ToArray()] = item;
            }
            _byKey = map;
            _keyPositions = positions;
        }
        return _byKey.TryGetValue(_keyPositions!.Select(p => target[p]).ToArray(), out var s)
            ? s
            : throw new InvalidOperationException("A matched target row has no source row with its key");
    }

    private int[]? _keyPositions;

    public Action<RowUpdate> Setters(Expression<Func<MergeSetPropertyCalls<T>, MergeSetPropertyCalls<T>>> setters) =>
        IcebergWrites.CompileSetters(table, binding, setters, _read, this);

    public Action<RowUpdate> AllFromSource() => update =>
    {
        foreach (var m in binding.Members)
        {
            if (keyColumns.Contains(m.Field.Name)) continue;
            var getter = TypeModelGetter(m);
            update.Set(m.Field.Name, row => m.ToIceberg(getter(SourceFor(row)!)));
        }
    };

    private static Func<object, object?> TypeModelGetter(IcebergMemberBinding m) =>
        o => ((System.Reflection.PropertyInfo)m.Member).GetValue(o);
}

/// <summary>Maps a full row to <typeparamref name="T"/> once, however many assignments read it.</summary>
internal sealed class RowReader<T>(Func<object?[], T> read)
{
    private readonly ConditionalWeakTable<object?[], Box> _cache = new();

    private sealed class Box(T value)
    {
        public T Value { get; } = value;
    }

    public T Read(object?[] row) => _cache.GetValue(row, r => new Box(read(r))).Value;
}

internal sealed class TupleComparer : IEqualityComparer<object?[]>
{
    public static readonly TupleComparer Instance = new();

    public bool Equals(object?[]? a, object?[]? b) =>
        a!.Length == b!.Length && a.Zip(b).All(p => p.First is byte[] x && p.Second is byte[] y ? x.AsSpan().SequenceEqual(y) : Equals(p.First, p.Second));

    public int GetHashCode(object?[] o) => o.Aggregate(17, (h, v) => HashCode.Combine(h, v is byte[] b ? b.Length : v?.GetHashCode() ?? 0));
}

/// <summary>
/// Typed writes over <c>TableMutations</c>, in the shape of EF Core's bulk API (<c>ExecuteDelete</c>,
/// <c>ExecuteUpdate</c>) so an EF Core provider can delegate to them. Each call commits one snapshot.
/// </summary>
public static class IcebergWrites
{
    /// <summary>Append <paramref name="items"/>; existing rows are not read.</summary>
    public static Task<MutationResult> InsertAsync<T>(this Table table, IEnumerable<T> items, IIcebergRowMapper<T>? mapper = null, CancellationToken ct = default) =>
        TableMutations.InsertAsync(table, Rows(table, items, mapper), ct);

    /// <summary>
    /// Insert-or-replace by key. <paramref name="key"/> selects the key members (<c>x => x.Id</c>,
    /// <c>x => new { x.Tenant, x.Id }</c>); without it, the table's identifier fields.
    /// </summary>
    public static Task<MutationResult> UpsertAsync<T>(this Table table, IEnumerable<T> items, Expression<Func<T, object?>>? key = null,
        DeleteStrategy strategy = DeleteStrategy.Default, IIcebergRowMapper<T>? mapper = null, CancellationToken ct = default)
    {
        var binding = Bind(table, mapper);
        return TableMutations.UpsertAsync(table, Rows(table, items, binding), key is null ? null : KeyColumns(binding, key), strategy, ct);
    }

    /// <summary>
    /// <c>DELETE … WHERE</c> for the query's filters. A fully translated filter deletes with file pruning
    /// (<c>DeleteWhereAsync(Expression)</c>); with a residual filter the rows are matched in .NET instead (every file is
    /// read), and strict queries throw. Only <c>Where</c> may precede it, and only on the current snapshot.
    /// </summary>
    public static Task<MutationResult> ExecuteDeleteAsync<T>(this IQueryable<T> query, DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var (source, plan) = MutationPlan(query, "ExecuteDeleteAsync");
        var table = source.Table;
        if (plan.Residual.Count == 0) return TableMutations.DeleteWhereAsync(table, plan.Request.Filter, strategy, ct);
        var schema = table.Schema;
        var read = source.Binding.CreateReader(schema);
        var pushed = RowEvaluator.Compile(Binder.Bind(schema, plan.Request.Filter));
        var residual = plan.ResidualFilter!;
        return TableMutations.DeleteWhereAsync(table, row => pushed(row) && residual(read(row)), strategy, ct);
    }

    /// <summary>
    /// <c>UPDATE … SET … WHERE</c>: the query's filters (which must translate completely) select the rows; the setters
    /// become <see cref="RowUpdate"/> assignments, value expressions evaluated over each row as <typeparamref name="T"/>.
    /// </summary>
    public static Task<MergeResult> ExecuteUpdateAsync<T>(this IQueryable<T> query, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> setters,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var (source, plan) = MutationPlan(query, "ExecuteUpdateAsync");
        if (plan.Residual.Count > 0)
            throw new NotSupportedException(
                $"ExecuteUpdateAsync needs filters that translate completely into an Iceberg expression; these do not: {string.Join("; ", plan.Residual)}");
        var table = source.Table;
        return TableMutations.UpdateAsync(table, plan.Request.Filter, CompileSetters(table, source.Binding, setters), strategy, ct);
    }

    /// <summary>
    /// <c>MERGE INTO table USING items ON key</c>: <paramref name="on"/> selects the key members like
    /// <see cref="UpsertAsync{T}"/>'s key; <paramref name="clauses"/> lists the WHEN clauses in order.
    /// </summary>
    public static Task<MergeResult> MergeAsync<T>(this Table table, IEnumerable<T> items, Expression<Func<T, object?>> on, Action<IcebergMergeBuilder<T>> clauses,
        DeleteStrategy strategy = DeleteStrategy.Default, IIcebergRowMapper<T>? mapper = null, CancellationToken ct = default)
    {
        var binding = Bind(table, mapper);
        var list = items.ToList();
        var keys = KeyColumns(binding, on);
        var typed = new IcebergMergeBuilder<T>();
        clauses(typed);
        var context = new MergeContext<T>(table, binding, keys, list);
        // Translate every clause once up front, so untranslatable filters and setters fail before anything is read.
        var probe = new MergeBuilder();
        foreach (var clause in typed.Clauses) clause(context, probe);
        return TableMutations.MergeAsync(table, Rows(table, list, binding), keys, m =>
        {
            foreach (var clause in typed.Clauses) clause(context, m);
        }, strategy, ct);
    }

    /// <summary>The <see cref="RowUpdate"/> assignments for EF Core-style setters (exposed for providers and tests).</summary>
    public static Action<RowUpdate> CompileSetters<T>(Table table, Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> setters, IIcebergRowMapper<T>? mapper = null) =>
        CompileSetters(table, Bind(table, mapper), setters);

    internal static Action<RowUpdate> CompileSetters<T>(Table table, IIcebergRowBinding<T> binding, LambdaExpression setters,
        RowReader<T>? reader = null, MergeContext<T>? merge = null)
    {
        reader ??= new RowReader<T>(binding.CreateReader(table.Schema));
        var assignments = new List<(string Column, Func<object?[], object?>? Compute, object? Constant)>();
        var calls = new List<MethodCallExpression>();
        var e = setters.Body;
        while (e is MethodCallExpression { Method.Name: "SetProperty" } call)
        {
            calls.Insert(0, call);
            e = call.Object!;
        }
        if (e != setters.Parameters[0]) throw new NotSupportedException($"Setters must be a chain of SetProperty calls: {setters}");
        var seen = new HashSet<string>();
        foreach (var call in calls)
        {
            var property = call.Arguments[0] as LambdaExpression ?? throw new NotSupportedException($"SetProperty needs a property lambda: {call}");
            var member = MemberOf(binding, property);
            var column = member.Field.Name;
            if (!seen.Add(column)) throw new ArgumentException($"'{column}' is set twice");
            var value = PartialEvaluator.Evaluate(call.Arguments[1]);
            switch (value)
            {
                case LambdaExpression { Parameters.Count: 1 } fromTarget:
                {
                    var f = fromTarget.Compile();
                    assignments.Add((column, row => member.ToIceberg(f.DynamicInvoke(reader.Read(row))), null));
                    break;
                }
                case LambdaExpression { Parameters.Count: 2 } fromBoth when merge is not null:
                {
                    var f = fromBoth.Compile();
                    assignments.Add((column, row => member.ToIceberg(f.DynamicInvoke(reader.Read(row), merge.SourceFor(row))), null));
                    break;
                }
                case ConstantExpression c:
                    assignments.Add((column, null, member.ToIceberg(c.Value)));
                    break;
                default:
                    throw new NotSupportedException($"Cannot evaluate the value for '{column}': {call.Arguments[1]}");
            }
        }
        return update =>
        {
            foreach (var (column, compute, constant) in assignments)
                if (compute is null) update.Set(column, constant);
                else update.Set(column, compute);
        };
    }

    // ---------- helpers ----------

    private static IIcebergRowBinding<T> Bind<T>(Table table, IIcebergRowMapper<T>? mapper)
    {
        var names = table.Metadata.Properties.GetValueOrDefault(NameMapping.Property);
        return (mapper ?? new ReflectionRowMapper<T>()).Bind(table.Schema, names is null ? null : NameMapping.Parse(names));
    }

    private static List<object?[]> Rows<T>(Table table, IEnumerable<T> items, IIcebergRowMapper<T>? mapper) => Rows(table, items, Bind(table, mapper));

    private static List<object?[]> Rows<T>(Table table, IEnumerable<T> items, IIcebergRowBinding<T> binding)
    {
        var mapped = binding.Members.Select(m => m.Field.Id).ToHashSet();
        var missing = table.Schema.Columns.Where(f => f.Required && f.WriteDefault is null && !mapped.Contains(f.Id)).Select(f => f.Name).ToList();
        if (missing.Count > 0)
            throw new InvalidOperationException($"{typeof(T).Name} does not map the required column(s) {string.Join(", ", missing)}");
        return items.Select(binding.ToRow).ToList();
    }

    private static IcebergMemberBinding MemberOf<T>(IIcebergRowBinding<T> binding, LambdaExpression property)
    {
        var body = property.Body is UnaryExpression { NodeType: ExpressionType.Convert } u ? u.Operand : property.Body;
        if (body is not MemberExpression m || m.Expression != property.Parameters[0])
            throw new NotSupportedException($"Expected a top-level property of {typeof(T).Name}: {property}");
        return binding.Members.FirstOrDefault(b => b.Member.Name == m.Member.Name)
            ?? throw new InvalidOperationException($"{typeof(T).Name}.{m.Member.Name} is not mapped to a column");
    }

    /// <summary>Column names from <c>x => x.Id</c> or <c>x => new { x.A, x.B }</c>.</summary>
    internal static IReadOnlyList<string> KeyColumns<T>(IIcebergRowBinding<T> binding, Expression<Func<T, object?>> key)
    {
        var body = key.Body is UnaryExpression { NodeType: ExpressionType.Convert } u ? u.Operand : key.Body;
        IEnumerable<LinqExpression> parts = body is NewExpression n ? n.Arguments : [body];
        return parts.Select(p => MemberOf(binding, LinqExpression.Lambda(p, key.Parameters)).Field.Name).ToList();
    }

    private static (QuerySource<T> Source, QueryPlan<T> Plan) MutationPlan<T>(IQueryable<T> query, string what)
    {
        if (IcebergQueryableExtensions.Root(query) is not QuerySource<T> source)
            throw new NotSupportedException($"{what} needs a query over the table's own rows ({typeof(T).Name}), with only Where before it");
        var plan = source.Plan(query.Expression);
        if (plan.InMemory.Count > 0)
            throw new NotSupportedException($"{what} supports only Where before it; found {string.Join(", ", plan.InMemory.Select(o => o.Method.Name))}");
        if (source.Snapshot is not SnapshotSelector.CurrentSelector)
            throw new NotSupportedException($"{what} changes the current table state; remove AsOf/AtSnapshot/AtRef");
        return (source, plan);
    }
}
