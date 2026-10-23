using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Linq.Query;
using Iceberg.Net.Types;
using IcebergExpression = Iceberg.Net.Expressions.Expression;

namespace Iceberg.Net.Linq;

/// <summary>
/// How a query runs: the operator chain is split into the part the scan does (filters, column projection, snapshot)
/// and the part LINQ to Objects does over the streamed rows (residual filters, ordering, paging, projection).
/// </summary>
internal sealed class QueryPlan<T>
{
    public required ScanRequest Request { get; init; }
    public required Schema RowSchema { get; init; }
    /// <summary>Filters (on <typeparamref name="T"/>) moved into the scan; <see cref="Residual"/> are those that only partly translated.</summary>
    public required IReadOnlyList<LambdaExpression> Pushed { get; init; }
    public required IReadOnlyList<LambdaExpression> Residual { get; init; }
    /// <summary>Operators that run in memory over the scanned rows, innermost first.</summary>
    public required IReadOnlyList<MethodCallExpression> InMemory { get; init; }
    public MethodCallExpression? Terminal { get; init; }

    public Func<T, bool>? ResidualFilter { get; init; }

    /// <summary><paramref name="source"/> with <see cref="InMemory"/> (and the terminal, if <paramref name="withTerminal"/>) applied.</summary>
    public Expression Rebuild(Expression source, bool withTerminal)
    {
        var current = source;
        foreach (var op in InMemory) current = Expression.Call(op.Method, [current, .. op.Arguments.Skip(1)]);
        if (withTerminal && Terminal is not null) current = Expression.Call(Terminal.Method, [current, .. Terminal.Arguments.Skip(1)]);
        return current;
    }
}

internal sealed class QuerySource<T> : IQuerySource
{
    private static readonly HashSet<string> OrderingOps = ["OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending"];
    private static readonly HashSet<string> PredicateTerminals = ["First", "FirstOrDefault", "Single", "SingleOrDefault", "Last", "LastOrDefault", "Any", "Count", "LongCount"];
    private static readonly HashSet<string> CountingTerminals = ["Count", "LongCount", "Any"];

    private (Schema Schema, string? Mapping, IIcebergRowBinding<T> Binding)? _binding;

    public QuerySource(Table table, IIcebergRowMapper<T> mapper, IcebergQueryOptions options, SnapshotSelector snapshot)
    {
        Table = table;
        Mapper = mapper;
        Options = options;
        Snapshot = snapshot;
        RootQueryable = new IcebergQueryable<T>(this);
    }

    public Table Table { get; }
    public IIcebergRowMapper<T> Mapper { get; }
    public IcebergQueryOptions Options { get; }
    public SnapshotSelector Snapshot { get; }
    public object RootQueryable { get; }

    public IQuerySource WithSnapshot(SnapshotSelector snapshot) => new QuerySource<T>(Table, Mapper, Options, snapshot);

    /// <summary>The mapper bound to the table's current schema (re-bound when the schema or name mapping changes).</summary>
    public IIcebergRowBinding<T> Binding
    {
        get
        {
            var schema = Table.Schema;
            var mapping = Table.Metadata.Properties.GetValueOrDefault(NameMapping.Property);
            if (_binding is { } b && ReferenceEquals(b.Schema, schema) && b.Mapping == mapping) return b.Binding;
            var bound = Mapper.Bind(schema, mapping is null ? null : NameMapping.Parse(mapping));
            _binding = (schema, mapping, bound);
            return bound;
        }
    }

    // ---------- planning ----------

    public QueryPlan<T> Plan(Expression expression)
    {
        var sequence = typeof(IQueryable).IsAssignableFrom(expression.Type);
        var ops = new List<MethodCallExpression>();
        var e = expression;
        while (e is MethodCallExpression mc && mc.Method.DeclaringType == typeof(Queryable))
        {
            ops.Insert(0, mc);
            e = mc.Arguments[0];
        }
        if (e is not ConstantExpression c || IcebergQueryProvider.SourceOf(c.Value) is null)
            throw new NotSupportedException($"Cannot translate {e}: only System.Linq.Queryable operators are supported over an Iceberg table");

        MethodCallExpression? terminal = null;
        if (!sequence)
        {
            terminal = ops[^1];
            ops.RemoveAt(ops.Count - 1);
            if (PredicateTerminals.Contains(terminal.Method.Name) && terminal.Arguments.Count == 2 && Lambda(terminal.Arguments[1]) is { Parameters.Count: 1 } predicate
                && predicate.ReturnType == typeof(bool))
            {
                var element = terminal.Method.GetGenericArguments()[0];
                ops.Add(Expression.Call(QueryableMethod("Where", 2, element, m => m.GetParameters()[1].ParameterType.GetGenericArguments()[0].GetGenericArguments().Length == 2),
                    terminal.Arguments[0], terminal.Arguments[1]));
                terminal = Expression.Call(QueryableMethod(terminal.Method.Name, 1, element), terminal.Arguments[0]);
            }
        }

        // Filters directly on the table's rows (only reordered so far) move into the scan; a filter after Skip/Take/Select must not.
        var pushed = new List<LambdaExpression>();
        var inMemory = new List<MethodCallExpression>();
        var pushing = true;
        foreach (var op in ops)
        {
            if (pushing && op.Method.Name == "Where" && Lambda(op.Arguments[1]) is { Parameters.Count: 1 } where && where.Parameters[0].Type == typeof(T))
            {
                pushed.Add(where);
                continue;
            }
            if (!OrderingOps.Contains(op.Method.Name)) pushing = false;
            inMemory.Add(op);
        }

        var metadata = Table.Metadata;
        var readSchema = TimeTravel.SchemaFor(metadata, TimeTravel.Resolve(metadata, Snapshot));
        var binding = Binding;
        var translator = new PredicateTranslator(binding.Members, readSchema, metadata.CurrentSchema);
        var filter = IcebergExpression.True;
        var residual = new List<LambdaExpression>();
        foreach (var where in pushed)
        {
            foreach (var conjunct in PredicateTranslator.Conjuncts(where.Body))
            {
                var lambda = Expression.Lambda(conjunct, where.Parameters);
                var t = translator.Translate(lambda);
                filter = Iceberg.Net.Expressions.Expr.And(filter, t.Filter);
                if (!t.Exact) residual.Add(lambda);
            }
        }
        if (Options.Strict)
        {
            if (residual.Count > 0)
                throw new InvalidOperationException(
                    $"Strict Iceberg query: these filters cannot be translated into the scan and would run in memory: {string.Join("; ", residual)}. " +
                    "Rewrite them with member comparisons, Contains, StartsWith or null checks, or turn off IcebergQueryOptions.Strict.");
            if (inMemory.FirstOrDefault(o => o.Method.Name is "Where" or "SkipWhile" or "TakeWhile") is { } late)
                throw new InvalidOperationException($"Strict Iceberg query: {late.Method.Name} after paging or projection runs in memory: {late}");
        }

        var columns = Columns(binding, readSchema, inMemory, terminal, residual);
        var request = new ScanRequest { Filter = filter, Snapshot = Snapshot, Columns = columns };
        var compiled = residual.Select(r => ((Expression<Func<T, bool>>)r).Compile()).ToArray();
        return new QueryPlan<T>
        {
            Request = request,
            RowSchema = TableScan.SchemaOf(Table, request),
            Pushed = pushed,
            Residual = residual,
            InMemory = inMemory,
            Terminal = terminal,
            ResidualFilter = compiled.Length == 0 ? null : compiled.Length == 1 ? compiled[0] : item => compiled.All(f => f(item)),
        };
    }

    private static LambdaExpression? Lambda(Expression e) => e switch
    {
        UnaryExpression { NodeType: ExpressionType.Quote } q => q.Operand as LambdaExpression,
        LambdaExpression l => l,
        _ => null,
    };

    private static MethodInfo QueryableMethod(string name, int parameters, Type element, Func<MethodInfo, bool>? pick = null) =>
        typeof(Queryable).GetMethods().Where(m => m.Name == name && m.GetParameters().Length == parameters && m.GetGenericArguments().Length == 1)
            .Where(m => pick?.Invoke(m) ?? true).First().MakeGenericMethod(element);

    /// <summary>
    /// The top-level columns to read: those holding members the residual filters and in-memory operators use, while
    /// rows are still <typeparamref name="T"/>; every mapped column once a <typeparamref name="T"/> escapes (returned,
    /// or passed to an operator or method as a whole). Null when that is every column of the read schema.
    /// </summary>
    private static IReadOnlyList<string>? Columns(IIcebergRowBinding<T> binding, Schema read, IReadOnlyList<MethodCallExpression> inMemory,
        MethodCallExpression? terminal, IReadOnlyList<LambdaExpression> residual)
    {
        var needed = new HashSet<int>();
        var all = false;
        void Use(LambdaExpression l) => all |= !MemberCollector.Collect(l, binding.Members, needed);
        foreach (var r in residual) Use(r);
        var stillT = true;
        foreach (var op in inMemory)
        {
            if (!stillT || all) break;
            switch (op.Method.Name)
            {
                case "Where" or "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "TakeWhile" or "SkipWhile":
                    Use(Lambda(op.Arguments[1])!);
                    break;
                case "Skip" or "Take":
                    break;
                case "Select" when Lambda(op.Arguments[1]) is { Parameters.Count: 1 } select:
                    Use(select);
                    stillT = false;
                    break;
                default:
                    all = true;
                    break;
            }
        }
        if (stillT && !(terminal is not null && CountingTerminals.Contains(terminal.Method.Name) && terminal.Arguments.Count == 1)) all = true;

        var mapped = binding.Members.Select(m => m.Field.Id).ToHashSet();
        var ids = all ? mapped : needed;
        if (all && read.Columns.All(f => mapped.Contains(f.Id))) return null;
        return read.Columns.Where(f => ids.Contains(f.Id)).Select(f => f.Name).ToList();
    }

    /// <summary>Collects the top-level mapped members a lambda reads from its (first) parameter; false when it uses the row in any other way.</summary>
    private sealed class MemberCollector : ExpressionVisitor
    {
        private readonly ParameterExpression _p;
        private readonly IReadOnlyList<IcebergMemberBinding> _members;
        private readonly HashSet<int> _ids;
        private bool _escapes;

        private MemberCollector(ParameterExpression p, IReadOnlyList<IcebergMemberBinding> members, HashSet<int> ids) => (_p, _members, _ids) = (p, members, ids);

        public static bool Collect(LambdaExpression l, IReadOnlyList<IcebergMemberBinding> members, HashSet<int> ids)
        {
            var c = new MemberCollector(l.Parameters[0], members, ids);
            c.Visit(l.Body);
            return !c._escapes;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == _p)
            {
                var m = _members.FirstOrDefault(b => b.Member.Name == node.Member.Name);
                if (m is null) _escapes = true; // an unmapped (e.g. computed) member may read any column
                else _ids.Add(m.Field.Id);
                return node;
            }
            return base.VisitMember(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _p) _escapes = true;
            return node;
        }
    }

    // ---------- execution ----------

    private async IAsyncEnumerable<T> ScanAsync(QueryPlan<T> plan, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var read = Binding.CreateReader(plan.RowSchema);
        var residual = plan.ResidualFilter;
        await foreach (var row in TableScan.ReadRowsAsync(Table, plan.Request, ct).WithCancellation(ct))
        {
            var item = read(row);
            if (residual is null || residual(item)) yield return item;
        }
    }

    public TResult Execute<TResult>(Expression expression)
    {
        var plan = Plan(expression);
        var source = Blocking.Enumerate(ScanAsync(plan));
        if (plan.Terminal is null && plan.InMemory.Count == 0) return (TResult)source;
        if (plan.Terminal is { Arguments.Count: 1 } t && plan.InMemory.Count == 0 && CountingTerminals.Contains(t.Method.Name))
        {
            var count = Blocking.Run(() => CountScanAsync(plan, t.Method.Name == "Any" ? 1 : long.MaxValue, CancellationToken.None));
            return (TResult)(object)(t.Method.Name switch { "Any" => count > 0, "Count" => checked((int)count), _ => count });
        }
        var rebuilt = plan.Rebuild(Expression.Constant(source.AsQueryable(), typeof(IQueryable<T>)), withTerminal: true);
        return Expression.Lambda<Func<TResult>>(Expression.Convert(rebuilt, typeof(TResult))).Compile()();
    }

    private async Task<long> CountScanAsync(QueryPlan<T> plan, long stopAt, CancellationToken ct)
    {
        // TODO: answer an unfiltered Count() from snapshot summaries (total-records with no delete files) once TableScan
        // exposes a metadata count; until then rows are counted by scanning (with no columns projected).
        long n = 0;
        await foreach (var _ in ScanAsync(plan, ct))
            if (++n >= stopAt) break;
        return n;
    }

    public async Task<long> CountAsync<TElement>(Expression expression, long stopAt, CancellationToken ct)
    {
        var plan = Plan(expression);
        if (plan.InMemory.Count == 0) return await CountScanAsync(plan, stopAt, ct);
        long n = 0;
        await foreach (var _ in ExecuteAsync<TElement>(expression, ct))
            if (++n >= stopAt) break;
        return n;
    }

    public IAsyncEnumerable<TElement> ExecuteAsync<TElement>(Expression expression, CancellationToken ct)
    {
        var plan = Plan(expression);
        if (plan.InMemory.Count == 0) return (IAsyncEnumerable<TElement>)(object)ScanAsync(plan, ct);
        return AsyncPipeline.CanStream(plan.InMemory)
            ? AsyncPipeline.Stream<T, TElement>(ScanAsync(plan, ct), plan.InMemory, ct)
            : MaterializeThenRun<TElement>(plan, ct);
    }

    /// <summary>Operators that need every row (ordering, grouping, …): buffer the scan, then run them with LINQ to Objects.</summary>
    private async IAsyncEnumerable<TElement> MaterializeThenRun<TElement>(QueryPlan<T> plan, [EnumeratorCancellation] CancellationToken ct)
    {
        var rows = new List<T>();
        await foreach (var item in ScanAsync(plan, ct)) rows.Add(item);
        var rebuilt = plan.Rebuild(Expression.Constant(rows.AsQueryable(), typeof(IQueryable<T>)), withTerminal: false);
        var result = Expression.Lambda<Func<IEnumerable<TElement>>>(Expression.Convert(rebuilt, typeof(IEnumerable<TElement>))).Compile()();
        foreach (var item in result) yield return item;
    }

    public ScanRequest ToScanRequest(Expression expression) => Plan(expression).Request;

    public string Describe(Expression expression)
    {
        var plan = Plan(expression);
        var sb = new StringBuilder();
        sb.Append("Iceberg scan of ").Append(Table.Identifier).Append(" at ").AppendLine(DescribeSnapshot(Snapshot));
        sb.Append("  filter:    ").AppendLine(plan.Request.Filter.ToString());
        sb.Append("  columns:   ").AppendLine(plan.Request.Columns is null ? "*" : "[" + string.Join(", ", plan.Request.Columns) + "]");
        if (plan.Residual.Count > 0) sb.Append("  residual:  ").AppendLine(string.Join(" && ", plan.Residual.Select(r => r.ToString())));
        if (plan.InMemory.Count > 0) sb.Append("  in memory: ").AppendLine(string.Join(" -> ", plan.InMemory.Select(o => $"{o.Method.Name}({string.Join(", ", o.Arguments.Skip(1).Select(a => Lambda(a)?.ToString() ?? a.ToString()))})")));
        if (plan.Terminal is not null) sb.Append("  terminal:  ").AppendLine(plan.Terminal.Method.Name);
        return sb.ToString().TrimEnd();
    }

    private static string DescribeSnapshot(SnapshotSelector s) => s switch
    {
        SnapshotSelector.IdSelector i => $"snapshot {i.SnapshotId}",
        SnapshotSelector.RefSelector r => $"ref '{r.Name}'",
        SnapshotSelector.AsOfSelector a => $"as of {a.Instant:O}",
        _ => "current snapshot",
    };
}
