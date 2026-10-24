using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.EntityFrameworkCore.Metadata;
using Iceberg.Net.EntityFrameworkCore.Storage;
using Iceberg.Net.Linq;
using Iceberg.Net.Linq.Query;
using IcebergLinq = Iceberg.Net.Linq.IcebergQueryableExtensions;

namespace Iceberg.Net.EntityFrameworkCore.Query;

/// <summary>
/// EF Core's query compiler, replaced: the <c>DbSet</c> root of a query becomes the table's Iceberg.Net.Linq
/// queryable, EF Core's own operators are applied or removed, and the query runs in the LINQ layer (filter pushdown,
/// projection, counting from manifests, streaming). Results of the entity type are tracked unless the query is
/// no-tracking or reads an older snapshot.
/// </summary>
internal sealed class IcebergQueryCompiler(ICurrentDbContext current, IcebergSession session) : IQueryCompiler
{
    private static readonly MethodInfo ExecuteDeleteMethod = typeof(RelationalQueryableExtensions).GetMethod(nameof(RelationalQueryableExtensions.ExecuteDelete))!;
    private static readonly MethodInfo ExecuteUpdateMethod = typeof(RelationalQueryableExtensions).GetMethod(nameof(RelationalQueryableExtensions.ExecuteUpdate))!;
    private static readonly HashSet<string> Strippable =
    [
        nameof(EntityFrameworkQueryableExtensions.AsNoTracking), nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution),
        nameof(EntityFrameworkQueryableExtensions.AsTracking), nameof(EntityFrameworkQueryableExtensions.TagWith),
        nameof(EntityFrameworkQueryableExtensions.TagWithCallSite), nameof(EntityFrameworkQueryableExtensions.IgnoreQueryFilters),
        nameof(EntityFrameworkQueryableExtensions.IgnoreAutoIncludes), nameof(RelationalQueryableExtensions.AsSplitQuery),
        nameof(RelationalQueryableExtensions.AsSingleQuery),
    ];

    private DbContext Context => current.Context;

    // ---------- IQueryCompiler ----------

    public TResult Execute<TResult>(Expression query)
    {
        var prepared = Blocking.Run(() => PrepareAsync(query, CancellationToken.None));
        if (prepared.Special is { } special) return (TResult)Blocking.Run(() => special(CancellationToken.None))!;
        var t = typeof(TResult);
        var element = t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>) ? t.GetGenericArguments()[0] : null;
        var result = prepared.Root.Provider.Execute<TResult>(prepared.Expression);
        if (element is null) return (TResult)prepared.Track(result)!;
        return prepared.Tracking ? (TResult)TrackAll((System.Collections.IEnumerable)result!, element, prepared.Track) : result;
    }

    public TResult ExecuteAsync<TResult>(Expression query, CancellationToken cancellationToken = default)
    {
        var t = typeof(TResult);
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
            return (TResult)Generic(nameof(StreamAsync), t.GetGenericArguments()[0]).Invoke(this, [query, cancellationToken])!;
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Task<>))
            return (TResult)Generic(nameof(ScalarAsync), t.GetGenericArguments()[0]).Invoke(this, [query, cancellationToken])!;
        throw new NotSupportedException($"Cannot execute a query returning {t} asynchronously");
    }

    public Func<QueryContext, TResult> CreateCompiledQuery<TResult>(Expression query) =>
        throw new NotSupportedException("EF.CompileQuery is not supported by the Iceberg provider; Iceberg queries are planned per execution");

    public Func<QueryContext, TResult> CreateCompiledAsyncQuery<TResult>(Expression query) =>
        throw new NotSupportedException("EF.CompileAsyncQuery is not supported by the Iceberg provider; Iceberg queries are planned per execution");

    // ---------- execution ----------

    private async IAsyncEnumerable<TElement> StreamAsync<TElement>(Expression query, [EnumeratorCancellation] CancellationToken ct)
    {
        var prepared = await PrepareAsync(query, ct);
        var q = prepared.Root.Provider.CreateQuery<TElement>(prepared.Expression);
        await foreach (var item in IcebergLinq.AsAsyncEnumerable(q, ct).WithCancellation(ct))
            yield return prepared.Tracking ? (TElement)prepared.Track(item)! : item;
    }

    private async Task<TResult> ScalarAsync<TResult>(Expression query, CancellationToken ct)
    {
        var prepared = await PrepareAsync(query, ct);
        if (prepared.Special is { } special) return (TResult)(await special(ct))!;
        if (prepared.Expression is not MethodCallExpression { Method.IsStatic: true } terminal || terminal.Arguments.Count == 0
            || ElementOf(terminal.Arguments[0].Type) is not { } element)
            throw new NotSupportedException($"Cannot execute {query} asynchronously");
        var result = await (Task<TResult>)Generic(nameof(TerminalAsync), element, typeof(TResult)).Invoke(this, [prepared, terminal, ct])!;
        return (TResult)prepared.Track(result)!;
    }

    /// <summary>
    /// An async terminal operator over the LINQ layer: counting and existence go to its async counters (manifest
    /// counts, early stop); <c>First</c>/<c>Single</c> read at most one or two rows; anything else buffers the sequence
    /// and applies the operator in memory.
    /// </summary>
    private static async Task<TResult> TerminalAsync<TElement, TResult>(Prepared prepared, MethodCallExpression terminal, CancellationToken ct)
    {
        var provider = prepared.Root.Provider;
        var name = terminal.Method.Name;
        var source = provider.CreateQuery<TElement>(terminal.Arguments[0]);
        var predicate = terminal.Arguments.Count == 2 && Unquote(terminal.Arguments[1]) is LambdaExpression { Parameters.Count: 1 } l && l.ReturnType == typeof(bool)
            ? (Expression<Func<TElement, bool>>)l : null;
        if (name is "Count" or "LongCount" or "Any" && (terminal.Arguments.Count == 1 || predicate is not null))
        {
            if (predicate is not null) source = source.Where(predicate);
            object r = name switch
            {
                "Count" => await IcebergLinq.CountAsync(source, ct),
                "LongCount" => await IcebergLinq.LongCountAsync(source, ct),
                _ => await IcebergLinq.AnyAsync(source, ct),
            };
            return (TResult)r;
        }
        var arguments = terminal.Arguments.Skip(1).ToList();
        var method = terminal.Method;
        if (name is "First" or "FirstOrDefault" or "Single" or "SingleOrDefault" && (terminal.Arguments.Count == 1 || predicate is not null))
        {
            if (predicate is not null)
            {
                source = source.Where(predicate);
                arguments.Clear();
                method = typeof(Queryable).GetMethods().Single(m => m.Name == name && m.GetParameters().Length == 1).MakeGenericMethod(typeof(TElement));
            }
            source = source.Take(name.StartsWith("First", StringComparison.Ordinal) ? 1 : 2);
        }
        var rows = await IcebergLinq.ToListAsync(source, ct);
        var call = Expression.Call(method, [Expression.Constant(rows.AsQueryable()), .. arguments]);
        return Expression.Lambda<Func<TResult>>(Expression.Convert(call, typeof(TResult))).Compile()();
    }

    private static System.Collections.IEnumerable TrackAll(System.Collections.IEnumerable rows, Type element, Func<object?, object?> track) =>
        (System.Collections.IEnumerable)typeof(IcebergQueryCompiler).GetMethod(nameof(TrackTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(element).Invoke(null, [rows, track])!;

    private static IEnumerable<T> TrackTyped<T>(IEnumerable<T> rows, Func<object?, object?> track)
    {
        foreach (var row in rows) yield return (T)track(row)!;
    }

    // ---------- preparation ----------

    private sealed class Prepared
    {
        public required IQueryable Root { get; init; }
        public required Expression Expression { get; init; }
        public bool Tracking { get; init; }
        public Func<object?, object?> Track { get; init; } = x => x;
        /// <summary>Set for operators that are not queries (ExecuteDelete, ExecuteUpdate, inspection).</summary>
        public Func<CancellationToken, Task<object?>>? Special { get; init; }
    }

    private async Task<Prepared> PrepareAsync(Expression query, CancellationToken ct)
    {
        var scanned = PrimaryRoot(query) ?? throw new NotSupportedException($"The query has no DbSet at its root: {query}");
        var rewriter = new Rewriter(Context.Model, scanned.EntityType);
        var body = rewriter.Visit(query);
        var primary = PrimaryRoot(body)!;
        var mapping = IcebergEntityMapping.For(primary.EntityType);
        var table = await session.LoadTableAsync(mapping, ct);
        var options = new IcebergQueryOptions { Strict = session.Options.Strict };
        var root = mapping.CreateQuery(table, options, rewriter.Snapshot);
        var expression = new RootReplacer(primary, ((ConstantExpression)root.Expression).Value!, this).Visit(body);

        var timeTravel = rewriter.Snapshot is not SnapshotSelector.CurrentSelector;
        var tracking = (rewriter.Tracking ?? Context.ChangeTracker.QueryTrackingBehavior == QueryTrackingBehavior.TrackAll) && !timeTravel;
        if (tracking) session.NoteRead(table);
        Func<object?, object?> track = tracking ? mapping.Tracker(Context) : x => x;

        Func<CancellationToken, Task<object?>>? special = null;
        if (expression is MethodCallExpression { Method.IsGenericMethod: true } call)
        {
            var definition = call.Method.GetGenericMethodDefinition();
            var strategy = session.Options.DeleteStrategy;
            if (definition == ExecuteDeleteMethod)
            {
                var source = root.Provider.CreateQuery(call.Arguments[0]);
                special = async c => checked((int)((await mapping.ExecuteDeleteAsync(source, strategy, c)).RowsDeleted ?? 0));
            }
            else if (definition == ExecuteUpdateMethod)
            {
                var source = root.Provider.CreateQuery(call.Arguments[0]);
                var setters = (LambdaExpression)Unquote(call.Arguments[1]);
                special = async c => checked((int)(await mapping.ExecuteUpdateAsync(source, setters, strategy, c)).RowsUpdated);
            }
            else if (definition == IcebergEntityFrameworkQueryableExtensions.ToIcebergQueryStringMethod)
            {
                var text = (string)Generic(nameof(Describe), call.Method.GetGenericArguments()[0]).Invoke(null, [root.Provider, call.Arguments[0]])!;
                special = _ => Task.FromResult<object?>(text);
            }
            else if (definition == IcebergEntityFrameworkQueryableExtensions.ToIcebergScanRequestMethod)
            {
                var request = Generic(nameof(ScanRequestOf), call.Method.GetGenericArguments()[0]).Invoke(null, [root.Provider, call.Arguments[0]]);
                special = _ => Task.FromResult(request);
            }
        }
        return new Prepared { Root = root, Expression = expression, Tracking = tracking && special is null, Track = special is null ? track : x => x, Special = special };
    }

    private static string Describe<T>(IQueryProvider provider, Expression source) => IcebergLinq.ToQueryString(provider.CreateQuery<T>(source));

    private static ScanRequest ScanRequestOf<T>(IQueryProvider provider, Expression source) => IcebergLinq.ToScanRequest(provider.CreateQuery<T>(source));

    /// <summary>The <c>DbSet</c> the operator chain starts from: the table the query scans.</summary>
    private static EntityQueryRootExpression? PrimaryRoot(Expression e)
    {
        while (e is MethodCallExpression mc && (mc.Object ?? mc.Arguments.FirstOrDefault()) is { } source) e = source;
        return e as EntityQueryRootExpression;
    }

    /// <summary>
    /// Another <c>DbSet</c> used as an operator argument (<c>Join</c>, <c>Concat</c>, …) runs as its own query when the
    /// in-memory part of the plan enumerates it.
    /// </summary>
    private IEnumerable<T> SecondarySet<T>() where T : class
    {
        foreach (var item in Context.Set<T>()) yield return item;
    }

    /// <summary>Removes EF Core's query-shaping operators (recording tracking and the snapshot) and resolves <c>EF.Property</c>.</summary>
    /// <param name="scanned">The entity type of the scanned set: what <c>EF.Property</c> on an <c>object</c> refers to (EF Core's own key lookups use that shape).</param>
    private sealed class Rewriter(IModel model, IEntityType scanned) : ExpressionVisitor
    {
        public bool? Tracking { get; private set; }
        public SnapshotSelector Snapshot { get; private set; } = SnapshotSelector.Current;

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var m = node.Method;
            if (m.DeclaringType == typeof(EntityFrameworkQueryableExtensions) || m.DeclaringType == typeof(RelationalQueryableExtensions))
            {
                if (m.Name is "Include" or "ThenInclude")
                    throw new NotSupportedException("Include is not supported by the Iceberg provider: entities have no navigations");
                if (Strippable.Contains(m.Name))
                {
                    if (m.Name is nameof(EntityFrameworkQueryableExtensions.AsNoTracking) or nameof(EntityFrameworkQueryableExtensions.AsNoTrackingWithIdentityResolution))
                        Tracking = false;
                    else if (m.Name == nameof(EntityFrameworkQueryableExtensions.AsTracking))
                        Tracking = node.Arguments.Count == 1 || Evaluate<QueryTrackingBehavior>(node.Arguments[1]) == QueryTrackingBehavior.TrackAll;
                    return Visit(node.Arguments[0]);
                }
            }
            if (m.DeclaringType == typeof(IcebergEntityFrameworkQueryableExtensions) && m.Name is nameof(IcebergEntityFrameworkQueryableExtensions.AsOfSnapshot)
                    or nameof(IcebergEntityFrameworkQueryableExtensions.AsOfRef))
            {
                var value = Evaluate<object>(node.Arguments[1]);
                var selector = value switch
                {
                    DateTimeOffset instant => SnapshotSelector.AsOf(instant),
                    long id => SnapshotSelector.Id(id),
                    string name => SnapshotSelector.Ref(name),
                    _ => throw new NotSupportedException($"Unexpected snapshot selector {value}"),
                };
                if (Snapshot is not SnapshotSelector.CurrentSelector && !Snapshot.Equals(selector))
                    throw new NotSupportedException("A query may select one snapshot");
                Snapshot = selector;
                return Visit(node.Arguments[0]);
            }
            if (m.DeclaringType == typeof(EF) && m.Name == nameof(EF.Property) && node.Arguments[1] is ConstantExpression { Value: string name2 })
            {
                var instance = Visit(node.Arguments[0]);
                while (instance is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.TypeAs } u) instance = u.Operand;
                var entityType = model.FindEntityType(instance.Type) ?? (instance.Type == typeof(object) ? scanned : null)
                    ?? throw new NotSupportedException($"EF.Property on {instance.Type.Name}, which is not an entity type of the model");
                var property = entityType.FindProperty(name2)?.PropertyInfo
                    ?? throw new NotSupportedException($"EF.Property: {entityType.DisplayName()} has no CLR property {name2}");
                var target = instance.Type == entityType.ClrType ? instance : Expression.Convert(instance, entityType.ClrType);
                Expression access = Expression.Property(target, property);
                return access.Type == node.Type ? access : Expression.Convert(access, node.Type);
            }
            return base.VisitMethodCall(node);
        }

        private static T Evaluate<T>(Expression e) => (T)Expression.Lambda<Func<object>>(Expression.Convert(e, typeof(object))).Compile()();
    }

    /// <summary>Puts the Iceberg queryable where the scanned <c>DbSet</c> was, and lazy queries where other sets are.</summary>
    private sealed class RootReplacer(EntityQueryRootExpression primary, object root, IcebergQueryCompiler compiler) : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node)
        {
            if (node == primary) return Expression.Constant(root, node.Type);
            if (node is EntityQueryRootExpression other)
            {
                var rows = (System.Collections.IEnumerable)Generic(nameof(SecondarySet), other.EntityType.ClrType).Invoke(compiler, null)!;
                return Expression.Constant(Queryable.AsQueryable(rows), node.Type);
            }
            return base.VisitExtension(node);
        }
    }

    // ---------- helpers ----------

    private static MethodInfo Generic(string name, params Type[] args) =>
        typeof(IcebergQueryCompiler).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static)!.MakeGenericMethod(args);

    private static Type? ElementOf(Type t)
    {
        if (t == typeof(string)) return null;
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>)) return t.GetGenericArguments()[0];
        return t.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))?.GetGenericArguments()[0];
    }

    private static Expression Unquote(Expression e) => e is UnaryExpression { NodeType: ExpressionType.Quote } q ? q.Operand : e;
}
