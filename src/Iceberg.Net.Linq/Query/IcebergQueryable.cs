using System.Collections;
using System.Linq.Expressions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.Linq;

/// <summary>Options for <see cref="IcebergQueryableExtensions.AsQueryable{T}(Table, IcebergQueryOptions?)"/>.</summary>
public sealed record IcebergQueryOptions
{
    public static IcebergQueryOptions Default { get; } = new();

    /// <summary>
    /// Throw instead of filtering in memory: every <c>Where</c> must translate completely into the scan filter (as EF
    /// Core refuses client evaluation of filters). Sorting, paging and projection still run in memory.
    /// </summary>
    public bool Strict { get; init; }

    /// <summary>How the default reflection mapper matches members to columns. Ignored when a mapper is passed.</summary>
    public IcebergMapperOptions Mapping { get; init; } = IcebergMapperOptions.Default;
}

/// <summary>A LINQ query over an Iceberg table. Translated to one <see cref="ScanRequest"/> when it runs.</summary>
public sealed class IcebergQueryable<T> : IOrderedQueryable<T>, IAsyncEnumerable<T>
{
    internal IcebergQueryable(QuerySource<T> source)
    {
        Source = source;
        Expression = System.Linq.Expressions.Expression.Constant(this);
    }

    internal IcebergQueryable(Expression expression) => Expression = expression;

    /// <summary>Set on the root query only.</summary>
    internal QuerySource<T>? Source { get; }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider => IcebergQueryProvider.Instance;

    public IEnumerator<T> GetEnumerator() => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        IcebergQueryProvider.RootOf(Expression).ExecuteAsync<T>(Expression, cancellationToken).GetAsyncEnumerator(cancellationToken);

    public override string ToString() => IcebergQueryProvider.RootOf(Expression).Describe(Expression);
}

/// <summary>The table, mapper, options and snapshot a query reads; non-generic face for the provider.</summary>
internal interface IQuerySource
{
    TResult Execute<TResult>(Expression expression);
    IAsyncEnumerable<TElement> ExecuteAsync<TElement>(Expression expression, CancellationToken ct);
    /// <summary>Rows of the sequence <paramref name="expression"/>, counting at most <paramref name="stopAt"/>.</summary>
    Task<long> CountAsync<TElement>(Expression expression, long stopAt, CancellationToken ct);
    ScanRequest ToScanRequest(Expression expression);
    string Describe(Expression expression);
    IQuerySource WithSnapshot(SnapshotSelector snapshot);
    object RootQueryable { get; }
}

internal sealed class IcebergQueryProvider : IQueryProvider
{
    public static readonly IcebergQueryProvider Instance = new();

    public IQueryable CreateQuery(Expression expression)
    {
        var element = ElementTypeOf(expression.Type) ?? throw new ArgumentException($"{expression.Type} is not a sequence");
        return (IQueryable)Activator.CreateInstance(typeof(IcebergQueryable<>).MakeGenericType(element),
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, null, [expression], null)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => new IcebergQueryable<TElement>(expression);

    public object? Execute(Expression expression) =>
        typeof(IcebergQueryProvider).GetMethod(nameof(ExecuteGeneric), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(expression.Type).Invoke(this, [expression]);

    private object? ExecuteGeneric<TResult>(Expression expression) => Execute<TResult>(expression);

    public TResult Execute<TResult>(Expression expression) => RootOf(expression).Execute<TResult>(expression);

    /// <summary>The query source at the root of <paramref name="expression"/>.</summary>
    public static IQuerySource RootOf(Expression expression)
    {
        var finder = new RootFinder();
        finder.Visit(expression);
        return finder.Root ?? throw new InvalidOperationException("The query has no Iceberg table at its root");
    }

    /// <summary><paramref name="expression"/> with its root replaced by <paramref name="root"/>.</summary>
    public static Expression ReplaceRoot(Expression expression, IQuerySource root) => new RootReplacer(root).Visit(expression);

    internal static Type? ElementTypeOf(Type seq)
    {
        if (seq.IsGenericType && seq.GetGenericTypeDefinition() == typeof(IEnumerable<>)) return seq.GetGenericArguments()[0];
        return seq.GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            .Select(i => i.GetGenericArguments()[0]).FirstOrDefault();
    }

    internal static IQuerySource? SourceOf(object? value) =>
        value?.GetType() is { IsGenericType: true } t && t.GetGenericTypeDefinition() == typeof(IcebergQueryable<>)
            ? (IQuerySource?)t.GetProperty(nameof(IcebergQueryable<int>.Source), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(value)
            : null;

    private sealed class RootFinder : ExpressionVisitor
    {
        public IQuerySource? Root { get; private set; }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (SourceOf(node.Value) is { } s)
            {
                if (Root is not null && !ReferenceEquals(Root, s))
                    throw new NotSupportedException("A query may read one Iceberg table; join in memory after ToListAsync()");
                Root = s;
            }
            return node;
        }
    }

    private sealed class RootReplacer(IQuerySource root) : ExpressionVisitor
    {
        protected override Expression VisitConstant(ConstantExpression node) =>
            SourceOf(node.Value) is not null ? Expression.Constant(root.RootQueryable, node.Type) : node;
    }
}
