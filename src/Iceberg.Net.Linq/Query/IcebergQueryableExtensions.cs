using System.Linq.Expressions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Linq.Query;

namespace Iceberg.Net.Linq;

/// <summary>Entry points and async terminals for LINQ over Iceberg tables.</summary>
public static class IcebergQueryableExtensions
{
    /// <summary>Query <paramref name="table"/> as <typeparamref name="T"/>, mapped with <see cref="ReflectionRowMapper{T}"/>.</summary>
    public static IQueryable<T> AsQueryable<T>(this Table table, IcebergQueryOptions? options = null)
    {
        options ??= IcebergQueryOptions.Default;
        return table.AsQueryable(new ReflectionRowMapper<T>(options.Mapping), options);
    }

    /// <summary>Query <paramref name="table"/> as <typeparamref name="T"/> with a custom (e.g. source-generated) mapper.</summary>
    public static IQueryable<T> AsQueryable<T>(this Table table, IIcebergRowMapper<T> mapper, IcebergQueryOptions? options = null) =>
        (IQueryable<T>)new QuerySource<T>(table, mapper, options ?? IcebergQueryOptions.Default, SnapshotSelector.Current).RootQueryable;

    // ---------- time travel ----------

    /// <summary>Read the table as it was at <paramref name="instant"/> (Spark's <c>TIMESTAMP AS OF</c>), with that snapshot's schema.</summary>
    public static IQueryable<T> AsOf<T>(this IQueryable<T> query, DateTimeOffset instant) => WithSnapshot(query, SnapshotSelector.AsOf(instant));

    /// <summary>Read one snapshot (Spark's <c>VERSION AS OF id</c>).</summary>
    public static IQueryable<T> AtSnapshot<T>(this IQueryable<T> query, long snapshotId) => WithSnapshot(query, SnapshotSelector.Id(snapshotId));

    /// <summary>Read the head of a branch or a tag (Spark's <c>VERSION AS OF 'name'</c>).</summary>
    public static IQueryable<T> AtRef<T>(this IQueryable<T> query, string name) => WithSnapshot(query, SnapshotSelector.Ref(name));

    private static IQueryable<T> WithSnapshot<T>(IQueryable<T> query, SnapshotSelector snapshot)
    {
        var root = Root(query).WithSnapshot(snapshot);
        return query.Provider.CreateQuery<T>(IcebergQueryProvider.ReplaceRoot(query.Expression, root));
    }

    // ---------- inspection ----------

    /// <summary>The scan the query runs: filter (the translated part of every <c>Where</c>), columns and snapshot.</summary>
    public static ScanRequest ToScanRequest<T>(this IQueryable<T> query) => Root(query).ToScanRequest(query.Expression);

    /// <summary>A readable plan: the scan, the residual filters and the operators that run in memory.</summary>
    public static string ToQueryString<T>(this IQueryable<T> query) => Root(query).Describe(query.Expression);

    // ---------- async terminals ----------

    /// <summary>Stream the results. Filters, projection and paging stream; ordering buffers the scanned rows first.</summary>
    public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IQueryable<T> query, CancellationToken ct = default) =>
        IsIceberg(query) ? Root(query).ExecuteAsync<T>(query.Expression, ct) : query as IAsyncEnumerable<T> ?? ToAsync(query);

    public static async Task<List<T>> ToListAsync<T>(this IQueryable<T> query, CancellationToken ct = default)
    {
        var list = new List<T>();
        await foreach (var item in query.AsAsyncEnumerable(ct).WithCancellation(ct)) list.Add(item);
        return list;
    }

    public static async Task<T[]> ToArrayAsync<T>(this IQueryable<T> query, CancellationToken ct = default) => [.. await query.ToListAsync(ct)];

    public static Task<T?> FirstOrDefaultAsync<T>(this IQueryable<T> query, CancellationToken ct = default) => FirstCoreAsync(query, orDefault: true, ct);

    public static Task<T?> FirstOrDefaultAsync<T>(this IQueryable<T> query, Expression<Func<T, bool>> predicate, CancellationToken ct = default) =>
        FirstCoreAsync(query.Where(predicate), orDefault: true, ct);

    public static async Task<T> FirstAsync<T>(this IQueryable<T> query, CancellationToken ct = default) => (await FirstCoreAsync(query, orDefault: false, ct))!;

    public static async Task<T> FirstAsync<T>(this IQueryable<T> query, Expression<Func<T, bool>> predicate, CancellationToken ct = default) =>
        (await FirstCoreAsync(query.Where(predicate), orDefault: false, ct))!;

    public static async Task<T?> SingleOrDefaultAsync<T>(this IQueryable<T> query, CancellationToken ct = default) => (await SingleCoreAsync(query, true, ct)).Item;

    public static async Task<T> SingleAsync<T>(this IQueryable<T> query, CancellationToken ct = default) => (await SingleCoreAsync(query, false, ct)).Item!;

    public static async Task<int> CountAsync<T>(this IQueryable<T> query, CancellationToken ct = default) => checked((int)await query.LongCountAsync(ct));

    public static Task<int> CountAsync<T>(this IQueryable<T> query, Expression<Func<T, bool>> predicate, CancellationToken ct = default) =>
        query.Where(predicate).CountAsync(ct);

    public static async Task<long> LongCountAsync<T>(this IQueryable<T> query, CancellationToken ct = default)
    {
        if (IsIceberg(query)) return await Root(query).CountAsync<T>(query.Expression, long.MaxValue, ct);
        return query.LongCount();
    }

    public static async Task<bool> AnyAsync<T>(this IQueryable<T> query, CancellationToken ct = default) =>
        IsIceberg(query) ? await Root(query).CountAsync<T>(query.Expression, 1, ct) > 0 : query.Any();

    public static Task<bool> AnyAsync<T>(this IQueryable<T> query, Expression<Func<T, bool>> predicate, CancellationToken ct = default) =>
        query.Where(predicate).AnyAsync(ct);

    private static async Task<T?> FirstCoreAsync<T>(IQueryable<T> query, bool orDefault, CancellationToken ct)
    {
        await foreach (var item in query.Take(1).AsAsyncEnumerable(ct).WithCancellation(ct)) return item;
        return orDefault ? default : throw new InvalidOperationException("Sequence contains no elements");
    }

    private static async Task<(bool Found, T? Item)> SingleCoreAsync<T>(IQueryable<T> query, bool orDefault, CancellationToken ct)
    {
        (bool, T?) result = (false, default);
        await foreach (var item in query.Take(2).AsAsyncEnumerable(ct).WithCancellation(ct))
        {
            if (result.Item1) throw new InvalidOperationException("Sequence contains more than one element");
            result = (true, item);
        }
        if (!result.Item1 && !orDefault) throw new InvalidOperationException("Sequence contains no elements");
        return result;
    }

    private static bool IsIceberg<T>(IQueryable<T> query) => query.Provider is IcebergQueryProvider;

    internal static IQuerySource Root<T>(IQueryable<T> query) => IsIceberg(query)
        ? IcebergQueryProvider.RootOf(query.Expression)
        : throw new ArgumentException("Not an Iceberg query: start from table.AsQueryable<T>()", nameof(query));

    private static async IAsyncEnumerable<T> ToAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items) yield return item;
        await Task.CompletedTask;
    }
}
