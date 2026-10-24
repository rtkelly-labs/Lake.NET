using System.Linq.Expressions;
using System.Reflection;
using Iceberg.Net.Data;

// Same namespace as EF Core's queryable extensions.
namespace Microsoft.EntityFrameworkCore;

/// <summary>Iceberg-specific operators for EF Core queries over the Iceberg provider.</summary>
public static class IcebergEntityFrameworkQueryableExtensions
{
    internal static readonly MethodInfo ToIcebergQueryStringMethod = typeof(IcebergEntityFrameworkQueryableExtensions).GetMethod(nameof(ToIcebergQueryString))!;
    internal static readonly MethodInfo ToIcebergScanRequestMethod = typeof(IcebergEntityFrameworkQueryableExtensions).GetMethod(nameof(ToIcebergScanRequest))!;

    private static readonly MethodInfo AsOfInstantMethod = typeof(IcebergEntityFrameworkQueryableExtensions).GetMethods()
        .Single(m => m.Name == nameof(AsOfSnapshot) && m.GetParameters()[1].ParameterType == typeof(DateTimeOffset));
    private static readonly MethodInfo AsOfIdMethod = typeof(IcebergEntityFrameworkQueryableExtensions).GetMethods()
        .Single(m => m.Name == nameof(AsOfSnapshot) && m.GetParameters()[1].ParameterType == typeof(long));
    private static readonly MethodInfo AsOfRefMethod = typeof(IcebergEntityFrameworkQueryableExtensions).GetMethod(nameof(AsOfRef))!;

    /// <summary>
    /// Read the table as it was at <paramref name="instant"/> (Spark's <c>TIMESTAMP AS OF</c>), with that snapshot's
    /// schema; like EF Core's temporal <c>TemporalAsOf</c>. Time-travel results are not tracked.
    /// </summary>
    public static IQueryable<T> AsOfSnapshot<T>(this IQueryable<T> source, DateTimeOffset instant) =>
        source.Provider.CreateQuery<T>(Expression.Call(AsOfInstantMethod.MakeGenericMethod(typeof(T)), source.Expression, Expression.Constant(instant)));

    /// <summary>Read one snapshot by ID (Spark's <c>VERSION AS OF id</c>). Not tracked.</summary>
    public static IQueryable<T> AsOfSnapshot<T>(this IQueryable<T> source, long snapshotId) =>
        source.Provider.CreateQuery<T>(Expression.Call(AsOfIdMethod.MakeGenericMethod(typeof(T)), source.Expression, Expression.Constant(snapshotId)));

    /// <summary>Read the head of a branch or a tag (Spark's <c>VERSION AS OF 'name'</c>). Not tracked.</summary>
    public static IQueryable<T> AsOfRef<T>(this IQueryable<T> source, string name) =>
        source.Provider.CreateQuery<T>(Expression.Call(AsOfRefMethod.MakeGenericMethod(typeof(T)), source.Expression, Expression.Constant(name)));

    /// <summary>The plan the Iceberg provider runs: scan filter and columns, residual filters, in-memory operators (like EF Core's <c>ToQueryString</c>).</summary>
    public static string ToIcebergQueryString<T>(this IQueryable<T> source) =>
        source.Provider.Execute<string>(Expression.Call(ToIcebergQueryStringMethod.MakeGenericMethod(typeof(T)), source.Expression));

    /// <summary>The <see cref="ScanRequest"/> (filter, columns, snapshot) the query's scan uses.</summary>
    public static ScanRequest ToIcebergScanRequest<T>(this IQueryable<T> source) =>
        source.Provider.Execute<ScanRequest>(Expression.Call(ToIcebergScanRequestMethod.MakeGenericMethod(typeof(T)), source.Expression));
}
