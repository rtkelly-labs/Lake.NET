using Microsoft.EntityFrameworkCore.Infrastructure;
using Iceberg.Net.Catalog;
using Iceberg.Net.EntityFrameworkCore.Infrastructure;

// Same namespace as EF Core's own UseXxx extensions, so they are found without an extra using.
namespace Microsoft.EntityFrameworkCore;

public static class IcebergDbContextOptionsExtensions
{
    /// <summary>
    /// Use Iceberg tables in <paramref name="namespace"/> of <paramref name="catalog"/> as the context's database. The
    /// context does not own the catalog: dispose it yourself.
    /// </summary>
    public static DbContextOptionsBuilder UseIceberg(this DbContextOptionsBuilder builder, ICatalog catalog, Namespace @namespace,
        Action<IcebergDbContextOptionsBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(@namespace);
        var extension = (builder.Options.FindExtension<IcebergOptionsExtension>() ?? new IcebergOptionsExtension()).WithCatalog(catalog, @namespace);
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(extension);
        configure?.Invoke(new IcebergDbContextOptionsBuilder(builder));
        return builder;
    }

    /// <summary><see cref="UseIceberg(DbContextOptionsBuilder, ICatalog, Namespace, Action{IcebergDbContextOptionsBuilder}?)"/> with a dotted namespace (<c>"sales.eu"</c>).</summary>
    public static DbContextOptionsBuilder UseIceberg(this DbContextOptionsBuilder builder, ICatalog catalog, string @namespace,
        Action<IcebergDbContextOptionsBuilder>? configure = null) =>
        builder.UseIceberg(catalog, Namespace.Of(@namespace.Split('.')), configure);

    public static DbContextOptionsBuilder<TContext> UseIceberg<TContext>(this DbContextOptionsBuilder<TContext> builder, ICatalog catalog, Namespace @namespace,
        Action<IcebergDbContextOptionsBuilder>? configure = null) where TContext : DbContext =>
        (DbContextOptionsBuilder<TContext>)((DbContextOptionsBuilder)builder).UseIceberg(catalog, @namespace, configure);

    public static DbContextOptionsBuilder<TContext> UseIceberg<TContext>(this DbContextOptionsBuilder<TContext> builder, ICatalog catalog, string @namespace,
        Action<IcebergDbContextOptionsBuilder>? configure = null) where TContext : DbContext =>
        (DbContextOptionsBuilder<TContext>)((DbContextOptionsBuilder)builder).UseIceberg(catalog, @namespace, configure);
}
