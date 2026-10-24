using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.EntityFrameworkCore.Infrastructure;

/// <summary>
/// The provider's options: the catalog and namespace the context's tables live in, and how queries and writes run.
/// The catalog is read from the options at run time (not baked into the internal service provider), so contexts over
/// different catalogs share one service provider.
/// </summary>
public sealed class IcebergOptionsExtension : IDbContextOptionsExtension
{
    private ExtensionInfo? _info;

    public IcebergOptionsExtension() { }

    private IcebergOptionsExtension(IcebergOptionsExtension copy)
    {
        Catalog = copy.Catalog;
        Namespace = copy.Namespace;
        Strict = copy.Strict;
        DeleteStrategy = copy.DeleteStrategy;
        RequireUnchangedSnapshot = copy.RequireUnchangedSnapshot;
        RequireAtomicSaveChanges = copy.RequireAtomicSaveChanges;
        FormatVersion = copy.FormatVersion;
        TableProperties = copy.TableProperties;
    }

    public ICatalog? Catalog { get; private set; }
    public Namespace? Namespace { get; private set; }

    /// <summary>Throw for a <c>Where</c> that cannot be translated into the scan filter, as EF Core refuses client evaluation.</summary>
    public bool Strict { get; private set; }

    /// <summary>How <c>SaveChanges</c>, <c>ExecuteUpdate</c> and <c>ExecuteDelete</c> remove rows; default follows each table's <c>write.*.mode</c>.</summary>
    public DeleteStrategy DeleteStrategy { get; private set; } = DeleteStrategy.Default;

    /// <summary>
    /// Fail <c>SaveChanges</c> with <see cref="DbUpdateConcurrencyException"/> when a table that has modified or deleted
    /// entities moved past the snapshot the context first read it at, whatever rows changed.
    /// </summary>
    public bool RequireUnchangedSnapshot { get; private set; }

    /// <summary>
    /// Fail a <c>SaveChanges</c> that spans several tables with <see cref="NotSupportedException"/>, before anything is
    /// written, when the catalog cannot commit several tables atomically
    /// (<see cref="ICatalog.SupportsMultiTableTransactions"/>), instead of committing table by table.
    /// </summary>
    public bool RequireAtomicSaveChanges { get; private set; }

    /// <summary>Format version for tables <c>EnsureCreated</c> creates (default: the library default, v2).</summary>
    public int? FormatVersion { get; private set; }

    /// <summary>Properties for tables <c>EnsureCreated</c> creates.</summary>
    public IReadOnlyDictionary<string, string>? TableProperties { get; private set; }

    public IcebergOptionsExtension WithCatalog(ICatalog catalog, Namespace ns) => new(this) { Catalog = catalog, Namespace = ns };
    public IcebergOptionsExtension WithStrict(bool strict) => new(this) { Strict = strict };
    public IcebergOptionsExtension WithDeleteStrategy(DeleteStrategy strategy) => new(this) { DeleteStrategy = strategy };
    public IcebergOptionsExtension WithRequireUnchangedSnapshot(bool require) => new(this) { RequireUnchangedSnapshot = require };
    public IcebergOptionsExtension WithRequireAtomicSaveChanges(bool require) => new(this) { RequireAtomicSaveChanges = require };
    public IcebergOptionsExtension WithFormatVersion(int version) => new(this) { FormatVersion = version };
    public IcebergOptionsExtension WithTableProperties(IReadOnlyDictionary<string, string> properties) => new(this) { TableProperties = properties };

    public DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services) => services.AddEntityFrameworkIceberg();

    public void Validate(IDbContextOptions options)
    {
        if (Catalog is null || Namespace is null) throw new InvalidOperationException("UseIceberg needs a catalog and a namespace");
    }

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        private new IcebergOptionsExtension Extension => (IcebergOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment => $"Iceberg catalog={Extension.Catalog?.Name} namespace={Extension.Namespace} ";

        // Everything is read from the options per context, so one internal service provider serves every catalog.
        public override int GetServiceProviderHashCode() => 0;

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ExtensionInfo;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) =>
            debugInfo["Iceberg:Namespace"] = Extension.Namespace?.ToString() ?? "";
    }
}

/// <summary>Provider-specific options for <see cref="IcebergDbContextOptionsExtensions.UseIceberg(DbContextOptionsBuilder, ICatalog, Namespace, Action{IcebergDbContextOptionsBuilder}?)"/>.</summary>
public sealed class IcebergDbContextOptionsBuilder(DbContextOptionsBuilder builder)
{
    public DbContextOptionsBuilder OptionsBuilder { get; } = builder;

    /// <summary>Throw instead of filtering in memory when a <c>Where</c> does not translate completely into the scan.</summary>
    public IcebergDbContextOptionsBuilder Strict(bool strict = true) => With(e => e.WithStrict(strict));

    /// <summary>The row-removal mechanism for writes (copy-on-write, position deletes / DVs).</summary>
    public IcebergDbContextOptionsBuilder DeleteStrategy(DeleteStrategy strategy) => With(e => e.WithDeleteStrategy(strategy));

    /// <summary>Table-level optimistic concurrency: see <see cref="IcebergOptionsExtension.RequireUnchangedSnapshot"/>.</summary>
    public IcebergDbContextOptionsBuilder RequireUnchangedSnapshot(bool require = true) => With(e => e.WithRequireUnchangedSnapshot(require));

    /// <summary>
    /// Refuse to save several tables non-atomically: see <see cref="IcebergOptionsExtension.RequireAtomicSaveChanges"/>.
    /// Without it, a catalog without multi-table transactions (Glue) commits table by table.
    /// </summary>
    public IcebergDbContextOptionsBuilder RequireAtomicSaveChanges(bool require = true) => With(e => e.WithRequireAtomicSaveChanges(require));

    /// <summary>Format version of tables created by <c>EnsureCreated</c>.</summary>
    public IcebergDbContextOptionsBuilder FormatVersion(int version) => With(e => e.WithFormatVersion(version));

    /// <summary>Table properties for tables created by <c>EnsureCreated</c> (e.g. <c>write.delete.mode</c>).</summary>
    public IcebergDbContextOptionsBuilder TableProperties(IReadOnlyDictionary<string, string> properties) => With(e => e.WithTableProperties(properties));

    private IcebergDbContextOptionsBuilder With(Func<IcebergOptionsExtension, IcebergOptionsExtension> change)
    {
        var current = OptionsBuilder.Options.FindExtension<IcebergOptionsExtension>() ?? new IcebergOptionsExtension();
        ((IDbContextOptionsBuilderInfrastructure)OptionsBuilder).AddOrUpdateExtension(change(current));
        return this;
    }
}
