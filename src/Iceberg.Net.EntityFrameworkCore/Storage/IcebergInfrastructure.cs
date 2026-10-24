using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Iceberg.Net.EntityFrameworkCore.Metadata;
using Iceberg.Net.Linq;

namespace Iceberg.Net.EntityFrameworkCore.Storage;

/// <summary>Iceberg has no client-visible transactions: each <c>SaveChanges</c> commits one snapshot per table.</summary>
internal sealed class IcebergTransactionManager : IDbContextTransactionManager
{
    private static NotSupportedException Unsupported() => new(
        "The Iceberg provider does not support explicit transactions: SaveChanges commits one snapshot per table (see docs/efcore.md)");

    public IDbContextTransaction? CurrentTransaction => null;
    public IDbContextTransaction BeginTransaction() => throw Unsupported();
    public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) => throw Unsupported();
    public void CommitTransaction() => throw Unsupported();
    public Task CommitTransactionAsync(CancellationToken cancellationToken = default) => throw Unsupported();
    public void RollbackTransaction() => throw Unsupported();
    public Task RollbackTransactionAsync(CancellationToken cancellationToken = default) => throw Unsupported();
    public void ResetState() { }
    public Task ResetStateAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}

internal sealed class IcebergLoggingDefinitions : LoggingDefinitions;

internal sealed class IcebergQueryContext(QueryContextDependencies dependencies) : QueryContext(dependencies);

internal sealed class IcebergQueryContextFactory(QueryContextDependencies dependencies) : IQueryContextFactory
{
    public QueryContext Create() => new IcebergQueryContext(dependencies);
}

/// <summary>Scalar CLR types the LINQ layer maps (docs/linq.md); nothing else is a property type.</summary>
internal sealed class IcebergTypeMappingSource(TypeMappingSourceDependencies dependencies) : TypeMappingSource(dependencies)
{
    protected override CoreTypeMapping? FindMapping(in TypeMappingInfo mappingInfo)
    {
        var clr = mappingInfo.ClrType;
        if (clr is null || !ValueConversions.IsScalar(clr) || ValueConversions.DefaultType(clr) is null) return null;
        return new IcebergTypeMapping(clr);
    }
}

internal sealed class IcebergTypeMapping : CoreTypeMapping
{
    public IcebergTypeMapping(Type clrType)
        : base(new CoreTypeMappingParameters(clrType,
            comparer: clrType == typeof(byte[]) ? ValueComparer.CreateDefault(clrType, favorStructuralComparisons: true) : null,
            keyComparer: clrType == typeof(byte[]) ? ValueComparer.CreateDefault(clrType, favorStructuralComparisons: true) : null))
    {
    }

    private IcebergTypeMapping(CoreTypeMappingParameters parameters) : base(parameters) { }

    protected override CoreTypeMapping Clone(CoreTypeMappingParameters parameters) => new IcebergTypeMapping(parameters);

    public override CoreTypeMapping WithComposedConverter(ValueConverter? converter, ValueComparer? comparer = null, ValueComparer? keyComparer = null,
        CoreTypeMapping? elementMapping = null, JsonValueReaderWriter? jsonValueReaderWriter = null) =>
        new IcebergTypeMapping(Parameters.WithComposedConverter(converter, comparer, keyComparer, elementMapping, jsonValueReaderWriter));
}

/// <summary>Iceberg has no identity columns: keys are never store-generated (only <c>Guid</c> keys get client-side values).</summary>
internal sealed class IcebergConventionSetBuilder(ProviderConventionSetBuilderDependencies dependencies) : ProviderConventionSetBuilder(dependencies)
{
    public override ConventionSet CreateConventionSet()
    {
        var set = base.CreateConventionSet();
        set.Replace<ValueGenerationConvention>(new IcebergValueGenerationConvention(Dependencies));
        return set;
    }
}

internal sealed class IcebergValueGenerationConvention(ProviderConventionSetBuilderDependencies dependencies) : ValueGenerationConvention(dependencies)
{
    protected override ValueGenerated? GetValueGenerated(IConventionProperty property) =>
        (Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType) == typeof(Guid) ? base.GetValueGenerated(property) : null;
}

/// <summary>What the provider can map: flat entity types over scalar properties, one table each.</summary>
internal sealed class IcebergModelValidator(ModelValidatorDependencies dependencies) : ModelValidator(dependencies)
{
    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        base.Validate(model, logger);
        var tables = new Dictionary<string, IEntityType>(StringComparer.Ordinal);
        foreach (var et in model.GetEntityTypes())
        {
            var name = et.DisplayName();
            if (et.IsOwned()) throw new InvalidOperationException($"{name}: owned entity types are not supported by the Iceberg provider");
            if (et.BaseType is not null || et.GetDirectlyDerivedTypes().Any())
                throw new InvalidOperationException($"{name}: inheritance is not supported by the Iceberg provider; map each type to its own table");
            if (et.GetNavigations().Any() || et.GetSkipNavigations().Any() || et.GetForeignKeys().Any())
                throw new InvalidOperationException($"{name}: relationships and navigations are not supported by the Iceberg provider (one table per query; join in memory)");
            if (et.GetComplexProperties().Any())
                throw new InvalidOperationException($"{name}: complex types are not supported by the Iceberg provider");
            if (et.GetDeclaredQueryFilters().Count > 0)
                throw new InvalidOperationException($"{name}: global query filters are not supported by the Iceberg provider");
            foreach (var p in et.GetProperties())
            {
                if (p.IsShadowProperty()) throw new InvalidOperationException($"{name}.{p.Name}: shadow properties are not supported by the Iceberg provider");
                if (p.GetValueConverter() is not null) throw new InvalidOperationException($"{name}.{p.Name}: value converters are not supported by the Iceberg provider");
            }
            var mapping = IcebergEntityMapping.For(et);
            if (!tables.TryAdd(mapping.TableName, et))
                throw new InvalidOperationException($"{name} and {tables[mapping.TableName].DisplayName()} both map to Iceberg table '{mapping.TableName}'");
        }
    }
}

/// <summary>
/// EF Core's query pipeline is not used (<see cref="Query.IcebergQueryCompiler"/> replaces it), but its dependency graph
/// needs these factories to exist.
/// </summary>
internal sealed class UnusedQueryPipelineFactory : IQueryableMethodTranslatingExpressionVisitorFactory, IShapedQueryCompilingExpressionVisitorFactory
{
    private static NotSupportedException Unused() => new("The Iceberg provider does not compile queries through EF Core's pipeline");

    QueryableMethodTranslatingExpressionVisitor IQueryableMethodTranslatingExpressionVisitorFactory.Create(QueryCompilationContext queryCompilationContext) => throw Unused();

    ShapedQueryCompilingExpressionVisitor IShapedQueryCompilingExpressionVisitorFactory.Create(QueryCompilationContext queryCompilationContext) => throw Unused();
}
