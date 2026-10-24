using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Iceberg.Net.EntityFrameworkCore.Infrastructure;
using Iceberg.Net.EntityFrameworkCore.Query;
using Iceberg.Net.EntityFrameworkCore.Storage;

namespace Microsoft.Extensions.DependencyInjection;

public static class IcebergServiceCollectionExtensions
{
    /// <summary>
    /// The provider's services. Called by <c>UseIceberg</c>; call it yourself only when building EF Core's internal
    /// service provider by hand.
    /// </summary>
    public static IServiceCollection AddEntityFrameworkIceberg(this IServiceCollection services)
    {
        var builder = new EntityFrameworkServicesBuilder(services)
            .TryAdd<LoggingDefinitions, IcebergLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<IcebergOptionsExtension>>()
            .TryAdd<IDatabase, IcebergDatabase>()
            .TryAdd<IDbContextTransactionManager, IcebergTransactionManager>()
            .TryAdd<IDatabaseCreator, IcebergDatabaseCreator>()
            .TryAdd<IQueryContextFactory, IcebergQueryContextFactory>()
            .TryAdd<ITypeMappingSource, IcebergTypeMappingSource>()
            .TryAdd<IProviderConventionSetBuilder, IcebergConventionSetBuilder>()
            .TryAdd<IModelValidator, IcebergModelValidator>()
            // Queries are not compiled by EF Core's pipeline: they are handed, as LINQ, to Iceberg.Net.Linq.
            .TryAdd<IQueryCompiler, IcebergQueryCompiler>()
            .TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory, UnusedQueryPipelineFactory>()
            .TryAdd<IShapedQueryCompilingExpressionVisitorFactory, UnusedQueryPipelineFactory>()
            .TryAddProviderSpecificServices(b => b.TryAddScoped<IcebergSession, IcebergSession>());
        builder.TryAddCoreServices();
        return services;
    }
}
