using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Iceberg.Net.IO;

namespace Iceberg.Net.RestServer;

/// <summary>
/// Hosting: <see cref="AddIcebergRestCatalog"/> registers the options, store, FileIO, catalog service and auth;
/// <see cref="Endpoints.MapIcebergRestCatalog"/> maps the routes. <see cref="Build"/> does both for a standalone server.
/// An embedding application can register its own <see cref="ICatalogStore"/> or <see cref="IFileIO"/>; the last
/// registration wins.
/// </summary>
public static class RestServer
{
    /// <summary>
    /// Options are bound from the <c>Iceberg</c> configuration section when first resolved (so test hosts and
    /// embedding apps can add configuration up to the last moment), then <paramref name="configure"/> runs.
    /// </summary>
    public static IServiceCollection AddIcebergRestCatalog(this IServiceCollection services, Action<RestServerOptions>? configure = null)
    {
        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IConfiguration>().GetSection(RestServerOptions.Section).Get<RestServerOptions>() ?? new RestServerOptions();
            configure?.Invoke(options);
            return options;
        });
        services.TryAddSingleton<ICatalogStore>(sp =>
        {
            var options = sp.GetRequiredService<RestServerOptions>();
            return options.Store.ToLowerInvariant() switch
            {
                "memory" => new InMemoryCatalogStore(),
                "sqlite" => SqlCatalogStore.Sqlite(options.SqlitePath ?? "catalog.db", options.CatalogName),
                var s => throw new InvalidOperationException($"Unknown Iceberg:Store '{s}' (use sqlite or memory)"),
            };
        });
        services.TryAddSingleton<IFileIO>(sp =>
            new ResolvingFileIO(() => new S3FileIO(S3FileIOOptions.FromProperties(sp.GetRequiredService<RestServerOptions>().FileIO))));
        services.AddSingleton(sp => new CatalogService(sp.GetRequiredService<ICatalogStore>(), sp.GetRequiredService<IFileIO>(), sp.GetRequiredService<RestServerOptions>())
        {
            Logger = sp.GetService<ILoggerFactory>()?.CreateLogger<CatalogService>(),
        });
        services.AddIcebergAuth();
        return services;
    }

    /// <summary>A configured standalone server (call <c>Run</c> or <c>StartAsync</c> on it).</summary>
    public static WebApplication Build(string[] args, Action<RestServerOptions>? configure = null, Action<WebApplicationBuilder>? builder = null)
    {
        var b = WebApplication.CreateBuilder(args);
        builder?.Invoke(b);
        b.Services.AddIcebergRestCatalog(configure);
        var app = b.Build();
        app.UseAuthentication();
        app.UseAuthorization();
        app.MapIcebergRestCatalog();
        return app;
    }
}
