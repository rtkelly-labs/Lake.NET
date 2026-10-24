using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Iceberg.Net.IO;

namespace Iceberg.Net.RestServer;

/// <summary>
/// Hosting: <see cref="AddIcebergRestCatalog"/> registers the options, store, FileIO, catalog service, authentication,
/// the authorizer, the credential vendor, the server state store, the scan planner and the idempotency store;
/// <see cref="Endpoints.MapIcebergRestCatalog"/> maps the routes. <see cref="Build"/> does both for a standalone server.
/// An embedding application can register its own <see cref="ICatalogStore"/>, <see cref="IFileIO"/>,
/// <see cref="IAuthorizer"/> or <see cref="IStorageCredentialVendor"/>; the last registration wins. A registered
/// <see cref="IGcpAccessTokenSource"/> (or <see cref="GcsDownscopedCredentialVendor"/>) turns GCS vending on next to the
/// configured S3 and Azure vendors.
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
        services.TryAddSingleton<IAuthorizer>(sp =>
        {
            var options = sp.GetRequiredService<RestServerOptions>().Authorization;
            return options.Enabled ? new StaticAuthorizer(options) : new AllowAllAuthorizer();
        });
        services.TryAddSingleton<IStorageCredentialVendor>(sp =>
        {
            var options = sp.GetRequiredService<RestServerOptions>();
            var vendors = new List<IStorageCredentialVendor>();
            if (options.CredentialVending.Enabled) vendors.Add(new StsCredentialVendor(options.CredentialVending, options.FileIO));
            if (options.CredentialVending.Adls.Enabled) vendors.Add(new AdlsSasCredentialVendor(options.CredentialVending.Adls));
            var gcpSource = sp.GetService<IGcpAccessTokenSource>();
            if (sp.GetService<GcsDownscopedCredentialVendor>() is { } gcs) vendors.Add(gcs);
            else if (options.CredentialVending.Gcs.Enabled || gcpSource is not null)
                vendors.Add(new GcsDownscopedCredentialVendor(options.CredentialVending.Gcs, gcpSource ?? options.CredentialVending.Gcs.CreateTokenSource()));
            return vendors.Count switch { 0 => NoCredentialVendor.Instance, 1 => vendors[0], _ => new CompositeCredentialVendor(vendors) };
        });
        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<RestServerOptions>();
            return new S3RequestSigner(options.RemoteSigning, options.FileIO);
        });
        // Idempotency keys and plans live with the catalog when its store can keep them, so instances share them.
        services.TryAddSingleton<IServerStateStore>(sp => sp.GetRequiredService<ICatalogStore>() as IServerStateStore ?? new InMemoryServerStateStore());
        services.TryAddSingleton(sp => new ScanPlanner(sp.GetRequiredService<CatalogService>(), sp.GetRequiredService<RestServerOptions>().ScanPlanning,
            sp.GetRequiredService<IServerStateStore>(), sp.GetService<ILoggerFactory>()?.CreateLogger<ScanPlanner>()));
        services.TryAddSingleton(sp => new IdempotencyStore(sp.GetRequiredService<RestServerOptions>().Idempotency, sp.GetRequiredService<IServerStateStore>()));
        services.AddIcebergAuth();
        services.AddIcebergOpenApi();
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
