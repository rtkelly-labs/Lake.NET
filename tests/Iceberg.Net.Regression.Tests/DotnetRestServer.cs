using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.RestServer;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// The "dotnet-rest" catalog kind: this repository's REST catalog server (src/Iceberg.Net.RestServer), started once
/// per test run on a free local port, with a SQLite store and its warehouse in the same S3 as every other kind. The
/// PyIceberg oracle finds it through <c>ICEBERG_DOTNET_REST_URI</c>.
/// </summary>
internal static class DotnetRestServer
{
    private static readonly Lazy<Uri> Server = new(Start);

#pragma warning disable CA2255 // a test assembly registering its hosted catalog kind before any test asks for it
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void Register()
    {
        TestEnvironment.HostedCatalogs["dotnet-rest"] = async () =>
        {
            var env = TestEnvironment.Current;
            return await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = Server.Value, AccessDelegation = null, Properties = env.S3Properties },
                props => new S3FileIO(S3FileIOOptions.FromProperties(props)), "dotnet-rest");
        };
        // The oracle runs before a test's first CatalogAsync call; it needs the address up front.
        if (TestEnvironment.Current.SkipReason is null) Environment.SetEnvironmentVariable("ICEBERG_DOTNET_REST_URI", Server.Value.ToString().TrimEnd('/'));
    }

    private static Uri Start()
    {
        var env = TestEnvironment.Current;
        var db = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-rest-server", $"{env.RunId}.db");
        var app = RestServer.RestServer.Build(["--urls", "http://127.0.0.1:0"], o =>
        {
            o.Store = "sqlite";
            o.SqlitePath = db;
            o.Warehouse = env.Warehouse.TrimEnd('/') + "/dotnet-rest";
            foreach (var (k, v) in env.S3Properties) o.FileIO[k] = v;
        }, b => b.Logging.SetMinimumLevel(LogLevel.Warning));
        app.StartAsync().GetAwaiter().GetResult();
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            app.StopAsync().GetAwaiter().GetResult();
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            File.Delete(db);
        };
        return new Uri(app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First());
    }
}
