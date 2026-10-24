using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// The server under <see cref="WebApplicationFactory{TEntryPoint}"/> (in-process, no sockets), with a temporary local
/// warehouse, driven by the library's own <see cref="RestCatalog"/> client.
/// </summary>
public sealed class ServerHost : IAsyncDisposable
{
    public WebApplicationFactory<Program> Factory { get; }
    public string Root { get; }
    public string Warehouse => Path.Combine(Root, "warehouse");
    public string Database => Path.Combine(Root, "catalog.db");

    private readonly bool _ownsRoot;

    private ServerHost(string root, WebApplicationFactory<Program> factory, bool ownsRoot)
    {
        Root = root;
        Factory = factory;
        _ownsRoot = ownsRoot;
    }

    /// <param name="store">"memory" or "sqlite".</param>
    /// <param name="settings">Extra configuration, keys relative to the <c>Iceberg</c> section (e.g. <c>Auth:Tokens:0</c>).</param>
    /// <param name="sharedStore">A store instance to use instead of building one (two servers over one store).</param>
    /// <param name="root">A directory to reuse (two servers over one SQLite file and warehouse).</param>
    /// <param name="services">Extra service registrations (e.g. an <see cref="IAuthorizer"/>), made after the server's own.</param>
    public static ServerHost Start(string store = "memory", IReadOnlyDictionary<string, string>? settings = null, ICatalogStore? sharedStore = null, string? root = null,
        Action<IServiceCollection>? services = null)
    {
        var owns = root is null;
        root ??= Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var config = new Dictionary<string, string?>
        {
            ["Iceberg:Store"] = store,
            ["Iceberg:Warehouse"] = Path.Combine(root, "warehouse"),
            ["Iceberg:SqlitePath"] = Path.Combine(root, "catalog.db"),
        };
        foreach (var (k, v) in settings ?? new Dictionary<string, string>()) config[$"Iceberg:{k}"] = v;
        var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseEnvironment("Development");
            b.ConfigureAppConfiguration((_, c) => c.AddInMemoryCollection(config));
            if (sharedStore is not null) b.ConfigureTestServices(s => s.AddSingleton(sharedStore));
            if (services is not null) b.ConfigureTestServices(services);
        });
        _ = factory.Server; // start now, so a bad configuration fails here
        return new ServerHost(root, factory, owns);
    }

    public HttpClient Http() => Factory.CreateClient();

    public Task<RestCatalog> ConnectAsync(string? token = null, string? credential = null, string name = "dotnet-rest", string? accessDelegation = null,
        Func<IReadOnlyDictionary<string, string>, IFileIO>? io = null, Func<HttpMessageHandler, HttpMessageHandler>? wrap = null) =>
        RestCatalog.ConnectAsync(new RestCatalogOptions
        {
            Uri = new Uri("http://localhost/"),
            Handler = wrap is null ? Factory.Server.CreateHandler() : wrap(Factory.Server.CreateHandler()),
            Token = token,
            Credential = credential,
            AccessDelegation = accessDelegation,
        }, io ?? (_ => new LocalFileIO()), name);

    public T Service<T>() where T : notnull => Factory.Services.GetRequiredService<T>();

    public async ValueTask DisposeAsync()
    {
        await Factory.DisposeAsync();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (_ownsRoot && Directory.Exists(Root)) Directory.Delete(Root, recursive: true);
    }
}
