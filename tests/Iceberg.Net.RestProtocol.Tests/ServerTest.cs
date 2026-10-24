using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.RestServer.Tests;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>A fresh in-process server per test (in-memory store, local warehouse), reached raw and through the client.</summary>
public abstract class ServerTest : IAsyncLifetime
{
    protected ServerHost Host { get; private set; } = null!;
    private protected Rest Api { get; private set; } = null!;
    protected RestCatalog Client { get; private set; } = null!;

    /// <summary>Server settings, keys relative to the <c>Iceberg</c> section.</summary>
    protected virtual IReadOnlyDictionary<string, string>? Settings => null;

    /// <summary>The client's OAuth2 credential (<c>id:secret</c>), for servers with authentication on.</summary>
    protected virtual string? ClientCredential => null;

    public virtual async Task InitializeAsync()
    {
        Host = ServerHost.Start(settings: Settings);
        Api = new Rest(Host.Http());
        Client = await Host.ConnectAsync(credential: ClientCredential);
    }

    public async Task DisposeAsync()
    {
        Client.Dispose();
        await Host.DisposeAsync();
    }

    /// <summary>A namespace path segment: levels joined with the unit separator (0x1F), URL-encoded.</summary>
    protected static string Ns(params string[] levels) => Uri.EscapeDataString(string.Join('\u001f', levels));

    protected async Task CreateNamespaceAsync(params string[] levels) =>
        Assert.Equal(200, (await Api.PostAsync("v1/namespaces", Rest.CreateNamespace(levels))).Code);

    protected async Task<JsonNode> CreateTableAsync(string ns, string name)
    {
        var r = await Api.PostAsync($"v1/namespaces/{Ns(ns)}/tables", Rest.CreateTable(name));
        Assert.Equal(200, r.Code);
        return r.Body!;
    }

    protected static JsonObject Identifier(string ns, string name) => new() { ["namespace"] = new JsonArray(ns), ["name"] = name };
}
