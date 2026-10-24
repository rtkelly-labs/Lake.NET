using System.Net;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Static role grants enforced on every endpoint: a principal sees only namespaces it holds a grant on, above or below,
/// reads what it may read, writes what it may write, and gets 403 <c>ForbiddenException</c> otherwise.
/// </summary>
public sealed class AuthorizationTests
{
    private static readonly Dictionary<string, string> Settings = new()
    {
        ["Auth:Clients:root"] = "root-secret",
        ["Auth:Clients:analyst"] = "analyst-secret",
        ["Auth:Clients:etl"] = "etl-secret",
        ["Auth:Clients:nobody"] = "nobody-secret",
        ["Auth:SigningKey"] = "an-authorization-test-signing-key-32-bytes",
        ["Authorization:Grants:root:0"] = "admin:*",
        ["Authorization:Grants:analyst:0"] = "read:sales",
        ["Authorization:Grants:etl:0"] = "write:sales.staging",
        ["Authorization:Grants:etl:1"] = "admin:sales.staging.scratch",
    };

    private static readonly TableIdentifier Orders = new(Namespace.Of("sales"), "orders");
    private static readonly TableIdentifier Events = new(Namespace.Of("sales", "staging"), "events");
    private static readonly TableIdentifier People = new(Namespace.Of("hr"), "people");

    private static async Task<ServerHost> StartAsync()
    {
        var host = ServerHost.Start(settings: Settings);
        using var root = await host.ConnectAsync(credential: "root:root-secret");
        foreach (var ns in new[] { Namespace.Of("sales"), Namespace.Of("sales", "staging"), Namespace.Of("hr") }) await root.CreateNamespaceAsync(ns);
        foreach (var id in new[] { Orders, Events, People })
        {
            var t = await root.CreateTableAsync(new CreateTableRequest(id, ScanPlanningTests.Schema, ScanPlanningTests.Spec));
            await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 6))).CommitAsync();
        }
        return host;
    }

    private static async Task AppendAsync(Table t) =>
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(100, 3))).CommitAsync();

    [Fact]
    public async Task Readers_see_and_read_only_their_scope()
    {
        await using var host = await StartAsync();
        using var analyst = await host.ConnectAsync(credential: "analyst:analyst-secret");

        Assert.Equal([Namespace.Of("sales")], await analyst.ListNamespacesAsync());
        Assert.Equal([Namespace.Of("sales", "staging")], await analyst.ListNamespacesAsync(Namespace.Of("sales")));
        Assert.Equal([Orders], await analyst.ListTablesAsync(Namespace.Of("sales")));
        var orders = await analyst.LoadTableAsync(Orders);
        Assert.Equal(6, (await TableScan.ReadAllAsync(orders)).Count);
        Assert.Equal(6, (await ServerSideScan.ReadAsync(analyst, orders)).Count);
        Assert.True(await analyst.TableExistsAsync(Events));

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => analyst.LoadTableAsync(People));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => analyst.ListTablesAsync(Namespace.Of("hr")));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => AppendAsync(orders));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => analyst.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("sales"), "mine"), ScanPlanningTests.Schema)));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => analyst.DropTableAsync(Orders));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => analyst.CreateNamespaceAsync(Namespace.Of("sales", "mine")));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => analyst.DropNamespaceAsync(Namespace.Of("sales", "staging")));
        // Nothing changed.
        using var root = await host.ConnectAsync(credential: "root:root-secret");
        Assert.Single((await root.LoadTableAsync(Orders)).Metadata.Snapshots);
        Assert.Equal(3, (await root.ListNamespacesAsync()).Count + (await root.ListNamespacesAsync(Namespace.Of("sales"))).Count);
    }

    [Fact]
    public async Task Writers_write_inside_their_scope_and_admins_drop()
    {
        await using var host = await StartAsync();
        using var etl = await host.ConnectAsync(credential: "etl:etl-secret");

        // A grant below "sales" makes "sales" visible, but not the tables in it.
        Assert.Equal([Namespace.Of("sales")], await etl.ListNamespacesAsync());
        Assert.Empty(await etl.ListTablesAsync(Namespace.Of("sales")));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => etl.LoadTableAsync(Orders));

        var events = await etl.LoadTableAsync(Events);
        await AppendAsync(events);
        var created = await etl.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("sales", "staging"), "daily"), ScanPlanningTests.Schema));
        await AppendAsync(created);
        await TableMutations.DeleteWhereAsync(events, Expr.Equal("id", 1L), DeleteStrategy.PositionDeletes);

        // Write is not admin: no drops or renames of the source, no namespace changes, except under the admin grant.
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => etl.DropTableAsync(Events));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => etl.RenameTableAsync(Events, new TableIdentifier(Namespace.Of("sales", "staging"), "renamed")));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => etl.CreateNamespaceAsync(Namespace.Of("sales", "staging", "other")));
        await etl.CreateNamespaceAsync(Namespace.Of("sales", "staging", "scratch"));
        var scratch = new TableIdentifier(Namespace.Of("sales", "staging", "scratch"), "tmp");
        await etl.CreateTableAsync(new CreateTableRequest(scratch, ScanPlanningTests.Schema));
        await etl.RenameTableAsync(scratch, new TableIdentifier(Namespace.Of("sales", "staging", "scratch"), "tmp2"));
        await etl.DropTableAsync(new TableIdentifier(Namespace.Of("sales", "staging", "scratch"), "tmp2"));
        await etl.DropNamespaceAsync(Namespace.Of("sales", "staging", "scratch"));

        // Multi-table transactions need write on every table.
        var orders = await (await host.ConnectAsync(credential: "root:root-secret")).LoadTableAsync(Orders);
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => etl.CommitTransactionAsync(
        [
            new TableCommit(Events, [], [new Metadata.MetadataUpdate.SetProperties(new Dictionary<string, string> { ["a"] = "1" })]),
            new TableCommit(Orders, [], [new Metadata.MetadataUpdate.SetProperties(new Dictionary<string, string> { ["a"] = "1" })]),
        ]));
        Assert.False(orders.Metadata.Properties.ContainsKey("a"));
        Assert.Equal(3, (await etl.LoadTableAsync(Events)).Metadata.Snapshots.Count);
    }

    [Fact]
    public async Task Refusals_are_403_in_the_error_model_and_principals_without_grants_see_nothing()
    {
        await using var host = await StartAsync();
        using var http = host.Http();
        using var tokenResp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = "nobody", ["client_secret"] = "nobody-secret",
        }));
        var token = JsonNode.Parse(await tokenResp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
        async Task<(HttpStatusCode, JsonNode?)> Send(HttpMethod method, string path, JsonNode? body = null)
        {
            using var req = new HttpRequestMessage(method, path);
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            if (body is not null) req.Content = ProtocolTests.JsonBody(body);
            using var resp = await http.SendAsync(req);
            var text = await resp.Content.ReadAsStringAsync();
            return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text));
        }

        Assert.Equal(HttpStatusCode.OK, (await Send(HttpMethod.Get, "/v1/config")).Item1);
        var (_, listed) = await Send(HttpMethod.Get, "/v1/namespaces");
        Assert.Empty(listed!["namespaces"]!.AsArray());
        foreach (var (method, path, body) in new (HttpMethod, string, JsonNode?)[]
        {
            (HttpMethod.Get, "/v1/namespaces/sales", null),
            (HttpMethod.Get, "/v1/namespaces/sales/tables", null),
            (HttpMethod.Get, "/v1/namespaces/sales/tables/orders", null),
            (HttpMethod.Post, "/v1/namespaces/sales/tables/orders/plan", new JsonObject()),
            (HttpMethod.Post, "/v1/namespaces/sales/tables/orders", new JsonObject { ["requirements"] = new JsonArray(), ["updates"] = new JsonArray() }),
            (HttpMethod.Delete, "/v1/namespaces/sales/tables/orders", null),
            (HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("x") }),
            (HttpMethod.Get, "/v1/namespaces/sales/views/v", null),
            // A table that does not exist is still refused, not reported missing.
            (HttpMethod.Get, "/v1/namespaces/hr/tables/nothing", null),
        })
        {
            var (status, error) = await Send(method, path, body);
            Assert.Equal(HttpStatusCode.Forbidden, status);
            Assert.Equal("ForbiddenException", error!["error"]!["type"]!.GetValue<string>());
            Assert.Equal(403, error["error"]!["code"]!.GetValue<int>());
            Assert.Contains("nobody", error["error"]!["message"]!.GetValue<string>());
        }
    }

    [Fact]
    public void Grants_are_parsed_strictly()
    {
        var (level, scope) = StaticAuthorizer.Parse("write:a.b");
        Assert.Equal(CatalogPrivilege.Write, level);
        Assert.Equal(["a", "b"], scope!);
        Assert.Null(StaticAuthorizer.Parse("admin:*").Scope);
        foreach (var bad in new[] { "read", "own:a", "read:", "read:a..b", ":a" })
            Assert.Throws<ArgumentException>(() => StaticAuthorizer.Parse(bad));
    }

    [Fact]
    public async Task Grants_to_everyone_and_anonymous_principals()
    {
        var authorizer = new StaticAuthorizer(new RestServerAuthorizationOptions { Grants = { ["*"] = ["read:public"], ["anonymous"] = ["write:public.inbox"] } });
        var anonymous = new ClaimsPrincipal(new ClaimsIdentity());
        var someone = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "someone")], "test"));
        Assert.True(await authorizer.IsAllowedAsync(someone, CatalogPrivilege.Read, new CatalogResource(Namespace.Of("public"), "t")));
        Assert.False(await authorizer.IsAllowedAsync(someone, CatalogPrivilege.Write, new CatalogResource(Namespace.Of("public", "inbox"), "t")));
        Assert.True(await authorizer.IsAllowedAsync(anonymous, CatalogPrivilege.Write, new CatalogResource(Namespace.Of("public", "inbox"), "t")));
        Assert.False(await authorizer.IsAllowedAsync(anonymous, CatalogPrivilege.Read, new CatalogResource(Namespace.Of("private"), "t")));
        // Browse reaches up from a grant to its ancestors, never sideways.
        Assert.True(await authorizer.IsAllowedAsync(someone, CatalogPrivilege.Browse, new CatalogResource(new Namespace([]))));
        Assert.False(await authorizer.IsAllowedAsync(someone, CatalogPrivilege.Browse, new CatalogResource(Namespace.Of("publicity"))));
    }

    /// <summary>An embedding application's own authorizer replaces the static grants.</summary>
    private sealed class ReadOnlyAuthorizer : IAuthorizer
    {
        public ValueTask<bool> IsAllowedAsync(ClaimsPrincipal principal, CatalogPrivilege privilege, CatalogResource resource, CancellationToken ct = default) =>
            ValueTask.FromResult(privilege <= CatalogPrivilege.Read || resource.Namespace.Levels is ["setup", ..]);
    }

    [Fact]
    public async Task A_registered_authorizer_is_used()
    {
        await using var host = ServerHost.Start(services: s => s.AddSingleton<IAuthorizer, ReadOnlyAuthorizer>());
        using var catalog = await host.ConnectAsync();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => catalog.CreateNamespaceAsync(Namespace.Of("db")));
        await catalog.CreateNamespaceAsync(Namespace.Of("setup"));
        await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("setup"), "t"), ScanPlanningTests.Schema));
        Assert.True(await catalog.TableExistsAsync(new TableIdentifier(Namespace.Of("setup"), "t")));
    }
}
