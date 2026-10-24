using System.Net;
using System.Security.Claims;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Microsoft.Extensions.DependencyInjection;
using static Iceberg.Net.RestServer.Tests.ProtocolTests;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Defaults that would otherwise widen access quietly: configurations refused at start-up, the token endpoint, what the
/// idempotency store keeps, what a 500 says, and how much one page holds.
/// </summary>
public sealed class ServerDefaultsTests
{
    private const string SigningKey = "a-server-defaults-test-signing-key-32b";

    [Fact]
    public async Task Authentication_without_grants_refuses_to_start_unless_allow_all_is_explicit()
    {
        var settings = new Dictionary<string, string> { ["Auth:Clients:alice"] = "alice-secret", ["Auth:SigningKey"] = SigningKey };
        var e = Assert.ThrowsAny<Exception>(() => ServerHost.Start(settings: settings));
        Assert.Contains("Authorization", e.ToString());
        await using var host = ServerHost.Start(settings: new Dictionary<string, string>(settings) { ["Authorization:AllowAll"] = "true" });
    }

    [Fact]
    public async Task An_external_authority_without_an_audience_refuses_to_start_unless_any_audience_is_explicit()
    {
        var settings = new Dictionary<string, string> { ["Auth:Authority"] = "https://idp.example", ["Authorization:AllowAll"] = "true" };
        var e = Assert.ThrowsAny<Exception>(() => ServerHost.Start(settings: settings));
        Assert.Contains("Audience", e.ToString());
        await using var host = ServerHost.Start(settings: new Dictionary<string, string>(settings) { ["Auth:AllowAnyAudience"] = "true" });
    }

    private static async Task<(HttpStatusCode Status, JsonNode? Body)> TokenAsync(HttpClient http, Dictionary<string, string> form)
    {
        using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(form));
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text));
    }

    private static Dictionary<string, string> AuthSettings(params (string Id, string Secret)[] clients)
    {
        var s = new Dictionary<string, string> { ["Auth:SigningKey"] = SigningKey, ["Authorization:AllowAll"] = "true" };
        foreach (var (id, secret) in clients) s[$"Auth:Clients:{id}"] = secret;
        return s;
    }

    [Fact]
    public async Task A_client_secret_alone_does_not_get_a_token()
    {
        await using var host = ServerHost.Start(settings: AuthSettings(("alice", "alice-secret"), ("bob", "bob-secret")));
        using var http = host.Http();
        var (status, body) = await TokenAsync(http, new() { ["grant_type"] = "client_credentials", ["client_secret"] = "bob-secret" });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Equal("invalid_request", body!["error"]!.GetValue<string>());
    }

    [Fact]
    public async Task A_removed_clients_tokens_are_not_refreshed()
    {
        await using var before = ServerHost.Start(settings: AuthSettings(("alice", "alice-secret"), ("bob", "bob-secret")));
        await using var after = ServerHost.Start(settings: AuthSettings(("alice", "alice-secret"))); // bob removed, same key
        using var http = before.Http();
        var token = (await TokenAsync(http, new() { ["grant_type"] = "client_credentials", ["client_id"] = "bob", ["client_secret"] = "bob-secret" }))
            .Body!["access_token"]!.GetValue<string>();
        using var afterHttp = after.Http();
        var exchange = new Dictionary<string, string>
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:token-exchange", ["subject_token"] = token,
            ["subject_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
        };
        Assert.Equal(HttpStatusCode.OK, (await TokenAsync(http, exchange)).Status);
        Assert.Equal(HttpStatusCode.Unauthorized, (await TokenAsync(afterHttp, exchange)).Status);
    }

    [Fact]
    public async Task Token_exchange_never_extends_a_session_past_its_lifetime()
    {
        var settings = AuthSettings(("alice", "alice-secret"));
        settings["Auth:MaxSessionLifetime"] = "00:00:30";
        await using var host = ServerHost.Start(settings: settings);
        using var http = host.Http();
        var first = (await TokenAsync(http, new() { ["grant_type"] = "client_credentials", ["client_id"] = "alice", ["client_secret"] = "alice-secret" })).Body!;
        Assert.True(first["expires_in"]!.GetValue<int>() <= 30);
        var refreshed = (await TokenAsync(http, new()
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:token-exchange", ["subject_token"] = first["access_token"]!.GetValue<string>(),
            ["subject_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
        })).Body!;
        Assert.True(refreshed["expires_in"]!.GetValue<int>() <= 30, "a refreshed token outlives the session");
    }

    private const string Account = "devstoreaccount1";
    private const string AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    [Fact]
    public async Task A_replayed_create_does_not_return_stored_credentials()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string>
        {
            ["Warehouse"] = $"abfss://lake@{Account}.dfs.core.windows.net/wh",
            ["CredentialVending:Adls:Accounts:" + Account] = AccountKey,
        }, services: s => s.AddSingleton<IFileIO>(new MemoryFileIO()));
        using var http = host.Http();
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") });
        async Task<(HttpStatusCode Status, JsonNode? Body, bool Replayed)> CreateAsync()
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, "/v1/namespaces/db/tables")
            {
                Content = JsonBody(new JsonObject { ["name"] = "t", ["schema"] = Json.TypeJson.SchemaToJson(ScanPlanningTests.Schema) }),
            };
            req.Headers.Add("X-Iceberg-Access-Delegation", "vended-credentials");
            req.Headers.Add("Idempotency-Key", "0192f6a0-0000-7000-8000-000000000001");
            using var resp = await http.SendAsync(req);
            return (resp.StatusCode, JsonNode.Parse(await resp.Content.ReadAsStringAsync()), resp.Headers.Contains("Idempotent-Replayed"));
        }
        var first = await CreateAsync();
        Assert.Equal(HttpStatusCode.OK, first.Status);
        Assert.NotNull(first.Body!["storage-credentials"]);
        var replay = await CreateAsync();
        Assert.True(replay.Replayed);
        Assert.Equal(HttpStatusCode.OK, replay.Status);
        Assert.Null(replay.Body!["storage-credentials"]);
        Assert.DoesNotContain(replay.Body["config"]!.AsObject(), kv => kv.Key.StartsWith("adls.sas-token", StringComparison.Ordinal));
        Assert.Equal(first.Body["metadata-location"]!.GetValue<string>(), replay.Body["metadata-location"]!.GetValue<string>());
    }

    private sealed class FailingAuthorizer : IAuthorizer
    {
        public ValueTask<bool> IsAllowedAsync(ClaimsPrincipal principal, CatalogPrivilege privilege, CatalogResource resource, CancellationToken ct = default) =>
            throw new Exception("connection string Server=db.internal;Password=hunter2");
    }

    [Fact]
    public async Task A_server_error_names_a_trace_id_not_its_cause()
    {
        await using var host = ServerHost.Start(services: s => s.AddSingleton<IAuthorizer>(new FailingAuthorizer()));
        using var http = host.Http();
        var (status, body) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db");
        Assert.Equal(HttpStatusCode.InternalServerError, status);
        var message = body!["error"]!["message"]!.GetValue<string>();
        Assert.DoesNotContain("hunter2", message);
        Assert.Contains("trace", message);
    }

    [Fact]
    public async Task A_page_holds_at_most_a_thousand_entries()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        var store = host.Service<ICatalogStore>();
        for (var i = 0; i < Endpoints.MaxPageSize + 5; i++) await store.CreateNamespaceAsync(Namespace.Of($"ns{i:D4}"), new Dictionary<string, string>());
        using var http = host.Http();
        var (_, unpaged) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces");
        Assert.Equal(Endpoints.MaxPageSize, unpaged!["namespaces"]!.AsArray().Count);
        Assert.NotNull(unpaged["next-page-token"]);
        var (_, big) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces?pageSize=100000");
        Assert.Equal(Endpoints.MaxPageSize, big!["namespaces"]!.AsArray().Count);
        Assert.Equal(Endpoints.MaxPageSize + 5, (await catalog.ListNamespacesAsync()).Count); // the client follows the token
    }
}
