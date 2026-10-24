using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.RestServer.Tests;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>Raw Iceberg REST requests against a server, for what is only observable at the HTTP level.</summary>
internal sealed class Rest(HttpClient http, string? token = null)
{
    public HttpClient Http { get; } = http;

    public sealed record Response(HttpStatusCode Status, JsonNode? Body, HttpResponseMessage Message)
    {
        public int Code => (int)Status;

        /// <summary>The spec's error model: <c>{"error": {"message", "type", "code"}}</c>, with the code matching the status.</summary>
        public void AssertError(int status, string? type = null)
        {
            Assert.True(Code == status, $"expected {status}, got {Code}: {Body?.ToJsonString()}");
            var error = Body?["error"] ?? throw new Xunit.Sdk.XunitException($"no error model in the {status} response");
            Assert.Equal(status, error["code"]!.GetValue<int>());
            Assert.False(string.IsNullOrEmpty(error["message"]?.GetValue<string>()), "error.message is empty");
            if (type is not null) Assert.Equal(type, error["type"]!.GetValue<string>());
        }
    }

    public async Task<Response> SendAsync(HttpMethod method, string path, JsonNode? body = null, Action<HttpRequestMessage>? configure = null)
    {
        using var req = new HttpRequestMessage(method, path);
        if (token is not null) req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        if (body is not null) req.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        configure?.Invoke(req);
        var resp = await Http.SendAsync(req);
        var text = resp.Content is null ? "" : await resp.Content.ReadAsStringAsync();
        return new Response(resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text), resp);
    }

    public Task<Response> GetAsync(string path) => SendAsync(HttpMethod.Get, path);
    public Task<Response> HeadAsync(string path) => SendAsync(HttpMethod.Head, path);
    public Task<Response> DeleteAsync(string path) => SendAsync(HttpMethod.Delete, path);
    public Task<Response> PostAsync(string path, JsonNode? body) => SendAsync(HttpMethod.Post, path, body);

    /// <summary>A malformed JSON body (not parseable), for the 400 responses.</summary>
    public async Task<Response> PostRawAsync(string path, string body)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, path) { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        if (token is not null) req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var resp = await Http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return new Response(resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text), resp);
    }

    // ---------- request bodies ----------

    public static JsonObject Schema(int id = 0, params (int Id, string Name, string Type)[] extra)
    {
        var fields = new JsonArray(
            new JsonObject { ["id"] = 1, ["name"] = "id", ["required"] = true, ["type"] = "long" },
            new JsonObject { ["id"] = 2, ["name"] = "name", ["required"] = false, ["type"] = "string" });
        foreach (var (fid, name, type) in extra) fields.Add(new JsonObject { ["id"] = fid, ["name"] = name, ["required"] = false, ["type"] = type });
        return new JsonObject { ["type"] = "struct", ["schema-id"] = id, ["fields"] = fields };
    }

    public static JsonObject CreateTable(string name) => new() { ["name"] = name, ["schema"] = Schema() };

    public static JsonObject CreateNamespace(params string[] levels) => new()
    {
        ["namespace"] = new JsonArray(levels.Select(l => (JsonNode)l).ToArray()),
        ["properties"] = new JsonObject { ["owner"] = "tests" },
    };

    public static JsonObject CreateView(string name) => new()
    {
        ["name"] = name,
        ["schema"] = Schema(),
        ["view-version"] = new JsonObject
        {
            ["version-id"] = 1, ["timestamp-ms"] = 1_700_000_000_000, ["schema-id"] = 0,
            ["summary"] = new JsonObject { ["engine-name"] = "tests" },
            ["representations"] = new JsonArray(new JsonObject { ["type"] = "sql", ["sql"] = "SELECT 1 AS id, 'a' AS name", ["dialect"] = "spark" }),
            ["default-namespace"] = new JsonArray(),
        },
        ["properties"] = new JsonObject(),
    };

    // ---------- hosts ----------

    /// <summary>The settings of a server with OAuth2 clients and grants (see docs/rest-server.md, Authorization).</summary>
    public static readonly IReadOnlyDictionary<string, string> AuthSettings = new Dictionary<string, string>
    {
        ["Auth:Clients:root"] = "root-secret",
        ["Auth:Clients:writer"] = "writer-secret",
        ["Auth:Clients:reader"] = "reader-secret",
        ["Auth:Clients:nobody"] = "nobody-secret",
        ["Auth:SigningKey"] = "a-rest-integration-test-signing-key-of-32-bytes",
        ["Authorization:Grants:root:0"] = "admin:*",
        ["Authorization:Grants:writer:0"] = "write:sales",
        ["Authorization:Grants:reader:0"] = "read:sales",
    };

    public static async Task<string> TokenAsync(ServerHost host, string client, string secret)
    {
        var resp = await host.Http().PostAsync("v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = client, ["client_secret"] = secret, ["scope"] = "catalog",
        }));
        resp.EnsureSuccessStatusCode();
        return JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
    }
}
