using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Request limits, on real Kestrel over a socket (TestServer has no body size limit): a body over 16 MiB is a 413
/// before anything buffers or hashes it, with or without an <c>Idempotency-Key</c>; JSON nested deeper than the limit
/// is a 400; and the token endpoint answers a burst from one address with 429.
/// </summary>
public sealed class LimitsTests : IAsyncLifetime
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "iceberg-restserver-limits", Guid.NewGuid().ToString("N"));
    private WebApplication _app = null!;
    private HttpClient _http = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_root);
        _app = RestServer.Build([], builder: b =>
        {
            b.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Iceberg:Store"] = "memory",
                ["Iceberg:Warehouse"] = Path.Combine(_root, "warehouse"),
            });
            b.WebHost.UseKestrel().UseUrls("http://127.0.0.1:0");
            b.Logging.ClearProviders();
        });
        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        // The client waits for the server's answer to Expect: 100-continue before sending a body. Its default wait is one
        // second, after which it sends anyway; on a loaded machine the 413 then races the upload and the client can see a
        // reset connection instead of the answer.
        _http = new HttpClient(new SocketsHttpHandler { Expect100ContinueTimeout = TimeSpan.FromSeconds(30) }) { BaseAddress = new Uri(address) };
    }

    public async Task DisposeAsync()
    {
        _http.Dispose();
        await _app.DisposeAsync();
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    /// <summary>A valid create-namespace request padded to <paramref name="bytes"/> with a property value.</summary>
    private static string CreateNamespace(string name, int bytes)
    {
        var head = $"{{\"namespace\":[\"{name}\"],\"properties\":{{\"pad\":\"";
        const string tail = "\"}}";
        return head + new string('a', Math.Max(0, bytes - head.Length - tail.Length)) + tail;
    }

    private async Task<(HttpStatusCode Status, JsonNode? Error)> PostAsync(string body, string? idempotencyKey)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, "/v1/namespaces") { Content = new StringContent(body, Encoding.UTF8, "application/json") };
        // The server may answer before the body is sent; Expect: 100-continue lets the client see that answer.
        req.Headers.ExpectContinue = true;
        if (idempotencyKey is not null) req.Headers.Add("Idempotency-Key", idempotencyKey);
        using var resp = await _http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text)?["error"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_body_over_the_limit_is_a_413_with_or_without_an_idempotency_key(bool withKey)
    {
        var name = withKey ? "keyed" : "plain";
        var (status, error) = await PostAsync(CreateNamespace(name, 17 * 1024 * 1024), withKey ? Guid.NewGuid().ToString() : null);
        Assert.Equal(HttpStatusCode.RequestEntityTooLarge, status);
        Assert.Equal(413, error?["code"]?.GetValue<int>());
        // Nothing was created.
        Assert.Equal(HttpStatusCode.NotFound, (await _http.GetAsync($"/v1/namespaces/{name}")).StatusCode);
        // A body under the limit is fine either way.
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(CreateNamespace(name, 1024 * 1024), withKey ? Guid.NewGuid().ToString() : null)).Status);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Json_nested_deeper_than_the_limit_is_a_400(bool withKey)
    {
        var deep = $"{{\"namespace\":[\"deep\"],\"properties\":{{}},\"x\":{new string('[', 10_000)}{new string(']', 10_000)}}}";
        var (status, error) = await PostAsync(deep, withKey ? Guid.NewGuid().ToString() : null);
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Equal("BadRequestException", error?["type"]?.GetValue<string>());
        var justOver = $"{{\"namespace\":[\"deep\"],\"properties\":{{}},\"x\":{new string('[', 64)}{new string(']', 64)}}}";
        Assert.Equal(HttpStatusCode.BadRequest, (await PostAsync(justOver, null)).Status);
    }

    [Fact]
    public async Task The_token_endpoint_limits_requests_per_address()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string>
        {
            ["Auth:Clients:alice"] = "alice-secret",
            ["Auth:SigningKey"] = "a-limits-test-signing-key-that-is-32-bytes",
            ["Auth:TokenRequestsPerMinute"] = "5",
            ["Authorization:AllowAll"] = "true",
        });
        using var http = host.Http();
        async Task<HttpResponseMessage> Guess(string secret) => await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = "alice", ["client_secret"] = secret,
        }));

        for (var i = 0; i < 5; i++) Assert.Equal(HttpStatusCode.Unauthorized, (await Guess($"guess-{i}")).StatusCode);
        using var limited = await Guess("alice-secret");
        Assert.Equal(HttpStatusCode.TooManyRequests, limited.StatusCode);
        Assert.Equal("temporarily_unavailable", JsonNode.Parse(await limited.Content.ReadAsStringAsync())!["error"]!.GetValue<string>());
        Assert.NotNull(limited.Headers.RetryAfter);
    }
}
