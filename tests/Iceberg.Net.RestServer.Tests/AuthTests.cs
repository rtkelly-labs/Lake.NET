using System.Net;
using System.Net.Http.Headers;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestServer.Tests;

public sealed class AuthTests
{
    private static readonly Dictionary<string, string> Secured = new()
    {
        ["Auth:Tokens:0"] = "static-secret-token",
        ["Auth:Clients:alice"] = "alice-secret",
        ["Auth:SigningKey"] = "a-test-signing-key-that-is-at-least-32-bytes",
        ["Authorization:AllowAll"] = "true", // authentication only: authorization is AuthorizationTests'
    };

    [Fact]
    public async Task Without_auth_configured_the_server_is_open()
    {
        await using var host = ServerHost.Start();
        using var http = host.Http();
        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync("/v1/config")).StatusCode);
    }

    [Fact]
    public async Task Requests_without_a_valid_bearer_token_get_401_in_the_error_model()
    {
        await using var host = ServerHost.Start(settings: Secured);
        using var http = host.Http();
        foreach (var path in new[] { "/v1/config", "/v1/namespaces" })
        {
            using var resp = await http.GetAsync(path);
            Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
            var error = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["error"]!;
            Assert.Equal("NotAuthorizedException", error["type"]!.GetValue<string>());
            Assert.Equal(401, error["code"]!.GetValue<int>());
        }
        using var bad = new HttpRequestMessage(HttpMethod.Get, "/v1/namespaces");
        bad.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "not-a-token");
        Assert.Equal(HttpStatusCode.Unauthorized, (await http.SendAsync(bad)).StatusCode);
    }

    [Fact]
    public async Task Static_tokens_are_accepted()
    {
        await using var host = ServerHost.Start(settings: Secured);
        using var catalog = await host.ConnectAsync(token: "static-secret-token");
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        Assert.True(await catalog.NamespaceExistsAsync(Namespace.Of("db")));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => host.ConnectAsync(token: "wrong"));
    }

    [Fact]
    public async Task Client_credentials_get_a_token_from_the_oauth_endpoint()
    {
        await using var host = ServerHost.Start(settings: Secured);
        using var catalog = await host.ConnectAsync(credential: "alice:alice-secret");
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        Assert.Contains(Namespace.Of("db"), await catalog.ListNamespacesAsync());
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => host.ConnectAsync(credential: "alice:wrong"));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => host.ConnectAsync(credential: "mallory:alice-secret"));
    }

    [Fact]
    public async Task Token_endpoint_speaks_oauth2()
    {
        await using var host = ServerHost.Start(settings: Secured);
        using var http = host.Http();
        async Task<(HttpStatusCode, JsonNode)> Token(Dictionary<string, string> form)
        {
            using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(form));
            return (resp.StatusCode, JsonNode.Parse(await resp.Content.ReadAsStringAsync())!);
        }

        var (status, issued) = await Token(new() { ["grant_type"] = "client_credentials", ["client_id"] = "alice", ["client_secret"] = "alice-secret", ["scope"] = "catalog" });
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("bearer", issued["token_type"]!.GetValue<string>());
        Assert.Equal("urn:ietf:params:oauth:token-type:access_token", issued["issued_token_type"]!.GetValue<string>());
        Assert.True(issued["expires_in"]!.GetValue<int>() > 0);
        var token = issued["access_token"]!.GetValue<string>();

        // Token exchange refreshes a token the server issued (how Java clients refresh before expiry).
        var (refreshStatus, refreshed) = await Token(new()
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:token-exchange",
            ["subject_token"] = token,
            ["subject_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
        });
        Assert.Equal(HttpStatusCode.OK, refreshStatus);
        using var req = new HttpRequestMessage(HttpMethod.Get, "/v1/namespaces");
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", refreshed["access_token"]!.GetValue<string>());
        Assert.Equal(HttpStatusCode.OK, (await http.SendAsync(req)).StatusCode);

        var (wrongStatus, wrong) = await Token(new() { ["grant_type"] = "client_credentials", ["client_id"] = "alice", ["client_secret"] = "nope" });
        Assert.Equal(HttpStatusCode.Unauthorized, wrongStatus);
        Assert.Equal("invalid_client", wrong["error"]!.GetValue<string>());
        var (grantStatus, grant) = await Token(new() { ["grant_type"] = "password", ["username"] = "alice" });
        Assert.Equal(HttpStatusCode.BadRequest, grantStatus);
        Assert.Equal("unsupported_grant_type", grant["error"]!.GetValue<string>());
        var (forgedStatus, _) = await Token(new() { ["grant_type"] = "urn:ietf:params:oauth:grant-type:token-exchange", ["subject_token"] = token + "x" });
        Assert.Equal(HttpStatusCode.Unauthorized, forgedStatus);
    }

    [Fact]
    public async Task Tokens_signed_by_another_server_are_rejected()
    {
        await using var issuer = ServerHost.Start(settings: new Dictionary<string, string>(Secured) { ["Auth:SigningKey"] = "another-signing-key-also-at-least-32-bytes-long" });
        await using var host = ServerHost.Start(settings: Secured);
        using var issuerHttp = issuer.Http();
        using var resp = await issuerHttp.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = "alice", ["client_secret"] = "alice-secret",
        }));
        var foreign = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => host.ConnectAsync(token: foreign));
    }
}
