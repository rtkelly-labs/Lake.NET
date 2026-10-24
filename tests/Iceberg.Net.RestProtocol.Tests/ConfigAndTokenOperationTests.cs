using System.Net;
using System.Text.Json.Nodes;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>getConfig and getToken. The token endpoint answers in OAuth2's own error form, not the catalog error model.</summary>
public sealed class ConfigAndTokenOperationTests : ServerTest
{
    protected override IReadOnlyDictionary<string, string> Settings => Rest.AuthSettings;
    protected override string ClientCredential => "root:root-secret";

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        Api = new Rest(Host.Http(), await Rest.TokenAsync(Host, "root", "root-secret"));
    }

    private new Rest Api { get; set; } = null!;

    private async Task<(HttpStatusCode Status, JsonNode? Body)> TokenRequestAsync(Dictionary<string, string> form)
    {
        var resp = await Host.Http().PostAsync("v1/oauth/tokens", new FormUrlEncodedContent(form));
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text));
    }

    [Spec("rest: getConfig")]
    [Fact]
    public async Task Config_returns_defaults_overrides_and_the_endpoints_served()
    {
        var r = await Api.GetAsync("v1/config");
        Assert.Equal(200, r.Code);
        Assert.NotNull(r.Body!["defaults"]?.AsObject());
        Assert.NotNull(r.Body["overrides"]?.AsObject());
        var endpoints = r.Body["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()).ToHashSet();
        foreach (var e in new[]
                 {
                     "GET /v1/{prefix}/namespaces", "POST /v1/{prefix}/namespaces", "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                     "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}", "POST /v1/{prefix}/transactions/commit",
                     "POST /v1/{prefix}/namespaces/{namespace}/properties", "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister",
                 })
            Assert.Contains(e, endpoints);
    }

    [Spec("rest: getConfig")]
    [Fact]
    public async Task Config_needs_authentication_when_auth_is_on() =>
        (await new Rest(Host.Http()).GetAsync("v1/config")).AssertError(401);

    [Spec("rest: getToken")]
    [Fact]
    public async Task Client_credentials_grant_issues_a_bearer_token()
    {
        var (status, body) = await TokenRequestAsync(new() { ["grant_type"] = "client_credentials", ["client_id"] = "reader", ["client_secret"] = "reader-secret" });
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.False(string.IsNullOrEmpty(body!["access_token"]!.GetValue<string>()));
        Assert.Equal("bearer", body["token_type"]!.GetValue<string>(), ignoreCase: true);
        Assert.True(body["expires_in"]!.GetValue<int>() > 0);
        Assert.Equal(200, (await new Rest(Host.Http(), body["access_token"]!.GetValue<string>()).GetAsync("v1/config")).Code);
    }

    [Spec("rest: getToken")]
    [Fact]
    public async Task A_wrong_secret_is_401_invalid_client()
    {
        var (status, body) = await TokenRequestAsync(new() { ["grant_type"] = "client_credentials", ["client_id"] = "reader", ["client_secret"] = "wrong" });
        Assert.Equal(HttpStatusCode.Unauthorized, status);
        Assert.Equal("invalid_client", body!["error"]!.GetValue<string>());
    }

    [Spec("rest: getToken")]
    [Fact]
    public async Task An_unknown_grant_type_is_400_unsupported_grant_type()
    {
        var (status, body) = await TokenRequestAsync(new() { ["grant_type"] = "password", ["username"] = "u", ["password"] = "p" });
        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Equal("unsupported_grant_type", body!["error"]!.GetValue<string>());
    }

    [Spec("rest: getToken")]
    [Fact]
    public async Task A_token_can_be_exchanged_for_a_fresh_one()
    {
        var token = await Rest.TokenAsync(Host, "reader", "reader-secret");
        var (status, body) = await TokenRequestAsync(new()
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:token-exchange",
            ["subject_token"] = token,
            ["subject_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
        });
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.False(string.IsNullOrEmpty(body!["access_token"]!.GetValue<string>()));
        var (bad, error) = await TokenRequestAsync(new()
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:token-exchange", ["subject_token"] = "not-a-token",
            ["subject_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
        });
        Assert.Equal(HttpStatusCode.Unauthorized, bad);
        Assert.Equal("invalid_client", error!["error"]!.GetValue<string>());
    }
}
