using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// JWTs from an external OpenID Connect issuer (<c>Auth:Authority</c>), against a stub IdP that serves discovery and
/// JWKS: every malformed or misaddressed token is a 401, and an external subject never takes on the grants of the
/// server's own client of the same name, because principals are keyed by (issuer, subject).
/// </summary>
public sealed class JwtTests
{
    private const string Idp = "https://idp.test/";
    private const string Audience = "iceberg-catalog";

    private static readonly RsaSecurityKey IdpKey = new(RSA.Create(2048)) { KeyId = "idp-1" };
    private static readonly RsaSecurityKey StrangerKey = new(RSA.Create(2048)) { KeyId = "idp-1" };

    private static readonly Dictionary<string, string> Settings = new()
    {
        ["Auth:Authority"] = Idp,
        ["Auth:Audience"] = Audience,
        ["Auth:Clients:root"] = "root-secret",
        ["Auth:Tokens:0"] = "static-secret-token",
        ["Auth:SigningKey"] = "a-jwt-test-signing-key-that-is-32-bytes-or-more",
        ["Authorization:Grants:root:0"] = "admin:*",
        ["Authorization:Grants:static-token:0"] = "read:*",
        ["Authorization:Grants:external/alice:0"] = "admin:alice",
    };

    /// <summary>The IdP: its discovery document and its key set, and nothing else.</summary>
    private sealed class StubIdp : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var p = IdpKey.Rsa.ExportParameters(false);
            var body = request.RequestUri!.AbsolutePath switch
            {
                "/.well-known/openid-configuration" => new JsonObject { ["issuer"] = Idp, ["jwks_uri"] = Idp + "jwks" }.ToJsonString(),
                "/jwks" => new JsonObject
                {
                    ["keys"] = new JsonArray(new JsonObject
                    {
                        ["kty"] = "RSA", ["use"] = "sig", ["alg"] = "RS256", ["kid"] = IdpKey.KeyId,
                        ["n"] = Base64UrlEncoder.Encode(p.Modulus), ["e"] = Base64UrlEncoder.Encode(p.Exponent),
                    }),
                }.ToJsonString(),
                _ => null,
            };
            return Task.FromResult(body is null
                ? new HttpResponseMessage(HttpStatusCode.NotFound)
                : new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }

    private static ServerHost Start() => ServerHost.Start(settings: Settings, services: s =>
        s.Configure<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme, o => o.BackchannelHttpHandler = new StubIdp()));

    private static string Jwt(string subject = "alice", string issuer = Idp, string? audience = Audience, DateTime? expires = null, bool noExpiry = false,
        SecurityKey? key = null, Dictionary<string, object>? claims = null)
    {
        var now = DateTime.UtcNow;
        var all = new Dictionary<string, object>(claims ?? []) { ["sub"] = subject };
        return new JsonWebTokenHandler { SetDefaultTimesOnTokenCreation = false }.CreateToken(new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Audience = audience,
            Claims = all,
            IssuedAt = now.AddMinutes(-10),
            NotBefore = now.AddMinutes(-10),
            Expires = noExpiry ? null : expires ?? now.AddMinutes(10),
            SigningCredentials = new SigningCredentials(key ?? IdpKey, key is SymmetricSecurityKey ? SecurityAlgorithms.HmacSha256 : SecurityAlgorithms.RsaSha256),
        });
    }

    /// <summary>An unsigned token (<c>alg: none</c>) with otherwise valid claims.</summary>
    private static string Unsigned()
    {
        static string Part(JsonObject o) => Base64UrlEncoder.Encode(Encoding.UTF8.GetBytes(o.ToJsonString()));
        var exp = DateTimeOffset.UtcNow.AddMinutes(10).ToUnixTimeSeconds();
        return $"{Part(new JsonObject { ["alg"] = "none", ["typ"] = "JWT" })}.{Part(new JsonObject { ["iss"] = Idp, ["aud"] = Audience, ["sub"] = "alice", ["exp"] = exp })}.";
    }

    private static async Task<HttpStatusCode> SendAsync(HttpClient http, HttpMethod method, string path, string token, JsonObject? body = null)
    {
        using var req = new HttpRequestMessage(method, path);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        if (body is not null) req.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        using var resp = await http.SendAsync(req);
        return resp.StatusCode;
    }

    private static JsonObject NewNamespace(string name) => new() { ["namespace"] = new JsonArray(name), ["properties"] = new JsonObject() };

    public static TheoryData<string, string> Rejected() => new()
    {
        { "expired", Jwt(expires: DateTime.UtcNow.AddMinutes(-5)) },
        { "alg=none", Unsigned() },
        { "wrong issuer", Jwt(issuer: "https://evil.test/") },
        { "wrong audience", Jwt(audience: "another-application") },
        { "no audience", Jwt(audience: null) },
        { "missing exp", Jwt(noExpiry: true) },
        { "signed by a key the IdP does not publish", Jwt(key: StrangerKey) },
        // The server's own issuer name, signed by the IdP: it must not pass for one of the server's tokens.
        { "IdP key, the server's issuer", Jwt(subject: "root", issuer: "iceberg-net-rest") },
        // The server's own key, the IdP's issuer: it must not pass for an external user either.
        { "server key, the IdP's issuer", Jwt(key: new SymmetricSecurityKey(Encoding.UTF8.GetBytes(Settings["Auth:SigningKey"]))) },
    };

    [Theory]
    [MemberData(nameof(Rejected))]
    public async Task A_bad_external_token_is_a_401(string why, string token)
    {
        await using var host = Start();
        using var http = host.Http();
        Assert.Equal(HttpStatusCode.OK, await SendAsync(http, HttpMethod.Get, "/v1/config", Jwt())); // the IdP is reachable
        Assert.True(HttpStatusCode.Unauthorized == await SendAsync(http, HttpMethod.Get, "/v1/config", token), why);
    }

    [Fact]
    public async Task An_external_subject_named_like_a_local_client_does_not_get_its_grants()
    {
        await using var host = Start();
        using var http = host.Http();
        // The IdP's user "root" is external/root, which holds no grant; the server's client "root" is admin of everything.
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(http, HttpMethod.Post, "/v1/namespaces", Jwt(subject: "root"), NewNamespace("taken")));
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(http, HttpMethod.Post, "/v1/namespaces", Jwt(subject: "static-token"), NewNamespace("taken")));
        // A token that names its own principal is not believed.
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(http, HttpMethod.Post, "/v1/namespaces",
            Jwt(subject: "mallory", claims: new() { ["iceberg_principal"] = "root" }), NewNamespace("taken")));

        // Grants for external users name them external/{sub}.
        Assert.Equal(HttpStatusCode.OK, await SendAsync(http, HttpMethod.Post, "/v1/namespaces", Jwt(subject: "alice"), NewNamespace("alice")));
        Assert.Equal(HttpStatusCode.Forbidden, await SendAsync(http, HttpMethod.Post, "/v1/namespaces", Jwt(subject: "alice"), NewNamespace("bob")));

        // The server's own client "root" keeps its grants, with an external issuer and audience configured.
        using var root = await host.ConnectAsync(credential: "root:root-secret");
        await root.CreateNamespaceAsync(Iceberg.Net.Catalog.Namespace.Of("taken"));
        // And so does the static token.
        Assert.Equal(HttpStatusCode.OK, await SendAsync(http, HttpMethod.Get, "/v1/namespaces", "static-secret-token"));
    }

    [Theory]
    [InlineData("static-secret-token", true)]
    [InlineData("static-secret-toke", false)]
    [InlineData("static-secret-token ", false)]
    [InlineData("static-secret-tokenx", false)]
    [InlineData("", false)]
    public void Static_tokens_match_exactly(string candidate, bool accepted)
    {
        var tokens = new TokenService(new RestServerAuthOptions { Tokens = ["another-token", "static-secret-token"] });
        Assert.Equal(accepted, tokens.IsStaticToken(candidate));
    }
}
