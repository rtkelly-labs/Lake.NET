using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace Iceberg.Net.RestServer;

/// <summary>
/// Bearer authentication: static tokens, tokens this server issued from <c>/v1/oauth/tokens</c> (HS256 JWTs), and
/// optionally JWTs from an external OpenID Connect issuer. Unauthenticated requests get a 401 in the spec's
/// <c>ErrorModel</c> shape.
/// </summary>
public sealed class TokenService
{
    private readonly RestServerAuthOptions _options;
    private readonly HashSet<string> _static;

    public TokenService(RestServerAuthOptions options)
    {
        _options = options;
        _static = new HashSet<string>(options.Tokens, StringComparer.Ordinal);
        var key = options.SigningKey is { Length: > 0 } k ? Encoding.UTF8.GetBytes(k) : RandomNumberGenerator.GetBytes(32);
        if (key.Length < 32) throw new ArgumentException("Iceberg:Auth:SigningKey must be at least 32 bytes");
        SigningKey = new SymmetricSecurityKey(key);
    }

    public SymmetricSecurityKey SigningKey { get; }

    public bool IsStaticToken(string token) => _static.Contains(token);

    /// <summary>Whether <paramref name="id"/> / <paramref name="secret"/> are a configured client (compared in constant time).</summary>
    public bool ValidClient(string? id, string? secret)
    {
        if (secret is null) return false;
        // A credential without "id:" sends only the secret; match it against any client then.
        foreach (var (clientId, clientSecret) in _options.Clients)
            if ((id is null || id == clientId)
                && CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(clientSecret), Encoding.UTF8.GetBytes(secret)))
                return true;
        return false;
    }

    public string Issue(string subject, string? scope) => new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
    {
        Issuer = _options.Issuer,
        Subject = new ClaimsIdentity([new Claim("sub", subject), new Claim("scope", scope ?? "catalog")]),
        Expires = DateTime.UtcNow.Add(_options.TokenLifetime),
        SigningCredentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256),
    });

    /// <summary>Validate a token this server issued (for the token-exchange refresh flow); returns its subject or null.</summary>
    public async Task<string?> ValidateIssuedAsync(string token)
    {
        if (IsStaticToken(token)) return "static";
        var result = await new JsonWebTokenHandler().ValidateTokenAsync(token, new TokenValidationParameters
        {
            ValidIssuer = _options.Issuer,
            IssuerSigningKey = SigningKey,
            ValidateAudience = false,
        });
        return result.IsValid ? result.ClaimsIdentity.FindFirst("sub")?.Value ?? "unknown" : null;
    }

    public TimeSpan Lifetime => _options.TokenLifetime;
}

public static class AuthSetup
{
    public const string Policy = "iceberg";

    public static void AddIcebergAuth(this IServiceCollection services)
    {
        services.TryAddSingleton(sp => new TokenService(sp.GetRequiredService<RestServerOptions>().Auth));
        services.AddAuthorization(a => a.AddPolicy(Policy, p => p.RequireAuthenticatedUser()));
        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer();
        services.AddOptions<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme).Configure<RestServerOptions, TokenService>((jwt, server, tokens) =>
        {
            var options = server.Auth;
            jwt.MapInboundClaims = false;
            jwt.RequireHttpsMetadata = options.RequireHttpsMetadata;
            if (options.Authority is not null) jwt.Authority = options.Authority;
            if (options.Audience is not null) jwt.Audience = options.Audience;
            // The server's own key and issuer are always accepted; an Authority adds its discovery keys and issuer.
            jwt.TokenValidationParameters = new TokenValidationParameters
            {
                IssuerSigningKeys = [tokens.SigningKey],
                ValidIssuers = [options.Issuer],
                ValidateAudience = options.Audience is not null,
                ValidAudience = options.Audience,
            };
            jwt.Events = new JwtBearerEvents
            {
                OnMessageReceived = ctx =>
                {
                    var header = ctx.Request.Headers.Authorization.ToString();
                    if (header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) && tokens.IsStaticToken(header["Bearer ".Length..].Trim()))
                    {
                        ctx.Principal = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "static-token")], "static"));
                        ctx.Success();
                    }
                    return Task.CompletedTask;
                },
                OnChallenge = async ctx =>
                {
                    ctx.HandleResponse();
                    ctx.Response.Headers.WWWAuthenticate = "Bearer";
                    await ErrorModel.WriteAsync(ctx.HttpContext, 401, "NotAuthorizedException",
                        ctx.AuthenticateFailure is null ? "Not authorized: a bearer token is required" : "Not authorized: the bearer token is invalid or expired");
                },
                OnForbidden = ctx => ErrorModel.WriteAsync(ctx.HttpContext, 403, "ForbiddenException", "Forbidden"),
            };
        });
    }

    /// <summary>
    /// <c>POST /v1/oauth/tokens</c>: the client-credentials grant, plus token exchange of a token this server issued
    /// (how Java clients refresh). Errors use the OAuth2 shape (<c>{"error", "error_description"}</c>), as the spec says.
    /// </summary>
    public static async Task<IResult> TokenAsync(HttpContext http, TokenService tokens)
    {
        if (!http.Request.HasFormContentType) return OAuthError(400, "invalid_request", "Expected application/x-www-form-urlencoded");
        var form = await http.Request.ReadFormAsync();
        string? Field(string k) => form.TryGetValue(k, out var v) && v.Count > 0 ? v[0] : null;
        var (id, secret) = (Field("client_id"), Field("client_secret"));
        // HTTP Basic client authentication is also allowed by RFC 6749.
        var header = http.Request.Headers.Authorization.ToString();
        if (secret is null && header.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                var pair = Encoding.UTF8.GetString(Convert.FromBase64String(header[6..].Trim())).Split(':', 2);
                (id, secret) = (Uri.UnescapeDataString(pair[0]), pair.Length > 1 ? Uri.UnescapeDataString(pair[1]) : null);
            }
            catch (FormatException)
            {
                return OAuthError(400, "invalid_request", "Malformed Basic authorization");
            }
        }

        string subject;
        switch (Field("grant_type"))
        {
            case "client_credentials":
                if (!tokens.ValidClient(id, secret)) return OAuthError(401, "invalid_client", "Unknown client or wrong secret");
                subject = id ?? "client";
                break;
            case "urn:ietf:params:oauth:grant-type:token-exchange":
                var subjectToken = Field("subject_token");
                if (subjectToken is null || await tokens.ValidateIssuedAsync(subjectToken) is not { } sub)
                    return OAuthError(401, "invalid_client", "The subject token is invalid or expired");
                subject = sub;
                break;
            case var other:
                return OAuthError(400, "unsupported_grant_type", $"Unsupported grant_type '{other}'");
        }

        return Results.Json(new JsonObject
        {
            ["access_token"] = tokens.Issue(subject, Field("scope")),
            ["token_type"] = "bearer",
            ["expires_in"] = (int)tokens.Lifetime.TotalSeconds,
            ["issued_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
        });
    }

    private static IResult OAuthError(int status, string error, string description) =>
        Results.Json(new JsonObject { ["error"] = error, ["error_description"] = description }, statusCode: status);
}
