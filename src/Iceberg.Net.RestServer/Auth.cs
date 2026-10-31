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
public sealed class TokenService : IDisposable
{
    private readonly RestServerAuthOptions _options;
    private readonly byte[][] _static;
    private readonly System.Threading.RateLimiting.PartitionedRateLimiter<string>? _tokenRequests;

    public TokenService(RestServerAuthOptions options)
    {
        _options = options;
        // Hashed, so every comparison is of equal-length values and takes the same time whatever the token.
        _static = options.Tokens.Select(t => SHA256.HashData(Encoding.UTF8.GetBytes(t))).ToArray();
        var key = options.SigningKey is { Length: > 0 } k ? Encoding.UTF8.GetBytes(k) : RandomNumberGenerator.GetBytes(32);
        if (key.Length < 32) throw new ArgumentException("Iceberg:Auth:SigningKey must be at least 32 bytes");
        SigningKey = new SymmetricSecurityKey(key);
        if (options.TokenRequestsPerMinute > 0)
            _tokenRequests = System.Threading.RateLimiting.PartitionedRateLimiter.Create<string, string>(address =>
                System.Threading.RateLimiting.RateLimitPartition.GetFixedWindowLimiter(address, _ => new System.Threading.RateLimiting.FixedWindowRateLimiterOptions
                {
                    PermitLimit = options.TokenRequestsPerMinute,
                    Window = TimeSpan.FromMinutes(1),
                    QueueLimit = 0,
                }));
    }

    /// <summary>
    /// Whether a token request from <paramref name="address"/> is within <see cref="RestServerAuthOptions.TokenRequestsPerMinute"/>:
    /// the endpoint checks client secrets, so unlimited requests would let a caller guess them as fast as it can send.
    /// </summary>
    public bool TryAcquireTokenRequest(string address)
    {
        if (_tokenRequests is null) return true;
        using var lease = _tokenRequests.AttemptAcquire(address);
        return lease.IsAcquired;
    }

    public void Dispose() => _tokenRequests?.Dispose();

    public SymmetricSecurityKey SigningKey { get; }

    /// <summary>Whether <paramref name="token"/> is a configured static token, compared in constant time against every one.</summary>
    public bool IsStaticToken(string token)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(token));
        var found = false;
        foreach (var candidate in _static) found |= CryptographicOperations.FixedTimeEquals(candidate, hash);
        return found;
    }

    /// <summary>The issuer of the tokens this server signs (<see cref="RestServerAuthOptions.Issuer"/>).</summary>
    public string Issuer => _options.Issuer;

    /// <summary>Whether <paramref name="key"/> is this server's own signing key.</summary>
    public bool IsOwnKey(SecurityKey? key) =>
        key is SymmetricSecurityKey symmetric && CryptographicOperations.FixedTimeEquals(symmetric.Key, SigningKey.Key);

    /// <summary>Whether <paramref name="id"/> / <paramref name="secret"/> are a configured client (compared in constant time).</summary>
    public bool ValidClient(string? id, string? secret)
    {
        // The client ID names whose secret to compare: a secret alone would be tried against every client.
        if (secret is null || id is null || !_options.Clients.TryGetValue(id, out var clientSecret)) return false;
        return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(clientSecret), Encoding.UTF8.GetBytes(secret));
    }

    private const string SessionStartClaim = "iceberg_session_start";

    public bool IsClient(string subject) => _options.Clients.ContainsKey(subject);

    /// <summary>
    /// Issue a token. <paramref name="sessionStart"/> is when the client-credentials grant that began this session
    /// happened; token exchange carries it forward, and no token outlives <see cref="RestServerAuthOptions.MaxSessionLifetime"/> from it.
    /// </summary>
    public (string Token, TimeSpan ExpiresIn) Issue(string subject, string? scope, DateTimeOffset? sessionStart = null)
    {
        var start = sessionStart ?? DateTimeOffset.UtcNow;
        var expires = DateTimeOffset.UtcNow.Add(_options.TokenLifetime);
        var sessionEnd = start.Add(_options.MaxSessionLifetime);
        if (expires > sessionEnd) expires = sessionEnd;
        var token = new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = _options.Issuer,
            Subject = new ClaimsIdentity([
                new Claim("sub", subject), new Claim("scope", scope ?? "catalog"),
                new Claim(SessionStartClaim, start.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture), ClaimValueTypes.Integer64)]),
            Expires = expires.UtcDateTime,
            SigningCredentials = new SigningCredentials(SigningKey, SecurityAlgorithms.HmacSha256),
        });
        return (token, expires - DateTimeOffset.UtcNow);
    }

    /// <summary>When the session a token belongs to started, or null if the session has run its course.</summary>
    public DateTimeOffset? LiveSessionStart(System.Security.Claims.ClaimsIdentity identity)
    {
        var start = identity.FindFirst(SessionStartClaim)?.Value is { } s && long.TryParse(s, out var seconds)
            ? DateTimeOffset.FromUnixTimeSeconds(seconds) : DateTimeOffset.UtcNow;
        return DateTimeOffset.UtcNow < start.Add(_options.MaxSessionLifetime) ? start : null;
    }

    /// <summary>Validate a token this server issued (for the token-exchange refresh flow); returns its subject or null.</summary>
    public async Task<(string Subject, DateTimeOffset? SessionStart)?> ValidateIssuedAsync(string token)
    {
        // Same principal name as a request with the static token, so grants line up.
        if (IsStaticToken(token)) return ("static-token", null);
        var result = await new JsonWebTokenHandler().ValidateTokenAsync(token, new TokenValidationParameters
        {
            ValidIssuer = _options.Issuer,
            IssuerSigningKey = SigningKey,
            ValidateAudience = false, // the server's own tokens carry no audience; issuer and key are its own
        });
        if (!result.IsValid || result.ClaimsIdentity.FindFirst("sub")?.Value is not { } subject) return null;
        // A removed client's tokens are not refreshed, and no session is refreshed past its lifetime.
        if (!IsClient(subject) || LiveSessionStart(result.ClaimsIdentity) is not { } start) return null;
        return (subject, start);
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
                // The server's own tokens carry no audience: the audience is asked of the external issuer's tokens only.
                AudienceValidator = options.Audience is not { } audience ? null : (audiences, token, _) =>
                    token.Issuer == options.Issuer || audiences.Contains(audience, StringComparer.Ordinal),
            };
            jwt.Events = new JwtBearerEvents
            {
                OnMessageReceived = ctx =>
                {
                    var header = ctx.Request.Headers.Authorization.ToString();
                    if (header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) && tokens.IsStaticToken(header["Bearer ".Length..].Trim()))
                    {
                        ctx.Principal = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "static-token"), new Claim(Principals.PrincipalClaim, "static-token")], "static"));
                        ctx.Success();
                    }
                    return Task.CompletedTask;
                },
                // Who the request is: the subject of a token this server issued, or (issuer, subject) of an external one,
                // so an external user whose sub is "root" is not this server's client "root" (Principals.Name).
                OnTokenValidated = ctx =>
                {
                    if (ctx.Principal?.Identity is not ClaimsIdentity identity) { ctx.Fail("No identity"); return Task.CompletedTask; }
                    foreach (var forged in identity.FindAll(Principals.PrincipalClaim).ToList()) identity.TryRemoveClaim(forged);
                    var issuer = ctx.SecurityToken?.Issuer;
                    var local = string.Equals(issuer, options.Issuer, StringComparison.Ordinal);
                    // The server's key signs only tokens in its own name, and only its key may sign them: an external
                    // issuer's key cannot mint a local principal, nor the server's key an external one.
                    if (local != tokens.IsOwnKey(ctx.SecurityToken?.SigningKey))
                    {
                        ctx.Fail("The token's issuer and signing key do not belong together");
                        return Task.CompletedTask;
                    }
                    if (identity.FindFirst("sub")?.Value is not { Length: > 0 } subject)
                    {
                        ctx.Fail("The token has no subject");
                        return Task.CompletedTask;
                    }
                    identity.AddClaim(new Claim(Principals.PrincipalClaim, local ? subject : Principals.External(subject)));
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
        if (!tokens.TryAcquireTokenRequest(http.Connection.RemoteIpAddress?.ToString() ?? "unknown"))
        {
            http.Response.Headers.RetryAfter = "60";
            return OAuthError(429, "temporarily_unavailable", "Too many token requests from this address; retry later");
        }
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
        DateTimeOffset? sessionStart = null;
        switch (Field("grant_type"))
        {
            case "client_credentials":
                if (id is null) return OAuthError(400, "invalid_request", "client_id is required");
                if (!tokens.ValidClient(id, secret)) return OAuthError(401, "invalid_client", "Unknown client or wrong secret");
                subject = id;
                break;
            case "urn:ietf:params:oauth:grant-type:token-exchange":
                var subjectToken = Field("subject_token");
                if (subjectToken is null || await tokens.ValidateIssuedAsync(subjectToken) is not { } exchanged)
                    return OAuthError(401, "invalid_client", "The subject token is invalid or expired, or its session has ended");
                (subject, sessionStart) = exchanged;
                break;
            case var other:
                return OAuthError(400, "unsupported_grant_type", $"Unsupported grant_type '{other}'");
        }

        var (accessToken, expiresIn) = tokens.Issue(subject, Field("scope"), sessionStart);
        return Results.Json(new JsonObject
        {
            ["access_token"] = accessToken,
            ["token_type"] = "bearer",
            ["expires_in"] = Math.Max(1, (int)expiresIn.TotalSeconds),
            ["issued_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
        });
    }

    private static IResult OAuthError(int status, string error, string description) =>
        Results.Json(new JsonObject { ["error"] = error, ["error_description"] = description }, statusCode: status);
}
