using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestServer;

/// <summary>An OAuth 2.0 access token for Google Cloud, and when it expires (null: unknown).</summary>
public sealed record GcpAccessToken(string Token, DateTimeOffset? ExpiresAt);

/// <summary>
/// The server's own Google Cloud access token, which <see cref="GcsDownscopedCredentialVendor"/> exchanges for
/// downscoped ones. It needs the <c>https://www.googleapis.com/auth/cloud-platform</c> scope and must itself be allowed
/// everything a vended token may do. Built in: a static token, the metadata server, and a service account key file.
/// </summary>
public interface IGcpAccessTokenSource
{
    Task<GcpAccessToken> GetTokenAsync(CancellationToken ct = default);
}

/// <summary>A fixed token (tests, or a token minted elsewhere). Google tokens live an hour at most, so this suits short runs.</summary>
public sealed class StaticGcpAccessTokenSource(string token, DateTimeOffset? expiresAt = null) : IGcpAccessTokenSource
{
    public Task<GcpAccessToken> GetTokenAsync(CancellationToken ct = default) => Task.FromResult(new GcpAccessToken(token, expiresAt));
}

/// <summary>A source that fetches tokens over HTTP and keeps one until a quarter of its lifetime (at least a minute) remains.</summary>
public abstract class CachingGcpAccessTokenSource : IGcpAccessTokenSource, IDisposable
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private (GcpAccessToken Token, DateTimeOffset FetchedAt)? _current;

    protected CachingGcpAccessTokenSource(HttpMessageHandler? inner, Func<DateTimeOffset>? clock)
    {
        Http = inner is null ? new HttpClient() : new HttpClient(inner, disposeHandler: false);
        Clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

    protected HttpClient Http { get; }
    protected Func<DateTimeOffset> Clock { get; }

    public async Task<GcpAccessToken> GetTokenAsync(CancellationToken ct = default)
    {
        if (Fresh() is { } token) return token;
        await _lock.WaitAsync(ct);
        try
        {
            if (Fresh() is { } again) return again;
            var now = Clock();
            var fetched = await FetchAsync(ct);
            _current = (fetched, now);
            return fetched;
        }
        finally { _lock.Release(); }
    }

    private GcpAccessToken? Fresh()
    {
        if (_current is not var (token, fetchedAt) || token.ExpiresAt is not { } expiresAt) return null;
        var margin = TimeSpan.FromTicks(Math.Max(TimeSpan.FromMinutes(1).Ticks, (expiresAt - fetchedAt).Ticks / 4));
        return expiresAt - Clock() > margin ? token : null;
    }

    protected abstract Task<GcpAccessToken> FetchAsync(CancellationToken ct);

    /// <summary>Reads an OAuth token response (<c>access_token</c>, optional <c>expires_in</c>).</summary>
    protected async Task<GcpAccessToken> ReadTokenAsync(HttpResponseMessage resp, string what, CancellationToken ct)
    {
        var text = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode) throw new InvalidOperationException($"{what} failed ({(int)resp.StatusCode}): {text}");
        return GcsDownscopedCredentialVendor.ParseTokenResponse(text, Clock(), what);
    }

    public void Dispose()
    {
        Http.Dispose();
        _lock.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// The default service account's token from the GCE / GKE / Cloud Run metadata server
/// (<c>GET /computeMetadata/v1/instance/service-accounts/default/token</c> with <c>Metadata-Flavor: Google</c>). The host
/// is <c>GCE_METADATA_HOST</c> when set, else <c>metadata.google.internal</c>, as in Google's client libraries.
/// </summary>
public sealed class MetadataServerGcpAccessTokenSource(string? endpoint = null, HttpMessageHandler? inner = null, Func<DateTimeOffset>? clock = null)
    : CachingGcpAccessTokenSource(inner, clock)
{
    public string Endpoint { get; } = (endpoint ?? (Environment.GetEnvironmentVariable("GCE_METADATA_HOST") is { Length: > 0 } host ? $"http://{host}" : "http://metadata.google.internal"))
        .TrimEnd('/');

    protected override async Task<GcpAccessToken> FetchAsync(CancellationToken ct)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, $"{Endpoint}/computeMetadata/v1/instance/service-accounts/default/token");
        req.Headers.Add("Metadata-Flavor", "Google");
        using var resp = await Http.SendAsync(req, ct);
        return await ReadTokenAsync(resp, "Metadata server token request", ct);
    }
}

/// <summary>
/// A token for a service account from its JSON key file: an RS256-signed JWT assertion (OAuth 2.0 JWT bearer grant,
/// RFC 7523) posted to the key's <c>token_uri</c>, for the <c>cloud-platform</c> scope.
/// </summary>
public sealed class ServiceAccountGcpAccessTokenSource : CachingGcpAccessTokenSource
{
    public const string Scope = "https://www.googleapis.com/auth/cloud-platform";
    private readonly string _email;
    private readonly string? _keyId;
    private readonly string _tokenUri;
    private readonly RSA _key;

    public ServiceAccountGcpAccessTokenSource(string keyJson, HttpMessageHandler? inner = null, Func<DateTimeOffset>? clock = null) : base(inner, clock)
    {
        var key = JsonNode.Parse(keyJson) ?? throw new InvalidDataException("Empty service account key");
        string Field(string name) => key[name]?.GetValue<string>() ?? throw new InvalidDataException($"Service account key without '{name}'");
        _email = Field("client_email");
        _keyId = key["private_key_id"]?.GetValue<string>();
        _tokenUri = key["token_uri"]?.GetValue<string>() ?? "https://oauth2.googleapis.com/token";
        _key = RSA.Create();
        _key.ImportFromPem(Field("private_key"));
    }

    public static ServiceAccountGcpAccessTokenSource FromFile(string path) => new(File.ReadAllText(path));

    /// <summary>The signed JWT assertion for a token request at <paramref name="now"/>.</summary>
    public string Assertion(DateTimeOffset now)
    {
        static string B64(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        var header = new JsonObject { ["alg"] = "RS256", ["typ"] = "JWT" };
        if (_keyId is not null) header["kid"] = _keyId;
        var iat = now.ToUnixTimeSeconds();
        var claims = new JsonObject { ["iss"] = _email, ["scope"] = Scope, ["aud"] = _tokenUri, ["iat"] = iat, ["exp"] = iat + 3600 };
        var signingInput = B64(Encoding.UTF8.GetBytes(header.ToJsonString())) + "." + B64(Encoding.UTF8.GetBytes(claims.ToJsonString()));
        var signature = _key.SignData(Encoding.ASCII.GetBytes(signingInput), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return signingInput + "." + B64(signature);
    }

    protected override async Task<GcpAccessToken> FetchAsync(CancellationToken ct)
    {
        using var resp = await Http.PostAsync(_tokenUri, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:jwt-bearer",
            ["assertion"] = Assertion(Clock()),
        }), ct);
        return await ReadTokenAsync(resp, $"Service account token request for {_email}", ct);
    }
}

/// <summary>
/// Vends downscoped OAuth tokens for tables at <c>gs://{bucket}/{path}</c>, using Google's Credential Access Boundary
/// flow: the server's own token (from <see cref="IGcpAccessTokenSource"/>) is exchanged at Google's STS
/// (<c>POST https://sts.googleapis.com/v1/token</c>, RFC 8693 token exchange) for one limited to the table's bucket,
/// to <c>roles/storage.objectViewer</c> for readers or <c>roles/storage.objectAdmin</c> for writers, and by an
/// availability condition to objects under the table's prefix (and listing with that prefix).
/// Returned as Iceberg's <c>gcs.oauth2.token</c> and <c>gcs.oauth2.token-expires-at</c> (milliseconds since the epoch),
/// which Iceberg Java's GCSFileIO and PyIceberg read. Tokens are cached per (location, access) until a quarter of their
/// lifetime remains. Other schemes get none.
/// </summary>
public sealed class GcsDownscopedCredentialVendor : IStorageCredentialVendor, IDisposable
{
    public const string ReadRole = "roles/storage.objectViewer";
    public const string WriteRole = "roles/storage.objectAdmin";
    private const string TokenExchange = "urn:ietf:params:oauth:grant-type:token-exchange";
    private const string AccessTokenType = "urn:ietf:params:oauth:token-type:access_token";

    private readonly GcsCredentialVendingOptions _options;
    private readonly IGcpAccessTokenSource _source;
    private readonly HttpClient _http;
    private readonly Func<DateTimeOffset> _clock;
    private readonly ConcurrentDictionary<(string Location, bool Write), (VendedCredential Credential, DateTimeOffset IssuedAt)> _cache = new();

    public GcsDownscopedCredentialVendor(GcsCredentialVendingOptions options, IGcpAccessTokenSource source, HttpMessageHandler? inner = null,
        Func<DateTimeOffset>? clock = null)
    {
        _options = options;
        _source = source;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        _http = inner is null ? new HttpClient() : new HttpClient(inner, disposeHandler: false);
    }

    public async Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default)
    {
        if (GcsLocation(location) is not var (bucket, key)) return [];
        var prefix = location.TrimEnd('/') + "/";
        if (_cache.TryGetValue((prefix, write), out var cached) && cached.Credential.Expiration is { } exp
            && exp - _clock() > (exp - cached.IssuedAt) / 4)
            return [cached.Credential];
        var objectPrefix = key.Trim('/') is { Length: > 0 } k ? k + "/" : "";
        var issuedAt = _clock();
        var vended = await ExchangeAsync(bucket, objectPrefix, prefix, write, ct);
        if (vended.Expiration is not null) _cache[(prefix, write)] = (vended, issuedAt);
        return [vended];
    }

    /// <summary>
    /// The Credential Access Boundary (the token exchange's <c>options</c>, before URL encoding): one rule on the bucket,
    /// with the reader or writer role, available only for objects under <paramref name="objectPrefix"/> and for
    /// listings whose prefix starts with it.
    /// </summary>
    public static string AccessBoundary(string bucket, string objectPrefix, bool write) => new JsonObject
    {
        ["accessBoundary"] = new JsonObject
        {
            ["accessBoundaryRules"] = new JsonArray(new JsonObject
            {
                ["availablePermissions"] = new JsonArray($"inRole:{(write ? WriteRole : ReadRole)}"),
                ["availableResource"] = $"//storage.googleapis.com/projects/_/buckets/{bucket}",
                ["availabilityCondition"] = new JsonObject { ["expression"] = Condition(bucket, objectPrefix) },
            }),
        },
    }.ToJsonString(BoundaryJson);

    // Quotes in the CEL expression stay readable ('…' rather than \u0027); the result is form-encoded anyway.
    private static readonly System.Text.Json.JsonSerializerOptions BoundaryJson = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>
    /// The CEL availability condition: object names (<c>projects/_/buckets/{bucket}/objects/{name}</c>) under the prefix,
    /// or a list request (on the bucket) whose <c>prefix</c> parameter starts with it.
    /// </summary>
    public static string Condition(string bucket, string objectPrefix) =>
        $"resource.name.startsWith({CelString($"projects/_/buckets/{bucket}/objects/{objectPrefix}")})"
        + $" || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith({CelString(objectPrefix)})";

    /// <summary>A single-quoted CEL string literal: backslashes, quotes and control characters escaped.</summary>
    public static string CelString(string value)
    {
        var sb = new StringBuilder("'");
        foreach (var c in value)
        {
            switch (c)
            {
                case '\\': sb.Append(@"\\"); break;
                case '\'': sb.Append(@"\'"); break;
                case '\n': sb.Append(@"\n"); break;
                case '\r': sb.Append(@"\r"); break;
                case '\t': sb.Append(@"\t"); break;
                case < ' ' or '\u007f': sb.Append(CultureInfo.InvariantCulture, $"\\u{(int)c:x4}"); break;
                default: sb.Append(c); break;
            }
        }
        return sb.Append('\'').ToString();
    }

    private async Task<VendedCredential> ExchangeAsync(string bucket, string objectPrefix, string prefix, bool write, CancellationToken ct)
    {
        var source = await _source.GetTokenAsync(ct);
        var form = new Dictionary<string, string>
        {
            ["grant_type"] = TokenExchange,
            ["subject_token_type"] = AccessTokenType,
            ["requested_token_type"] = AccessTokenType,
            ["subject_token"] = source.Token,
            ["options"] = AccessBoundary(bucket, objectPrefix, write),
        };
        using var resp = await _http.PostAsync(_options.StsEndpoint, new FormUrlEncodedContent(form), ct);
        var text = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException($"GCS token exchange for gs://{bucket}/{objectPrefix} failed ({(int)resp.StatusCode}): {text}");
        var token = ParseTokenResponse(text, _clock(), "GCS token exchange");
        // Google's STS returns expires_in only for some source tokens; a downscoped token never outlives its source.
        DateTimeOffset? expiresAt = (token.ExpiresAt, source.ExpiresAt) switch
        {
            ({ } a, { } b) => a < b ? a : b,
            ({ } a, null) => a,
            (null, var b) => b,
        };
        var config = new Dictionary<string, string> { ["gcs.oauth2.token"] = token.Token };
        if (expiresAt is { } e) config["gcs.oauth2.token-expires-at"] = e.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
        return new VendedCredential(prefix, config, expiresAt);
    }

    /// <summary>An OAuth token response: <c>access_token</c> and, when present, <c>expires_in</c> seconds from <paramref name="now"/>.</summary>
    public static GcpAccessToken ParseTokenResponse(string json, DateTimeOffset now, string what)
    {
        JsonNode? doc;
        try { doc = JsonNode.Parse(json); }
        catch (System.Text.Json.JsonException e) { throw new InvalidDataException($"{what}: the response is not JSON", e); }
        var token = doc?["access_token"]?.GetValue<string>();
        if (string.IsNullOrEmpty(token)) throw new InvalidDataException($"{what}: the response has no access_token");
        DateTimeOffset? expiresAt = doc!["expires_in"] switch
        {
            JsonValue v when v.TryGetValue<long>(out var s) => now.AddSeconds(s),
            JsonValue v when v.TryGetValue<string>(out var s) && long.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out var n) => now.AddSeconds(n),
            _ => null,
        };
        return new GcpAccessToken(token, expiresAt);
    }

    /// <summary>(bucket, path) of a <c>gs://</c> (or PyIceberg's <c>gcs://</c>) location; null otherwise.</summary>
    public static (string Bucket, string Key)? GcsLocation(string location)
    {
        var scheme = location.IndexOf("://", StringComparison.Ordinal);
        if (scheme < 0 || location[..scheme] is not ("gs" or "gcs")) return null;
        var rest = location[(scheme + 3)..];
        var slash = rest.IndexOf('/');
        var bucket = slash < 0 ? rest : rest[..slash];
        if (bucket.Length == 0) return null;
        return (bucket, slash < 0 ? "" : rest[(slash + 1)..]);
    }

    public void Dispose() => _http.Dispose();
}
