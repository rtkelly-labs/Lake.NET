using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using Amazon.Runtime;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestServer;

/// <summary>A storage credential for everything under <see cref="Prefix"/> (REST <c>StorageCredential</c>).</summary>
public sealed record VendedCredential(string Prefix, IReadOnlyDictionary<string, string> Config, DateTimeOffset? Expiration)
{
    public JsonObject ToJson() => new() { ["prefix"] = Prefix, ["config"] = Json.MetadataJson.StringMap(Config) };
}

/// <summary>
/// Vends short-lived storage credentials for a table's location. Asked only when the client sends
/// <c>X-Iceberg-Access-Delegation: vended-credentials</c> (or calls <c>loadCredentials</c>), after authorization.
/// Register your own to replace the STS one.
/// </summary>
public interface IStorageCredentialVendor
{
    /// <param name="write">The principal may write the table: vend credentials that can write and delete under the location.</param>
    Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default);
}

/// <summary>No credential vending: the header is ignored and <c>loadCredentials</c> is not served.</summary>
public sealed class NoCredentialVendor : IStorageCredentialVendor
{
    public static readonly NoCredentialVendor Instance = new();

    public Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<VendedCredential>>([]);
}

/// <summary>
/// STS <c>AssumeRole</c> (Query API over HTTP, signed with <see cref="SigV4Handler"/>) on the configured role, with a
/// session policy that narrows it to the table's S3 prefix: <c>s3:GetObject</c> and listing for readers, plus
/// <c>s3:PutObject</c> and <c>s3:DeleteObject</c> for writers. Credentials are cached per (location, access) until a
/// quarter of their lifetime remains. Locations that are not <c>s3://</c> get none.
/// </summary>
public sealed class StsCredentialVendor : IStorageCredentialVendor, IDisposable
{
    private readonly CredentialVendingOptions _options;
    private readonly HttpClient _http;
    private readonly Func<DateTimeOffset> _clock;
    private readonly ConcurrentDictionary<(string Location, bool Write), VendedCredential> _cache = new();

    public StsCredentialVendor(CredentialVendingOptions options, IReadOnlyDictionary<string, string> fileIO, HttpMessageHandler? inner = null, Func<DateTimeOffset>? clock = null)
    {
        _options = options;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        var accessKey = options.AccessKeyId ?? fileIO.GetValueOrDefault("s3.access-key-id");
        var secret = options.SecretAccessKey ?? fileIO.GetValueOrDefault("s3.secret-access-key");
        var token = options.AccessKeyId is not null ? options.SessionToken : fileIO.GetValueOrDefault("s3.session-token");
        Func<CancellationToken, Task<ImmutableCredentials>> credentials = accessKey is not null && secret is not null
            ? _ => Task.FromResult(new ImmutableCredentials(accessKey, secret, token))
            : _ => Amazon.Runtime.Credentials.DefaultAWSCredentialsIdentityResolver.GetCredentials().GetCredentialsAsync();
        var endpoint = options.StsEndpoint ?? $"https://sts.{options.Region}.amazonaws.com/";
        _http = new HttpClient(new SigV4Handler(credentials, options.Region, "sts", () => DateTime.UtcNow, inner)) { BaseAddress = new Uri(endpoint) };
    }

    public async Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default)
    {
        if (S3Location(location) is not var (bucket, key)) return [];
        var prefix = location.TrimEnd('/') + "/";
        if (_cache.TryGetValue((prefix, write), out var cached) && cached.Expiration is { } exp
            && exp - _clock() > TimeSpan.FromSeconds(_options.DurationSeconds / 4.0))
            return [cached];
        var vended = await AssumeRoleAsync(table, bucket, key.TrimEnd('/'), prefix, write, ct);
        _cache[(prefix, write)] = vended;
        return [vended];
    }

    /// <summary>The session policy: the role's permissions, narrowed to <paramref name="key"/> in <paramref name="bucket"/>.</summary>
    public static string SessionPolicy(string bucket, string key, bool write)
    {
        var objects = new JsonArray("s3:GetObject", "s3:GetObjectVersion");
        if (write) { objects.Add("s3:PutObject"); objects.Add("s3:DeleteObject"); objects.Add("s3:AbortMultipartUpload"); }
        var keyPrefix = key.Length == 0 ? "" : key + "/";
        return new JsonObject
        {
            ["Version"] = "2012-10-17",
            ["Statement"] = new JsonArray(
                new JsonObject { ["Effect"] = "Allow", ["Action"] = objects, ["Resource"] = $"arn:aws:s3:::{bucket}/{keyPrefix}*" },
                new JsonObject
                {
                    ["Effect"] = "Allow", ["Action"] = "s3:ListBucket", ["Resource"] = $"arn:aws:s3:::{bucket}",
                    ["Condition"] = new JsonObject { ["StringLike"] = new JsonObject { ["s3:prefix"] = $"{keyPrefix}*" } },
                },
                new JsonObject { ["Effect"] = "Allow", ["Action"] = "s3:GetBucketLocation", ["Resource"] = $"arn:aws:s3:::{bucket}" }),
        }.ToJsonString();
    }

    private async Task<VendedCredential> AssumeRoleAsync(TableIdentifier table, string bucket, string key, string prefix, bool write, CancellationToken ct)
    {
        // Session names allow [\w+=,.@-], up to 64 characters.
        var session = new string($"iceberg-{table}".Select(c => char.IsAsciiLetterOrDigit(c) || "+=,.@-_".Contains(c) ? c : '-').Take(64).ToArray());
        var form = new Dictionary<string, string>
        {
            ["Action"] = "AssumeRole",
            ["Version"] = "2011-06-15",
            ["RoleArn"] = _options.RoleArn!,
            ["RoleSessionName"] = session,
            ["DurationSeconds"] = _options.DurationSeconds.ToString(CultureInfo.InvariantCulture),
            ["Policy"] = SessionPolicy(bucket, key, write),
        };
        if (_options.ExternalId is { } externalId) form["ExternalId"] = externalId;
        using var resp = await _http.PostAsync("", new FormUrlEncodedContent(form), ct);
        var text = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode)
            throw new InvalidOperationException($"STS AssumeRole on {_options.RoleArn} failed ({(int)resp.StatusCode}): {text}");
        var doc = XDocument.Parse(text);
        string Field(string name) => doc.Descendants().FirstOrDefault(e => e.Name.LocalName == "Credentials")?.Elements().FirstOrDefault(e => e.Name.LocalName == name)?.Value
            ?? throw new InvalidDataException($"STS AssumeRole response without Credentials/{name}");
        var expiration = DateTimeOffset.Parse(Field("Expiration"), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
        return new VendedCredential(prefix, new Dictionary<string, string>
        {
            ["s3.access-key-id"] = Field("AccessKeyId"),
            ["s3.secret-access-key"] = Field("SecretAccessKey"),
            ["s3.session-token"] = Field("SessionToken"),
            ["s3.session-token-expires-at-ms"] = expiration.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
            ["client.region"] = _options.Region,
        }, expiration);
    }

    /// <summary>(bucket, key) of an <c>s3://</c>, <c>s3a://</c> or <c>s3n://</c> location; null otherwise.</summary>
    public static (string Bucket, string Key)? S3Location(string location)
    {
        var scheme = location.IndexOf("://", StringComparison.Ordinal);
        if (scheme < 0 || location[..scheme] is not ("s3" or "s3a" or "s3n")) return null;
        var rest = location[(scheme + 3)..];
        var slash = rest.IndexOf('/');
        return slash < 0 ? (rest, "") : (rest[..slash], rest[(slash + 1)..]);
    }

    public void Dispose() => _http.Dispose();
}
