using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Amazon.Runtime;
using Iceberg.Net.Catalog;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.RestServer;

/// <summary>A <c>RemoteSignRequest</c>: the S3 request a client wants signed.</summary>
public sealed record RemoteSignRequest(string Region, Uri Uri, string Method, IReadOnlyDictionary<string, IReadOnlyList<string>> Headers, string? Body, string? Provider)
{
    public static RemoteSignRequest Parse(JsonNode body)
    {
        var headers = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in body["headers"]?.AsObject() ?? throw new InvalidDataException("headers are required"))
        {
            var values = v is JsonArray a ? a.Select(x => x!.GetValue<string>()).ToList() : [v!.GetValue<string>()];
            headers[k] = headers.TryGetValue(k, out var prev) ? [.. prev, .. values] : values;
        }
        var uri = new Uri(body["uri"]!.GetValue<string>(), UriKind.Absolute);
        if (uri.Scheme is not ("http" or "https")) throw new InvalidDataException($"Cannot sign a {uri.Scheme} URI");
        return new RemoteSignRequest(body["region"]!.GetValue<string>(), uri, body["method"]!.GetValue<string>().ToUpperInvariant(), headers,
            body["body"]?.GetValue<string>(), body["provider"]?.GetValue<string>());
    }

    /// <summary>Whether the request changes storage (anything but GET and HEAD).</summary>
    public bool Writes => Method is not ("GET" or "HEAD");
}

/// <summary>
/// S3 remote signing (<c>POST …/tables/{table}/sign</c>, Iceberg Java's <c>S3V4RestSignerClient</c> and PyIceberg's
/// <c>S3V4RestSigner</c>): the client sends the S3 request it is about to make, unsigned; the server checks that it only
/// touches the table's own objects and signs it (SigV4, <c>UNSIGNED-PAYLOAD</c>) with the server's credentials. So
/// clients never hold storage credentials at all.
/// <list type="bullet">
/// <item>The URI must address the configured S3 endpoint (<c>FileIO:s3.endpoint</c>, path-style or virtual-hosted), or AWS
/// S3 when none is configured.</item>
/// <item>Object requests must name a key under the table's location (or its <c>write.data.path</c> /
/// <c>write.metadata.path</c>); a copy's <c>x-amz-copy-source</c> too. Bucket requests are limited to listing under such a
/// prefix, <c>DeleteObjects</c> whose body names only such keys, <c>GetBucketLocation</c> and <c>HeadBucket</c>.</item>
/// <item>GET and HEAD need <c>read</c> on the table; anything else needs <c>write</c>.</item>
/// </list>
/// </summary>
public sealed partial class S3RequestSigner
{
    private const string Unsigned = "UNSIGNED-PAYLOAD";
    private const string UnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";

    /// <summary>Never signed: they change between attempts, or the signer sets them itself.</summary>
    private static readonly HashSet<string> UnsignedHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "authorization", "range", "x-amz-date", "x-amz-content-sha256", "x-amz-security-token", "amz-sdk-invocation-id", "amz-sdk-retry",
        "amz-sdk-request", "expect", "connection", "transfer-encoding", "content-length", "user-agent", "host",
    };

    private readonly RemoteSigningOptions _options;
    private readonly Uri? _endpoint;
    private readonly Func<CancellationToken, Task<ImmutableCredentials>> _credentials;
    private readonly Func<DateTime> _clock;

    public S3RequestSigner(RemoteSigningOptions options, IReadOnlyDictionary<string, string> fileIO, Func<DateTime>? clock = null)
    {
        _options = options;
        _clock = clock ?? (() => DateTime.UtcNow);
        _endpoint = fileIO.GetValueOrDefault("s3.endpoint") is { Length: > 0 } e ? new Uri(e) : null;
        var accessKey = options.AccessKeyId ?? fileIO.GetValueOrDefault("s3.access-key-id");
        var secret = options.SecretAccessKey ?? fileIO.GetValueOrDefault("s3.secret-access-key");
        var token = options.AccessKeyId is not null ? options.SessionToken : fileIO.GetValueOrDefault("s3.session-token");
        _credentials = accessKey is not null && secret is not null
            ? _ => Task.FromResult(new ImmutableCredentials(accessKey, secret, token))
            : _ => Amazon.Runtime.Credentials.DefaultAWSCredentialsIdentityResolver.GetCredentials().GetCredentialsAsync();
        Region = options.Region ?? fileIO.GetValueOrDefault("client.region");
    }

    /// <summary>The region requests are signed for; null: whatever the client says.</summary>
    public string? Region { get; }

    /// <summary>The table's key prefixes, as (bucket, key prefix ending in '/'): its location and any data/metadata path.</summary>
    public static IReadOnlyList<(string Bucket, string Prefix)> TablePrefixes(TableMetadata metadata)
    {
        var result = new List<(string, string)>();
        foreach (var location in new[] { metadata.Location, metadata.Properties.GetValueOrDefault("write.data.path"), metadata.Properties.GetValueOrDefault("write.metadata.path") })
            if (location is not null && StsCredentialVendor.S3Location(location) is var (bucket, key))
                result.Add((bucket, key.Length == 0 ? "" : key.TrimEnd('/') + "/"));
        return result;
    }

    /// <summary>Check the request against <paramref name="prefixes"/> and sign it; returns the <c>RemoteSignResult</c> and its <c>Cache-Control</c>.</summary>
    /// <exception cref="ForbiddenException">The request reaches outside the table, or another endpoint.</exception>
    /// <exception cref="BadRequestException">The request cannot be signed as given.</exception>
    public async Task<(JsonObject Result, string CacheControl)> SignAsync(RemoteSignRequest request, IReadOnlyList<(string Bucket, string Prefix)> prefixes, CancellationToken ct = default)
    {
        if (request.Provider is { } provider && !provider.Equals("s3", StringComparison.OrdinalIgnoreCase))
            throw new BadRequestException($"This server signs S3 requests only, not '{provider}'");
        if (Region is { } region && !region.Equals(request.Region, StringComparison.Ordinal))
            throw new BadRequestException($"Requests are signed for region {region}, not {request.Region}");
        Authorize(request, prefixes);

        var payload = request.Body is { } body
            ? Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(body))).ToLowerInvariant()
            : request.Headers.TryGetValue("x-amz-content-sha256", out var sha) && sha.Contains(UnsignedTrailer) ? UnsignedTrailer : Unsigned;
        var toSign = request.Headers.Where(h => !UnsignedHeaders.Contains(h.Key)).Select(h => KeyValuePair.Create(h.Key, (IEnumerable<string>)h.Value));
        var signing = SigV4Handler.SignS3(request.Method, request.Uri, toSign, payload, await _credentials(ct), request.Region, _clock());

        var headers = new JsonObject();
        foreach (var (k, v) in request.Headers.Where(h => !signing.ContainsKey(h.Key)))
            headers[k] = new JsonArray(v.Select(x => (JsonNode)x).ToArray());
        foreach (var (k, v) in signing) headers[k] = new JsonArray(v);
        return (new JsonObject { ["uri"] = request.Uri.OriginalString, ["headers"] = headers }, request.Writes ? "no-cache" : "private");
    }

    // What S3 clients (Iceberg Java, PyIceberg, this library) send for a table's objects: plain object reads and writes,
    // multipart uploads, versions, and the AWS SDK for Java's x-id operation hint. Anything else on an object is a
    // sub-resource a table does not need (acl, tagging, retention, legal-hold, restore, …).
    private static readonly HashSet<string> ObjectQuery = new(StringComparer.Ordinal) { "uploads", "uploadId", "partNumber", "versionId", "x-id" };
    private static readonly HashSet<string> ListQuery = new(StringComparer.Ordinal)
        { "prefix", "list-type", "delimiter", "continuation-token", "max-keys", "start-after", "encoding-type", "fetch-owner", "x-id" };

    /// <summary>The methods S3 object and bucket requests of a table use; anything else (PATCH, OPTIONS, …) is not signed.</summary>
    private static readonly HashSet<string> Methods = new(StringComparer.Ordinal) { "GET", "HEAD", "PUT", "POST", "DELETE" };

    /// <summary>
    /// The headers a request to be signed may carry: what S3 clients send to read, write, copy, list and delete a
    /// table's objects (transport, SDK and tracing headers, content and conditional headers, checksums, user metadata,
    /// storage class, SSE-S3 and SSE-C). Any other header is refused, and with it everything that changes who may read
    /// an object or how long it is kept (<c>x-amz-acl</c>, <c>x-amz-grant-*</c>, <c>x-amz-object-lock-*</c>), tags it
    /// (<c>x-amz-tagging</c>) or encrypts it with a KMS key the client picks (<c>x-amz-server-side-encryption-aws-kms-key-id</c>,
    /// <c>-context</c>), and every header S3 adds in future until it is listed here.
    /// </summary>
    private static readonly HashSet<string> AllowedHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "host", "authorization", "range", "expect", "connection", "transfer-encoding", "content-length", "user-agent", "accept", "accept-encoding",
        "x-amz-user-agent", "amz-sdk-invocation-id", "amz-sdk-retry", "amz-sdk-request", "traceparent", "tracestate", "x-amzn-trace-id",
        "content-type", "content-md5", "content-encoding", "content-language", "content-disposition", "cache-control", "expires",
        "if-match", "if-none-match", "if-modified-since", "if-unmodified-since",
        "x-amz-date", "x-amz-content-sha256", "x-amz-security-token", "x-amz-decoded-content-length", "x-amz-trailer", "x-amz-te",
        "x-amz-sdk-checksum-algorithm", "x-amz-checksum-mode", "x-amz-checksum-type",
        "x-amz-copy-source", "x-amz-copy-source-range", "x-amz-copy-source-if-match", "x-amz-copy-source-if-none-match",
        "x-amz-copy-source-if-modified-since", "x-amz-copy-source-if-unmodified-since", "x-amz-metadata-directive",
        "x-amz-storage-class", "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-customer-algorithm", "x-amz-server-side-encryption-customer-key", "x-amz-server-side-encryption-customer-key-md5",
        "x-amz-copy-source-server-side-encryption-customer-algorithm", "x-amz-copy-source-server-side-encryption-customer-key",
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
        "x-amz-request-payer", "x-amz-expected-bucket-owner", "x-amz-source-expected-bucket-owner", "x-amz-mp-object-size",
    };

    private static bool AllowedHeader(string name) =>
        AllowedHeaders.Contains(name)
        || name.StartsWith("x-amz-checksum-", StringComparison.OrdinalIgnoreCase)
        || name.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase);

    /// <summary>SSE-S3 or SSE-KMS under the bucket's own key: a key the client names is refused with its header.</summary>
    private static readonly HashSet<string> Encryption = new(StringComparer.Ordinal) { "AES256", "aws:kms", "aws:kms:dsse" };

    private void Authorize(RemoteSignRequest request, IReadOnlyList<(string Bucket, string Prefix)> prefixes)
    {
        var (bucket, key) = Address(request.Uri);
        bool Allowed(string b, string k) => prefixes.Any(p => p.Bucket == b && k.StartsWith(p.Prefix, StringComparison.Ordinal)) && !HasDotSegment(k);
        if (!Methods.Contains(request.Method)) throw new ForbiddenException($"{request.Method} requests are not signed");
        if (request.Headers.Keys.FirstOrDefault(h => !AllowedHeader(h)) is { } unknown)
            throw new ForbiddenException($"Requests with the {unknown} header are not signed");
        if (request.Headers.TryGetValue("x-amz-server-side-encryption", out var sse) && sse.FirstOrDefault(v => !Encryption.Contains(v.Trim())) is { } mode)
            throw new ForbiddenException($"Requests encrypting with {mode} are not signed");
        var query = ParseQuery(request.Uri.Query);
        if (key is not null)
        {
            if (!Allowed(bucket, key)) throw new ForbiddenException($"s3://{bucket}/{key} is outside the table");
            if (query.Keys.FirstOrDefault(k => !ObjectQuery.Contains(k)) is { } sub)
                throw new ForbiddenException($"The ?{sub} sub-resource of an object is not something a table needs");
            if (request.Headers.TryGetValue("x-amz-copy-source", out var sources))
                foreach (var source in sources)
                {
                    var path = Uri.UnescapeDataString(source.Split('?')[0]).TrimStart('/');
                    var slash = path.IndexOf('/');
                    if (slash <= 0 || !Allowed(path[..slash], path[(slash + 1)..])) throw new ForbiddenException($"Copy source {source} is outside the table");
                }
            return;
        }

        // Bucket requests: only the table's own buckets, and only the parameters each operation takes.
        if (!prefixes.Any(p => p.Bucket == bucket)) throw new ForbiddenException($"Bucket {bucket} holds none of the table's files");
        switch (request.Method)
        {
            case "GET" when query.Keys.All(k => k is "location" or "x-id") && query.ContainsKey("location"):
            case "HEAD" when query.Count == 0:
                return;
            case "GET" when query.Keys.All(ListQuery.Contains) && query.TryGetValue("prefix", out var prefix):
                if (!Allowed(bucket, prefix)) throw new ForbiddenException($"Listing s3://{bucket}/{prefix} reaches outside the table");
                return;
            case "POST" when query.ContainsKey("delete") && query.Keys.All(k => k is "delete" or "x-id"):
                if (request.Body is null) throw new BadRequestException("A DeleteObjects request must be signed with its body");
                XDocument doc;
                try { doc = XDocument.Parse(request.Body); }
                catch (System.Xml.XmlException e) { throw new BadRequestException($"The DeleteObjects body is not XML: {e.Message}"); }
                var keys = doc.Descendants().Where(e => e.Name.LocalName == "Key").Select(e => e.Value).ToList();
                if (keys.Count == 0) throw new BadRequestException("The DeleteObjects body names no keys");
                foreach (var k in keys)
                    if (!Allowed(bucket, k)) throw new ForbiddenException($"s3://{bucket}/{k} is outside the table");
                return;
            default:
                throw new ForbiddenException($"{request.Method} on bucket {bucket} is not something a table needs");
        }
    }

    private static bool HasDotSegment(string key) => key.Split('/').Any(s => s is "." or "..");

    /// <summary>(bucket, key or null for a bucket request) of an S3 URI on the configured endpoint, path-style or virtual-hosted.</summary>
    internal (string Bucket, string? Key) Address(Uri uri)
    {
        var segments = uri.AbsolutePath.TrimStart('/').Split('/').Select(Uri.UnescapeDataString).ToList();
        string? virtualBucket;
        if (_endpoint is not null)
        {
            if (!uri.Scheme.Equals(_endpoint.Scheme, StringComparison.OrdinalIgnoreCase) || uri.Port != _endpoint.Port)
                throw new ForbiddenException($"{uri.GetLeftPart(UriPartial.Authority)} is not the configured S3 endpoint");
            if (uri.Host.Equals(_endpoint.Host, StringComparison.OrdinalIgnoreCase)) virtualBucket = null;
            else if (uri.Host.EndsWith("." + _endpoint.Host, StringComparison.OrdinalIgnoreCase)) virtualBucket = uri.Host[..^(_endpoint.Host.Length + 1)];
            else throw new ForbiddenException($"{uri.Host} is not the configured S3 endpoint");
        }
        else
        {
            var m = AwsHost().Match(uri.Host);
            if (!m.Success || uri.Scheme != "https") throw new ForbiddenException($"{uri.GetLeftPart(UriPartial.Authority)} is not an S3 endpoint");
            virtualBucket = m.Groups["bucket"].Success ? m.Groups["bucket"].Value : null;
        }

        if (virtualBucket is not null)
            return (virtualBucket, segments is [""] ? null : string.Join('/', segments));
        if (segments is [""]) throw new ForbiddenException("Requests to the S3 service itself are not signed");
        return (segments[0], segments.Count == 1 || segments is [_, ""] ? null : string.Join('/', segments.Skip(1)));
    }

    [GeneratedRegex(@"^(?:(?<bucket>[a-z0-9][a-z0-9.\-]*)\.)?s3(?:[.\-][a-z0-9\-]+)*\.amazonaws\.com(?:\.cn)?$", RegexOptions.IgnoreCase)]
    private static partial Regex AwsHost();

    private static Dictionary<string, string> ParseQuery(string query)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var i = pair.IndexOf('=');
            result[Uri.UnescapeDataString(i < 0 ? pair : pair[..i])] = i < 0 ? "" : Uri.UnescapeDataString(pair[(i + 1)..].Replace('+', ' '));
        }
        return result;
    }

    /// <summary>
    /// The <c>config</c> that tells clients to sign through this server: Java's <c>s3.remote-signing-enabled</c> (Java
    /// 1.12 then calls the table's own <c>…/sign</c> route), and PyIceberg's <c>s3.signer</c> and
    /// <c>s3.signer.endpoint</c> (with the fsspec FileIO, the one that can sign remotely).
    /// </summary>
    public IReadOnlyDictionary<string, string> TableConfig(string? prefix, TableIdentifier id)
    {
        var path = $"v1/{(prefix is { Length: > 0 } p ? Uri.EscapeDataString(p) + "/" : "")}namespaces/{RestCatalog.EncodeNamespace(id.Namespace)}/tables/{Uri.EscapeDataString(id.Name)}/sign";
        var config = new Dictionary<string, string>
        {
            ["s3.remote-signing-enabled"] = "true",
            ["s3.signer"] = "S3V4RestSigner",
            ["s3.signer.endpoint"] = path,
            ["py-io-impl"] = "pyiceberg.io.fsspec.FsspecFileIO",
        };
        if (Region is { } region) config["client.region"] = region;
        return config;
    }
}
