using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Amazon.Runtime;

namespace Iceberg.Net.Catalog;

/// <summary>
/// AWS Signature Version 4 for REST catalog requests — what Java's <c>rest.sigv4-enabled</c> does — so
/// <see cref="RestCatalog"/> can talk to the Glue Iceberg REST endpoint (<c>https://glue.{region}.amazonaws.com/iceberg</c>,
/// signing name <c>glue</c>) and S3 Tables (<c>https://s3tables.{region}.amazonaws.com/iceberg</c>, <c>s3tables</c>).
/// Plug it in through <see cref="RestCatalogOptions.Handler"/>. An existing <c>Authorization</c> header (an OAuth token)
/// is moved to <c>Original-Authorization</c>, as Java does.
/// </summary>
public sealed class SigV4Handler : DelegatingHandler
{
    private readonly Func<CancellationToken, Task<ImmutableCredentials>> _credentials;
    private readonly string _region;
    private readonly string _service;
    private readonly Func<DateTime> _clock;

    public SigV4Handler(AWSCredentials credentials, string region, string service, HttpMessageHandler? inner = null)
        : this(ct => credentials.GetCredentialsAsync(), region, service, () => DateTime.UtcNow, inner) { }

    public SigV4Handler(Func<CancellationToken, Task<ImmutableCredentials>> credentials, string region, string service, Func<DateTime> clock, HttpMessageHandler? inner = null)
        // No redirects: a followed redirect would carry the signed headers, the session token among them, to its target.
        : base(inner ?? new HttpClientHandler { AllowAutoRedirect = false })
    {
        _credentials = credentials;
        _region = region;
        _service = service;
        _clock = clock;
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        await SignAsync(request, ct);
        return await base.SendAsync(request, ct);
    }

    public async Task SignAsync(HttpRequestMessage request, CancellationToken ct = default)
    {
        var creds = await _credentials(ct);
        var body = request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(ct);
        var now = _clock();
        var amzDate = now.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);

        if (request.Headers.Authorization is { } existing)
        {
            request.Headers.Remove("Authorization");
            request.Headers.TryAddWithoutValidation("Original-Authorization", existing.ToString());
        }
        foreach (var h in new[] { "X-Amz-Date", "X-Amz-Security-Token", "X-Amz-Content-SHA256" }) request.Headers.Remove(h);
        request.Headers.TryAddWithoutValidation("X-Amz-Date", amzDate);
        var payloadHash = Hex(SHA256.HashData(body));
        request.Headers.TryAddWithoutValidation("X-Amz-Content-SHA256", payloadHash);
        if (!string.IsNullOrEmpty(creds.Token)) request.Headers.TryAddWithoutValidation("X-Amz-Security-Token", creds.Token);

        var (canonical, signedHeaders) = CanonicalRequest(request, payloadHash);
        request.Headers.TryAddWithoutValidation("Authorization", Authorization(canonical, signedHeaders, now, _region, _service, creds.AccessKey, creds.SecretKey));
    }

    /// <summary>The <c>Authorization</c> header value for a canonical request (string to sign, derived key, signature).</summary>
    public static string Authorization(string canonical, string signedHeaders, DateTime now, string region, string service, string accessKey, string secretKey)
    {
        var amzDate = now.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var date = amzDate[..8];
        var scope = $"{date}/{region}/{service}/aws4_request";
        var toSign = $"AWS4-HMAC-SHA256\n{amzDate}\n{scope}\n{Hex(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)))}";
        var key = Hmac(Hmac(Hmac(Hmac(Encoding.UTF8.GetBytes("AWS4" + secretKey), date), region), service), "aws4_request");
        return $"AWS4-HMAC-SHA256 Credential={accessKey}/{scope}, SignedHeaders={signedHeaders}, Signature={Hex(Hmac(key, toSign))}";
    }

    /// <summary>The SigV4 canonical request (exposed for the test-suite vectors).</summary>
    public static (string Canonical, string SignedHeaders) CanonicalRequest(HttpRequestMessage request, string payloadHash)
    {
        var uri = request.RequestUri ?? throw new ArgumentException("Request has no URI");
        var all = request.Headers.Concat(request.Content?.Headers ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>());
        return CanonicalRequest(request.Method.Method, uri, all, payloadHash, s3: false);
    }

    /// <summary>
    /// The SigV4 canonical request of a method, URI and headers. <paramref name="s3"/> selects S3's rule for the path:
    /// each segment URI-encoded once, where every other service encodes it twice. <c>host</c> comes from the URI;
    /// <c>authorization</c>, <c>user-agent</c> and <c>content-length</c> are never signed.
    /// </summary>
    public static (string Canonical, string SignedHeaders) CanonicalRequest(string method, Uri uri, IEnumerable<KeyValuePair<string, IEnumerable<string>>> requestHeaders,
        string payloadHash, bool s3)
    {
        var headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["host"] = uri.IsDefaultPort ? uri.Host : $"{uri.Host}:{uri.Port}",
        };
        foreach (var (name, values) in requestHeaders)
        {
            var n = name.ToLowerInvariant();
            if (n is "authorization" or "original-authorization" or "user-agent" or "content-length" or "host") continue;
            var v = string.Join(",", values.Select(x => string.Join(' ', x.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))));
            headers[n] = headers.TryGetValue(n, out var prev) ? $"{prev},{v}" : v;
        }

        var signed = string.Join(';', headers.Keys);
        var canonical = string.Join('\n',
            method.ToUpperInvariant(),
            CanonicalPath(uri.AbsolutePath, s3),
            CanonicalQuery(uri.Query),
            string.Concat(headers.Select(h => $"{h.Key}:{h.Value}\n")),
            signed,
            payloadHash);
        return (canonical, signed);
    }

    /// <summary>
    /// The headers that sign an S3 request (what a remote signer returns): <c>x-amz-date</c>,
    /// <c>x-amz-content-sha256</c> (<paramref name="payloadHash"/>, e.g. <c>UNSIGNED-PAYLOAD</c>), the session token when
    /// there is one, and <c>Authorization</c>, over <paramref name="headers"/> plus those.
    /// </summary>
    public static IReadOnlyDictionary<string, string> SignS3(string method, Uri uri, IEnumerable<KeyValuePair<string, IEnumerable<string>>> headers, string payloadHash,
        ImmutableCredentials credentials, string region, DateTime now)
    {
        var added = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["x-amz-date"] = now.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture),
            ["x-amz-content-sha256"] = payloadHash,
        };
        if (!string.IsNullOrEmpty(credentials.Token)) added["x-amz-security-token"] = credentials.Token;
        var all = headers.Where(h => !added.ContainsKey(h.Key)).Concat(added.Select(a => KeyValuePair.Create(a.Key, (IEnumerable<string>)[a.Value])));
        var (canonical, signed) = CanonicalRequest(method, uri, all, payloadHash, s3: true);
        added["Authorization"] = Authorization(canonical, signed, now, region, "s3", credentials.AccessKey, credentials.SecretKey);
        return added;
    }

    /// <summary>Each path segment URI-encoded once (S3), or once more (all services except S3 double-encode).</summary>
    private static string CanonicalPath(string absolutePath, bool s3) =>
        absolutePath.Length == 0 ? "/" : string.Join('/', absolutePath.Split('/').Select(seg => Encode(Uri.UnescapeDataString(seg)) is var once && !s3 ? Encode(once) : once));

    private static string CanonicalQuery(string query)
    {
        if (query.Length <= 1) return "";
        var pairs = query[1..].Split('&', StringSplitOptions.RemoveEmptyEntries).Select(p =>
        {
            var i = p.IndexOf('=');
            var k = Uri.UnescapeDataString((i < 0 ? p : p[..i]).Replace('+', ' '));
            var v = i < 0 ? "" : Uri.UnescapeDataString(p[(i + 1)..].Replace('+', ' '));
            return (K: Encode(k), V: Encode(v));
        });
        return string.Join('&', pairs.OrderBy(p => p.K, StringComparer.Ordinal).ThenBy(p => p.V, StringComparer.Ordinal).Select(p => $"{p.K}={p.V}"));
    }

    /// <summary>RFC 3986 unreserved characters stay; everything else is %XX (upper-case), UTF-8.</summary>
    private static string Encode(string s)
    {
        var sb = new StringBuilder();
        foreach (var b in Encoding.UTF8.GetBytes(s))
        {
            var c = (char)b;
            if (char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' or '~') sb.Append(c);
            else sb.Append('%').Append(b.ToString("X2", CultureInfo.InvariantCulture));
        }
        return sb.ToString();
    }

    public static string PayloadHash(byte[] body) => Hex(SHA256.HashData(body));

    private static byte[] Hmac(byte[] key, string data) => HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(data));
    private static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();
}
