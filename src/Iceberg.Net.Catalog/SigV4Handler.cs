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
        : base(inner ?? new HttpClientHandler())
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
        var headers = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["host"] = uri.IsDefaultPort ? uri.Host : $"{uri.Host}:{uri.Port}",
        };
        void Add(string name, IEnumerable<string> values)
        {
            var n = name.ToLowerInvariant();
            if (n is "authorization" or "original-authorization" or "user-agent" or "content-length") return;
            var v = string.Join(",", values.Select(x => string.Join(' ', x.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries))));
            headers[n] = headers.TryGetValue(n, out var prev) && n != "host" ? $"{prev},{v}" : v;
        }
        foreach (var h in request.Headers) Add(h.Key, h.Value);
        if (request.Content is not null) foreach (var h in request.Content.Headers) Add(h.Key, h.Value);

        var signed = string.Join(';', headers.Keys);
        var canonical = string.Join('\n',
            request.Method.Method,
            CanonicalPath(uri.AbsolutePath),
            CanonicalQuery(uri.Query),
            string.Concat(headers.Select(h => $"{h.Key}:{h.Value}\n")),
            signed,
            payloadHash);
        return (canonical, signed);
    }

    /// <summary>Each path segment URI-encoded once more (all services except S3 double-encode).</summary>
    private static string CanonicalPath(string absolutePath) =>
        absolutePath.Length == 0 ? "/" : string.Join('/', absolutePath.Split('/').Select(seg => Encode(Uri.UnescapeDataString(seg)) is var once ? Encode(once) : once));

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
