using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace Iceberg.Net.IO;

/// <summary>
/// Storage abstraction (Java <c>FileIO</c>). Iceberg only needs whole-object writes, ranged reads, and
/// deletes — never rename or list — which is what makes it safe on object stores.
/// </summary>
public interface IFileIO : IDisposable
{
    Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default);
    Task<Stream> OpenReadAsync(string location, CancellationToken ct = default);
    Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default);
    Task<long> LengthAsync(string location, CancellationToken ct = default);
    Task<bool> ExistsAsync(string location, CancellationToken ct = default);

    /// <summary>Write an object. When <paramref name="overwrite"/> is false, fail if it exists (metadata files must never be overwritten).</summary>
    Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default);
    Task DeleteAsync(string location, CancellationToken ct = default);
}

/// <summary>A stored object found by listing.</summary>
public sealed record StoredFile(string Location, long Size, DateTime LastModifiedUtc);

/// <summary>FileIO that can list by prefix (Java's <c>SupportsPrefixOperations</c>); needed by orphan-file cleanup.</summary>
public interface ISupportsListing
{
    /// <summary>Every object under <paramref name="prefix"/>, recursively; locations use the same scheme as the prefix.</summary>
    IAsyncEnumerable<StoredFile> ListAsync(string prefix, CancellationToken ct = default);
}

public sealed class FileAlreadyExistsException(string location) : IOException($"File already exists: {location}");

/// <summary>Local filesystem FileIO for <c>file://</c> and bare paths. Used by unit tests and local catalogs.</summary>
public sealed class LocalFileIO : IFileIO, ISupportsListing
{
    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var root = PathOf(prefix);
        if (!Directory.Exists(root)) yield break;
        var scheme = prefix.StartsWith("file:", StringComparison.Ordinal) ? "file://" : "";
        foreach (var path in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
        {
            ct.ThrowIfCancellationRequested();
            var info = new FileInfo(path);
            yield return new StoredFile(scheme + path, info.Length, info.LastWriteTimeUtc);
        }
        await Task.CompletedTask;
    }

    /// <summary>
    /// <c>file:///tmp/x</c> and bare paths, plus Hadoop's <c>file:/tmp/x</c> form, which Java engines write into manifests
    /// (<see cref="Uri"/> rejects it: the empty authority is missing).
    /// </summary>
    private static string PathOf(string location) =>
        StorageScheme.Of(location) switch
        {
            null => location,
            "file" when location.StartsWith("file://", StringComparison.Ordinal) => new Uri(location).LocalPath,
            "file" => Uri.UnescapeDataString(location["file:".Length..]),
            var scheme => throw StorageScheme.Unsupported(scheme, location),
        };

    // A missing parent directory is a missing file: callers handle FileNotFoundException alone, as they do for S3.
    private static FileStream OpenExisting(string location)
    {
        try { return File.OpenRead(PathOf(location)); }
        catch (DirectoryNotFoundException e) { throw new FileNotFoundException($"No such file: {location}", location, e); }
    }

    public async Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default)
    {
        await using var fs = OpenExisting(location);
        var buf = new byte[fs.Length];
        await fs.ReadExactlyAsync(buf, ct);
        return buf;
    }

    public Task<Stream> OpenReadAsync(string location, CancellationToken ct = default) => Task.FromResult<Stream>(OpenExisting(location));

    public async Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default)
    {
        await using var fs = OpenExisting(location);
        fs.Seek(offset, SeekOrigin.Begin);
        var buf = new byte[length];
        await fs.ReadExactlyAsync(buf, ct);
        return buf;
    }

    public Task<long> LengthAsync(string location, CancellationToken ct = default)
    {
        var info = new FileInfo(PathOf(location));
        return info.Exists ? Task.FromResult(info.Length) : throw new FileNotFoundException($"No such file: {location}", location);
    }

    public Task<bool> ExistsAsync(string location, CancellationToken ct = default) => Task.FromResult(File.Exists(PathOf(location)));

    public async Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default)
    {
        var path = PathOf(location);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        try
        {
            await using var fs = new FileStream(path, overwrite ? FileMode.Create : FileMode.CreateNew, FileAccess.Write);
            await fs.WriteAsync(content, ct);
        }
        catch (IOException) when (!overwrite && File.Exists(path))
        {
            throw new FileAlreadyExistsException(location);
        }
    }

    public Task DeleteAsync(string location, CancellationToken ct = default)
    {
        try { File.Delete(PathOf(location)); }
        catch (DirectoryNotFoundException) { /* already gone, as with a missing file */ }
        return Task.CompletedTask;
    }

    public void Dispose() { }
}

/// <summary>Options for <see cref="S3FileIO"/>, named after the Iceberg <c>s3.*</c> catalog properties.</summary>
public sealed record S3FileIOOptions
{
    /// <summary><c>s3.endpoint</c>: override for emulators (moto, MinIO, LocalStack) or VPC endpoints.</summary>
    public string? Endpoint { get; init; }
    /// <summary><c>s3.path-style-access</c>: required by most emulators.</summary>
    public bool PathStyleAccess { get; init; }
    /// <summary><c>client.region</c>.</summary>
    public string Region { get; init; } = "eu-west-1";
    public string? AccessKeyId { get; init; }
    public string? SecretAccessKey { get; init; }
    public string? SessionToken { get; init; }
    /// <summary>
    /// <c>s3.retry.num-retries</c> (Iceberg Java's name; default 5, as there): retries of throttling (<c>503 SlowDown</c>),
    /// 5xx and transient network errors, with the SDK's standard jittered exponential back-off.
    /// </summary>
    public int MaxRetries { get; init; } = 5;
    /// <summary>
    /// The SDK's client-wide retry quota (token bucket). Off by default: under sustained throttling the quota drains and
    /// the SDK then fails requests without retrying, mid-commit, even though each request's retries are already capped
    /// by <see cref="MaxRetries"/>. Found by <c>ThrottlingTests</c>.
    /// </summary>
    public bool RetryQuota { get; init; }
    /// <summary>Test hook: the HTTP handler under the SDK (e.g. to inject throttling responses).</summary>
    public HttpMessageHandler? HttpHandler { get; init; }
    /// <summary>
    /// Remote signing (<c>s3.remote-signing-enabled</c>): the client holds no credentials; each S3 request leaves the SDK
    /// unsigned, with <c>x-amz-content-sha256: UNSIGNED-PAYLOAD</c>, and this signs it in place (setting its headers, and
    /// URI if need be) before it is sent. <c>RestCatalog</c> supplies one that calls the catalog's signer endpoint.
    /// </summary>
    public Func<HttpRequestMessage, CancellationToken, Task>? RequestSigner { get; init; }

    private bool PrintMembers(System.Text.StringBuilder builder)
    {
        builder.Append($"Endpoint = {Endpoint}, PathStyleAccess = {PathStyleAccess}, Region = {Region}, AccessKeyId = {AccessKeyId}, ");
        builder.Append($"SecretAccessKey = {(SecretAccessKey is null ? "null" : "***")}, SessionToken = {(SessionToken is null ? "null" : "***")}, ");
        builder.Append($"MaxRetries = {MaxRetries}, RetryQuota = {RetryQuota}, RequestSigner = {(RequestSigner is null ? "null" : "set")}");
        return true;
    }

    /// <summary>Build from Iceberg catalog properties (e.g. a REST <c>config</c> response or vended credentials).</summary>
    public static S3FileIOOptions FromProperties(IReadOnlyDictionary<string, string> p) => new()
    {
        Endpoint = p.GetValueOrDefault("s3.endpoint"),
        PathStyleAccess = p.GetValueOrDefault("s3.path-style-access") == "true",
        Region = p.GetValueOrDefault("client.region") ?? p.GetValueOrDefault("s3.region") ?? "eu-west-1",
        AccessKeyId = p.GetValueOrDefault("s3.access-key-id"),
        SecretAccessKey = p.GetValueOrDefault("s3.secret-access-key"),
        SessionToken = p.GetValueOrDefault("s3.session-token"),
        MaxRetries = int.TryParse(p.GetValueOrDefault("s3.retry.num-retries"), out var n) ? n : 5,
    };
}

/// <summary>
/// S3 FileIO over AWSSDK.S3. Accepts <c>s3://</c>, <c>s3a://</c> and <c>s3n://</c> locations (all seen in
/// the wild from Hadoop-era writers). Conditional create uses <c>If-None-Match: *</c>, which S3 has
/// supported since 2024 and which closes the "two writers, same metadata file" race.
/// </summary>
public sealed class S3FileIO : IFileIO, ISupportsListing
{
    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var (bucket, key) = Parse(prefix);
        var scheme = prefix[..prefix.IndexOf("://", StringComparison.Ordinal)];
        var request = new ListObjectsV2Request { BucketName = bucket, Prefix = key };
        do
        {
            var page = await _s3.ListObjectsV2Async(request, ct);
            foreach (var o in page.S3Objects ?? [])
                yield return new StoredFile($"{scheme}://{bucket}/{o.Key}", o.Size ?? 0, o.LastModified?.ToUniversalTime() ?? DateTime.MinValue);
            request.ContinuationToken = page.IsTruncated == true ? page.NextContinuationToken : null;
        }
        while (request.ContinuationToken is not null);
    }

    private readonly IAmazonS3 _s3;
    private readonly bool _ownsClient;

    public S3FileIO(IAmazonS3 s3)
    {
        _s3 = s3;
    }

    public S3FileIO(S3FileIOOptions options)
    {
        var config = new AmazonS3Config
        {
            RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(options.Region),
            ForcePathStyle = options.PathStyleAccess,
            RetryMode = Amazon.Runtime.RequestRetryMode.Standard,
            MaxErrorRetry = options.MaxRetries,
            ThrottleRetries = options.RetryQuota,
        };
        if (options.RequestSigner is { } signer)
        {
            config.HttpClientFactory = new HandlerClientFactory(new RemoteSigningHandler(signer, options.HttpHandler ?? new HttpClientHandler()));
            // Checksum trailers would need aws-chunked uploads, which a remote signer cannot sign for an unknown body.
            config.RequestChecksumCalculation = Amazon.Runtime.RequestChecksumCalculation.WHEN_REQUIRED;
            config.ResponseChecksumValidation = Amazon.Runtime.ResponseChecksumValidation.WHEN_REQUIRED;
        }
        else if (options.HttpHandler is { } handler) config.HttpClientFactory = new HandlerClientFactory(handler);
        if (options.Endpoint is not null)
        {
            config.ServiceURL = options.Endpoint;
            config.AuthenticationRegion = options.Region;
        }
        _s3 = options.RequestSigner is not null ? new AmazonS3Client(new AnonymousAWSCredentials(), config)
            : options.AccessKeyId is null
            ? new AmazonS3Client(config)
            : new AmazonS3Client(options.SessionToken is null
                ? new BasicAWSCredentials(options.AccessKeyId, options.SecretAccessKey)
                : new SessionAWSCredentials(options.AccessKeyId, options.SecretAccessKey, options.SessionToken), config);
        _ownsClient = true;
    }

    public IAmazonS3 Client => _s3;

    private sealed class HandlerClientFactory(HttpMessageHandler handler) : Amazon.Runtime.HttpClientFactory
    {
        public override HttpClient CreateHttpClient(Amazon.Runtime.IClientConfig clientConfig) => new(handler, disposeHandler: false);
    }

    /// <summary>Under the SDK (which signs nothing, having anonymous credentials): drop any signing headers, then let the remote signer sign.</summary>
    private sealed class RemoteSigningHandler(Func<HttpRequestMessage, CancellationToken, Task> signer, HttpMessageHandler inner) : DelegatingHandler(inner)
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            foreach (var h in new[] { "Authorization", "X-Amz-Date", "X-Amz-Security-Token", "X-Amz-Content-SHA256" }) request.Headers.Remove(h);
            request.Headers.TryAddWithoutValidation("x-amz-content-sha256", "UNSIGNED-PAYLOAD");
            await signer(request, ct);
            return await base.SendAsync(request, ct);
        }
    }

    public static (string Bucket, string Key) Parse(string location)
    {
        var schemeEnd = location.IndexOf("://", StringComparison.Ordinal);
        if (schemeEnd < 0) throw new ArgumentException($"Not an S3 location: {location}");
        var scheme = location[..schemeEnd];
        if (scheme is not ("s3" or "s3a" or "s3n")) throw new ArgumentException($"Unsupported scheme {scheme}");
        var rest = location[(schemeEnd + 3)..];
        var slash = rest.IndexOf('/');
        if (slash <= 0) throw new ArgumentException($"S3 location has no key: {location}");
        return (rest[..slash], rest[(slash + 1)..]);
    }

    // A missing object surfaces as FileNotFoundException, as it does for LocalFileIO, so callers handle one exception
    // type whatever the store. The S3 error stays available as the inner exception.
    private static async Task<T> NotFoundAsFileNotFound<T>(string location, Func<Task<T>> call)
    {
        try
        {
            return await call();
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new FileNotFoundException($"No such object: {location}", location, e);
        }
    }

    public Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default) =>
        NotFoundAsFileNotFound(location, () => ReadAllCoreAsync(location, ct));

    private async Task<byte[]> ReadAllCoreAsync(string location, CancellationToken ct)
    {
        var (b, k) = Parse(location);
        using var resp = await _s3.GetObjectAsync(b, k, ct);
        using var ms = new MemoryStream();
        await resp.ResponseStream.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    public async Task<Stream> OpenReadAsync(string location, CancellationToken ct = default) => new MemoryStream(await ReadAllAsync(location, ct));

    public Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default) =>
        NotFoundAsFileNotFound(location, () => ReadRangeCoreAsync(location, offset, length, ct));

    private async Task<byte[]> ReadRangeCoreAsync(string location, long offset, int length, CancellationToken ct)
    {
        var (b, k) = Parse(location);
        using var resp = await _s3.GetObjectAsync(new GetObjectRequest { BucketName = b, Key = k, ByteRange = new ByteRange(offset, offset + length - 1) }, ct);
        using var ms = new MemoryStream(length);
        await resp.ResponseStream.CopyToAsync(ms, ct);
        // S3 answers a range that runs past the end with the bytes that exist; a short buffer must not pass for the range.
        if (ms.Length != length)
            throw new EndOfStreamException($"Read {ms.Length} of {length} bytes at {offset} from {location}: the object is shorter");
        return ms.ToArray();
    }

    public Task<long> LengthAsync(string location, CancellationToken ct = default) =>
        NotFoundAsFileNotFound(location, async () =>
        {
            var (b, k) = Parse(location);
            var meta = await _s3.GetObjectMetadataAsync(b, k, ct);
            return meta.ContentLength;
        });

    public async Task<bool> ExistsAsync(string location, CancellationToken ct = default)
    {
        try
        {
            await LengthAsync(location, ct);
            return true;
        }
        catch (FileNotFoundException)
        {
            return false;
        }
    }

    public async Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default)
    {
        var (b, k) = Parse(location);
        var req = new PutObjectRequest { BucketName = b, Key = k, InputStream = new MemoryStream(content.ToArray()) };
        if (!overwrite) req.IfNoneMatch = "*";
        try
        {
            await _s3.PutObjectAsync(req, ct);
        }
        catch (AmazonS3Exception e) when (!overwrite && (int)e.StatusCode is 412 or 409)
        {
            // A retried PUT whose first attempt landed (response lost, e.g. 503 after apply) fails its own
            // If-None-Match. Locations are unique per write, so identical bytes mean the object is ours.
            if (await ReadAllAsync(location, ct) is var existing && existing.AsSpan().SequenceEqual(content.Span)) return;
            throw new FileAlreadyExistsException(location);
        }
    }

    public async Task DeleteAsync(string location, CancellationToken ct = default)
    {
        var (b, k) = Parse(location);
        await _s3.DeleteObjectAsync(b, k, ct);
    }

    public void Dispose()
    {
        if (_ownsClient) _s3.Dispose();
    }
}

/// <summary>Routes by scheme so a table whose files span local and S3 (e.g. fixtures) still resolves.</summary>
public sealed class ResolvingFileIO(Func<IFileIO> s3Factory) : IFileIO, ISupportsListing
{
    public IAsyncEnumerable<StoredFile> ListAsync(string prefix, CancellationToken ct = default) =>
        For(prefix) is ISupportsListing l ? l.ListAsync(prefix, ct) : throw new NotSupportedException("The FileIO for this location cannot list");

    private readonly LocalFileIO _local = new();
    private readonly Lazy<IFileIO> _s3 = new(s3Factory);

    /// <summary>
    /// <c>s3</c>, <c>s3a</c> and <c>s3n</c> go to S3; <c>file:</c> and bare paths to the local filesystem. Any other
    /// scheme (<c>gs</c>, <c>abfss</c>, <c>hdfs</c>, …) is refused: writing such a location to local disk would put a
    /// table's files somewhere no other engine looks, and reading it would read the local filesystem.
    /// </summary>
    private IFileIO For(string location) => StorageScheme.Of(location) switch
    {
        null or "file" => _local,
        "s3" or "s3a" or "s3n" => _s3.Value,
        var scheme => throw StorageScheme.Unsupported(scheme, location),
    };

    public Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default) => For(l).ReadAllAsync(l, ct);
    public Task<Stream> OpenReadAsync(string l, CancellationToken ct = default) => For(l).OpenReadAsync(l, ct);
    public Task<byte[]> ReadRangeAsync(string l, long o, int n, CancellationToken ct = default) => For(l).ReadRangeAsync(l, o, n, ct);
    public Task<long> LengthAsync(string l, CancellationToken ct = default) => For(l).LengthAsync(l, ct);
    public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => For(l).ExistsAsync(l, ct);
    public Task WriteAsync(string l, ReadOnlyMemory<byte> c, bool overwrite = false, CancellationToken ct = default) => For(l).WriteAsync(l, c, overwrite, ct);
    public Task DeleteAsync(string l, CancellationToken ct = default) => For(l).DeleteAsync(l, ct);

    public void Dispose()
    {
        if (_s3.IsValueCreated) _s3.Value.Dispose();
    }
}

/// <summary>The URI scheme of a storage location, as the FileIOs route on it.</summary>
public static class StorageScheme
{
    /// <summary>
    /// The scheme in lower case (<c>s3</c>, <c>file</c>, <c>gs</c>, …), or null for a bare path. A single letter before
    /// the colon is a Windows drive (<c>C:\x</c>, <c>C:/x</c>), not a scheme.
    /// </summary>
    public static string? Of(string location)
    {
        var colon = location.IndexOf(':');
        if (colon < 2) return null;
        if (!char.IsAsciiLetter(location[0])) return null;
        for (var i = 1; i < colon; i++)
            if (!(char.IsAsciiLetterOrDigit(location[i]) || location[i] is '+' or '-' or '.')) return null;
        return location[..colon].ToLowerInvariant();
    }

    internal static NotSupportedException Unsupported(string scheme, string location) =>
        new($"No FileIO for the '{scheme}' scheme ({location}); supported: s3, s3a, s3n, file and local paths");
}
