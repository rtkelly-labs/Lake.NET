using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json.Nodes;
using Amazon.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>Object storage in memory, for any scheme: the server's FileIO when a test only needs S3-looking locations.</summary>
internal sealed class MemoryFileIO : IFileIO
{
    private readonly ConcurrentDictionary<string, byte[]> _files = new(StringComparer.Ordinal);

    public Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default) =>
        _files.TryGetValue(l, out var b) ? Task.FromResult(b) : throw new FileNotFoundException(l);
    public async Task<Stream> OpenReadAsync(string l, CancellationToken ct = default) => new MemoryStream(await ReadAllAsync(l, ct));
    public async Task<byte[]> ReadRangeAsync(string l, long o, int n, CancellationToken ct = default) => (await ReadAllAsync(l, ct)).AsSpan((int)o, n).ToArray();
    public async Task<long> LengthAsync(string l, CancellationToken ct = default) => (await ReadAllAsync(l, ct)).Length;
    public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => Task.FromResult(_files.ContainsKey(l));
    public Task WriteAsync(string l, ReadOnlyMemory<byte> c, bool overwrite = false, CancellationToken ct = default)
    {
        if (!overwrite && _files.ContainsKey(l)) throw new FileAlreadyExistsException(l);
        _files[l] = c.ToArray();
        return Task.CompletedTask;
    }
    public Task DeleteAsync(string l, CancellationToken ct = default) { _files.TryRemove(l, out _); return Task.CompletedTask; }
    public void Dispose() { }
}

/// <summary>
/// S3 remote signing (<c>POST …/tables/{table}/sign</c>): clients that ask for <c>remote-signing</c> are told to sign
/// through the server, which signs only requests for the table's own objects, and only reads for principals without
/// write access. Against moto, our client holds no keys and every S3 request it makes is signed by the server.
/// </summary>
[Spec("rest: signRequest")]
public sealed class RemoteSigningTests
{
    /// <summary>AWS's documented S3 SigV4 example ("GET Object", Signature Version 4 examples in the S3 API reference).</summary>
    [Fact]
    public void S3_signing_matches_the_aws_documented_example()
    {
        var headers = new Dictionary<string, IEnumerable<string>> { ["Range"] = ["bytes=0-9"] };
        var signed = SigV4Handler.SignS3("GET", new Uri("https://examplebucket.s3.amazonaws.com/test.txt"), headers,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            new ImmutableCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", null), "us-east-1",
            new DateTime(2013, 5, 24, 0, 0, 0, DateTimeKind.Utc));
        Assert.Equal("20130524T000000Z", signed["x-amz-date"]);
        Assert.Equal("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
            + "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            signed["Authorization"]);
        Assert.False(signed.ContainsKey("x-amz-security-token"));
    }

    private static readonly Dictionary<string, string> Settings = new()
    {
        ["Warehouse"] = "s3://bucket/wh",
        ["RemoteSigning:Enabled"] = "true",
        ["FileIO:s3.endpoint"] = "http://s3.test:9000",
        ["FileIO:s3.path-style-access"] = "true",
        ["FileIO:client.region"] = "us-east-1",
        ["FileIO:s3.access-key-id"] = "AKSERVER",
        ["FileIO:s3.secret-access-key"] = "server-secret",
        ["Auth:Clients:writer"] = "writer-secret",
        ["Auth:Clients:reader"] = "reader-secret",
        ["Auth:SigningKey"] = "a-remote-signing-test-signing-key-of-32b",
        ["Authorization:Grants:writer:0"] = "admin:*",
        ["Authorization:Grants:reader:0"] = "read:db",
    };

    private static JsonObject Sign(string method, string uri, JsonObject? headers = null, string? body = null)
    {
        var o = new JsonObject
        {
            ["region"] = "us-east-1",
            ["uri"] = uri,
            ["method"] = method,
            ["headers"] = headers ?? new JsonObject { ["x-amz-content-sha256"] = new JsonArray("UNSIGNED-PAYLOAD"), ["amz-sdk-invocation-id"] = new JsonArray("abc") },
        };
        if (body is not null) o["body"] = body;
        return o;
    }

    private static async Task<(HttpStatusCode Status, JsonNode? Body, string? CacheControl)> PostAsync(HttpClient http, string token, string path, JsonNode body,
        string? delegation = null)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, path) { Content = ProtocolTests.JsonBody(body) };
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text), resp.Headers.CacheControl?.ToString());
    }

    private static async Task<string> TokenAsync(HttpClient http, string id, string secret)
    {
        using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = id, ["client_secret"] = secret,
        }));
        return JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
    }

    [Fact]
    public async Task The_signer_signs_only_the_tables_own_objects_at_the_principals_level()
    {
        await using var host = ServerHost.Start(settings: Settings, services: s => s.AddSingleton<IFileIO>(new MemoryFileIO()));
        using var http = host.Http();
        var writer = await TokenAsync(http, "writer", "writer-secret");
        var reader = await TokenAsync(http, "reader", "reader-secret");
        await PostAsync(http, writer, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") });
        await PostAsync(http, writer, "/v1/namespaces/db/tables", new JsonObject { ["name"] = "t", ["schema"] = Json.TypeJson.SchemaToJson(ScanPlanningTests.Schema) });

        // Advertised; offered to clients that ask for remote signing, and only them.
        using (var configRequest = new HttpRequestMessage(HttpMethod.Get, "/v1/config"))
        {
            configRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", reader);
            using var configResponse = await http.SendAsync(configRequest);
            var config = JsonNode.Parse(await configResponse.Content.ReadAsStringAsync())!;
            Assert.Contains(Endpoints.Sign, config["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()));
        }
        var table = await LoadAsync(http, reader, "remote-signing");
        Assert.Equal("true", table["config"]!["s3.remote-signing-enabled"]!.GetValue<string>());
        Assert.Equal("v1/namespaces/db/tables/t/sign", table["config"]!["s3.signer.endpoint"]!.GetValue<string>());
        Assert.Equal("us-east-1", table["config"]!["client.region"]!.GetValue<string>());
        Assert.Null(table["config"]!["s3.access-key-id"]);
        Assert.Null((await LoadAsync(http, reader, "vended-credentials"))["config"]!["s3.remote-signing-enabled"]);
        Assert.Null((await LoadAsync(http, reader, null))["config"]!["s3.remote-signing-enabled"]);

        const string sign = "/v1/namespaces/db/tables/t/sign";
        const string obj = "http://s3.test:9000/bucket/wh/db/t/data/00000-0.parquet";

        // A read inside the table: signed for the server's key, cacheable, the client's headers kept.
        var (status, signed, cache) = await PostAsync(http, reader, sign, Sign("GET", obj));
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("private", cache);
        Assert.Equal(obj, signed!["uri"]!.GetValue<string>());
        var auth = signed["headers"]!["Authorization"]![0]!.GetValue<string>();
        Assert.StartsWith("AWS4-HMAC-SHA256 Credential=AKSERVER/", auth);
        Assert.Contains("/us-east-1/s3/aws4_request", auth);
        Assert.Contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date,", auth); // the SDK's invocation ID is not signed
        Assert.Equal("abc", signed["headers"]!["amz-sdk-invocation-id"]![0]!.GetValue<string>());
        Assert.Equal("UNSIGNED-PAYLOAD", signed["headers"]!["x-amz-content-sha256"]![0]!.GetValue<string>());

        // Virtual-hosted addressing works too; writes need write access and are not cacheable.
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(http, reader, sign, Sign("HEAD", "http://bucket.s3.test:9000/wh/db/t/metadata/v1.json"))).Status);
        var refused = await PostAsync(http, reader, sign, Sign("PUT", obj));
        ProtocolTests.AssertError(HttpStatusCode.Forbidden, "ForbiddenException", (refused.Status, refused.Body));
        var put = await PostAsync(http, writer, sign, Sign("PUT", obj));
        Assert.Equal((HttpStatusCode.OK, "no-cache"), (put.Status, put.CacheControl));

        // Outside the table, the bucket, the endpoint, or through dot segments: refused.
        foreach (var outside in new[]
        {
            "http://s3.test:9000/bucket/wh/db/other/data/x.parquet",
            "http://s3.test:9000/bucket/wh/db/t2/x",
            "http://s3.test:9000/other-bucket/wh/db/t/x",
            "http://elsewhere:9000/bucket/wh/db/t/x",
            "https://bucket.s3.amazonaws.com/wh/db/t/x",
            "http://s3.test:9000/",
            "http://s3.test:9000/bucket/wh/db/t/data/%2E%2E/%2E%2E/other/x",
        })
            Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(http, writer, sign, Sign("GET", outside))).Status);

        // Bucket requests: listing under the table only; DeleteObjects by its body; nothing else.
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(http, reader, sign, Sign("GET", "http://s3.test:9000/bucket?list-type=2&prefix=wh%2Fdb%2Ft%2Fdata%2F"))).Status);
        Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(http, reader, sign, Sign("GET", "http://s3.test:9000/bucket?list-type=2&prefix=wh%2Fdb%2F"))).Status);
        Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(http, reader, sign, Sign("GET", "http://s3.test:9000/bucket?list-type=2"))).Status);
        static string Delete(params string[] keys) =>
            $"<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{string.Concat(keys.Select(k => $"<Object><Key>{k}</Key></Object>"))}</Delete>";
        var deleted = await PostAsync(http, writer, sign, Sign("POST", "http://s3.test:9000/bucket?delete", body: Delete("wh/db/t/data/a", "wh/db/t/data/b")));
        Assert.Equal(HttpStatusCode.OK, deleted.Status);
        Assert.DoesNotContain("UNSIGNED-PAYLOAD", deleted.Body!["headers"]!["x-amz-content-sha256"]![0]!.GetValue<string>()); // the body is signed
        Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(http, writer, sign, Sign("POST", "http://s3.test:9000/bucket?delete", body: Delete("wh/db/t/a", "wh/db/u/b")))).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await PostAsync(http, writer, sign, Sign("POST", "http://s3.test:9000/bucket?delete"))).Status);
        Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(http, writer, sign, Sign("DELETE", "http://s3.test:9000/bucket"))).Status);

        // A copy must come from the table as well.
        var copyFromOutside = new JsonObject { ["x-amz-copy-source"] = new JsonArray("/bucket/wh/db/other/x") };
        Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(http, writer, sign, Sign("PUT", obj, copyFromOutside))).Status);
        var copyFromInside = new JsonObject { ["x-amz-copy-source"] = new JsonArray("bucket/wh/db/t/data/a") };
        Assert.Equal(HttpStatusCode.OK, (await PostAsync(http, writer, sign, Sign("PUT", obj, copyFromInside))).Status);

        // Other providers, regions and tables.
        var gcs = Sign("GET", obj);
        gcs["provider"] = "gs";
        Assert.Equal(HttpStatusCode.BadRequest, (await PostAsync(http, writer, sign, gcs)).Status);
        var region = Sign("GET", obj);
        region["region"] = "eu-west-1";
        Assert.Equal(HttpStatusCode.BadRequest, (await PostAsync(http, writer, sign, region)).Status);
        Assert.Equal(HttpStatusCode.NotFound, (await PostAsync(http, writer, "/v1/namespaces/db/tables/missing/sign", Sign("GET", obj))).Status);
        Assert.Equal(HttpStatusCode.Forbidden, (await PostAsync(http, reader, "/v1/namespaces/other/tables/t/sign", Sign("GET", obj))).Status);
    }

    private static async Task<JsonNode> LoadAsync(HttpClient http, string token, string? delegation)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, "/v1/namespaces/db/tables/t");
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        if (delegation is not null) req.Headers.TryAddWithoutValidation("X-Iceberg-Access-Delegation", delegation);
        using var resp = await http.SendAsync(req);
        return JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
    }

    [Fact]
    public async Task Without_remote_signing_the_route_is_absent()
    {
        await using var host = ServerHost.Start();
        using var http = host.Http();
        var (_, config) = await ProtocolTests.SendAsync(http, HttpMethod.Get, "/v1/config");
        Assert.DoesNotContain(Endpoints.Sign, config!["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()));
        Assert.Equal(HttpStatusCode.NotFound, (await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/t/sign", Sign("GET", "http://x/b/k"))).Status);
    }

    /// <summary>
    /// Against a moto that checks signatures (<see cref="AuthenticatingMoto"/>): the server keeps its warehouse there and
    /// signs; our client asks for <c>remote-signing</c>, has no keys, and creates, appends, deletes, reads (locally and
    /// through server-side planning) and purges a table, every S3 request signed by the server and accepted by moto. A
    /// reader's client can read but not write.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Emulation")]
    public async Task Our_client_reads_and_writes_s3_through_the_signer()
    {
        Skip.If(AuthenticatingMoto.SkipReason is not null, AuthenticatingMoto.SkipReason);
        await using var moto = await AuthenticatingMoto.StartAsync();
        var settings = MotoSettings(moto);
        settings["Auth:Clients:writer"] = "writer-secret";
        settings["Auth:Clients:reader"] = "reader-secret";
        settings["Auth:SigningKey"] = "a-remote-signing-test-signing-key-of-32b";
        settings["Authorization:Grants:writer:0"] = "admin:*";
        settings["Authorization:Grants:reader:0"] = "read:db";
        await using var host = ServerHost.Start(settings: settings);

        var signs = 0;
        RestCatalog Connect(string credential) => RestCatalog.ConnectAsync(new RestCatalogOptions
        {
            Uri = new Uri("http://localhost/"),
            Handler = new CountingHandler(host.Factory.Server.CreateHandler(), r => { if (r.RequestUri!.AbsolutePath.EndsWith("/sign", StringComparison.Ordinal)) Interlocked.Increment(ref signs); }),
            Credential = credential,
            AccessDelegation = "remote-signing",
        }).GetAwaiter().GetResult();

        using var catalog = Connect("writer:writer-secret");
        Assert.DoesNotContain("s3.access-key-id", catalog.Properties.Keys);
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var id = TableIdentifier.Of("db", "signed");
        var t = await catalog.CreateTableAsync(new CreateTableRequest(id, ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        Assert.StartsWith("s3://", t.Location);
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 9))).CommitAsync();
        await TableMutations.DeleteWhereAsync(t, Expressions.Expr.Equal("id", 4L), DeleteStrategy.PositionDeletes);
        Assert.True(signs > 0, "nothing was signed");
        var loaded = await catalog.LoadTableAsync(id);
        Assert.Equal(8, (await TableScan.ReadAllAsync(loaded)).Count);
        Assert.Equal(8, (await ServerSideScan.ReadAsync(catalog, loaded)).Count);

        using var readerCatalog = Connect("reader:reader-secret");
        var forReader = await readerCatalog.LoadTableAsync(id);
        Assert.Equal(8, (await TableScan.ReadAllAsync(forReader)).Count);
        // The signer refuses the reader's PUT (403), which surfaces from the S3 call.
        var denied = await Assert.ThrowsAnyAsync<Exception>(async () => await TableWriter.WriteAsync(forReader, ScanPlanningTests.Rows(100, 2)));
        Assert.Contains("reader may not write", denied.ToString(), StringComparison.Ordinal);

        await catalog.DropTableAsync(id, purge: true);
    }

    /// <summary>Server settings for a warehouse on <paramref name="moto"/>, signed with its user's key; clients get only the endpoint.</summary>
    internal static Dictionary<string, string> MotoSettings(AuthenticatingMoto moto) => new()
    {
        ["Warehouse"] = $"s3://{moto.Bucket}/signing-{Guid.NewGuid():N}",
        ["RemoteSigning:Enabled"] = "true",
        ["FileIO:s3.endpoint"] = moto.Endpoint,
        ["FileIO:s3.path-style-access"] = "true",
        ["FileIO:client.region"] = AuthenticatingMoto.Region,
        ["FileIO:s3.access-key-id"] = moto.AccessKeyId,
        ["FileIO:s3.secret-access-key"] = moto.SecretAccessKey,
        ["TableConfig:s3.endpoint"] = moto.Endpoint,
        ["TableConfig:s3.path-style-access"] = "true",
    };

    private sealed class CountingHandler(HttpMessageHandler inner, Action<HttpRequestMessage> seen) : DelegatingHandler(inner)
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            seen(request);
            return base.SendAsync(request, ct);
        }
    }
}
