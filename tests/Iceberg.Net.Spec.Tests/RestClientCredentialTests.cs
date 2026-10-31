using System.Net;
using System.Text;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Where the REST client is willing to send its catalog token, and the AWS credentials it signs with.</summary>
public sealed class RestClientCredentialTests
{
    /// <summary>A catalog server that answers <c>GET v1/config</c> with <paramref name="overrides"/>.</summary>
    private sealed class ConfigServer(string overrides) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("{\"defaults\":{},\"overrides\":{" + overrides + "}}", Encoding.UTF8, "application/json"),
            });
    }

    private static Task<RestCatalog> Connect(string uri, string overrides = "", bool allowEndpoint = false, Dictionary<string, string>? properties = null,
        string? token = "t", bool allowInsecure = false) =>
        RestCatalog.ConnectAsync(new RestCatalogOptions
        {
            Uri = new Uri(uri), Token = token, Handler = new ConfigServer(overrides), AccessDelegation = null,
            AllowServerStorageEndpoint = allowEndpoint, AllowInsecureCredentials = allowInsecure,
            Properties = properties ?? new Dictionary<string, string>(),
        });

    [Theory]
    [InlineData("https://catalog.example/", true)]
    [InlineData("http://localhost:8181/", true)]
    [InlineData("http://127.0.0.1:8181/", true)]
    [InlineData("http://catalog.example/", false)]
    public async Task A_token_is_only_sent_over_https_or_to_loopback(string uri, bool allowed)
    {
        if (allowed) (await Connect(uri)).Dispose();
        else await Assert.ThrowsAsync<InvalidOperationException>(() => Connect(uri));
        (await Connect(uri, allowInsecure: true)).Dispose();
        (await Connect(uri, token: null)).Dispose(); // nothing to protect
    }

    [Theory]
    [InlineData("http://attacker.example:9000", false)]
    [InlineData("https://s3.eu-west-1.amazonaws.com", true)]
    [InlineData("http://localhost:5000", true)]
    // Genuine S3 service endpoints.
    [InlineData("https://s3.amazonaws.com", true)]
    [InlineData("https://s3-eu-west-1.amazonaws.com", true)]
    [InlineData("https://s3.dualstack.us-east-1.amazonaws.com", true)]
    [InlineData("https://s3-fips.us-gov-west-1.amazonaws.com", true)]
    [InlineData("https://s3-fips.dualstack.us-east-1.amazonaws.com", true)]
    [InlineData("https://s3-external-1.amazonaws.com", true)]
    [InlineData("https://s3.cn-north-1.amazonaws.com.cn", true)]
    [InlineData("https://s3express-control.us-east-1.amazonaws.com", true)]
    [InlineData("https://s3express-use1-az4.us-east-1.amazonaws.com", true)]
    // Other AWS services and hosts anyone can name: API Gateway, load balancers, STS, buckets and access points.
    [InlineData("https://a1b2c3d4e5.execute-api.us-east-1.amazonaws.com", false)]
    [InlineData("https://my-lb-1234567890.us-east-1.elb.amazonaws.com", false)]
    [InlineData("https://s3-1234567890.us-east-1.elb.amazonaws.com", false)]
    [InlineData("https://sts.us-east-1.amazonaws.com", false)]
    [InlineData("https://attacker.s3.amazonaws.com", false)]
    [InlineData("https://attacker.s3.eu-west-1.amazonaws.com", false)]
    [InlineData("https://ap-111122223333.s3-accesspoint.us-east-1.amazonaws.com", false)]
    [InlineData("https://s3.amazonaws.com.attacker.example", false)]
    // S3 itself, but in clear, through a user name, or with a path that would prefix every key (a bucket of the server's choosing).
    [InlineData("http://s3.eu-west-1.amazonaws.com", false)]
    [InlineData("https://user@s3.eu-west-1.amazonaws.com", false)]
    [InlineData("https://s3.eu-west-1.amazonaws.com/attacker-bucket", false)]
    public async Task A_server_chosen_s3_endpoint_never_receives_the_clients_own_signatures(string endpoint, bool allowed)
    {
        var overrides = $"\"s3.endpoint\":\"{endpoint}\"";
        if (allowed) (await Connect("https://catalog.example/", overrides)).Dispose();
        else await Assert.ThrowsAsync<InvalidOperationException>(() => Connect("https://catalog.example/", overrides));

        (await Connect("https://catalog.example/", overrides, allowEndpoint: true)).Dispose();
        (await Connect("https://catalog.example/", overrides, properties: new() { ["s3.endpoint"] = endpoint })).Dispose(); // the client's own choice
        (await Connect("https://catalog.example/", overrides + ",\"s3.access-key-id\":\"vended\"")).Dispose(); // the server's credentials
    }

    /// <summary>A catalog server that answers config with <c>prefix</c> and records the paths of the other requests.</summary>
    private sealed class PrefixServer(string prefix) : HttpMessageHandler
    {
        public List<string> Paths { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var config = request.RequestUri!.AbsolutePath.EndsWith("/v1/config", StringComparison.Ordinal);
            if (!config) Paths.Add(request.RequestUri.AbsolutePath);
            var body = config
                ? "{\"defaults\":{},\"overrides\":{\"prefix\":" + System.Text.Json.JsonSerializer.Serialize(prefix) + "}}"
                : "{\"namespaces\":[]}";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }

    [Theory]
    [InlineData("warehouse", "/catalog/v1/warehouse/namespaces")]
    [InlineData("/catalogs/main/", "/catalog/v1/catalogs/main/namespaces")]
    // S3 Tables' prefix is its URL-encoded table bucket ARN.
    [InlineData("arn%3Aaws%3As3tables%3Aus-east-1%3A111122223333%3Abucket%2Fmy-bucket",
        "/catalog/v1/arn%3Aaws%3As3tables%3Aus-east-1%3A111122223333%3Abucket%2Fmy-bucket/namespaces")]
    public async Task A_server_prefix_is_a_path_below_v1(string prefix, string path)
    {
        var server = new PrefixServer(prefix);
        using var catalog = await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = new Uri("https://host.example/catalog/"), Token = "t", Handler = server });
        await catalog.ListNamespacesAsync();
        Assert.Equal([path], server.Paths);
    }

    [Theory]
    [InlineData("../admin")]
    [InlineData("a/../../admin")]
    [InlineData("..")]
    [InlineData("%2e%2e/admin")]
    [InlineData("a/%2E%2E/%2e%2e/admin")]
    [InlineData("a%2F..%2F..%2Fadmin")]
    [InlineData("./x")]
    [InlineData("a//b")]
    [InlineData("x?redirect=1")]
    [InlineData("x#fragment")]
    [InlineData("a\\..\\b")]
    [InlineData("a b")]
    public async Task A_server_prefix_that_reshapes_the_path_is_refused(string prefix)
    {
        var server = new PrefixServer(prefix);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = new Uri("https://host.example/catalog/"), Token = "t", Handler = server }));
        Assert.Empty(server.Paths);
    }

    /// <summary>Two loopback HTTP servers: the catalog, which redirects everything, and the target, which records what reaches it.</summary>
    private sealed class RedirectingCatalog : IDisposable
    {
        private readonly HttpListener _catalog = new();
        private readonly HttpListener _target = new();
        public List<(string Path, string? Authorization, string Body)> Received { get; } = [];
        public Uri Uri { get; }

        public RedirectingCatalog(HttpStatusCode redirect)
        {
            var (catalogPort, targetPort) = (FreePort(), FreePort());
            Uri = new Uri($"http://127.0.0.1:{catalogPort}/");
            _catalog.Prefixes.Add(Uri.ToString());
            _target.Prefixes.Add($"http://127.0.0.1:{targetPort}/");
            _catalog.Start();
            _target.Start();
            _ = Task.Run(async () =>
            {
                while (_catalog.IsListening)
                {
                    HttpListenerContext ctx;
                    try { ctx = await _catalog.GetContextAsync(); } catch (Exception) { return; }
                    ctx.Response.StatusCode = (int)redirect;
                    ctx.Response.RedirectLocation = $"http://127.0.0.1:{targetPort}/stolen{ctx.Request.Url!.AbsolutePath}";
                    ctx.Response.Close();
                }
            });
            _ = Task.Run(async () =>
            {
                while (_target.IsListening)
                {
                    HttpListenerContext ctx;
                    try { ctx = await _target.GetContextAsync(); } catch (Exception) { return; }
                    using (var reader = new StreamReader(ctx.Request.InputStream)) lock (Received) Received.Add((ctx.Request.Url!.AbsolutePath, ctx.Request.Headers["Authorization"], reader.ReadToEnd()));
                    var body = Encoding.UTF8.GetBytes("{\"access_token\":\"x\",\"defaults\":{},\"overrides\":{}}");
                    ctx.Response.ContentType = "application/json";
                    ctx.Response.OutputStream.Write(body);
                    ctx.Response.Close();
                }
            });
        }

        private static int FreePort()
        {
            var l = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
            l.Start();
            var port = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return port;
        }

        public void Dispose()
        {
            _catalog.Close();
            _target.Close();
        }
    }

    [Theory]
    [InlineData(HttpStatusCode.TemporaryRedirect)]
    [InlineData(HttpStatusCode.PermanentRedirect)]
    [InlineData(HttpStatusCode.Found)]
    public async Task A_redirect_never_carries_the_client_secret_or_the_token_elsewhere(HttpStatusCode redirect)
    {
        using var server = new RedirectingCatalog(redirect);
        // The token request: its body is the client secret.
        await Assert.ThrowsAnyAsync<Exception>(() => RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = server.Uri, Credential = "client:the-client-secret", AccessDelegation = null }));
        // A request with the bearer token.
        await Assert.ThrowsAnyAsync<Exception>(() => RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = server.Uri, Token = "the-bearer-token", AccessDelegation = null }));
        lock (server.Received) Assert.Empty(server.Received);
    }

    [Fact]
    public async Task With_vended_credentials_requested_the_endpoint_check_waits_for_the_table()
    {
        // Config names a non-AWS endpoint; the client asked for vended credentials, so connecting is fine.
        using var catalog = await RestCatalog.ConnectAsync(new RestCatalogOptions
        {
            Uri = new Uri("https://catalog.example/"), Token = "t", Handler = new ConfigServer("\"s3.endpoint\":\"http://minio.example:9000\""),
            AccessDelegation = "vended-credentials",
        });
        Assert.Equal("http://minio.example:9000", catalog.Properties["s3.endpoint"]);
    }

    [Fact]
    public async Task The_signer_must_be_the_catalogs_own_origin()
    {
        using var catalog = await Connect("https://catalog.example/");
        var id = TableIdentifier.Of("db", "t");
        catalog.RemoteSigner(id, new Dictionary<string, string>());
        catalog.RemoteSigner(id, new Dictionary<string, string> { ["signer.uri"] = "https://catalog.example/", ["signer.endpoint"] = "v1/sign" });
        Assert.Throws<InvalidOperationException>(() =>
            catalog.RemoteSigner(id, new Dictionary<string, string> { ["signer.uri"] = "https://elsewhere.example/", ["signer.endpoint"] = "v1/sign" }));
    }

    [Fact]
    public void Options_do_not_print_their_secrets()
    {
        var rest = new RestCatalogOptions { Uri = new Uri("https://c.example/"), Token = "tok-secret", Credential = "client:cred-secret" }.ToString();
        Assert.DoesNotContain("tok-secret", rest);
        Assert.DoesNotContain("cred-secret", rest);
        Assert.Contains("client:***", rest);
        var s3 = new S3FileIOOptions { AccessKeyId = "AKIDEXAMPLE", SecretAccessKey = "s3-secret", SessionToken = "session-secret" }.ToString();
        Assert.DoesNotContain("s3-secret", s3);
        Assert.DoesNotContain("session-secret", s3);
        Assert.Contains("AKIDEXAMPLE", s3);
    }
}
