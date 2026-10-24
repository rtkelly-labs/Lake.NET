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
    public async Task A_server_chosen_s3_endpoint_never_receives_the_clients_own_signatures(string endpoint, bool allowed)
    {
        var overrides = $"\"s3.endpoint\":\"{endpoint}\"";
        if (allowed) (await Connect("https://catalog.example/", overrides)).Dispose();
        else await Assert.ThrowsAsync<InvalidOperationException>(() => Connect("https://catalog.example/", overrides));

        (await Connect("https://catalog.example/", overrides, allowEndpoint: true)).Dispose();
        (await Connect("https://catalog.example/", overrides, properties: new() { ["s3.endpoint"] = endpoint })).Dispose(); // the client's own choice
        (await Connect("https://catalog.example/", overrides + ",\"s3.access-key-id\":\"vended\"")).Dispose(); // the server's credentials
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
