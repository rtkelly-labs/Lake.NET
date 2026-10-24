using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// GCS vending (<see cref="GcsDownscopedCredentialVendor"/>): the token exchange sent to Google's STS has the documented
/// shape (form fields, the Credential Access Boundary JSON, the CEL conditions, the role per access level), responses
/// are parsed and cached, the server returns the tokens under Iceberg's <c>gcs.oauth2.*</c> keys next to the other
/// vendors, and the built-in source tokens (metadata server, service account key) are requested correctly. No real
/// Google endpoint is involved: the STS here is scripted.
/// </summary>
[Spec("rest: loadCredentials")]
public sealed class GcsCredentialVendingTests
{
    private static readonly DateTimeOffset Now = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private const string SourceToken = "ya29.server-source-token";

    /// <summary>A scripted Google STS: records each token exchange and answers per <see cref="Respond"/>.</summary>
    internal sealed class FakeGoogleSts : HttpMessageHandler
    {
        public List<(HttpRequestMessage Request, string Body, Dictionary<string, string> Form)> Requests { get; } = [];

        /// <summary>The response for a request (its form, its 1-based number); default: a token naming the role, valid an hour.</summary>
        public Func<Dictionary<string, string>, int, (HttpStatusCode, string)> Respond { get; set; } = (form, n) =>
            (HttpStatusCode.OK, new JsonObject
            {
                ["access_token"] = $"ya29.{(Role(form) == GcsDownscopedCredentialVendor.WriteRole ? "write" : "read")}-{n}",
                ["issued_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
                ["token_type"] = "Bearer",
                ["expires_in"] = 3600,
            }.ToJsonString());

        public static string Role(Dictionary<string, string> form) =>
            Rule(form)["availablePermissions"]![0]!.GetValue<string>()["inRole:".Length..];

        public static JsonNode Rule(Dictionary<string, string> form) =>
            JsonNode.Parse(form["options"])!["accessBoundary"]!["accessBoundaryRules"]!.AsArray().Single()!;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var body = await request.Content!.ReadAsStringAsync(ct);
            var form = Form(body);
            Requests.Add((request, body, form));
            var (status, json) = Respond(form, Requests.Count);
            return new HttpResponseMessage(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
        }
    }

    internal static Dictionary<string, string> Form(string body) => body.Split('&').Select(p => p.Split('=', 2))
        .ToDictionary(p => Uri.UnescapeDataString(p[0].Replace('+', ' ')), p => Uri.UnescapeDataString(p[1].Replace('+', ' ')));

    private static GcsDownscopedCredentialVendor Vendor(FakeGoogleSts sts, Func<DateTimeOffset> clock, IGcpAccessTokenSource? source = null) =>
        new(new GcsCredentialVendingOptions(), source ?? new StaticGcpAccessTokenSource(SourceToken, Now.AddHours(2)), sts, clock);

    [Fact]
    public async Task The_token_exchange_has_googles_documented_shape()
    {
        var sts = new FakeGoogleSts();
        using var vendor = Vendor(sts, () => Now);
        var read = Assert.Single(await vendor.VendAsync(TableIdentifier.Of("db", "t"), "gs://lake/wh/db/t", write: false));

        var (request, body, form) = Assert.Single(sts.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("https://sts.googleapis.com/v1/token", request.RequestUri!.ToString());
        Assert.Equal("application/x-www-form-urlencoded", request.Content!.Headers.ContentType!.MediaType);
        Assert.Null(request.Headers.Authorization); // the subject token is the credential, in the body
        Assert.Equal(new Dictionary<string, string>
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:token-exchange",
            ["subject_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
            ["requested_token_type"] = "urn:ietf:params:oauth:token-type:access_token",
            ["subject_token"] = SourceToken,
            ["options"] = """{"accessBoundary":{"accessBoundaryRules":[{"availablePermissions":["inRole:roles/storage.objectViewer"],"availableResource":"//storage.googleapis.com/projects/_/buckets/lake","availabilityCondition":{"expression":"resource.name.startsWith('projects/_/buckets/lake/objects/wh/db/t/') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('wh/db/t/')"}}]}}""",
        }, form);
        // options is the URL-encoded JSON (form encoding: spaces as '+').
        Assert.Contains("options=%7B%22accessBoundary%22%3A%7B%22accessBoundaryRules%22%3A%5B%7B", body);

        Assert.Equal("gs://lake/wh/db/t/", read.Prefix);
        Assert.Equal(new Dictionary<string, string>
        {
            ["gcs.oauth2.token"] = "ya29.read-1",
            ["gcs.oauth2.token-expires-at"] = Now.AddHours(1).ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
        }, read.Config);
        Assert.Equal(Now.AddHours(1), read.Expiration);
        Assert.Equal("""{"prefix":"gs://lake/wh/db/t/","config":{"gcs.oauth2.token":"ya29.read-1","gcs.oauth2.token-expires-at":"1893459600000"}}""",
            read.ToJson().ToJsonString());
    }

    [Theory]
    [InlineData(false, "roles/storage.objectViewer")]
    [InlineData(true, "roles/storage.objectAdmin")]
    public async Task Readers_get_object_viewer_and_writers_object_admin(bool write, string role)
    {
        var sts = new FakeGoogleSts();
        using var vendor = Vendor(sts, () => Now);
        var credential = Assert.Single(await vendor.VendAsync(TableIdentifier.Of("db", "t"), "gs://lake/wh/db/t/", write));
        Assert.Equal(write ? "ya29.write-1" : "ya29.read-1", credential.Config["gcs.oauth2.token"]);
        var rule = FakeGoogleSts.Rule(sts.Requests[0].Form);
        Assert.Equal(new[] { $"inRole:{role}" }, rule["availablePermissions"]!.AsArray().Select(p => p!.GetValue<string>()));
        Assert.Equal("//storage.googleapis.com/projects/_/buckets/lake", rule["availableResource"]!.GetValue<string>());
        // The condition is the same for both: access level is the role, scope is the prefix.
        Assert.Equal(GcsDownscopedCredentialVendor.Condition("lake", "wh/db/t/"), rule["availabilityCondition"]!["expression"]!.GetValue<string>());
    }

    [Theory]
    [InlineData("gs://lake/wh/db/t", "wh/db/t/")]
    [InlineData("gs://lake/wh/db/t/", "wh/db/t/")]
    [InlineData("gs://lake", "")]
    [InlineData("gs://lake/", "")]
    [InlineData("gcs://lake/wh/t", "wh/t/")]
    public async Task The_condition_covers_the_tables_prefix(string location, string objectPrefix)
    {
        var sts = new FakeGoogleSts();
        using var vendor = Vendor(sts, () => Now);
        var credential = Assert.Single(await vendor.VendAsync(TableIdentifier.Of("db", "t"), location, write: true));
        Assert.Equal(location.TrimEnd('/') + "/", credential.Prefix);
        Assert.Equal(
            $"resource.name.startsWith('projects/_/buckets/lake/objects/{objectPrefix}') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('{objectPrefix}')",
            FakeGoogleSts.Rule(sts.Requests[0].Form)["availabilityCondition"]!["expression"]!.GetValue<string>());
    }

    [Fact]
    public async Task Quotes_and_backslashes_in_the_prefix_are_escaped_in_the_cel_strings()
    {
        Assert.Equal(@"'it\'s'", GcsDownscopedCredentialVendor.CelString("it's"));
        Assert.Equal(@"'a\\b'", GcsDownscopedCredentialVendor.CelString(@"a\b"));
        Assert.Equal(@"'x\ny\r\tz\u0001\u007f'", GcsDownscopedCredentialVendor.CelString("x\ny\r\tz\u0001\u007f"));
        Assert.Equal("'tête/ü'", GcsDownscopedCredentialVendor.CelString("tête/ü"));
        // The quote that would otherwise close the string and append "|| true".
        Assert.Equal(@"'a\') || true || (\''", GcsDownscopedCredentialVendor.CelString("a') || true || ('"));

        var sts = new FakeGoogleSts();
        using var vendor = Vendor(sts, () => Now);
        await vendor.VendAsync(TableIdentifier.Of("db", "t"), @"gs://lake/wh/o'brien\x/t", write: false);
        Assert.Equal(
            @"resource.name.startsWith('projects/_/buckets/lake/objects/wh/o\'brien\\x/t/') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('wh/o\'brien\\x/t/')",
            FakeGoogleSts.Rule(sts.Requests[0].Form)["availabilityCondition"]!["expression"]!.GetValue<string>());
    }

    [Fact]
    public async Task Tokens_are_cached_per_location_and_access_until_a_quarter_of_their_life_remains()
    {
        var sts = new FakeGoogleSts();
        var now = Now;
        using var vendor = Vendor(sts, () => now, new StaticGcpAccessTokenSource(SourceToken, Now.AddDays(1)));
        var id = TableIdentifier.Of("db", "t");
        async Task<string> Token(string location, bool write) => (await vendor.VendAsync(id, location, write)).Single().Config["gcs.oauth2.token"];

        Assert.Equal("ya29.read-1", await Token("gs://lake/wh/db/t", false));
        Assert.Equal("ya29.read-1", await Token("gs://lake/wh/db/t/", false)); // the same prefix
        Assert.Equal("ya29.write-2", await Token("gs://lake/wh/db/t", true));
        Assert.Equal("ya29.read-3", await Token("gs://lake/wh/db/u", false));
        Assert.Equal(3, sts.Requests.Count);

        now = Now.AddMinutes(44); // 16 of 60 minutes left
        Assert.Equal("ya29.read-1", await Token("gs://lake/wh/db/t", false));
        now = Now.AddMinutes(46); // 14 left: renewed
        Assert.Equal("ya29.read-4", await Token("gs://lake/wh/db/t", false));
        Assert.Equal(Now.AddMinutes(106).ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
            (await vendor.VendAsync(id, "gs://lake/wh/db/t", false)).Single().Config["gcs.oauth2.token-expires-at"]);
        Assert.Equal(4, sts.Requests.Count);
    }

    [Fact]
    public async Task The_expiry_comes_from_expires_in_else_the_source_token_and_never_outlives_the_source()
    {
        var id = TableIdentifier.Of("db", "t");
        async Task<VendedCredential> Vend(string response, DateTimeOffset? sourceExpiry, FakeGoogleSts? sts = null)
        {
            sts ??= new FakeGoogleSts();
            sts.Respond = (_, _) => (HttpStatusCode.OK, response);
            using var vendor = Vendor(sts, () => Now, new StaticGcpAccessTokenSource(SourceToken, sourceExpiry));
            return (await vendor.VendAsync(id, "gs://lake/t", false)).Single();
        }

        // Google's STS omits expires_in for some source tokens: the downscoped token then expires with its source.
        var noExpiresIn = await Vend("""{"access_token":"ya29.a","token_type":"Bearer"}""", Now.AddMinutes(30));
        Assert.Equal(Now.AddMinutes(30), noExpiresIn.Expiration);
        Assert.Equal(Now.AddMinutes(30).ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture), noExpiresIn.Config["gcs.oauth2.token-expires-at"]);
        Assert.Equal(Now.AddMinutes(20), (await Vend("""{"access_token":"ya29.a","expires_in":1200}""", Now.AddMinutes(30))).Expiration);
        Assert.Equal(Now.AddMinutes(30), (await Vend("""{"access_token":"ya29.a","expires_in":3600}""", Now.AddMinutes(30))).Expiration);
        Assert.Equal(Now.AddMinutes(10), (await Vend("""{"access_token":"ya29.a","expires_in":"600"}""", null)).Expiration);

        // Neither: no expiry key, and nothing cached.
        var sts = new FakeGoogleSts();
        var unknown = await Vend("""{"access_token":"ya29.a"}""", null, sts);
        Assert.Null(unknown.Expiration);
        Assert.Equal(new[] { "gcs.oauth2.token" }, unknown.Config.Keys);
        using var vendor = Vendor(sts, () => Now, new StaticGcpAccessTokenSource(SourceToken));
        await vendor.VendAsync(id, "gs://lake/t", false);
        await vendor.VendAsync(id, "gs://lake/t", false);
        Assert.Equal(3, sts.Requests.Count);
    }

    [Fact]
    public async Task Errors_from_the_sts_are_reported_without_the_source_token()
    {
        var sts = new FakeGoogleSts
        {
            Respond = (_, _) => (HttpStatusCode.BadRequest, """{"error":"invalid_grant","error_description":"The access boundary is invalid."}"""),
        };
        using var vendor = Vendor(sts, () => Now);
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => vendor.VendAsync(TableIdentifier.Of("db", "t"), "gs://lake/t", false));
        Assert.Contains("(400)", e.Message);
        Assert.Contains("The access boundary is invalid.", e.Message);
        Assert.DoesNotContain(SourceToken, e.Message);

        sts.Respond = (_, _) => (HttpStatusCode.OK, """{"token_type":"Bearer"}""");
        await Assert.ThrowsAsync<InvalidDataException>(() => vendor.VendAsync(TableIdentifier.Of("db", "t"), "gs://lake/t", false));
        sts.Respond = (_, _) => (HttpStatusCode.OK, "<html>");
        await Assert.ThrowsAsync<InvalidDataException>(() => vendor.VendAsync(TableIdentifier.Of("db", "t"), "gs://lake/t", false));
    }

    [Fact]
    public async Task Other_schemes_get_nothing_and_no_exchange()
    {
        var sts = new FakeGoogleSts();
        using var vendor = Vendor(sts, () => Now);
        foreach (var location in new[] { "s3://lake/t", "abfss://c@a.dfs.core.windows.net/t", "/local/t", "file:///t", "gs:///t", "gsx://lake/t" })
            Assert.Empty(await vendor.VendAsync(TableIdentifier.Of("db", "t"), location, true));
        Assert.Empty(sts.Requests);
        Assert.Equal(("lake", "a/b"), GcsDownscopedCredentialVendor.GcsLocation("gs://lake/a/b"));
        Assert.Equal(("lake", ""), GcsDownscopedCredentialVendor.GcsLocation("gs://lake"));
    }

    [Fact]
    public async Task The_server_combines_gcs_with_the_s3_and_azure_vendors_and_scopes_tokens_to_the_principal()
    {
        var sts = new FakeGoogleSts();
        using var gcs = Vendor(sts, () => DateTimeOffset.UtcNow, new StaticGcpAccessTokenSource(SourceToken, DateTimeOffset.UtcNow.AddHours(2)));
        var settings = new Dictionary<string, string>
        {
            ["CredentialVending:RoleArn"] = "arn:aws:iam::123456789012:role/unused", // S3 vending on: no STS call for gs:// or abfss://
            ["CredentialVending:Adls:Accounts:acct"] = Convert.ToBase64String(new byte[32]),
            ["Auth:Clients:writer"] = "writer-secret",
            ["Auth:Clients:reader"] = "reader-secret",
            ["Auth:SigningKey"] = "a-gcs-vending-test-signing-key-of-32-b",
            ["Authorization:Grants:writer:0"] = "admin:*",
            ["Authorization:Grants:reader:0"] = "read:db",
            ["TableConfig:gcs.project-id"] = "my-project",
        };
        await using var host = ServerHost.Start(settings: settings, services: s => s.AddSingleton<IFileIO>(new MemoryFileIO()).AddSingleton(gcs));
        using var http = host.Http();
        Assert.IsType<CompositeCredentialVendor>(host.Factory.Services.GetRequiredService<IStorageCredentialVendor>());
        var writer = await TokenAsync(http, "writer", "writer-secret");
        var reader = await TokenAsync(http, "reader", "reader-secret");
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces", writer, new JsonObject { ["namespace"] = new JsonArray("db") });
        const string Location = "gs://lake/wh/db/t";
        var (created, createdTable) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", writer,
            new JsonObject { ["name"] = "t", ["location"] = Location, ["schema"] = Json.TypeJson.SchemaToJson(ScanPlanningTests.Schema) }, "vended-credentials");
        Assert.Equal(HttpStatusCode.OK, created);
        Assert.Equal("ya29.write-1", createdTable!["config"]!["gcs.oauth2.token"]!.GetValue<string>());

        foreach (var (token, role) in new[] { (writer, GcsDownscopedCredentialVendor.WriteRole), (reader, GcsDownscopedCredentialVendor.ReadRole) })
        {
            var (status, table) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t", token, delegation: "vended-credentials");
            Assert.Equal(HttpStatusCode.OK, status);
            var credential = table!["storage-credentials"]!.AsArray().Single()!; // only the GCS vendor answers for gs://
            Assert.Equal(Location + "/", credential["prefix"]!.GetValue<string>());
            var config = credential["config"]!.AsObject();
            Assert.Equal(new[] { "gcs.oauth2.token", "gcs.oauth2.token-expires-at" }, config.Select(p => p.Key).Order());
            var vended = config["gcs.oauth2.token"]!.GetValue<string>();
            Assert.StartsWith(role == GcsDownscopedCredentialVendor.WriteRole ? "ya29.write-" : "ya29.read-", vended);
            Assert.True(long.Parse(config["gcs.oauth2.token-expires-at"]!.GetValue<string>(), CultureInfo.InvariantCulture)
                > DateTimeOffset.UtcNow.AddMinutes(50).ToUnixTimeMilliseconds());
            // The same keys in config, next to the static table config.
            Assert.Equal(vended, table["config"]!["gcs.oauth2.token"]!.GetValue<string>());
            Assert.Equal("my-project", table["config"]!["gcs.project-id"]!.GetValue<string>());
            var (credStatus, loaded) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t/credentials", token);
            Assert.Equal(HttpStatusCode.OK, credStatus);
            Assert.Equal(vended, loaded!["storage-credentials"]![0]!["config"]!["gcs.oauth2.token"]!.GetValue<string>());

            // Without the header: no token.
            var (_, plain) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t", token);
            Assert.Null(plain!["storage-credentials"]);
            Assert.Null(plain["config"]!["gcs.oauth2.token"]);
        }
        Assert.All(sts.Requests, r => Assert.Equal(SourceToken, r.Form["subject_token"]));
        Assert.Equal(2, sts.Requests.Count); // the writer's (create, then cached) and the reader's

        // An Azure table on the same server gets a SAS from the Azure vendor and no GCS token.
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", writer, new JsonObject
        {
            ["name"] = "az", ["location"] = "abfss://c@acct.dfs.core.windows.net/wh/db/az", ["schema"] = Json.TypeJson.SchemaToJson(ScanPlanningTests.Schema),
        });
        var (_, azure) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/az", reader, delegation: "vended-credentials");
        var azureCredential = azure!["storage-credentials"]!.AsArray().Single()!;
        Assert.NotNull(azureCredential["config"]!["adls.sas-token.acct.dfs.core.windows.net"]);
        Assert.Null(azureCredential["config"]!["gcs.oauth2.token"]);
        Assert.Equal(2, sts.Requests.Count);
    }

    [Fact]
    public async Task Configuring_a_source_token_turns_gcs_vending_on()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string>
        {
            ["CredentialVending:Gcs:AccessToken"] = SourceToken,
            ["CredentialVending:Gcs:AccessTokenExpiresAt"] = "2030-01-01T00:00:00Z",
        });
        Assert.IsType<GcsDownscopedCredentialVendor>(host.Factory.Services.GetRequiredService<IStorageCredentialVendor>());
        using var http = host.Http();
        var (_, config) = await ProtocolTests.SendAsync(http, HttpMethod.Get, "/v1/config");
        Assert.Contains(Endpoints.Credentials, config!["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()));

        // A registered source does too.
        await using var registered = ServerHost.Start(services: s => s.AddSingleton<IGcpAccessTokenSource>(new StaticGcpAccessTokenSource(SourceToken)));
        Assert.IsType<GcsDownscopedCredentialVendor>(registered.Factory.Services.GetRequiredService<IStorageCredentialVendor>());

        var options = new GcsCredentialVendingOptions { AccessToken = "t", AccessTokenExpiresAt = Now };
        Assert.Equal(new GcpAccessToken("t", Now), await options.CreateTokenSource().GetTokenAsync());
        Assert.IsType<MetadataServerGcpAccessTokenSource>(new GcsCredentialVendingOptions { UseMetadataServer = true }.CreateTokenSource());
        Assert.False(new GcsCredentialVendingOptions().Enabled);
    }

    /// <summary>Answers token requests with the given JSON bodies in turn, recording each request.</summary>
    private sealed class TokenEndpoint(params string[] responses) : HttpMessageHandler
    {
        public List<(HttpRequestMessage Request, string Body)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request, request.Content is null ? "" : await request.Content.ReadAsStringAsync(ct)));
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(responses[Math.Min(Requests.Count, responses.Length) - 1], Encoding.UTF8, "application/json") };
        }
    }

    [Fact]
    public async Task The_metadata_server_source_asks_for_the_default_service_accounts_token_and_caches_it()
    {
        var endpoint = new TokenEndpoint("""{"access_token":"ya29.md-1","expires_in":3599,"token_type":"Bearer"}""",
            """{"access_token":"ya29.md-2","expires_in":3599,"token_type":"Bearer"}""");
        var now = Now;
        using var source = new MetadataServerGcpAccessTokenSource("http://169.254.169.254/", endpoint, () => now);
        Assert.Equal(new GcpAccessToken("ya29.md-1", Now.AddSeconds(3599)), await source.GetTokenAsync());
        var (request, _) = Assert.Single(endpoint.Requests);
        Assert.Equal(HttpMethod.Get, request.Method);
        Assert.Equal("http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token", request.RequestUri!.ToString());
        Assert.Equal(new[] { "Google" }, request.Headers.GetValues("Metadata-Flavor"));

        now = Now.AddMinutes(40);
        Assert.Equal("ya29.md-1", (await source.GetTokenAsync()).Token);
        now = Now.AddMinutes(46);
        Assert.Equal("ya29.md-2", (await source.GetTokenAsync()).Token);
        Assert.Equal(2, endpoint.Requests.Count);
    }

    [Fact]
    public async Task The_service_account_source_posts_a_signed_jwt_assertion()
    {
        using var rsa = RSA.Create(2048);
        var key = new JsonObject
        {
            ["type"] = "service_account",
            ["client_email"] = "iceberg@my-project.iam.gserviceaccount.com",
            ["private_key_id"] = "kid-1",
            ["private_key"] = rsa.ExportPkcs8PrivateKeyPem(),
            ["token_uri"] = "https://oauth2.example/token",
        }.ToJsonString();
        var endpoint = new TokenEndpoint("""{"access_token":"ya29.sa","expires_in":3599,"token_type":"Bearer"}""");
        using var source = new ServiceAccountGcpAccessTokenSource(key, endpoint, () => Now);
        Assert.Equal(new GcpAccessToken("ya29.sa", Now.AddSeconds(3599)), await source.GetTokenAsync());
        Assert.Equal("ya29.sa", (await source.GetTokenAsync()).Token);
        var (request, body) = Assert.Single(endpoint.Requests);
        Assert.Equal("https://oauth2.example/token", request.RequestUri!.ToString());
        var form = Form(body);
        Assert.Equal("urn:ietf:params:oauth:grant-type:jwt-bearer", form["grant_type"]);

        static byte[] B64(string s) => Convert.FromBase64String(s.Replace('-', '+').Replace('_', '/').PadRight(s.Length + (4 - s.Length % 4) % 4, '='));
        var parts = form["assertion"].Split('.');
        Assert.Equal(3, parts.Length);
        Assert.True(rsa.VerifyData(Encoding.ASCII.GetBytes(parts[0] + "." + parts[1]), B64(parts[2]), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));
        var header = JsonNode.Parse(B64(parts[0]))!;
        Assert.Equal(("RS256", "JWT", "kid-1"), (header["alg"]!.GetValue<string>(), header["typ"]!.GetValue<string>(), header["kid"]!.GetValue<string>()));
        var claims = JsonNode.Parse(B64(parts[1]))!;
        Assert.Equal("iceberg@my-project.iam.gserviceaccount.com", claims["iss"]!.GetValue<string>());
        Assert.Equal("https://www.googleapis.com/auth/cloud-platform", claims["scope"]!.GetValue<string>());
        Assert.Equal("https://oauth2.example/token", claims["aud"]!.GetValue<string>());
        Assert.Equal(Now.ToUnixTimeSeconds(), claims["iat"]!.GetValue<long>());
        Assert.Equal(Now.ToUnixTimeSeconds() + 3600, claims["exp"]!.GetValue<long>());
    }

    internal static async Task<string> TokenAsync(HttpClient http, string id, string secret)
    {
        using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = id, ["client_secret"] = secret,
        }));
        return JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
    }

    internal static async Task<(HttpStatusCode Status, JsonNode? Body)> SendAsync(HttpClient http, HttpMethod method, string path, string token, JsonNode? body = null,
        string? delegation = null)
    {
        using var req = new HttpRequestMessage(method, path);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        if (delegation is not null) req.Headers.TryAddWithoutValidation("X-Iceberg-Access-Delegation", delegation);
        if (body is not null) req.Content = ProtocolTests.JsonBody(body);
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text));
    }
}
