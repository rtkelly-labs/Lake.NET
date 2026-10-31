using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Credential vending: with <c>X-Iceberg-Access-Delegation: vended-credentials</c> a loaded table carries
/// <c>storage-credentials</c> (and the same keys in <c>config</c>) scoped to its location, writable only for principals
/// that may write it; <c>loadCredentials</c> serves the same. The STS vendor is checked against a scripted STS and,
/// when the emulators run, against moto's STS with the vended keys then used for S3.
/// </summary>
[Spec("rest: loadCredentials")]
public sealed class CredentialVendingTests
{
    private sealed class RecordingVendor : IStorageCredentialVendor
    {
        public List<(TableIdentifier Table, string Location, bool Write)> Calls { get; } = [];

        public Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default)
        {
            Calls.Add((table, location, write));
            return Task.FromResult<IReadOnlyList<VendedCredential>>(
            [
                new VendedCredential(location, new Dictionary<string, string> { ["s3.access-key-id"] = write ? "WRITER" : "READER", ["s3.session-token"] = "token" }, null),
            ]);
        }
    }

    private static readonly Dictionary<string, string> Secured = new()
    {
        ["Auth:Clients:writer"] = "writer-secret",
        ["Auth:Clients:reader"] = "reader-secret",
        ["Auth:SigningKey"] = "a-credential-vending-test-signing-key-32b",
        ["Authorization:Grants:writer:0"] = "admin:*",
        ["Authorization:Grants:reader:0"] = "read:db",
        ["TableConfig:s3.endpoint"] = "http://object-store",
    };

    [Fact]
    public async Task Loaded_tables_carry_credentials_scoped_to_the_principal()
    {
        var vendor = new RecordingVendor();
        await using var host = ServerHost.Start(settings: Secured, services: s => s.AddSingleton<IStorageCredentialVendor>(vendor));
        var seen = new List<IReadOnlyDictionary<string, string>>();
        IFileIO Io(IReadOnlyDictionary<string, string> props) { seen.Add(props); return new LocalFileIO(); }

        using var writer = await host.ConnectAsync(credential: "writer:writer-secret", accessDelegation: "vended-credentials", io: Io);
        await writer.CreateNamespaceAsync(Namespace.Of("db"));
        var id = TableIdentifier.Of("db", "t");
        var created = await writer.CreateTableAsync(new CreateTableRequest(id, ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        Assert.Equal("WRITER", seen[^1]["s3.access-key-id"]);
        await SnapshotProducer.Append(created).AddFiles(await TableWriter.WriteAsync(created, ScanPlanningTests.Rows(0, 4))).CommitAsync();

        using var reader = await host.ConnectAsync(credential: "reader:reader-secret", accessDelegation: "vended-credentials", io: Io);
        var loaded = await reader.LoadTableAsync(id);
        Assert.Equal("READER", seen[^1]["s3.access-key-id"]);
        Assert.Equal("http://object-store", seen[^1]["s3.endpoint"]);
        Assert.Equal((id, loaded.Location, false), vendor.Calls[^1]);
        Assert.Equal(4, (await TableScan.ReadAllAsync(loaded)).Count);

        // Without the header nothing is vended.
        var calls = vendor.Calls.Count;
        using var plain = await host.ConnectAsync(credential: "writer:writer-secret", io: Io);
        await plain.LoadTableAsync(id);
        Assert.Equal(calls, vendor.Calls.Count);
        Assert.False(seen[^1].ContainsKey("s3.access-key-id"));

        // The wire shape, and loadCredentials.
        using var http = host.Http();
        var token = await TokenAsync(http, "reader", "reader-secret");
        var (_, config) = await SendAsync(http, HttpMethod.Get, "/v1/config", token);
        Assert.Contains(Endpoints.Credentials, config!["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()));
        var (_, table) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t", token, "vended-credentials,remote-signing");
        var credential = table!["storage-credentials"]!.AsArray().Single()!;
        Assert.Equal(loaded.Location, credential["prefix"]!.GetValue<string>());
        Assert.Equal("READER", credential["config"]!["s3.access-key-id"]!.GetValue<string>());
        Assert.Equal("READER", table["config"]!["s3.access-key-id"]!.GetValue<string>());
        var (status, loadCredentials) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t/credentials", token);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("READER", loadCredentials!["storage-credentials"]![0]!["config"]!["s3.access-key-id"]!.GetValue<string>());
        Assert.Equal(HttpStatusCode.Forbidden, (await SendAsync(http, HttpMethod.Get, "/v1/namespaces/other/tables/t/credentials", token)).Status);
    }

    [Fact]
    public async Task Without_a_vendor_the_header_is_ignored_and_the_route_is_absent()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync(accessDelegation: "vended-credentials");
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), ScanPlanningTests.Schema));
        using var http = host.Http();
        var (_, config) = await ProtocolTests.SendAsync(http, HttpMethod.Get, "/v1/config");
        Assert.DoesNotContain(Endpoints.Credentials, config!["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()));
        Assert.Equal(HttpStatusCode.NotFound, (await ProtocolTests.SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t/credentials")).Status);
        var (_, table) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t", null, "vended-credentials");
        Assert.Null(table!["storage-credentials"]);
    }

    /// <summary>A scripted STS: records the AssumeRole requests and answers with fixed credentials.</summary>
    private sealed class FakeSts : HttpMessageHandler
    {
        public List<(HttpRequestMessage Request, Dictionary<string, string> Form)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var form = (await request.Content!.ReadAsStringAsync(ct)).Split('&').Select(p => p.Split('=', 2))
                .ToDictionary(p => Uri.UnescapeDataString(p[0].Replace('+', ' ')), p => Uri.UnescapeDataString(p[1].Replace('+', ' ')));
            Requests.Add((request, form));
            var xml = $"""
                <AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                  <AssumeRoleResult>
                    <Credentials>
                      <AccessKeyId>ASIAFAKE{Requests.Count}</AccessKeyId>
                      <SecretAccessKey>secret</SecretAccessKey>
                      <SessionToken>session</SessionToken>
                      <Expiration>2030-01-01T00:00:00Z</Expiration>
                    </Credentials>
                  </AssumeRoleResult>
                </AssumeRoleResponse>
                """;
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(xml, Encoding.UTF8, "text/xml") };
        }
    }

    /// <summary>
    /// A table stored before locations were checked for IAM wildcards can still hold one. Loading it works; vending for
    /// it is refused with an explicit 422 that says why, before any vendor is asked, rather than failing inside one.
    /// </summary>
    [Fact]
    public async Task A_stored_location_with_an_iam_wildcard_gets_a_clean_refusal_instead_of_credentials()
    {
        var sts = new FakeSts();
        using var vendor = new StsCredentialVendor(
            new CredentialVendingOptions { RoleArn = "arn:aws:iam::123456789012:role/lake", Region = "eu-west-1", StsEndpoint = "https://sts.example/" },
            new Dictionary<string, string> { ["s3.access-key-id"] = "server", ["s3.secret-access-key"] = "server-secret" }, sts);
        await using var host = ServerHost.Start(settings: Secured, services: s => s.AddSingleton<IStorageCredentialVendor>(vendor));
        using (var writer = await host.ConnectAsync(credential: "writer:writer-secret"))
        {
            await writer.CreateNamespaceAsync(Namespace.Of("db"));
            await writer.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), ScanPlanningTests.Schema));
        }
        // What an older server let through: the table's location now reads s3://bucket/wh/db/t*.
        var id = TableIdentifier.Of("db", "t");
        var store = host.Service<ICatalogStore>();
        var current = (await store.GetAsync(id, CatalogEntryKind.Table))!;
        var metadata = JsonNode.Parse(await File.ReadAllTextAsync(current))!;
        metadata["location"] = "s3://bucket/wh/db/t*";
        var tampered = Path.Combine(Path.GetDirectoryName(current)!, "00009-tampered.metadata.json");
        await File.WriteAllTextAsync(tampered, metadata.ToJsonString());
        await store.SwapAsync([new PointerSwap(id, current, tampered)], CatalogEntryKind.Table);

        using var http = host.Http();
        var token = await TokenAsync(http, "reader", "reader-secret");
        Assert.Equal(HttpStatusCode.OK, (await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t", token)).Status);
        foreach (var (path, delegation) in new[] { ("/v1/namespaces/db/tables/t", "vended-credentials"), ("/v1/namespaces/db/tables/t/credentials", (string?)null) })
        {
            var (status, body) = await SendAsync(http, HttpMethod.Get, path, token, delegation);
            Assert.True(status == HttpStatusCode.UnprocessableEntity, $"{path}: {(int)status} {body}");
            Assert.Equal("UnprocessableEntityException", body!["error"]!["type"]!.GetValue<string>());
            Assert.Contains("wildcard", body["error"]!["message"]!.GetValue<string>(), StringComparison.Ordinal);
        }
        Assert.Empty(sts.Requests);

        // A namespace stored with such a location: a table created in it gets a 400 that says why, not a 500.
        await store.CreateNamespaceAsync(Namespace.Of("wild"), new Dictionary<string, string> { ["location"] = $"{host.Warehouse}/wild*" });
        using var writerHttp = host.Http();
        var writerToken = await TokenAsync(writerHttp, "writer", "writer-secret");
        using var create = new HttpRequestMessage(HttpMethod.Post, "/v1/namespaces/wild/tables")
        {
            Content = new StringContent(new JsonObject { ["name"] = "t", ["schema"] = Json.TypeJson.SchemaToJson(ScanPlanningTests.Schema) }.ToJsonString(), Encoding.UTF8, "application/json"),
        };
        create.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", writerToken);
        create.Headers.Add("X-Iceberg-Access-Delegation", "vended-credentials");
        using var created = await writerHttp.SendAsync(create);
        var createdBody = await created.Content.ReadAsStringAsync();
        Assert.True(created.StatusCode == HttpStatusCode.BadRequest, createdBody);
        Assert.Contains("wildcard", createdBody, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_sts_vendor_assumes_the_role_with_a_policy_narrowed_to_the_table()
    {
        var sts = new FakeSts();
        using var vendor = new StsCredentialVendor(
            new CredentialVendingOptions { RoleArn = "arn:aws:iam::123456789012:role/lake", Region = "eu-west-1", StsEndpoint = "https://sts.example/", ExternalId = "ext" },
            new Dictionary<string, string> { ["s3.access-key-id"] = "server", ["s3.secret-access-key"] = "server-secret" },
            sts, () => new DateTimeOffset(2029, 12, 31, 23, 0, 0, TimeSpan.Zero));
        var id = TableIdentifier.Of("db", "t");

        var read = Assert.Single(await vendor.VendAsync(id, "s3://bucket/wh/db/t", write: false));
        Assert.Equal("s3://bucket/wh/db/t/", read.Prefix);
        Assert.Equal("ASIAFAKE1", read.Config["s3.access-key-id"]);
        Assert.Equal("session", read.Config["s3.session-token"]);
        Assert.Equal(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds().ToString(), read.Config["s3.session-token-expires-at-ms"]);
        var (request, form) = sts.Requests[0];
        Assert.StartsWith("AWS4-HMAC-SHA256 Credential=server/", string.Join(",", request.Headers.GetValues("Authorization")));
        Assert.Contains("/eu-west-1/sts/aws4_request", string.Join(",", request.Headers.GetValues("Authorization")));
        Assert.Equal("AssumeRole", form["Action"]);
        Assert.Equal("arn:aws:iam::123456789012:role/lake", form["RoleArn"]);
        Assert.Equal("ext", form["ExternalId"]);
        Assert.Equal("iceberg-db.t", form["RoleSessionName"]);
        var policy = JsonNode.Parse(form["Policy"])!["Statement"]!.AsArray();
        Assert.Equal("arn:aws:s3:::bucket/wh/db/t/*", policy[0]!["Resource"]!.GetValue<string>());
        Assert.DoesNotContain("s3:PutObject", policy[0]!["Action"]!.AsArray().Select(a => a!.GetValue<string>()));
        Assert.Equal("wh/db/t/*", policy[1]!["Condition"]!["StringLike"]!["s3:prefix"]!.GetValue<string>());

        // Cached while fresh; writers get their own credentials with write actions.
        Assert.Equal("ASIAFAKE1", (await vendor.VendAsync(id, "s3://bucket/wh/db/t", write: false))[0].Config["s3.access-key-id"]);
        Assert.Equal("ASIAFAKE2", (await vendor.VendAsync(id, "s3://bucket/wh/db/t", write: true))[0].Config["s3.access-key-id"]);
        Assert.Contains("s3:PutObject", JsonNode.Parse(sts.Requests[1].Form["Policy"])!["Statement"]![0]!["Action"]!.AsArray().Select(a => a!.GetValue<string>()));
        Assert.Equal(2, sts.Requests.Count);

        // Only S3 locations get credentials.
        Assert.Empty(await vendor.VendAsync(id, "/local/warehouse/db/t", write: true));
        Assert.Equal(2, sts.Requests.Count);
    }

    [Theory]
    [InlineData("bucket", "wh/db/t*")]
    [InlineData("bucket", "wh/*/t")]
    [InlineData("bucket", "wh/db/t?")]
    [InlineData("bucket", "wh/${aws:username}/t")]
    [InlineData("bucket*", "wh/db/t")]
    [InlineData("b?cket", "wh/db/t")]
    public void The_session_policy_refuses_iam_wildcards_and_variables(string bucket, string key)
    {
        Assert.Throws<ArgumentException>(() => StsCredentialVendor.SessionPolicy(bucket, key, write: false));
        Assert.Throws<ArgumentException>(() => StsCredentialVendor.SessionPolicy(bucket, key, write: true));
    }

    /// <summary>
    /// Whatever namespace and table names a client tries, a table the server accepts gets a vended policy whose only
    /// wildcard is the trailing <c>/*</c> on its own prefix: no name can reach another table through IAM pattern matching.
    /// </summary>
    [Fact]
    public async Task Every_accepted_name_vends_a_policy_with_only_the_trailing_wildcard()
    {
        var vendor = new RecordingVendor();
        await using var host = ServerHost.Start(services: s => s.AddSingleton<IStorageCredentialVendor>(vendor));
        using var catalog = await host.ConnectAsync(accessDelegation: "vended-credentials");
        const string alphabet = "ab9_-.*?$[]{}%~=+@ é\u00e9";
        var random = new Random(20261031);
        string Name() => new(Enumerable.Range(0, random.Next(1, 7)).Select(_ => alphabet[random.Next(alphabet.Length)]).ToArray());
        int accepted = 0, refused = 0;
        for (var i = 0; i < 120; i++)
        {
            var ns = Namespace.Of($"n{i}", Name());
            var id = new TableIdentifier(ns, Name());
            try
            {
                await catalog.CreateNamespaceAsync(ns);
                await catalog.CreateTableAsync(new CreateTableRequest(id, ScanPlanningTests.Schema));
            }
            catch (HttpRequestException e) when (e.StatusCode == HttpStatusCode.BadRequest)
            {
                refused++;
                continue;
            }
            accepted++;
            var (_, location, _) = vendor.Calls[^1];
            var key = "wh/" + Path.GetRelativePath(host.Warehouse, location).Replace('\\', '/');
            var statements = JsonNode.Parse(StsCredentialVendor.SessionPolicy("bucket", key, write: true))!["Statement"]!.AsArray();
            foreach (var pattern in new[] { statements[0]!["Resource"]!.GetValue<string>(), statements[1]!["Condition"]!["StringLike"]!["s3:prefix"]!.GetValue<string>() })
            {
                Assert.EndsWith("/*", pattern);
                Assert.Equal(-1, pattern[..^2].IndexOfAny(['*', '?', '$']));
            }
        }
        // Both sides of the property were exercised.
        Assert.True(accepted > 10, $"{accepted} accepted");
        Assert.True(refused > 10, $"{refused} refused");
    }

    /// <summary>
    /// Against the emulators' moto: the server keeps its warehouse on S3 and vends STS credentials; the client brings no
    /// keys of its own, so every S3 call it makes uses the vended ones.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Emulation")]
    public async Task Moto_sts_credentials_are_vended_and_used()
    {
        Require.Target();
        var env = TestEnvironment.Current;
        Skip.If(env.Target != Target.Emulation, "moto STS only in emulation");
        var s3 = new Dictionary<string, string>
        {
            ["s3.endpoint"] = env.S3Endpoint!, ["s3.path-style-access"] = "true", ["client.region"] = env.Region,
        };
        var settings = new Dictionary<string, string>
        {
            ["Warehouse"] = $"{env.Warehouse.TrimEnd('/')}/vending-{Guid.NewGuid():N}",
            ["CredentialVending:RoleArn"] = "arn:aws:iam::123456789012:role/iceberg-vending",
            ["CredentialVending:StsEndpoint"] = env.S3Endpoint!,
            ["CredentialVending:Region"] = env.Region,
            ["FileIO:s3.access-key-id"] = "test",
            ["FileIO:s3.secret-access-key"] = "test",
        };
        foreach (var (k, v) in s3) { settings[$"FileIO:{k}"] = v; settings[$"TableConfig:{k}"] = v; }
        await using var host = ServerHost.Start(settings: settings);
        var seen = new List<IReadOnlyDictionary<string, string>>();
        using var catalog = await host.ConnectAsync(accessDelegation: "vended-credentials", io: props =>
        {
            seen.Add(props);
            return new S3FileIO(S3FileIOOptions.FromProperties(props));
        });
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var id = TableIdentifier.Of("db", "vended");
        var t = await catalog.CreateTableAsync(new CreateTableRequest(id, ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        Assert.StartsWith("s3://", t.Location);
        var vended = seen[^1];
        Assert.StartsWith("ASIA", vended["s3.access-key-id"]);
        Assert.False(string.IsNullOrEmpty(vended["s3.session-token"]));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 9))).CommitAsync();
        var loaded = await catalog.LoadTableAsync(id);
        Assert.StartsWith("ASIA", seen[^1]["s3.access-key-id"]);
        Assert.Equal(9, (await TableScan.ReadAllAsync(loaded)).Count);
        Assert.Equal(9, (await ServerSideScan.ReadAsync(catalog, loaded)).Count);
        await catalog.DropTableAsync(id, purge: true);
    }

    private static async Task<string> TokenAsync(HttpClient http, string id, string secret)
    {
        using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = id, ["client_secret"] = secret,
        }));
        return JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
    }

    private static async Task<(HttpStatusCode Status, JsonNode? Body)> SendAsync(HttpClient http, HttpMethod method, string path, string? token, string? delegation = null)
    {
        using var req = new HttpRequestMessage(method, path);
        if (token is not null) req.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
        if (delegation is not null) req.Headers.TryAddWithoutValidation("X-Iceberg-Access-Delegation", delegation);
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text));
    }
}
