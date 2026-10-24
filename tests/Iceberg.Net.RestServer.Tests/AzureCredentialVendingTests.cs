using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Azure SAS vending (<see cref="AdlsSasCredentialVendor"/>): tokens match the Azure SDK's, are returned under the keys
/// Iceberg's ADLS FileIOs read, are read-only for readers, and Azurite (when its image is present) enforces them.
/// </summary>
[Spec("rest: loadCredentials")]
public sealed class AzureCredentialVendingTests
{
    // Azurite's well-known development account.
    private const string Account = "devstoreaccount1";
    private const string Key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private static Dictionary<string, string> Query(string sas) =>
        sas.Split('&').Select(p => p.Split('=', 2)).ToDictionary(p => p[0], p => Uri.UnescapeDataString(p[1]));

    /// <summary>Vectors from the Azure SDK for Python (azure-storage-blob 12.31, <c>BlobSharedAccessSignature</c> pinned to version 2023-11-03).</summary>
    [Theory]
    [InlineData(null, "racwdl", null, "ow4O6ewmJOXy1VdYDCspAmx2gw0TaKg70Qy9eoEGmCI=")]
    [InlineData("wh/db/t", "rl", null, "+xF0PRhoZZNZHCYx/1bURMNfI31rEMoOc3jM1Oj3zkg=")]
    [InlineData(null, "rl", "https", "Yqd6ltEAlSaqFDkjaOdYRINntI9bC7CWHc1qUNzbkRw=")]
    public void Sas_tokens_match_the_azure_sdk(string? directory, string permissions, string? protocol, string signature)
    {
        var sas = Query(AdlsSasCredentialVendor.Sas(Account, Key, "lake", directory, permissions, new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero), "2023-11-03", protocol));
        Assert.Equal(signature, sas["sig"]);
        Assert.Equal(("2023-11-03", permissions, "2030-01-01T00:00:00Z"), (sas["sv"], sas["sp"], sas["se"]));
        Assert.Equal(directory is null ? "c" : "d", sas["sr"]);
        Assert.Equal(directory is null ? null : "3", sas.GetValueOrDefault("sdd"));
        Assert.Equal(protocol, sas.GetValueOrDefault("spr"));
    }

    [Fact]
    public async Task Locations_and_accounts()
    {
        Assert.Equal(("lake", "acct.dfs.core.windows.net", "wh/t"), AdlsSasCredentialVendor.AzureLocation("abfss://lake@acct.dfs.core.windows.net/wh/t"));
        Assert.Equal(("c", "acct.blob.core.windows.net", ""), AdlsSasCredentialVendor.AzureLocation("wasbs://c@acct.blob.core.windows.net"));
        Assert.Null(AdlsSasCredentialVendor.AzureLocation("s3://bucket/wh"));
        Assert.Null(AdlsSasCredentialVendor.AzureLocation("abfss://acct.dfs.core.windows.net/wh"));
        var vendor = new AdlsSasCredentialVendor(new AdlsCredentialVendingOptions { Accounts = { [Account] = Key } });
        Assert.Empty(await vendor.VendAsync(TableIdentifier.Of("db", "t"), "abfss://lake@other.dfs.core.windows.net/wh/t", true));
        Assert.Empty(await vendor.VendAsync(TableIdentifier.Of("db", "t"), "s3://bucket/wh/t", true));
    }

    private const string Location = $"abfss://lake@{Account}.dfs.core.windows.net/wh/db/t";
    private const string TokenKey = $"adls.sas-token.{Account}.dfs.core.windows.net";

    [Fact]
    public async Task The_server_vends_sas_tokens_scoped_to_the_principal()
    {
        var settings = new Dictionary<string, string>
        {
            ["CredentialVending:Adls:Accounts:" + Account] = Key,
            ["CredentialVending:RoleArn"] = "arn:aws:iam::123456789012:role/unused", // S3 vending on too: both vendors are combined
            ["Auth:Clients:writer"] = "writer-secret",
            ["Auth:Clients:reader"] = "reader-secret",
            ["Auth:SigningKey"] = "an-azure-vending-test-signing-key-of-32b",
            ["Authorization:Grants:writer:0"] = "admin:*",
            ["Authorization:Grants:reader:0"] = "read:db",
        };
        await using var host = ServerHost.Start(settings: settings, services: s => s.AddSingleton<IFileIO>(new MemoryFileIO()));
        using var http = host.Http();
        var writer = await TokenAsync(http, "writer", "writer-secret");
        var reader = await TokenAsync(http, "reader", "reader-secret");
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces", writer, new JsonObject { ["namespace"] = new JsonArray("db") });
        var (created, _) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", writer,
            new JsonObject { ["name"] = "t", ["location"] = Location, ["schema"] = Json.TypeJson.SchemaToJson(ScanPlanningTests.Schema) });
        Assert.Equal(HttpStatusCode.OK, created);

        foreach (var (token, permissions) in new[] { (writer, "racwdl"), (reader, "rl") })
        {
            var (_, table) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t", token, delegation: "vended-credentials");
            var credential = table!["storage-credentials"]!.AsArray().Single()!;
            Assert.Equal(Location + "/", credential["prefix"]!.GetValue<string>());
            var sas = Query(credential["config"]![TokenKey]!.GetValue<string>());
            Assert.Equal((permissions, "d", "3"), (sas["sp"], sas["sr"], sas["sdd"]));
            Assert.True(long.Parse(credential["config"]![$"adls.sas-token-expires-at-ms.{Account}.dfs.core.windows.net"]!.GetValue<string>(), CultureInfo.InvariantCulture)
                > DateTimeOffset.UtcNow.AddMinutes(50).ToUnixTimeMilliseconds());
            Assert.Equal(credential["config"]![TokenKey]!.GetValue<string>(), table["config"]![TokenKey]!.GetValue<string>());
            var (status, loaded) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t/credentials", token);
            Assert.Equal(HttpStatusCode.OK, status);
            Assert.Equal(permissions, Query(loaded!["storage-credentials"]![0]!["config"]![TokenKey]!.GetValue<string>())["sp"]);
        }
    }

    /// <summary>
    /// Against Azurite (one-off container): a writer's container SAS uploads and reads a blob under the table; a reader's
    /// reads but cannot write or delete; an expired or tampered token is refused. Azurite has no hierarchical
    /// namespace, so it cannot check the default directory scope; the container scope is what it verifies.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Emulation")]
    public async Task Azurite_enforces_the_vended_tokens()
    {
        Skip.If(Azurite.SkipReason is not null, Azurite.SkipReason);
        await using var azurite = await Azurite.StartAsync();
        await azurite.CreateContainerAsync("lake");
        var options = new AdlsCredentialVendingOptions { Accounts = { [Account] = Key }, Scope = "container" };
        var vendor = new AdlsSasCredentialVendor(options);
        var id = TableIdentifier.Of("db", "t");
        string Sas(VendedCredential c) => c.Config[TokenKey];
        var write = Sas((await vendor.VendAsync(id, Location, write: true)).Single());
        var read = Sas((await vendor.VendAsync(id, Location, write: false)).Single());
        using var http = new HttpClient();
        var blob = $"{azurite.Endpoint}/lake/wh/db/t/data/00000.parquet";

        Assert.Equal(HttpStatusCode.Created, await azurite.PutAsync(http, $"{blob}?{write}", "rows"));
        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync($"{blob}?{write}")).StatusCode);
        using (var got = await http.GetAsync($"{blob}?{read}"))
        {
            Assert.Equal(HttpStatusCode.OK, got.StatusCode);
            Assert.Equal("rows", await got.Content.ReadAsStringAsync());
        }
        Assert.Equal(HttpStatusCode.Forbidden, await azurite.PutAsync(http, $"{blob}?{read}", "overwrite"));
        Assert.Equal(HttpStatusCode.Forbidden, (await http.DeleteAsync($"{blob}?{read}")).StatusCode);

        // Upgrading a reader's token to write permissions breaks its signature.
        var tampered = read.Replace("sp=rl", "sp=racwdl", StringComparison.Ordinal);
        Assert.Equal(HttpStatusCode.Forbidden, await azurite.PutAsync(http, $"{blob}?{tampered}", "x"));

        // An expired token is refused.
        var past = new AdlsSasCredentialVendor(new AdlsCredentialVendingOptions { Accounts = { [Account] = Key }, Scope = "container", DurationSeconds = 60 },
            () => DateTimeOffset.UtcNow.AddHours(-1));
        var expired = Sas((await past.VendAsync(id, Location, write: true)).Single());
        Assert.Equal(HttpStatusCode.Forbidden, (await http.GetAsync($"{blob}?{expired}")).StatusCode);

        Assert.Equal(HttpStatusCode.Accepted, (await http.DeleteAsync($"{blob}?{write}")).StatusCode);
    }

    private static async Task<string> TokenAsync(HttpClient http, string id, string secret)
    {
        using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = id, ["client_secret"] = secret,
        }));
        return JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
    }

    private static async Task<(HttpStatusCode Status, JsonNode? Body)> SendAsync(HttpClient http, HttpMethod method, string path, string token, JsonNode? body = null,
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

    /// <summary>A throwaway Azurite blob service (one-off <c>docker run</c> on a free port).</summary>
    private sealed class Azurite : IAsyncDisposable
    {
        public const string Image = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
        private readonly string _container;
        public string Endpoint { get; }

        private Azurite(string container, string endpoint) { _container = container; Endpoint = endpoint; }

        public static string? SkipReason
        {
            get
            {
                try { return Run("docker", "image", "inspect", Image).GetAwaiter().GetResult().Code == 0 ? null : $"{Image} is not present (docker pull {Image})"; }
                catch (Exception e) when (e is System.ComponentModel.Win32Exception or InvalidOperationException) { return "docker is not available"; }
            }
        }

        public static async Task<Azurite> StartAsync()
        {
            int port;
            using (var l = new TcpListener(IPAddress.Loopback, 0)) { l.Start(); port = ((IPEndPoint)l.LocalEndpoint).Port; }
            var name = $"iceberg-azurite-{Guid.NewGuid():N}"[..30];
            var (code, output) = await Run("docker", "run", "-d", "--rm", "--name", name, "-p", $"127.0.0.1:{port}:10000", Image,
                "azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck");
            if (code != 0) throw new InvalidOperationException($"docker run {Image} failed: {output}");
            var azurite = new Azurite(name, $"http://127.0.0.1:{port}/{Account}");
            using var http = new HttpClient();
            for (var i = 0; ; i++)
            {
                try { using var _ = await http.GetAsync(azurite.Endpoint); break; }
                catch (HttpRequestException) when (i < 100) { await Task.Delay(100); }
            }
            return azurite;
        }

        /// <summary>Create a container with Shared Key authorization (what the vendor's key is for).</summary>
        public async Task CreateContainerAsync(string container)
        {
            using var http = new HttpClient();
            using var req = new HttpRequestMessage(HttpMethod.Put, $"{Endpoint}/{container}?restype=container");
            var date = DateTime.UtcNow.ToString("R", CultureInfo.InvariantCulture);
            req.Headers.TryAddWithoutValidation("x-ms-date", date);
            req.Headers.TryAddWithoutValidation("x-ms-version", "2023-11-03");
            var toSign = $"PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:2023-11-03\n/{Account}/{Account}/{container}\nrestype:container";
            var signature = Convert.ToBase64String(HMACSHA256.HashData(Convert.FromBase64String(Key), Encoding.UTF8.GetBytes(toSign)));
            req.Headers.TryAddWithoutValidation("Authorization", $"SharedKey {Account}:{signature}");
            using var resp = await http.SendAsync(req);
            if (resp.StatusCode != HttpStatusCode.Created) throw new InvalidOperationException($"Create container: {resp.StatusCode} {await resp.Content.ReadAsStringAsync()}");
        }

        public async Task<HttpStatusCode> PutAsync(HttpClient http, string url, string content)
        {
            using var req = new HttpRequestMessage(HttpMethod.Put, url) { Content = new StringContent(content) };
            req.Headers.TryAddWithoutValidation("x-ms-blob-type", "BlockBlob");
            using var resp = await http.SendAsync(req);
            return resp.StatusCode;
        }

        private static async Task<(int Code, string Output)> Run(string file, params string[] args)
        {
            var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (var a in args) psi.ArgumentList.Add(a);
            using var p = Process.Start(psi) ?? throw new InvalidOperationException($"{file} did not start");
            var stdout = p.StandardOutput.ReadToEndAsync();
            var stderr = p.StandardError.ReadToEndAsync();
            await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(2));
            return (p.ExitCode, await stdout + await stderr);
        }

        public async ValueTask DisposeAsync() => await Run("docker", "rm", "-f", _container);
    }
}
