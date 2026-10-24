using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Other engines' GCS FileIOs use the tokens the server vends: PyIceberg 0.12 (PyArrowFileIO) and Iceberg Java 1.11
/// (RESTCatalog's default ResolvingFileIO, delegating to GCSFileIO) load a <c>gs://</c> table with vended credentials and then talk to a recording
/// stand-in for the GCS JSON API (<c>gcs.service.host</c>), which shows the bearer token each request carries. The
/// token exchange itself goes to a scripted STS, and nothing checks the token's permissions: this verifies key names,
/// prefix matching and the expiry format, not Google's enforcement.
/// </summary>
[Trait("Category", "CrossEngine")]
public sealed class GcsCrossEngineTests
{
    private const string Credential = "probe:probe-secret";
    private const string JavaImage = "apache/gravitino-iceberg-rest:1.3.1"; // has iceberg-core, iceberg-gcp 1.11.0 and the GCS client
    private const string JavaLibs = "/opt/gravitino-iceberg-rest-server/libs/*";

    private static string? Python => Environment.GetEnvironmentVariable("ICEBERG_PYTHON") is { Length: > 0 } p && File.Exists(p) ? p : null;

    /// <summary>The server on a port, vending through a scripted STS, with a writer-created <c>gs://lake/wh/db/t</c> table.</summary>
    private static async Task<(KestrelHost Host, GcsCredentialVendingTests.FakeGoogleSts Sts)> StartAsync()
    {
        var sts = new GcsCredentialVendingTests.FakeGoogleSts();
        var vendor = new GcsDownscopedCredentialVendor(new GcsCredentialVendingOptions(), new StaticGcpAccessTokenSource("ya29.source", DateTimeOffset.UtcNow.AddHours(2)), sts);
        var host = await KestrelHost.StartAsync(o =>
        {
            o.Auth.Clients["probe"] = "probe-secret";
            o.Auth.Clients["writer"] = "writer-secret";
            o.Auth.SigningKey = "gcs-cross-engine-signing-key-of-32-bytes";
            o.Authorization.Grants["writer"] = ["admin:*"];
            o.Authorization.Grants["probe"] = ["read:db"];
        }, services: s => s.AddSingleton<IFileIO>(new MemoryFileIO()).AddSingleton(vendor));
        using var http = new HttpClient { BaseAddress = host.Uri };
        var writer = await GcsCredentialVendingTests.TokenAsync(http, "writer", "writer-secret");
        await GcsCredentialVendingTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces", writer, new JsonObject { ["namespace"] = new JsonArray("db") });
        var (status, _) = await GcsCredentialVendingTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", writer,
            new JsonObject { ["name"] = "t", ["location"] = "gs://lake/wh/db/t", ["schema"] = Json.TypeJson.SchemaToJson(ScanPlanningTests.Schema) });
        Assert.Equal(HttpStatusCode.OK, status);
        return (host, sts);
    }

    [SkippableFact]
    public async Task PyIceberg_sends_the_vended_token_to_gcs()
    {
        Skip.If(Python is null, "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)");
        var (host, sts) = await StartAsync();
        await using var _ = host;
        using var gcs = RecordingGcs.Start("127.0.0.1");

        var result = await RunAsync(Python!, ["-I", Path.Combine(AppContext.BaseDirectory, "interop", "rest_server_client.py"),
            "gcs-token", host.Uri.ToString().TrimEnd('/'), Credential, "db", "t", gcs.Url], "");
        Assert.Equal("PyArrowFileIO", result["io"]!.GetValue<string>());
        Assert.Equal("ya29.read-1", result["token"]!.GetValue<string>()); // the reader's token from storage-credentials
        Assert.Equal("ya29.source", Assert.Single(sts.Requests).Form["subject_token"]);
        var expiresAt = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(result["expires_at"]!.GetValue<string>(), CultureInfo.InvariantCulture));
        Assert.InRange(expiresAt, DateTimeOffset.UtcNow.AddMinutes(50), DateTimeOffset.UtcNow.AddMinutes(61));
        Assert.False(result["exists"]!.GetValue<bool>());
        Assert.NotEmpty(gcs.Requests);
        Assert.All(gcs.Requests, r => Assert.Equal("Bearer ya29.read-1", r.Authorization));
        Assert.Contains(gcs.Requests, r => r.Path.StartsWith("/storage/v1/b/lake/o/wh%2Fdb%2Ft%2Fmetadata%2Fprobe.json", StringComparison.Ordinal));
    }

    [SkippableFact]
    public async Task Iceberg_java_gcs_file_io_sends_the_vended_token_for_the_tables_prefix_only()
    {
        var missing = DockerImageMissing(JavaImage);
        Skip.If(missing is not null, missing);
        var (host, _) = await StartAsync();
        await using var _ = host;
        using var gcs = RecordingGcs.Start("127.0.0.1");

        var interop = Path.Combine(AppContext.BaseDirectory, "interop");
        var output = await RunAsync("docker", ["run", "--rm", "--network", "host", "-v", $"{interop}:/client:ro", "--entrypoint", "java", JavaImage,
            "-cp", JavaLibs, "/client/GcsRestProbe.java", host.Uri.ToString().TrimEnd('/'), Credential, "db", "t", gcs.Url], "@@PROBE@@");
        Assert.Equal("org.apache.iceberg.io.ResolvingFileIO", output["io"]!.GetValue<string>()); // delegating gs:// to GCSFileIO
        Assert.Equal(1, output["credentials"]!.GetValue<int>());
        Assert.Equal("gs://lake/wh/db/t/", output["prefix"]!.GetValue<string>());
        Assert.Equal("ya29.read-1", output["token"]!.GetValue<string>());
        Assert.False(output["inside"]!.GetValue<bool>());
        var inside = Assert.Single(gcs.Requests, r => r.Path.StartsWith("/storage/v1/b/lake/o/wh%2Fdb%2Ft%2Fmetadata%2Fprobe.json", StringComparison.Ordinal));
        Assert.Equal("Bearer ya29.read-1", inside.Authorization);
        // Outside the table's prefix GCSFileIO falls back to the table config, where the token is too (for older clients).
        var outside = Assert.Single(gcs.Requests, r => r.Path.StartsWith("/storage/v1/b/lake/o/elsewhere%2Fprobe.json", StringComparison.Ordinal));
        Assert.Equal("Bearer ya29.read-1", outside.Authorization);
    }

    private static string? DockerImageMissing(string image)
    {
        try
        {
            using var p = Process.Start(new ProcessStartInfo("docker", ["image", "inspect", image]) { RedirectStandardOutput = true, RedirectStandardError = true })!;
            p.StandardOutput.ReadToEnd();
            p.WaitForExit();
            return p.ExitCode == 0 ? null : $"{image} is not present (docker pull {image})";
        }
        catch (System.ComponentModel.Win32Exception) { return "docker is not available"; }
    }

    /// <summary>Runs a process and parses the JSON after <paramref name="marker"/> on its last matching stdout line.</summary>
    private static async Task<JsonNode> RunAsync(string file, string[] args, string marker)
    {
        var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(5));
        var line = (await stdout).Split('\n').LastOrDefault(l => l.StartsWith(marker, StringComparison.Ordinal) && l.Trim().Length > marker.Length);
        if (p.ExitCode != 0 || line is null)
            throw new InvalidOperationException($"{file} failed ({p.ExitCode}): {await stderr}\n{await stdout}");
        return JsonNode.Parse(line[marker.Length..])!;
    }

    /// <summary>A stand-in for the GCS JSON API: records each request's path and Authorization header and answers 404.</summary>
    private sealed class RecordingGcs : IDisposable
    {
        private readonly HttpListener _listener = new();
        public ConcurrentQueue<(string Path, string? Authorization)> Requests { get; } = new();
        public string Url { get; }

        private RecordingGcs(string url) => Url = url;

        public static RecordingGcs Start(string host)
        {
            int port;
            using (var l = new TcpListener(IPAddress.Loopback, 0)) { l.Start(); port = ((IPEndPoint)l.LocalEndpoint).Port; }
            var gcs = new RecordingGcs($"http://{host}:{port}");
            gcs._listener.Prefixes.Add(gcs.Url + "/");
            gcs._listener.Start();
            _ = gcs.ServeAsync();
            return gcs;
        }

        private async Task ServeAsync()
        {
            while (_listener.IsListening)
            {
                HttpListenerContext ctx;
                try { ctx = await _listener.GetContextAsync(); }
                catch (Exception e) when (e is HttpListenerException or ObjectDisposedException) { return; }
                Requests.Enqueue((ctx.Request.RawUrl ?? "", ctx.Request.Headers["Authorization"]));
                var body = Encoding.UTF8.GetBytes("""{"error":{"code":404,"message":"No such object"}}""");
                ctx.Response.StatusCode = 404;
                ctx.Response.ContentType = "application/json";
                await ctx.Response.OutputStream.WriteAsync(body);
                ctx.Response.Close();
            }
        }

        public void Dispose() => _listener.Close();
    }
}
