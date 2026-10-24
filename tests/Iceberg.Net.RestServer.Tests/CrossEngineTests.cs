using System.Diagnostics;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>The server on a real port (Kestrel), so engines in other processes can use it.</summary>
public sealed class KestrelHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    public string Root { get; }
    public Uri Uri { get; }
    public IServiceProvider Services => _app.Services;

    private KestrelHost(WebApplication app, string root, Uri uri)
    {
        _app = app;
        Root = root;
        Uri = uri;
    }

    /// <param name="under">Parent directory of the server's warehouse and database (default: a temp directory).</param>
    public static async Task<KestrelHost> StartAsync(Action<RestServerOptions>? configure = null, string? under = null)
    {
        var root = Path.Combine(under ?? Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests"), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var app = RestServer.Build(["--urls", "http://127.0.0.1:0"], o =>
        {
            o.Store = "sqlite";
            o.SqlitePath = Path.Combine(root, "catalog.db");
            o.Warehouse = Path.Combine(root, "warehouse");
            configure?.Invoke(o);
        }, b => b.Logging.SetMinimumLevel(LogLevel.Warning));
        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new KestrelHost(app, root, new Uri(address));
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        if (Directory.Exists(Root)) Directory.Delete(Root, recursive: true);
    }
}

/// <summary>
/// PyIceberg (a separate implementation of the REST client) against the server on a local port: creating, appending,
/// evolving and reading tables, interleaved with this library's client, both ways. Needs <c>ICEBERG_PYTHON</c> (an
/// interpreter with interop/pyproject.toml installed); skipped otherwise.
/// </summary>
[Trait("Category", "CrossEngine")]
public sealed class CrossEngineTests
{
    private const string Credential = "spark:spark-secret";

    private static string? Python => Environment.GetEnvironmentVariable("ICEBERG_PYTHON") is { Length: > 0 } p && File.Exists(p) ? p : null;

    private static async Task<JsonNode> PyIcebergAsync(params string[] args)
    {
        var script = Path.Combine(AppContext.BaseDirectory, "interop", "rest_server_client.py");
        var psi = new ProcessStartInfo(Python!) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in new[] { "-I", script }.Concat(args)) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(3));
        if (p.ExitCode != 0) throw new InvalidOperationException($"PyIceberg failed ({p.ExitCode}): {await stderr}");
        return JsonNode.Parse((await stdout).Trim().Split('\n')[^1])!;
    }

    private static Task<KestrelHost> StartSecuredAsync(string? under = null) => KestrelHost.StartAsync(o =>
    {
        o.Auth.Clients["spark"] = "spark-secret";
        o.Auth.SigningKey = "cross-engine-signing-key-of-at-least-32-bytes";
    }, under);

    private static Task<RestCatalog> ConnectAsync(KestrelHost host) =>
        RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = host.Uri, Credential = Credential, AccessDelegation = null }, _ => new LocalFileIO());

    private static IReadOnlyList<long> Ids(IEnumerable<object?[]> rows) => rows.Select(r => (long)r[0]!).Order().ToList();

    [SkippableFact]
    public async Task PyIceberg_and_the_dotnet_client_share_tables_through_the_server()
    {
        Skip.If(Python is null, "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)");
        await using var host = await StartSecuredAsync();
        var uri = host.Uri.ToString().TrimEnd('/');

        // PyIceberg creates and appends (through the OAuth2 token endpoint); we read what it wrote.
        var written = await PyIcebergAsync("write", uri, Credential, "py", "events", "0", "10");
        Assert.Equal(10, written["rows"]!.GetValue<int>());
        using var catalog = await ConnectAsync(host);
        var id = TableIdentifier.Of("py", "events");
        var table = await catalog.LoadTableAsync(id);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), Ids(await TableScan.ReadAllAsync(table)));

        // We append; PyIceberg sees both appends.
        var rows = Enumerable.Range(10, 5).Select(i => new object?[] { (long)i, $"net-{i}", i % 2 == 0 ? "eu" : "us" }).ToList();
        await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, rows)).CommitAsync();
        var read = await PyIcebergAsync("read", uri, Credential, "py", "events");
        Assert.Equal(Enumerable.Range(0, 15), read["ids"]!.AsArray().Select(i => i!.GetValue<int>()));
        Assert.Equal(2, read["snapshots"]!.GetValue<int>());

        // PyIceberg appends again on top of our snapshot, then evolves the schema; we see both.
        await PyIcebergAsync("write", uri, Credential, "py", "events", "15", "5");
        var evolved = await PyIcebergAsync("evolve", uri, Credential, "py", "events", "note");
        Assert.Contains("note", evolved["columns"]!.AsArray().Select(c => c!.GetValue<string>()));
        var final = await catalog.LoadTableAsync(id);
        Assert.Equal(20, (await TableScan.ReadAllAsync(final)).Count);
        Assert.Equal(3, final.Metadata.Snapshots.Count);
        Assert.NotNull(final.Schema.FindField("note"));
        Assert.Equal(3, final.Metadata.Ancestors(final.Metadata.CurrentSnapshotId).Count());
    }

    [SkippableFact]
    public async Task PyIceberg_staged_create_transactions_commit_through_the_server()
    {
        Skip.If(Python is null, "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)");
        await using var host = await StartSecuredAsync();
        var uri = host.Uri.ToString().TrimEnd('/');
        var written = await PyIcebergAsync("write", uri, Credential, "py", "staged", "0", "6", "stage");
        Assert.Equal(6, written["rows"]!.GetValue<int>());
        using var catalog = await ConnectAsync(host);
        var table = await catalog.LoadTableAsync(TableIdentifier.Of("py", "staged"));
        Assert.Single(table.Metadata.Snapshots);
        Assert.Equal(Enumerable.Range(0, 6).Select(i => (long)i), Ids(await TableScan.ReadAllAsync(table)));
    }

    [SkippableFact]
    public async Task PyIceberg_reads_tables_the_dotnet_client_creates()
    {
        Skip.If(Python is null, "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)");
        await using var host = await StartSecuredAsync();
        using var catalog = await ConnectAsync(host);
        await catalog.CreateNamespaceAsync(Namespace.Of("net"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("net", "events"), CatalogContractTests.Schema, CatalogContractTests.Spec));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, CatalogContractTests.Rows(0, 25))).CommitAsync();
        await SnapshotProducer.Overwrite(t, f => (string?)f.Partition[2] == "us").CommitAsync();
        var read = await PyIcebergAsync("read", host.Uri.ToString().TrimEnd('/'), Credential, "net", "events");
        Assert.Equal(Enumerable.Range(0, 25).Where(i => i % 2 == 0), read["ids"]!.AsArray().Select(i => i!.GetValue<int>()));
        Assert.Equal(2, read["snapshots"]!.GetValue<int>());
    }

    /// <summary>
    /// Spark 4 with Iceberg Java's RESTCatalog (apache/spark image, host network) against the server: Spark creates,
    /// inserts and deletes (merge-on-read position deletes), we read that and append, Spark reads our append and a table
    /// we created, and evolves its schema. The warehouse lives under <see cref="SparkOracle.SharedRoot"/>, mounted at the
    /// same path in the container.
    /// </summary>
    [SkippableFact]
    public async Task Spark_and_the_dotnet_client_share_tables_through_the_server()
    {
        Require.Spark();
        Directory.CreateDirectory(SparkOracle.SharedRoot);
        await using var host = await StartSecuredAsync(SparkOracle.SharedRoot);
        var uri = host.Uri.ToString().TrimEnd('/');

        var first = await SparkAsync(host, uri,
            "CREATE NAMESPACE IF NOT EXISTS rest.spark",
            "CREATE TABLE rest.spark.events (id BIGINT, name STRING, region STRING) USING iceberg PARTITIONED BY (region) "
                + "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')",
            "INSERT INTO rest.spark.events VALUES " + string.Join(", ", Enumerable.Range(0, 10).Select(i => $"({i}, 's-{i}', '{(i % 2 == 0 ? "eu" : "us")}')")),
            "DELETE FROM rest.spark.events WHERE id = 3",
            "SELECT id FROM rest.spark.events ORDER BY id");
        Assert.Equal(Enumerable.Range(0, 10).Where(i => i != 3), SparkIds(first[^1]!));

        using var catalog = await ConnectAsync(host);
        var events = await catalog.LoadTableAsync(TableIdentifier.Of("spark", "events"));
        Assert.Equal(Enumerable.Range(0, 10).Where(i => i != 3).Select(i => (long)i), Ids(await TableScan.ReadAllAsync(events)));
        Assert.Equal("1", events.Metadata.CurrentSnapshot!.Summary["added-position-delete-files"]);
        var rows = Enumerable.Range(10, 5).Select(i => new object?[] { (long)i, $"net-{i}", i % 2 == 0 ? "eu" : "us" }).ToList();
        await SnapshotProducer.Append(events).AddFiles(await TableWriter.WriteAsync(events, rows)).CommitAsync();

        await catalog.CreateNamespaceAsync(Namespace.Of("net"));
        var ours = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("net", "events"), CatalogContractTests.Schema, CatalogContractTests.Spec));
        await SnapshotProducer.Append(ours).AddFiles(await TableWriter.WriteAsync(ours, CatalogContractTests.Rows(0, 12))).CommitAsync();

        var second = await SparkAsync(host, uri,
            "SELECT id FROM rest.spark.events ORDER BY id",
            "SELECT id FROM rest.net.events ORDER BY id",
            "ALTER TABLE rest.net.events ADD COLUMN note STRING",
            "INSERT INTO rest.net.events (id, name, amount, event_ts, region, qty, note) VALUES (100, 'spark', 1.0, TIMESTAMP '2024-01-02 00:00:00', 'eu', 1, 'from spark')");
        Assert.Equal(Enumerable.Range(0, 15).Where(i => i != 3), SparkIds(second[0]!));
        Assert.Equal(Enumerable.Range(0, 12), SparkIds(second[1]!));

        var evolved = await catalog.LoadTableAsync(ours.Identifier);
        Assert.NotNull(evolved.Schema.FindField("note"));
        var all = await TableScan.ReadAllAsync(evolved);
        Assert.Equal(13, all.Count);
        Assert.Equal("from spark", all.Single(r => (long)r[0]! == 100)[6]);
    }

    private static IEnumerable<int> SparkIds(JsonNode step) => step["rows"]!.AsArray().Select(r => int.Parse(r!["id"]!.GetValue<string>()));

    private static async Task<JsonArray> SparkAsync(KestrelHost host, string uri, params string[] steps)
    {
        var stepsFile = Path.Combine(host.Root, $"steps-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(stepsFile, new JsonArray(steps.Select(s => (JsonNode)s).ToArray()).ToJsonString());
        var jar = Environment.GetEnvironmentVariable("ICEBERG_SPARK_JAR") ?? Path.Combine(TestEnvironment.RepoDir(), "emulation", ".cache", SparkOracle.JarName);
        var interop = Path.Combine(AppContext.BaseDirectory, "interop");
        var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in new[]
        {
            "run", "--rm", "--user", "root", "--network", "host", "-e", "SPARK_LOCAL_IP=127.0.0.1",
            "-v", $"{Path.GetDirectoryName(jar)}:/jars:ro", "-v", $"{interop}:/client:ro", "-v", $"{SparkOracle.SharedRoot}:{SparkOracle.SharedRoot}",
            SparkOracle.Image, "/opt/spark/bin/spark-submit", "--jars", $"/jars/{Path.GetFileName(jar)}", "/client/spark_rest_client.py",
            uri, Credential, stepsFile,
        }) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(10));
        var output = await stdout;
        var marker = output.LastIndexOf("@@ORACLE@@", StringComparison.Ordinal);
        if (p.ExitCode != 0 || marker < 0)
            throw new InvalidOperationException($"Spark failed ({p.ExitCode}): {string.Join('\n', (await stderr).Split('\n').TakeLast(40))}");
        return JsonNode.Parse(output[(marker + "@@ORACLE@@".Length)..])!.AsArray();
    }
}
