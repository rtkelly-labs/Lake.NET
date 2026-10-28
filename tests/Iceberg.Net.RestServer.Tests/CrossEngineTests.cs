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
    /// <param name="services">Registrations made before the server's own (which do not replace them).</param>
    public static async Task<KestrelHost> StartAsync(Action<RestServerOptions>? configure = null, string? under = null, Action<IServiceCollection>? services = null)
    {
        var root = Path.Combine(under ?? Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests"), Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var app = RestServer.Build(["--urls", "http://127.0.0.1:0"], o =>
        {
            o.Store = "sqlite";
            o.SqlitePath = Path.Combine(root, "catalog.db");
            o.Warehouse = Path.Combine(root, "warehouse");
            configure?.Invoke(o);
        }, b =>
        {
            b.Logging.SetMinimumLevel(LogLevel.Warning);
            services?.Invoke(b.Services);
        });
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
        o.Authorization.AllowAll = true;
    }, under);

    private static Task<RestCatalog> ConnectAsync(KestrelHost host) =>
        RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = host.Uri, Credential = Credential, AccessDelegation = null }, _ => new LocalFileIO());

    private static IReadOnlyList<long> Ids(IEnumerable<object?[]> rows) => rows.Select(r => (long)r[0]!).Order().ToList();

    [SkippableFact]
    public async Task PyIceberg_and_the_dotnet_client_share_tables_through_the_server()
    {
        Require.Dependency("pyiceberg", Python is null ? "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)" : null);
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
        Require.Dependency("pyiceberg", Python is null ? "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)" : null);
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
        Require.Dependency("pyiceberg", Python is null ? "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)" : null);
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
    /// PyIceberg 0.12 with <c>scan-planning-mode=server</c>: it plans through the server (split into plan tasks here),
    /// applies the position deletes the server matched to each file, and reads the same rows we do.
    /// </summary>
    [SkippableFact]
    [Spec("rest: planTableScan")]
    [Spec("rest: fetchScanTasks")]
    public async Task PyIceberg_reads_through_server_side_scan_planning()
    {
        Require.Dependency("pyiceberg", Python is null ? "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)" : null);
        await using var host = await KestrelHost.StartAsync(o =>
        {
            o.Auth.Clients["spark"] = "spark-secret";
            o.Auth.SigningKey = "cross-engine-signing-key-of-at-least-32-bytes";
            o.Authorization.AllowAll = true;
            o.ScanPlanning.FilesPerPlanTask = 3;
        });
        using var catalog = await ConnectAsync(host);
        var table = await ScanPlanningTests.TableWithDeletesAsync(catalog, "planned", 2, "position");
        var planner = host.Services.GetRequiredService<ScanPlanner>();
        foreach (var maxId in new long?[] { null, 20 })
        {
            var before = planner.PlansStarted;
            var read = await PyIcebergAsync([.. new[] { "plan-read", host.Uri.ToString().TrimEnd('/'), Credential, "db", "planned" }, .. maxId is { } m ? [m.ToString()] : Array.Empty<string>()]);
            Assert.True(read["server_side"]!.GetValue<bool>());
            Assert.True(planner.PlansStarted > before, "PyIceberg did not plan on the server");
            var ours = maxId is { } max ? await TableScan.ReadAsync(table, Expressions.Expr.LessThan("id", max)) : await TableScan.ReadAllAsync(table);
            Assert.Equal(Ids(ours).Select(i => (int)i), read["ids"]!.AsArray().Select(i => i!.GetValue<int>()));
            Assert.NotEmpty(read["delete_files"]!.AsArray());
        }
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

    /// <summary>
    /// Spark 4 / Iceberg Java 1.12 told by the table config (<c>scan-planning-mode=server</c>) to plan on the server:
    /// its reads of tables with v2 position deletes and v3 deletion vectors, whole and filtered, go through
    /// <c>planTableScan</c> and plan tasks, and must return the rows we read.
    /// </summary>
    [SkippableFact]
    [Spec("rest: planTableScan")]
    [Spec("rest: fetchScanTasks")]
    public async Task Spark_reads_through_server_side_scan_planning()
    {
        Require.Spark();
        Directory.CreateDirectory(SparkOracle.SharedRoot);
        await using var host = await KestrelHost.StartAsync(o =>
        {
            o.Auth.Clients["spark"] = "spark-secret";
            o.Auth.SigningKey = "cross-engine-signing-key-of-at-least-32-bytes";
            o.Authorization.AllowAll = true;
            o.ScanPlanning.Mode = "server";
            o.ScanPlanning.FilesPerPlanTask = 3;
        }, SparkOracle.SharedRoot);
        using var catalog = await ConnectAsync(host);
        var v2 = await ScanPlanningTests.TableWithDeletesAsync(catalog, "pos_v2", 2, "position");
        var v3 = await ScanPlanningTests.TableWithDeletesAsync(catalog, "dv_v3", 3, "position");
        var planner = host.Services.GetRequiredService<ScanPlanner>();
        var before = planner.PlansStarted;

        var steps = await SparkAsync(host, host.Uri.ToString().TrimEnd('/'),
            "SELECT id FROM rest.db.pos_v2 ORDER BY id",
            "SELECT id FROM rest.db.pos_v2 WHERE id < 20 AND region <> 'us' ORDER BY id",
            "SELECT id FROM rest.db.dv_v3 ORDER BY id",
            "SELECT id FROM rest.db.dv_v3 WHERE region = 'eu' ORDER BY id");
        Assert.True(planner.PlansStarted >= before + 4, $"Spark made {planner.PlansStarted - before} server plans");
        var filter = Expressions.Expr.And(Expressions.Expr.LessThan("id", 20L), Expressions.Expr.NotEqual("region", "us"));
        Assert.Equal(Ids(await TableScan.ReadAllAsync(v2)).Select(i => (int)i), SparkIds(steps[0]!));
        Assert.Equal(Ids(await TableScan.ReadAsync(v2, filter)).Select(i => (int)i), SparkIds(steps[1]!));
        Assert.Equal(Ids(await TableScan.ReadAllAsync(v3)).Select(i => (int)i), SparkIds(steps[2]!));
        Assert.Equal(Ids(await TableScan.ReadAsync(v3, Expressions.Expr.Equal("region", "eu"))).Select(i => (int)i), SparkIds(steps[3]!));
    }

    /// <summary>
    /// Spark's batch incremental read (<c>start-snapshot-id</c> / <c>end-snapshot-id</c>: Java's
    /// <c>IncrementalAppendScan</c>, which Iceberg 1.12 plans on the client even for a REST table) must return the rows
    /// our server's incremental plan does, over a history of appends, a merge-on-read delete and a copy-on-write delete.
    /// </summary>
    [SkippableFact]
    [Spec("rest: planTableScan")]
    public async Task Spark_incremental_reads_match_server_incremental_plans()
    {
        Require.Spark();
        Directory.CreateDirectory(SparkOracle.SharedRoot);
        await using var host = await StartSecuredAsync(SparkOracle.SharedRoot);
        using var catalog = await ConnectAsync(host);
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "inc"), ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 10))).CommitAsync();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(10, 10))).CommitAsync();
        await TableMutations.DeleteWhereAsync(t, Expressions.Expr.In("id", 2L, 12L), DeleteStrategy.PositionDeletes);
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(20, 10))).CommitAsync();
        await TableMutations.DeleteWhereAsync(t, Expressions.Expr.Equal("id", 21L), DeleteStrategy.CopyOnWrite);
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(30, 10))).CommitAsync();
        var table = await catalog.LoadTableAsync(t.Identifier);
        var history = table.Metadata.Ancestors(table.Metadata.CurrentSnapshotId).Reverse().Select(s => s.SnapshotId).ToList();

        var ranges = new (int From, int To)[] { (0, 1), (0, 5), (1, 3), (2, 4), (3, 5), (1, 2) };
        var steps = ranges.Select(r => (JsonNode)new JsonObject
        {
            ["read"] = "rest.db.inc",
            ["options"] = new JsonObject { ["start-snapshot-id"] = history[r.From].ToString(), ["end-snapshot-id"] = history[r.To].ToString() },
        }).ToArray();
        var spark = await SparkAsync(host, host.Uri.ToString().TrimEnd('/'), steps);
        for (var i = 0; i < ranges.Length; i++)
        {
            var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest { StartSnapshotId = history[ranges[i].From], EndSnapshotId = history[ranges[i].To] });
            var ours = Ids(await ServerSideScan.ReadAsync(table, plan)).Select(id => (int)id);
            Assert.Equal(SparkIds(spark[i]!).Order(), ours);
        }
        Assert.NotEmpty(SparkIds(spark[1]!));
    }

    /// <summary>
    /// Spark 4 / Iceberg 1.12 asking for <c>remote-signing</c>, with no AWS credentials at all: its S3FileIO
    /// (<c>S3V4RestSignerClient</c>) has the server sign every request for the table's objects on a moto that checks
    /// signatures (<see cref="AuthenticatingMoto"/>). Spark creates,
    /// inserts, deletes (merge-on-read) and reads; our client, also signing remotely, reads that and appends; Spark reads
    /// our append.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Emulation")]
    [Spec("rest: signRequest")]
    public async Task Spark_signs_s3_requests_through_the_server()
    {
        Require.Spark();
        Require.Dependency("moto-signing", AuthenticatingMoto.SkipReason);
        Require.Dependency("spark-aws", AwsBundleJar is null ? "iceberg-aws-bundle-1.12.0.jar is not next to the Spark jar" : null);
        Directory.CreateDirectory(SparkOracle.SharedRoot);
        await using var moto = await AuthenticatingMoto.StartAsync();
        var settings = RemoteSigningTests.MotoSettings(moto);
        await using var host = await KestrelHost.StartAsync(o =>
        {
            o.Auth.Clients["spark"] = "spark-secret";
            o.Auth.SigningKey = "cross-engine-signing-key-of-at-least-32-bytes";
            o.Authorization.AllowAll = true;
            o.Warehouse = settings["Warehouse"];
            o.RemoteSigning.Enabled = true;
            foreach (var (k, v) in settings.Where(kv => kv.Key.StartsWith("FileIO:", StringComparison.Ordinal))) o.FileIO[k["FileIO:".Length..]] = v;
            foreach (var (k, v) in settings.Where(kv => kv.Key.StartsWith("TableConfig:", StringComparison.Ordinal))) o.TableConfig[k["TableConfig:".Length..]] = v;
        }, SparkOracle.SharedRoot);
        var uri = host.Uri.ToString().TrimEnd('/');
        var catalogProps = new Dictionary<string, string> { ["header.X-Iceberg-Access-Delegation"] = "remote-signing" };

        var first = await SparkAsync(host, uri, new JsonNode[]
        {
            "CREATE NAMESPACE IF NOT EXISTS rest.signed",
            "CREATE TABLE rest.signed.events (id BIGINT, name STRING, region STRING) USING iceberg PARTITIONED BY (region) "
                + "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')",
            "INSERT INTO rest.signed.events VALUES " + string.Join(", ", Enumerable.Range(0, 10).Select(i => $"({i}, 's-{i}', '{(i % 2 == 0 ? "eu" : "us")}')")),
            "DELETE FROM rest.signed.events WHERE id = 3",
            "SELECT id FROM rest.signed.events ORDER BY id",
        }, catalogProps, awsBundle: true);
        Assert.Equal(Enumerable.Range(0, 10).Where(i => i != 3), SparkIds(first[^1]!));

        using var catalog = await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = host.Uri, Credential = Credential, AccessDelegation = "remote-signing" });
        var events = await catalog.LoadTableAsync(TableIdentifier.Of("signed", "events"));
        Assert.StartsWith("s3://", events.Location);
        Assert.Equal(Enumerable.Range(0, 10).Where(i => i != 3).Select(i => (long)i), Ids(await TableScan.ReadAllAsync(events)));
        var rows = Enumerable.Range(10, 5).Select(i => new object?[] { (long)i, $"net-{i}", i % 2 == 0 ? "eu" : "us" }).ToList();
        await SnapshotProducer.Append(events).AddFiles(await TableWriter.WriteAsync(events, rows)).CommitAsync();

        var second = await SparkAsync(host, uri, new JsonNode[] { "SELECT id FROM rest.signed.events ORDER BY id" }, catalogProps, awsBundle: true);
        Assert.Equal(Enumerable.Range(0, 15).Where(i => i != 3), SparkIds(second[0]!));
        // Not Spark's DROP TABLE … PURGE: it drops the table first and then deletes the files itself, and the server
        // cannot sign for a table that no longer exists. The server purges with its own credentials instead.
        await catalog.DropTableAsync(events.Identifier, purge: true);
    }

    /// <summary>
    /// PyIceberg 0.12 asking for <c>remote-signing</c>: the table config switches it to its fsspec FileIO with the
    /// <c>S3V4RestSigner</c>, which has the server sign each request (on a moto that checks signatures). It appends to
    /// and reads a table we created; we read its append.
    /// </summary>
    [SkippableFact]
    [Trait("Category", "Emulation")]
    [Spec("rest: signRequest")]
    public async Task PyIceberg_signs_s3_requests_through_the_server()
    {
        Require.Dependency("pyiceberg", Python is null ? "ICEBERG_PYTHON is not set (an interpreter with interop/pyproject.toml installed)" : null);
        Require.Dependency("moto-signing", AuthenticatingMoto.SkipReason);
        await using var moto = await AuthenticatingMoto.StartAsync();
        var settings = RemoteSigningTests.MotoSettings(moto);
        await using var host = await KestrelHost.StartAsync(o =>
        {
            o.Auth.Clients["spark"] = "spark-secret";
            o.Auth.SigningKey = "cross-engine-signing-key-of-at-least-32-bytes";
            o.Authorization.AllowAll = true;
            o.Warehouse = settings["Warehouse"];
            o.RemoteSigning.Enabled = true;
            foreach (var (k, v) in settings.Where(kv => kv.Key.StartsWith("FileIO:", StringComparison.Ordinal))) o.FileIO[k["FileIO:".Length..]] = v;
            foreach (var (k, v) in settings.Where(kv => kv.Key.StartsWith("TableConfig:", StringComparison.Ordinal))) o.TableConfig[k["TableConfig:".Length..]] = v;
        });
        using var catalog = await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = host.Uri, Credential = Credential, AccessDelegation = "remote-signing" });
        await catalog.CreateNamespaceAsync(Namespace.Of("py"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("py", "signed"), ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 6))).CommitAsync();

        var read = await PyIcebergAsync("signed-append-read", host.Uri.ToString().TrimEnd('/'), Credential, "py", "signed", "6", "4");
        Assert.Equal("FsspecFileIO", read["io"]!.GetValue<string>());
        Assert.Equal("S3V4RestSigner", read["signer"]!.GetValue<string>());
        Assert.Equal(Enumerable.Range(0, 10), read["ids"]!.AsArray().Select(i => i!.GetValue<int>()));
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), Ids(await TableScan.ReadAllAsync(await catalog.LoadTableAsync(t.Identifier))));
        await catalog.DropTableAsync(t.Identifier, purge: true);
    }

    private static IEnumerable<int> SparkIds(JsonNode step) => step["rows"]!.AsArray().Select(r => int.Parse(r!["id"]!.GetValue<string>()));

    private static Task<JsonArray> SparkAsync(KestrelHost host, string uri, params string[] steps) =>
        SparkAsync(host, uri, steps.Select(s => (JsonNode)s).ToArray());

    /// <summary>The Iceberg runtime jar (<c>ICEBERG_SPARK_JAR</c>, else the emulation cache).</summary>
    private static string SparkJar =>
        Environment.GetEnvironmentVariable("ICEBERG_SPARK_JAR") ?? Path.Combine(TestEnvironment.RepoDir(), "emulation", ".cache", SparkOracle.JarName);

    /// <summary>Iceberg's AWS bundle (S3FileIO and the AWS SDK), next to the runtime jar; null when absent.</summary>
    internal static string? AwsBundleJar => Path.Combine(Path.GetDirectoryName(SparkJar)!, "iceberg-aws-bundle-1.12.0.jar") is var p && File.Exists(p) ? p : null;

    /// <param name="steps">SQL strings, or <c>{"read": table, "options": {...}}</c> DataFrame reads.</param>
    /// <param name="catalog">Extra properties of the Spark catalog <c>rest</c>.</param>
    /// <param name="awsBundle">Put Iceberg's AWS bundle on the classpath too (S3FileIO).</param>
    private static async Task<JsonArray> SparkAsync(KestrelHost host, string uri, JsonNode[] steps, IReadOnlyDictionary<string, string>? catalog = null, bool awsBundle = false)
    {
        var stepsFile = Path.Combine(host.Root, $"steps-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(stepsFile, new JsonArray(steps.Select(s => s.DeepClone()).ToArray()).ToJsonString());
        var catalogFile = Path.Combine(host.Root, $"catalog-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(catalogFile, Json.MetadataJson.StringMap(catalog ?? new Dictionary<string, string>()).ToJsonString());
        var jar = SparkJar;
        var jars = $"/jars/{Path.GetFileName(jar)}";
        if (awsBundle) jars += $",/jars/{Path.GetFileName(AwsBundleJar ?? throw new InvalidOperationException("iceberg-aws-bundle-1.12.0.jar is not next to the Spark jar"))}";
        var interop = Path.Combine(AppContext.BaseDirectory, "interop");
        var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in new[]
        {
            "run", "--rm", "--user", "root", "--network", "host", "-e", "SPARK_LOCAL_IP=127.0.0.1", "-e", "AWS_REGION=us-east-1",
            "-v", $"{Path.GetDirectoryName(jar)}:/jars:ro", "-v", $"{interop}:/client:ro", "-v", $"{SparkOracle.SharedRoot}:{SparkOracle.SharedRoot}",
            // Spark writes root-owned files on the shared mount. Let the host client read/rewrite them
            // after this invocation, as the shared Spark oracle does, while preserving Spark's exit code.
            "-e", $"SHARED_ROOT={SparkOracle.SharedRoot}",
            SparkOracle.Image, "sh", "-c", "\"$0\" \"$@\"; rc=$?; chmod -R a+rwX \"$SHARED_ROOT\"; exit $rc",
            "/opt/spark/bin/spark-submit", "--jars", jars, "/client/spark_rest_client.py",
            uri, Credential, stepsFile, catalogFile,
        }) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(10));
        var output = await stdout;
        var marker = output.LastIndexOf("@@ORACLE@@", StringComparison.Ordinal);
        if (p.ExitCode != 0 || marker < 0)
        {
            // The errors, then the end of the log (Spark's shutdown chatter follows a failure).
            if (Environment.GetEnvironmentVariable("ICEBERG_SPARK_LOG") is { Length: > 0 } log) await File.WriteAllTextAsync(log, output + "\n----\n" + await stderr);
            var lines = (await stderr).Split('\n');
            var errors = lines.Where(l => l.Contains("Exception", StringComparison.Ordinal) || l.Contains("Error", StringComparison.Ordinal)
                || l.TrimStart().StartsWith("Caused by", StringComparison.Ordinal)).Take(30);
            throw new InvalidOperationException($"Spark failed ({p.ExitCode}): {string.Join('\n', errors)}\n...\n{string.Join('\n', lines.TakeLast(15))}");
        }
        return JsonNode.Parse(output[(marker + "@@ORACLE@@".Length)..])!.AsArray();
    }
}
