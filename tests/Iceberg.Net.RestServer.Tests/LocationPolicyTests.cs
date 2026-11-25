using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using static Iceberg.Net.RestServer.Tests.ProtocolTests;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Locations a client chooses (docs/rest-server.md, Storage locations): the server acts on them with its own access, so
/// they must stay under an allowed root, inside their namespace, and clear of every other table, view and namespace.
/// Every refusal is a 400 and leaves nothing behind.
/// </summary>
public sealed class LocationPolicyTests : IAsyncLifetime
{
    private ServerHost _host = null!;
    private HttpClient _http = null!;
    private Catalog.RestCatalog _catalog = null!;

    public async Task InitializeAsync()
    {
        _host = ServerHost.Start();
        _http = _host.Http();
        _catalog = await _host.ConnectAsync();
        await _catalog.CreateNamespaceAsync(Namespace.Of("sales"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("hr"));
    }

    public async Task DisposeAsync()
    {
        _catalog.Dispose();
        _http.Dispose();
        await _host.DisposeAsync();
    }

    private string Wh(params string[] parts) => _host.Warehouse + "/" + string.Join('/', parts);

    private static JsonObject CreateBody(string name, string? location = null, Dictionary<string, string>? properties = null)
    {
        var body = new JsonObject { ["name"] = name, ["schema"] = TypeJson.SchemaToJson(ScanPlanningTests.Schema) };
        if (location is not null) body["location"] = location;
        if (properties is not null) body["properties"] = new JsonObject(properties.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value)));
        return body;
    }

    private async Task AssertRefusedAsync(string path, JsonNode body)
    {
        var r = await SendAsync(_http, HttpMethod.Post, path, body);
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", r);
    }

    [Theory]
    [InlineData("s3://another-bucket/wh/sales/t")]
    [InlineData("s3://*")]
    [InlineData("/etc")]
    [InlineData("file:///")]
    public async Task A_table_cannot_be_created_outside_the_allowed_storage(string location)
    {
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", location));
        Assert.False(await _catalog.TableExistsAsync(TableIdentifier.Of("sales", "t")));
    }

    [Fact]
    public async Task A_table_cannot_be_created_in_another_namespace_or_another_tables_directory()
    {
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("hr", "salaries"), ScanPlanningTests.Schema));
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("hr", "salaries")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("hr", "elsewhere")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "orders")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "orders", "inner")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "x", "..", "orders")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables",
            CreateBody("t", properties: new() { ["write.data.path"] = Wh("hr", "salaries", "data") }));
        // Inside its own namespace and clear of others, a chosen location is fine.
        Assert.Equal(200, (int)(await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "custom-t")))).Status);
    }

    [Fact]
    public async Task A_commit_cannot_move_a_table_or_its_data_onto_another_table()
    {
        var victim = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        var mine = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "mine"), ScanPlanningTests.Schema));
        UpdateRequirement[] uuid = [new UpdateRequirement.AssertTableUuid(mine.Metadata.TableUuid)];
        foreach (MetadataUpdate update in new MetadataUpdate[]
                 {
                     new MetadataUpdate.SetLocation(victim.Location),
                     new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["write.data.path"] = $"{victim.Location}/data" }),
                     new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["write.metadata.path"] = "s3://another-bucket/meta" }),
                 })
        {
            var e = await Assert.ThrowsAsync<HttpRequestException>(() => _catalog.CommitTableAsync(new TableCommit(mine.Identifier, uuid, [update])));
            Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        }
        var reloaded = await _catalog.LoadTableAsync(mine.Identifier);
        Assert.Equal(mine.Location, reloaded.Location);
        Assert.False(reloaded.Metadata.Properties.ContainsKey("write.data.path"));
    }

    [Fact]
    public async Task A_new_table_cannot_take_over_a_renamed_tables_directory()
    {
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        await _catalog.RenameTableAsync(TableIdentifier.Of("sales", "orders"), TableIdentifier.Of("sales", "orders_2024"));
        // The renamed table keeps its directory, sales/orders, which is where a new "orders" would go by default.
        var e = await Assert.ThrowsAsync<HttpRequestException>(() =>
            _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
    }

    [Fact]
    public async Task A_table_renamed_into_another_namespace_keeps_its_directory()
    {
        await _catalog.CreateNamespaceAsync(Namespace.Of("archive"));
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        await _catalog.RenameTableAsync(TableIdentifier.Of("sales", "orders"), TableIdentifier.Of("archive", "orders"));
        // archive.orders still writes to sales/orders: neither a new sales.orders nor a namespace there may claim it.
        var e = await Assert.ThrowsAsync<HttpRequestException>(() =>
            _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        await AssertRefusedAsync("/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("sales", "orders") });
    }

    [Fact]
    public async Task A_second_spelling_of_a_directory_is_refused()
    {
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        // A local file system reads sales//orders as sales/orders.
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", _host.Warehouse + "/sales//orders"));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", _host.Warehouse + "/sales//t"));
        Assert.False(await _catalog.TableExistsAsync(TableIdentifier.Of("sales", "t")));
    }

    [Fact]
    public async Task A_transaction_cannot_move_two_tables_into_one_directory()
    {
        var a = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "a"), ScanPlanningTests.Schema));
        var b = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "b"), ScanPlanningTests.Schema));
        var shared = Wh("sales", "shared");
        var e = await Assert.ThrowsAsync<HttpRequestException>(() => _catalog.CommitTransactionAsync(
        [
            new TableCommit(a.Identifier, [new UpdateRequirement.AssertTableUuid(a.Metadata.TableUuid)], [new MetadataUpdate.SetLocation(shared)]),
            new TableCommit(b.Identifier, [new UpdateRequirement.AssertTableUuid(b.Metadata.TableUuid)], [new MetadataUpdate.SetLocation(shared + "/inner")]),
        ]));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Equal(a.Location, (await _catalog.LoadTableAsync(a.Identifier)).Location);
        Assert.Equal(b.Location, (await _catalog.LoadTableAsync(b.Identifier)).Location);
    }

    [Fact]
    public async Task An_implied_child_namespace_does_not_block_creates_in_its_parent()
    {
        // The memory store lists a.b as a child of a once a.b.c exists, though a.b was never created.
        await _catalog.CreateNamespaceAsync(Namespace.Of("a"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("a", "b", "c"));
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("a", "t"), ScanPlanningTests.Schema));
        Assert.True(await _catalog.TableExistsAsync(TableIdentifier.Of("a", "t")));
    }

    [Fact]
    public async Task Register_checks_the_metadata_location_before_reading_it()
    {
        var existing = Path.Combine(_host.Root, "catalog.db"); // exists, outside the namespace
        var missing = Path.Combine(_host.Root, "absent.metadata.json");
        var first = await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/register", new JsonObject { ["name"] = "x", ["metadata-location"] = existing });
        var second = await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/register", new JsonObject { ["name"] = "x", ["metadata-location"] = missing });
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", first);
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", second);
        // Same answer whether or not the file exists: the location is refused before anything is read.
        Assert.Contains("outside", first.Body!["error"]!["message"]!.GetValue<string>());
        Assert.Contains("outside", second.Body!["error"]!["message"]!.GetValue<string>());
    }

    [Fact]
    public async Task Namespace_locations_stay_in_allowed_storage_and_clear_of_other_namespaces()
    {
        await AssertRefusedAsync("/v1/namespaces", new JsonObject
            { ["namespace"] = new JsonArray("ops"), ["properties"] = new JsonObject { ["location"] = "s3://another-bucket/ops" } });
        await AssertRefusedAsync("/v1/namespaces", new JsonObject
            { ["namespace"] = new JsonArray("ops"), ["properties"] = new JsonObject { ["location"] = Wh("hr", "ops") } });
        Assert.False(await _catalog.NamespaceExistsAsync(Namespace.Of("ops")));

        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        // A namespace that has tables cannot move, and a child cannot be rooted in a parent's table.
        await AssertRefusedAsync("/v1/namespaces/sales/properties", new JsonObject { ["updates"] = new JsonObject { ["location"] = Wh("sales2") } });
        await AssertRefusedAsync("/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("sales", "orders") });
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("a/b")]
    [InlineData("..")]
    [InlineData("t*")]
    [InlineData("t?")]
    [InlineData("*")]
    [InlineData("${aws:username}")]
    public async Task Names_that_would_reshape_a_path_are_refused(string name)
    {
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody(name));
        await AssertRefusedAsync("/v1/namespaces/sales/views", new JsonObject
        {
            ["name"] = name, ["schema"] = TypeJson.SchemaToJson(ScanPlanningTests.Schema),
            ["view-version"] = new JsonObject
            {
                ["version-id"] = 1, ["timestamp-ms"] = 0, ["schema-id"] = 0, ["summary"] = new JsonObject(),
                ["default-namespace"] = new JsonArray("sales"),
                ["representations"] = new JsonArray(new JsonObject { ["type"] = "sql", ["sql"] = "SELECT 1", ["dialect"] = "spark" }),
            },
        });
    }

    // Credential vending scopes an IAM policy to a table's location: a '*', '?' or '${…}' anywhere in it (a namespace
    // level, a table name, or a chosen location) would widen that policy to other tables' files.
    [Theory]
    [InlineData("*")]
    [InlineData("s*")]
    [InlineData("s?")]
    [InlineData("$x")]
    [InlineData("..")]
    public async Task Namespace_levels_that_would_reshape_a_path_or_policy_are_refused(string level)
    {
        await AssertRefusedAsync("/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray(level) });
        await AssertRefusedAsync("/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("sales", level) });
        Assert.False(await _catalog.NamespaceExistsAsync(Namespace.Of(level)));
    }

    [Fact]
    public async Task Locations_with_policy_wildcards_are_refused()
    {
        await AssertRefusedAsync("/v1/namespaces", new JsonObject
            { ["namespace"] = new JsonArray("ops"), ["properties"] = new JsonObject { ["location"] = Wh("*") } });
        await AssertRefusedAsync("/v1/namespaces", new JsonObject
            { ["namespace"] = new JsonArray("ops"), ["properties"] = new JsonObject { ["location"] = Wh("ops*") } });
        await AssertRefusedAsync("/v1/namespaces", new JsonObject
            { ["namespace"] = new JsonArray("ops"), ["properties"] = new JsonObject { ["location"] = Wh("ops", "${aws:username}") } });
        Assert.False(await _catalog.NamespaceExistsAsync(Namespace.Of("ops")));
        await _catalog.CreateNamespaceAsync(Namespace.Of("ops"));
        await AssertRefusedAsync("/v1/namespaces/ops/properties", new JsonObject { ["updates"] = new JsonObject { ["location"] = Wh("ops", "*") } });
        Assert.Null((await SendAsync(_http, HttpMethod.Get, "/v1/namespaces/ops")).Body!["properties"]?["location"]);

        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "t*")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "t?")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables",
            CreateBody("t", properties: new() { ["write.data.path"] = Wh("sales", "t", "*") }));
        Assert.False(await _catalog.TableExistsAsync(TableIdentifier.Of("sales", "t")));

        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "t"), ScanPlanningTests.Schema));
        var e = await Assert.ThrowsAsync<HttpRequestException>(() => _catalog.CommitTableAsync(new TableCommit(t.Identifier,
            [new UpdateRequirement.AssertTableUuid(t.Metadata.TableUuid)], [new MetadataUpdate.SetLocation(Wh("sales", "t*"))])));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Equal(t.Location, (await _catalog.LoadTableAsync(t.Identifier)).Location);
    }

    [Fact]
    public async Task A_table_cannot_be_renamed_to_a_name_that_would_reshape_a_path_or_policy()
    {
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        foreach (var name in new[] { "o*", "o?", "$o", "../o" })
            await AssertRefusedAsync("/v1/tables/rename", new JsonObject
            {
                ["source"] = new JsonObject { ["namespace"] = new JsonArray("sales"), ["name"] = "orders" },
                ["destination"] = new JsonObject { ["namespace"] = new JsonArray("sales"), ["name"] = name },
            });
        Assert.True(await _catalog.TableExistsAsync(TableIdentifier.Of("sales", "orders")));
    }

    // ---------- files a table's metadata points at ----------

    private static MetadataUpdate[] AddSnapshot(Table t, string manifestList)
    {
        var id = (t.Metadata.CurrentSnapshotId ?? 0) + 1000;
        return
        [
            new MetadataUpdate.AddSnapshot(new Snapshot
            {
                SnapshotId = id, ParentSnapshotId = t.Metadata.CurrentSnapshotId, SequenceNumber = t.Metadata.NextSequenceNumber,
                TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), ManifestList = manifestList, SchemaId = t.Metadata.CurrentSchemaId,
                Summary = new Dictionary<string, string> { ["operation"] = Operations.Append },
            }),
            new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, SnapshotRef.Branch(id)),
        ];
    }

    private static StatisticsFile Statistics(long snapshotId, string path) => new(snapshotId, path, 100, 10, []);

    /// <summary>Paths outside table <c>sales.mine</c>: another table's directory, another bucket, system files, escapes.</summary>
    private string[] Outside(Table mine, Table victim) =>
    [
        $"{victim.Location}/metadata/snap-1-1-victim.avro",
        "s3://another-bucket/wh/sales/mine/metadata/snap.avro",
        "file:///etc/passwd",
        "/etc/passwd",
        $"{mine.Location}/../orders/metadata/snap.avro",
        $"file://{mine.Location}/../orders/metadata/snap.avro",
        $"{mine.Location}/metadata/%2e%2e/%2e%2e/orders/metadata/snap.avro",
        $"{mine.Location}",
        $"{mine.Location}-other/snap.avro",
    ];

    private async Task<(Table Mine, Table Victim)> TwoTablesWithDataAsync()
    {
        var victim = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        await SnapshotProducer.Append(victim).AddFiles(await TableWriter.WriteAsync(victim, ScanPlanningTests.Rows(0, 4))).CommitAsync();
        var mine = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "mine"), ScanPlanningTests.Schema));
        await SnapshotProducer.Append(mine).AddFiles(await TableWriter.WriteAsync(mine, ScanPlanningTests.Rows(0, 2))).CommitAsync();
        return (await _catalog.LoadTableAsync(mine.Identifier), await _catalog.LoadTableAsync(victim.Identifier));
    }

    private async Task AssertCommitRefusedAsync(Table t, MetadataUpdate[] updates)
    {
        var e = await Assert.ThrowsAsync<HttpRequestException>(() =>
            _catalog.CommitTableAsync(new TableCommit(t.Identifier, [new UpdateRequirement.AssertTableUuid(t.Metadata.TableUuid)], updates)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Equal(t.MetadataLocation, (await _catalog.LoadTableAsync(t.Identifier)).MetadataLocation);
    }

    [Fact]
    public async Task A_snapshot_cannot_point_at_a_manifest_list_outside_its_table()
    {
        var (mine, victim) = await TwoTablesWithDataAsync();
        foreach (var path in Outside(mine, victim)) await AssertCommitRefusedAsync(mine, AddSnapshot(mine, path));
        // The victim's own manifest list, the most direct way to read its files through server-side planning.
        await AssertCommitRefusedAsync(mine, AddSnapshot(mine, victim.Metadata.CurrentSnapshot!.ManifestList!));
        // In the same transaction as a legitimate change to another table, nothing moves.
        var e = await Assert.ThrowsAsync<HttpRequestException>(() => _catalog.CommitTransactionAsync(
        [
            new TableCommit(victim.Identifier, [new UpdateRequirement.AssertTableUuid(victim.Metadata.TableUuid)],
                [new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["k"] = "v" })]),
            new TableCommit(mine.Identifier, [new UpdateRequirement.AssertTableUuid(mine.Metadata.TableUuid)],
                AddSnapshot(mine, victim.Metadata.CurrentSnapshot!.ManifestList!)),
        ]));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Equal(victim.MetadataLocation, (await _catalog.LoadTableAsync(victim.Identifier)).MetadataLocation);

        // Inside its own directories (its location or a write.*.path), a snapshot commits as before.
        await _catalog.CommitTableAsync(new TableCommit(mine.Identifier, [new UpdateRequirement.AssertTableUuid(mine.Metadata.TableUuid)],
            AddSnapshot(mine, $"{mine.Location}/metadata/snap-own.avro")));
        mine = await _catalog.LoadTableAsync(mine.Identifier);
        Assert.Equal($"{mine.Location}/metadata/snap-own.avro", mine.Metadata.CurrentSnapshot!.ManifestList);
        await SnapshotProducer.Append(victim).AddFiles(await TableWriter.WriteAsync(victim, ScanPlanningTests.Rows(4, 4))).CommitAsync();
        Assert.Equal(8, (await TableScan.ReadAllAsync(await _catalog.LoadTableAsync(victim.Identifier))).Count);
    }

    [Fact]
    public async Task Planning_refuses_manifests_outside_the_table_named_by_a_manifest_list_inside_it()
    {
        // The commit check sees only the manifest list's path. A copy of the victim's manifest list, written inside
        // mine, still names the victim's manifests; the planner reads those with the server's access and would hand back
        // the victim's data files and column bounds.
        var (mine, victim) = await TwoTablesWithDataAsync();
        var smuggled = $"{mine.Location}/metadata/snap-smuggled.avro";
        await mine.IO.WriteAsync(smuggled, await victim.IO.ReadAllAsync(victim.Metadata.CurrentSnapshot!.ManifestList!));
        await _catalog.CommitTableAsync(new TableCommit(mine.Identifier, [new UpdateRequirement.AssertTableUuid(mine.Metadata.TableUuid)],
            AddSnapshot(mine, smuggled)));
        mine = await _catalog.LoadTableAsync(mine.Identifier);

        var e = await Assert.ThrowsAsync<HttpRequestException>(() => _catalog.PlanTableScanAsync(mine, new ScanPlanRequest()));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.DoesNotContain(victim.Location, e.Message, StringComparison.Ordinal);

        // The victim plans as before, and so does mine at its own earlier snapshot.
        var plan = await _catalog.PlanTableScanAsync(victim, new ScanPlanRequest());
        Assert.NotEmpty(plan.Tasks);
        var own = await _catalog.PlanTableScanAsync(mine, new ScanPlanRequest(SnapshotId: mine.Metadata.CurrentSnapshot!.ParentSnapshotId));
        Assert.All(own.Tasks, t => Assert.StartsWith(mine.Location, t.DataFile.FilePath, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Planning_still_reads_a_tables_own_older_manifests_after_its_location_moves()
    {
        var (mine, _) = await TwoTablesWithDataAsync();
        var rows = (await TableScan.ReadAllAsync(mine)).Count;
        await _catalog.CommitTableAsync(new TableCommit(mine.Identifier, [new UpdateRequirement.AssertTableUuid(mine.Metadata.TableUuid)],
            [new MetadataUpdate.SetLocation(Wh("sales", "mine-moved"))]));
        mine = await _catalog.LoadTableAsync(mine.Identifier);
        Assert.Equal(Wh("sales", "mine-moved"), mine.Location);

        var plan = await _catalog.PlanTableScanAsync(mine, new ScanPlanRequest());
        Assert.Equal(rows, plan.Tasks.Sum(t => t.DataFile.RecordCount));
    }

    [Theory]
    [InlineData("t*")]
    [InlineData("t?")]
    [InlineData("$t")]
    public async Task A_staged_create_refuses_a_name_the_create_endpoint_refuses(string name)
    {
        var id = new TableIdentifier(Namespace.Of("sales"), name);
        var updates = new List<MetadataUpdate>
        {
            new MetadataUpdate.AssignUuid(Guid.NewGuid()), new MetadataUpdate.AddSchema(ScanPlanningTests.Schema),
            new MetadataUpdate.SetCurrentSchema(MetadataUpdate.LastAdded),
        };
        var e = await Assert.ThrowsAsync<HttpRequestException>(() =>
            _catalog.CommitTableAsync(new TableCommit(id, [new UpdateRequirement.AssertCreate()], updates)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Empty(await _catalog.ListTablesAsync(Namespace.Of("sales")));
    }

    [Fact]
    public async Task A_staged_create_cannot_carry_a_snapshot_outside_its_table()
    {
        var (mine, victim) = await TwoTablesWithDataAsync();
        var id = TableIdentifier.Of("sales", "staged");
        var updates = new List<MetadataUpdate>
        {
            new MetadataUpdate.AssignUuid(Guid.NewGuid()), new MetadataUpdate.AddSchema(ScanPlanningTests.Schema),
            new MetadataUpdate.SetCurrentSchema(MetadataUpdate.LastAdded),
        };
        var snapshot = AddSnapshot(mine, victim.Metadata.CurrentSnapshot!.ManifestList!);
        var e = await Assert.ThrowsAsync<HttpRequestException>(() =>
            _catalog.CommitTableAsync(new TableCommit(id, [new UpdateRequirement.AssertCreate()], [.. updates, .. snapshot])));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.False(await _catalog.TableExistsAsync(id));
    }

    [Fact]
    public async Task Statistics_cannot_point_outside_their_table()
    {
        var (mine, victim) = await TwoTablesWithDataAsync();
        var snapshot = mine.Metadata.CurrentSnapshotId!.Value;
        foreach (var path in Outside(mine, victim))
            await AssertCommitRefusedAsync(mine, [new MetadataUpdate.SetStatistics(Statistics(snapshot, path))]);
        // set-partition-statistics is not an update this server applies at all.
        var r = await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/tables/mine", new JsonObject
        {
            ["requirements"] = new JsonArray(),
            ["updates"] = new JsonArray(new JsonObject
            {
                ["action"] = "set-partition-statistics",
                ["partition-statistics"] = new JsonObject { ["snapshot-id"] = snapshot, ["statistics-path"] = "/etc/passwd", ["file-size-in-bytes"] = 1 },
            }),
        });
        Assert.Equal(HttpStatusCode.BadRequest, r.Status);
        Assert.Empty((await _catalog.LoadTableAsync(mine.Identifier)).Metadata.PartitionStatistics);

        var own = $"{mine.Location}/metadata/stats-own.puffin";
        await _catalog.CommitTableAsync(new TableCommit(mine.Identifier, [], [new MetadataUpdate.SetStatistics(Statistics(snapshot, own))]));
        Assert.Equal(own, (await _catalog.LoadTableAsync(mine.Identifier)).Metadata.Statistics.Single().StatisticsPath);
    }

    [Fact]
    public async Task Register_refuses_metadata_that_points_outside_the_table()
    {
        var (mine, _) = await TwoTablesWithDataAsync();
        var location = Wh("sales", "registered");
        async Task<(HttpStatusCode Status, JsonNode? Body)> RegisterAsync(string file, TableMetadata m)
        {
            var path = $"{location}/metadata/{file}";
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await File.WriteAllTextAsync(path, MetadataJson.Write(m with { Location = location, Properties = new Dictionary<string, string>() }));
            return await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/register", new JsonObject { ["name"] = "registered", ["metadata-location"] = path });
        }
        var bare = mine.Metadata with { Snapshots = [], Refs = new Dictionary<string, SnapshotRef>(), CurrentSnapshotId = null, SnapshotLog = [], MetadataLog = [] };
        // Another table's snapshots (manifest lists in its directory), or statistics anywhere else.
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", await RegisterAsync("00001-a.metadata.json", mine.Metadata));
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", await RegisterAsync("00001-b.metadata.json",
            bare with { PartitionStatistics = [new PartitionStatisticsFile(1, "/etc/passwd", 1)] }));
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", await RegisterAsync("00001-c.metadata.json",
            bare with { Statistics = [Statistics(1, $"{mine.Location}/metadata/stats.puffin")] }));
        Assert.False(await _catalog.TableExistsAsync(TableIdentifier.Of("sales", "registered")));
        Assert.Equal(HttpStatusCode.OK, (await RegisterAsync("00001-d.metadata.json", bare)).Status);
    }
}
