using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Json;
using Iceberg.Net.Manifests;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Server-side scan planning: the server plans with the library's planner and delete index, and the library's client
/// (<see cref="RestCatalog.PlanTableScanAsync"/> + <see cref="ServerSideScan"/>) must read exactly the rows a local scan
/// reads, under every delete mechanism, inline, split into plan tasks and asynchronously.
/// </summary>
[Spec("rest: planTableScan")]
[Spec("rest: fetchScanTasks")]
public sealed class ScanPlanningTests
{
    internal static readonly Schema Schema = new(
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "region", PrimitiveType.String));

    internal static readonly PartitionSpec Spec = PartitionSpec.BuilderFor(Schema).Identity("region").Build();

    private static readonly Namespace Ns = Namespace.Of("db");

    internal static List<object?[]> Rows(int start, int count) =>
        Enumerable.Range(start, count).Select(i => new object?[] { (long)i, $"n-{i}", i % 3 == 0 ? "eu" : i % 3 == 1 ? "us" : "apac" }).ToList();

    public static readonly Expression?[] Filters =
    [
        null,
        Expr.LessThan("id", 20L),
        Expr.Equal("region", "eu"),
        Expr.In("id", 1L, 5L, 9L, 22L, 31L, 40L),
        Expr.And(Expr.GreaterThanOrEqual("id", 12L), Expr.NotEqual("region", "us")),
    ];

    public static TheoryData<int, string> Cases => new()
    {
        { 2, "position" }, { 3, "position" }, { 2, "equality" }, { 3, "equality" },
    };

    /// <summary>A table written in four appends (four manifests), then deleted from with the given mechanism, twice.</summary>
    internal static async Task<Table> TableWithDeletesAsync(RestCatalog catalog, string name, int formatVersion, string deletes)
    {
        if (!await catalog.NamespaceExistsAsync(Ns)) await catalog.CreateNamespaceAsync(Ns);
        var t = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Ns, name), Schema, Spec, FormatVersion: formatVersion));
        for (var batch = 0; batch < 4; batch++)
            await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(batch * 12, 12))).CommitAsync();
        if (deletes == "position")
        {
            await TableMutations.DeleteWhereAsync(t, Expr.In("id", 3L, 4L, 17L, 30L), DeleteStrategy.PositionDeletes);
            await TableMutations.DeleteWhereAsync(t, Expr.Equal("id", 5L), DeleteStrategy.PositionDeletes);
        }
        else
        {
            await TableMutations.DeleteByKeysAsync(t, ["id", "region"], [[3L, "eu"], [4L, "us"], [17L, "apac"]], DeleteStrategy.EqualityDeletes);
            await TableMutations.DeleteByKeysAsync(t, ["id", "region"], [[30L, "eu"]], DeleteStrategy.EqualityDeletes);
        }
        return t;
    }

    private static List<long> Ids(IEnumerable<object?[]> rows) => rows.Select(r => (long)r[0]!).Order().ToList();

    private static async Task AssertSameRowsAsync(RestCatalog catalog, Table table, Expression? filter, long? snapshotId = null)
    {
        var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest(filter, snapshotId));
        var theirs = await ServerSideScan.ReadAsync(table, plan, filter);
        var ours = await TableScan.ReadAsync(table, filter ?? Expression.True, snapshotId);
        Assert.Equal(Ids(ours), Ids(theirs));
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Server_plans_read_the_same_rows_as_local_scans(int formatVersion, string deletes)
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        Assert.True(catalog.SupportsScanPlanning);
        var table = await TableWithDeletesAsync(catalog, $"t{formatVersion}{deletes}", formatVersion, deletes);
        foreach (var filter in Filters) await AssertSameRowsAsync(catalog, table, filter);

        var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest());
        Assert.Equal(0, plan.PlanTasksFetched);
        Assert.Contains(plan.Tasks, t => t.Deletes.Count > 0);
        if (deletes == "equality") Assert.All(plan.Tasks.SelectMany(t => t.Deletes), d => Assert.Equal(FileContent.EqualityDeletes, d.Content));
        if (deletes == "position" && formatVersion == 3)
            Assert.All(plan.Tasks.SelectMany(t => t.Deletes), d => Assert.Equal("PUFFIN", d.FileFormat));
        // A DV is only referenced by its own data file. (v2 position delete files are referenced by every data file of
        // their partition whose path is within their file_path bounds; ours truncate those bounds, so no file is excluded.)
        if (deletes == "position" && formatVersion == 3)
        {
            Assert.Contains(plan.Tasks, t => t.Deletes.Count == 0);
            Assert.All(plan.Tasks, t => Assert.All(t.Deletes, d => Assert.Equal(t.DataFile.FilePath, d.ReferencedDataFile)));
        }
        if (deletes == "position" && formatVersion == 2)
            Assert.All(plan.Tasks, t => Assert.All(t.Deletes, d => Assert.Equal(t.DataFile.Partition, d.Partition)));
        // Partition pruning happened on the server.
        var eu = await catalog.PlanTableScanAsync(table, new ScanPlanRequest(Expr.Equal("region", "eu")));
        Assert.True(eu.Tasks.Count < plan.Tasks.Count);
        Assert.All(eu.Tasks, t => Assert.Equal("eu", t.DataFile.Partition[0]));
    }

    [Fact]
    public async Task Big_scans_are_split_into_plan_tasks()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["ScanPlanning:FilesPerPlanTask"] = "2" });
        using var catalog = await host.ConnectAsync();
        var table = await TableWithDeletesAsync(catalog, "big", 2, "position");
        foreach (var filter in Filters) await AssertSameRowsAsync(catalog, table, filter);
        var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest());
        Assert.True(plan.PlanTasksFetched >= 2, $"fetched {plan.PlanTasksFetched}");
        Assert.Contains(plan.Tasks, t => t.Deletes.Count > 0);
        // Every data file comes back exactly once.
        Assert.Equal((await TableScan.PlanFilesAsync(table)).Select(t => t.File.FilePath).Order(), plan.Tasks.Select(t => t.DataFile.FilePath).Order());
    }

    [Fact]
    [Spec("rest: fetchPlanningResult")]
    public async Task Asynchronous_plans_are_polled_to_completion()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["ScanPlanning:Async"] = "true" });
        using var catalog = await host.ConnectAsync();
        var table = await TableWithDeletesAsync(catalog, "async", 3, "position");
        var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest(Expr.LessThan("id", 30L)));
        Assert.True(plan.Polls >= 1);
        Assert.Equal(Ids(await TableScan.ReadAsync(table, Expr.LessThan("id", 30L))), Ids(await ServerSideScan.ReadAsync(table, plan, Expr.LessThan("id", 30L))));
    }

    [Fact]
    public async Task Time_travel_plans_the_requested_snapshot()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        var table = await TableWithDeletesAsync(catalog, "travel", 2, "equality");
        var history = table.Metadata.Snapshots.Select(s => s.SnapshotId).ToList();
        foreach (var snapshotId in history) await AssertSameRowsAsync(catalog, table, Expr.LessThan("id", 40L), snapshotId);
    }

    [Fact]
    [Spec("rest: fetchPlanningResult")]
    [Spec("rest: cancelPlanning")]
    [Spec("rest: getConfig")]
    public async Task Protocol_details()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["ScanPlanning:FilesPerPlanTask"] = "1" });
        using var catalog = await host.ConnectAsync();
        var table = await TableWithDeletesAsync(catalog, "proto", 2, "position");
        await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Ns, "empty"), Schema, Spec));
        using var http = host.Http();
        const string path = "/v1/namespaces/db/tables/proto";

        var (_, config) = await ProtocolTests.SendAsync(http, HttpMethod.Get, "/v1/config");
        var endpoints = config!["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()).ToList();
        Assert.Subset(endpoints.ToHashSet(), Endpoints.Planning.ToHashSet());

        // A plan split into tasks: completed, with a plan ID that fetchPlanningResult answers and cancelPlanning forgets.
        var (status, planned) = await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/plan", new JsonObject());
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("completed", planned!["status"]!.GetValue<string>());
        var planId = planned["plan-id"]!.GetValue<string>();
        var planTasks = planned["plan-tasks"]!.AsArray();
        Assert.Equal(4, planTasks.Count); // one per data manifest
        var (_, fetched) = await ProtocolTests.SendAsync(http, HttpMethod.Get, $"{path}/plan/{planId}");
        Assert.Equal("completed", fetched!["status"]!.GetValue<string>());
        Assert.Equal(4, fetched["plan-tasks"]!.AsArray().Count);
        Assert.Equal(HttpStatusCode.NoContent, (await ProtocolTests.SendAsync(http, HttpMethod.Delete, $"{path}/plan/{planId}")).Status);
        ProtocolTests.AssertError(HttpStatusCode.NotFound, "NoSuchPlanIdException", await ProtocolTests.SendAsync(http, HttpMethod.Get, $"{path}/plan/{planId}"));
        ProtocolTests.AssertError(HttpStatusCode.NotFound, "NoSuchPlanIdException", await ProtocolTests.SendAsync(http, HttpMethod.Get, $"{path}/plan/nope"));

        // A plan task: tasks with data files trimmed to no stats, delete files with theirs and every reference in range.
        var (_, tasks) = await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/tasks", new JsonObject { ["plan-task"] = planTasks[0]!.GetValue<string>() });
        var fileTasks = tasks!["file-scan-tasks"]!.AsArray();
        Assert.NotEmpty(fileTasks);
        var deleteFiles = tasks["delete-files"]!.AsArray();
        foreach (var task in fileTasks)
        {
            var dataFile = task!["data-file"]!;
            Assert.Equal("data", dataFile["content"]!.GetValue<string>());
            Assert.Equal("parquet", dataFile["file-format"]!.GetValue<string>());
            Assert.Null(dataFile["lower-bounds"]);
            Assert.True(task["residual-filter"]!.GetValue<bool>());
            foreach (var r in task["delete-file-references"]?.AsArray() ?? []) Assert.InRange(r!.GetValue<int>(), 0, deleteFiles.Count - 1);
        }
        Assert.All(deleteFiles, d => Assert.Equal("position-deletes", d!["content"]!.GetValue<string>()));

        // The same plan task cannot be fetched through another table, and garbage is not a plan task.
        ProtocolTests.AssertError(HttpStatusCode.NotFound, "NoSuchPlanTaskException",
            await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/empty/tasks", new JsonObject { ["plan-task"] = planTasks[0]!.GetValue<string>() }));
        ProtocolTests.AssertError(HttpStatusCode.NotFound, "NoSuchPlanTaskException",
            await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/tasks", new JsonObject { ["plan-task"] = "not-a-task" }));

        // stats-fields keeps just those metrics; the filter comes back as each task's residual.
        var filter = ExpressionJson.ToJson(Expr.LessThan("id", 10L));
        var (_, withStats) = await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/tasks",
            new JsonObject { ["plan-task"] = (await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/plan",
                new JsonObject { ["filter"] = filter.DeepClone(), ["stats-fields"] = new JsonArray("id") })).Body!["plan-tasks"]![0]!.GetValue<string>() });
        foreach (var task in withStats!["file-scan-tasks"]!.AsArray())
        {
            Assert.Equal([1], task!["data-file"]!["lower-bounds"]!["keys"]!.AsArray().Select(k => k!.GetValue<int>()));
            Assert.True(JsonNode.DeepEquals(filter, task["residual-filter"]));
        }

        // Case-insensitive names resolve; unknown names, unknown snapshots and incremental scans do not.
        var (insensitive, _) = await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/plan",
            new JsonObject { ["filter"] = ExpressionJson.ToJson(Expr.Equal("REGION", "eu")), ["case-sensitive"] = false, ["select"] = new JsonArray("ID") });
        Assert.Equal(HttpStatusCode.OK, insensitive);
        ProtocolTests.AssertError(HttpStatusCode.BadRequest, "BadRequestException", await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/plan",
            new JsonObject { ["filter"] = ExpressionJson.ToJson(Expr.Equal("REGION", "eu")) }));
        ProtocolTests.AssertError(HttpStatusCode.BadRequest, "BadRequestException", await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/plan",
            new JsonObject { ["snapshot-id"] = 42 }));
        ProtocolTests.AssertError(HttpStatusCode.NotAcceptable, "UnsupportedOperationException", await ProtocolTests.SendAsync(http, HttpMethod.Post, $"{path}/plan",
            new JsonObject { ["start-snapshot-id"] = table.Metadata.Snapshots[0].SnapshotId, ["end-snapshot-id"] = table.Metadata.CurrentSnapshotId }));
        ProtocolTests.AssertError(HttpStatusCode.NotFound, "NoSuchTableException", await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/missing/plan", new JsonObject()));

        // A table without snapshots plans to nothing.
        var (_, empty) = await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/empty/plan", new JsonObject());
        Assert.Equal("completed", empty!["status"]!.GetValue<string>());
        Assert.Empty(empty["file-scan-tasks"]!.AsArray());
    }

    [Fact]
    public async Task Planning_can_be_switched_off_and_advertised_to_java_clients()
    {
        await using (var off = ServerHost.Start(settings: new Dictionary<string, string> { ["ScanPlanning:Enabled"] = "false" }))
        {
            using var catalog = await off.ConnectAsync();
            Assert.False(catalog.SupportsScanPlanning);
            using var http = off.Http();
            Assert.Equal(HttpStatusCode.NotFound, (await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/t/plan", new JsonObject())).Status);
        }

        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["ScanPlanning:Mode"] = "server" });
        using var client = await host.ConnectAsync();
        await client.CreateNamespaceAsync(Ns);
        await client.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Ns, "t"), Schema, Spec));
        using var raw = host.Http();
        var (_, loaded) = await ProtocolTests.SendAsync(raw, HttpMethod.Get, "/v1/namespaces/db/tables/t");
        Assert.Equal("server", loaded!["config"]!["scan-planning-mode"]!.GetValue<string>());
    }
}
