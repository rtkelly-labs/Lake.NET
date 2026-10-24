using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Incremental scan planning (<c>start-snapshot-id</c> exclusive, <c>end-snapshot-id</c> inclusive) with Java's
/// <c>IncrementalAppendScan</c> semantics: the files the <c>append</c> snapshots in the range added, and nothing from
/// overwrites or deletes. The oracle is the library's change feed (an independent code path): the INSERT rows of the
/// append snapshots in the range.
/// </summary>
[Spec("rest: planTableScan")]
public sealed class IncrementalScanPlanningTests
{
    private static readonly Namespace Ns = Namespace.Of("db");

    /// <summary>append, append, position delete, append, copy-on-write delete, append (v2) — six snapshots.</summary>
    private static async Task<Table> MixedHistoryAsync(RestCatalog catalog, string name)
    {
        if (!await catalog.NamespaceExistsAsync(Ns)) await catalog.CreateNamespaceAsync(Ns);
        var t = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Ns, name), ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 12))).CommitAsync();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(12, 12))).CommitAsync();
        await TableMutations.DeleteWhereAsync(t, Expr.In("id", 3L, 14L), DeleteStrategy.PositionDeletes);
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(24, 12))).CommitAsync();
        await TableMutations.DeleteWhereAsync(t, Expr.Equal("id", 25L), DeleteStrategy.CopyOnWrite);
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(36, 12))).CommitAsync();
        return await catalog.LoadTableAsync(t.Identifier);
    }

    /// <summary>The change feed's INSERT rows of the append snapshots after <paramref name="from"/> up to <paramref name="to"/>.</summary>
    private static async Task<List<long>> AppendedIdsAsync(Table table, long? from, long to, Func<object?[], bool>? where = null)
    {
        var ids = new List<long>();
        await foreach (var change in ChangeFeed.ChangesAsync(table, from, to))
            if (change.ChangeType == ChangeType.Insert && table.Metadata.SnapshotById(change.CommitSnapshotId)!.Operation == Operations.Append
                && (where is null || where(change.Row)))
                ids.Add((long)change.Row[0]!);
        return ids.Order().ToList();
    }

    private static List<long> Ids(IEnumerable<object?[]> rows) => rows.Select(r => (long)r[0]!).Order().ToList();

    [Theory]
    [InlineData(100)]
    [InlineData(1)] // one manifest per plan task: the incremental plan is answered through fetchScanTasks
    public async Task Incremental_plans_return_the_appended_files_of_the_range(int filesPerPlanTask)
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["ScanPlanning:FilesPerPlanTask"] = filesPerPlanTask.ToString() });
        using var catalog = await host.ConnectAsync();
        var table = await MixedHistoryAsync(catalog, $"mixed{filesPerPlanTask}");
        var history = table.Metadata.Ancestors(table.Metadata.CurrentSnapshotId).Reverse().Select(s => s.SnapshotId).ToList();
        Assert.Equal(6, history.Count);
        Assert.Equal(4, table.Metadata.Snapshots.Count(s => s.Operation == Operations.Append));

        var fetched = 0;
        for (var to = 0; to < history.Count; to++)
        {
            for (var from = -1; from < to; from++)
            {
                long? start = from < 0 ? null : history[from];
                var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest { StartSnapshotId = start, EndSnapshotId = history[to] });
                fetched += plan.PlanTasksFetched;
                Assert.All(plan.Tasks, t => Assert.Empty(t.Deletes));
                // Deletes are not applied: an append scan returns what was appended, even rows deleted later.
                Assert.Equal(await AppendedIdsAsync(table, start, history[to]), Ids(await ServerSideScan.ReadAsync(table, plan)));
                Assert.Equal((await TableScan.PlanAppendsAsync(table, start, history[to])).Tasks.Select(t => t.File.FilePath).Order(),
                    plan.Tasks.Select(t => t.DataFile.FilePath).Order());
            }
        }
        if (filesPerPlanTask == 1) Assert.True(fetched > 0, "no plan tasks were fetched");

        // A filter prunes on the server and comes back as each task's residual.
        var filter = Expr.And(Expr.Equal("region", "eu"), Expr.GreaterThanOrEqual("id", 10L));
        var filtered = await catalog.PlanTableScanAsync(table, new ScanPlanRequest(filter) { StartSnapshotId = history[0], EndSnapshotId = history[^1] });
        Assert.All(filtered.Tasks, t => Assert.Equal("eu", t.DataFile.Partition[0]));
        Assert.Equal(await AppendedIdsAsync(table, history[0], history[^1], r => (string)r[2]! == "eu" && (long)r[0]! >= 10),
            Ids(await ServerSideScan.ReadAsync(table, filtered, filter)));
    }

    [Fact]
    public async Task Incremental_requests_are_validated()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        var table = await MixedHistoryAsync(catalog, "checked");
        var history = table.Metadata.Ancestors(table.Metadata.CurrentSnapshotId).Reverse().Select(s => s.SnapshotId).ToList();
        using var http = host.Http();
        const string path = "/v1/namespaces/db/tables/checked/plan";

        // Start without end, start after end, unknown snapshots, and snapshot-id as well are 400s.
        foreach (var body in new[]
        {
            new JsonObject { ["start-snapshot-id"] = history[0] },
            new JsonObject { ["start-snapshot-id"] = history[3], ["end-snapshot-id"] = history[1] },
            new JsonObject { ["start-snapshot-id"] = 42, ["end-snapshot-id"] = history[1] },
            new JsonObject { ["start-snapshot-id"] = history[0], ["end-snapshot-id"] = 42 },
            new JsonObject { ["snapshot-id"] = history[1], ["start-snapshot-id"] = history[0], ["end-snapshot-id"] = history[2] },
        })
            ProtocolTests.AssertError(HttpStatusCode.BadRequest, "BadRequestException", await ProtocolTests.SendAsync(http, HttpMethod.Post, path, body));

        // The same start and end: nothing. A range of only deletes: nothing either.
        var (_, same) = await ProtocolTests.SendAsync(http, HttpMethod.Post, path, new JsonObject { ["start-snapshot-id"] = history[2], ["end-snapshot-id"] = history[2] });
        Assert.Empty(same!["file-scan-tasks"]!.AsArray());
        var (_, deletesOnly) = await ProtocolTests.SendAsync(http, HttpMethod.Post, path, new JsonObject { ["start-snapshot-id"] = history[1], ["end-snapshot-id"] = history[2] });
        Assert.Empty(deletesOnly!["file-scan-tasks"]!.AsArray());
        Assert.Empty(deletesOnly["delete-files"]!.AsArray());

        // An incremental plan gets a plan ID like any other.
        var (_, planned) = await ProtocolTests.SendAsync(http, HttpMethod.Post, path,
            new JsonObject { ["start-snapshot-id"] = history[0], ["end-snapshot-id"] = history[^1], ["filter"] = ExpressionJson.ToJson(Expr.LessThan("id", 30L)) });
        Assert.Equal("completed", planned!["status"]!.GetValue<string>());
        var (_, again) = await ProtocolTests.SendAsync(http, HttpMethod.Get, $"{path}/{planned["plan-id"]!.GetValue<string>()}");
        Assert.Equal(planned["file-scan-tasks"]!.AsArray().Count, again!["file-scan-tasks"]!.AsArray().Count);
    }
}
