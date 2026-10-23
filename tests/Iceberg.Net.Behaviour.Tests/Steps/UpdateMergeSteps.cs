using Reqnroll;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Xunit;

namespace Iceberg.Net.Behaviour.Tests.Steps;

/// <summary>Per-scenario state for UPDATE and MERGE (context injection: fresh per scenario).</summary>
public sealed class ChangeWorld
{
    public MergeResult? Result { get; set; }
    public Dictionary<long, long?> RowIds { get; set; } = [];
}

/// <summary>Steps for Features/UpdateMerge.feature; the table, row and PyIceberg steps are shared with <see cref="IcebergSteps"/>.</summary>
[Binding]
public sealed class UpdateMergeSteps(IcebergWorld world, ChangeWorld change)
{
    private static List<object?[]> RowsOf(DataTable table) => table.Rows.Select(r => new object?[]
    {
        long.Parse(r["id"], System.Globalization.CultureInfo.InvariantCulture),
        r["name"] is "" or "null" ? null : r["name"],
        r["region"],
    }).ToList();

    private async Task Attempt(Func<Task> action)
    {
        try { await action(); }
        catch (Exception e) when (e is ArgumentException or ValidationException or NotSupportedException) { world.Error = e; }
    }

    [Given(@"^I note every row's row id$")]
    public async Task GivenRowIds() =>
        change.RowIds = (await TableScan.ReadWithLineageAsync(await world.Catalog.LoadTableAsync(world.T.Identifier))).ToDictionary(r => (long)r.Row[0]!, r => r.RowId);

    [When(@"^I set (name|region) to ""(.*)"" where id is (\d+) with (.*)$")]
    public Task WhenUpdate(string column, string value, long id, DeleteStrategy strategy) =>
        Attempt(async () => change.Result = await TableMutations.UpdateAsync(world.T, Expr.Equal("id", id), u => u.Set(column, value), strategy));

    [When(@"^I merge the feed with (.*), updating matches and inserting new rows:$")]
    public Task WhenMerge(DeleteStrategy strategy, DataTable rows) =>
        Attempt(async () => change.Result = await TableMutations.MergeAsync(world.T, RowsOf(rows), ["id"],
            m => m.WhenMatchedUpdate((target, source) => source).WhenNotMatchedInsert(), strategy));

    [When(@"^I merge the ""(.*)"" feed with (.*), updating matches, inserting new rows and deleting unlisted rows:$")]
    public Task WhenMergeRegion(string region, DeleteStrategy strategy, DataTable rows) =>
        Attempt(async () => change.Result = await TableMutations.MergeAsync(world.T, RowsOf(rows), ["id"],
            m => m.WhenMatchedUpdate((target, source) => source).WhenNotMatchedInsert().WhenNotMatchedBySourceDelete(Expr.Equal("region", region)), strategy));

    [Then(@"^the change reports (\d+) updated, (\d+) deleted and (\d+) inserted$")]
    public void ThenCounts(long updated, long deleted, long inserted)
    {
        Assert.Null(world.Error);
        Assert.Equal((updated, deleted, inserted), (change.Result!.RowsUpdated, change.Result.RowsDeleted, change.Result.RowsInserted));
    }

    [Then(@"^no snapshot was committed$")]
    public async Task ThenNoSnapshot()
    {
        Assert.Null(world.Error);
        Assert.Null(change.Result!.Snapshot);
        Assert.Equal(world.T.Metadata.CurrentSnapshotId, (await world.Catalog.LoadTableAsync(world.T.Identifier)).Metadata.CurrentSnapshotId);
    }

    [Then(@"^the table still holds the default rows and nothing new was written$")]
    public async Task ThenUnchanged()
    {
        var table = await world.Catalog.LoadTableAsync(world.T.Identifier);
        Assert.Equal(world.T.Metadata.CurrentSnapshotId, table.Metadata.CurrentSnapshotId);
        static string Describe(object?[] r) => $"{r[0]}|{r[1] ?? "null"}|{r[2]}";
        Assert.Equal(world.DefaultRows.Select(Describe).Order(), (await TableScan.ReadAllAsync(table)).Select(Describe).Order());
    }

    [Then(@"^the row with id (\d+) is stored in the ""(.*)"" partition$")]
    public async Task ThenPartition(long id, string region)
    {
        var table = await world.Catalog.LoadTableAsync(world.T.Identifier);
        var rows = await TableScan.ReadAsync(table, Expr.Equal("region", region)); // pruned to that partition's files
        Assert.Contains(rows, r => (long)r[0]! == id);
        Assert.All(await TableScan.PlanFilesAsync(table, Expr.Equal("region", region)), t => Assert.Equal(region, t.File.Partition[0]));
    }

    [Then(@"^rows (\d+(?:, \d+)*) and (\d+) keep their row ids$")]
    public async Task ThenKeepRowIds(string ids, long last)
    {
        Assert.Null(world.Error);
        var now = (await TableScan.ReadWithLineageAsync(await world.Catalog.LoadTableAsync(world.T.Identifier))).ToDictionary(r => (long)r.Row[0]!, r => r.RowId);
        foreach (var id in ids.Split(", ").Select(long.Parse).Append(last))
        {
            Assert.NotNull(now[id]);
            Assert.Equal(change.RowIds[id], now[id]);
        }
    }

    [Then(@"^row (\d+) has a new row id$")]
    public async Task ThenNewRowId(long id)
    {
        var now = (await TableScan.ReadWithLineageAsync(await world.Catalog.LoadTableAsync(world.T.Identifier))).ToDictionary(r => (long)r.Row[0]!, r => r.RowId);
        Assert.NotNull(now[id]);
        Assert.DoesNotContain(now[id], change.RowIds.Values);
    }
}
