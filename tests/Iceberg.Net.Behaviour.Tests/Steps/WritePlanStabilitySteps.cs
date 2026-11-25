using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;
using Reqnroll;
using Xunit;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.Behaviour.Tests.Steps;

[Binding]
public sealed class WritePlanStabilitySteps : IDisposable
{
    private string? _warehouse;
    private ScheduledCommitCatalog? _catalog;
    private Table? _table;
    private Exception? _error;
    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Require(2, "value", PrimitiveType.Long)], [1]);

    [Given(@"^a format v(\d) stability table$")]
    public async Task GivenTable(int version)
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Iceberg.Net.slnx"))) root = root.Parent ?? throw new InvalidOperationException("Repository root missing");
        _warehouse = Path.Combine(root.FullName, "temp", "write-plan-bdd", Guid.NewGuid().ToString("N"));
        _catalog = new ScheduledCommitCatalog(_warehouse);
        await _catalog.CreateNamespaceAsync(Namespace.Of("stability"));
        _table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("stability", "events"), Schema,
            Properties: new Dictionary<string, string> { ["commit.retry.min-wait-ms"] = "0" }, FormatVersion: version));
        await TableMutations.InsertAsync(_table, [[1L, 100L]]);
    }

    [When(@"^a schema reorder and competing upsert land during my (upsert|staged|update)$")]
    public async Task WhenSchemaChanges(string mode)
    {
        var other = await _catalog!.LoadTableAsync(_table!.Identifier);
        _catalog.BeforeNextSwap = async () =>
        {
            await other.UpdateSchemaAsync(new Schema(1, [Schema.Columns[1], Schema.Columns[0]], [1]));
            await TableMutations.UpsertAsync(other, [[300L, 1L]], ["id"]);
        };
        _error = await Record.ExceptionAsync(async () =>
        {
            if (mode == "upsert") await TableMutations.UpsertAsync(_table, [[1L, 200L]], ["id"]);
            else if (mode == "update") await TableMutations.UpdateAsync(_table, Expr.Equal("id", 1L), update => update.Set("value", 200L));
            else
            {
                var stage = await TableMutations.StageMergeAsync(_table, [[1L, 200L]], ["id"], merge => merge.WhenMatchedUpdate((_, source) => source).WhenNotMatchedInsert());
                await StagedCommit.CommitAllAsync([stage]);
            }
        });
    }

    [When("a prepared append races a schema-only reorder")]
    public async Task WhenOnlySchemaChanges()
    {
        var producer = SnapshotProducer.Append(_table!).AddFiles(await TableWriter.WriteAsync(_table!, [[2L, 200L]]));
        var other = await _catalog!.LoadTableAsync(_table!.Identifier);
        await other.UpdateSchemaAsync(new Schema(1, [Schema.Columns[1], Schema.Columns[0]], [1]));
        _error = await Record.ExceptionAsync(() => producer.CommitAsync());
    }

    [Then("the prepared stability append is rejected")]
    public async Task ThenAppendRejected()
    {
        Assert.IsType<ValidationException>(_error);
        var fresh = await _catalog!.LoadTableAsync(_table!.Identifier);
        Assert.Equal(new object?[] { 100L, 1L }, Assert.Single(await TableScan.ReadAllAsync(fresh)));
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Then("PyIceberg reads only the unchanged stability input")]
    public async Task ThenUnchanged()
    {
        var fresh = await _catalog!.LoadTableAsync(_table!.Identifier);
        var result = await TestEnvironment.Current.OracleAsync("read-metadata", fresh.MetadataLocation!);
        var row = Assert.Single(result["rows"]!.AsArray());
        Assert.Equal(1L, row!["id"]!.GetValue<long>());
        Assert.Equal(100L, row["value"]!.GetValue<long>());
    }

    [Then("the old stability plan is rejected without replay")]
    public async Task ThenRejected()
    {
        Assert.IsType<ValidationException>(_error);
        var fresh = await _catalog!.LoadTableAsync(_table!.Identifier);
        var row = Assert.Single(await TableScan.ReadAllAsync(fresh));
        Assert.Equal(new object?[] { 300L, 1L }, row);
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Then("PyIceberg reads only the concurrent stability winner")]
    public async Task ThenWinner()
    {
        var fresh = await _catalog!.LoadTableAsync(_table!.Identifier);
        var result = await TestEnvironment.Current.OracleAsync("read-metadata", fresh.MetadataLocation!);
        var row = Assert.Single(result["rows"]!.AsArray());
        Assert.Equal(1L, row!["id"]!.GetValue<long>());
        Assert.Equal(300L, row["value"]!.GetValue<long>());
    }

    [When("a prepared append races a partition spec change")]
    public async Task WhenSpecChanges()
    {
        var files = await TableWriter.WriteAsync(_table!, [[2L, 200L]]);
        var producer = SnapshotProducer.Append(_table!).AddFiles(files);
        var other = await _catalog!.LoadTableAsync(_table!.Identifier);
        await other.UpdateSpecAsync(spec => spec.AddField("id", new BucketTransform(4)));
        await producer.CommitAsync();
        Assert.All(files, file => Assert.Equal(0, file.SpecId));
    }

    [Then("PyIceberg reads both stability rows")]
    public async Task ThenBothRows()
    {
        var fresh = await _catalog!.LoadTableAsync(_table!.Identifier);
        var result = await TestEnvironment.Current.OracleAsync("read-metadata", fresh.MetadataLocation!);
        Assert.Equal(new long[] { 1, 2 }, result["rows"]!.AsArray().Select(row => row!["id"]!.GetValue<long>()).Order());
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    public void Dispose()
    {
        _catalog?.Dispose();
        if (_warehouse is not null && Directory.Exists(_warehouse)) Directory.Delete(_warehouse, true);
    }
}
