using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

[Spec("Commit Conflict Resolution and Retry")]
[Spec("Schema Evolution")]
public sealed class WritePlanGuardTests : IDisposable
{
    private readonly string _warehouse;
    private readonly ScheduledCommitCatalog _catalog;
    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long)], [1]);

    public WritePlanGuardTests()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Iceberg.Net.slnx"))) root = root.Parent ?? throw new InvalidOperationException("Repository root missing");
        _warehouse = Path.Combine(root.FullName, "temp", "write-plan", Guid.NewGuid().ToString("N"));
        _catalog = new ScheduledCommitCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("guard")).GetAwaiter().GetResult();
    }

    private async Task<Table> TableAsync()
    {
        var table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("guard", "events"), Schema,
            Properties: new Dictionary<string, string> { ["commit.retry.min-wait-ms"] = "0" }));
        await TableMutations.InsertAsync(table, [[1L]]);
        return table;
    }

    [Theory]
    [InlineData("producer")]
    [InlineData("mutation")]
    [InlineData("staged")]
    [InlineData("refs")]
    [InlineData("expiry")]
    public async Task A_command_never_follows_a_dropped_table_into_its_replacement(string mode)
    {
        var table = await TableAsync();
        var uuid = table.Metadata.TableUuid;
        var producer = SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[2L]]));
        var staged = await StagedCommit.StageAsync(table, async ct => SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[2L]], ct)));
        async Task ReplaceAsync()
        {
            await _catalog.DropTableAsync(table.Identifier);
            var replacement = await _catalog.CreateTableAsync(new CreateTableRequest(table.Identifier, Schema));
            await TableMutations.InsertAsync(replacement, [[9L]]);
        }
        if (mode is "mutation" or "refs" or "expiry") _catalog.BeforeNextSwap = ReplaceAsync;
        else await ReplaceAsync();
        if (mode == "producer") await Assert.ThrowsAsync<ValidationException>(() => producer.CommitAsync());
        else if (mode == "staged") await Assert.ThrowsAsync<ValidationException>(() => StagedCommit.CommitAllAsync([staged]));
        else if (mode == "refs") await Assert.ThrowsAsync<ValidationException>(() => table.ManageSnapshots().CreateBranch("audit").CommitAsync());
        else if (mode == "expiry")
        {
            // Give expiry an old snapshot to remove, then schedule replacement only at its commit boundary.
            _catalog.BeforeNextSwap = null;
            await TableMutations.InsertAsync(table, [[3L]]);
            _catalog.BeforeNextSwap = ReplaceAsync;
            await Assert.ThrowsAsync<ValidationException>(() => Maintenance.ExpireAsync(table, long.MaxValue, retainLast: 1));
        }
        else await Assert.ThrowsAsync<ValidationException>(() => TableMutations.UpsertAsync(table, [[2L]]));
        var fresh = await _catalog.LoadTableAsync(table.Identifier);
        Assert.NotEqual(uuid, fresh.Metadata.TableUuid);
        Assert.Equal(9L, Assert.Single(await TableScan.ReadAllAsync(fresh))[0]);
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    [Spec("Partition Evolution")]
    public async Task A_file_filter_cannot_be_reinterpreted_under_a_new_default_spec()
    {
        var table = await TableAsync();
        var producer = SnapshotProducer.DeleteFiles(table, _ => true);
        var other = await _catalog.LoadTableAsync(table.Identifier);
        await other.UpdateSpecAsync(spec => spec.AddField("id", Transform.Bucket(4)));
        await Assert.ThrowsAsync<ValidationException>(() => producer.CommitAsync());
        var fresh = await _catalog.LoadTableAsync(table.Identifier);
        Assert.Single(await TableScan.ReadAllAsync(fresh));
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    public async Task Refreshing_a_handle_does_not_rebind_a_prepared_producer()
    {
        var table = await TableAsync();
        var producer = SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[2L]]));
        var other = await _catalog.LoadTableAsync(table.Identifier);
        await other.UpdateSchemaAsync(schema => schema.RenameColumn("id", "renamed"));
        await table.RefreshAsync();
        await Assert.ThrowsAsync<ValidationException>(() => producer.CommitAsync());
        Assert.Single(await TableScan.ReadAllAsync(table));
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, true);
    }
}
