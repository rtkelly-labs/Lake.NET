using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

[Spec("Commit Conflict Resolution and Retry")]
[Spec("Metastore Tables")]
public sealed class CommitRecoveryTests : IDisposable
{
    private readonly string _warehouse;
    public CommitRecoveryTests()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Iceberg.Net.slnx"))) root = root.Parent ?? throw new InvalidOperationException("Repository root missing");
        _warehouse = Path.Combine(root.FullName, "temp", "commit-recovery", Guid.NewGuid().ToString("N"));
    }

    private static async Task<Table> CreateAsync(UncertainCommitCatalog catalog, string name = "events", Dictionary<string, string>? extra = null)
    {
        if (!await catalog.NamespaceExistsAsync(Namespace.Of("recovery"))) await catalog.CreateNamespaceAsync(Namespace.Of("recovery"));
        var properties = new Dictionary<string, string>
        {
            ["commit.status-check.num-retries"] = "2",
            ["commit.status-check.min-wait-ms"] = "0",
            ["commit.status-check.max-wait-ms"] = "0",
        };
        foreach (var item in extra ?? []) properties[item.Key] = item.Value;
        var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("recovery", name),
            new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long)]), Properties: properties));
        await TableMutations.InsertAsync(table, [[0L]]);
        return table;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_transient_status_read_failure_retries_only_the_read(bool serverResponded)
    {
        using var catalog = new UncertainCommitCatalog(_warehouse) { ServerResponded = serverResponded, ReadFailuresAfterError = 1 };
        var table = await CreateAsync(catalog);
        catalog.Armed = true;
        var result = await TableMutations.InsertAsync(table, [[1L]]);
        Assert.Equal(1, result.Attempts);
        Assert.Equal(2, catalog.RecoveryReads);
        Assert.Equal(2, table.Metadata.Snapshots.Count);
        Assert.Equal(new long[] { 0, 1 }, (await TableScan.ReadAllAsync(table)).Select(row => (long)row[0]!).Order());
    }

    [Theory]
    [InlineData(false, 3)]
    [InlineData(true, 1)]
    public async Task Exhausted_status_count_or_elapsed_budget_preserves_uncertainty(bool zeroTimeout, int expectedReads)
    {
        using var catalog = new UncertainCommitCatalog(_warehouse) { ReadFailuresAfterError = 20 };
        var table = await CreateAsync(catalog, extra: new Dictionary<string, string>
        {
            ["commit.status-check.total-timeout-ms"] = zeroTimeout ? "0" : "1800000",
        });
        catalog.Armed = true;
        var error = await Assert.ThrowsAsync<CommitStateUnknownException>(() => TableMutations.InsertAsync(table, [[1L]]));
        Assert.IsType<IOException>(error.ResolutionFailure);
        Assert.IsType<CommitStateUnknownException>(error.InnerException);
        Assert.Equal(expectedReads, catalog.RecoveryReads);
        Assert.All(catalog.UncertainFiles, path => Assert.True(File.Exists(path), path));
    }

    [Fact]
    public async Task Invalid_status_configuration_does_not_turn_an_applied_commit_into_a_definite_error()
    {
        using var catalog = new UncertainCommitCatalog(_warehouse);
        var table = await CreateAsync(catalog, extra: new Dictionary<string, string> { ["commit.status-check.num-retries"] = "-1" });
        catalog.Armed = true;
        var error = await Assert.ThrowsAsync<CommitStateUnknownException>(() => TableMutations.InsertAsync(table, [[1L]]));
        Assert.IsType<ArgumentException>(error.ResolutionFailure);
        Assert.Equal(0, catalog.RecoveryReads);
        Assert.All(catalog.UncertainFiles, path => Assert.True(File.Exists(path), path));
    }

    [Fact]
    public async Task Cancellation_during_recovery_cannot_allow_producer_replay()
    {
        using var catalog = new UncertainCommitCatalog(_warehouse);
        var table = await CreateAsync(catalog);
        using var cancel = new CancellationTokenSource();
        catalog.BeforeError = _ => { cancel.Cancel(); return Task.CompletedTask; };
        var producer = SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[1L]]));
        catalog.Armed = true;
        var error = await Assert.ThrowsAsync<CommitStateUnknownException>(() => producer.CommitAsync(cancel.Token));
        Assert.IsAssignableFrom<OperationCanceledException>(error.ResolutionFailure);
        var before = Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length;
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => producer.CommitAsync());
        Assert.Equal(before, Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length);
        Assert.All(catalog.UncertainFiles, path => Assert.True(File.Exists(path), path));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Uncertain_stages_cannot_be_replayed_or_explicitly_discarded(int tables)
    {
        using var catalog = new UncertainCommitCatalog(_warehouse) { ApplyBeforeError = false };
        var stages = new List<StagedCommit>();
        for (var i = 0; i < tables; i++)
        {
            var table = await CreateAsync(catalog, "t" + i);
            stages.Add(await StagedCommit.StageAsync(table, async ct => SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[1L]], ct))));
        }
        catalog.Armed = true;
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => StagedCommit.CommitAllAsync(stages));
        foreach (var stage in stages) await stage.DiscardAsync();
        Assert.All(catalog.UncertainFiles, path => Assert.True(File.Exists(path), path));
        var before = Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length;
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => StagedCommit.CommitAllAsync(stages));
        Assert.Equal(before, Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length);
    }

    [Fact]
    public async Task Cancellation_after_multi_table_swap_preserves_the_committed_artifacts()
    {
        using var catalog = new UncertainCommitCatalog(_warehouse) { Failure = new OperationCanceledException("lost response after cancellation") };
        using var cancel = new CancellationTokenSource();
        catalog.BeforeError = _ => { cancel.Cancel(); return Task.CompletedTask; };
        var stages = new List<StagedCommit>();
        for (var i = 0; i < 2; i++)
        {
            var table = await CreateAsync(catalog, "t" + i);
            stages.Add(await StagedCommit.StageAsync(table, async ct => SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[1L]], ct))));
        }
        catalog.Armed = true;
        var error = await Assert.ThrowsAsync<CommitStateUnknownException>(() => StagedCommit.CommitAllAsync(stages, cancel.Token));
        Assert.IsType<CommitStateUnknownException>(error.InnerException);
        Assert.IsAssignableFrom<OperationCanceledException>(error.ResolutionFailure);
        foreach (var stage in stages)
        {
            await stage.DiscardAsync();
            var fresh = await catalog.LoadTableAsync(stage.Table.Identifier);
            Assert.Equal(new long[] { 0, 1 }, (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order());
            await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        }
        Assert.All(catalog.UncertainFiles, path => Assert.True(File.Exists(path), path));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Unexpected_multi_table_swap_errors_are_never_definite_failures(bool applied)
    {
        using var catalog = new UncertainCommitCatalog(_warehouse) { ApplyBeforeError = applied, Failure = new IOException("lost transaction response") };
        var stages = new List<StagedCommit>();
        for (var i = 0; i < 2; i++)
        {
            var table = await CreateAsync(catalog, "t" + i);
            stages.Add(await StagedCommit.StageAsync(table, async ct => SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[1L]], ct))));
        }
        catalog.Armed = true;
        if (applied) await StagedCommit.CommitAllAsync(stages);
        else await Assert.ThrowsAsync<CommitStateUnknownException>(() => StagedCommit.CommitAllAsync(stages));
        foreach (var stage in stages)
        {
            Assert.Equal(applied, stage.IsCommitted);
            await stage.DiscardAsync();
            var fresh = await catalog.LoadTableAsync(stage.Table.Identifier);
            Assert.Equal(applied ? new long[] { 0, 1 } : [0L], (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order());
            await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        }
        Assert.All(catalog.UncertainFiles, path => Assert.True(File.Exists(path), path));
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }
}
