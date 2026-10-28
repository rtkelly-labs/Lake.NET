using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

[Spec("Commit Conflict Resolution and Retry")]
public sealed class CommitRetryBudgetTests : IDisposable
{
    private readonly string _warehouse;
    private readonly ScheduledCommitCatalog _catalog;
    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long)], [1]);

    public CommitRetryBudgetTests()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Iceberg.Net.slnx"))) root = root.Parent ?? throw new InvalidOperationException("Repository root missing");
        _warehouse = Path.Combine(root.FullName, "temp", "retry-budget", Guid.NewGuid().ToString("N"));
        _catalog = new ScheduledCommitCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("budget")).GetAwaiter().GetResult();
    }

    private async Task<Table> CreateAsync(string name, Dictionary<string, string>? properties = null)
    {
        var table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("budget", name), Schema, Properties: properties));
        await TableMutations.InsertAsync(table, [[0L]]);
        await TableMutations.InsertAsync(table, [[1L]]);
        return table;
    }

    public static TheoryData<string> Operations => new()
    {
        "producer", "insert", "upsert", "staged-single", "staged-multi", "expiry", "refs", "cherry-pick", "compaction", "partial-compaction",
    };

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Zero_elapsed_budget_allows_the_initial_attempt_and_no_retry(string operation)
    {
        var props = new Dictionary<string, string> { ["commit.retry.total-timeout-ms"] = "0", ["commit.retry.min-wait-ms"] = "0" };
        var table = await CreateAsync("a", props);
        Func<Task> run;
        switch (operation)
        {
            case "producer":
                var producer = SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[2L]]));
                run = async () => await producer.CommitAsync();
                break;
            case "insert": run = async () => await TableMutations.InsertAsync(table, [[2L]]); break;
            case "upsert": run = async () => await TableMutations.UpsertAsync(table, [[1L]]); break;
            case "staged-single":
            case "staged-multi":
                var stages = new List<StagedCommit> { await StagedCommit.StageAsync(table, async ct => SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[2L]], ct))) };
                if (operation == "staged-multi")
                {
                    var other = await CreateAsync("b", new Dictionary<string, string> { ["commit.retry.total-timeout-ms"] = "1800000" });
                    stages.Add(await StagedCommit.StageAsync(other, async ct => SnapshotProducer.Append(other).AddFiles(await TableWriter.WriteAsync(other, [[2L]], ct))));
                }
                run = () => StagedCommit.CommitAllAsync(stages);
                break;
            case "expiry": run = async () => await Maintenance.ExpireAsync(table, long.MaxValue, retainLast: 1); break;
            case "refs": run = async () => await table.ManageSnapshots().CreateBranch("audit").CommitAsync(); break;
            case "cherry-pick":
                var staged = await SnapshotProducer.Append(table).ToBranch("audit").AddFiles(await TableWriter.WriteAsync(table, [[2L]])).CommitAsync();
                run = async () => await table.ManageSnapshots().CherryPick(staged.SnapshotId).CommitAsync();
                break;
            default:
                run = async () =>
                {
                    var result = await Compaction.RewriteDataFilesAsync(table, new RewriteOptions(RewriteAll: true, PartialProgressEnabled: operation == "partial-compaction"));
                    Assert.Equal(1, result.FailedGroups);
                    Assert.Empty(result.Commits);
                };
                break;
        }
        var initialSnapshots = table.Metadata.Snapshots.Count;
        _catalog.SwapAttempts = 0;
        _catalog.RejectSwaps = true;
        if (operation == "partial-compaction") await run();
        else await Assert.ThrowsAsync<CommitFailedException>(run);
        Assert.Equal(1, _catalog.SwapAttempts);
        _catalog.RejectSwaps = false;
        var fresh = await _catalog.LoadTableAsync(table.Identifier);
        Assert.Equal(initialSnapshots, fresh.Metadata.Snapshots.Count);
        Assert.Equal(new long[] { 0, 1 }, (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order());
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    public async Task Oversized_backoff_cannot_overflow_or_outlive_the_elapsed_budget()
    {
        var table = await CreateAsync("large", new Dictionary<string, string>
        {
            ["commit.retry.min-wait-ms"] = int.MaxValue.ToString(),
            ["commit.retry.max-wait-ms"] = int.MaxValue.ToString(),
            ["commit.retry.total-timeout-ms"] = "1",
        });
        _catalog.SwapAttempts = 0;
        _catalog.RejectSwaps = true;
        await Assert.ThrowsAsync<CommitFailedException>(() => TableMutations.InsertAsync(table, [[2L]]));
        Assert.Equal(1, _catalog.SwapAttempts);
    }

    [Theory]
    [InlineData("commit.retry.num-retries", "-1")]
    [InlineData("commit.retry.min-wait-ms", "-1")]
    [InlineData("commit.retry.max-wait-ms", "-1")]
    [InlineData("commit.retry.total-timeout-ms", "-1")]
    [InlineData("commit.retry.total-timeout-ms", "2147483648")]
    [InlineData("commit.retry.num-retries", "invalid")]
    public async Task Invalid_policy_is_rejected_before_a_write(string key, string value)
    {
        var table = await CreateAsync("invalid");
        await table.SetPropertiesAsync(new Dictionary<string, string> { [key] = value });
        var files = Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length;
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.InsertAsync(table, [[2L]]));
        Assert.Equal(files, Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories).Length);
    }

    [Fact]
    public async Task Nested_producers_do_not_reset_the_parent_retry_count()
    {
        var table = await CreateAsync("count", new Dictionary<string, string> { ["commit.retry.num-retries"] = "1", ["commit.retry.min-wait-ms"] = "0" });
        _catalog.SwapAttempts = 0;
        _catalog.RejectSwaps = true;
        await Assert.ThrowsAsync<CommitFailedException>(() => TableMutations.UpsertAsync(table, [[1L]]));
        Assert.Equal(2, _catalog.SwapAttempts);
    }

    [Fact]
    public async Task A_staged_replan_cannot_restart_the_retry_count_after_a_missing_file_race()
    {
        var table = await CreateAsync("nested", new Dictionary<string, string> { ["commit.retry.num-retries"] = "1", ["commit.retry.min-wait-ms"] = "0" });
        var plans = 0;
        var stage = await StagedCommit.StageAsync(table, async ct =>
        {
            plans++;
            if (plans == 2)
            {
                _catalog.RejectSwaps = false;
                var other = await _catalog.LoadTableAsync(table.Identifier);
                await TableMutations.InsertAsync(other, [[3L]], ct);
                _catalog.RejectSwaps = true;
                throw new FileNotFoundException("Injected file expiry during the replan");
            }
            return SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[2L]], ct));
        });
        _catalog.RejectSwaps = true;
        await Assert.ThrowsAsync<FileNotFoundException>(() => StagedCommit.CommitAllAsync([stage]));
        Assert.Equal(2, plans);
        _catalog.RejectSwaps = false;
        var fresh = await _catalog.LoadTableAsync(table.Identifier);
        Assert.Equal(new long[] { 0, 1, 3 }, (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order());
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    public async Task Exhausted_write_retry_time_does_not_disable_read_only_status_recovery()
    {
        using var catalog = new UncertainCommitCatalog(_warehouse) { ReadFailuresAfterError = 1, ServerResponded = false };
        await catalog.CreateNamespaceAsync(Namespace.Of("status"));
        var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("status", "events"), Schema,
            Properties: new Dictionary<string, string>
            {
                ["commit.retry.total-timeout-ms"] = "0",
                ["commit.status-check.min-wait-ms"] = "0",
                ["commit.status-check.num-retries"] = "1",
            }));
        catalog.Armed = true;
        var result = await TableMutations.InsertAsync(table, [[2L]]);
        Assert.Equal(1, result.Attempts);
        Assert.Equal(2, catalog.RecoveryReads);
        Assert.Equal(2L, Assert.Single(await TableScan.ReadAllAsync(table))[0]);
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, true);
    }
}
