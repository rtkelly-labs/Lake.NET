using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Staged mutations (<see cref="TableMutations.StageMergeAsync"/>, <see cref="TableMutations.StageUpdateAsync"/>)
/// committed across tables with <see cref="StagedCommit.CommitAllAsync"/>: all or nothing, and every table re-planned
/// after a conflict on any of them.
/// </summary>
[Spec("rest: commitTransaction")]
[Spec("Commit Conflict Resolution and Retry")]
public sealed class StagedCommitTests : IDisposable
{
    /// <summary>Runs <see cref="BeforeNextTransaction"/> once, just before the next multi-table pointer swap.</summary>
    private sealed class HookCatalog(string warehouse, bool transactions = true) : InMemoryCatalog(warehouse)
    {
        public Func<Task>? BeforeNextTransaction { get; set; }
        public bool AlwaysInterfere { get; set; }
        public int Transactions { get; private set; }

        public override bool SupportsMultiTableTransactions => transactions;

        protected override async Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct)
        {
            Transactions++;
            if (BeforeNextTransaction is { } interfere)
            {
                if (!AlwaysInterfere) BeforeNextTransaction = null;
                await interfere();
            }
            await base.SwapMetadataLocationsAsync(swaps, ct);
        }
    }

    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-staged", Guid.NewGuid().ToString("N"));
    private readonly HookCatalog _catalog;

    private static readonly Schema Schema = new(0,
        [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "v", PrimitiveType.String), NestedField.Optional(3, "n", PrimitiveType.Int)],
        identifierFieldIds: [1]);

    public StagedCommitTests()
    {
        _catalog = new HookCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("s")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    /// <summary>A table with ids 1..3 (v = "base", n = id).</summary>
    private async Task<Table> TableAsync(string name, int formatVersion = 2, ICatalog? catalog = null, IReadOnlyDictionary<string, string>? properties = null)
    {
        catalog ??= _catalog;
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", name), Schema, FormatVersion: formatVersion, Properties: properties));
        await TableMutations.InsertAsync(t, [[1L, "base", 1], [2L, "base", 2], [3L, "base", 3]]);
        await t.RefreshAsync();
        return t;
    }

    private async Task<Dictionary<long, (string? V, int? N)>> RowsAsync(TableIdentifier id, ICatalog? catalog = null)
    {
        var fresh = await (catalog ?? _catalog).LoadTableAsync(id);
        var report = await TableValidator.ValidateAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        Assert.True(report.IsValid, report.ToString());
        return (await TableScan.ReadAllAsync(fresh)).ToDictionary(r => (long)r[0]!, r => ((string?)r[1], (int?)r[2]));
    }

    private static Task<StagedCommit<MergeResult>> Upsert(Table t, params object?[][] rows) =>
        TableMutations.StageMergeAsync(t, rows, ["id"], m => m.WhenMatchedUpdate((_, s) => s).WhenNotMatchedInsert());

    private async Task<long?> HeadAsync(TableIdentifier id) => (await _catalog.LoadTableAsync(id)).Metadata.CurrentSnapshotId;

    [Theory]
    [InlineData(2, DeleteStrategy.CopyOnWrite)]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    public async Task Staged_changes_to_two_tables_commit_together_as_one_snapshot_each(int formatVersion, DeleteStrategy strategy)
    {
        var a = await TableAsync("a", formatVersion);
        var b = await TableAsync("b", formatVersion);
        var (headA, headB) = (a.Metadata.CurrentSnapshotId, b.Metadata.CurrentSnapshotId);

        var merge = await TableMutations.StageMergeAsync(a, [[2L, "merged", 20], [9L, "new", 90]], ["id"],
            m => m.WhenMatchedUpdate((_, s) => s).WhenNotMatchedInsert(), strategy);
        var update = await TableMutations.StageUpdateAsync(b, Expr.LessThan("id", 3L), u => u.Set("v", "updated"), strategy);

        // Staged, not committed: neither table moved.
        Assert.Equal(headA, await HeadAsync(a.Identifier));
        Assert.Equal(headB, await HeadAsync(b.Identifier));
        Assert.NotNull(merge.Commit);
        Assert.False(merge.IsCommitted);

        await StagedCommit.CommitAllAsync([merge, update]);

        Assert.True(merge.IsCommitted && update.IsCommitted);
        Assert.Equal(1, _catalog.Transactions);
        Assert.Equal((1L, 0L, 1L), (merge.Result.RowsUpdated, merge.Result.RowsDeleted, merge.Result.RowsInserted));
        Assert.Equal(2L, update.Result.RowsUpdated);
        Assert.Equal(merge.Snapshot!.SnapshotId, a.Metadata.CurrentSnapshotId);  // the table handle is refreshed
        Assert.Equal(headA, a.Metadata.CurrentSnapshot!.ParentSnapshotId);
        Assert.Equal(update.Snapshot!.SnapshotId, (await HeadAsync(b.Identifier)));

        var rowsA = await RowsAsync(a.Identifier);
        Assert.Equal([("base", 1), ("merged", 20), ("base", 3), ("new", 90)], new long[] { 1, 2, 3, 9 }.Select(k => rowsA[k]));
        var rowsB = await RowsAsync(b.Identifier);
        Assert.Equal([("updated", 1), ("updated", 2), ("base", 3)], new long[] { 1, 2, 3 }.Select(k => rowsB[k]));
    }

    [Fact]
    public async Task A_conflict_on_one_table_re_plans_every_table_against_the_winners_state()
    {
        var a = await TableAsync("a");
        var b = await TableAsync("b");
        var headB = b.Metadata.CurrentSnapshotId;
        var seen = new List<int?>();
        // UPDATE b SET n = n + 1; MERGE into a, whose WHEN MATCHED reads the target row's n (as a concurrency check would).
        var stagedA = await TableMutations.StageMergeAsync(a, [[1L, "mine", 0]], ["id"], m => m
            .WhenMatchedUpdate((t, s) =>
            {
                seen.Add((int?)t[2]);
                return [s[0], s[1], (int?)t[2] + 100];
            }));
        var stagedB = await TableMutations.StageUpdateAsync(b, Expression.True, u => u.Set("n", row => (int?)row[2] + 1));
        var firstPlanB = stagedB.Snapshot!.SnapshotId;
        var firstList = stagedB.Snapshot.ManifestList!;

        _catalog.BeforeNextTransaction = async () =>
        {
            // Another writer changes row 1 of a and row 3 of b after both were planned.
            await TableMutations.UpdateAsync(await _catalog.LoadTableAsync(a.Identifier), Expr.Equal("id", 1L), u => u.Set("n", 7));
            await TableMutations.UpdateAsync(await _catalog.LoadTableAsync(b.Identifier), Expr.Equal("id", 3L), u => u.Set("n", 30));
        };
        await StagedCommit.CommitAllAsync([stagedA, stagedB]);

        Assert.Equal(2, _catalog.Transactions);
        Assert.Equal(2, stagedA.Attempts);
        Assert.Equal(2, stagedB.Attempts);
        Assert.Equal([1, 7], seen);                               // the re-plan saw the winner's value
        Assert.NotEqual(firstPlanB, stagedB.Snapshot!.SnapshotId);
        Assert.False(File.Exists(new Uri(firstList).LocalPath));  // the lost attempt's manifest list was removed

        Assert.Equal(("mine", 107), (await RowsAsync(a.Identifier))[1]);
        var rowsB = await RowsAsync(b.Identifier);
        Assert.Equal([2, 3, 31], new long[] { 1, 2, 3 }.Select(k => rowsB[k].N)); // the concurrent n = 30 was kept, then incremented
        // b: the concurrent snapshot, then ours on top of it.
        var tableB = await _catalog.LoadTableAsync(b.Identifier);
        Assert.Equal(stagedB.Snapshot.SnapshotId, tableB.Metadata.CurrentSnapshotId);
        Assert.Equal(headB, tableB.Metadata.SnapshotById(tableB.Metadata.CurrentSnapshot!.ParentSnapshotId!.Value)!.ParentSnapshotId);
    }

    [Fact]
    public async Task When_retries_run_out_neither_table_changes_and_the_staged_files_are_removed()
    {
        var noRetries = new Dictionary<string, string> { ["commit.retry.num-retries"] = "1", ["commit.retry.min-wait-ms"] = "1" };
        var a = await TableAsync("a", properties: noRetries);
        var b = await TableAsync("b", properties: noRetries);
        var headB = b.Metadata.CurrentSnapshotId;
        var stagedA = await Upsert(a, [1L, "mine", 1]);
        var stagedB = await Upsert(b, [1L, "mine", 1]);
        var lists = new List<string>();
        var interference = 0;
        _catalog.AlwaysInterfere = true;
        _catalog.BeforeNextTransaction = async () =>
        {
            lists.Add(stagedA.Snapshot!.ManifestList!);
            lists.Add(stagedB.Snapshot!.ManifestList!);
            await TableMutations.InsertAsync(await _catalog.LoadTableAsync(a.Identifier), [[100L + interference++, "other", 0]]);
        };

        await Assert.ThrowsAsync<CommitFailedException>(() => StagedCommit.CommitAllAsync([stagedA, stagedB]));

        Assert.Equal(2, _catalog.Transactions);                 // the first attempt and one retry
        Assert.False(stagedA.IsCommitted);
        Assert.Equal(headB, await HeadAsync(b.Identifier));      // b never moved
        var rowsA = await RowsAsync(a.Identifier);
        Assert.Equal(("base", 1), rowsA[1]);                     // a has only the other writer's rows
        Assert.Equal(5, rowsA.Count);
        Assert.Equal(4, lists.Count);
        Assert.All(lists, l => Assert.False(File.Exists(new Uri(l).LocalPath)));
    }

    [Fact]
    public async Task A_check_that_fails_on_the_re_plan_commits_nothing()
    {
        var a = await TableAsync("a");
        var b = await TableAsync("b");
        var headA = a.Metadata.CurrentSnapshotId;
        // A concurrency token: the row must still have n == 2.
        var stagedA = await TableMutations.StageMergeAsync(a, [[2L, "mine", 2]], ["id"], m => m
            .WhenMatchedUpdate((t, s) => (int?)t[2] == 2 ? s : throw new InvalidOperationException("token changed")));
        var stagedB = await Upsert(b, [5L, "new", 5]);
        _catalog.BeforeNextTransaction = async () =>
            await TableMutations.UpdateAsync(await _catalog.LoadTableAsync(a.Identifier), Expr.Equal("id", 2L), u => u.Set("n", 3));

        await Assert.ThrowsAsync<InvalidOperationException>(() => StagedCommit.CommitAllAsync([stagedA, stagedB]));

        Assert.False((await RowsAsync(b.Identifier)).ContainsKey(5));
        var tableA = await _catalog.LoadTableAsync(a.Identifier);
        Assert.Equal(headA, tableA.Metadata.CurrentSnapshot!.ParentSnapshotId); // only the other writer's snapshot
        Assert.Equal(("base", 3), (await RowsAsync(a.Identifier))[2]);
    }

    [Fact]
    public async Task Changes_that_change_nothing_commit_nothing()
    {
        var a = await TableAsync("a");
        var b = await TableAsync("b");
        var update = await TableMutations.StageUpdateAsync(a, Expr.Equal("id", 42L), u => u.Set("v", "x"));
        var merge = await TableMutations.StageMergeAsync(b, [[42L, "x", 0]], ["id"], m => m.WhenMatchedDelete());
        Assert.Null(update.Commit);
        Assert.Null(merge.Commit);

        await StagedCommit.CommitAllAsync([update, merge]);

        Assert.True(update.IsCommitted && merge.IsCommitted);
        Assert.Null(merge.Result.Snapshot);
        Assert.Equal(0, _catalog.Transactions);
        Assert.Equal(a.Metadata.CurrentSnapshotId, await HeadAsync(a.Identifier));
    }

    [Fact]
    public async Task A_catalog_without_transactions_refuses_several_tables_but_commits_one()
    {
        var warehouse = Path.Combine(_warehouse, "plain");
        using var plain = new HookCatalog(warehouse, transactions: false);
        await plain.CreateNamespaceAsync(Namespace.Of("s"));
        var a = await TableAsync("a", catalog: plain);
        var b = await TableAsync("b", catalog: plain);
        var stagedA = await Upsert(a, [1L, "mine", 1]);
        var list = stagedA.Snapshot!.ManifestList!;

        var stagedB = await Upsert(b, [1L, "mine", 1]);
        await Assert.ThrowsAsync<NotSupportedException>(() => StagedCommit.CommitAllAsync([stagedA, stagedB]));
        Assert.False(File.Exists(new Uri(list).LocalPath));
        Assert.Equal(("base", 1), (await RowsAsync(a.Identifier, plain))[1]);

        var single = await Upsert(await plain.LoadTableAsync(a.Identifier), [1L, "mine", 1]);
        await StagedCommit.CommitAllAsync([single]);
        Assert.Equal(("mine", 1), (await RowsAsync(a.Identifier, plain))[1]);
        Assert.Equal(0, plain.Transactions);
    }

    [Fact]
    public async Task A_snapshot_producer_can_be_staged_too_and_tables_must_be_distinct()
    {
        var a = await TableAsync("a");
        var b = await TableAsync("b");
        var append = await StagedCommit.StageAsync(a, async ct =>
            SnapshotProducer.Append(a).AddFiles(await TableWriter.WriteAsync(a, [[10L, "appended", 10]], ct)));
        var merge = await Upsert(b, [1L, "mine", 1]);
        await Assert.ThrowsAsync<ArgumentException>(() => StagedCommit.CommitAllAsync([merge, merge]));

        await StagedCommit.CommitAllAsync([append, merge]);
        Assert.Equal(("appended", 10), (await RowsAsync(a.Identifier))[10]);
        Assert.Equal(("mine", 1), (await RowsAsync(b.Identifier))[1]);
        await Assert.ThrowsAsync<InvalidOperationException>(() => StagedCommit.CommitAllAsync([append]));
    }
}
