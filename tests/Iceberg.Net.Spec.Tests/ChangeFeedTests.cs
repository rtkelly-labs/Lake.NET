using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Partitioning;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The change feed replays to every snapshot's state, under every delete strategy.</summary>
[Spec("Row-level Deletes")]
[Spec("Scan Planning")]
[Spec("Sequence Numbers")]
public sealed class ChangeFeedTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-changes", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public ChangeFeedTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("c")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static List<object?[]> Rows(int from, int n, string v = "a") => SnapshotManagementTests.Rows(from, n, v);

    private Task<Table> NewTable(int formatVersion) =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("c", $"t{Guid.NewGuid():N}"), SnapshotManagementTests.Schema,
            PartitionSpec.BuilderFor(SnapshotManagementTests.Schema).Identity("p").Build(), FormatVersion: formatVersion));

    private static async Task<List<ChangeRow>> Changes(Table t, long? from, long? to = null, ChangeFeedOptions? options = null)
    {
        var list = new List<ChangeRow>();
        await foreach (var c in ChangeFeed.ChangesAsync(t, from, to, options)) list.Add(c);
        return list;
    }

    private static string Show(object?[] r) => $"{r[0]}:{r[1]}:{r[2]}";

    [Theory]
    [InlineData(2, DeleteStrategy.CopyOnWrite)]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(2, DeleteStrategy.EqualityDeletes)]
    [InlineData(3, DeleteStrategy.CopyOnWrite)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.EqualityDeletes)]
    public async Task Replaying_the_feed_reproduces_every_snapshot(int formatVersion, DeleteStrategy strategy)
    {
        var t = await NewTable(formatVersion);
        await TableMutations.InsertAsync(t, Rows(0, 8));
        await TableMutations.InsertAsync(t, Rows(8, 4));
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[1L, 1], [2L, 0], [9L, 1]], strategy);
        await TableMutations.UpsertAsync(t, Rows(3, 3, "u"), ["id", "p"], strategy);
        if (strategy != DeleteStrategy.EqualityDeletes) await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! == 10, strategy);
        await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 2, RewriteAll: true)); // a replace: no changes
        await TableMutations.InsertAsync(t, Rows(20, 1));

        foreach (var removeCarryovers in new[] { false, true })
        {
            var changes = await Changes(t, null, options: new ChangeFeedOptions { RemoveCarryovers = removeCarryovers });
            var ordered = t.Metadata.Ancestors(t.Metadata.CurrentSnapshotId).Reverse().Where(s => s.Operation != Metadata.Operations.Replace).ToList();
            Assert.Equal(ordered.Select(s => s.SnapshotId), changes.Select(c => c.CommitSnapshotId).Distinct());
            var state = new List<string>();
            for (var ordinal = 0; ordinal < ordered.Count; ordinal++)
            {
                var step = changes.Where(c => c.ChangeOrdinal == ordinal).ToList();
                Assert.All(step, c => Assert.Equal(ordered[ordinal].SnapshotId, c.CommitSnapshotId));
                foreach (var c in step.Where(c => c.ChangeType == ChangeType.Delete)) Assert.True(state.Remove(Show(c.Row)), $"deleted a row that was not there: {Show(c.Row)}");
                state.AddRange(step.Where(c => c.ChangeType == ChangeType.Insert).Select(c => Show(c.Row)));
                var expected = (await TableScan.ReadAllAsync(t, ordered[ordinal].SnapshotId)).Select(Show).Order().ToList();
                Assert.Equal(expected, state.Order().ToList());
            }
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Position_deletes_report_exactly_the_rows_they_reference(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        await TableMutations.InsertAsync(t, Rows(0, 6));
        var start = t.Metadata.CurrentSnapshotId;
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[1L, 1], [4L, 0]], DeleteStrategy.PositionDeletes);
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[3L, 1]], DeleteStrategy.PositionDeletes); // v3: merged into the DV, only 3 is new

        var changes = await Changes(t, start);
        Assert.All(changes, c => Assert.Equal(ChangeType.Delete, c.ChangeType));
        Assert.Equal(["1", "4"], changes.Where(c => c.ChangeOrdinal == 0).Select(c => $"{c.Row[0]}").Order());
        Assert.Equal(["3"], changes.Where(c => c.ChangeOrdinal == 1).Select(c => $"{c.Row[0]}"));
    }

    [Fact]
    public async Task Carryover_removal_leaves_only_the_changed_rows_of_a_copy_on_write_update()
    {
        var t = await NewTable(2);
        await TableMutations.InsertAsync(t, Rows(0, 6));
        var start = t.Metadata.CurrentSnapshotId;
        await TableMutations.UpsertAsync(t, Rows(2, 1, "new"), ["id", "p"], DeleteStrategy.CopyOnWrite);

        var raw = await Changes(t, start);
        Assert.Equal(raw.Count(c => c.ChangeType == ChangeType.Delete), raw.Count(c => c.ChangeType == ChangeType.Insert)); // the whole p=0 file, twice
        Assert.True(raw.Count > 2);
        var net = await Changes(t, start, options: new ChangeFeedOptions { RemoveCarryovers = true });
        Assert.Equal(["Delete 2:a", "Insert 2:new"], net.Select(c => $"{c.ChangeType} {c.Row[0]}:{c.Row[1]}").Order());
    }

    [Fact]
    public async Task Range_must_follow_the_branch_ancestry()
    {
        var t = await NewTable(2);
        await TableMutations.InsertAsync(t, Rows(0, 2));
        var s1 = t.Metadata.CurrentSnapshotId!.Value;
        await TableMutations.InsertAsync(t, Rows(2, 2));
        var s2 = t.Metadata.CurrentSnapshotId!.Value;
        await t.ManageSnapshots().RollbackTo(s1).CommitAsync();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, Rows(10, 1))).ToBranch("dev").CommitAsync();

        await Assert.ThrowsAsync<ArgumentException>(() => Changes(t, s2));       // not an ancestor of main any more
        Assert.Empty(await Changes(t, s1));                                       // nothing on main after s1
        Assert.Equal([2L, 3], (await Changes(t, s1, s2)).Select(c => (long)c.Row[0]!).Order()); // explicit end
        Assert.Equal([10L], (await Changes(t, s1, options: new ChangeFeedOptions { Branch = "dev" })).Select(c => (long)c.Row[0]!));
    }
}
