using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Partial-progress compaction (one commit per batch of file groups) and the position-delete-file rewrite.</summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("Position Delete Files")]
public sealed class PartialCompactionTests : IDisposable
{
    /// <summary>Runs <see cref="Interfere"/> before each pointer swap it selects (not re-entrantly), so its commit lands first.</summary>
    private sealed class HookCatalog(string warehouse) : InMemoryCatalog(warehouse)
    {
        public Func<int, Task>? Interfere { get; set; }
        private int _swaps;
        private bool _inside;

        protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
        {
            if (Interfere is { } interfere && !_inside)
            {
                _inside = true;
                try { await interfere(++_swaps); }
                finally { _inside = false; }
            }
            await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
        }
    }

    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-partial", Guid.NewGuid().ToString("N"));
    private readonly HookCatalog _catalog;

    public PartialCompactionTests()
    {
        _catalog = new HookCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("p")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "v", PrimitiveType.String),
        NestedField.Require(3, "p", PrimitiveType.Int),
    ], identifierFieldIds: [1]);

    /// <summary>Four partitions (id % 4), each with three small files.</summary>
    private async Task<Table> SmallFiles(int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("p", $"t{Guid.NewGuid():N}"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("p").Build(), FormatVersion: formatVersion));
        for (var i = 0; i < 3; i++)
            await TableMutations.InsertAsync(t, Enumerable.Range(i * 8, 8).Select(x => new object?[] { (long)x, "a", x % 4 }).ToList());
        return t;
    }

    private async Task<Dictionary<long, string?>> State(Table t)
    {
        var fresh = await _catalog.LoadTableAsync(t.Identifier);
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { AllSnapshots = true, Deep = true });
        return (await TableScan.ReadAllAsync(fresh)).ToDictionary(r => (long)r[0]!, r => (string?)r[1]);
    }

    private static readonly RewriteOptions Partial = new(MinInputFiles: 2, PartialProgressEnabled: true, PartialProgressMaxCommits: 2);

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Partial_progress_commits_each_batch_of_groups_separately(int formatVersion)
    {
        var t = await SmallFiles(formatVersion);
        var before = await State(t);
        var lineage = formatVersion >= 3 ? (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!, r => r.RowId) : null;

        var result = await Compaction.RewriteDataFilesAsync(t, Partial);
        Assert.Equal(2, result.Commits.Count);                         // 4 groups over at most 2 commits
        Assert.All(result.Commits, s => Assert.Equal(Operations.Replace, s.Operation));
        Assert.Equal(result.Commits[0].SnapshotId, result.Commits[1].ParentSnapshotId);
        Assert.Equal(12, result.FilesRewritten);
        Assert.Equal(4, result.FilesAdded);
        Assert.Equal(0, result.FailedGroups);
        Assert.Equal(before, await State(t));
        if (lineage is not null) Assert.Equal(lineage, (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!, r => r.RowId));

        // Without partial progress the same work is one commit.
        var t2 = await SmallFiles(formatVersion);
        Assert.Single((await Compaction.RewriteDataFilesAsync(t2, Partial with { PartialProgressEnabled = false })).Commits);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task A_concurrent_delete_invalidates_only_the_batch_it_races(int formatVersion)
    {
        var t = await SmallFiles(formatVersion);
        var other = await _catalog.LoadTableAsync(t.Identifier);
        // Before the second compaction commit swaps: delete one row from every partition.
        _catalog.Interfere = async swap =>
        {
            if (swap != 2) return;
            await other.RefreshAsync();
            await TableMutations.DeleteWhereAsync(other, r => (long)r[0]! < 4, DeleteStrategy.PositionDeletes);
        };
        var result = await Compaction.RewriteDataFilesAsync(t, Partial);
        _catalog.Interfere = null;

        Assert.Equal(2, result.Commits.Count);
        Assert.Equal(2, result.Attempts);                             // the second batch was re-planned once
        var state = await State(t);
        Assert.Equal(20, state.Count);                                // 24 rows minus the concurrent deletes
        Assert.All(Enumerable.Range(0, 4), id => Assert.False(state.ContainsKey(id)));
        var ancestry = t.Metadata.Ancestors(t.Metadata.CurrentSnapshotId).Select(s => s.SnapshotId).ToList();
        Assert.Contains(result.Commits[0].SnapshotId, ancestry);      // the first batch stood
    }

    [Fact]
    public async Task Groups_that_keep_losing_are_skipped_not_fatal()
    {
        var t = await SmallFiles(2);
        var other = await _catalog.LoadTableAsync(t.Identifier);
        var next = 1000L;
        _catalog.Interfere = async _ =>
        {
            await other.RefreshAsync();
            await TableMutations.InsertAsync(other, [[next++, "x", 0]]);
        };
        var result = await Compaction.RewriteDataFilesAsync(t, Partial, maxAttempts: 2);
        _catalog.Interfere = null;
        Assert.Empty(result.Commits);
        Assert.Equal(4, result.FailedGroups);
        Assert.Equal(24 + (int)(next - 1000), (await State(t)).Count); // every concurrent insert landed, nothing was rewritten
    }

    /// <summary>Position delete files of a v2 table: three delete commits against the same files, one dangling.</summary>
    private async Task<Table> ManyDeleteFiles()
    {
        var t = await SmallFiles(2);
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[0L, 0], [1L, 1]], DeleteStrategy.PositionDeletes);
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[4L, 0], [5L, 1]], DeleteStrategy.PositionDeletes);
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[2L, 2], [8L, 0]], DeleteStrategy.PositionDeletes);
        // Copy-on-write removes the p=2 file that a delete file points at: that delete is now dangling.
        await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! == 6, DeleteStrategy.CopyOnWrite);
        return t;
    }

    [Fact]
    public async Task Position_delete_files_are_rewritten_to_one_per_data_file_and_dangling_ones_dropped()
    {
        var t = await ManyDeleteFiles();
        var before = await State(t);
        var deletesBefore = await MetadataTables.DeleteFilesAsync(t);

        var result = await Compaction.RewritePositionDeletesAsync(t);
        Assert.NotNull(result.Snapshot);
        Assert.Equal(Operations.Replace, result.Snapshot!.Operation);
        Assert.Equal(deletesBefore.Count, result.DeleteFilesRemoved);
        Assert.True(result.DanglingPositionsDropped > 0);
        Assert.Equal(before, await State(t));

        var after = await MetadataTables.DeleteFilesAsync(t);
        Assert.Equal(result.DeleteFilesAdded, after.Count);
        Assert.True(after.Count < deletesBefore.Count);
        var index = await TableScan.DeletesAsync(t);
        var live = (await TableScan.PlanFilesAsync(t)).Select(x => x.File.FilePath).ToHashSet();
        var targets = new List<string>();
        foreach (var d in after)
        {
            var references = await index.PositionDeletesAsync(d.FilePath);
            Assert.Single(references);                                  // one data file per delete file...
            Assert.Contains(references.Keys.Single(), live);           // ...that is live
            targets.Add(references.Keys.Single());
        }
        Assert.Equal(targets.Count, targets.Distinct().Count());         // and one delete file per data file

        // Nothing left to do: a second run commits nothing.
        Assert.Null((await Compaction.RewritePositionDeletesAsync(t)).Snapshot);
    }

    [Fact]
    public async Task Position_delete_rewrite_conflicts_cleanly_with_a_concurrent_rewrite_and_composes_with_new_deletes()
    {
        var t = await ManyDeleteFiles();
        var other = await _catalog.LoadTableAsync(t.Identifier);
        _catalog.Interfere = async _ =>
        {
            _catalog.Interfere = null;
            await other.RefreshAsync();
            await Compaction.RewritePositionDeletesAsync(other);
        };
        await Assert.ThrowsAsync<ValidationException>(() => Compaction.RewritePositionDeletesAsync(t));
        var state = await State(t);

        // A concurrent new delete is kept: the rewrite retries on the new state and leaves the new delete file alone.
        var t2 = await ManyDeleteFiles();
        var other2 = await _catalog.LoadTableAsync(t2.Identifier);
        _catalog.Interfere = async _ =>
        {
            _catalog.Interfere = null;
            await other2.RefreshAsync();
            await TableMutations.DeleteByKeysAsync(other2, ["id", "p"], [[9L, 1]], DeleteStrategy.PositionDeletes);
        };
        var result = await Compaction.RewritePositionDeletesAsync(t2);
        Assert.NotNull(result.Snapshot);
        var state2 = await State(t2);
        Assert.False(state2.ContainsKey(9));
        Assert.Equal(state.Count - 1, state2.Count);
    }

    [Fact]
    public async Task On_v3_position_deletes_become_deletion_vectors()
    {
        var t = await ManyDeleteFiles();
        await t.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(3)]);
        var before = await State(t);
        var result = await Compaction.RewritePositionDeletesAsync(t);
        Assert.NotNull(result.Snapshot);
        Assert.Equal(before, await State(t));
        Assert.All(await MetadataTables.DeleteFilesAsync(t), d => Assert.Equal("PUFFIN", d.FileFormat));
        Assert.Equal((int)FileContent.PositionDeletes, (await MetadataTables.DeleteFilesAsync(t)).Select(d => d.Content).Distinct().Single());
    }
}
