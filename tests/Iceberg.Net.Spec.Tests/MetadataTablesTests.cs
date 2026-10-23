using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Metadata tables agree with the metadata and manifests they summarise (Spark's columns; cross-checked in SparkCrossEngineTests).</summary>
[Spec("Snapshots")]
[Spec("Manifests")]
[Spec("Manifest Lists")]
public sealed class MetadataTablesTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-mdtables", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public MetadataTablesTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("m")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private async Task<Table> Build(int formatVersion, DeleteStrategy strategy)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", $"t{Guid.NewGuid():N}"), SnapshotManagementTests.Schema,
            PartitionSpec.BuilderFor(SnapshotManagementTests.Schema).Identity("p").Build(), FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, SnapshotManagementTests.Rows(0, 10));
        await t.ManageSnapshots().CreateTag("first", t.Metadata.CurrentSnapshotId!.Value).CommitAsync();
        await TableMutations.InsertAsync(t, SnapshotManagementTests.Rows(10, 6));
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[1L, 1], [2L, 0], [12L, 0]], strategy);
        await TableMutations.UpsertAsync(t, SnapshotManagementTests.Rows(3, 2, "u"), ["id", "p"], strategy);
        return await _catalog.LoadTableAsync(t.Identifier);
    }

    [Theory]
    [InlineData(2, DeleteStrategy.CopyOnWrite)]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(2, DeleteStrategy.EqualityDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    public async Task Tables_describe_the_snapshot_they_select(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Build(formatVersion, strategy);
        var m = t.Metadata;

        var snapshots = MetadataTables.Snapshots(t);
        Assert.Equal(m.Snapshots.Count, snapshots.Count);
        Assert.Equal(m.CurrentSnapshotId, snapshots[^1].SnapshotId);
        Assert.All(snapshots, s => Assert.Equal(m.SnapshotById(s.SnapshotId)!.ParentSnapshotId, s.ParentId));

        Assert.Equal(m.SnapshotLog.Select(e => e.SnapshotId), MetadataTables.History(t).Select(h => h.SnapshotId));
        Assert.All(MetadataTables.History(t), h => Assert.True(h.IsCurrentAncestor));
        Assert.Equal(["first", "main"], MetadataTables.Refs(t).Select(r => r.Name));
        Assert.Equal("TAG", MetadataTables.Refs(t)[0].Type);

        var manifests = await MetadataTables.ManifestsAsync(t);
        var list = await TableScan.ManifestsAsync(t, m.CurrentSnapshot!);
        Assert.Equal(list.Select(x => x.ManifestPath), manifests.Select(x => x.Path));
        Assert.All(manifests.Where(x => x.Content == 0), x => Assert.Equal(0, x.AddedDeleteFilesCount + x.ExistingDeleteFilesCount + x.DeletedDeleteFilesCount));
        Assert.All(manifests.SelectMany(x => x.PartitionSummaries), p => Assert.Contains(p.LowerBound, new[] { "0", "1", null })); // null: only deleted entries left

        // files = the planned data files + the live delete files; data_files/delete_files split them.
        var files = await MetadataTables.FilesAsync(t);
        var planned = (await TableScan.PlanFilesAsync(t)).Select(x => x.File.FilePath).Order().ToList();
        Assert.Equal(planned, (await MetadataTables.DataFilesAsync(t)).Select(f => f.FilePath).Order());
        Assert.Equal(files.Count, (await MetadataTables.DataFilesAsync(t)).Count + (await MetadataTables.DeleteFilesAsync(t)).Count);
        Assert.Equal(strategy != DeleteStrategy.CopyOnWrite, (await MetadataTables.DeleteFilesAsync(t)).Count > 0);
        Assert.All(files, f => Assert.Contains(f.Partition["p"], new object?[] { 0, 1 }));

        // entries include the deleted ones a rewrite left in its manifests; files only the live ones.
        var entries = await MetadataTables.EntriesAsync(t);
        Assert.Equal(files.Count, entries.Count(e => e.Status != 2));
        if (strategy == DeleteStrategy.CopyOnWrite) Assert.Contains(entries, e => e.Status == 2);

        // partitions: data record counts per partition add up to the snapshot's total-records.
        var partitions = await MetadataTables.PartitionsAsync(t);
        Assert.Equal(2, partitions.Count);
        Assert.Equal(long.Parse(m.CurrentSnapshot!.Summary["total-records"], System.Globalization.CultureInfo.InvariantCulture), partitions.Sum(p => p.RecordCount));
        Assert.Equal(planned.Count, partitions.Sum(p => p.FileCount));
        Assert.Equal((await MetadataTables.DeleteFilesAsync(t)).Count, partitions.Sum(p => p.PositionDeleteFileCount + p.EqualityDeleteFileCount));
        Assert.All(partitions, p => Assert.NotNull(p.LastUpdatedSnapshotId));

        // Selected by tag: the state the tag sees.
        var tagged = await MetadataTables.DataFilesAsync(t, SnapshotSelector.Ref("first"));
        Assert.Equal(10, tagged.Sum(f => f.RecordCount));
        Assert.Equal(10, (await MetadataTables.PartitionsAsync(t, SnapshotSelector.Ref("first"))).Sum(p => p.RecordCount));
        Assert.NotEmpty(MetadataTables.MetadataLogEntries(t));
    }

    [Fact]
    public async Task History_marks_rolled_back_snapshots_as_not_current_ancestors()
    {
        var t = await Build(2, DeleteStrategy.PositionDeletes);
        var first = t.Metadata.Refs["first"].SnapshotId;
        await t.ManageSnapshots().RollbackTo(first).CommitAsync();
        var history = MetadataTables.History(t);
        Assert.Equal(first, history[^1].SnapshotId);
        Assert.All(history.Where(h => h.SnapshotId == first), h => Assert.True(h.IsCurrentAncestor));
        Assert.Equal(t.Metadata.Snapshots.Count - 1, history.Count(h => !h.IsCurrentAncestor));
    }
}
