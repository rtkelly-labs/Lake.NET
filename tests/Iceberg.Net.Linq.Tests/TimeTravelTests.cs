using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq.Tests;

[Spec("Point in Time Reads (Time Travel)")]
public sealed class TimeTravelTests : CatalogTest
{
    private async Task<(Table T, long S1, long S2)> HistoryAsync()
    {
        var schema = new Schema(0, [NestedField.Require(1, "key", PrimitiveType.Long), NestedField.Optional(2, "label", PrimitiveType.String)]);
        var t = await Catalog.CreateTableAsync(new CreateTableRequest(NewId(), schema));
        var s1 = (await t.InsertAsync([new ById { Key = 1, Label = "a" }, new ById { Key = 2, Label = "b" }])).Snapshot.SnapshotId;
        await t.UpdateSchemaAsync(u => u.RenameColumn("label", "title"));
        var s2 = (await t.InsertAsync([new ById { Key = 3, Label = "a" }])).Snapshot.SnapshotId;
        return (t, s1, s2);
    }

    [Fact]
    public async Task Selectors_reach_the_scan_request_and_read_that_snapshot()
    {
        var (t, s1, s2) = await HistoryAsync();
        var q = t.AsQueryable<ById>();
        Assert.Equal(SnapshotSelector.Current, q.ToScanRequest().Snapshot);
        Assert.Equal(SnapshotSelector.Id(s1), q.AtSnapshot(s1).ToScanRequest().Snapshot);
        Assert.Equal([1L, 2L], q.AtSnapshot(s1).Select(x => x.Key).ToList().Order());
        Assert.Equal(3, await q.AtSnapshot(s2).CountAsync());

        await t.CommitAsync([], [new MetadataUpdate.SetSnapshotRef("v1", SnapshotRef.Tag(s1))]);
        Assert.Equal(SnapshotSelector.Ref("v1"), q.AtRef("v1").ToScanRequest().Snapshot);
        Assert.Equal(2, q.Where(x => x.Key > 0).AtRef("v1").Count()); // a selector may come after operators

        var at1 = DateTimeOffset.FromUnixTimeMilliseconds(t.Metadata.SnapshotLog.Single(e => e.SnapshotId == s1).TimestampMs);
        Assert.Equal(SnapshotSelector.AsOf(at1), q.AsOf(at1).ToScanRequest().Snapshot);
        Assert.Equal(2, await q.AsOf(at1).CountAsync());
    }

    [Fact]
    public async Task A_column_renamed_since_the_snapshot_is_filtered_in_memory_not_by_the_wrong_name()
    {
        // The core plans files with the current schema's names and filters rows with the snapshot's: a filter on a
        // renamed column cannot be pushed down for an old snapshot, so it runs as a residual.
        var (t, s1, _) = await HistoryAsync();
        var old = t.AsQueryable<ById>().AtSnapshot(s1).Where(x => x.Label == "a");
        Assert.Equal("true", old.ToScanRequest().Filter.ToString());
        Assert.Equal([1L], old.Select(x => x.Key).ToList());
        var current = t.AsQueryable<ById>().Where(x => x.Label == "a");
        Assert.Equal("title == 'a'", current.ToScanRequest().Filter.ToString());
        Assert.Equal([1L, 3L], current.Select(x => x.Key).ToList().Order());
        Assert.Equal(["label"], t.AsQueryable<ById>().AtSnapshot(s1).Select(x => x.Label).ToScanRequest().Columns);
    }
}
