using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The shared read contract: snapshot selection, the snapshot's own schema, projection and filtering.</summary>
[Spec("Point in Time Reads (Time Travel)")]
[Spec("Column Projection")]
public sealed class ScanContractTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-scan", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public ScanContractTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("s")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)]);

    private static async Task<List<object?[]>> Read(Table t, ScanRequest r)
    {
        var rows = new List<object?[]>();
        await foreach (var row in TableScan.ReadRowsAsync(t, r)) rows.Add(row);
        return rows;
    }

    /// <summary>Three snapshots: ids 1-2, then id 3, then a rename of "name" to "title" and id 4 with a title.</summary>
    private async Task<(Table T, long S1, long S2, long S3)> HistoryAsync()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", $"t{Guid.NewGuid():N}"), Schema));
        var s1 = (await TableMutations.InsertAsync(t, [[1L, "a"], [2L, "b"]])).Snapshot.SnapshotId;
        var s2 = (await TableMutations.InsertAsync(t, [[3L, "c"]])).Snapshot.SnapshotId;
        await t.UpdateSchemaAsync(u => u.RenameColumn("name", "title"));
        var s3 = (await TableMutations.InsertAsync(t, [[4L, "d"]])).Snapshot.SnapshotId;
        return (await _catalog.LoadTableAsync(t.Identifier), s1, s2, s3);
    }

    [Fact]
    public async Task Current_reads_every_row_with_the_current_schema()
    {
        var (t, _, _, _) = await HistoryAsync();
        Assert.Equal(["id", "title"], TableScan.SchemaOf(t, new ScanRequest()).Columns.Select(c => c.Name));
        Assert.Equal([1L, 2L, 3L, 4L], (await Read(t, new ScanRequest())).Select(r => (long)r[0]!).Order());
    }

    [Fact]
    public async Task An_old_snapshot_reads_with_the_schema_it_was_written_with()
    {
        var (t, s1, s2, _) = await HistoryAsync();
        var request = new ScanRequest { Snapshot = SnapshotSelector.Id(s2) };
        Assert.Equal(["id", "name"], TableScan.SchemaOf(t, request).Columns.Select(c => c.Name));
        Assert.Equal([1L, 2L, 3L], (await Read(t, request)).Select(r => (long)r[0]!).Order());
        Assert.Equal(["a", "b"], (await Read(t, new ScanRequest { Snapshot = SnapshotSelector.Id(s1), Columns = ["name"] })).Select(r => (string)r[0]!).Order());
    }

    [Fact]
    public async Task As_of_an_instant_reads_what_was_current_then()
    {
        var (t, s1, s2, _) = await HistoryAsync();
        var at2 = t.Metadata.SnapshotLog.Single(e => e.SnapshotId == s2).TimestampMs;
        Assert.Equal(s2, TimeTravel.Resolve(t.Metadata, SnapshotSelector.AsOf(DateTimeOffset.FromUnixTimeMilliseconds(at2)))!.SnapshotId);
        Assert.Equal(s1, TimeTravel.Resolve(t.Metadata, SnapshotSelector.AsOf(DateTimeOffset.FromUnixTimeMilliseconds(at2 - 1)))!.SnapshotId);
        Assert.Throws<ArgumentException>(() => TimeTravel.Resolve(t.Metadata, SnapshotSelector.AsOf(DateTimeOffset.UnixEpoch)));
    }

    [Fact]
    public async Task A_tag_reads_the_snapshot_it_points_at()
    {
        var (t, s1, _, _) = await HistoryAsync();
        await t.CommitAsync([], [new MetadataUpdate.SetSnapshotRef("v1", SnapshotRef.Tag(s1))]);
        Assert.Equal([1L, 2L], (await Read(t, new ScanRequest { Snapshot = SnapshotSelector.Ref("v1") })).Select(r => (long)r[0]!).Order());
        Assert.Throws<ArgumentException>(() => TimeTravel.Resolve(t.Metadata, SnapshotSelector.Ref("nope")));
    }

    [Fact]
    public async Task Projection_and_filter_apply_together()
    {
        var (t, _, _, _) = await HistoryAsync();
        var rows = await Read(t, new ScanRequest { Filter = Expr.GreaterThan("id", 2L), Columns = ["title", "id"] });
        Assert.Equal([("c", 3L), ("d", 4L)], rows.Select(r => ((string)r[0]!, (long)r[1]!)).OrderBy(p => p.Item2));
        Assert.Throws<ArgumentException>(() => TableScan.SchemaOf(t, new ScanRequest { Columns = ["missing"] }));
    }

    [Fact]
    public async Task An_empty_table_yields_nothing()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", "empty"), Schema));
        Assert.Empty(await Read(t, new ScanRequest()));
    }
}
