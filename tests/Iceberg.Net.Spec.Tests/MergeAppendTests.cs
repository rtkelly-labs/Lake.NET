using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Merge-append keeps the manifest count bounded without changing rows, sequence numbers or row lineage.</summary>
[Spec("Manifests")]
[Spec("Sequence Number Inheritance")]
[Spec("First Row ID Inheritance")]
public sealed class MergeAppendTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-merge", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public MergeAppendTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("mg")).GetAwaiter().GetResult();
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

    private Task<Table> Create(string name, int formatVersion, int minCount) =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("mg", name), Schema, PartitionSpec.BuilderFor(Schema).Identity("p").Build(),
            FormatVersion: formatVersion, Properties: new Dictionary<string, string> { [SnapshotProducer.ManifestMinCountToMerge] = minCount.ToString() }));

    private static List<object?[]> Rows(int from, int n, string v = "a") => Enumerable.Range(from, n).Select(i => new object?[] { (long)i, v, i % 2 }).ToList();

    private static async Task<int> DataManifests(Table t) =>
        (await TableScan.ManifestsAsync(t, t.Metadata.CurrentSnapshot!)).Count(m => m.Content == ManifestContent.Data);

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Appends_merge_once_the_threshold_is_reached(int formatVersion)
    {
        var t = await Create($"a{formatVersion}", formatVersion, minCount: 4);
        for (var i = 0; i < 3; i++) await TableMutations.InsertAsync(t, Rows(i * 10, 10));
        Assert.Equal(3, await DataManifests(t)); // below the threshold: one manifest per append
        await TableMutations.InsertAsync(t, Rows(30, 10));
        Assert.Equal(1, await DataManifests(t)); // fourth append merges everything into one manifest

        for (var i = 4; i < 20; i++) await TableMutations.InsertAsync(t, Rows(i * 10, 10));
        Assert.True(await DataManifests(t) < 4);
        Assert.Equal(200, (await TableScan.ReadAllAsync(t)).Count);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }

    [Theory]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(2, DeleteStrategy.EqualityDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.CopyOnWrite)]
    public async Task Merging_preserves_sequence_numbers_lineage_and_delete_applicability(int formatVersion, DeleteStrategy strategy)
    {
        var t = await Create($"d{formatVersion}{strategy}", formatVersion, minCount: 3);
        await TableMutations.InsertAsync(t, Rows(0, 10));
        await TableMutations.UpsertAsync(t, Rows(0, 3, "b"), ["id", "p"], strategy); // deletes apply only to the older files
        await TableMutations.InsertAsync(t, Rows(10, 10));
        var before = await Snapshot(t);

        await TableMutations.InsertAsync(t, Rows(20, 5)); // triggers a merge of the earlier data manifests
        await TableMutations.InsertAsync(t, Rows(25, 5));
        var after = await Snapshot(t);

        foreach (var (id, row) in before) Assert.Equal(row, after[id]); // same value, row id, last-updated sequence number
        Assert.Equal(30, after.Count);
        Assert.True(await DataManifests(t) <= 2);
        // Data sequence numbers survived the merge: the upsert's deletes still apply to exactly the old versions.
        Assert.Equal(3, after.Values.Count(r => r.V == "b"));
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }

    private static async Task<Dictionary<long, (string? V, long? RowId, long? Seq)>> Snapshot(Table t)
    {
        if (t.Metadata.FormatVersion >= 3)
            return (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!, r => ((string?)r.Row[1], r.RowId, r.LastUpdatedSequenceNumber));
        return (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => ((string?)r[1], (long?)null, (long?)null));
    }

    [Fact]
    public async Task Merging_can_be_disabled()
    {
        var t = await Create("off", 2, minCount: 2);
        await t.SetPropertiesAsync(new Dictionary<string, string> { [SnapshotProducer.ManifestMergeEnabled] = "false" });
        for (var i = 0; i < 5; i++) await TableMutations.InsertAsync(t, Rows(i * 10, 10));
        Assert.Equal(5, await DataManifests(t));
    }
}
