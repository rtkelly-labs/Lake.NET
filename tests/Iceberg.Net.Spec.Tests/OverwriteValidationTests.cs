using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Overwrite and file deletes are serializable: a concurrent writer's matching data is never silently removed.</summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("Optimistic Concurrency")]
public sealed class OverwriteValidationTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-ovw", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public OverwriteValidationTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("o")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Require(2, "region", PrimitiveType.String)]);

    private async Task<(Table A, Table B)> Setup(int minMergeCount = 100)
    {
        var a = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("o", $"t{Guid.NewGuid():N}"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("region").Build(),
            Properties: new Dictionary<string, string> { [SnapshotProducer.ManifestMinCountToMerge] = minMergeCount.ToString() }));
        await TableMutations.InsertAsync(a, [[1L, "us"], [2L, "eu"]]);
        await a.RefreshAsync();
        return (a, await _catalog.LoadTableAsync(a.Identifier));
    }

    private static bool Us(DataFile f) => (string?)f.Partition[0] == "us";

    [Theory]
    [InlineData(100)]
    [InlineData(2)] // the concurrent append lands in a merged manifest
    public async Task Overwrite_fails_when_a_concurrent_writer_added_matching_data(int minMergeCount)
    {
        var (a, b) = await Setup(minMergeCount);
        var overwrite = SnapshotProducer.Overwrite(a, Us).AddFiles(await TableWriter.WriteAsync(a, [[10L, "us"]]));
        await TableMutations.InsertAsync(b, [[3L, "us"]]); // lands first

        await Assert.ThrowsAsync<ValidationException>(() => overwrite.CommitAsync());
        var rows = (await TableScan.ReadAllAsync(await _catalog.LoadTableAsync(a.Identifier))).Select(r => (long)r[0]!).Order();
        Assert.Equal([1L, 2L, 3L], rows); // nothing of the overwrite applied, the concurrent row kept
    }

    [Fact]
    public async Task Overwrite_rebases_over_unrelated_concurrent_data()
    {
        var (a, b) = await Setup();
        var overwrite = SnapshotProducer.Overwrite(a, Us).AddFiles(await TableWriter.WriteAsync(a, [[10L, "us"]]));
        await TableMutations.InsertAsync(b, [[3L, "eu"]]);
        await overwrite.CommitAsync();
        Assert.Equal([2L, 3L, 10L], (await TableScan.ReadAllAsync(a)).Select(r => (long)r[0]!).Order());
    }

    [Fact]
    public async Task Snapshot_isolation_reapplies_the_predicate_instead()
    {
        var (a, b) = await Setup();
        var overwrite = SnapshotProducer.Overwrite(a, Us).AddFiles(await TableWriter.WriteAsync(a, [[10L, "us"]])).WithSnapshotIsolation();
        await TableMutations.InsertAsync(b, [[3L, "us"]]);
        await overwrite.CommitAsync();
        Assert.Equal([2L, 10L], (await TableScan.ReadAllAsync(a)).Select(r => (long)r[0]!).Order()); // the concurrent "us" row is replaced too
    }

    [Fact]
    public async Task Dropping_files_fails_when_matching_data_was_added_meanwhile()
    {
        var (a, b) = await Setup();
        var drop = SnapshotProducer.DeleteFiles(a, Us);
        await TableMutations.InsertAsync(b, [[3L, "us"]]);
        await Assert.ThrowsAsync<ValidationException>(() => drop.CommitAsync());
    }
}
