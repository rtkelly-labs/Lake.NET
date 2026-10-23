using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Compaction merges small files without changing rows, row lineage, or the effect of concurrent deletes.</summary>
[Spec("Row Lineage")]
[Spec("Commit Conflict Resolution and Retry")]
public sealed class CompactionTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-compact", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public CompactionTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("c")).GetAwaiter().GetResult();
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

    private static List<object?[]> Rows(int from, int n, string v = "a") => Enumerable.Range(from, n).Select(i => new object?[] { (long)i, v, i % 2 }).ToList();

    private async Task<Table> SmallFiles(int formatVersion, DeleteStrategy strategy)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("c", $"t{Guid.NewGuid():N}"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("p").Build(), FormatVersion: formatVersion));
        for (var i = 0; i < 8; i++) await TableMutations.InsertAsync(t, Rows(i * 10, 10));
        await TableMutations.UpsertAsync(t, Rows(0, 5, "b"), ["id", "p"], strategy);
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[11L, 1], [12L, 0]], strategy);
        return t;
    }

    private static async Task<Dictionary<long, (string? V, long? RowId, long? Seq)>> State(Table t) =>
        t.Metadata.FormatVersion >= 3
            ? (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!, r => ((string?)r.Row[1], r.RowId, r.LastUpdatedSequenceNumber))
            : (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => ((string?)r[1], (long?)null, (long?)null));

    [Theory]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(2, DeleteStrategy.EqualityDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.CopyOnWrite)]
    public async Task Rewrite_merges_small_files_and_keeps_rows_and_lineage(int formatVersion, DeleteStrategy strategy)
    {
        var t = await SmallFiles(formatVersion, strategy);
        var before = await State(t);
        var filesBefore = (await TableScan.PlanFilesAsync(t)).Count;

        var result = await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 3));

        Assert.NotNull(result.Snapshot);
        Assert.Equal("replace", result.Snapshot!.Operation);
        Assert.Equal(2, result.FilesAdded); // one per partition
        Assert.True((await TableScan.PlanFilesAsync(t)).Count < filesBefore);
        Assert.Equal(before, await State(t)); // same rows, same row ids, same last-updated sequence numbers
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });

        // Rewritten rows are still governed by later deletes and upserts.
        await TableMutations.UpsertAsync(t, Rows(1, 1, "c"), ["id", "p"], strategy);
        Assert.Equal("c", (await State(t))[1].V);
    }

    [Fact]
    public async Task Nothing_to_do_is_a_no_op()
    {
        var t = await SmallFiles(2, DeleteStrategy.PositionDeletes);
        var snapshots = t.Metadata.Snapshots.Count;
        var result = await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 100));
        Assert.Null(result.Snapshot);
        Assert.Equal(snapshots, t.Metadata.Snapshots.Count);
    }

    [Fact]
    public async Task Filter_limits_the_rewrite_to_matching_partitions()
    {
        var t = await SmallFiles(2, DeleteStrategy.PositionDeletes);
        async Task<List<string>> FilesOf(int p) => (await TableScan.PlanFilesAsync(t)).Where(f => (int)f.File.Partition[0]! == p).Select(f => f.File.FilePath).Order().ToList();
        var untouched = await FilesOf(1);
        var result = await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 3, Filter: Expr.Equal("p", 0)));
        Assert.Equal(1, result.FilesAdded);
        Assert.Single(await FilesOf(0));
        Assert.Equal(untouched, await FilesOf(1));
    }

    [Theory]
    [InlineData(DeleteStrategy.EqualityDeletes)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    public async Task Concurrent_deletes_are_never_undone_by_a_rewrite(DeleteStrategy strategy)
    {
        var t = await SmallFiles(2, strategy);
        var other = await _catalog.LoadTableAsync(t.Identifier);
        var expected = (await State(t)).Keys.ToHashSet();
        using var stop = new CancellationTokenSource();
        var deleter = Task.Run(async () =>
        {
            for (long id = 20; id < 60; id += 3)
            {
                await TableMutations.DeleteByKeysAsync(other, ["id", "p"], [[id, (int)(id % 2)]], strategy);
                lock (expected) expected.Remove(id);
            }
        });
        var rewrites = 0;
        while (!deleter.IsCompleted)
        {
            await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 2), maxAttempts: 50);
            rewrites++;
        }
        await deleter;
        await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 2), maxAttempts: 50);
        await t.RefreshAsync();
        Assert.Equal(expected.Order(), (await State(t)).Keys.Order());
        Assert.True(rewrites > 0);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }
}
