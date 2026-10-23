using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Writers honour the table's default sort order, and compaction applies a new order to existing data.</summary>
[Spec("Sorting")]
[Spec("Sort Orders")]
public sealed class SortOrderTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-sort", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public SortOrderTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("s")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "score", PrimitiveType.Double),
    ]);

    private Task<Table> Create(int formatVersion = 2) =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", $"t{Guid.NewGuid():N}"), Schema, PartitionSpec.Unpartitioned, FormatVersion: formatVersion));

    [Fact]
    public void Sorter_follows_direction_null_order_and_nan_placement()
    {
        var order = SortOrder.BuilderFor(Schema).Desc("score", NullOrder.NullsFirst).Asc("id").Build();
        var rows = new List<object?[]>
        {
            new object?[] { 1L, "a", 1.0 }, new object?[] { 2L, "b", double.NaN }, new object?[] { 3L, "c", null },
            new object?[] { 4L, "d", double.NegativeInfinity }, new object?[] { 5L, "e", 1.0 }, new object?[] { 6L, "f", 7.5 },
        };
        var sorted = rows.OrderBy(r => r, new RowSorter(Schema, order)).Select(r => (long)r[0]!).ToList();
        // nulls first regardless of direction; descending puts NaN (the greatest value) next; ties broken by id.
        Assert.Equal([3L, 2L, 6L, 1L, 5L, 4L], sorted);
    }

    [Fact]
    public void Sorter_applies_the_field_transform()
    {
        var order = SortOrder.BuilderFor(Schema).Asc("name", transform: Transform.Truncate(1)).Asc("id").Build();
        var rows = new List<object?[]> { new object?[] { 3L, "ba", 0.0 }, new object?[] { 1L, "bz", 0.0 }, new object?[] { 2L, "ab", 0.0 } };
        Assert.Equal([2L, 1L, 3L], rows.OrderBy(r => r, new RowSorter(Schema, order)).Select(r => (long)r[0]!));
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Writes_are_sorted_and_record_the_order_id(int formatVersion)
    {
        var t = await Create(formatVersion);
        await t.ReplaceSortOrderAsync(SortOrder.BuilderFor(t.Schema).Asc("name", NullOrder.NullsLast).Build());
        Assert.Equal(1, t.Metadata.DefaultSortOrderId);
        await TableMutations.InsertAsync(t, [[1L, "c", 0.0], [2L, null, 0.0], [3L, "a", 0.0], [4L, "b", 0.0]]);

        var file = Assert.Single((await TableScan.PlanAsync(t, Expression.True)).Tasks).File;
        Assert.Equal(1, file.SortOrderId);
        Assert.Equal([3L, 4L, 1L, 2L], (await TableScan.ReadAllAsync(t)).Select(r => (long)r[0]!)); // file order is scan order
        await TableValidator.AssertValidAsync(t, new ValidationOptions { Deep = true });

        // Setting the same order again is a no-op: the existing order is reused.
        await t.ReplaceSortOrderAsync(SortOrder.BuilderFor(t.Schema).Asc("name", NullOrder.NullsLast).Build());
        Assert.Single(t.Metadata.SortOrders, o => !o.IsUnsorted);
    }

    [Fact]
    public async Task Compaction_applies_a_new_sort_order_and_keeps_lineage()
    {
        var t = await Create(formatVersion: 3);
        var random = new Random(7);
        for (var batch = 0; batch < 3; batch++)
            await TableMutations.InsertAsync(t, Enumerable.Range(batch * 100, 100).Select(i => new object?[] { (long)i, $"n{random.Next(1000):D3}", random.NextDouble() }).ToList());
        var before = (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!, r => (r.RowId, r.LastUpdatedSequenceNumber));

        await t.ReplaceSortOrderAsync(SortOrder.BuilderFor(t.Schema).Asc("name").Build());
        // Three small files is below MinInputFiles, but none of them are in the new order, so they are rewritten.
        var result = await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 10));
        Assert.Equal(3, result.FilesRewritten);

        var file = Assert.Single((await TableScan.PlanAsync(t, Expression.True)).Tasks).File;
        Assert.Equal(t.Metadata.DefaultSortOrderId, file.SortOrderId);
        var names = (await TableScan.ReadAllAsync(t)).Select(r => (string)r[1]!).ToList();
        Assert.Equal(names.Order(StringComparer.Ordinal), names);
        var after = (await TableScan.ReadWithLineageAsync(t)).ToDictionary(r => (long)r.Row[0]!, r => (r.RowId, r.LastUpdatedSequenceNumber));
        Assert.Equal(before, after);

        // Now everything is in order: nothing further to do.
        Assert.Equal(0, (await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 10))).FilesRewritten);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }
}
