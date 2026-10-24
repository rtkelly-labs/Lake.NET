using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Creating a table with the caller's field IDs, and deleting by a filter plus a row refinement.</summary>
public sealed class CreateAndRefineTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-refine", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public CreateAndRefineTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("s")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Gapped = new(0, [NestedField.Require(5, "id", PrimitiveType.Long), NestedField.Optional(9, "name", PrimitiveType.String)]);

    [Fact]
    public async Task Field_ids_are_reassigned_by_default_and_kept_on_request()
    {
        var fresh = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", "fresh"), Gapped));
        Assert.Equal([1, 2], fresh.Schema.Columns.Select(c => c.Id));
        var kept = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", "kept"), Gapped, PreserveFieldIds: true));
        Assert.Equal([5, 9], kept.Schema.Columns.Select(c => c.Id));
        Assert.Equal(9, kept.Metadata.LastColumnId);
        await kept.UpdateSchemaAsync(u => u.AddColumn("extra", PrimitiveType.Int));
        Assert.Equal(10, (await _catalog.LoadTableAsync(kept.Identifier)).Schema.FindField("extra")!.Id);
    }

    [Theory]
    [InlineData(DeleteStrategy.CopyOnWrite)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    public async Task Delete_applies_both_the_filter_and_the_refinement(DeleteStrategy strategy)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", $"d{strategy}"), Gapped));
        await TableMutations.InsertAsync(t, [[1L, "apple"], [2L, "avocado"], [3L, "banana"], [4L, "apricot"]]);
        await TableMutations.DeleteWhereAsync(t, Expr.LessThan("id", 4L), row => ((string)row[1]!).StartsWith('a'), strategy);
        var rows = new List<long>();
        await foreach (var r in TableScan.ReadRowsAsync(await _catalog.LoadTableAsync(t.Identifier), new ScanRequest())) rows.Add((long)r[0]!);
        Assert.Equal([3L, 4L], rows.Order());
    }
}
