using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Catalog commit paths that the stryker-catalog mutation run (see docs/roadmap.md, "Mutation testing") showed no test
/// pinned down: a caller's table location, multi-table transaction requirements, and the IDs given to schemas and sort
/// orders that the client renumbers.
/// </summary>
public sealed class CatalogCommitTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-catalogcommit", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    private static readonly Schema TwoColumns = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)]);

    public CatalogCommitTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("c")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private Task<Table> Create(string name, string? location = null) =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("c", name), TwoColumns, Location: location));

    [Spec("Table Metadata Fields")]
    [Fact]
    public async Task A_location_given_at_creation_is_the_table_location()
    {
        var location = Path.Combine(_warehouse, "elsewhere", "t");
        var t = await Create("t", location);
        Assert.Equal(location, t.Location);
        Assert.StartsWith(location, t.MetadataLocation, StringComparison.Ordinal);
    }

    [Spec("rest: commitTransaction")]
    [Spec("Optimistic Concurrency")]
    [Fact]
    public async Task A_transaction_with_a_failed_requirement_changes_no_table()
    {
        var a = await Create("a");
        var b = await Create("b");
        var props = new Dictionary<string, string> { ["k"] = "v" };
        var commits = new List<TableCommit>
        {
            new(a.Identifier, [new UpdateRequirement.AssertTableUuid(a.Metadata.TableUuid)], [new MetadataUpdate.SetProperties(props)]),
            // b's requirement names another table's UUID, so the whole transaction must fail.
            new(b.Identifier, [new UpdateRequirement.AssertTableUuid(a.Metadata.TableUuid)], [new MetadataUpdate.SetProperties(props)]),
        };
        await Assert.ThrowsAsync<CommitFailedException>(() => _catalog.CommitTransactionAsync(commits));
        Assert.Equal(a.MetadataLocation, (await _catalog.LoadTableAsync(a.Identifier)).MetadataLocation);
        Assert.Equal(b.MetadataLocation, (await _catalog.LoadTableAsync(b.Identifier)).MetadataLocation);
        // The metadata file written for a before b's requirement failed is removed: nothing references it.
        var metadataFiles = Directory.GetFiles(Path.Combine(a.Location, "metadata"), "*.metadata.json");
        Assert.Single(metadataFiles);
    }

    [Spec("Schema Evolution")]
    [Fact]
    public async Task A_schema_sent_with_a_taken_id_gets_the_next_free_one()
    {
        var t = await Create("s");
        await t.UpdateSchemaAsync(u => u.AddColumn("extra", PrimitiveType.Int));
        Assert.Equal(1, t.Metadata.CurrentSchemaId);
        // Schema ID 0 is taken; so is 1. The client renumbers to max + 1, as Java's does.
        var narrower = new Schema(0, [.. t.Schema.Columns.Where(c => c.Name != "name")]);
        await t.UpdateSchemaAsync(narrower);
        Assert.Equal(2, t.Metadata.CurrentSchemaId);
        Assert.Equal(new[] { 0, 1, 2 }, t.Metadata.Schemas.Select(s => s.SchemaId).Order());
        Assert.DoesNotContain(t.Schema.Columns, c => c.Name == "name");
    }

    [Spec("Sort Orders")]
    [Fact]
    public async Task Each_new_sort_order_gets_the_next_free_id()
    {
        var t = await Create("o");
        await t.ReplaceSortOrderAsync(SortOrder.BuilderFor(t.Schema).Asc("id").Build());
        await t.ReplaceSortOrderAsync(SortOrder.BuilderFor(t.Schema).Desc("name").Build());
        Assert.Equal(2, t.Metadata.DefaultSortOrderId);
        Assert.Equal(new[] { 0, 1, 2 }, t.Metadata.SortOrders.Select(o => o.OrderId).Order());
        Assert.Equal("name", t.Schema.FindColumnName(t.Metadata.DefaultSortOrder.Fields.Single().SourceId));
    }
}
