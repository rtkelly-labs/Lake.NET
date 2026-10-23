using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The schema evolution API: every change keeps existing data readable by field ID.</summary>
[Spec("Schema Evolution")]
public sealed class SchemaUpdateTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-schema", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public SchemaUpdateTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("s")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Base = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "qty", PrimitiveType.Int),
        NestedField.Optional(3, "note", PrimitiveType.String),
        NestedField.Require(4, "region", PrimitiveType.String),
    ], identifierFieldIds: [1]);

    private async Task<Table> Seeded(int formatVersion = 2)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", $"t{Guid.NewGuid():N}"), Base,
            PartitionSpec.BuilderFor(Base).Identity("region").Build(), FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, [[1L, 10, "a", "eu"], [2L, null, "b", "us"]]);
        return t;
    }

    [Fact]
    public async Task Add_rename_widen_delete_and_move_keep_old_files_readable()
    {
        var t = await Seeded();
        await t.UpdateSchemaAsync(s => s
            .AddColumn("score", PrimitiveType.Double)
            .RenameColumn("qty", "quantity")
            .UpdateColumn("quantity", PrimitiveType.Long)
            .DeleteColumn("note")
            .MoveFirst("region")
            .UpdateDoc("id", "the key"));

        Assert.Equal(["region", "id", "quantity", "score"], t.Schema.Columns.Select(c => c.Name));
        Assert.Equal(5, t.Metadata.LastColumnId);  // the new column took the next unused id
        Assert.Equal(5, t.Schema.FindField("score")!.Id);
        Assert.Equal(2, t.Schema.FindField("quantity")!.Id);
        Assert.Equal("the key", t.Schema.FindField("id")!.Doc);

        var rows = (await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[1]!).ToList();
        Assert.Equal(new object?[] { "eu", 1L, 10L, null }, rows[0]); // int widened to long, new column null, deleted gone
        Assert.Equal(new object?[] { "us", 2L, null, null }, rows[1]);

        await TableMutations.InsertAsync(t, [["eu", 3L, 5_000_000_000L, 1.5]]);
        Assert.Equal(3, (await TableScan.ReadAllAsync(t)).Count);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }

    [Fact]
    public async Task Ids_of_deleted_columns_are_not_reused()
    {
        var t = await Seeded();
        await t.UpdateSchemaAsync(s => s.DeleteColumn("note"));
        await t.UpdateSchemaAsync(s => s.AddColumn("note", PrimitiveType.String));
        Assert.Equal(5, t.Schema.FindField("note")!.Id);
        // Old 'note' values (id 3) must not resurface under the new column.
        Assert.All(await TableScan.ReadAllAsync(t), r => Assert.Null(r[3]));
    }

    [Fact]
    public async Task Unsafe_changes_are_refused()
    {
        var t = await Seeded();
        await Assert.ThrowsAsync<InvalidOperationException>(() => t.UpdateSchemaAsync(s => s.DeleteColumn("region"))); // partition source
        await Assert.ThrowsAsync<InvalidOperationException>(() => t.UpdateSchemaAsync(s => s.DeleteColumn("id")));     // identifier
        await Assert.ThrowsAsync<InvalidOperationException>(() => t.UpdateSchemaAsync(s => s.UpdateColumn("note", PrimitiveType.Long)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => t.UpdateSchemaAsync(s => s.RequireColumn("note")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => t.UpdateSchemaAsync(s => s.AddRequiredColumn("x", PrimitiveType.Int)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => t.UpdateSchemaAsync(s => s.AddRequiredColumn("x", PrimitiveType.Int, 7))); // defaults need v3
        await Assert.ThrowsAsync<ArgumentException>(() => t.UpdateSchemaAsync(s => s.RenameColumn("note", "id")));
        await Assert.ThrowsAsync<ArgumentException>(() => t.UpdateSchemaAsync(s => s.AddColumn("qty", PrimitiveType.Int)));
        Assert.Equal(0, t.Metadata.CurrentSchemaId); // nothing committed
    }

    [Fact]
    public async Task Identifier_fields_can_be_changed_and_are_validated()
    {
        var t = await Seeded();
        await t.UpdateSchemaAsync(s => s.SetIdentifierFields("id", "region"));
        Assert.Equal([1, 4], t.Schema.IdentifierFieldIds);
        await Assert.ThrowsAnyAsync<ArgumentException>(() => t.UpdateSchemaAsync(s => s.SetIdentifierFields("note"))); // optional
    }

    [Fact]
    public async Task V3_required_column_with_a_default_reads_the_default_for_old_rows()
    {
        var t = await Seeded(formatVersion: 3);
        await t.UpdateSchemaAsync(s => s.AddRequiredColumn("tier", PrimitiveType.Int, initialDefault: 1));
        Assert.All(await TableScan.ReadAllAsync(t), r => Assert.Equal(1, r[4]));
    }

    [Fact]
    public async Task Concurrent_schema_changes_are_re_applied_on_top_of_each_other()
    {
        var a = await Seeded();
        var b = await _catalog.LoadTableAsync(a.Identifier);
        await a.UpdateSchemaAsync(s => s.AddColumn("from_a", PrimitiveType.String));
        await b.UpdateSchemaAsync(s => s.AddColumn("from_b", PrimitiveType.String)); // stale base: refreshed and re-applied
        var final = await _catalog.LoadTableAsync(a.Identifier);
        Assert.Equal(["id", "qty", "note", "region", "from_a", "from_b"], final.Schema.Columns.Select(c => c.Name));
        Assert.Equal(6, final.Schema.FindField("from_b")!.Id);
    }
}
