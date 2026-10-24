using Microsoft.EntityFrameworkCore;
using Iceberg.Net.Catalog;
using Iceberg.Net.Types;

namespace Iceberg.Net.EntityFrameworkCore.Tests;

public sealed class ModelTests : EfTest
{
    [Fact]
    public async Task EnsureCreated_creates_the_namespace_and_tables_from_the_model()
    {
        await using var ctx = NewContext();
        Assert.True(await ctx.Database.EnsureCreatedAsync());
        Assert.False(await ctx.Database.EnsureCreatedAsync());
        Assert.True(await ctx.Database.CanConnectAsync());

        var orders = await TableAsync("orders");
        Assert.Equal(["id", "customer", "qty", "amount", "status", "created_at", "note"], orders.Schema.Columns.Select(c => c.Name));
        Assert.Equal([1, 2, 3, 4, 5, 6, 7], orders.Schema.Columns.Select(c => c.Id));
        Assert.Equal([1], orders.Schema.IdentifierFieldIds);
        Assert.Equal("decimal(12, 2)", orders.Schema.FindField("amount")!.Type.ToString());
        Assert.True(orders.Schema.FindField("customer")!.Required);
        Assert.False(orders.Schema.FindField("note")!.Required);
        var partition = Assert.Single(orders.Metadata.DefaultSpec.Fields);
        Assert.Equal("day", partition.Transform.ToString());
        Assert.Equal(6, partition.SourceId);

        var customers = await TableAsync("customer");
        Assert.Equal(["name", "region", "version"], customers.Schema.Columns.Select(c => c.Name));
        Assert.Equal("decimal(18, 2)", (await TableAsync("account")).Schema.FindField("balance")!.Type.ToString());

        Assert.True(await ctx.Database.EnsureDeletedAsync());
        Assert.False(await Catalog.TableExistsAsync(TableIdentifier.Of("shop", "orders")));
        Assert.False(await ctx.Database.EnsureDeletedAsync());
    }

    public sealed class Explicit
    {
        public long Id { get; set; }
        [Iceberg.Net.Linq.IcebergField(10)] public string Name { get; set; } = "";
        public int Size { get; set; }
    }

    private sealed class ExplicitContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder b) =>
            b.Entity<Explicit>(e =>
            {
                e.Property(x => x.Size).HasIcebergFieldId(7).HasIcebergColumnName("bytes");
            });
    }

    [Fact]
    public async Task Explicit_field_ids_are_kept_and_bind_by_id_across_renames()
    {
        await using var ctx = new ExplicitContext(new DbContextOptionsBuilder().UseIceberg(Catalog, "shop").Options);
        await ctx.Database.EnsureCreatedAsync();
        var table = await TableAsync("Explicit");
        Assert.Equal([(1, "Id"), (10, "Name"), (7, "bytes")], table.Schema.Columns.Select(c => (c.Id, c.Name)));
        ctx.Add(new Explicit { Id = 1, Name = "a", Size = 3 });
        await ctx.SaveChangesAsync();

        // Renamed outside EF Core: members with field IDs still bind.
        await table.UpdateSchemaAsync(u => u.RenameColumn("Name", "title").RenameColumn("bytes", "size_bytes"));
        await using var fresh = new ExplicitContext(new DbContextOptionsBuilder().UseIceberg(Catalog, "shop").Options);
        var read = await fresh.Set<Explicit>().SingleAsync(x => x.Name == "a");
        Assert.Equal(3, read.Size);
    }

    public sealed class Parent
    {
        public long Id { get; set; }
        public List<Child> Children { get; set; } = [];
    }

    public sealed class Child
    {
        public long Id { get; set; }
        public long ParentId { get; set; }
    }

    private sealed class RelationshipContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Parent> Parents => Set<Parent>();
    }

    [Fact]
    public void Relationships_are_refused_at_model_validation()
    {
        using var ctx = new RelationshipContext(new DbContextOptionsBuilder().UseIceberg(Catalog, "shop").Options);
        var e = Assert.Throws<InvalidOperationException>(() => ctx.Model);
        Assert.Contains("relationships", e.Message);
    }

    [Fact]
    public async Task Explicit_transactions_are_not_supported()
    {
        await using var ctx = await CreatedAsync();
        Assert.Throws<NotSupportedException>(() => ctx.Database.BeginTransaction());
    }

    [Fact]
    public async Task Querying_a_missing_table_says_how_to_create_it()
    {
        await using var ctx = NewContext();
        await Catalog.CreateNamespaceAsync(Ns);
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.Orders.ToListAsync());
        Assert.Contains("EnsureCreated", e.Message);
    }

    [Fact]
    public async Task A_property_without_a_column_fails_with_the_fix()
    {
        await Catalog.CreateNamespaceAsync(Ns);
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Require(2, "customer", PrimitiveType.String)]);
        await Catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("shop", "orders"), schema));
        await using var ctx = NewContext();
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.Orders.ToListAsync());
        Assert.Contains("UpdateSchemaAsync", e.Message);
    }
}
