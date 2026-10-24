using Iceberg.Net.Catalog;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq.Tests;

public sealed class SchemaFromTypeTests : CatalogTest
{
    public sealed class Nested
    {
        [IcebergKey] public long Id { get; set; }
        public string Name { get; set; } = "";
        public string? Nick { get; set; }
        public Point? Where { get; set; }
        public List<string?> Tags { get; set; } = [];
        public Dictionary<string, Point> Places { get; set; } = [];
        [IcebergField(Type = "decimal(9, 3)", Doc = "money")] public decimal Price { get; set; }
        [IcebergField(Nullability = IcebergNullability.Optional)] public int Forced { get; set; }
        [IcebergIgnore] public int Ignored { get; set; }
    }

    [Fact]
    public void Ids_types_and_nullability_follow_the_model()
    {
        var s = IcebergSchema.FromType<Nested>();
        Assert.Equal("""
            table {
              1: Id: required long
              2: Name: required string
              3: Nick: optional string
              4: Where: optional struct<9: X: required double, 10: Y: required double>
              5: Tags: required list<string>
              6: Places: required map<string, struct<14: X: required double, 15: Y: required double>>
              7: Price: required decimal(9, 3)
              8: Forced: optional int
            }
            """.ReplaceLineEndings("\n"), s.ToString());
        Assert.Equal([1], s.IdentifierFieldIds);
        Assert.False(((ListType)s.FindField("Tags")!.Type).ElementRequired);
        Assert.Equal("money", s.FindField("Price")!.Doc);
        // Same IDs a catalog assigns on create, so the model binds to the created table unchanged.
        var last = 0;
        Assert.True(TypeUtil.AssignFreshIds(s, () => ++last).SameSchema(s));
    }

    public sealed class Explicit
    {
        [IcebergField(5)] public long A { get; set; }
        public string? B { get; set; }
        [IcebergField(1)] public int C { get; set; }
    }

    [Fact]
    public async Task Explicit_ids_are_kept_and_others_fill_the_gaps()
    {
        var s = IcebergSchema.FromType<Explicit>(Snake);
        Assert.Equal([5, 2, 1], s.Columns.Select(c => c.Id));
        Assert.Equal(["a", "b", "c"], s.Columns.Select(c => c.Name));
        // Catalogs that build metadata locally keep the model's IDs (CreateTableRequest.PreserveFieldIds).
        var table = await Catalog.CreateTableAsync<Explicit>(NewId(), Snake);
        Assert.Equal([5, 2, 1], table.Schema.Columns.Select(c => c.Id));
        Assert.Equal(5, table.Metadata.LastColumnId);
        await table.InsertAsync([new Explicit { A = 7, B = "b", C = 3 }], new ReflectionRowMapper<Explicit>(Snake));
        var read = Assert.Single(await table.AsQueryable<Explicit>(SnakeQuery).ToListAsync());
        Assert.Equal((7L, "b", 3), (read.A, read.B, read.C));
        // A REST server builds the metadata itself and would renumber them: refuse before creating anything.
        using var rest = new RemoteLikeCatalog(Catalog);
        var id = NewId();
        await Assert.ThrowsAsync<InvalidOperationException>(() => rest.CreateTableAsync<Explicit>(id, Snake));
        Assert.False(await Catalog.TableExistsAsync(id));
    }

    public sealed class Bad
    {
        [IcebergKey] public long? Id { get; set; }
    }

    public sealed class Twice
    {
        [IcebergField(1)] public long A { get; set; }
        [IcebergField(1)] public long B { get; set; }
    }

    public sealed class Recursive
    {
        public Recursive? Next { get; set; }
    }

    [Fact]
    public void Invalid_models_are_rejected()
    {
        Assert.Throws<InvalidOperationException>(() => IcebergSchema.FromType<Bad>());
        Assert.Throws<InvalidOperationException>(() => IcebergSchema.FromType<Twice>());
        Assert.Throws<InvalidOperationException>(() => IcebergSchema.FromType<Recursive>());
    }

    [Fact]
    public async Task A_table_created_from_a_model_round_trips_it()
    {
        var table = await Catalog.CreateTableAsync<Nested>(NewId());
        Assert.True(table.Schema.SameSchema(IcebergSchema.FromType<Nested>()));
        var item = new Nested
        {
            Id = 1, Name = "n", Where = new Point(1, 2), Tags = ["a", null], Places = new() { ["p"] = new Point(3, 4) }, Price = 1.5m, Forced = 3,
        };
        await table.InsertAsync([item]);
        var read = Assert.Single(await table.AsQueryable<Nested>().ToListAsync());
        Assert.Equal(item.Where, read.Where);
        Assert.Equal(item.Tags, read.Tags);
        Assert.Equal(item.Places, read.Places);
        Assert.Equal(1.5m, read.Price);
    }
}

/// <summary>An <see cref="ICatalog"/> that is not a metastore catalog (as a REST client is), forwarding to one.</summary>
internal sealed class RemoteLikeCatalog(ICatalog inner) : ICatalog
{
    public string Name => "remote-like";
    public Iceberg.Net.IO.IFileIO FileIO => inner.FileIO;
    public Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default) => inner.CreateNamespaceAsync(ns, properties, ct);
    public Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default) => inner.ListNamespacesAsync(parent, ct);
    public Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default) => inner.NamespaceExistsAsync(ns, ct);
    public Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default) => inner.DropNamespaceAsync(ns, ct);
    public Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default) => inner.ListTablesAsync(ns, ct);
    public Task<Table> CreateTableAsync(CreateTableRequest request, CancellationToken ct = default) => inner.CreateTableAsync(request with { PreserveFieldIds = false }, ct);
    public Task<Table> LoadTableAsync(TableIdentifier id, CancellationToken ct = default) => inner.LoadTableAsync(id, ct);
    public Task<bool> TableExistsAsync(TableIdentifier id, CancellationToken ct = default) => inner.TableExistsAsync(id, ct);
    public Task<Table> RegisterTableAsync(TableIdentifier id, string metadataLocation, CancellationToken ct = default) => inner.RegisterTableAsync(id, metadataLocation, ct);
    public Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default) => inner.RenameTableAsync(from, to, ct);
    public Task DropTableAsync(TableIdentifier id, bool purge = false, CancellationToken ct = default) => inner.DropTableAsync(id, purge, ct);
    public Task<Iceberg.Net.Metadata.TableMetadata> CommitTableAsync(TableCommit commit, CancellationToken ct = default) => inner.CommitTableAsync(commit, ct);
    public Task CommitTransactionAsync(IReadOnlyList<TableCommit> commits, CancellationToken ct = default) => inner.CommitTransactionAsync(commits, ct);
    public void Dispose() { }
}
