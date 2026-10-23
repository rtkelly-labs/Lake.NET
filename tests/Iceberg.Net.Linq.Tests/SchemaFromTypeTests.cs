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
        // A catalog would renumber them on create: refuse rather than create a table the model's IDs do not match.
        await Assert.ThrowsAsync<InvalidOperationException>(() => Catalog.CreateTableAsync<Explicit>(NewId(), Snake));
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
