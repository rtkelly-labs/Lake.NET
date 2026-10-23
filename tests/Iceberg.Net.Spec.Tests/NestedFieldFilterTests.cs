using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Filters, metrics pruning and partitioning on fields inside structs (by full dotted name).</summary>
[Spec("Scan Planning")]
[Spec("Partitioning")]
[Spec("Nested Types")]
public sealed class NestedFieldFilterTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-nestedfilter", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public NestedFieldFilterTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("n")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "place", new StructType([
            NestedField.Require(3, "city", PrimitiveType.String),
            NestedField.Optional(4, "geo", new StructType([NestedField.Require(5, "lat", PrimitiveType.Double), NestedField.Optional(6, "lon", PrimitiveType.Double)]))])),
        NestedField.Require(7, "meta", new StructType([NestedField.Require(8, "source", PrimitiveType.String)])),
    ]);

    private static List<object?[]> Rows(int from, int n) => Enumerable.Range(from, n).Select(i => new object?[]
    {
        (long)i,
        i % 5 == 0 ? null : new object?[] { i % 2 == 0 ? "London" : "Paris", i % 3 == 0 ? null : new object?[] { i * 1.5, i % 4 == 0 ? null : -i * 0.5 } },
        new object?[] { i < 50 ? "batch" : "stream" },
    }).ToList();

    private async Task<Table> Create(PartitionSpec? spec = null, int formatVersion = 2) =>
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("n", $"t{Guid.NewGuid():N}"), Schema, spec, FormatVersion: formatVersion));

    public static TheoryData<string> Filters() => new()
    {
        "place.city == 'Paris'", "place.geo.lat > 30", "place.geo.lon is null", "place.city is null", "meta.source is null",
        "meta.source == 'stream' and place.geo.lat < 90", "place.geo.lat is not null", "not place.city == 'London'",
    };

    private static Expression Parse(string f) => f switch
    {
        "place.city == 'Paris'" => Expr.Equal("place.city", "Paris"),
        "place.geo.lat > 30" => Expr.GreaterThan("place.geo.lat", 30.0),
        "place.geo.lon is null" => Expr.IsNull("place.geo.lon"),
        "place.city is null" => Expr.IsNull("place.city"), // required, but null whenever place is
        "meta.source is null" => Expr.IsNull("meta.source"), // required all the way down: never null
        "meta.source == 'stream' and place.geo.lat < 90" => Expr.And(Expr.Equal("meta.source", "stream"), Expr.LessThan("place.geo.lat", 90.0)),
        "place.geo.lat is not null" => Expr.NotNull("place.geo.lat"),
        "not place.city == 'London'" => new Not(Expr.Equal("place.city", "London")),
        _ => throw new ArgumentException(f),
    };

    /// <summary>Reference semantics, written out per row: a null struct makes every field below it null.</summary>
    private static bool Expected(string f, object?[] r)
    {
        var place = (object?[]?)r[1];
        var city = (string?)place?[0];
        var geo = (object?[]?)place?[1];
        var lat = (double?)geo?[0];
        var lon = (double?)geo?[1];
        var source = (string)((object?[])r[2]!)[0]!;
        return f switch
        {
            "place.city == 'Paris'" => city == "Paris",
            "place.geo.lat > 30" => lat > 30,
            "place.geo.lon is null" => lon is null,
            "place.city is null" => city is null,
            "meta.source is null" => false,
            "meta.source == 'stream' and place.geo.lat < 90" => source == "stream" && lat < 90,
            "place.geo.lat is not null" => lat is not null,
            "not place.city == 'London'" => city != "London", // NotEq is true for nulls (Java semantics)
            _ => throw new ArgumentException(f),
        };
    }

    [Theory]
    [MemberData(nameof(Filters))]
    public async Task Filters_on_nested_fields_select_the_right_rows(string filter)
    {
        var t = await Create();
        for (var b = 0; b < 4; b++) await TableMutations.InsertAsync(t, Rows(b * 25, 25)); // 4 files with disjoint ids
        var expected = Rows(0, 100).Where(r => Expected(filter, r)).Select(r => (long)r[0]!).Order().ToList();
        Assert.Equal(expected, (await TableScan.ReadAsync(t, Parse(filter))).Select(r => (long)r[0]!).Order());
        // Deleting by the same filter removes exactly those rows.
        await TableMutations.DeleteWhereAsync(t, Parse(filter));
        Assert.Equal(100 - expected.Count, (await TableScan.ReadAllAsync(t)).Count);
    }

    [Fact]
    public async Task Metrics_on_struct_leaves_prune_files()
    {
        var t = await Create();
        for (var b = 0; b < 4; b++) await TableMutations.InsertAsync(t, Rows(b * 25, 25));
        var file = (await TableScan.PlanAsync(t, Expression.True)).Tasks[0].File;
        var lat = t.Schema.FindField("place.geo.lat")!.Id; // ids are reassigned on create
        var lon = t.Schema.FindField("place.geo.lon")!.Id;
        Assert.True(file.LowerBounds!.ContainsKey(lat) && file.NullValueCounts!.ContainsKey(lon));
        // lat = 1.5 * id: only the last file (ids 75..99) can hold lat > 120.
        var plan = await TableScan.PlanAsync(t, Expr.GreaterThan("place.geo.lat", 120.0));
        Assert.Single(plan.Tasks);
        Assert.Equal(3, plan.FilesSkippedByMetrics);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Tables_partition_by_fields_inside_structs(int formatVersion)
    {
        var spec = PartitionSpec.BuilderFor(Schema).Identity("place.city").Bucket("meta.source", 4).Build();
        var t = await Create(spec, formatVersion);
        await TableMutations.InsertAsync(t, Rows(0, 100));
        var files = (await TableScan.PlanAsync(t, Expression.True)).Tasks.Select(x => x.File).ToList();
        Assert.Equal(["London", "Paris", null], files.Select(f => (string?)f.Partition[0]).Distinct().OrderBy(c => c ?? "~", StringComparer.Ordinal));
        await TableValidator.AssertValidAsync(t, new ValidationOptions { Deep = true }); // every row's partition recomputed

        var paris = await TableScan.PlanAsync(t, Expr.Equal("place.city", "Paris"));
        Assert.True(paris.FilesSkippedByPartition > 0);
        Assert.All(paris.Tasks, x => Assert.Equal("Paris", x.File.Partition[0]));
        Assert.Equal(Rows(0, 100).Count(r => Expected("place.city == 'Paris'", r)), (await TableScan.ReadAsync(t, Expr.Equal("place.city", "Paris"))).Count);
    }

    [Fact]
    public void Lists_and_maps_cannot_be_filtered_through()
    {
        var schema = new Schema(0, [NestedField.Optional(1, "tags", new ListType(2, PrimitiveType.String, false)), NestedField.Optional(3, "s", new StructType([NestedField.Optional(4, "x", PrimitiveType.Int)]))]);
        Assert.ThrowsAny<Exception>(() => Binder.Bind(schema, Expr.Equal("tags.element", "a")));
        Assert.Throws<NotSupportedException>(() => Binder.Bind(schema, Expr.IsNull("s")));
        Assert.Equal("s.x == 1", Binder.Bind(schema, Expr.Equal("s.x", 1)).ToString());
    }

    [Fact]
    public async Task String_bounds_follow_code_point_order()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "s", PrimitiveType.String)]);
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("n", "cp"), schema));
        // U+FFFF sorts before U+10000 by code point, after it in UTF-16 (a surrogate pair starts with 0xD800).
        await TableMutations.InsertAsync(t, [[1L, "￿"], [2L, "\U00010000"]]);
        var file = (await TableScan.PlanAsync(t, Expression.True)).Tasks.Single().File;
        Assert.Equal("￿", System.Text.Encoding.UTF8.GetString(file.LowerBounds![2]));
        Assert.Equal("\U00010000", System.Text.Encoding.UTF8.GetString(file.UpperBounds![2]));
        Assert.Single(await TableScan.ReadAsync(t, Expr.Equal("s", "\U00010000")));
    }
}
