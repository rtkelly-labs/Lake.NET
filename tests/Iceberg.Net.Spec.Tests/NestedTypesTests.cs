using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Struct, list and map columns round-trip through Parquet with every null/empty shape Dremel levels can express.</summary>
[Spec("Nested Types")]
[Spec("Parquet")]
public sealed class NestedTypesTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-nested", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public NestedTypesTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("n")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    // id, location struct<lat double, lon double, label string>, tags list<string?>, scores map<string, int?>,
    // events list<struct<at long, kind string, amounts list<decimal(38,4)>>>, matrix list<list<int>> (required elements)
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "location", new StructType([
            NestedField.Require(10, "lat", PrimitiveType.Double),
            NestedField.Require(11, "lon", PrimitiveType.Double),
            NestedField.Optional(12, "label", PrimitiveType.String)])),
        NestedField.Optional(3, "tags", new ListType(20, PrimitiveType.String, ElementRequired: false)),
        NestedField.Optional(4, "scores", new MapType(30, PrimitiveType.String, 31, PrimitiveType.Int, ValueRequired: false)),
        NestedField.Optional(5, "events", new ListType(40, new StructType([
            NestedField.Require(41, "at", PrimitiveType.Long),
            NestedField.Optional(42, "kind", PrimitiveType.String),
            NestedField.Optional(43, "amounts", new ListType(44, new DecimalType(38, 4), ElementRequired: true))]), ElementRequired: false)),
        NestedField.Require(6, "matrix", new ListType(50, new ListType(51, PrimitiveType.Int, ElementRequired: true), ElementRequired: true)),
        NestedField.Optional(7, "ids", new MapType(60, PrimitiveType.Uuid, 61, new StructType([NestedField.Optional(62, "n", PrimitiveType.Long)]), ValueRequired: true)),
    ]);

    private static IcebergDecimal D(string s) => IcebergDecimal.Parse(s);

    internal static List<object?[]> Rows() =>
    [
        [1L, new object?[] { 51.5, -0.12, "London" }, new List<object?> { "a", null, "c" }, new Dictionary<object, object?> { ["x"] = 1, ["y"] = null },
            new List<object?> { new object?[] { 10L, "play", new List<object?> { D("1.2345"), D("-99999999999999999999999999999999.9999") } }, null, new object?[] { 11L, null, null } },
            new List<object?> { new List<object?> { 1, 2 }, new List<object?>(), new List<object?> { 3 } },
            new Dictionary<object, object?> { [Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e")] = new object?[] { 5L } }],
        [2L, null, null, null, null, new List<object?>(), null],
        [3L, new object?[] { 0.0, 0.0, null }, new List<object?>(), new Dictionary<object, object?>(), new List<object?>(),
            new List<object?> { new List<object?>() }, new Dictionary<object, object?> { [Guid.Empty] = new object?[] { null } }],
        [4L, new object?[] { 1.0, 2.0, "" }, new List<object?> { null }, new Dictionary<object, object?> { [""] = 0 },
            new List<object?> { null, null }, new List<object?> { new List<object?> { -1 } }, new Dictionary<object, object?>()],
    ];

    private async Task<Table> Create(int formatVersion = 2) =>
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("n", $"t{Guid.NewGuid():N}"), Schema, PartitionSpec.Unpartitioned, FormatVersion: formatVersion));

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Nested_values_round_trip(int formatVersion)
    {
        var t = await Create(formatVersion);
        await TableMutations.InsertAsync(t, Rows());
        var read = (await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal(Canonical(Rows()), Canonical(read));
        await TableValidator.AssertValidAsync(t, new ValidationOptions { Deep = true });
    }

    [Fact]
    public async Task Leaf_column_sizes_are_keyed_by_leaf_field_id()
    {
        var t = await Create();
        await TableMutations.InsertAsync(t, Rows());
        var file = Assert.Single((await TableScan.PlanAsync(t, Expression.True)).Tasks).File;
        Assert.Equal(t.Schema.Columns.SelectMany(Leaves).Order(), file.ColumnSizes!.Keys.Order()); // ids are reassigned on create
        Assert.Equal(13, file.ColumnSizes!.Count);
        Assert.Equal(4, file.ValueCounts![1]); // top-level primitive metrics still recorded
    }

    [Fact]
    public async Task Rows_with_nested_columns_can_be_deleted_and_upserted()
    {
        var t = await Create(3);
        await TableMutations.InsertAsync(t, Rows());
        await TableMutations.DeleteWhereAsync(t, Expr.Equal("id", 2L));
        var replacement = Rows()[0];
        replacement[2] = new List<object?> { "z" };
        await TableMutations.UpsertAsync(t, [replacement], ["id"], DeleteStrategy.PositionDeletes);
        var read = (await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal([1L, 3L, 4L], read.Select(r => (long)r[0]!));
        Assert.Equal(["z"], (List<object?>)read[0][2]!);
        await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(RewriteAll: true));
        Assert.Equal(Canonical(read), Canonical((await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!).ToList()));
    }

    [Fact]
    public async Task Fields_added_to_a_struct_read_as_null_from_older_files()
    {
        var t = await Create();
        await TableMutations.InsertAsync(t, Rows());
        await t.UpdateSchemaAsync(u => u.AddColumn("location", "alt", PrimitiveType.Double).DeleteColumn("location.label"));
        var read = (await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal(new object?[] { 51.5, -0.12, null }, (object?[])read[0][1]!); // lat, lon, alt
    }

    [Fact]
    public async Task Required_nested_values_are_enforced()
    {
        var t = await Create();
        var bad = Rows()[1];
        bad[5] = null; // matrix is required
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.InsertAsync(t, [bad]));
        var badElement = Rows()[1];
        badElement[5] = new List<object?> { null }; // matrix elements are required
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.InsertAsync(t, [badElement]));
    }

    private static IEnumerable<int> Leaves(NestedField f) => f.Type switch
    {
        StructType s => s.Fields.SelectMany(Leaves),
        ListType l => Leaves(l.ElementField),
        MapType m => Leaves(m.KeyField).Concat(Leaves(m.ValueField)),
        _ => [f.Id],
    };

    /// <summary>A comparable rendering: dictionaries by key order, decimals and GUIDs as text.</summary>
    internal static string Canonical(IEnumerable<object?[]> rows) => string.Join("\n", rows.Select(r => Render(r)));

    private static string Render(object? v) => v switch
    {
        null => "null",
        string s => $"\"{s}\"",
        object?[] a => "(" + string.Join(", ", a.Select(Render)) + ")",
        System.Collections.IDictionary d => "{" + string.Join(", ", d.Keys.Cast<object>().Select(k => (K: Render(k), V: Render(d[k]))).OrderBy(x => x.K, StringComparer.Ordinal).Select(x => $"{x.K}: {x.V}")) + "}",
        System.Collections.IEnumerable e => "[" + string.Join(", ", e.Cast<object?>().Select(Render)) + "]",
        double x => x.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
        _ => Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)!,
    };
}
