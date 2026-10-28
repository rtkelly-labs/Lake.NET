using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq.Tests;

[Spec("Default values")]
public sealed class WriteDefaultTests : CatalogTest
{
    public sealed class IdOnly { [IcebergField(1)] public long Id { get; init; } }
    public sealed class ExplicitValue
    {
        [IcebergField(1)] public long Id { get; init; }
        [IcebergField(2)] public int? Extra { get; init; }
    }
    public sealed class NamedValue
    {
        [IcebergField(1)] public long Id { get; init; }
        [IcebergField(3)] public string? Name { get; init; }
    }
    public sealed class Envelope { [IcebergField(10)] public Child? Child { get; init; } }
    public sealed class Child { [IcebergField(11)] public long Id { get; init; } }

    private Task<Table> CreateAsync(string format, bool required) => Catalog.CreateTableAsync(new CreateTableRequest(NewId(), new Schema(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        new NestedField(2, "extra", PrimitiveType.Int, required, WriteDefault: JsonValue.Create(7)),
        NestedField.Optional(3, "name", PrimitiveType.String),
    ], identifierFieldIds: [1]), Properties: new Dictionary<string, string> { ["write.format.default"] = format }, FormatVersion: 3));

    [Theory]
    [InlineData("parquet", false)]
    [InlineData("parquet", true)]
    [InlineData("avro", false)]
    [InlineData("avro", true)]
    public async Task Omitted_columns_receive_the_write_default(string format, bool required)
    {
        var table = await CreateAsync(format, required);
        await table.InsertAsync([new IdOnly { Id = 1 }]);
        var row = Assert.Single(await TableScan.ReadAllAsync(table));
        Assert.Equal(7, row[1]);
        await TableValidator.AssertValidAsync(table, new ValidationOptions { Deep = true });
    }

    [Theory]
    [InlineData("parquet")]
    [InlineData("avro")]
    public async Task Explicit_null_in_a_required_column_fails_without_committing(string format)
    {
        var table = await CreateAsync(format, required: true);
        await Assert.ThrowsAsync<ArgumentException>(() => table.InsertAsync([new ExplicitValue { Id = 1, Extra = null }]));
        await table.RefreshAsync();
        Assert.Empty(table.Metadata.Snapshots);
    }

    [Theory]
    [InlineData("parquet")]
    [InlineData("avro")]
    public async Task Explicit_null_and_explicit_values_are_not_replaced_by_defaults(string format)
    {
        var table = await CreateAsync(format, required: false);
        await table.InsertAsync([new ExplicitValue { Id = 1, Extra = null }, new ExplicitValue { Id = 2, Extra = 0 }]);
        var rows = (await TableScan.ReadAllAsync(table)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Null(rows[0][1]);
        Assert.Equal(0, rows[1][1]);
    }

    [Theory]
    [InlineData("parquet")]
    [InlineData("avro")]
    public async Task Merge_preserves_unmapped_existing_values_and_defaults_only_inserted_rows(string format)
    {
        var table = await CreateAsync(format, required: true);
        await TableMutations.InsertAsync(table, [[1L, 99, "before"]]);
        await table.MergeAsync([new NamedValue { Id = 1, Name = "after" }, new NamedValue { Id = 2, Name = "new" }], x => x.Id,
            m => m.WhenMatchedUpdateAll().WhenNotMatchedInsert());
        var rows = (await TableScan.ReadAllAsync(table)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal("after", rows[0][2]);
        Assert.Equal(99, rows[0][1]);
        Assert.Equal(7, rows[1][1]);
    }

    [Fact]
    public void Nested_defaults_are_independent_and_a_null_struct_stays_null()
    {
        var schema = new Schema(0, [NestedField.Optional(10, "child", new StructType(
        [
            NestedField.Require(11, "id", PrimitiveType.Long),
            new NestedField(12, "score", PrimitiveType.Int, true, WriteDefault: JsonValue.Create(7)),
            new NestedField(13, "bytes", PrimitiveType.Binary, false, WriteDefault: JsonValue.Create("0102")),
        ]))]);
        var binding = new ReflectionRowMapper<Envelope>().Bind(schema);
        var first = (object?[])binding.ToRow(new Envelope { Child = new Child { Id = 1 } })[0]!;
        ((byte[])first[2]!)[0] = 99;
        var second = (object?[])binding.ToRow(new Envelope { Child = new Child { Id = 2 } })[0]!;
        Assert.Equal(7, second[1]);
        Assert.Equal(new byte[] { 1, 2 }, (byte[])second[2]!);
        Assert.Null(binding.ToRow(new Envelope())[0]);
    }
}
