using ParquetSharp;
using ParquetSharp.Schema;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

public sealed class NameMappingAndImportTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public NameMappingAndImportTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    [Spec("Name Mapping Serialization")]
    [Fact]
    public void Name_mapping_json_matches_the_spec_shape_and_round_trips()
    {
        var schema = new Schema(
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "loc", new StructType(NestedField.Optional(3, "lat", PrimitiveType.Double))),
            NestedField.Optional(4, "tags", new ListType(5, PrimitiveType.String, false)));
        var mapping = NameMapping.FromSchema(schema).WithAlias(1, "legacy_id");
        Assert.Equal(
            """[{"field-id":1,"names":["id","legacy_id"]},{"field-id":2,"names":["loc"],"fields":[{"field-id":3,"names":["lat"]}]},{"field-id":4,"names":["tags"],"fields":[{"field-id":5,"names":["element"]}]}]""",
            mapping.ToJson());
        Assert.Equal(mapping.ToJson(), NameMapping.Parse(mapping.ToJson()).ToJson());
    }

    [Spec("Parquet")]
    [Spec("Primitive Types")]
    [Fact]
    public async Task Decimal_and_uuid_columns_round_trip()
    {
        var schema = new Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "amount", new DecimalType(9, 2)),
            NestedField.Optional(3, "big", new DecimalType(18, 4)), NestedField.Require(4, "uid", PrimitiveType.Uuid));
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "dec"), schema));
        var g = Guid.NewGuid();
        var rows = new List<object?[]>
        {
            new object?[] { 1L, IcebergDecimal.Parse("14.20"), IcebergDecimal.Parse("-12345.6789"), g },
            new object?[] { 2L, null, null, Guid.Empty },
        };
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, rows)).CommitAsync();
        var back = (await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal("14.20", back[0][1]!.ToString());
        Assert.Equal("-12345.6789", back[0][2]!.ToString());
        Assert.Equal(g, back[0][3]);
        Assert.Null(back[1][1]);
    }

    /// <summary>A file written without field IDs, with a column that has since been renamed.</summary>
    private static void WriteLegacyFile(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var root = new GroupNode("schema", Repetition.Required, new Node[]
        {
            new PrimitiveNode("id", Repetition.Required, LogicalType.None(), PhysicalType.Int64),
            new PrimitiveNode("old_name", Repetition.Optional, LogicalType.String(), PhysicalType.ByteArray),
        });
        using var w = new ParquetFileWriter(path, root, new WriterPropertiesBuilder().Build());
        using (var rg = w.AppendRowGroup())
        {
            using (var c = rg.NextColumn().LogicalWriter<long>()) c.WriteBatch([1L, 2L]);
            using (var c = rg.NextColumn().LogicalWriter<string?>()) c.WriteBatch(["a", null]);
        }
        w.Close();
    }

    [Spec("Column Projection")]
    [Spec("Name Mapping Serialization")]
    [Fact]
    public async Task Files_without_ids_resolve_through_the_name_mapping_including_renames()
    {
        var schema = new Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String));
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "legacy"), schema));
        var path = Path.Combine(t.Location, "data", "legacy.parquet");
        WriteLegacyFile(path);

        await Assert.ThrowsAsync<ParquetImportException>(() => ParquetImport.ToDataFileAsync(t, path));
        var mapping = NameMapping.FromSchema(t.Schema).WithAlias(2, "old_name");
        await t.SetPropertiesAsync(new Dictionary<string, string> { [NameMapping.Property] = mapping.ToJson() });

        var file = await ParquetImport.ToDataFileAsync(t, path);
        Assert.Equal(1, file.NullValueCounts![2]);
        await SnapshotProducer.Append(t).AddFiles([file]).CommitAsync();
        var rows = (await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal("a", rows[0][1]);
        Assert.Null(rows[1][1]);
    }

    [Spec("Writing data files")]
    [Spec("Parquet")]
    [Fact]
    public async Task Import_rejects_files_that_span_partitions_or_break_required_columns()
    {
        var schema = new Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Require(2, "name", PrimitiveType.String));
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "strict"), schema,
            Partitioning.PartitionSpec.BuilderFor(schema).Identity("id").Build()));
        var path = Path.Combine(t.Location, "data", "legacy.parquet");
        WriteLegacyFile(path);
        await t.SetPropertiesAsync(new Dictionary<string, string> { [NameMapping.Property] = NameMapping.FromSchema(t.Schema).WithAlias(2, "old_name").ToJson() });
        var e = await Assert.ThrowsAsync<ParquetImportException>(() => ParquetImport.ToDataFileAsync(t, path));
        Assert.Contains(e.Problems, p => p.Column == "name" && p.Problem.Contains("nulls"));
    }
}
