using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Tables with <c>write.format.default=avro</c>: we write Avro data files for every type and read them back, with deletes, metrics and lineage.</summary>
[Spec("Avro")]
public sealed class AvroDataFileTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-avrodata", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public AvroDataFileTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("a")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private Task<Table> Create(Types.Schema schema, int formatVersion, PartitionSpec? spec = null) =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("a", $"t{Guid.NewGuid():N}"), schema, spec, FormatVersion: formatVersion,
            Properties: new Dictionary<string, string> { ["write.format.default"] = "avro" }));

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Every_primitive_type_round_trips(int formatVersion)
    {
        var t = await Create(SampleData.AllTypes, formatVersion);
        var rows = SampleData.AllTypeRows(40);
        await TableMutations.InsertAsync(t, rows);
        var file = (await TableScan.PlanAsync(t, Expression.True)).Tasks.Single().File;
        Assert.Equal("AVRO", file.FileFormat);
        Assert.EndsWith(".avro", file.FilePath);
        Assert.Equal(NestedTypesTests.Canonical(rows), NestedTypesTests.Canonical((await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!)));
        await TableValidator.AssertValidAsync(t, new ValidationOptions { Deep = true });
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Nested_types_round_trip(int formatVersion)
    {
        var t = await Create(NestedTypesTests.SchemaForTests, formatVersion);
        await TableMutations.InsertAsync(t, NestedTypesTests.Rows());
        Assert.Equal(NestedTypesTests.Canonical(NestedTypesTests.Rows()), NestedTypesTests.Canonical((await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!)));
    }

    [Theory]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(2, DeleteStrategy.EqualityDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.CopyOnWrite)]
    public async Task Deletes_metrics_and_lineage_work_on_avro_files(int formatVersion, DeleteStrategy strategy)
    {
        var schema = new Types.Schema(0, [Types.NestedField.Require(1, "id", Types.PrimitiveType.Long), Types.NestedField.Optional(2, "v", Types.PrimitiveType.String)], identifierFieldIds: [1]);
        var t = await Create(schema, formatVersion);
        for (var b = 0; b < 3; b++) await TableMutations.InsertAsync(t, Enumerable.Range(b * 10, 10).Select(i => new object?[] { (long)i, $"v{i}" }).ToList());
        await TableMutations.UpsertAsync(t, [[3L, "changed"], [100L, "new"]], ["id"], strategy);
        await TableMutations.DeleteByKeysAsync(t, ["id"], [[4L], [25L]], strategy);
        var rows = (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => (string?)r[1]);
        Assert.Equal(29, rows.Count);
        Assert.Equal("changed", rows[3]);
        Assert.False(rows.ContainsKey(25));
        // Metrics prune Avro files like Parquet ones.
        Assert.True((await TableScan.PlanAsync(t, Expr.GreaterThan("id", 21L))).FilesSkippedByMetrics > 0);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
        if (formatVersion >= 3)
        {
            var lineage = await TableScan.ReadWithLineageAsync(t);
            Assert.Equal(lineage.Count, lineage.Select(r => r.RowId).Distinct().Count());
            Assert.All(lineage, r => Assert.NotNull(r.RowId));
        }
        await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(RewriteAll: true));
        Assert.Equal(rows, (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => (string?)r[1]));
    }

    [Fact]
    public async Task Unsupported_formats_are_refused()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("a", "orc"), SampleData.AllTypes,
            Properties: new Dictionary<string, string> { ["write.format.default"] = "orc" }));
        await Assert.ThrowsAsync<NotSupportedException>(() => TableMutations.InsertAsync(t, SampleData.AllTypeRows(1)));
    }
}
