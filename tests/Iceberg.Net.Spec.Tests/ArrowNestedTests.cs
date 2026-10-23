using Apache.Arrow;
using Apache.Arrow.Ipc;
using Iceberg.Net.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Partitioning;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Struct, list and map columns through Arrow: schema with field IDs on every child, values exact both ways, IPC and tables.</summary>
[Spec("Nested Types")]
public sealed class ArrowNestedTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-arrow-nested", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private static Types.Schema Schema => NestedTypesTests.SchemaForTests;

    [Fact]
    public void Nested_schema_maps_with_field_ids_on_every_child()
    {
        var arrow = ArrowInterop.ToArrow(Schema);
        var location = (Apache.Arrow.Types.StructType)arrow.GetFieldByName("location").DataType;
        Assert.Equal("10", location.Fields[0].Metadata[ArrowInterop.FieldIdKey]);
        var tags = (Apache.Arrow.Types.ListType)arrow.GetFieldByName("tags").DataType;
        Assert.Equal("20", tags.ValueField.Metadata[ArrowInterop.FieldIdKey]);
        Assert.True(tags.ValueField.IsNullable);
        var scores = (Apache.Arrow.Types.MapType)arrow.GetFieldByName("scores").DataType;
        Assert.Equal("30", scores.KeyField.Metadata[ArrowInterop.FieldIdKey]);
        Assert.False(scores.KeyField.IsNullable);
        Assert.Equal("31", scores.ValueField.Metadata[ArrowInterop.FieldIdKey]);
    }

    [Fact]
    public async Task Nested_rows_round_trip_through_record_batches_and_ipc()
    {
        var rows = NestedTypesTests.Rows();
        var batch = ArrowInterop.ToRecordBatch(Schema, rows);
        Assert.Equal(NestedTypesTests.Canonical(rows), NestedTypesTests.Canonical(ArrowInterop.ToRows(Schema, batch)));

        using var ms = new MemoryStream();
        using (var writer = new ArrowStreamWriter(ms, batch.Schema, leaveOpen: true))
        {
            await writer.WriteRecordBatchAsync(batch);
            await writer.WriteEndAsync();
        }
        ms.Position = 0;
        using var reader = new ArrowStreamReader(ms);
        var back = await reader.ReadNextRecordBatchAsync();
        Assert.Equal(NestedTypesTests.Canonical(rows), NestedTypesTests.Canonical(ArrowInterop.ToRows(Schema, back)));
    }

    [Fact]
    public async Task Tables_with_nested_columns_take_and_give_arrow()
    {
        using var catalog = new InMemoryCatalog(_dir);
        await catalog.CreateNamespaceAsync(Namespace.Of("a"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("a", "t"), Schema, PartitionSpec.Unpartitioned, FormatVersion: 3));
        var rows = NestedTypesTests.Rows(); // ids are reassigned on create: build the batch against the table's schema
        await TableArrow.InsertAsync(t, ArrowInterop.ToRecordBatch(t.Schema, rows));
        var read = new List<object?[]>();
        await foreach (var b in TableArrow.ReadBatchesAsync(t)) read.AddRange(ArrowInterop.ToRows(t.Schema, b));
        Assert.Equal(NestedTypesTests.Canonical(rows), NestedTypesTests.Canonical(read.OrderBy(r => (long)r[0]!)));
    }
}
