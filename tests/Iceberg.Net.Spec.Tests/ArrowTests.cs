using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Iceberg.Net.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;
using IcebergSchema = Iceberg.Net.Types.Schema;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Arrow interop: every primitive type round-trips exactly, field IDs travel as metadata, and batches read and write tables.</summary>
public sealed class ArrowTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-arrow", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    internal static IcebergSchema AllTypes => SampleData.AllTypes;
    internal static List<object?[]> Rows(int n) => SampleData.AllTypeRows(n);

    private static void AssertRowsEqual(IReadOnlyList<object?[]> expected, IReadOnlyList<object?[]> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (var r = 0; r < expected.Count; r++)
            for (var c = 0; c < expected[r].Length; c++)
                if (expected[r][c] is byte[] eb) Assert.Equal(eb, (byte[]?)actual[r][c]);
                else Assert.Equal(expected[r][c], actual[r][c]);
    }

    [Fact]
    public void Every_primitive_type_round_trips_with_field_ids()
    {
        var rows = Rows(40);
        var batch = ArrowInterop.ToRecordBatch(AllTypes, rows);
        Assert.Equal("7", batch.Schema.GetFieldByName("wide").Metadata[ArrowInterop.FieldIdKey]);
        Assert.False(batch.Schema.GetFieldByName("id").IsNullable);
        Assert.Equal("UTC", ((TimestampType)batch.Schema.GetFieldByName("tstz").DataType).Timezone);
        Assert.Null(((TimestampType)batch.Schema.GetFieldByName("ts").DataType).Timezone);
        AssertRowsEqual(rows, ArrowInterop.ToRows(AllTypes, batch));
    }

    [Fact]
    public void Foreign_batches_match_columns_by_field_id_then_name()
    {
        var schema = new IcebergSchema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String), NestedField.Optional(3, "extra", PrimitiveType.Int)]);
        // A producer with its own column order and names: "label" carries field ID 2; "id" matches by name; "extra" is absent.
        var batch = new RecordBatch(
            new Apache.Arrow.Schema([
                new Field("label", StringType.Default, true, [KeyValuePair.Create(ArrowInterop.FieldIdKey, "2")]),
                new Field("id", Int64Type.Default, false),
            ], null),
            [new StringArray.Builder().Append("x").Append("y").Build(), new Int64Array.Builder().Append(10).Append(11).Build()], 2);
        var rows = ArrowInterop.ToRows(schema, batch);
        Assert.Equal(new object?[] { 10L, "x", null }, rows[0]);
        Assert.Equal(new object?[] { 11L, "y", null }, rows[1]);
    }

    [Fact]
    public async Task Batches_write_and_read_tables_with_filters()
    {
        using var catalog = new InMemoryCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("a"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("a", "t"), AllTypes, FormatVersion: 3));
        var rows = Rows(30);
        await TableArrow.InsertAsync(t, ArrowInterop.ToRecordBatch(AllTypes, rows));
        var changed = Rows(30).Take(5).Select(r => { r[11] = "changed"; return r; }).ToList();
        await TableArrow.UpsertAsync(t, ArrowInterop.ToRecordBatch(AllTypes, changed), strategy: DeleteStrategy.PositionDeletes);

        var read = new List<object?[]>();
        await foreach (var batch in TableArrow.ReadBatchesAsync(t, Expr.LessThan("id", 10L))) read.AddRange(ArrowInterop.ToRows(AllTypes, batch));
        Assert.Equal(10, read.Count);
        Assert.Equal(5, read.Count(r => (string?)r[11] == "changed"));
        AssertRowsEqual((await TableScan.ReadAsync(t, Expr.LessThan("id", 10L))).OrderBy(r => (long)r[0]!).ToList(), read.OrderBy(r => (long)r[0]!).ToList());
    }
}
