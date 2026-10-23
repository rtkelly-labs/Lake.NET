using System.Numerics;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Decimals above 18 digits (fixed-length, minimal width) and fixed(L) columns (spec Appendix A, Parquet).</summary>
[Spec("Parquet")]
[Spec("Primitive Types")]
public sealed class WideTypesTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-wide", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    [Theory]
    [InlineData(1, 1)] [InlineData(2, 1)] [InlineData(3, 2)] [InlineData(9, 4)] [InlineData(10, 5)] [InlineData(18, 8)]
    [InlineData(19, 9)] [InlineData(28, 12)] [InlineData(29, 13)] [InlineData(38, 16)]
    public void Decimal_width_is_the_smallest_that_fits(int precision, int bytes) => Assert.Equal(bytes, ParquetFiles.DecimalBytes(precision));

    [Fact]
    public async Task Wide_decimals_and_fixed_round_trip_with_metrics()
    {
        var schema = new Schema(0,
        [
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "d38", new DecimalType(38, 10)),
            NestedField.Optional(3, "d20", new DecimalType(20, 0)),
            NestedField.Optional(4, "fx", new FixedType(3)),
        ]);
        var max38 = BigInteger.Pow(10, 38) - 1;
        object?[][] rows =
        [
            [1L, new IcebergDecimal(max38, 10), new IcebergDecimal(BigInteger.Pow(10, 20) - 1, 0), new byte[] { 1, 2, 3 }],
            [2L, new IcebergDecimal(-max38, 10), new IcebergDecimal(BigInteger.Parse("-12345678901234567890"), 0), new byte[] { 0xff, 0, 0x80 }],
            [3L, new IcebergDecimal(-1, 10), new IcebergDecimal(0, 0), null],
            [4L, null, null, new byte[] { 0, 0, 0 }],
        ];
        using var catalog = new InMemoryCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("w"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("w", "t"), schema));
        await TableMutations.InsertAsync(t, rows);

        var back = (await TableScan.ReadAllAsync(t)).OrderBy(r => (long)r[0]!).ToList();
        for (var i = 0; i < rows.Length; i++)
        {
            Assert.Equal(rows[i][1], back[i][1]);
            Assert.Equal(rows[i][2], back[i][2]);
            Assert.Equal((byte[]?)rows[i][3], (byte[]?)back[i][3]);
        }
        var file = (await TableScan.PlanFilesAsync(t)).Single().File;
        Assert.Equal(new IcebergDecimal(-max38, 10), Conversions.FromBytes(schema.FindField(2)!.Type, file.LowerBounds![2]));
        Assert.Equal(new IcebergDecimal(max38, 10), Conversions.FromBytes(schema.FindField(2)!.Type, file.UpperBounds![2]));
        Assert.Single(await TableScan.ReadAsync(t, Expressions.Expr.LessThan("d38", new IcebergDecimal(-max38 + 1, 10))));
        await TableValidator.AssertValidAsync(t, new ValidationOptions { Deep = true });
    }

    [Fact]
    public async Task Fixed_values_must_have_the_declared_length()
    {
        using var catalog = new InMemoryCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("w"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("w", "bad"), new Schema(0, [NestedField.Optional(1, "fx", new FixedType(2))])));
        await Assert.ThrowsAsync<ArgumentException>(() => TableMutations.InsertAsync(t, [[new byte[] { 1, 2, 3 }]]));
    }
}
