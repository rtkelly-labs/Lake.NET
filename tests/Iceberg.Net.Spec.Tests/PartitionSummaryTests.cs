using System.Text;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Manifest-list partition summaries bound partition values in Iceberg's order, not the CLR's. Found by the generated
/// differential campaign: a binary partition source (truncate[100] over binary) failed the write with
/// "Unable to cast System.Byte[] to IComparable", and strings were ordered by UTF-16 code unit, which puts U+1F600
/// below U+FFFD and makes the summary prune a manifest that holds a matching row.
/// </summary>
[Spec("Manifest Lists")]
[Spec("Partition Transforms")]
public sealed class PartitionSummaryTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-partsummary", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public PartitionSummaryTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("p")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Require(2, "b", PrimitiveType.Binary),
        NestedField.Require(3, "s", PrimitiveType.String),
    ]);

    [Fact]
    public async Task Binary_and_string_partition_bounds_follow_iceberg_order()
    {
        var spec = PartitionSpec.BuilderFor(Schema).Add("b", Transform.Truncate(100)).Identity("s").Build();
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("p", "t"), Schema, spec, FormatVersion: 2));
        await TableMutations.InsertAsync(t, [[1L, new byte[] { 0xFF }, "�"], [2L, new byte[] { 0x01 }, "\U0001F600"]]);

        var manifest = Assert.Single(await TableScan.ManifestsAsync(t, t.Metadata.CurrentSnapshot!));
        var summaries = manifest.Partitions!;
        Assert.Equal(new byte[] { 0x01 }, summaries[0].LowerBound);
        Assert.Equal(new byte[] { 0xFF }, summaries[0].UpperBound);
        Assert.Equal(Encoding.UTF8.GetBytes("�"), summaries[1].LowerBound);
        Assert.Equal(Encoding.UTF8.GetBytes("\U0001F600"), summaries[1].UpperBound);

        Assert.Equal(new[] { 1L }, (await TableScan.ReadAsync(t, Expr.Equal("s", "�"))).Select(r => (long)r[0]!));
    }
}
