using Apache.Arrow.Ipc;
using Iceberg.Net.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.Regression.Tests;

/// <summary>Our Arrow batches match PyIceberg's <c>to_arrow()</c> of the same table: types, field IDs and values.</summary>
[Trait("Category", "Regression")]
public sealed class ArrowCrossEngineTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-arrow-x", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    [SkippableTheory]
    [InlineData(2, false)]
    [InlineData(3, false)]
    [InlineData(2, true)]
    [InlineData(3, true)]
    public async Task Arrow_output_matches_pyiceberg_to_arrow(int formatVersion, bool nested)
    {
        Require.LocalOracle();
        using var catalog = new InMemoryCatalog(_dir);
        await catalog.CreateNamespaceAsync(Namespace.Of("x"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("x", "t"), nested ? ShadowCompareTests.Nested : SampleData.AllTypes, FormatVersion: formatVersion));
        await TableArrow.InsertAsync(t, ArrowInterop.ToRecordBatch(t.Schema, nested ? ShadowCompareTests.NestedRows(25) : SampleData.AllTypeRows(25)));
        await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 4 == 1, DeleteStrategy.PositionDeletes);

        var ipc = Path.Combine(_dir, "rows.arrow");
        await using (var stream = File.Create(ipc))
        {
            using var writer = new ArrowFileWriter(stream, ArrowInterop.ToArrow(t.Schema));
            await foreach (var batch in TableArrow.ReadBatchesAsync(t)) await writer.WriteRecordBatchAsync(batch);
            await writer.WriteEndAsync();
        }
        var py = await TestEnvironment.Current.OracleAsync("compare-arrow", t.MetadataLocation!, ipc);

        foreach (var (column, pair) in py["types"]!.AsObject())
            Assert.True(pair![0]!.GetValue<string>() == pair[1]!.GetValue<string>(), $"{column}: ours {pair[0]} vs PyIceberg {pair[1]}");
        // We always carry field IDs; PyIceberg 0.12's to_arrow() drops them, so they are compared only where present.
        foreach (var (column, pair) in py["field_ids"]!.AsObject())
        {
            Assert.False(string.IsNullOrEmpty(pair![0]?.GetValue<string>()), $"{column} has no field id");
            if (pair[1]?.GetValue<string>() is { Length: > 0 } theirs) Assert.Equal(theirs, pair[0]!.GetValue<string>());
        }
        Assert.Equal(py["rows"]![1]!.GetValue<int>(), py["rows"]![0]!.GetValue<int>());
        Assert.True(py["mismatch"] is null, py["mismatch"]?.ToString());
    }
}
