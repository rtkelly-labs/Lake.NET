using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Merge-on-read deletes and Puffin files written by .NET, verified by independent readers:
/// PyIceberg's scan (applies v2 position deletes and v3 deletion vectors), PyIceberg's Puffin/DV decoder,
/// zlib CRC-32, and Apache DataSketches for Theta sketches. Runs on the local filesystem (no emulator).
/// </summary>
[Trait("Category", "Regression")]
public sealed class PuffinCrossEngineTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-x", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public PuffinCrossEngineTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private async Task<(Table Table, IReadOnlyList<DataFile> Files)> Create(string name, int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", name), CatalogConformanceTests.Schema, CatalogConformanceTests.Spec, FormatVersion: formatVersion));
        var files = await TableWriter.WriteAsync(t, CatalogConformanceTests.Rows(0, 40));
        await SnapshotProducer.Append(t).AddFiles(files).CommitAsync();
        return (t, files);
    }

    private static async Task<List<long>> PyIcebergIds(Table t) =>
        (await TestEnvironment.Current.OracleAsync("read-metadata", t.MetadataLocation!))["ids"]!.AsArray().Select(i => i!.GetValue<long>()).ToList();

    private static async Task<List<long>> OurIds(Table t) => (await TableScan.ReadAllAsync(t)).Select(r => (long)r[0]!).Order().ToList();

    /// <summary>Delete the first two rows of the three biggest files.</summary>
    private static Dictionary<string, IEnumerable<long>> SomeDeletes(IReadOnlyList<DataFile> files) =>
        files.OrderByDescending(f => f.RecordCount).ThenBy(f => f.FilePath, StringComparer.Ordinal).Take(3)
            .ToDictionary(f => f.FilePath, f => (IEnumerable<long>)Enumerable.Range(0, (int)Math.Min(2, f.RecordCount)).Select(i => (long)i).ToList());

    [SkippableFact]
    public async Task Pyiceberg_applies_our_v2_position_deletes()
    {
        Require.LocalOracle();
        var (t, files) = await Create("v2pos", 2);
        await SnapshotProducer.DeleteRows(t, SomeDeletes(files)).CommitAsync();
        var ours = await OurIds(t);
        Assert.True(ours.Count < 40);
        Assert.Equal(ours, await PyIcebergIds(t));
    }

    [SkippableFact]
    public async Task Pyiceberg_applies_our_v3_deletion_vectors_and_decodes_them()
    {
        Require.LocalOracle();
        var (t, files) = await Create("v3dv", 3);
        var deletes = SomeDeletes(files);
        await SnapshotProducer.DeleteRows(t, deletes).CommitAsync();
        await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [deletes.Keys.First()] = [deletes.Values.First().Count()] }).CommitAsync(); // merge
        var ours = await OurIds(t);
        Assert.Equal(40 - 7, ours.Count);
        Assert.Equal(ours, await PyIcebergIds(t));

        // The merged DV lives in the second commit's Puffin file; the other two stay in the first.
        var index = await TableScan.DeletesAsync(t);
        Assert.Equal(2, index.All.Select(e => e.File.FilePath).Distinct().Count());
        var dvFile = index.DeletionVectorFor(deletes.Keys.First())!.File.FilePath;
        var inspected = await TestEnvironment.Current.OracleAsync("inspect-puffin", dvFile);
        Assert.All(inspected["blobs"]!.AsArray(), b => Assert.True(b!["crc_ok"]!.GetValue<bool>()));
        var decoded = inspected["deletion_vectors"]!.AsObject();
        Assert.Equal([0L, 1L, 2L], decoded[deletes.Keys.First()]!.AsArray().Select(p => p!.GetValue<long>()));
    }

    [SkippableFact]
    public async Task Converted_table_reads_identically_in_pyiceberg()
    {
        Require.LocalOracle();
        var (t, files) = await Create("convert", 2);
        await SnapshotProducer.DeleteRows(t, SomeDeletes(files)).CommitAsync();
        var before = await PyIcebergIds(t);

        await t.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(3)]);
        var snap = await SnapshotProducer.ConvertPositionDeletesToDeletionVectors(t).CommitAsync();
        Assert.Equal("replace", snap.Operation);
        Assert.All((await TableScan.DeletesAsync(t)).All, e => Assert.True(e.IsDeletionVector));
        Assert.Equal(before, await PyIcebergIds(t));
        Assert.Equal(before, await OurIds(t));
    }

    [SkippableFact]
    public async Task Datasketches_reads_our_ndv_statistics()
    {
        Require.LocalOracle();
        var (t, _) = await Create("stats", 3);
        var stats = await PuffinStatistics.ComputeNdvAsync(t);
        var inspected = await TestEnvironment.Current.OracleAsync("inspect-puffin", stats.StatisticsPath);
        var blobs = inspected["blobs"]!.AsArray();
        Assert.Equal(t.Schema.Columns.Count, blobs.Count);
        var ours = await PuffinStatistics.ReadNdvAsync(t, stats);
        foreach (var b in blobs)
        {
            var fieldId = b!["fields"]![0]!.GetValue<int>();
            Assert.Equal(ours[fieldId], b["estimate"]!.GetValue<double>(), 6);
            Assert.Equal(((long)Math.Round(ours[fieldId])).ToString(), b["properties"]!["ndv"]!.GetValue<string>());
        }
        Assert.Equal(40, ours[1]);  // id is unique
        Assert.Equal(2, ours[5]);   // region: eu / us
    }
}
