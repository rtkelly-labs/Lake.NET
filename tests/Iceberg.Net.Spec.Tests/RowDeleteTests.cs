using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Puffin;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Merge-on-read deletes: v2 position delete files, v3 deletion vectors, and the v2 → v3 converter.</summary>
public sealed class RowDeleteTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public RowDeleteTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    /// <summary>30 rows in 3 files (partitioned by event_date), ids 0..29.</summary>
    private async Task<(Table Table, IReadOnlyList<DataFile> Files)> TableWithData(int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t{formatVersion}"), LocalTableTests.Schema,
            PartitionSpec.BuilderFor(LocalTableTests.Schema).Identity("event_date").Build(), FormatVersion: formatVersion));
        var files = await TableWriter.WriteAsync(t, LocalTableTests.Rows(0, 30));
        await SnapshotProducer.Append(t).AddFiles(files).CommitAsync();
        return (t, files);
    }

    private static async Task<List<ManifestEntry>> DeleteEntries(Table t, bool includeDeleted = false)
    {
        var all = new List<ManifestEntry>();
        foreach (var mf in (await TableScan.ManifestsAsync(t, t.Metadata.CurrentSnapshot!)).Where(m => m.Content == ManifestContent.Deletes))
            all.AddRange(ManifestCodec.ReadManifest(await t.IO.ReadAllAsync(mf.ManifestPath), t.Metadata.SpecById(mf.PartitionSpecId), t.Schema, mf).Entries
                .Where(e => includeDeleted || e.Status != ManifestEntryStatus.Deleted));
        return all;
    }

    private static Dictionary<string, IEnumerable<long>> Del(DataFile f, params long[] positions) => new() { [f.FilePath] = positions };

    [Spec("Position Delete Files")]
    [Fact]
    public async Task V2_row_deletes_write_position_delete_files()
    {
        var (t, files) = await TableWithData(2);
        var before = t.Metadata.CurrentSnapshotId;
        var snap = await SnapshotProducer.DeleteRows(t, Del(files[0], 0, 2)).CommitAsync();

        Assert.Equal(Operations.Delete, snap.Operation);
        Assert.Equal("1", snap.Summary["added-delete-files"]);
        Assert.Equal("2", snap.Summary["total-position-deletes"]);
        var entry = Assert.Single(await DeleteEntries(t));
        Assert.Equal(FileContent.PositionDeletes, entry.DataFile.Content);
        Assert.Equal("PARQUET", entry.DataFile.FileFormat);
        Assert.Equal(files[0].Partition, entry.DataFile.Partition); // delete files carry their data file's partition

        var rows = await TableScan.ReadAllAsync(t);
        Assert.Equal(28, rows.Count);
        Assert.Equal(30, (await TableScan.ReadAllAsync(t, before)).Count);
    }

    [Spec("Deletion Vectors")]
    [Fact]
    public async Task V3_row_deletes_write_one_deletion_vector_per_data_file_and_merge_later_deletes()
    {
        var (t, files) = await TableWithData(3);
        await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [files[0].FilePath] = [0], [files[1].FilePath] = [1, 3] }).CommitAsync();
        var dvs = await DeleteEntries(t);
        Assert.Equal(2, dvs.Count);
        Assert.All(dvs, e => Assert.Equal("PUFFIN", e.DataFile.FileFormat));
        Assert.Single(dvs.Select(e => e.DataFile.FilePath).Distinct()); // one Puffin file, two blobs
        Assert.Equal(27, (await TableScan.ReadAllAsync(t)).Count);

        // A second delete on file 0 must replace its DV with a merged one, not add a second DV.
        var snap = await SnapshotProducer.DeleteRows(t, Del(files[0], 4)).CommitAsync();
        Assert.Equal("1", snap.Summary["removed-dvs"]);
        Assert.Equal("1", snap.Summary["added-dvs"]);
        var live = await DeleteEntries(t);
        Assert.Equal(2, live.Count);
        var merged = live.Single(e => e.DataFile.ReferencedDataFile == files[0].FilePath);
        Assert.Equal(2, merged.DataFile.RecordCount);
        Assert.Equal(26, (await TableScan.ReadAllAsync(t)).Count);
        Assert.Contains(await DeleteEntries(t, includeDeleted: true), e => e.Status == ManifestEntryStatus.Deleted && e.DataFile.ReferencedDataFile == files[0].FilePath);
    }

    [Spec("Deletion Vectors")]
    [Spec("Version 3")]
    [Fact]
    public async Task Converter_folds_v2_position_deletes_into_deletion_vectors()
    {
        var (t, files) = await TableWithData(2);
        await SnapshotProducer.DeleteRows(t, Del(files[0], 0, 1)).CommitAsync();
        await SnapshotProducer.DeleteRows(t, Del(files[0], 2)).CommitAsync();   // second v2 delete file for the same data file
        await SnapshotProducer.DeleteRows(t, Del(files[2], 5)).CommitAsync();
        var expected = (await TableScan.ReadAllAsync(t)).Select(r => (long)r[0]!).Order().ToList();
        Assert.Equal(26, expected.Count);

        await Assert.ThrowsAsync<NotSupportedException>(() => SnapshotProducer.ConvertPositionDeletesToDeletionVectors(t).CommitAsync());
        await t.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(3)]);
        var snap = await SnapshotProducer.ConvertPositionDeletesToDeletionVectors(t).CommitAsync();

        Assert.Equal(Operations.Replace, snap.Operation);
        Assert.Equal("3", snap.Summary["removed-delete-files"]);
        Assert.Equal("2", snap.Summary["added-dvs"]);
        var live = await DeleteEntries(t);
        Assert.All(live, e => Assert.Equal("PUFFIN", e.DataFile.FileFormat));
        Assert.Equal(3, live.Single(e => e.DataFile.ReferencedDataFile == files[0].FilePath).DataFile.RecordCount);
        Assert.Equal(expected, (await TableScan.ReadAllAsync(t)).Select(r => (long)r[0]!).Order());
        Assert.Equal("4", snap.Summary["total-position-deletes"]);
    }

    [Spec("Scan Planning")]
    [Spec("Sequence Numbers")]
    [Fact]
    public async Task Deletion_vector_applies_only_to_older_data()
    {
        // A data file added after the DV (higher sequence number) is untouched even if it reused a path.
        var (t, files) = await TableWithData(3);
        await SnapshotProducer.DeleteRows(t, Del(files[0], 0)).CommitAsync();
        var index = await TableScan.DeletesAsync(t);
        var dv = index.DeletionVectorFor(files[0].FilePath)!;
        Assert.NotNull(await index.DeletedPositionsAsync(files[0], dv.DataSequenceNumber));
        Assert.Null(await index.DeletedPositionsAsync(files[0], dv.DataSequenceNumber + 1));
    }

    [Spec("Commit Conflict Resolution and Retry")]
    [Fact]
    public async Task Deleting_from_a_removed_data_file_is_a_validation_failure()
    {
        var (t, files) = await TableWithData(3);
        await Assert.ThrowsAsync<ValidationException>(() => SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { ["s3://nope.parquet"] = [0] }).CommitAsync());

        var stale = await _catalog.LoadTableAsync(t.Identifier);
        await SnapshotProducer.DeleteFiles(t, f => f.FilePath == files[0].FilePath).CommitAsync();
        // The stale writer's first attempt loses the CAS; on retry it re-validates and must not resurrect the file's deletes.
        await Assert.ThrowsAsync<ValidationException>(() => SnapshotProducer.DeleteRows(stale, Del(files[0], 1)).CommitAsync());
    }

    [Spec("Deletion Vectors")]
    [Spec("Commit Conflict Resolution and Retry")]
    [Fact]
    public async Task Concurrent_deletes_on_one_file_merge_into_one_deletion_vector()
    {
        var (t, files) = await TableWithData(3);
        var stale = await _catalog.LoadTableAsync(t.Identifier);
        await SnapshotProducer.DeleteRows(t, Del(files[0], 0)).CommitAsync();
        await SnapshotProducer.DeleteRows(stale, Del(files[0], 1)).CommitAsync();
        var final = await _catalog.LoadTableAsync(t.Identifier);
        var dv = Assert.Single(await DeleteEntries(final));
        Assert.Equal(2, dv.DataFile.RecordCount);
        Assert.Equal(28, (await TableScan.ReadAllAsync(final)).Count);
    }

    [Spec("Table Statistics")]
    [Fact]
    public async Task Ndv_statistics_are_written_to_puffin_and_registered()
    {
        var (t, files) = await TableWithData(3);
        await SnapshotProducer.DeleteRows(t, Del(files[0], 0)).CommitAsync();
        var stats = await PuffinStatistics.ComputeNdvAsync(t, ["id", "name", "event_date"]);

        Assert.Equal(t.Metadata.CurrentSnapshotId, stats.SnapshotId);
        Assert.Equal(stats, t.Metadata.Statistics.Single());
        var ndv = await PuffinStatistics.ReadNdvAsync(t, stats);
        Assert.Equal(29, ndv[1]);     // ids: 30 rows minus the deleted one
        Assert.Equal(3, ndv[3]);      // 3 distinct dates
        Assert.InRange(ndv[2], 22, 24); // names: null every 5th row
        Assert.Equal("29", stats.BlobMetadata.Single(b => b.Fields[0] == 1).Properties![PuffinStatistics.NdvProperty]);

        var puffin = PuffinFile.Read(await t.IO.ReadAllAsync(stats.StatisticsPath));
        Assert.Equal(stats.FileFooterSizeInBytes, puffin.Bytes.Length - puffin.Blobs.Sum(b => b.Length) - 4);
    }
}
