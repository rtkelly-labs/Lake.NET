using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The fsck must pass on healthy tables and name each kind of corruption precisely.</summary>
public sealed class TableValidatorTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;
    private int _n;
    private static readonly ValidationOptions Deep = new() { Deep = true, AllSnapshots = true };

    public TableValidatorTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private async Task<(Table T, IReadOnlyList<DataFile> Files)> Healthy(int formatVersion = 3)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t{_n++}"), LocalTableTests.Schema,
            PartitionSpec.BuilderFor(LocalTableTests.Schema).Identity("event_date").Build(), FormatVersion: formatVersion));
        var files = await TableWriter.WriteAsync(t, LocalTableTests.Rows(0, 30));
        await SnapshotProducer.Append(t).AddFiles(files).CommitAsync();
        return (t, files);
    }

    private static async Task<IReadOnlyList<string>> Codes(Table t) =>
        (await TableValidator.ValidateAsync(t, Deep)).Errors.Select(e => e.Code).Distinct().ToList();

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Healthy_tables_through_every_mutation_pass(int formatVersion)
    {
        var (t, files) = await Healthy(formatVersion);
        await TableMutations.UpsertAsync(t, [[1L, "x", 19_701, 1.0]], ["id", "event_date"], DeleteStrategy.PositionDeletes);
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], [[3L, 19_700]], DeleteStrategy.CopyOnWrite);
        await TableMutations.UpsertAsync(t, [[4L, "y", 19_701, 2.0]], ["id", "event_date"], DeleteStrategy.EqualityDeletes);
        await PuffinStatistics.ComputeNdvAsync(t);
        var report = await TableValidator.ValidateAsync(t, Deep);
        Assert.True(report.IsValid, report.ToString());
        Assert.Equal(t.Metadata.Snapshots.Count, report.SnapshotsChecked);
    }

    [Fact]
    public async Task Detects_missing_and_resized_files()
    {
        var (t, files) = await Healthy();
        File.Delete(files[0].FilePath);
        await File.AppendAllTextAsync(files[1].FilePath, "garbage");
        var codes = await Codes(t);
        Assert.Contains("FILE-MISSING", codes);
        Assert.Contains("FILE-SIZE", codes);
    }

    [Fact]
    public async Task Detects_a_summary_that_lies_about_totals()
    {
        var (t, _) = await Healthy();
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, LocalTableTests.Rows(100, 1))).Set("total-records", "999").CommitAsync();
        Assert.Contains("SUMMARY-TOTAL", await Codes(t));
    }

    [Fact]
    public async Task Detects_two_deletion_vectors_for_one_data_file()
    {
        var (t, files) = await Healthy();
        await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [files[0].FilePath] = [0] }).CommitAsync();
        var dv = (await TableScan.DeletesAsync(t)).All.Single().File;
        await SnapshotProducer.RowDelta(t).AddDeleteFiles([dv]).CommitAsync(); // a writer that forgot to merge
        Assert.Contains("DV-MULTIPLE", await Codes(t));
    }

    [Fact]
    public async Task Detects_bounds_that_would_prune_live_rows()
    {
        var (t, _) = await Healthy();
        var file = (await TableWriter.WriteAsync(t, LocalTableTests.Rows(200, 3).Select(r => { r[2] = 19_700; return r; }).ToList())).Single();
        var lying = file with { UpperBounds = new Dictionary<int, byte[]>(file.UpperBounds!) { [1] = Conversions.ToBytes(PrimitiveType.Long, 0L) } };
        await SnapshotProducer.Append(t).AddFiles([lying]).CommitAsync();
        Assert.Contains("METRIC-UPPER", await Codes(t));
    }

    [Fact]
    public async Task Detects_wrong_partition_and_record_count()
    {
        var (t, _) = await Healthy();
        var file = (await TableWriter.WriteAsync(t, LocalTableTests.Rows(300, 3).Select(r => { r[2] = 19_700; return r; }).ToList())).Single();
        await SnapshotProducer.Append(t).AddFiles([file with { Partition = [19_999], RecordCount = 5 }]).CommitAsync();
        var codes = await Codes(t);
        Assert.Contains("DATA-PARTITION", codes);
        Assert.Contains("DATA-RECORD-COUNT", codes);
    }

    [Fact]
    public async Task Detects_corrupt_deletion_vector_bytes()
    {
        var (t, files) = await Healthy();
        await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [files[0].FilePath] = [0, 1] }).CommitAsync();
        var dv = (await TableScan.DeletesAsync(t)).All.Single().File;
        var bytes = await File.ReadAllBytesAsync(dv.FilePath);
        bytes[dv.ContentOffset!.Value + 10] ^= 0xFF;
        await File.WriteAllBytesAsync(dv.FilePath, bytes);
        Assert.Contains("DV-CORRUPT", await Codes(t));
    }

    [Fact]
    public async Task Shallow_mode_skips_data_reads()
    {
        var (t, _) = await Healthy();
        var report = await TableValidator.ValidateAsync(t, new ValidationOptions { CheckFiles = false });
        Assert.True(report.IsValid);
        Assert.False(report.Deep);
    }
}
