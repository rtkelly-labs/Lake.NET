using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The fsck must pass on healthy tables and name each kind of corruption precisely.</summary>
[Spec("Manifests")]
[Spec("Content file uniqueness")]
[Spec("Field-level Metrics and Statistics")]
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

    // ---------- snapshot timestamps (v4: strictly increasing) ----------

    private sealed class FixedClock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Snapshot_timestamps_strictly_increase_along_a_branch_whatever_the_clock_says(int formatVersion)
    {
        var (t, _) = await Healthy(formatVersion); // the first snapshot: the system clock
        var first = t.Metadata.CurrentSnapshot!.TimestampMs;

        // A clock that stands still, years behind the parent: every child is its parent + 1 ms.
        var stale = new FixedClock(new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero));
        for (var i = 0; i < 3; i++)
            await SnapshotProducer.Append(t).WithClock(stale).AddFiles(await TableWriter.WriteAsync(t, LocalTableTests.Rows(100 + i, 1))).CommitAsync();
        Assert.Equal([first, first + 1, first + 2, first + 3], t.Metadata.Ancestors(t.Metadata.CurrentSnapshotId).Reverse().Select(s => s.TimestampMs));

        // A clock ahead of the parent is used as is; on a branch, the parent is the branch head.
        var ahead = new DateTimeOffset(2100, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await SnapshotProducer.Append(t).WithClock(new FixedClock(ahead)).AddFiles(await TableWriter.WriteAsync(t, LocalTableTests.Rows(200, 1))).ToBranch("b").CommitAsync();
        await SnapshotProducer.Append(t).WithClock(new FixedClock(ahead)).AddFiles(await TableWriter.WriteAsync(t, LocalTableTests.Rows(201, 1))).ToBranch("b").CommitAsync();
        var branch = t.Metadata.Ancestors(t.Metadata.Refs["b"].SnapshotId).Take(2).Select(s => s.TimestampMs).ToList();
        Assert.Equal([ahead.ToUnixTimeMilliseconds() + 1, ahead.ToUnixTimeMilliseconds()], branch);
        Assert.Equal(first + 3, t.Metadata.CurrentSnapshot!.TimestampMs); // main untouched

        var report = await TableValidator.ValidateAsync(await _catalog.LoadTableAsync(t.Identifier), Deep);
        Assert.True(report.IsValid, report.ToString());
        Assert.DoesNotContain(report.Issues, i => i.Code == "META-TIMESTAMP-ORDER");
    }

    [Fact]
    public async Task Non_increasing_snapshot_timestamps_are_an_error_only_on_v4_metadata()
    {
        var (t, _) = await Healthy(3);
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, LocalTableTests.Rows(100, 1))).CommitAsync();
        var m = t.Metadata;
        // Another writer's history: the child stamped at its parent's millisecond (or before it, a skewed clock).
        var parentTs = m.Snapshots[0].TimestampMs;
        var skewed = m with { Snapshots = [m.Snapshots[0], m.Snapshots[1] with { TimestampMs = parentTs }] };

        var v3 = await TableValidator.ValidateAsync(new Table(t.Catalog, t.Identifier, skewed, t.IO), new ValidationOptions { CheckFiles = false });
        Assert.True(v3.IsValid, v3.ToString());
        Assert.DoesNotContain(v3.Issues, i => i.Code == "META-TIMESTAMP-ORDER"); // not even a warning before v4

        var v4 = await TableValidator.ValidateAsync(new Table(t.Catalog, t.Identifier, skewed with { FormatVersion = 4 }, t.IO), new ValidationOptions { CheckFiles = false });
        var issue = Assert.Single(v4.Issues, i => i.Code == "META-TIMESTAMP-ORDER");
        Assert.Equal(IssueSeverity.Error, issue.Severity);
        Assert.False(v4.IsValid);

        // The same history with increasing timestamps passes on v4.
        var ordered = skewed with { FormatVersion = 4, Snapshots = [m.Snapshots[0], m.Snapshots[1] with { TimestampMs = parentTs + 1 }] };
        Assert.DoesNotContain((await TableValidator.ValidateAsync(new Table(t.Catalog, t.Identifier, ordered, t.IO), new ValidationOptions { CheckFiles = false })).Issues,
            i => i.Code == "META-TIMESTAMP-ORDER");
    }
}
