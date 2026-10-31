using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>End-to-end on the local filesystem with the in-memory catalog: the fastest full-stack loop.</summary>
public sealed class LocalTableTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public static readonly Schema Schema = new(
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "event_date", PrimitiveType.Date),
        NestedField.Optional(4, "score", PrimitiveType.Double));

    public LocalTableTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    public static List<object?[]> Rows(int start, int count) => Enumerable.Range(start, count)
        .Select(i => new object?[] { (long)i, i % 5 == 0 ? null : $"n{i}", 19_700 + i % 3, i % 4 == 0 ? double.NaN : i * 0.5 })
        .ToList();

    private Task<Table> Create(int formatVersion = 2, PartitionSpec? spec = null) => _catalog.CreateTableAsync(new CreateTableRequest(
        TableIdentifier.Of("db", "t"), Schema, spec ?? PartitionSpec.BuilderFor(Schema).Identity("event_date").Build(), FormatVersion: formatVersion));

    [Spec("Scan Planning")]
    [Spec("Parquet")]
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Append_then_read_round_trips(int formatVersion)
    {
        var table = await Create(formatVersion);
        var files = await TableWriter.WriteAsync(table, Rows(0, 30));
        Assert.Equal(3, files.Count); // one file per event_date partition
        var snap = await SnapshotProducer.Append(table).AddFiles(files).CommitAsync();

        var reloaded = await _catalog.LoadTableAsync(table.Identifier);
        Assert.Equal(snap.SnapshotId, reloaded.Metadata.CurrentSnapshotId);
        Assert.EndsWith(".metadata.json", reloaded.MetadataLocation);
        Assert.Contains("/00001-", reloaded.MetadataLocation);
        var rows = await TableScan.ReadAllAsync(reloaded);
        Assert.Equal(30, rows.Count);
        Assert.Equal(Enumerable.Range(0, 30).Select(i => (long)i), rows.Select(r => (long)r[0]!).Order());
        Assert.Equal("30", snap.Summary["total-records"]);
        Assert.Equal("3", snap.Summary["total-data-files"]);

        if (formatVersion == 3)
        {
            Assert.Equal(0, snap.FirstRowId);
            Assert.Equal(30, snap.AddedRows);
            Assert.Equal(30, reloaded.Metadata.NextRowId);
            var tasks = await TableScan.PlanFilesAsync(reloaded);
            // first_row_id is inherited per file in manifest order with no gaps or overlaps.
            var ranges = tasks.Select(t => (Start: t.File.FirstRowId!.Value, t.File.RecordCount)).OrderBy(r => r.Start).ToList();
            Assert.Equal(0, ranges[0].Start);
            for (var i = 1; i < ranges.Count; i++) Assert.Equal(ranges[i - 1].Start + ranges[i - 1].RecordCount, ranges[i].Start);
        }
    }

    [Spec("Field-level Metrics and Statistics")]
    [Spec("Data File Fields")]
    [Fact]
    public async Task Metrics_include_nan_counts_and_exclude_nan_from_bounds()
    {
        var table = await Create();
        var file = (await TableWriter.WriteAsync(table, Rows(0, 8))).First(f => f.RecordCount > 0);
        Assert.True(file.NanValueCounts![4] >= 0);
        var upper = (double)Conversions.FromBytes(PrimitiveType.Double, file.UpperBounds![4]);
        Assert.False(double.IsNaN(upper));
        Assert.Equal(file.RecordCount, file.ValueCounts![1]);
    }

    [Spec("Snapshots")]
    [Spec("Point in Time Reads (Time Travel)")]
    [Fact]
    public async Task Overwrite_marks_replaced_files_deleted_and_keeps_history()
    {
        var table = await Create();
        var first = await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(0, 30))).CommitAsync();
        var replacement = await TableWriter.WriteAsync(table, Rows(100, 3).Select(r => { r[2] = 19_700; return r; }).ToList());
        var overwrite = await SnapshotProducer.Overwrite(table, f => (int?)f.Partition[0] == 19_700).AddFiles(replacement).CommitAsync();

        Assert.Equal(Operations.Overwrite, overwrite.Operation);
        Assert.Equal(first.SnapshotId, overwrite.ParentSnapshotId);
        Assert.Equal(2, overwrite.SequenceNumber);
        var rows = await TableScan.ReadAllAsync(table);
        Assert.Equal(20 + 3, rows.Count);
        Assert.Equal(30, (await TableScan.ReadAllAsync(table, first.SnapshotId)).Count); // time travel

        var manifests = await TableScan.ManifestsAsync(table, overwrite);
        var all = new List<ManifestEntry>();
        foreach (var mf in manifests)
            all.AddRange(ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(mf.ManifestPath), table.Spec, table.Schema, mf).Entries);
        Assert.Single(all, e => e.Status == ManifestEntryStatus.Deleted);
        Assert.All(all.Where(e => e.Status == ManifestEntryStatus.Existing), e => Assert.Equal(1, e.SequenceNumber));
        Assert.All(all.Where(e => e.Status == ManifestEntryStatus.Added), e => Assert.Equal(2, e.SequenceNumber));
    }

    [Spec("Optimistic Concurrency")]
    [Spec("Commit Conflict Resolution and Retry")]
    [Fact]
    public async Task Concurrent_writers_both_land_via_refresh_and_retry()
    {
        var a = await Create();
        var b = await _catalog.LoadTableAsync(a.Identifier); // stale view of the same table
        await SnapshotProducer.Append(a).AddFiles(await TableWriter.WriteAsync(a, Rows(0, 10))).CommitAsync();

        // b still believes main has no snapshot; its first attempt fails assert-ref-snapshot-id and retries.
        var snapB = await SnapshotProducer.Append(b).AddFiles(await TableWriter.WriteAsync(b, Rows(10, 10))).CommitAsync();
        var final = await _catalog.LoadTableAsync(a.Identifier);
        Assert.Equal(snapB.SnapshotId, final.Metadata.CurrentSnapshotId);
        Assert.Equal(2, final.Metadata.Snapshots.Count);
        Assert.Equal(20, (await TableScan.ReadAllAsync(final)).Count);
        Assert.Equal(2, snapB.SequenceNumber);

        // The failed attempt's manifest list was cleaned up; the reused manifest was not.
        var metaDir = Path.Combine(final.Location, "metadata");
        var lists = Directory.GetFiles(metaDir, "snap-*.avro");
        Assert.Equal(2, lists.Length);
    }

    [Spec("Optimistic Concurrency")]
    [Fact]
    public async Task Stale_commit_without_retry_is_rejected_by_requirements()
    {
        var a = await Create();
        var b = await _catalog.LoadTableAsync(a.Identifier);
        await a.SetPropertiesAsync(new Dictionary<string, string> { ["owner"] = "a" });
        await b.CommitAsync([new UpdateRequirement.AssertRefSnapshotId("main", null), new UpdateRequirement.AssertCurrentSchemaId(0)],
            [new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["owner"] = "b" })]);
        // Property commits don't conflict with each other (no requirement on properties), last writer wins.
        Assert.Equal("b", (await _catalog.LoadTableAsync(a.Identifier)).Metadata.Properties["owner"]);

        await a.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(5, "extra", PrimitiveType.Int)]));
        await Assert.ThrowsAsync<CommitFailedException>(() =>
            b.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(5, "other", PrimitiveType.String)])));
    }

    [Spec("Schema Evolution")]
    [Spec("Column Projection")]
    [Fact]
    public async Task Schema_evolution_then_append_reads_old_files_with_nulls()
    {
        var table = await Create();
        await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(0, 5))).CommitAsync();
        await table.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(5, "tag", PrimitiveType.String)]));
        var newRows = Rows(5, 5).Select(r => r.Append("t").ToArray()).ToList();
        await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, newRows)).CommitAsync();

        var rows = await TableScan.ReadAllAsync(await _catalog.LoadTableAsync(table.Identifier));
        Assert.Equal(10, rows.Count);
        Assert.Equal(5, rows.Count(r => r[4] is null));
        Assert.Equal(5, rows.Count(r => (string?)r[4] == "t"));
    }

    [Spec("Snapshot Retention Policy")]
    [Fact]
    public async Task Expire_snapshots_keeps_refs_and_current()
    {
        var table = await Create();
        for (var i = 0; i < 3; i++)
            await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(i * 10, 10))).CommitAsync();
        var first = table.Metadata.Snapshots[0].SnapshotId;
        await table.CommitAsync([], [new MetadataUpdate.SetSnapshotRef("keep-me", SnapshotRef.Tag(first))]);

        var expired = await Maintenance.ExpireSnapshotsAsync(table, olderThanMs: long.MaxValue, retainLast: 1);
        Assert.Single(expired); // the middle snapshot: first is tagged, last is current
        Assert.Equal(2, table.Metadata.Snapshots.Count);
        Assert.Equal(30, (await TableScan.ReadAllAsync(table)).Count);
    }

    [Fact]
    public async Task Catalog_lifecycle_and_errors()
    {
        var table = await Create();
        await Assert.ThrowsAsync<AlreadyExistsException>(() => Create());
        await Assert.ThrowsAsync<NoSuchNamespaceException>(() => _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("nope", "t"), Schema)));
        Assert.Equal([table.Identifier], await _catalog.ListTablesAsync(Namespace.Of("db")));

        await _catalog.RenameTableAsync(table.Identifier, TableIdentifier.Of("db", "t2"));
        Assert.False(await _catalog.TableExistsAsync(table.Identifier));
        var registered = await _catalog.RegisterTableAsync(TableIdentifier.Of("db", "t3"), table.MetadataLocation!);
        Assert.Equal(table.Metadata.TableUuid, registered.Metadata.TableUuid);

        await _catalog.DropTableAsync(TableIdentifier.Of("db", "t2"), purge: true);
        await Assert.ThrowsAsync<NoSuchTableException>(() => _catalog.LoadTableAsync(TableIdentifier.Of("db", "t2")));
    }

    [Spec("File System Operations")]
    [Theory]
    [InlineData("00007-{0}.gz.metadata.json")] // Java's name for write.metadata.compression-codec=gzip
    [InlineData("00007-{0}.metadata.json.gz")] // older Java releases
    public async Task Gzipped_metadata_is_read_whatever_it_is_called_and_numbering_continues(string nameFormat)
    {
        var table = await Create();
        await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, Rows(0, 10))).CommitAsync();
        var gzipped = Path.Combine(_warehouse, "gz", string.Format(System.Globalization.CultureInfo.InvariantCulture, nameFormat, Guid.NewGuid()));
        Directory.CreateDirectory(Path.GetDirectoryName(gzipped)!);
        await using (var file = File.Create(gzipped))
        await using (var gz = new System.IO.Compression.GZipStream(file, System.IO.Compression.CompressionLevel.Optimal))
            gz.Write(await table.IO.ReadAllAsync(table.MetadataLocation!));

        var registered = await _catalog.RegisterTableAsync(TableIdentifier.Of("db", "gz"), gzipped);
        Assert.Equal(table.Metadata.TableUuid, registered.Metadata.TableUuid);
        Assert.Equal(10, (await TableScan.ReadAllAsync(registered)).Count);

        await SnapshotProducer.Append(registered).AddFiles(await TableWriter.WriteAsync(registered, Rows(10, 5))).CommitAsync();
        Assert.Matches(@"/00008-[0-9a-f-]+\.metadata\.json$", registered.MetadataLocation!.Replace('\\', '/'));
        Assert.Equal(15, (await TableScan.ReadAllAsync(await _catalog.LoadTableAsync(TableIdentifier.Of("db", "gz")))).Count);
    }

    [Fact]
    public void Gzipped_metadata_that_inflates_past_the_limit_is_refused()
    {
        using var ms = new MemoryStream();
        using (var gz = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.SmallestSize, leaveOpen: true))
            gz.Write(new byte[1 << 20]); // 1 MiB of zeros: a few KiB compressed
        Assert.True(ms.Length < 16 * 1024);
        Assert.Throws<InvalidDataException>(() => MetastoreCatalog.DecodeMetadata(ms.ToArray(), "bomb.gz.metadata.json", maxDecompressedBytes: 64 * 1024));
        Assert.True(MetastoreCatalog.MaxDecompressedMetadataBytes >= 64 * 1024 * 1024);
    }

    [Theory]
    [InlineData("/wh/db/t/metadata/00007-1b4e28ba-2fa1-11d2-883f-0016d3cca427.metadata.json")]
    [InlineData(@"C:\wh\db\t\metadata\00007-1b4e28ba-2fa1-11d2-883f-0016d3cca427.metadata.json")]
    [InlineData("s3://b/wh/t/metadata/00007-1b4e28ba-2fa1-11d2-883f-0016d3cca427.gz.metadata.json")]
    public async Task The_next_metadata_version_follows_the_previous_on_any_path_style(string previous)
    {
        var table = await Create();
        Assert.Matches(@"/00008-[0-9a-f-]+\.metadata\.json$", MetastoreCatalog.NewMetadataLocation(table.Metadata, previous));
    }

    [Spec("File System Operations")]
    [Fact]
    public async Task Metadata_files_are_never_overwritten()
    {
        var table = await Create();
        await Assert.ThrowsAsync<IO.FileAlreadyExistsException>(() => table.IO.WriteAsync(table.MetadataLocation!, new byte[] { 1 }));
    }

    [Spec("File System Operations")]
    [Fact]
    public async Task Local_file_io_accepts_every_file_uri_form_engines_write()
    {
        var dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-uri", Guid.NewGuid().ToString("N"), "a b");
        Directory.CreateDirectory(dir);
        try
        {
            var path = Path.Combine(dir, "x.bin");
            await File.WriteAllBytesAsync(path, [1, 2, 3]);
            using var io = new LocalFileIO();
            // bare path, RFC 8089 file:///, Hadoop/Java file:/ (as Spark writes into manifests), percent-encoded
            foreach (var location in new[] { path, $"file://{path}", $"file:{path}", $"file:{path.Replace(" ", "%20")}" })
                Assert.Equal([1, 2, 3], await io.ReadAllAsync(location));
        }
        finally
        {
            Directory.Delete(Path.GetDirectoryName(dir)!, recursive: true);
        }
    }

    [Spec("Version 1: Analytic Data Tables")]
    [Fact]
    public async Task V1_tables_are_read_only()
    {
        var table = await Create(formatVersion: 1, spec: PartitionSpec.Unpartitioned);
        var files = await TableWriter.WriteAsync(table, Rows(0, 3));
        await Assert.ThrowsAsync<NotSupportedException>(() => SnapshotProducer.Append(table).AddFiles(files).CommitAsync());
    }
}
