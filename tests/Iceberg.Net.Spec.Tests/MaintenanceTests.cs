using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Expiring snapshots deletes exactly the files nothing retained references; dropping with purge deletes everything.</summary>
[Spec("Snapshot Retention Policy")]
public sealed class MaintenanceTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-maint", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public MaintenanceTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("m")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "v", PrimitiveType.String),
        NestedField.Require(3, "p", PrimitiveType.Int),
    ], identifierFieldIds: [1]);

    private static List<object?[]> Rows(int from, int n, string v = "a") => Enumerable.Range(from, n).Select(i => new object?[] { (long)i, v, i % 3 }).ToList();

    private string[] FilesUnder(Table t) => Directory.GetFiles(new Uri(t.Location.StartsWith('/') ? "file://" + t.Location : t.Location).LocalPath, "*", SearchOption.AllDirectories);

    [Theory]
    [InlineData(2, DeleteStrategy.CopyOnWrite)]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    [InlineData(2, DeleteStrategy.EqualityDeletes)]
    public async Task Expiry_deletes_only_files_no_retained_snapshot_references(int formatVersion, DeleteStrategy strategy)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", $"e{formatVersion}{strategy}"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("p").Build(), FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, Rows(0, 30));
        await TableMutations.UpsertAsync(t, Rows(0, 10, "b"), ["id", "p"], strategy);   // rewrites / deletes in snapshot 2
        await TableMutations.InsertAsync(t, Rows(30, 10));
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], Rows(30, 5).Select(r => new object?[] { r[0], r[2] }), strategy);
        await TableMutations.UpsertAsync(t, Rows(5, 10, "c"), strategy: DeleteStrategy.CopyOnWrite);
        var rowsBefore = (await TableScan.ReadAllAsync(t)).Select(r => ((long)r[0]!, (string?)r[1])).Order().ToList();
        var allBefore = FilesUnder(t).ToHashSet();

        var result = await Maintenance.ExpireAsync(t, long.MaxValue, retainLast: 2);

        Assert.Equal(3, result.Expired.Count);
        Assert.NotEmpty(result.DeletedFiles);
        var allAfter = FilesUnder(t).ToHashSet();
        Assert.All(result.DeletedFiles, f => Assert.DoesNotContain(new Uri(f.StartsWith('/') ? "file://" + f : f).LocalPath, allAfter));
        Assert.True(allAfter.Count < allBefore.Count);
        // Every retained snapshot still reads, and every file it references exists with the recorded size.
        Assert.Equal(rowsBefore, (await TableScan.ReadAllAsync(t)).Select(r => ((long)r[0]!, (string?)r[1])).Order().ToList());
        var parent = t.Metadata.CurrentSnapshot!.ParentSnapshotId!.Value;
        Assert.NotEmpty(await TableScan.ReadAllAsync(t, parent));
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }

    [Fact]
    public async Task Expiry_without_cleanup_keeps_files_and_a_ref_keeps_its_snapshot_files()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "tagged"), Schema));
        await TableMutations.InsertAsync(t, Rows(0, 5));
        var tagged = t.Metadata.CurrentSnapshotId!.Value;
        await t.CommitAsync([], [new Metadata.MetadataUpdate.SetSnapshotRef("v1", Metadata.SnapshotRef.Tag(tagged))]);
        await TableMutations.UpsertAsync(t, Rows(0, 5, "z"), strategy: DeleteStrategy.CopyOnWrite);
        await TableMutations.InsertAsync(t, Rows(5, 5));

        var kept = await Maintenance.ExpireAsync(t, long.MaxValue, retainLast: 1, cleanExpiredFiles: false);
        Assert.Single(kept.Expired); // the middle snapshot; the tagged one is retained
        Assert.Empty(kept.DeletedFiles);
        Assert.Equal(5, (await TableScan.ReadAllAsync(t, tagged)).Count);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }

    [Fact]
    public async Task Drop_with_purge_deletes_every_file_the_table_ever_referenced()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "purged"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("p").Build(), FormatVersion: 3));
        await TableMutations.InsertAsync(t, Rows(0, 20));
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[1L, 1], [2L, 2]], DeleteStrategy.PositionDeletes); // DVs
        await TableMutations.UpsertAsync(t, Rows(3, 4, "x"), strategy: DeleteStrategy.CopyOnWrite);
        await PuffinStatistics.ComputeNdvAsync(t);
        Assert.NotEmpty(FilesUnder(t));

        await _catalog.DropTableAsync(t.Identifier, purge: true);
        Assert.Empty(FilesUnder(t));
    }

    [Fact]
    public async Task Drop_without_purge_keeps_files_for_re_registration()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "kept"), Schema));
        await TableMutations.InsertAsync(t, Rows(0, 3));
        var count = FilesUnder(t).Length;
        await _catalog.DropTableAsync(t.Identifier);
        Assert.Equal(count, FilesUnder(t).Length);
        var back = await _catalog.RegisterTableAsync(TableIdentifier.Of("m", "kept2"), t.MetadataLocation!);
        Assert.Equal(3, (await TableScan.ReadAllAsync(back)).Count);
    }

    [Fact]
    public async Task Orphan_cleanup_deletes_only_old_unreferenced_files()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "orphans"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("p").Build(), FormatVersion: 3));
        await TableMutations.InsertAsync(t, Rows(0, 12));
        await TableMutations.DeleteByKeysAsync(t, ["id", "p"], [[1L, 1]], DeleteStrategy.PositionDeletes);
        var root = new Uri(t.Location.StartsWith('/') ? "file://" + t.Location : t.Location).LocalPath;
        var strayData = Path.Combine(root, "data", "p=1", "abandoned-attempt.parquet");
        var strayMeta = Path.Combine(root, "metadata", "00099-lost-commit.metadata.json");
        var recent = Path.Combine(root, "data", "in-flight.parquet");
        foreach (var f in new[] { strayData, strayMeta, recent }) await File.WriteAllTextAsync(f, "x");
        var old = DateTime.UtcNow.AddDays(-10);
        foreach (var f in FilesUnder(t).Where(f => f != recent)) File.SetLastWriteTimeUtc(f, old); // referenced files are old too

        var dry = await Maintenance.DeleteOrphanFilesAsync(t, dryRun: true);
        Assert.Equal(new[] { strayMeta, strayData }.Order(), dry.Select(f => new Uri(f.StartsWith('/') ? "file://" + f : f).LocalPath).Order());
        Assert.True(File.Exists(strayData));

        var deleted = await Maintenance.DeleteOrphanFilesAsync(t);
        Assert.Equal(2, deleted.Count);
        Assert.False(File.Exists(strayData) || File.Exists(strayMeta));
        Assert.True(File.Exists(recent)); // younger than the cutoff: may belong to a writer still running
        Assert.Equal(11, (await TableScan.ReadAllAsync(t)).Count);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }

    private static string Local(string f) => new Uri(f.StartsWith('/') ? "file://" + f : f).LocalPath;

    /// <summary>Append a data file that lives outside the table's location, as tampered metadata or an in-place adoption would.</summary>
    private async Task<string> AppendOutsideFileAsync(Table t, int from)
    {
        var written = await TableWriter.WriteAsync(t, Rows(from, 3));
        var outside = Path.Combine(_warehouse, "elsewhere", $"{Guid.NewGuid():N}.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(outside)!);
        File.Move(Local(written[0].FilePath), outside);
        await SnapshotProducer.Append(t).AddFiles([written[0] with { FilePath = outside }]).CommitAsync();
        return outside;
    }

    [Fact]
    public async Task Expiry_skips_files_outside_the_table_location()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "outside_expiry"), Schema));
        await TableMutations.InsertAsync(t, Rows(0, 3));
        var outside = await AppendOutsideFileAsync(t, 100);
        await SnapshotProducer.DeleteFiles(t, f => Local(f.FilePath) == outside).CommitAsync(); // the outside file leaves the table
        await Maintenance.RewriteManifestsAsync(t); // live entries only: nothing retained references it any more

        var result = await Maintenance.ExpireAsync(t, long.MaxValue, retainLast: 1);

        Assert.True(File.Exists(outside));
        Assert.Equal([outside], result.SkippedFiles.Select(Local));
        Assert.DoesNotContain(result.DeletedFiles, f => Local(f) == outside);
        Assert.NotEmpty(result.DeletedFiles); // files under the table still go
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
    }

    [Fact]
    public async Task Purge_deletes_only_under_the_table_location()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "outside_purge"), Schema));
        await TableMutations.InsertAsync(t, Rows(0, 3));
        var outside = await AppendOutsideFileAsync(t, 100);

        await _catalog.DropTableAsync(t.Identifier, purge: true);

        Assert.True(File.Exists(outside));
        Assert.Empty(FilesUnder(t));
    }

    [Fact]
    public async Task Without_gc_expiry_and_orphan_cleanup_refuse_and_purge_keeps_data_files()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "no_gc"), Schema));
        await TableMutations.InsertAsync(t, Rows(0, 3));
        await TableMutations.UpsertAsync(t, Rows(0, 3, "b"), strategy: DeleteStrategy.CopyOnWrite);
        await t.CommitAsync([], [new Metadata.MetadataUpdate.SetProperties(new Dictionary<string, string> { [TableLocations.GcEnabled] = "false" })]);

        await Assert.ThrowsAsync<ValidationException>(() => Maintenance.ExpireAsync(t, long.MaxValue, retainLast: 1));
        await Assert.ThrowsAsync<ValidationException>(() => Maintenance.DeleteOrphanFilesAsync(t, dryRun: true));

        var dataFiles = FilesUnder(t).Where(f => f.EndsWith(".parquet", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(dataFiles);
        await _catalog.DropTableAsync(t.Identifier, purge: true);
        Assert.Equal(dataFiles.Order(), FilesUnder(t).Order()); // data stays; manifests, manifest lists and metadata go
    }

    [Fact]
    public async Task Orphan_cleanup_refuses_a_short_age_and_an_unsafe_location()
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("m", "orphan_guard"), Schema));
        await TableMutations.InsertAsync(t, Rows(0, 3));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => Maintenance.DeleteOrphanFilesAsync(t, TimeSpan.FromHours(1), dryRun: true));
        // Keep metadata writes in the table while its location points somewhere unsafe.
        await t.CommitAsync([], [new Metadata.MetadataUpdate.SetProperties(new Dictionary<string, string> { ["write.metadata.path"] = $"{t.Location}/metadata" })]);

        foreach (var location in new[] { "/", "file:///", $"{t.Location}/..", "s3://bucket" })
        {
            await t.CommitAsync([], [new Metadata.MetadataUpdate.SetLocation(location)]);
            await Assert.ThrowsAsync<ArgumentException>(() => Maintenance.DeleteOrphanFilesAsync(t, dryRun: true));
        }
    }

    [Theory]
    [InlineData("m", "../escape")]
    [InlineData("m", "t/data")]
    [InlineData("m", "..")]
    [InlineData("a/b", "t")]
    public async Task Names_that_would_reshape_a_storage_path_are_refused(string ns, string name)
    {
        if (!await _catalog.NamespaceExistsAsync(Namespace.Of(ns))) await _catalog.CreateNamespaceAsync(Namespace.Of(ns));
        await Assert.ThrowsAsync<ArgumentException>(() => _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of(ns, name), Schema)));
    }
}
