using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Partitioning;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The contract for deleting files after a commit (expiry, orphan cleanup, purge): the operation completes and reports
/// what it could not delete; it never throws once its commit (or drop) has happened, because the caller would then retry
/// or roll back something that already took effect.
/// </summary>
[Spec("Snapshot Retention Policy")]
[Spec("File System Operations")]
public sealed class CleanupContractTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-cleanup", Guid.NewGuid().ToString("N"));
    private readonly FaultInjectingFileIO _io = new(new LocalFileIO());
    private readonly InMemoryCatalog _catalog;

    public CleanupContractTests()
    {
        _catalog = new InMemoryCatalog(_warehouse, _io);
        _catalog.CreateNamespaceAsync(Namespace.Of("c")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Require(2, "p", PrimitiveType.Int),
    ], identifierFieldIds: [1]);

    private static List<object?[]> Rows(int from, int n) => Enumerable.Range(from, n).Select(i => new object?[] { (long)i, i % 2 }).ToList();

    private static string Local(string location) => location.StartsWith('/') ? location : new Uri(location).LocalPath;

    /// <summary>Three snapshots; the first two replaced by copy-on-write rewrites, so expiring them frees files.</summary>
    private async Task<Table> TableWithHistory(string name)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("c", name), Schema, PartitionSpec.BuilderFor(Schema).Identity("p").Build()));
        await TableMutations.InsertAsync(t, Rows(0, 6));
        await TableMutations.UpsertAsync(t, Rows(0, 6), strategy: DeleteStrategy.CopyOnWrite);
        await TableMutations.UpsertAsync(t, Rows(0, 6), strategy: DeleteStrategy.CopyOnWrite);
        return t;
    }

    [Fact]
    public async Task Expiry_reports_failed_deletes_instead_of_throwing_after_its_commit()
    {
        var t = await TableWithHistory("e1");
        _io.Reset(2, FaultKind.FailDelete);

        var result = await Maintenance.ExpireAsync(t, long.MaxValue, retainLast: 1);

        Assert.Equal(2, result.Expired.Count);
        var failed = Assert.Single(result.FailedDeletes);
        Assert.DoesNotContain(failed, result.DeletedFiles);
        Assert.True(File.Exists(Local(failed)));
        Assert.All(result.DeletedFiles, f => Assert.False(File.Exists(Local(f))));
        Assert.False(result.CleanupComplete);
        var reloaded = await _catalog.LoadTableAsync(t.Identifier);
        Assert.Single(reloaded.Metadata.Snapshots);
        await TableValidator.AssertValidAsync(reloaded, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    public async Task Expiry_that_cannot_read_a_manifest_after_its_commit_reports_it_and_deletes_nothing()
    {
        var t = await TableWithHistory("e2");
        var before = Directory.GetFiles(Local(t.Location), "*", SearchOption.AllDirectories).Length;
        // Metadata reads are the commit's; the first other read is the post-commit reachability scan.
        _io.PathFilter = p => !p.EndsWith(".metadata.json", StringComparison.Ordinal);
        _io.Reset(1, FaultKind.FailRead);

        var result = await Maintenance.ExpireAsync(t, long.MaxValue, retainLast: 1);

        Assert.Equal(2, result.Expired.Count);
        Assert.IsType<IOException>(result.CleanupError);
        Assert.Empty(result.DeletedFiles);
        Assert.Empty(_io.Deleted);
        Assert.Equal(before + 1, Directory.GetFiles(Local(t.Location), "*", SearchOption.AllDirectories).Length); // + the new metadata file
        Assert.Single((await _catalog.LoadTableAsync(t.Identifier)).Metadata.Snapshots);
    }

    [Fact]
    public async Task Orphan_cleanup_tries_every_orphan_and_reports_the_ones_it_could_not_delete()
    {
        var t = await TableWithHistory("o1");
        var dataDir = Path.Combine(Local(t.Location), "data");
        var orphans = Enumerable.Range(0, 5).Select(i => Path.Combine(dataDir, $"orphan-{i}.parquet")).ToList();
        foreach (var o in orphans) await File.WriteAllBytesAsync(o, [1, 2, 3]);
        foreach (var f in Directory.GetFiles(Local(t.Location), "*", SearchOption.AllDirectories)) File.SetLastWriteTimeUtc(f, DateTime.UtcNow.AddDays(-10));
        _io.Reset(1, FaultKind.FailDelete);

        var result = await Maintenance.RemoveOrphanFilesAsync(t);

        Assert.Equal(5, result.Orphans.Count);
        Assert.Single(result.FailedDeletes);
        Assert.Single(orphans, File.Exists);
        await TableValidator.AssertValidAsync(await _catalog.LoadTableAsync(t.Identifier), new ValidationOptions { Deep = true, AllSnapshots = true });

        // The old entry point tries every orphan too, then throws.
        foreach (var o in orphans) await File.WriteAllBytesAsync(o, [1, 2, 3]);
        foreach (var o in orphans) File.SetLastWriteTimeUtc(o, DateTime.UtcNow.AddDays(-10));
        _io.Reset(1, FaultKind.FailDelete);
        await Assert.ThrowsAsync<IOException>(() => Maintenance.DeleteOrphanFilesAsync(t));
        Assert.Single(orphans, File.Exists);
    }

    [Fact]
    public async Task Purge_drops_the_table_and_reports_files_it_could_not_delete()
    {
        var t = await TableWithHistory("p1");
        _io.Reset(3, FaultKind.FailDelete);

        var result = await _catalog.PurgeTableAsync(t.Identifier);

        Assert.False(await _catalog.TableExistsAsync(t.Identifier));
        var failed = Assert.Single(result.FailedDeletes);
        Assert.True(File.Exists(Local(failed)));
        Assert.NotEmpty(result.DeletedFiles);
        Assert.False(result.Complete);
    }

    [Fact]
    public async Task Drop_with_purge_does_not_throw_once_the_table_is_dropped()
    {
        var t = await TableWithHistory("p2");
        _io.Reset(1, FaultKind.FailDelete);
        await _catalog.DropTableAsync(t.Identifier, purge: true);
        Assert.False(await _catalog.TableExistsAsync(t.Identifier));

        var u = await TableWithHistory("p3");
        _io.PathFilter = p => !p.EndsWith(".metadata.json", StringComparison.Ordinal);
        _io.Reset(1, FaultKind.FailRead);
        var result = await _catalog.PurgeTableAsync(u.Identifier);
        Assert.False(await _catalog.TableExistsAsync(u.Identifier));
        Assert.IsType<IOException>(result.Error);
        Assert.Empty(result.DeletedFiles);
    }

    [Fact]
    public async Task Best_effort_delete_tries_every_file_and_the_strict_one_throws_after_trying_them_all()
    {
        var dir = Path.Combine(_warehouse, "loose");
        Directory.CreateDirectory(dir);
        var files = Enumerable.Range(0, 20).Select(i => Path.Combine(dir, $"f{i}")).ToList();
        foreach (var f in files) await File.WriteAllBytesAsync(f, [1]);

        _io.Reset(4, FaultKind.FailDelete);
        var failed = await TableFiles.TryDeleteAllAsync(_io, files.Append(Path.Combine(dir, "missing")));
        Assert.Single(failed);
        Assert.Equal(failed, files.Where(File.Exists));

        foreach (var f in files) await File.WriteAllBytesAsync(f, [1]);
        _io.Reset(1, FaultKind.FailDelete);
        await Assert.ThrowsAsync<IOException>(() => TableFiles.DeleteAllAsync(_io, files));
        Assert.Single(files, File.Exists);
    }
}
