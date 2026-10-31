using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// A representative subset of the suite on each storage backend (gap ST2): local disk and S3 (the moto emulator,
/// through <c>S3FileIO</c>). Writes, reads, deletes, updates, time travel, inlining and flushes, encryption, merging,
/// expiry with cleanup, and orphan cleanup must behave the same wherever the files are; each test ends with a deep
/// validation, which reads every live file back through the lake's storage.
/// </summary>
public abstract class StorageTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected StorageTests(CatalogBackend backend, string version, StorageBackend storage) => _lake = new TestLake(backend, version, storage);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private static async Task CommitAsync(DuckLakeCatalog catalog, Func<DuckLakeTransaction, Task> work)
    {
        await using var tx = catalog.BeginTransaction();
        await work(tx);
        await tx.CommitAsync();
    }

    private static async Task<List<string>> RowsAsync(DuckLakeCatalog catalog, long? snapshot = null) =>
        (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t", snapshot))).Select(r => $"{r[0]}:{r[1]}").Order(StringComparer.Ordinal).ToList();

    private async Task<DuckLakeCatalog> TableAsync(DuckLakeOptions? options = null)
    {
        var catalog = await _lake.CreateAsync(options);
        await CommitAsync(catalog, tx => tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("v", DuckLakeType.Varchar)]));
        return catalog;
    }

    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    [Spec("ducklake: INSERT")]
    [Spec("ducklake: DELETE")]
    [Spec("ducklake: UPDATE")]
    public async Task Writes_deletes_updates_and_time_travel(bool encrypted)
    {
        var catalog = await TableAsync(new DuckLakeOptions { Encrypted = encrypted, DataInliningRowLimit = 2 });
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", Enumerable.Range(0, 10).Select(i => new object?[] { (long)i, $"v{i}" })));
        var first = (await catalog.CurrentSnapshotAsync()).SnapshotId;
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[100L, "inlined"]]));
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.In("id", 1L, 2L, 3L)));
        await CommitAsync(catalog, tx => tx.UpdateAsync("main", "t", Expr.Equal("id", 4L), r => [r[0], "u"]));
        var expected = new[] { "0:v0", "100:inlined", "4:u", "5:v5", "6:v6", "7:v7", "8:v8", "9:v9" };
        Assert.Equal(expected, await RowsAsync(catalog));
        Assert.Equal(Enumerable.Range(0, 10).Select(i => $"{i}:v{i}").Order(StringComparer.Ordinal), await RowsAsync(catalog, first));
        Assert.Equal(["5:v5", "6:v6"], (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"), Expr.In("id", 5L, 6L)))
            .Select(r => $"{r[0]}:{r[1]}").Order(StringComparer.Ordinal));
        await catalog.FlushInlinedDataAsync();
        Assert.Equal(expected, await RowsAsync(await _lake.OpenAsync()));
        // Every file is where the lake says: under data_path, in the lake's storage.
        var files = _lake.Files().ToList();
        Assert.NotEmpty(files);
        Assert.All(files, f => Assert.StartsWith(_lake.DataPath, f, StringComparison.Ordinal));
        if (_lake.Storage == StorageBackend.S3) Assert.All(files, f => Assert.StartsWith("s3://", f, StringComparison.Ordinal));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Merging_expiry_and_cleanup_delete_from_storage()
    {
        var catalog = await TableAsync();
        for (var c = 0; c < 4; c++) await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[(long)c, "x"]]));
        Assert.Equal(4, _lake.Files().Count());
        Assert.Single(await catalog.MergeAdjacentFilesAsync());
        var cleaned = await catalog.CleanupOldFilesAsync();
        Assert.Equal(4, cleaned.Count);
        Assert.All(cleaned, f => Assert.StartsWith(_lake.DataPath, f, StringComparison.Ordinal));
        Assert.Single(_lake.Files());
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.Equal("id", 0L)));
        Assert.Single(await catalog.RewriteDataFilesAsync(deleteThreshold: 0.1));
        Assert.Equal(3, _lake.Files().Count()); // merged file, its delete file, the rewrite
        await catalog.ExpireSnapshotsAsync(DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.Equal(2, (await catalog.CleanupOldFilesAsync()).Count);
        Assert.Single(_lake.Files());
        Assert.Equal(["1:x", "2:x", "3:x"], await RowsAsync(catalog));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Orphan_cleanup_deletes_unreferenced_files_under_data_path_only()
    {
        var catalog = await TableAsync();
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L, "a"]]));
        var orphan = _lake.DataPath + "main/t/ducklake-crashed-writer.parquet";
        await _lake.IO.WriteAsync(orphan, new byte[] { 1, 2, 3 });
        // Beside data_path, in the same bucket or directory: a listing that strays there must not lead to a delete.
        var sentinel = _lake.DataPath[..^"data/".Length] + "sentinel.parquet";
        await _lake.IO.WriteAsync(sentinel, new byte[] { 1, 2, 3 });
        var live = _lake.Files().Where(f => f != orphan).ToList();
        Assert.Single(live);

        Assert.Empty(await catalog.DeleteOrphanedFilesAsync(TimeSpan.FromHours(1))); // everything is young
        var aged = await DuckLakeCatalog.OpenAsync(_lake.Store, new AgedListingIO(_lake.IO, TimeSpan.FromDays(4), [sentinel]));
        Assert.Equal([orphan], await aged.DeleteOrphanedFilesAsync(dryRun: true));
        Assert.True(await _lake.IO.ExistsAsync(orphan));
        Assert.Equal([orphan], await aged.DeleteOrphanedFilesAsync());
        Assert.False(await _lake.IO.ExistsAsync(orphan));
        Assert.True(await _lake.IO.ExistsAsync(sentinel));
        Assert.Equal(live, _lake.Files().ToList());
        Assert.Equal(["1:a"], await RowsAsync(catalog));
        await _lake.IO.DeleteAsync(sentinel);
    }
}

public sealed class SqliteLocalStorageTests11() : StorageTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11, StorageBackend.Local);
public sealed class SqliteS3StorageTests10() : StorageTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10, StorageBackend.S3);
public sealed class SqliteS3StorageTests11() : StorageTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11, StorageBackend.S3);
public sealed class PostgresS3StorageTests11() : StorageTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11, StorageBackend.S3);

/// <summary>The orphan scan compares catalog paths and listed objects in one normal form that keeps a scheme intact.</summary>
public sealed class OrphanLocationTests
{
    [Theory]
    [InlineData("s3://bucket/data/main/t/f.parquet", "s3://bucket/data/main/t/f.parquet")]
    [InlineData("s3://bucket/data//main/t/f.parquet", "s3://bucket/data/main/t/f.parquet")]
    [InlineData("s3a://bucket/data///f.parquet", "s3a://bucket/data/f.parquet")]
    [InlineData("file:///tmp/lake//data/f.parquet", "file:///tmp/lake/data/f.parquet")]
    [InlineData("file:/tmp/lake/data/f.parquet", "file:///tmp/lake/data/f.parquet")] // Hadoop's spelling: the same file
    [InlineData("file:/C:/lake/data/f.parquet", "file:///C:/lake/data/f.parquet")]
    [InlineData("/tmp/lake//data\\main\\f.parquet", "/tmp/lake/data/main/f.parquet")]
    [InlineData("lake/data/f.parquet", "lake/data/f.parquet")]
    public void Normalising_keeps_the_scheme_and_collapses_the_path(string location, string expected) =>
        Assert.Equal(expected, DuckLakeCatalog.NormaliseLocation(location));
}
