using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Paths and names are confined to <c>data_path</c> (gap S10): catalog rows are untrusted, so a path flagged relative
/// that climbs out (<c>../</c>, percent-encoded dots, an absolute path) is a format error; schema and table names that
/// are not one directory are refused; and neither scheduled-file cleanup nor orphan cleanup deletes anything outside
/// <c>data_path</c>, whatever a row or a listing says (a sentinel file outside survives).
/// </summary>
public abstract class PathConfinementTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected PathConfinementTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private async Task<DuckLakeCatalog> TableAsync(int commits = 1)
    {
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64)]);
            await tx.CommitAsync();
        }
        for (var c = 0; c < commits; c++)
        {
            await using var tx = catalog.BeginTransaction();
            await tx.InsertAsync("main", "t", [[(long)c]]);
            await tx.CommitAsync();
        }
        return catalog;
    }

    private string True => _lake.Backend == CatalogBackend.Sqlite ? "1" : "TRUE";
    private string False => _lake.Backend == CatalogBackend.Sqlite ? "0" : "FALSE";

    [SkippableTheory]
    [Spec("ducklake: Note on Paths")]
    [InlineData(".")]
    [InlineData("..")]
    [InlineData("a/b")]
    [InlineData("a\\b")]
    [InlineData("../escape")]
    public async Task Schema_and_table_names_that_are_not_one_directory_are_refused(string name)
    {
        var catalog = await TableAsync(commits: 0);
        await using var tx = catalog.BeginTransaction();
        await Assert.ThrowsAnyAsync<ArgumentException>(() => tx.CreateTableAsync("main", name, [new("id", DuckLakeType.Int64)]));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => tx.CreateSchemaAsync(name));
        await Assert.ThrowsAnyAsync<ArgumentException>(() => tx.RenameTableAsync("main", "t", name));
        Assert.DoesNotContain(Directory.EnumerateFileSystemEntries(_lake.Root), e => !e.StartsWith(_lake.DataPath.TrimEnd('/'), StringComparison.Ordinal)
                                                                                    && !e.EndsWith("catalog.sqlite", StringComparison.Ordinal));
    }

    [SkippableTheory]
    [Spec("ducklake: Note on Paths")]
    [InlineData("../../../outside.parquet")]
    [InlineData("%2e%2e/%2e%2e/%2e%2e/outside.parquet")]
    [InlineData("%252e%252e/%252e%252e/%252e%252e/outside.parquet")]
    [InlineData("sub/../../../../outside.parquet")]
    [InlineData("..\\..\\..\\outside.parquet")]
    [InlineData("ABSOLUTE")]              // an absolute path flagged relative
    [InlineData("file:///etc/passwd")]
    public async Task Relative_data_file_paths_that_escape_the_table_directory_are_refused(string path)
    {
        var catalog = await TableAsync();
        // A readable Parquet file outside data_path, with the table's layout: an escaping row would read it.
        var inside = _lake.Files().Single();
        var outside = Path.Combine(_lake.Root, "outside.parquet");
        File.Copy(inside, outside);
        if (path == "ABSOLUTE") path = outside;
        _lake.Corrupted = true;
        await _lake.ExecAsync($"UPDATE ducklake_data_file SET path = '{path}', path_is_relative = {True}");
        var table = await catalog.LoadTableAsync("main", "t");
        await Assert.ThrowsAsync<InvalidDataException>(() => catalog.ReadAsync(table));
        Assert.True(File.Exists(outside));
    }

    [SkippableTheory]
    [Spec("ducklake: Note on Paths")]
    [InlineData("ducklake_schema", "../../")]
    [InlineData("ducklake_table", "../../../")]
    [InlineData("ducklake_schema", "%2E%2E/")]
    public async Task Relative_schema_and_table_paths_that_escape_data_path_are_refused(string table, string path)
    {
        var catalog = await TableAsync();
        _lake.Corrupted = true;
        await _lake.ExecAsync($"UPDATE {table} SET path = '{path}', path_is_relative = {True}");
        await Assert.ThrowsAsync<InvalidDataException>(() => catalog.LoadTableAsync("main", "t"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Cleanup_never_deletes_outside_data_path()
    {
        var catalog = await TableAsync(commits: 4);
        Assert.Single(await catalog.MergeAdjacentFilesAsync()); // schedules the four inputs
        var sentinel = Path.Combine(_lake.Root, "sentinel.parquet");
        await File.WriteAllBytesAsync(sentinel, [1, 2, 3]);
        var scheduled = await _lake.SqlAsync("SELECT data_file_id, path FROM ducklake_files_scheduled_for_deletion ORDER BY data_file_id");
        Assert.Equal(4, scheduled.Count);
        // Three rows now name the sentinel (climbing out, encoded, absolute); the fourth stays a real scheduled file.
        _lake.Corrupted = true;
        await _lake.ExecAsync($"UPDATE ducklake_files_scheduled_for_deletion SET path = '../sentinel.parquet' WHERE data_file_id = {scheduled[0][0]}");
        await _lake.ExecAsync($"UPDATE ducklake_files_scheduled_for_deletion SET path = '%2e%2e/sentinel.parquet' WHERE data_file_id = {scheduled[1][0]}");
        await _lake.ExecAsync($"UPDATE ducklake_files_scheduled_for_deletion SET path = '{sentinel}', path_is_relative = {False} WHERE data_file_id = {scheduled[2][0]}");
        var kept = Path.GetFullPath(_lake.DataPath + (string)scheduled[3][1]!);
        Assert.True(File.Exists(kept));

        Assert.Equal([kept], (await catalog.CleanupOldFilesAsync(dryRun: true)).Select(Path.GetFullPath));
        Assert.Equal([kept], (await catalog.CleanupOldFilesAsync()).Select(Path.GetFullPath));
        Assert.True(File.Exists(sentinel));
        Assert.False(File.Exists(kept));
        // The refused rows stay, for an operator to look at; the deleted file's row is gone.
        Assert.Equal(3, (await _lake.SqlAsync("SELECT data_file_id FROM ducklake_files_scheduled_for_deletion")).Count);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Orphan_cleanup_never_deletes_what_a_listing_returns_from_outside_data_path()
    {
        await TableAsync();
        var sentinel = Path.Combine(_lake.Root, "sentinel.parquet");
        await File.WriteAllBytesAsync(sentinel, [1, 2, 3]);
        var orphan = Path.Combine(_lake.DataPath, "main", "t", "ducklake-crashed.parquet");
        await File.WriteAllBytesAsync(orphan, [1, 2, 3]);
        // A store whose listing also returns locations outside the prefix (a misbehaving or shared listing), all old.
        var io = new OutsideListing(_lake.IO, [sentinel, _lake.DataPath + "main/../../sentinel.parquet"]);
        var catalog = await DuckLakeCatalog.OpenAsync(_lake.Store, io);
        Assert.Equal([orphan], (await catalog.DeleteOrphanedFilesAsync()).Select(Path.GetFullPath));
        Assert.True(File.Exists(sentinel));
        Assert.False(File.Exists(orphan));
    }

    /// <summary>A local store whose listing is aged by four days and also returns <paramref name="extra"/>.</summary>
    private sealed class OutsideListing(LocalFileIO inner, IReadOnlyList<string> extra) : IFileIO, ISupportsListing
    {
        public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var f in inner.ListAsync(prefix, ct)) yield return f with { LastModifiedUtc = DateTime.UtcNow.AddDays(-4) };
            foreach (var e in extra) yield return new StoredFile(e, 3, DateTime.UtcNow.AddDays(-4));
        }

        public Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default) => inner.ReadAllAsync(location, ct);
        public Task<Stream> OpenReadAsync(string location, CancellationToken ct = default) => inner.OpenReadAsync(location, ct);
        public Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default) => inner.ReadRangeAsync(location, offset, length, ct);
        public Task<long> LengthAsync(string location, CancellationToken ct = default) => inner.LengthAsync(location, ct);
        public Task<bool> ExistsAsync(string location, CancellationToken ct = default) => inner.ExistsAsync(location, ct);
        public Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default) => inner.WriteAsync(location, content, overwrite, ct);
        public Task DeleteAsync(string location, CancellationToken ct = default) => inner.DeleteAsync(location, ct);
        public void Dispose() { }
    }
}

public sealed class SqlitePathConfinementTests10() : PathConfinementTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqlitePathConfinementTests11() : PathConfinementTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresPathConfinementTests11() : PathConfinementTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
