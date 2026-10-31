using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Maintenance (docs/ducklake.md, "Maintenance"): merging and rewriting files, expiring snapshots, deleting scheduled
/// and orphaned files. The invariant throughout: every snapshot that is kept reads exactly what it read before, with
/// the same row IDs, and no file a kept snapshot reads is deleted. On every catalog database and format version.
/// </summary>
public abstract class MaintenanceTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected MaintenanceTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private Task<List<object?[]>> SqlAsync(string sql) => _lake.SqlAsync(sql);

    /// <summary>A table filled by <paramref name="commits"/> one-row-group commits of <paramref name="rows"/> rows each.</summary>
    private async Task<DuckLakeCatalog> SmallFilesAsync(int commits, int rows = 3, bool partitioned = false)
    {
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("part", DuckLakeType.Int32), new("v", DuckLakeType.Varchar)]);
            if (partitioned) await tx.SetPartitioningAsync("main", "t", [("part", "identity")]);
            await tx.CommitAsync();
        }
        for (var c = 0; c < commits; c++)
        {
            await using var tx = catalog.BeginTransaction();
            await tx.InsertAsync("main", "t", Enumerable.Range(c * rows, rows).Select(i => new object?[] { (long)i, i % 2, $"v{i}" }));
            await tx.CommitAsync();
        }
        return catalog;
    }

    /// <summary>Every snapshot's rows with their row IDs, canonically ordered.</summary>
    private static async Task<Dictionary<long, string>> HistoryAsync(DuckLakeCatalog catalog, IEnumerable<long>? only = null)
    {
        var result = new Dictionary<long, string>();
        foreach (var s in await catalog.ListSnapshotsAsync())
        {
            if (only is not null && !only.Contains(s.SnapshotId)) continue;
            DuckLakeTable table;
            try { table = await catalog.LoadTableAsync("main", "t", s.SnapshotId); }
            catch (DuckLakeNotFoundException) { continue; }
            var rows = new List<string>();
            await foreach (var r in catalog.ReadRowsAsync(table)) rows.Add($"{r.RowId}:{string.Join("|", r.Values)}");
            result[s.SnapshotId] = string.Join(" ", rows.Order(StringComparer.Ordinal));
        }
        return result;
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Merging_small_files_keeps_every_snapshot_and_row_id()
    {
        var catalog = await SmallFilesAsync(commits: 5);
        var before = await HistoryAsync(catalog);
        var merged = await catalog.MergeAdjacentFilesAsync();
        Assert.Equal([new DuckLakeCompaction("main", "t", 5, 1)], merged);
        var snapshot = await catalog.CurrentSnapshotAsync();
        Assert.Equal(["merge_adjacent:1"], snapshot.Changes.Select(c => c.ToString()));
        // One partial file, beginning at the first input's snapshot; the inputs are gone from the catalog and scheduled.
        Assert.Equal([[2L, 6L, 15L, 0L]], await SqlAsync("SELECT begin_snapshot, partial_max, record_count, row_id_start FROM ducklake_data_file"));
        Assert.Equal(5, (await SqlAsync("SELECT data_file_id FROM ducklake_files_scheduled_for_deletion WHERE path_is_relative = " + (_lake.Backend == CatalogBackend.Sqlite ? "1" : "TRUE"))).Count);
        Assert.Equal(before, await HistoryAsync(catalog, before.Keys));
        Assert.Equal(5, (await catalog.CleanupOldFilesAsync()).Count);
        Assert.Single(_lake.Files());
        Assert.Equal(before, await HistoryAsync(catalog, before.Keys));
        // Nothing left to merge: no snapshot.
        Assert.Empty(await catalog.MergeAdjacentFilesAsync());
        Assert.Equal(snapshot.SnapshotId, (await catalog.CurrentSnapshotAsync()).SnapshotId);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    [Spec("ducklake: ducklake_file_partition_value")]
    public async Task Merging_respects_partitions_deletes_and_the_target_size()
    {
        var catalog = await SmallFilesAsync(commits: 4, partitioned: true); // 8 files: 2 partitions x 4 commits
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Equal("id", 0L)); // the first file of partition 0 now has a delete file
            await tx.CommitAsync();
        }
        var before = await HistoryAsync(catalog);
        // Files at or above the target are left alone.
        Assert.Empty(await catalog.MergeAdjacentFilesAsync(options: new DuckLakeMergeOptions { TargetFileSize = 1 }));
        // Partition 0: the three files without deletes; partition 1: all four.
        Assert.Equal([new DuckLakeCompaction("main", "t", 7, 2)], await catalog.MergeAdjacentFilesAsync());
        Assert.Equal([[3L, 2L]], await SqlAsync("SELECT count(*), count(partial_max) FROM ducklake_data_file"));
        Assert.Equal(["0", "0", "1"], (await SqlAsync("SELECT partition_value FROM ducklake_file_partition_value")).Select(r => (string)r[0]!).Order());
        // Merged files sit in their partition's directory.
        Assert.All(await SqlAsync("SELECT path FROM ducklake_data_file"), r => Assert.StartsWith("part=", (string)r[0]!));
        Assert.Equal(before, await HistoryAsync(catalog, before.Keys));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task Rewriting_heavily_deleted_files_drops_the_deleted_rows_and_keeps_row_ids()
    {
        var catalog = await SmallFilesAsync(commits: 2, rows: 20);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.LessThan("id", 19L)); // 19 of file 0's 20 rows; file 1 untouched
            await tx.CommitAsync();
        }
        var before = await HistoryAsync(catalog);
        Assert.Equal([new DuckLakeCompaction("main", "t", 1, 1)], await catalog.RewriteDataFilesAsync());
        var snapshot = await catalog.CurrentSnapshotAsync();
        Assert.Equal(["rewrite_delete:1"], snapshot.Changes.Select(c => c.ToString()));
        // The new file holds the one survivor with its row ID stored (no row_id_start); the old file and its delete file end.
        Assert.Equal([[snapshot.SnapshotId, null, 1L]],
            await SqlAsync($"SELECT begin_snapshot, row_id_start, record_count FROM ducklake_data_file WHERE begin_snapshot = {snapshot.SnapshotId}"));
        Assert.Equal([[snapshot.SnapshotId]], await SqlAsync("SELECT end_snapshot FROM ducklake_delete_file"));
        Assert.Equal(before, await HistoryAsync(catalog, before.Keys));
        Assert.Equal(before[snapshot.SnapshotId - 1], (await HistoryAsync(catalog))[snapshot.SnapshotId]);
        // Below the threshold: nothing to do.
        Assert.Empty(await catalog.RewriteDataFilesAsync(deleteThreshold: 0.95));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_snapshot")]
    [Spec("ducklake: ducklake_snapshot_changes")]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Expiring_snapshots_removes_what_only_they_could_see()
    {
        var catalog = await SmallFilesAsync(commits: 2);                    // snapshots 1 (create), 2, 3
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "gone", [new("x", DuckLakeType.Int32)]);
            await tx.InsertAsync("main", "gone", [[1]]);
            await tx.CommitAsync();                                         // 4
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.LessThan("id", 3L));    // 5: file 0 loses every row: it ends
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DropTableAsync("main", "gone");                       // 6
            await tx.CommitAsync();
        }
        var before = await HistoryAsync(catalog);

        var dry = await catalog.ExpireSnapshotsAsync([2, 3, 4], dryRun: true);
        Assert.Equal([2L, 3L, 4L], dry.ExpiredSnapshots);
        Assert.Equal(7, (await catalog.ListSnapshotsAsync()).Count);       // a dry run changes nothing
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_files_scheduled_for_deletion"));

        var expiry = await catalog.ExpireSnapshotsAsync([2, 3, 4, 6]);     // 6 is the latest: never expired
        Assert.Equal([2L, 3L, 4L], expiry.ExpiredSnapshots);
        Assert.Equal([0L, 1L, 5L, 6L], (await catalog.ListSnapshotsAsync()).Select(s => s.SnapshotId));
        // File 0 (snapshots 2-4) is visible nowhere now: removed and scheduled. The dropped table (4-5) is still seen by 5.
        Assert.Single(expiry.FilesScheduledForDeletion);
        Assert.Equal([[2L]], await SqlAsync("SELECT count(*) FROM ducklake_data_file"));
        Assert.Equal([["gone"], ["t"]], await SqlAsync("SELECT table_name FROM ducklake_table ORDER BY table_name"));
        // The changes of expired snapshots stay (spec ducklake_snapshot_changes).
        Assert.Equal(7, (await SqlAsync("SELECT snapshot_id FROM ducklake_snapshot_changes")).Count);
        await Assert.ThrowsAsync<DuckLakeNotFoundException>(() => catalog.LoadTableAsync("main", "t", 3));
        Assert.Equal(before.Where(kv => kv.Key is 1 or 5 or 6), await HistoryAsync(catalog));
        Assert.Single(await catalog.CleanupOldFilesAsync());
        Assert.Equal(before.Where(kv => kv.Key is 1 or 5 or 6), await HistoryAsync(catalog));

        // By time: everything but the latest. Now nothing sees the dropped table: it goes, with its file and statistics.
        Assert.Equal([0L, 1L, 5L], (await catalog.ExpireSnapshotsAsync(DateTimeOffset.UtcNow.AddMinutes(1))).ExpiredSnapshots);
        Assert.Equal([6L], (await catalog.ListSnapshotsAsync()).Select(s => s.SnapshotId));
        Assert.Equal([["t"]], await SqlAsync("SELECT table_name FROM ducklake_table"));
        Assert.Equal([[1L]], await SqlAsync("SELECT table_id FROM ducklake_table_stats"));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_column WHERE table_id <> 1"));
        Assert.Single(await catalog.CleanupOldFilesAsync());
        Assert.Single(_lake.Files());
        Assert.Equal(before[6], (await HistoryAsync(catalog))[6]);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Orphaned_files_are_deleted_only_when_old_enough()
    {
        var catalog = await SmallFilesAsync(commits: 1);
        var orphan = Path.Combine(_lake.DataPath, "main", "t", "ducklake-crashed-writer.parquet");
        await File.WriteAllBytesAsync(orphan, [1, 2, 3]);
        Assert.Empty(await catalog.DeleteOrphanedFilesAsync(TimeSpan.FromHours(2)));            // too young
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => catalog.DeleteOrphanedFilesAsync(TimeSpan.FromMinutes(5)));
        File.SetLastWriteTimeUtc(orphan, DateTime.UtcNow.AddDays(-4));
        foreach (var f in _lake.Files()) File.SetLastWriteTimeUtc(f, DateTime.UtcNow.AddDays(-4)); // the live file too
        var dry = await catalog.DeleteOrphanedFilesAsync(dryRun: true);
        Assert.Equal([orphan], dry.Select(Path.GetFullPath));
        Assert.True(File.Exists(orphan));
        Assert.Equal([orphan], (await catalog.DeleteOrphanedFilesAsync()).Select(Path.GetFullPath));
        Assert.False(File.Exists(orphan));
        Assert.Single(_lake.Files());
        Assert.Equal(3, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Count);
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    public async Task Compaction_conflicts_with_deletes_from_the_same_files_only()
    {
        var catalog = await SmallFilesAsync(commits: 3);
        // A delete commits after a merge staged over the file it deletes from: the merge loses.
        await using (var merge = catalog.BeginTransaction())
        {
            Assert.NotNull(await merge.MergeAdjacentFilesAsync("main", "t", new DuckLakeMergeOptions(), default));
            await using (var delete = catalog.BeginTransaction())
            {
                await delete.DeleteAsync("main", "t", Expr.Equal("id", 4L));
                await delete.CommitAsync();
            }
            await Assert.ThrowsAsync<DuckLakeConflictException>(() => merge.CommitAsync());
        }
        // A merge commits first: a delete staged over one of its inputs loses.
        await using (var delete = catalog.BeginTransaction())
        {
            await delete.DeleteAsync("main", "t", Expr.Equal("id", 7L));
            Assert.Single(await catalog.MergeAdjacentFilesAsync()); // merges files 0 and 2 (file 1 has a delete file)
            await Assert.ThrowsAsync<DuckLakeConflictException>(() => delete.CommitAsync());
        }
        // A delete from a file the merge does not touch commits alongside it.
        for (var c = 0; c < 2; c++)
        {
            await using var tx = catalog.BeginTransaction();
            await tx.InsertAsync("main", "t", Enumerable.Range(9 + c * 3, 3).Select(i => new object?[] { (long)i, i % 2, $"v{i}" }));
            await tx.CommitAsync();
        }
        await using (var delete = catalog.BeginTransaction())
        {
            Assert.Equal(1, await delete.DeleteAsync("main", "t", Expr.Equal("id", 3L))); // file 1, which the merge skips (it has deletes)
            Assert.Equal([new DuckLakeCompaction("main", "t", 3, 1)], await catalog.MergeAdjacentFilesAsync()); // the merged file and the two new ones
            await delete.CommitAsync();
        }
        var rows = await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"));
        Assert.Equal([0L, 1L, 2L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L], rows.Select(r => (long)r[0]!).Order());
    }
}

public sealed class SqliteMaintenanceTests10() : MaintenanceTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteMaintenanceTests11() : MaintenanceTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresMaintenanceTests10() : MaintenanceTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresMaintenanceTests11() : MaintenanceTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
