using Iceberg.Net.IO;
using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

/// <summary>What <see cref="DuckLakeCatalog.ExpireSnapshotsAsync(IReadOnlyCollection{long}, bool, CancellationToken)"/> removed (or, in a dry run, would remove).</summary>
public sealed record DuckLakeExpiry(IReadOnlyList<long> ExpiredSnapshots, IReadOnlyList<string> FilesScheduledForDeletion);

/// <summary>
/// Maintenance (DuckDB's <c>ducklake_expire_snapshots</c>, <c>ducklake_cleanup_old_files</c>,
/// <c>ducklake_delete_orphaned_files</c>, <c>ducklake_merge_adjacent_files</c> and <c>ducklake_rewrite_data_files</c>),
/// with the catalog effects DuckDB has (docs/ducklake/operations.md, "Maintenance"). Nothing here deletes a file a retained
/// snapshot can still read: expiry and compaction only schedule files (<c>ducklake_files_scheduled_for_deletion</c>),
/// and cleanup deletes what was scheduled.
/// </summary>
public sealed partial class DuckLakeCatalog
{
    /// <summary>Orphan cleanup ignores files younger than this: they may belong to a commit still in flight.</summary>
    public static readonly TimeSpan MinOrphanAge = TimeSpan.FromHours(1);

    /// <summary>A file to schedule for deletion, its path relative to <c>data_path</c> when it lies under it (as DuckDB records it).</summary>
    internal FileScheduledForDeletionEntity ScheduledFile(long fileId, string location, DateTimeOffset at) =>
        location.StartsWith(DataPath, StringComparison.Ordinal)
            ? new FileScheduledForDeletionEntity { DataFileId = fileId, Path = location[DataPath.Length..], PathIsRelative = true, ScheduleStart = at }
            : new FileScheduledForDeletionEntity { DataFileId = fileId, Path = location, PathIsRelative = false, ScheduleStart = at };

    /// <summary>Merge adjacent small files of one table, or of every table (one snapshot for all of them). Returns what was merged.</summary>
    public async Task<IReadOnlyList<DuckLakeCompaction>> MergeAdjacentFilesAsync(string? schema = null, string? table = null, DuckLakeMergeOptions? options = null,
        CancellationToken ct = default) =>
        await CompactAsync(schema, table, (tx, s, t) => tx.MergeAdjacentFilesAsync(s, t, options ?? new DuckLakeMergeOptions(), ct), ct);

    /// <summary>
    /// Rewrite files whose deleted share is at least <paramref name="deleteThreshold"/> (default 0.95, as DuckDB) without
    /// their deleted rows, in one table or every table (one snapshot). Returns what was rewritten.
    /// </summary>
    public async Task<IReadOnlyList<DuckLakeCompaction>> RewriteDataFilesAsync(string? schema = null, string? table = null, double deleteThreshold = 0.95,
        int? maxCompactedFiles = null, CancellationToken ct = default) =>
        await CompactAsync(schema, table, (tx, s, t) => tx.RewriteDataFilesAsync(s, t, deleteThreshold, maxCompactedFiles, ct), ct);

    /// <summary>
    /// Move inlined rows and inlined deletions of one table, or of every table, to Parquet (DuckDB's
    /// <c>ducklake_flush_inlined_data</c>; one snapshot, <c>inline_flush:{table}</c>). Every snapshot reads exactly what it
    /// did before. Returns how many inlined rows each table had.
    /// </summary>
    public async Task<IReadOnlyList<DuckLakeFlush>> FlushInlinedDataAsync(string? schema = null, string? table = null, CancellationToken ct = default) =>
        await CompactAsync(schema, table, (tx, s, t) => tx.FlushInlinedDataAsync(s, t, ct), ct);

    private async Task<IReadOnlyList<T>> CompactAsync<T>(string? schema, string? table, Func<DuckLakeTransaction, string, string, Task<T?>> stage, CancellationToken ct)
        where T : class
    {
        if (schema is not null && table is null) throw new ArgumentException("A schema needs a table name", nameof(table));
        List<(string Schema, string Table)> targets;
        if (table is not null) targets = [(schema ?? "main", table)];
        else
        {
            await using var db = Context();
            var s = await ResolveSnapshotAsync(db, null, ct);
            var schemas = (await SchemasAsync(db, s, ct)).ToDictionary(x => x.SchemaId);
            targets = (await TablesAsync(db, s, ct)).Where(t => schemas.ContainsKey(t.SchemaId)).Select(t => (schemas[t.SchemaId].Name, t.Name)).ToList();
        }
        await using var tx = BeginTransaction();
        var done = new List<T>();
        foreach (var (s, t) in targets)
            if (await stage(tx, s, t) is { } result) done.Add(result);
        if (done.Count > 0) await tx.CommitAsync(ct);
        return done;
    }

    /// <summary>Expire the given snapshots (never the latest). See <see cref="ExpireSnapshotsAsync(DateTimeOffset, bool, CancellationToken)"/>.</summary>
    public Task<DuckLakeExpiry> ExpireSnapshotsAsync(IReadOnlyCollection<long> versions, bool dryRun = false, CancellationToken ct = default) =>
        ExpireAsync(snapshots => snapshots.Where(s => versions.Contains(s.SnapshotId)), dryRun, ct);

    /// <summary>
    /// Expire every snapshot committed before <paramref name="olderThan"/> except the latest. Catalog rows no retained
    /// snapshot can see are removed (files, delete files, dropped tables and columns, their statistics, deleted inlined
    /// rows), and the files among them are scheduled for deletion (<see cref="CleanupOldFilesAsync"/> deletes them).
    /// <c>ducklake_snapshot_changes</c> rows are kept, as the spec says, so conflict checks still see every change.
    /// </summary>
    public Task<DuckLakeExpiry> ExpireSnapshotsAsync(DateTimeOffset olderThan, bool dryRun = false, CancellationToken ct = default) =>
        ExpireAsync(snapshots => snapshots.Where(s => s.SnapshotTime < olderThan), dryRun, ct);

    private async Task<DuckLakeExpiry> ExpireAsync(Func<List<SnapshotEntity>, IEnumerable<SnapshotEntity>> choose, bool dryRun, CancellationToken ct)
    {
        using var scope = DuckLakeTelemetry.Start("ducklake.maintenance.expire", this);
        await using var db = Context(scope);
        await db.Database.OpenConnectionAsync(ct);
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var snapshots = await Guarded("ducklake_snapshot", () => db.Snapshots.OrderBy(s => s.SnapshotId).ToListAsync(ct));
        if (snapshots.Count == 0) throw new InvalidDataException("ducklake_snapshot is empty");
        var latest = snapshots[^1].SnapshotId;
        var expired = choose(snapshots).Select(s => s.SnapshotId).Where(id => id != latest).Distinct().Order().ToList();
        if (expired.Count == 0)
        {
            scope.Succeeded();
            return new DuckLakeExpiry([], []);
        }
        await db.Snapshots.Where(s => expired.Contains(s.SnapshotId)).ExecuteDeleteAsync(ct);

        // A versioned row is kept while some retained snapshot s has begin_snapshot <= s < end_snapshot.
        var files = await db.DataFiles.Where(f => f.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= f.BeginSnapshot && s.SnapshotId < f.EndSnapshot))
            .Select(f => new { f.DataFileId, f.TableId, f.Path, f.PathIsRelative, f.MappingId }).ToListAsync(ct);
        var deletes = await db.DeleteFiles.Where(f => f.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= f.BeginSnapshot && s.SnapshotId < f.EndSnapshot))
            .Select(f => new { f.DeleteFileId, f.TableId, f.Path, f.PathIsRelative }).ToListAsync(ct);
        var locations = await TableLocationsAsync(db, files.Select(f => f.TableId).Concat(deletes.Select(d => d.TableId)).Distinct().ToList(), ct);
        var scheduled = new List<FileScheduledForDeletionEntity>();
        var now = DateTimeOffset.UtcNow;
        foreach (var f in files) scheduled.Add(ScheduledFile(f.DataFileId, Resolve(locations[f.TableId], f.Path, f.PathIsRelative ?? true), now));
        foreach (var d in deletes) scheduled.Add(ScheduledFile(d.DeleteFileId, Resolve(locations[d.TableId], d.Path, d.PathIsRelative ?? true), now));

        var fileIds = files.Select(f => f.DataFileId).ToList();
        var deleteIds = deletes.Select(d => d.DeleteFileId).ToList();
        await db.FileColumnStats.Where(x => fileIds.Contains(x.DataFileId)).ExecuteDeleteAsync(ct);
        await db.FilePartitionValues.Where(x => fileIds.Contains(x.DataFileId)).ExecuteDeleteAsync(ct);
        await RawAsync(db, $"DELETE FROM ducklake_file_variant_stats WHERE data_file_id IN ({IdList(fileIds)})", fileIds.Count, ct);
        await db.DataFiles.Where(x => fileIds.Contains(x.DataFileId)).ExecuteDeleteAsync(ct);
        await db.DeleteFiles.Where(x => deleteIds.Contains(x.DeleteFileId)).ExecuteDeleteAsync(ct);
        // Delete files of a removed data file go with it (they were ended no later than it).
        var orphanDeletes = await db.DeleteFiles.Where(d => !db.DataFiles.Any(f => f.DataFileId == d.DataFileId))
            .Select(d => new { d.DeleteFileId, d.TableId, d.Path, d.PathIsRelative }).ToListAsync(ct);
        foreach (var d in orphanDeletes)
            scheduled.Add(ScheduledFile(d.DeleteFileId, Resolve((await TableLocationsAsync(db, [d.TableId], ct))[d.TableId], d.Path, d.PathIsRelative ?? true), now));
        var orphanDeleteIds = orphanDeletes.Select(d => d.DeleteFileId).ToList();
        await db.DeleteFiles.Where(x => orphanDeleteIds.Contains(x.DeleteFileId)).ExecuteDeleteAsync(ct);

        // Versioned catalog rows nothing retained can see.
        await db.Tables.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.Columns.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.Schemas.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.PartitionInfos.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.Tags.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.ColumnTags.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.Views.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        if (db.Is11)
            await db.ViewColumnTags.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.Macros.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);
        await db.SortInfos.Where(x => x.EndSnapshot != null && !db.Snapshots.Any(s => s.SnapshotId >= x.BeginSnapshot && s.SnapshotId < x.EndSnapshot)).ExecuteDeleteAsync(ct);

        // Rows that hang off removed ones: partition keys, sort keys, macro bodies, name mappings, and everything of a
        // table that no longer has any version.
        await db.PartitionColumns.Where(c => !db.PartitionInfos.Any(p => p.PartitionId == c.PartitionId)).ExecuteDeleteAsync(ct);
        await db.SortExpressions.Where(e => !db.SortInfos.Any(i => i.SortId == e.SortId)).ExecuteDeleteAsync(ct);
        await db.MacroImpls.Where(i => !db.Macros.Any(m => m.MacroId == i.MacroId)).ExecuteDeleteAsync(ct);
        await db.MacroParameters.Where(p => !db.Macros.Any(m => m.MacroId == p.MacroId)).ExecuteDeleteAsync(ct);
        await db.NameMappings.Where(n => !db.DataFiles.Any(f => f.MappingId == n.MappingId)).ExecuteDeleteAsync(ct);
        await db.ColumnMappings.Where(m => !db.DataFiles.Any(f => f.MappingId == m.MappingId)).ExecuteDeleteAsync(ct);
        var goneTables = await db.SchemaVersions.Select(v => v.TableId).Concat(db.TableStats.Select(t => t.TableId)).Distinct()
            .Where(id => !db.Tables.Any(t => t.TableId == id)).ToListAsync(ct);
        if (goneTables.Count > 0)
        {
            await db.TableStats.Where(x => goneTables.Contains(x.TableId)).ExecuteDeleteAsync(ct);
            await db.TableColumnStats.Where(x => goneTables.Contains(x.TableId)).ExecuteDeleteAsync(ct);
            await db.SchemaVersions.Where(x => goneTables.Contains(x.TableId)).ExecuteDeleteAsync(ct);
            foreach (var inlined in await db.InlinedDataTables.Where(x => goneTables.Contains(x.TableId)).Select(x => x.TableName).ToListAsync(ct))
                await RawAsync(db, $"DROP TABLE IF EXISTS {Db.Quote(inlined)}", 1, ct);
            await db.InlinedDataTables.Where(x => goneTables.Contains(x.TableId)).ExecuteDeleteAsync(ct);
            foreach (var id in goneTables) await RawAsync(db, $"DROP TABLE IF EXISTS {Db.Quote(InlinedDeleteTable(id))}", 1, ct);
        }

        // Inlined rows deleted before every retained snapshot, and inlined deletions of files that are gone.
        var (rowId, begin, end) = DuckLakeInlinedData.SystemColumns(db.Is11);
        foreach (var inlined in await db.InlinedDataTables.Select(x => x.TableName).ToListAsync(ct))
            await RawAsync(db, $"DELETE FROM {Db.Quote(inlined)} WHERE {end} IS NOT NULL AND NOT EXISTS " +
                               $"(SELECT 1 FROM ducklake_snapshot s WHERE s.snapshot_id >= {Db.Quote(inlined)}.{begin} AND s.snapshot_id < {Db.Quote(inlined)}.{end})", 1, ct);
        if (fileIds.Count > 0)
            foreach (var tableId in files.Select(f => f.TableId).Distinct())
                if (await TableExistsAsync(db, InlinedDeleteTable(tableId), ct))
                    await RawAsync(db, $"DELETE FROM {Db.Quote(InlinedDeleteTable(tableId))} WHERE file_id IN ({IdList(fileIds)})", 1, ct);

        await BulkInsert.InsertAsync(db, scheduled, ct);
        if (dryRun) await tx.RollbackAsync(ct);
        else await tx.CommitAsync(ct);
        scope.Succeeded();
        return new DuckLakeExpiry(expired, scheduled.Select(f => f.PathIsRelative == true ? DataPath + f.Path : f.Path).ToList());
    }

    /// <summary>Each table's directory (relative file paths resolve against it), from its latest catalog row.</summary>
    private async Task<Dictionary<long, string>> TableLocationsAsync(DuckLakeDbContext db, List<long> tableIds, CancellationToken ct)
    {
        var tables = (await db.Tables.Where(t => tableIds.Contains(t.TableId)).ToListAsync(ct))
            .GroupBy(t => t.TableId).ToDictionary(g => g.Key, g => g.MaxBy(t => t.BeginSnapshot)!);
        var schemaIds = tables.Values.Select(t => t.SchemaId).Distinct().ToList();
        var schemas = (await db.Schemas.Where(s => schemaIds.Contains(s.SchemaId)).ToListAsync(ct)).GroupBy(s => s.SchemaId)
            .ToDictionary(g => g.Key, g => g.MaxBy(s => s.BeginSnapshot)!);
        return tableIds.ToDictionary(id => id, id =>
        {
            if (!tables.TryGetValue(id, out var t)) throw new InvalidDataException($"Table {id} has files but no ducklake_table row");
            var schema = schemas.TryGetValue(t.SchemaId, out var s) ? Resolve(DataPath, s.Path, s.PathIsRelative ?? true) : DataPath;
            return Resolve(schema, t.Path, t.PathIsRelative ?? true);
        });
    }

    private static string IdList(IEnumerable<long> ids) =>
        string.Join(", ", ids.Select(i => i.ToString(System.Globalization.CultureInfo.InvariantCulture)).DefaultIfEmpty("NULL"));

    private static async Task RawAsync(DuckLakeDbContext db, string sql, int when, CancellationToken ct)
    {
        if (when == 0) return;
        var (c, t) = db.Raw();
        await Db.ExecAsync(c, t, sql, ct);
    }

    /// <summary>
    /// Delete the files scheduled for deletion before <paramref name="olderThan"/> (all of them when null) and their
    /// rows. A file already gone is fine. Only files under <c>data_path</c> are deleted (see
    /// <see cref="DuckLakeOptions.CleanupOutsideDataPath"/>); other rows stay. Returns the locations deleted (or, in a dry
    /// run, that would be).
    /// </summary>
    public async Task<IReadOnlyList<string>> CleanupOldFilesAsync(DateTimeOffset? olderThan = null, bool dryRun = false, CancellationToken ct = default)
    {
        using var scope = DuckLakeTelemetry.Start("ducklake.maintenance.cleanup", this);
        await using var db = Context(scope);
        // Only files under data_path are deleted: a row naming anything else (../x, an absolute path elsewhere) is left
        // alone, row and file. Files registered in place outside data_path go only when the options allow it.
        var due = (await Guarded("ducklake_files_scheduled_for_deletion", () => db.FilesScheduledForDeletion.ToListAsync(ct)))
            .Where(f => olderThan is null || f.ScheduleStart < olderThan)
            .Where(f => f.PathIsRelative == true ? f.Path.Length > 0 && DuckLakePaths.IsConfined(f.Path)
                : DuckLakePaths.IsUnder(DataPath, f.Path) || (Options.CleanupOutsideDataPath && DuckLakePaths.IsCleanAbsolute(f.Path))).ToList();
        var locations = due.Select(f => f.PathIsRelative == true ? DataPath + f.Path : f.Path).ToList();
        if (dryRun || due.Count == 0)
        {
            scope.Succeeded();
            return locations;
        }
        foreach (var location in locations)
        {
            try { await IO.DeleteAsync(location, ct); }
            catch (FileNotFoundException) { /* already gone */ }
            catch (DirectoryNotFoundException) { /* already gone */ }
        }
        foreach (var chunk in due.Chunk(500))
        {
            var ids = chunk.Select(f => f.DataFileId).ToList();
            var paths = chunk.Select(f => f.Path).ToList();
            await db.FilesScheduledForDeletion.Where(f => ids.Contains(f.DataFileId) && paths.Contains(f.Path)).ExecuteDeleteAsync(ct);
        }
        scope.Succeeded();
        return locations;
    }

    /// <summary>
    /// Delete <c>.parquet</c> and <c>.puffin</c> files under <c>data_path</c> that no catalog row refers to (left by a crashed
    /// writer, say) and that are older than <paramref name="olderThan"/> (default 3 days, at least <see cref="MinOrphanAge"/>:
    /// a younger file may belong to a commit still running). Needs an <see cref="IFileIO"/> that can list.
    /// </summary>
    public async Task<IReadOnlyList<string>> DeleteOrphanedFilesAsync(TimeSpan? olderThan = null, bool dryRun = false, CancellationToken ct = default)
    {
        if (IO is not ISupportsListing lister) throw new NotSupportedException($"{IO.GetType().Name} cannot list files");
        var age = olderThan ?? TimeSpan.FromDays(3);
        if (age < MinOrphanAge)
            throw new ArgumentOutOfRangeException(nameof(olderThan), age, $"Orphan cleanup needs files at least {MinOrphanAge.TotalHours:0} hour(s) old");
        using var scope = DuckLakeTelemetry.Start("ducklake.maintenance.orphans", this);
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        await using (var db = Context(scope))
        {
            var files = await db.DataFiles.Select(f => new { f.TableId, f.Path, f.PathIsRelative }).ToListAsync(ct);
            var deletes = await db.DeleteFiles.Select(f => new { f.TableId, f.Path, f.PathIsRelative }).ToListAsync(ct);
            var locations = await TableLocationsAsync(db, files.Select(f => f.TableId).Concat(deletes.Select(d => d.TableId)).Distinct().ToList(), ct);
            foreach (var f in files.Concat(deletes)) referenced.Add(Normalise(Resolve(locations[f.TableId], f.Path, f.PathIsRelative ?? true)));
            foreach (var f in await db.FilesScheduledForDeletion.ToListAsync(ct)) referenced.Add(Normalise(f.PathIsRelative == true ? DataPath + f.Path : f.Path));
        }
        var cutoff = DateTime.UtcNow - age;
        var orphans = new List<string>();
        await foreach (var f in lister.ListAsync(DataPath, ct))
        {
            if (!f.Location.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) && !f.Location.EndsWith(".puffin", StringComparison.OrdinalIgnoreCase)) continue;
            if (f.LastModifiedUtc < cutoff && !referenced.Contains(Normalise(f.Location)) && DuckLakePaths.IsUnder(DataPath, f.Location)) orphans.Add(f.Location);
        }
        if (!dryRun)
            foreach (var o in orphans)
            {
                try { await IO.DeleteAsync(o, ct); }
                catch (FileNotFoundException) { /* gone meanwhile */ }
            }
        scope.Succeeded();
        return orphans;

        static string Normalise(string location) => location.Replace('\\', '/').Replace("//", "/");
    }
}
