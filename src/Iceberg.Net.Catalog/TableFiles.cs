using Iceberg.Net.Avro;
using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Catalog;

/// <summary>
/// Every file a set of snapshots references — manifest lists, manifests, data and delete files (including Puffin
/// deletion-vector files), statistics files — for purging a dropped table and for deleting what expired snapshots
/// alone referenced. Deleted-status manifest entries are included: their files are still on storage. Paths come from
/// the metadata and may point anywhere; callers delete only those <see cref="TableLocations.Partition"/> places under
/// the table.
/// </summary>
public static class TableFiles
{
    /// <param name="dataFiles">
    /// Include the data and delete files the manifests list. False when the table does not own them
    /// (<see cref="TableLocations.IsGcEnabled"/>): only manifest lists, manifests and statistics files are returned.
    /// </param>
    public static async Task<HashSet<string>> ReachableAsync(TableMetadata metadata, IFileIO io, IEnumerable<Snapshot> snapshots, CancellationToken ct = default, bool dataFiles = true)
    {
        var files = new HashSet<string>(StringComparer.Ordinal);
        var readManifests = new HashSet<string>(StringComparer.Ordinal);
        foreach (var snapshot in snapshots)
        {
            IEnumerable<(string Path, int SpecId)> manifests;
            if (snapshot.ManifestList is { } list)
            {
                files.Add(list);
                manifests = (await ManifestStore.ReadListAsync(io, list, ct)).Select(m => (m.ManifestPath, m.PartitionSpecId));
            }
            else manifests = (snapshot.Manifests ?? []).Select(p => (p, metadata.DefaultSpecId)); // v1 embedded manifest paths

            foreach (var (path, specId) in manifests)
            {
                files.Add(path);
                if (!dataFiles || !readManifests.Add(path)) continue; // manifests are shared between snapshots; read each once
                var contents = (await ManifestStore.ReadAsync(io, path, metadata.SpecById(specId), metadata.CurrentSchema, ct: ct));
                foreach (var e in contents.Entries) files.Add(e.DataFile.FilePath);
            }
            foreach (var s in metadata.Statistics.Where(s => s.SnapshotId == snapshot.SnapshotId)) files.Add(s.StatisticsPath);
            foreach (var s in metadata.PartitionStatistics.Where(s => s.SnapshotId == snapshot.SnapshotId)) files.Add(s.StatisticsPath);
        }
        return files;
    }

    /// <summary>
    /// Delete files concurrently, ignoring ones already gone; returns how many deletes were issued. Every file is tried;
    /// when some could not be deleted, the first error is then thrown. Use <see cref="TryDeleteAllAsync"/> to get the
    /// undeleted files back instead.
    /// </summary>
    public static async Task<int> DeleteAllAsync(IFileIO io, IEnumerable<string> files, CancellationToken ct = default)
    {
        var list = files.ToList();
        var failed = await DeleteEachAsync(io, list, ct);
        if (failed.Count > 0) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failed[0].Error).Throw();
        return list.Count;
    }

    /// <summary>
    /// Best-effort delete, the contract of every cleanup that runs after a commit (expiry, orphan cleanup, purge): every
    /// file is tried, concurrently, and the ones that could not be deleted are returned (in ordinal order) instead of
    /// thrown. A file already gone counts as deleted. Only cancellation is thrown.
    /// </summary>
    public static async Task<IReadOnlyList<string>> TryDeleteAllAsync(IFileIO io, IEnumerable<string> files, CancellationToken ct = default) =>
        (await DeleteEachAsync(io, files.ToList(), ct)).Select(f => f.Path).Order(StringComparer.Ordinal).ToList();

    private static async Task<List<(string Path, Exception Error)>> DeleteEachAsync(IFileIO io, List<string> files, CancellationToken ct)
    {
        var failed = new List<(string Path, Exception Error)>();
        await Parallel.ForEachAsync(files, new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = ct }, async (f, c) =>
        {
            try
            {
                await io.DeleteAsync(f, c);
            }
            catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException)
            {
                // already gone
            }
            catch (Exception e) when (e is not OperationCanceledException || !c.IsCancellationRequested)
            {
                lock (failed) failed.Add((f, e));
            }
        });
        return failed;
    }

    /// <summary>
    /// Delete a dropped table's files: every file its snapshots reference (data and delete files only when the table owns
    /// them, <see cref="TableLocations.IsGcEnabled"/>), its metadata log and <paramref name="metadataLocation"/>, and only
    /// those under the table's own directories. Best effort, as Java's <c>dropTableData</c>: run after the catalog entry is
    /// gone, so a failure never throws (except cancellation) — the result says what is left.
    /// </summary>
    public static async Task<PurgeResult> PurgeAsync(TableMetadata metadata, string metadataLocation, IFileIO io, CancellationToken ct = default)
    {
        HashSet<string> files;
        try
        {
            files = await ReachableAsync(metadata, io, metadata.Snapshots, ct, dataFiles: TableLocations.IsGcEnabled(metadata));
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // A manifest could not be read: the files it lists are unknown, so nothing is deleted.
            return new PurgeResult([], [], []) { Error = e };
        }
        files.UnionWith(metadata.MetadataLog.Select(l => l.MetadataFile).Append(metadataLocation));
        var (inside, outside) = TableLocations.Partition(metadata, files);
        var failed = await TryDeleteAllAsync(io, inside, ct);
        var failedSet = failed.ToHashSet(StringComparer.Ordinal);
        return new PurgeResult(inside.Where(f => !failedSet.Contains(f)).Order(StringComparer.Ordinal).ToList(), failed, outside.Order(StringComparer.Ordinal).ToList());
    }
}

/// <summary>What purging a dropped table's files did (<see cref="TableFiles.PurgeAsync"/>).</summary>
/// <param name="DeletedFiles">Files deleted (or already gone).</param>
/// <param name="FailedDeletes">Files whose delete failed; nothing references them any more, so only listing finds them.</param>
/// <param name="SkippedFiles">Files the metadata names outside the table's own directories, never deleted.</param>
public sealed record PurgeResult(IReadOnlyList<string> DeletedFiles, IReadOnlyList<string> FailedDeletes, IReadOnlyList<string> SkippedFiles)
{
    /// <summary>Set when the files could not even be listed (a manifest read failed): nothing was deleted.</summary>
    public Exception? Error { get; init; }

    /// <summary>Every file was deleted.</summary>
    public bool Complete => Error is null && FailedDeletes.Count == 0;
}
