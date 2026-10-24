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

    /// <summary>Delete files concurrently, ignoring ones already gone; returns how many deletes were issued.</summary>
    public static async Task<int> DeleteAllAsync(IFileIO io, IEnumerable<string> files, CancellationToken ct = default)
    {
        var list = files.ToList();
        await Parallel.ForEachAsync(list, new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = ct }, async (f, c) =>
        {
            try
            {
                await io.DeleteAsync(f, c);
            }
            catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException)
            {
                // already gone
            }
        });
        return list.Count;
    }
}
