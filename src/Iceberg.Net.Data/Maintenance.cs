using System.Globalization;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;
/// <param name="Expired">Snapshot IDs removed from the table.</param>
/// <param name="DeletedFiles">Files only expired snapshots referenced, deleted after the commit.</param>
public sealed record ExpireResult(IReadOnlyList<long> Expired, IReadOnlyList<string> DeletedFiles);

public static class Maintenance
{
    /// <summary>Expire snapshots and delete the files only they referenced; see <see cref="ExpireAsync"/>.</summary>
    public static async Task<IReadOnlyList<long>> ExpireSnapshotsAsync(Table table, long olderThanMs, int retainLast = 1, CancellationToken ct = default) =>
        (await ExpireAsync(table, olderThanMs, retainLast, cleanExpiredFiles: true, ct)).Expired;

    /// <summary>
    /// Expire snapshots older than <paramref name="olderThanMs"/>, keeping at least <paramref name="retainLast"/>
    /// ancestors of main and every snapshot a ref points at (spec "Snapshot Retention Policy"). With
    /// <paramref name="cleanExpiredFiles"/> (Java's default), files reachable from expired snapshots but from no retained
    /// one — manifest lists, manifests, data, delete and statistics files — are deleted once the commit has succeeded.
    /// Nothing is deleted if the commit fails or its outcome is unknown.
    /// </summary>
    public static async Task<ExpireResult> ExpireAsync(Table table, long olderThanMs, int retainLast = 1, bool cleanExpiredFiles = true, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var keep = m.Ancestors(m.CurrentSnapshotId).Take(Math.Max(1, retainLast)).Select(s => s.SnapshotId).ToHashSet();
        keep.UnionWith(m.Refs.Values.Select(r => r.SnapshotId));
        var expire = m.Snapshots.Where(s => s.TimestampMs < olderThanMs && !keep.Contains(s.SnapshotId)).ToList();
        if (expire.Count == 0) return new ExpireResult([], []);
        await table.CommitAsync(
            [new UpdateRequirement.AssertTableUuid(m.TableUuid), new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, m.CurrentSnapshotId)],
            [new MetadataUpdate.RemoveSnapshots(expire.Select(s => s.SnapshotId).ToList())], ct);
        if (!cleanExpiredFiles) return new ExpireResult(expire.Select(s => s.SnapshotId).ToList(), []);

        // Reachability diff against the committed metadata, so snapshots another writer added meanwhile are retained.
        var after = table.Metadata;
        var candidates = await TableFiles.ReachableAsync(m, table.IO, expire, ct);
        candidates.ExceptWith(await TableFiles.ReachableAsync(after, table.IO, after.Snapshots, ct));
        await TableFiles.DeleteAllAsync(table.IO, candidates, ct);
        return new ExpireResult(expire.Select(s => s.SnapshotId).ToList(), candidates.Order(StringComparer.Ordinal).ToList());
    }

    /// <summary>Regroup data manifests by partition into about <paramref name="targetSizeBytes"/> each (see <see cref="SnapshotProducer.RewriteManifests"/>).</summary>
    public static Task<Snapshot> RewriteManifestsAsync(Table table, long targetSizeBytes = 8L * 1024 * 1024, CancellationToken ct = default) =>
        SnapshotProducer.RewriteManifests(table, targetSizeBytes).CommitAsync(ct);

    /// <summary>
    /// Delete files under the table location that no snapshot or metadata file references (Java's
    /// <c>DeleteOrphanFiles</c>): leftovers of failed or abandoned writes. Only files older than
    /// <paramref name="olderThan"/> (default 3 days, as Java) are candidates, so in-flight writes of other writers are
    /// safe; <paramref name="dryRun"/> only reports. Locations are compared after normalising URI forms
    /// (<c>s3a://</c>/<c>s3n://</c> vs <c>s3://</c>, <c>file:/</c> vs <c>file:///</c> vs bare paths).
    /// </summary>
    public static async Task<IReadOnlyList<string>> DeleteOrphanFilesAsync(Table table, TimeSpan? olderThan = null, bool dryRun = false, CancellationToken ct = default)
    {
        if (table.IO is not IO.ISupportsListing lister) throw new NotSupportedException($"{table.IO.GetType().Name} cannot list files");
        var m = table.Metadata;
        var referenced = await TableFiles.ReachableAsync(m, table.IO, m.Snapshots, ct);
        referenced.UnionWith(m.MetadataLog.Select(l => l.MetadataFile));
        if (m.MetadataFileLocation is { } current) referenced.UnionWith([current, current[..(current.LastIndexOf('/') + 1)] + "version-hint.text"]);
        referenced.UnionWith(m.Statistics.Select(s => s.StatisticsPath));
        referenced.UnionWith(m.PartitionStatistics.Select(s => s.StatisticsPath));
        var keep = referenced.Select(Normalise).ToHashSet(StringComparer.Ordinal);

        var cutoff = DateTime.UtcNow - (olderThan ?? TimeSpan.FromDays(3));
        var orphans = new List<string>();
        await foreach (var f in lister.ListAsync(m.Location.TrimEnd('/') + "/", ct))
            if (f.LastModifiedUtc < cutoff && !keep.Contains(Normalise(f.Location))) orphans.Add(f.Location);
        if (!dryRun) await TableFiles.DeleteAllAsync(table.IO, orphans, ct);
        return orphans;
    }

    private static string Normalise(string location)
    {
        if (location.StartsWith("s3a://", StringComparison.Ordinal) || location.StartsWith("s3n://", StringComparison.Ordinal)) return "s3://" + location[6..];
        if (location.StartsWith("file://", StringComparison.Ordinal)) return new Uri(location).LocalPath;
        if (location.StartsWith("file:", StringComparison.Ordinal)) return Uri.UnescapeDataString(location[5..]);
        return location;
    }
}
