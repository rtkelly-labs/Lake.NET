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
/// <param name="DeletedFiles">Files only expired snapshots referenced, deleted after the commit (or already gone).</param>
/// <remarks>
/// Deleting files is best effort once the commit has succeeded: a delete that fails is reported in
/// <see cref="FailedDeletes"/>, and a failure to work out which files to delete in <see cref="CleanupError"/> — never
/// thrown, because the snapshots are already expired and the caller must learn that. What is left is unreferenced;
/// orphan-file cleanup removes it.
/// </remarks>
public sealed record ExpireResult(IReadOnlyList<long> Expired, IReadOnlyList<string> DeletedFiles)
{
    /// <summary>Branches and tags removed because their snapshot was older than their max-ref-age.</summary>
    public IReadOnlyList<string> RemovedRefs { get; init; } = [];
    /// <summary>
    /// Files only expired snapshots referenced that were not deleted because they are outside the table's own
    /// directories (<see cref="TableLocations.Prefixes"/>): another table's files, or paths the metadata should not name.
    /// </summary>
    public IReadOnlyList<string> SkippedFiles { get; init; } = [];
    /// <summary>Files only expired snapshots referenced whose delete failed after the commit; left for orphan-file cleanup.</summary>
    public IReadOnlyList<string> FailedDeletes { get; init; } = [];
    /// <summary>
    /// Set when the files to delete could not be worked out after the commit (a manifest could not be read). Nothing
    /// was deleted: deleting without the full set of files the retained snapshots reference could remove live files.
    /// </summary>
    public Exception? CleanupError { get; init; }
    /// <summary>The snapshots expired and every file they alone referenced is gone (or was not to be deleted).</summary>
    public bool CleanupComplete => CleanupError is null && FailedDeletes.Count == 0;
}

/// <summary>
/// What <see cref="Maintenance.ExpireAsync(Table, ExpireOptions?, CancellationToken)"/> keeps. Unset values come from the
/// table properties <c>history.expire.*</c> (Java's defaults: 5 days, 1 snapshot, refs never age out); a branch's or tag's
/// own retention settings override the table-wide values for that ref, as in Java's <c>RemoveSnapshots</c>.
/// </summary>
public sealed record ExpireOptions
{
    /// <summary>Snapshots older than this (epoch ms) may expire; default <c>now − history.expire.max-snapshot-age-ms</c>.</summary>
    public long? OlderThanMs { get; init; }
    /// <summary>Ancestors each branch keeps regardless of age; default <c>history.expire.min-snapshots-to-keep</c>.</summary>
    public int? RetainLast { get; init; }
    /// <summary>Also expire these snapshots, whatever their age, unless a retained ref points at one (an error).</summary>
    public IReadOnlyList<long> SnapshotIds { get; init; } = [];
    /// <summary>Delete the files only expired snapshots referenced, after the commit (Java's default).</summary>
    public bool CleanExpiredFiles { get; init; } = true;
    /// <summary>The instant ages are measured from (epoch ms); default <see cref="TimeProvider"/>'s now.</summary>
    public long? NowMs { get; init; }
    /// <summary>
    /// The clock "now" is read from when <see cref="NowMs"/> is unset (default <see cref="System.TimeProvider.System"/>).
    /// Snapshots stamped after it (a writer whose clock runs ahead) are younger than every cut-off, so they are kept.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }
}

public static class Maintenance
{
    public const string MaxSnapshotAgeMs = "history.expire.max-snapshot-age-ms";
    public const string MinSnapshotsToKeep = "history.expire.min-snapshots-to-keep";
    public const string MaxRefAgeMs = "history.expire.max-ref-age-ms";
    public const long MaxSnapshotAgeMsDefault = 5L * 24 * 60 * 60 * 1000;
    /// <summary>The youngest file orphan cleanup may delete: anything younger may belong to a writer still running.</summary>
    public static readonly TimeSpan MinOrphanAge = TimeSpan.FromDays(1);

    /// <summary>Expire snapshots and delete the files only they referenced; see <see cref="ExpireAsync(Table, ExpireOptions?, CancellationToken)"/>.</summary>
    public static async Task<IReadOnlyList<long>> ExpireSnapshotsAsync(Table table, long olderThanMs, int? retainLast = null, CancellationToken ct = default) =>
        (await ExpireAsync(table, olderThanMs, retainLast, cleanExpiredFiles: true, ct)).Expired;

    /// <summary>
    /// Expire snapshots older than <paramref name="olderThanMs"/>, keeping <paramref name="retainLast"/> ancestors of every
    /// branch (default <c>history.expire.min-snapshots-to-keep</c>); see <see cref="ExpireAsync(Table, ExpireOptions?, CancellationToken)"/>.
    /// </summary>
    public static Task<ExpireResult> ExpireAsync(Table table, long olderThanMs, int? retainLast = null, bool cleanExpiredFiles = true, CancellationToken ct = default) =>
        ExpireAsync(table, new ExpireOptions { OlderThanMs = olderThanMs, RetainLast = retainLast, CleanExpiredFiles = cleanExpiredFiles }, ct);

    /// <summary>
    /// Expire snapshots (spec "Snapshot Retention Policy", Java's <c>RemoveSnapshots</c>). Refs other than <c>main</c> whose
    /// snapshot is older than their <c>max-ref-age-ms</c> (default <c>history.expire.max-ref-age-ms</c>) are removed. Each
    /// remaining branch keeps its head, its first <c>min-snapshots-to-keep</c> ancestors, and every further ancestor younger
    /// than its <c>max-snapshot-age-ms</c>; each remaining tag keeps its snapshot; snapshots no ref reaches are kept while
    /// younger than the table-wide age. Everything else expires. With <see cref="ExpireOptions.CleanExpiredFiles"/>, files
    /// reachable from expired snapshots but from no retained one — manifest lists, manifests, data, delete and statistics
    /// files — are deleted once the commit has succeeded. Nothing is deleted if the commit fails or its outcome is unknown.
    /// The commit asserts every ref it based its decision on; after a lost race the retention is recomputed. Only files
    /// under the table's own directories are deleted; the rest are reported in <see cref="ExpireResult.SkippedFiles"/>.
    /// As Java, a table with <c>gc.enabled=false</c> is refused: it does not own its files.
    /// </summary>
    public static async Task<ExpireResult> ExpireAsync(Table table, ExpireOptions? options, CancellationToken ct = default)
    {
        options ??= new ExpireOptions();
        var policy = CommitRetryPolicy.From(table.Metadata);
        for (var attempt = 0; ; attempt++)
        {
            var m = table.Metadata;
            // Checked on every attempt: a retry refreshes the table, and another writer may have turned GC off meanwhile.
            RequireGc(m);
            var (expire, removedRefs) = PlanExpiry(m, options);
            if (expire.Count == 0 && removedRefs.Count == 0) return new ExpireResult([], []);
            var requirements = new List<UpdateRequirement> { new UpdateRequirement.AssertTableUuid(m.TableUuid) };
            requirements.AddRange(m.Refs.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => new UpdateRequirement.AssertRefSnapshotId(kv.Key, kv.Value.SnapshotId)));
            var updates = new List<MetadataUpdate>();
            updates.AddRange(removedRefs.Select(r => new MetadataUpdate.RemoveSnapshotRef(r)));
            if (expire.Count > 0) updates.Add(new MetadataUpdate.RemoveSnapshots(expire.Select(s => s.SnapshotId).ToList()));
            try
            {
                await table.CommitAsync(requirements, updates, ct);
            }
            catch (CommitFailedException) when (attempt < policy.NumRetries)
            {
                var wait = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * (1 << Math.Min(attempt, 20)));
                await Task.Delay(wait + Random.Shared.Next(wait / 10 + 1), ct);
                await table.RefreshAsync(ct);
                continue;
            }

            var expired = expire.Select(s => s.SnapshotId).ToList();
            if (!options.CleanExpiredFiles) return new ExpireResult(expired, []) { RemovedRefs = removedRefs };
            // From here on the snapshots are expired: report, never throw (but for cancellation), so the caller learns it.
            return await DeleteExpiredFilesAsync(table, m, expire, expired, removedRefs, ct);
        }
    }

    private static async Task<ExpireResult> DeleteExpiredFilesAsync(Table table, TableMetadata m, List<Snapshot> expire, List<long> expired, List<string> removedRefs, CancellationToken ct)
    {
        // Reachability diff against the committed metadata, so snapshots another writer added meanwhile are retained.
        var after = table.Metadata;
        // The commit asserted refs and the UUID, not properties: if the committed state disables GC, delete nothing.
        if (!TableLocations.IsGcEnabled(after)) return new ExpireResult(expired, []) { RemovedRefs = removedRefs };
        HashSet<string> candidates;
        try
        {
            candidates = await TableFiles.ReachableAsync(m, table.IO, expire, ct);
            // Both sets must be complete: a retained snapshot's file missing from the second would be deleted.
            candidates.ExceptWith(await TableFiles.ReachableAsync(after, table.IO, after.Snapshots, ct));
        }
        catch (Exception e) when (e is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            return new ExpireResult(expired, []) { RemovedRefs = removedRefs, CleanupError = e };
        }
        var (inside, outside) = TableLocations.Partition(after, candidates);
        var failed = await TableFiles.TryDeleteAllAsync(table.IO, inside, ct);
        var failedSet = failed.ToHashSet(StringComparer.Ordinal);
        return new ExpireResult(expired, inside.Where(f => !failedSet.Contains(f)).Order(StringComparer.Ordinal).ToList())
        {
            RemovedRefs = removedRefs,
            SkippedFiles = outside.Order(StringComparer.Ordinal).ToList(),
            FailedDeletes = failed,
        };
    }

    private static void RequireGc(TableMetadata m)
    {
        if (!TableLocations.IsGcEnabled(m))
            throw new ValidationException("Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
    }

    /// <summary>Java's <c>RemoveSnapshots.internalApply</c>: the snapshots to remove and the refs that aged out.</summary>
    internal static (List<Snapshot> Expire, List<string> RemovedRefs) PlanExpiry(TableMetadata m, ExpireOptions options)
    {
        static long Long(TableMetadata m, string key, long fallback) =>
            long.TryParse(m.Property(key, ""), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : fallback;

        var now = options.NowMs ?? (options.TimeProvider ?? TimeProvider.System).GetUtcNow().ToUnixTimeMilliseconds();
        var defaultMaxRefAge = Long(m, MaxRefAgeMs, long.MaxValue);
        var defaultMinSnapshots = options.RetainLast ?? (int)Long(m, MinSnapshotsToKeep, 1);
        if (defaultMinSnapshots < 1) throw new ArgumentOutOfRangeException(nameof(options), "Number of snapshots to retain must be at least 1");
        var defaultOlderThan = options.OlderThanMs ?? now - Long(m, MaxSnapshotAgeMs, MaxSnapshotAgeMsDefault);

        var retainedRefs = new Dictionary<string, SnapshotRef>(StringComparer.Ordinal);
        var removedRefs = new List<string>();
        foreach (var (name, r) in m.Refs.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            var snapshot = m.SnapshotById(r.SnapshotId);
            if (name == SnapshotRef.Main || (snapshot is not null && now - snapshot.TimestampMs <= (r.MaxRefAgeMs ?? defaultMaxRefAge))) retainedRefs[name] = r;
            else removedRefs.Add(name);
        }

        var explicitIds = options.SnapshotIds.ToHashSet();
        foreach (var id in explicitIds)
        {
            var holders = retainedRefs.Where(kv => kv.Value.SnapshotId == id).Select(kv => kv.Key).ToList();
            if (holders.Count > 0) throw new ArgumentException($"Cannot expire {id}. Still referenced by refs: {string.Join(", ", holders)}");
        }

        var retain = retainedRefs.Values.Select(r => r.SnapshotId).ToHashSet();
        var reachable = new HashSet<long>();
        foreach (var r in retainedRefs.Values)
        {
            if (r.Type == SnapshotRefType.Tag)
            {
                reachable.Add(r.SnapshotId);
                continue;
            }
            var minSnapshots = r.MinSnapshotsToKeep ?? defaultMinSnapshots;
            var olderThan = r.MaxSnapshotAgeMs is { } age ? now - age : defaultOlderThan;
            var kept = 0;
            var withinRetention = true;
            foreach (var ancestor in m.Ancestors(r.SnapshotId))
            {
                reachable.Add(ancestor.SnapshotId);
                if (withinRetention && (kept < minSnapshots || ancestor.TimestampMs >= olderThan))
                {
                    retain.Add(ancestor.SnapshotId);
                    kept++;
                }
                else withinRetention = false;
            }
        }
        // Snapshots no retained ref reaches (rolled back, or of a removed ref) stay while younger than the table-wide age.
        retain.UnionWith(m.Snapshots.Where(s => !reachable.Contains(s.SnapshotId) && s.TimestampMs >= defaultOlderThan).Select(s => s.SnapshotId));

        var expire = m.Snapshots.Where(s => explicitIds.Contains(s.SnapshotId) || !retain.Contains(s.SnapshotId)).ToList();
        return (expire, removedRefs);
    }

    /// <summary>Regroup data manifests by partition into about <paramref name="targetSizeBytes"/> each (see <see cref="SnapshotProducer.RewriteManifests"/>).</summary>
    public static Task<Snapshot> RewriteManifestsAsync(Table table, long targetSizeBytes = 8L * 1024 * 1024, CancellationToken ct = default) =>
        SnapshotProducer.RewriteManifests(table, targetSizeBytes).CommitAsync(ct);

    /// <summary>
    /// Delete files under the table location that no snapshot or metadata file references; see
    /// <see cref="RemoveOrphanFilesAsync"/>. Returns the orphans found. Every orphan is tried; if some could not be
    /// deleted, the first error is thrown afterwards (nothing was committed, so nothing is lost by throwing).
    /// <see cref="RemoveOrphanFilesAsync"/> reports them instead.
    /// </summary>
    public static async Task<IReadOnlyList<string>> DeleteOrphanFilesAsync(Table table, TimeSpan? olderThan = null, bool dryRun = false, CancellationToken ct = default)
    {
        var result = await RemoveOrphanFilesAsync(table, new OrphanFilesOptions { OlderThan = olderThan, DryRun = dryRun }, ct);
        if (result.FailedDeletes.Count > 0)
            throw new IOException($"Could not delete {result.FailedDeletes.Count} of {result.Orphans.Count} orphan files of {table.Identifier}, e.g. {result.FailedDeletes[0]}");
        return result.Orphans;
    }

    /// <summary>
    /// Delete files under the table location that no snapshot or metadata file references (Java's
    /// <c>DeleteOrphanFiles</c>): leftovers of failed or abandoned writes. Only files older than
    /// <see cref="OrphanFilesOptions.OlderThan"/> (default 3 days, as Java; at least <see cref="MinOrphanAge"/>, as Spark's
    /// procedure) are candidates, so in-flight writes of other writers are safe; <see cref="OrphanFilesOptions.DryRun"/>
    /// only reports. The table is refreshed first, so files committed since it was loaded are not taken for orphans.
    /// Refused when the location is not safe to list under (<see cref="TableLocations.RequireSafeRoot"/>) or
    /// <c>gc.enabled</c> is false. Locations are compared after normalising URI forms (<c>s3a://</c>/<c>s3n://</c> vs
    /// <c>s3://</c>, <c>file:/</c> vs <c>file:///</c> vs bare paths). Deletes are best effort: the ones that fail are
    /// reported in <see cref="OrphanFilesResult.FailedDeletes"/>, not thrown.
    /// </summary>
    public static async Task<OrphanFilesResult> RemoveOrphanFilesAsync(Table table, OrphanFilesOptions? options = null, CancellationToken ct = default)
    {
        options ??= new OrphanFilesOptions();
        if (table.IO is not IO.ISupportsListing lister) throw new NotSupportedException($"{table.IO.GetType().Name} cannot list files");
        var age = options.OlderThan ?? TimeSpan.FromDays(3);
        if (age < MinOrphanAge)
            throw new ArgumentOutOfRangeException(nameof(options), age, $"Orphan cleanup needs files at least {MinOrphanAge.TotalHours:0} hours old: younger ones may belong to a writer still running");
        await table.RefreshAsync(ct);
        var m = table.Metadata;
        if (!TableLocations.IsGcEnabled(m))
            throw new ValidationException("Cannot remove orphan files: GC is disabled (deleting files may corrupt other tables)");
        TableLocations.RequireSafeRoot(m.Location);
        var referenced = await TableFiles.ReachableAsync(m, table.IO, m.Snapshots, ct);
        referenced.UnionWith(m.MetadataLog.Select(l => l.MetadataFile));
        if (m.MetadataFileLocation is { } current) referenced.UnionWith([current, current[..(current.LastIndexOf('/') + 1)] + "version-hint.text"]);
        referenced.UnionWith(m.Statistics.Select(s => s.StatisticsPath));
        referenced.UnionWith(m.PartitionStatistics.Select(s => s.StatisticsPath));
        var keep = referenced.Select(TableLocations.Normalise).ToHashSet(StringComparer.Ordinal);

        // The store stamps LastModified with its clock and we compare it with ours: a file written "in the future" by a
        // skewed writer is younger than the cut-off, so skew up to the minimum age only delays deletes, never hastens them.
        var cutoff = (options.TimeProvider ?? TimeProvider.System).GetUtcNow().UtcDateTime - age;
        var orphans = new List<string>();
        await foreach (var f in lister.ListAsync(m.Location.TrimEnd('/') + "/", ct))
            if (f.LastModifiedUtc < cutoff && !keep.Contains(TableLocations.Normalise(f.Location))) orphans.Add(f.Location);
        if (options.DryRun) return new OrphanFilesResult(orphans);
        return new OrphanFilesResult(orphans) { FailedDeletes = await TableFiles.TryDeleteAllAsync(table.IO, orphans, ct) };
    }
}

/// <summary>What <see cref="Maintenance.RemoveOrphanFilesAsync"/> considers and does.</summary>
public sealed record OrphanFilesOptions
{
    /// <summary>Only files older than this are candidates; default 3 days, at least <see cref="Maintenance.MinOrphanAge"/>.</summary>
    public TimeSpan? OlderThan { get; init; }
    /// <summary>Only report the orphans.</summary>
    public bool DryRun { get; init; }
    /// <summary>The clock file ages are measured with (default <see cref="System.TimeProvider.System"/>).</summary>
    public TimeProvider? TimeProvider { get; init; }
}

/// <param name="Orphans">Unreferenced files old enough to delete (deleted unless a dry run, or listed in <see cref="FailedDeletes"/>).</param>
public sealed record OrphanFilesResult(IReadOnlyList<string> Orphans)
{
    /// <summary>Orphans whose delete failed; the next run finds them again.</summary>
    public IReadOnlyList<string> FailedDeletes { get; init; } = [];
}
