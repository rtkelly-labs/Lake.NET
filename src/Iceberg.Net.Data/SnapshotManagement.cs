using System.Globalization;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Data;

/// <summary>Retention settings of a branch or tag (spec "Snapshot Retention Policy"); null inherits the table default.</summary>
/// <param name="MinSnapshotsToKeep">Branches only: ancestors of the head always kept by expiry.</param>
/// <param name="MaxSnapshotAgeMs">Branches only: ancestors younger than this are kept by expiry.</param>
/// <param name="MaxRefAgeMs">How old the ref's snapshot may get before expiry removes the ref itself (never <c>main</c>).</param>
public sealed record RefRetention(int? MinSnapshotsToKeep = null, long? MaxSnapshotAgeMs = null, long? MaxRefAgeMs = null);

public static class SnapshotManagementExtensions
{
    /// <summary>
    /// Branches, tags, rollback, cherry-pick and fast-forward (Java's <c>ManageSnapshots</c>), e.g.
    /// <c>await table.ManageSnapshots().CreateTag("v1", id).FastForwardBranch("main", "audit").CommitAsync()</c>.
    /// </summary>
    public static SnapshotManager ManageSnapshots(this Table table) => new(table);
}

/// <summary>
/// Collects ref changes and commits them as one metadata update, guarded by <c>assert-ref-snapshot-id</c> for every ref it
/// reads or changes (as Java's <c>UpdateRequirements</c> derives them), so a concurrent change to one of those refs fails
/// the swap instead of being overwritten.
/// </summary>
/// <remarks>
/// After a lost race the changes are re-validated and re-applied on the refreshed table only when that cannot discard the
/// other writer's work: fast-forwards (still checked to be fast-forwards), creating refs and retention settings. A change
/// that moves or removes a ref to a fixed place — rollback, <see cref="SetCurrentSnapshot"/>, replace, rename, remove —
/// throws <see cref="CommitFailedException"/> when that ref moved since this manager was created, because applying it to
/// the new head would silently drop the commits made meanwhile. Nothing is applied in that case. Cherry-pick is committed
/// as a snapshot of its own (replayed appends compose with concurrent appends, as Java's <c>CherryPickOperation</c>), so it
/// cannot be combined with other changes in one manager.
/// </remarks>
public sealed class SnapshotManager
{
    private readonly Table _table;
    private readonly TableMetadata _observed;
    private readonly List<Func<RefState, ValueTask>> _ops = [];
    /// <summary>Refs whose value when this manager was created is what an operation expects; moved since → conflict.</summary>
    private readonly HashSet<string> _pinned = new(StringComparer.Ordinal);
    private long? _cherryPick;
    private int? _retries;

    internal SnapshotManager(Table table)
    {
        _table = table;
        _observed = table.Metadata;
    }

    private SnapshotManager Add(Func<RefState, ValueTask> op, params string[] pins)
    {
        if (_cherryPick is not null) throw new InvalidOperationException("Cherry-pick commits a snapshot of its own and cannot be combined with other changes");
        _ops.Add(op);
        _pinned.UnionWith(pins);
        return this;
    }

    private SnapshotManager Add(Action<RefState> op, params string[] pins) => Add(s => { op(s); return ValueTask.CompletedTask; }, pins);

    /// <summary>Override <c>commit.retry.num-retries</c> for re-applying rebasable changes after a lost race.</summary>
    public SnapshotManager WithRetries(int retries)
    {
        _retries = retries;
        return this;
    }

    /// <summary>Create a branch at <paramref name="snapshotId"/> (default: the current snapshot of <c>main</c>).</summary>
    public SnapshotManager CreateBranch(string name, long? snapshotId = null, RefRetention? retention = null)
    {
        var id = snapshotId ?? _observed.CurrentSnapshotId ?? throw new ArgumentException($"Cannot create branch {name}: the table has no current snapshot; pass a snapshot ID");
        return Add(s =>
        {
            s.RequireAbsent(name);
            s.RequireSnapshot(id);
            s.Set(name, new SnapshotRef(id, SnapshotRefType.Branch, retention?.MinSnapshotsToKeep, retention?.MaxSnapshotAgeMs, retention?.MaxRefAgeMs));
        });
    }

    /// <summary>Create a tag at <paramref name="snapshotId"/>; tags only take <see cref="RefRetention.MaxRefAgeMs"/>.</summary>
    public SnapshotManager CreateTag(string name, long snapshotId, RefRetention? retention = null)
    {
        if (retention is { MinSnapshotsToKeep: not null } or { MaxSnapshotAgeMs: not null })
            throw new ArgumentException("Tags do not support min-snapshots-to-keep or max-snapshot-age-ms");
        return Add(s =>
        {
            s.RequireAbsent(name);
            s.RequireSnapshot(snapshotId);
            s.Set(name, new SnapshotRef(snapshotId, SnapshotRefType.Tag, MaxRefAgeMs: retention?.MaxRefAgeMs));
        });
    }

    public SnapshotManager RemoveBranch(string name)
    {
        if (name == SnapshotRef.Main) throw new ArgumentException("Cannot remove main branch");
        return Add(s =>
        {
            s.Require(name, SnapshotRefType.Branch);
            s.Remove(name);
        }, name);
    }

    public SnapshotManager RemoveTag(string name) => Add(s =>
    {
        s.Require(name, SnapshotRefType.Tag);
        s.Remove(name);
    }, name);

    /// <summary>Rename a branch, keeping its snapshot and retention settings.</summary>
    public SnapshotManager RenameBranch(string name, string newName)
    {
        if (name == SnapshotRef.Main) throw new ArgumentException("Cannot rename main branch");
        return Add(s =>
        {
            var r = s.Require(name, SnapshotRefType.Branch);
            s.RequireAbsent(newName);
            s.Remove(name);
            s.Set(newName, r);
        }, name);
    }

    /// <summary>Point a branch at any snapshot (keeping its retention settings), as Java's <c>replaceBranch(name, id)</c>.</summary>
    public SnapshotManager ReplaceBranch(string name, long snapshotId) => Add(s =>
    {
        var r = s.Require(name, SnapshotRefType.Branch);
        s.RequireSnapshot(snapshotId);
        s.Set(name, r with { SnapshotId = snapshotId });
    }, name);

    /// <summary>Point branch <paramref name="name"/> at the snapshot ref <paramref name="source"/> points at, fast-forward or not.</summary>
    public SnapshotManager ReplaceBranch(string name, string source) => Add(s =>
    {
        var r = s.Require(name, SnapshotRefType.Branch);
        var to = s.Read(source);
        s.Set(name, r with { SnapshotId = to.SnapshotId });
    }, name, source);

    /// <summary>
    /// Move branch <paramref name="name"/> to the head of <paramref name="source"/>, which must descend from it (Java's
    /// <c>fastForwardBranch</c>; Spark's <c>fast_forward</c> procedure). Re-validated and re-applied after a lost race.
    /// </summary>
    public SnapshotManager FastForwardBranch(string name, string source) => Add(s =>
    {
        var r = s.Require(name, SnapshotRefType.Branch);
        var to = s.Read(source);
        if (!s.Metadata.Ancestors(to.SnapshotId).Any(a => a.SnapshotId == r.SnapshotId))
            throw new ArgumentException($"Cannot fast-forward: {name} is not an ancestor of {source}");
        s.Set(name, r with { SnapshotId = to.SnapshotId });
    });

    public SnapshotManager ReplaceTag(string name, long snapshotId) => Add(s =>
    {
        var r = s.Require(name, SnapshotRefType.Tag);
        s.RequireSnapshot(snapshotId);
        s.Set(name, r with { SnapshotId = snapshotId });
    }, name);

    public SnapshotManager SetMinSnapshotsToKeep(string branch, int minSnapshotsToKeep)
    {
        if (minSnapshotsToKeep <= 0) throw new ArgumentOutOfRangeException(nameof(minSnapshotsToKeep), "Min snapshots to keep must be greater than 0");
        return Add(s => s.Set(branch, s.Require(branch, SnapshotRefType.Branch) with { MinSnapshotsToKeep = minSnapshotsToKeep }));
    }

    public SnapshotManager SetMaxSnapshotAgeMs(string branch, long maxSnapshotAgeMs)
    {
        if (maxSnapshotAgeMs <= 0) throw new ArgumentOutOfRangeException(nameof(maxSnapshotAgeMs), "Max snapshot age must be greater than 0 ms");
        return Add(s => s.Set(branch, s.Require(branch, SnapshotRefType.Branch) with { MaxSnapshotAgeMs = maxSnapshotAgeMs }));
    }

    public SnapshotManager SetMaxRefAgeMs(string name, long maxRefAgeMs)
    {
        if (maxRefAgeMs <= 0) throw new ArgumentOutOfRangeException(nameof(maxRefAgeMs), "Max reference age must be greater than 0");
        return Add(s => s.Set(name, s.Read(name) with { MaxRefAgeMs = maxRefAgeMs }));
    }

    /// <summary>Roll <c>main</c> back to <paramref name="snapshotId"/>, which must be an ancestor of its current snapshot (Java's <c>rollbackTo</c>).</summary>
    public SnapshotManager RollbackTo(long snapshotId) => Add(s =>
    {
        s.RequireSnapshot(snapshotId);
        if (!s.Metadata.Ancestors(s.Head(SnapshotRef.Main)).Any(a => a.SnapshotId == snapshotId))
            throw new ValidationException($"Cannot roll back to snapshot, not an ancestor of the current state: {snapshotId}");
        s.SetMain(snapshotId);
    }, SnapshotRef.Main);

    /// <summary>Roll <c>main</c> back to its latest ancestor committed strictly before <paramref name="instant"/> (Java's <c>rollbackToTime</c>).</summary>
    public SnapshotManager RollbackToTime(DateTimeOffset instant)
    {
        var ms = instant.ToUnixTimeMilliseconds();
        return Add(s =>
        {
            var target = s.Metadata.Ancestors(s.Head(SnapshotRef.Main)).FirstOrDefault(a => a.TimestampMs < ms)
                ?? throw new ValidationException($"Cannot roll back, no valid snapshot older than: {ms}");
            s.SetMain(target.SnapshotId);
        }, SnapshotRef.Main);
    }

    /// <summary>Make any existing snapshot the current one (Java's <c>setCurrentSnapshot</c>; it need not be an ancestor).</summary>
    public SnapshotManager SetCurrentSnapshot(long snapshotId) => Add(s =>
    {
        s.RequireSnapshot(snapshotId);
        s.SetMain(snapshotId);
    }, SnapshotRef.Main);

    /// <summary>
    /// Apply snapshot <paramref name="snapshotId"/> (typically staged on another branch) to <c>main</c>, as Java's
    /// <c>CherryPickOperation</c>: fast-forward when its parent is main's head; otherwise an <c>append</c> is replayed as a
    /// new snapshot adding the same data files, and a dynamic partition overwrite (<c>replace-partitions=true</c>) is
    /// replayed when no file was added to its partitions since its parent. The new snapshot records
    /// <c>source-snapshot-id</c>; picking a snapshot already in main's history, or picked before, fails.
    /// </summary>
    public SnapshotManager CherryPick(long snapshotId)
    {
        if (_ops.Count > 0 || _cherryPick is not null) throw new InvalidOperationException("Cherry-pick commits a snapshot of its own and cannot be combined with other changes");
        _cherryPick = snapshotId;
        return this;
    }

    public async Task<TableMetadata> CommitAsync(CancellationToken ct = default)
    {
        if (_cherryPick is { } pick) return await CherryPicker.CommitAsync(_table, pick, _retries, ct);
        if (_ops.Count == 0) return _table.Metadata;

        var policy = CommitRetryPolicy.From(_table.Metadata);
        if (_retries is { } r) policy = policy with { NumRetries = r };
        for (var attempt = 0; ; attempt++)
        {
            var @base = _table.Metadata;
            foreach (var name in _pinned)
            {
                var then = _observed.Refs.GetValueOrDefault(name)?.SnapshotId;
                var now = @base.Refs.GetValueOrDefault(name)?.SnapshotId;
                if (then != now)
                    throw new CommitFailedException($"Ref {name} moved from {Show(then)} to {Show(now)} since these snapshot changes were planned; re-plan against the current table state");
            }
            var state = new RefState(@base);
            foreach (var op in _ops) await op(state);
            var (requirements, updates) = state.ToCommit();
            if (updates.Count == 0) return @base;
            try
            {
                return await _table.CommitAsync(requirements, updates, ct);
            }
            catch (CommitFailedException) when (attempt < policy.NumRetries)
            {
                var wait = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * (1 << Math.Min(attempt, 20)));
                await Task.Delay(wait + Random.Shared.Next(wait / 10 + 1), ct);
                await _table.RefreshAsync(ct);
            }
        }
    }

    private static string Show(long? id) => id?.ToString(CultureInfo.InvariantCulture) ?? "(none)";

    /// <summary>The refs as the operations so far left them, plus every ref read or changed (for the requirements).</summary>
    private sealed class RefState(TableMetadata @base)
    {
        private readonly Dictionary<string, SnapshotRef> _refs = new(@base.Refs, StringComparer.Ordinal);
        private readonly HashSet<string> _touched = new(StringComparer.Ordinal);

        public TableMetadata Metadata => @base;

        public long? Head(string name)
        {
            _touched.Add(name);
            return _refs.GetValueOrDefault(name)?.SnapshotId;
        }

        public SnapshotRef Read(string name)
        {
            _touched.Add(name);
            return _refs.GetValueOrDefault(name) ?? throw new ArgumentException($"Ref does not exist: {name}");
        }

        public SnapshotRef Require(string name, SnapshotRefType type)
        {
            var r = Read(name);
            if (r.Type != type) throw new ArgumentException($"Ref {name} is a {r.Type.ToString().ToLowerInvariant()}, not a {type.ToString().ToLowerInvariant()}");
            return r;
        }

        public void RequireAbsent(string name)
        {
            _touched.Add(name);
            if (_refs.ContainsKey(name)) throw new ArgumentException($"Ref {name} already exists");
        }

        public void RequireSnapshot(long id)
        {
            if (@base.SnapshotById(id) is null) throw new ArgumentException($"Cannot find snapshot with ID {id}");
        }

        public void Set(string name, SnapshotRef r)
        {
            _touched.Add(name);
            _refs[name] = r;
        }

        public void SetMain(long id) =>
            Set(SnapshotRef.Main, _refs.TryGetValue(SnapshotRef.Main, out var main) ? main with { SnapshotId = id } : SnapshotRef.Branch(id));

        public void Remove(string name)
        {
            _touched.Add(name);
            _refs.Remove(name);
        }

        public (List<UpdateRequirement>, List<MetadataUpdate>) ToCommit()
        {
            var requirements = new List<UpdateRequirement> { new UpdateRequirement.AssertTableUuid(@base.TableUuid) };
            var updates = new List<MetadataUpdate>();
            foreach (var name in _touched.Order(StringComparer.Ordinal))
                requirements.Add(new UpdateRequirement.AssertRefSnapshotId(name, @base.Refs.GetValueOrDefault(name)?.SnapshotId));
            foreach (var name in @base.Refs.Keys.Where(n => !_refs.ContainsKey(n)).Order(StringComparer.Ordinal))
                updates.Add(new MetadataUpdate.RemoveSnapshotRef(name));
            // main last, so the snapshot log records it after any other change in this commit.
            foreach (var (name, r) in _refs.OrderBy(kv => kv.Key == SnapshotRef.Main).ThenBy(kv => kv.Key, StringComparer.Ordinal))
                if (@base.Refs.GetValueOrDefault(name) != r) updates.Add(new MetadataUpdate.SetSnapshotRef(name, r));
            return (requirements, updates);
        }
    }
}

/// <summary>Java's <c>CherryPickOperation</c> on top of <see cref="SnapshotProducer"/>.</summary>
internal static class CherryPicker
{
    public const string SourceSnapshotId = "source-snapshot-id";
    public const string ReplacePartitions = "replace-partitions";

    public static async Task<TableMetadata> CommitAsync(Table table, long snapshotId, int? retries, CancellationToken ct)
    {
        var policy = CommitRetryPolicy.From(table.Metadata);
        if (retries is { } r) policy = policy with { NumRetries = r };
        for (var attempt = 0; ; attempt++)
        {
            var m = table.Metadata;
            var pick = m.SnapshotById(snapshotId) ?? throw new ValidationException($"Cannot cherry-pick unknown snapshot ID: {snapshotId}");
            var dynamicOverwrite = pick.Operation == Operations.Overwrite && pick.Summary.GetValueOrDefault(ReplacePartitions) == "true";
            if (IsFastForward(m, pick))
            {
                // Its parent is main's head: publishing it is a ref move (no new snapshot), whatever the operation.
                var main = m.Refs.GetValueOrDefault(SnapshotRef.Main);
                try
                {
                    return await table.CommitAsync(
                        [new UpdateRequirement.AssertTableUuid(m.TableUuid), new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, m.CurrentSnapshotId)],
                        [new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, main is null ? SnapshotRef.Branch(pick.SnapshotId) : main with { SnapshotId = pick.SnapshotId })], ct);
                }
                catch (CommitFailedException) when (attempt < policy.NumRetries)
                {
                    await table.RefreshAsync(ct);
                    continue; // main moved: no longer a fast-forward; replay instead
                }
            }
            if (pick.Operation != Operations.Append && !dynamicOverwrite)
                throw new ValidationException($"Cannot cherry-pick snapshot {snapshotId}: not append, dynamic overwrite, or fast-forward");

            var (added, removed) = await ChangedDataFilesAsync(table, pick, ct);
            if (added.FirstOrDefault(f => f.SpecId != m.DefaultSpecId) is { } other)
                throw new NotSupportedException($"Cannot cherry-pick snapshot {snapshotId}: {other.FilePath} was written with partition spec {other.SpecId}, not the default spec {m.DefaultSpecId}");
            var replaced = added.Select(f => PartitionKey(f.SpecId, f.Partition)).ToHashSet(StringComparer.Ordinal);
            var removedPaths = removed.Select(f => f.FilePath).ToHashSet(StringComparer.Ordinal);

            var producer = dynamicOverwrite
                ? SnapshotProducer.Overwrite(table, f => removedPaths.Contains(f.FilePath)).WithSnapshotIsolation().Set(ReplacePartitions, "true")
                : SnapshotProducer.Append(table);
            producer.AddFiles(added.Select(f => f with { FirstRowId = null }))
                .Set(SourceSnapshotId, snapshotId.ToString(CultureInfo.InvariantCulture))
                .WithRetries(policy.NumRetries)
                .ValidateEachAttempt((current, c) => ValidateAsync(table, current, pick, dynamicOverwrite ? replaced : null, removedPaths, c));
            await producer.CommitAsync(ct);
            return table.Metadata;
        }
    }

    private static bool IsFastForward(TableMetadata m, Snapshot pick) => m.CurrentSnapshotId == pick.ParentSnapshotId;

    /// <summary>Java's <c>validateNonAncestor</c>, <c>validateReplacedPartitions</c> and <c>failMissingDeletePaths</c>, against the state a replay commits on.</summary>
    private static async Task ValidateAsync(Table table, TableMetadata m, Snapshot pick, HashSet<string>? replacedPartitions, HashSet<string> removedPaths, CancellationToken ct)
    {
        var ancestors = m.Ancestors(m.CurrentSnapshotId).ToList();
        if (ancestors.Any(a => a.SnapshotId == pick.SnapshotId))
            throw new ValidationException($"Cannot cherry-pick snapshot {pick.SnapshotId}: already an ancestor");
        var source = pick.SnapshotId.ToString(CultureInfo.InvariantCulture);
        if (ancestors.FirstOrDefault(a => a.Summary.GetValueOrDefault(SourceSnapshotId) == source) is { } earlier)
            throw new ValidationException($"Cannot cherry-pick snapshot {pick.SnapshotId}: already picked to create ancestor {earlier.SnapshotId}");
        if (replacedPartitions is null) return;

        if (pick.ParentSnapshotId is { } parent)
        {
            var since = ancestors.TakeWhile(a => a.SnapshotId != parent).ToList();
            if (since.Count == ancestors.Count)
                throw new ValidationException($"Cannot cherry-pick overwrite not based on an ancestor of the current state: {parent}");
            foreach (var snapshot in since)
                foreach (var f in (await ChangedDataFilesAsync(table, snapshot, ct)).Added)
                    if (replacedPartitions.Contains(PartitionKey(f.SpecId, f.Partition)))
                        throw new ValidationException($"Cannot cherry-pick replace partitions with changed partition: {string.Join(", ", f.Partition)}");
        }
        if (removedPaths.Count > 0)
        {
            var live = (await TableScan.PlanFilesAsync(table, m.CurrentSnapshotId, null, ct)).Select(t => t.File.FilePath).ToHashSet(StringComparer.Ordinal);
            if (removedPaths.FirstOrDefault(p => !live.Contains(p)) is { } gone)
                throw new ValidationException($"Cannot cherry-pick snapshot {pick.SnapshotId}: {gone}, which it replaces, is no longer live");
        }
    }

    /// <summary>Data files a snapshot added and removed itself (from the manifests it wrote).</summary>
    internal static async Task<(List<DataFile> Added, List<DataFile> Removed)> ChangedDataFilesAsync(Table table, Snapshot snapshot, CancellationToken ct)
    {
        var m = table.Metadata;
        var added = new List<DataFile>();
        var removed = new List<DataFile>();
        foreach (var manifest in (await TableScan.ManifestsAsync(table, snapshot, ct)).Where(mf => mf.Content == ManifestContent.Data && mf.AddedSnapshotId == snapshot.SnapshotId))
        {
            var entries = ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(manifest.ManifestPath, ct), m.SpecById(manifest.PartitionSpecId), m.CurrentSchema, manifest).Entries;
            foreach (var e in entries.Where(e => e.SnapshotId == snapshot.SnapshotId))
            {
                var file = e.DataFile with { SpecId = manifest.PartitionSpecId };
                if (e.Status == ManifestEntryStatus.Added) added.Add(file);
                else if (e.Status == ManifestEntryStatus.Deleted) removed.Add(file);
            }
        }
        return (added, removed);
    }

    internal static string PartitionKey(int specId, IReadOnlyList<object?> partition) =>
        specId.ToString(CultureInfo.InvariantCulture) + "|" + string.Join('\u001f', partition.Select(v => v is byte[] b ? Convert.ToHexString(b) : Convert.ToString(v, CultureInfo.InvariantCulture) ?? "\0"));
}
