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
public sealed record CommitRetryPolicy(int NumRetries = 4, int MinWaitMs = 100, int MaxWaitMs = 60_000)
{
    /// <summary>From table properties <c>commit.retry.*</c>, as Java reads them.</summary>
    public static CommitRetryPolicy From(TableMetadata m) => new(
        int.Parse(m.Property("commit.retry.num-retries", "4"), CultureInfo.InvariantCulture),
        int.Parse(m.Property("commit.retry.min-wait-ms", "100"), CultureInfo.InvariantCulture),
        int.Parse(m.Property("commit.retry.max-wait-ms", "60000"), CultureInfo.InvariantCulture));
}

/// <summary>
/// Produces a new snapshot: writes manifests and a manifest list, then commits
/// <c>add-snapshot</c> + <c>set-snapshot-ref main</c> guarded by <c>assert-ref-snapshot-id</c>.
/// On <see cref="CommitFailedException"/> it refreshes, re-derives the manifest list on the new parent,
/// and retries with exponential backoff. New manifests are reused across attempts (their entries
/// inherit sequence numbers, so they stay valid on any parent).
/// </summary>
public sealed class SnapshotProducer
{
    private readonly Table _table;
    private readonly List<DataFile> _added = [];
    private Func<DataFile, bool>? _deleteWhere;
    /// <summary>Null = derive from what the snapshot does (append / delete / overwrite).</summary>
    private string? _operation = Operations.Append;
    private readonly HashSet<string> _removePaths = new(StringComparer.Ordinal);
    private readonly List<DataFile> _addedDeleteFiles = [];
    private int? _retries;
    private readonly Dictionary<string, string> _extraSummary = [];
    /// <summary>The manifests of added files, one per partition spec, written once and reused across retries.</summary>
    private List<ManifestFile>? _writtenManifests;
    private readonly HashSet<string> _writtenManifestPaths = new(StringComparer.Ordinal);
    /// <summary>Files written by failed attempts (deleted after a successful commit unless the winner references them).</summary>
    private readonly List<string> _staleArtifacts = [];
    /// <summary>Files written by the current attempt (kept if it commits).</summary>
    private readonly List<string> _attemptArtifacts = [];
    private Dictionary<string, RoaringPositionBitmap>? _rowDeletes;
    private bool _convertPositionDeletes;

    private SnapshotProducer(Table table)
    {
        _table = table;
        _startingSnapshotId = table.Metadata.CurrentSnapshotId;
    }

    private long? _startingSnapshotId;
    private string _branch = SnapshotRef.Main;

    /// <summary>
    /// Commit to <paramref name="branch"/> instead of <c>main</c> (Java's <c>toBranch</c>): the new snapshot's parent is the
    /// branch head, validation looks at the branch's history, and only that ref moves, so <c>main</c> is untouched (the
    /// write half of write-audit-publish). A branch that does not exist yet is created from <c>main</c>'s current snapshot.
    /// </summary>
    public SnapshotProducer ToBranch(string branch)
    {
        ArgumentException.ThrowIfNullOrEmpty(branch);
        if (_table.Metadata.Refs.TryGetValue(branch, out var r) && r.Type != SnapshotRefType.Branch)
            throw new ArgumentException($"{branch} is a tag, not a branch");
        _branch = branch;
        _startingSnapshotId = Head(_table.Metadata);
        return this;
    }

    /// <summary>The branch head this producer commits on (main's current snapshot for a branch not created yet).</summary>
    private long? Head(TableMetadata m) =>
        _branch != SnapshotRef.Main && m.Refs.TryGetValue(_branch, out var r) ? r.SnapshotId : m.CurrentSnapshotId;

    private UpdateRequirement.AssertRefSnapshotId BranchRequirement(TableMetadata m) =>
        new(_branch, m.Refs.TryGetValue(_branch, out var r) ? r.SnapshotId : null);
    private Func<DataFile, bool>? _conflictFilter;
    /// <summary>The data files the first attempt removed: a serializable retry must still find every one of them live.</summary>
    private List<DataFile>? _removedAtStart;

    public static SnapshotProducer Append(Table table) => new(table);

    /// <summary>Copy-on-write overwrite: whole files matching <paramref name="deleteWhere"/> are removed, then new files added.</summary>
    /// <remarks>
    /// Serializable by default (Java's <c>validateNoConflictingData</c>, as Spark uses): if, by the time this commits,
    /// another writer has added a data file <paramref name="deleteWhere"/> selects, the commit fails with
    /// <see cref="ValidationException"/> rather than silently removing that writer's data on retry. Opt out with
    /// <see cref="WithSnapshotIsolation"/>.
    /// </remarks>
    public static SnapshotProducer Overwrite(Table table, Func<DataFile, bool> deleteWhere) =>
        new(table) { _deleteWhere = deleteWhere, _operation = Operations.Overwrite, _conflictFilter = deleteWhere };

    /// <summary>Remove whole data files without adding any (a metadata-only delete); serializable like <see cref="Overwrite"/>.</summary>
    public static SnapshotProducer DeleteFiles(Table table, Func<DataFile, bool> deleteWhere) =>
        new(table) { _deleteWhere = deleteWhere, _operation = Operations.Delete, _conflictFilter = deleteWhere };

    /// <summary>Skip conflict validation: on retry the predicate is simply re-applied to the newer table state.</summary>
    public SnapshotProducer WithSnapshotIsolation()
    {
        _conflictFilter = null;
        return this;
    }

    /// <summary>
    /// Data files added since this producer's starting snapshot that <see cref="_conflictFilter"/> selects. Read from each
    /// new snapshot's own manifest list, so files later merged into other manifests are still found.
    /// </summary>
    private async Task ValidateNoConflictingDataAsync(CancellationToken ct)
    {
        if (_conflictFilter is null) return;
        var m = _table.Metadata;
        var since = new List<Snapshot>();
        var reachedStart = _startingSnapshotId is null;
        var head = Head(m);
        foreach (var snapshot in m.Ancestors(head))
        {
            if (snapshot.SnapshotId == _startingSnapshotId)
            {
                reachedStart = true;
                break;
            }
            since.Add(snapshot);
        }
        // The starting snapshot expired (Glue's retention optimizer, Athena VACUUM, ExpireAsync) or main was rolled back
        // past it. Snapshots in between may have expired with it, so a conflicting file can no longer be ruled out (Java:
        // "Cannot determine history between starting snapshot ... and the last known ancestor").
        if (!reachedStart)
            throw new ValidationException($"Cannot determine history between starting snapshot {_startingSnapshotId} and current snapshot {head} of {_branch}: " +
                $"it has expired or is no longer an ancestor of {_branch}; re-plan this {_operation} against the current table state");
        var removed = _removedAtStart ?? [];
        var removedPaths = removed.Select(f => f.FilePath).ToHashSet();
        foreach (var snapshot in since)
        {
            if (snapshot.ManifestList is null) continue;
            foreach (var manifest in (await TableScan.ManifestsAsync(_table, snapshot, ct)).Where(mf => mf.AddedSnapshotId == snapshot.SnapshotId))
            {
                var entries = ManifestCodec.ReadManifest(await _table.IO.ReadAllAsync(manifest.ManifestPath, ct), m.SpecById(manifest.PartitionSpecId), m.CurrentSchema, manifest).Entries
                    .Where(e => e.Status == ManifestEntryStatus.Added);
                if (manifest.Content == ManifestContent.Data)
                {
                    if (entries.FirstOrDefault(e => _conflictFilter(e.DataFile)) is { } conflict)
                        throw new ValidationException($"Concurrent snapshot {snapshot.SnapshotId} ({snapshot.Operation}) added {conflict.DataFile.FilePath}, which this {_operation} would remove; re-plan against the new table state");
                    continue;
                }
                // Java's validateNoConflictingDeletes: a delete added meanwhile against a file this commit removes would be
                // dropped with the file while this commit re-adds rows read before it, so the deleted rows would return.
                // A delete naming its data file conflicts only with that file; one that does not (multi-file position
                // deletes, equality deletes) conflicts with any removed file of its partition.
                if (entries.FirstOrDefault(e => e.DataFile.ReferencedDataFile is { } target
                        ? removedPaths.Contains(target)
                        : removed.Any(f => f.SpecId == manifest.PartitionSpecId && f.Partition.SequenceEqual(e.DataFile.Partition))) is { } delete)
                    throw new ValidationException($"Concurrent snapshot {snapshot.SnapshotId} ({snapshot.Operation}) added delete file {delete.DataFile.FilePath} against data this {_operation} replaces; re-plan against the new table state");
            }
        }

        // Java's failMissingDeletePaths: compaction or another writer already removed a file this commit replaces. Re-applying
        // the filter would remove nothing and add the replacement rows a second time.
        if (removed.Count > 0)
        {
            var live = head is null ? [] : (await TableScan.PlanFilesAsync(_table, Expression.True, head, ct)).Select(t => t.File.FilePath).ToHashSet();
            if (removed.FirstOrDefault(f => !live.Contains(f.FilePath)) is { } gone)
                throw new ValidationException($"{gone.FilePath}, which this {_operation} replaces, was removed by a concurrent commit (compaction or another writer); re-plan against the new table state");
        }
    }

    /// <summary>
    /// Merge-on-read row delete: mark <paramref name="positions"/> (0-based row positions) of the given data
    /// files as deleted. v2 tables get one position delete file per data file; v3 tables get deletion
    /// vectors in a Puffin file, merged with any existing DV or v2 position deletes for the same data file
    /// (the spec allows at most one DV per data file).
    /// </summary>
    public static SnapshotProducer DeleteRows(Table table, IReadOnlyDictionary<string, IEnumerable<long>> positions)
    {
        var producer = new SnapshotProducer(table) { _operation = Operations.Delete, _rowDeletes = new(StringComparer.Ordinal) };
        foreach (var (file, pos) in positions)
        {
            var bm = new RoaringPositionBitmap();
            bm.AddRange(pos);
            producer._rowDeletes[file] = bm;
        }
        return producer;
    }

    /// <summary>
    /// The v2 → v3 "Puffin converter": fold every v2 position delete file (and any existing DV) into one
    /// deletion vector per data file and remove the position delete files. The table's rows do not
    /// change, so the snapshot operation is <c>replace</c>. Requires a v3 table.
    /// </summary>
    public static SnapshotProducer ConvertPositionDeletesToDeletionVectors(Table table) =>
        new(table) { _operation = Operations.Replace, _convertPositionDeletes = true };

    /// <summary>
    /// A general row-level change committed as one snapshot: any mix of added data files, removed data files
    /// (copy-on-write), position deletes / DVs and equality delete files. The operation is derived
    /// (<c>append</c>, <c>delete</c> or <c>overwrite</c>).
    /// </summary>
    public static SnapshotProducer RowDelta(Table table) => new(table) { _operation = null };

    /// <summary>
    /// Rewrite manifests (Java's <c>RewriteManifests</c>): regroup every live data file into new manifests of about
    /// <paramref name="targetSizeBytes"/>, clustered by partition so planning prunes well; no data changes
    /// (operation <c>replace</c>). Entries keep their sequence numbers, snapshot IDs and v3 <c>first_row_id</c>s.
    /// </summary>
    public static SnapshotProducer RewriteManifests(Table table, long targetSizeBytes = 8L * 1024 * 1024) =>
        new(table) { _operation = Operations.Replace, _rewriteManifestsTarget = targetSizeBytes };

    private long? _rewriteManifestsTarget;

    /// <summary>Compaction: replace data files with rewritten ones holding the same rows (operation <c>replace</c>).</summary>
    public static SnapshotProducer Rewrite(Table table) => new(table) { _operation = Operations.Replace };

    /// <summary>Remove these data files (copy-on-write); DVs that reference them are removed in the same commit.</summary>
    public SnapshotProducer RemoveDataFiles(IEnumerable<string> paths)
    {
        _removePaths.UnionWith(paths);
        var previous = _deleteWhere;
        _deleteWhere = f => _removePaths.Contains(f.FilePath) || (previous?.Invoke(f) ?? false);
        return this;
    }

    public SnapshotProducer DeletePositions(string dataFile, IEnumerable<long> positions)
    {
        _rowDeletes ??= new(StringComparer.Ordinal);
        if (!_rowDeletes.TryGetValue(dataFile, out var bm)) _rowDeletes[dataFile] = bm = new RoaringPositionBitmap();
        bm.AddRange(positions);
        return this;
    }

    private readonly List<DataFile> _removeDeleteFiles = [];

    /// <summary>
    /// Remove these delete files (a delete-file rewrite). Each must still be live when the commit is applied, else
    /// <see cref="ValidationException"/>: another writer rewrote or dropped it meanwhile.
    /// </summary>
    public SnapshotProducer RemoveDeleteFiles(IEnumerable<DataFile> files)
    {
        _removeDeleteFiles.AddRange(files);
        return this;
    }

    /// <summary>Add pre-written delete files (e.g. equality deletes).</summary>
    public SnapshotProducer AddDeleteFiles(IEnumerable<DataFile> files)
    {
        _addedDeleteFiles.AddRange(files);
        return this;
    }

    /// <summary>Override <c>commit.retry.num-retries</c>; 0 makes a conflict surface as <see cref="CommitFailedException"/> so the caller can re-plan.</summary>
    public SnapshotProducer WithRetries(int retries)
    {
        _retries = retries;
        return this;
    }

    private TimeProvider _clock = TimeProvider.System;

    /// <summary>
    /// The clock snapshot timestamps are taken from (default: the system clock). Whatever it says, a snapshot's
    /// <c>timestamp-ms</c> is strictly greater than its parent's on the same branch (Iceberg v4's rule; harmless on
    /// v1–v3): <c>max(now, parent + 1)</c>, so a clock that stands still or runs behind still yields an ordered history.
    /// </summary>
    public SnapshotProducer WithClock(TimeProvider clock)
    {
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        return this;
    }

    public SnapshotProducer AddFiles(IEnumerable<DataFile> files)
    {
        _added.AddRange(files);
        return this;
    }

    public SnapshotProducer Set(string summaryKey, string value)
    {
        _extraSummary[summaryKey] = value;
        return this;
    }

    private Func<TableMetadata, CancellationToken, Task>? _validateEachAttempt;

    /// <summary>A check run against the table state every attempt commits on (cherry-pick's ancestry and partition checks).</summary>
    internal SnapshotProducer ValidateEachAttempt(Func<TableMetadata, CancellationToken, Task> check)
    {
        _validateEachAttempt = check;
        return this;
    }

    public async Task<Snapshot> CommitAsync(CancellationToken ct = default)
    {
        var policy = CommitRetryPolicy.From(_table.Metadata);
        if (_retries is { } r) policy = policy with { NumRetries = r };
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                if (_validateEachAttempt is { } check) await check(_table.Metadata, ct);
                if (attempt > 0) await ValidateNoConflictingDataAsync(ct);
                var snapshot = await ApplyOrConflictAsync(ct);
                var requirements = new List<UpdateRequirement>
                {
                    new UpdateRequirement.AssertTableUuid(_table.Metadata.TableUuid),
                    BranchRequirement(_table.Metadata),
                };
                try
                {
                    await _table.CommitAsync(requirements, [new MetadataUpdate.AddSnapshot(snapshot), SetBranch(snapshot.SnapshotId)], ct);
                }
                catch (ArgumentException stale) when (_table.Metadata.FormatVersion > 1)
                {
                    if (snapshot.SequenceNumber > await LastSequenceNumberAsync(ct)) throw;
                    // The branch head is what we planned against, but another writer added a snapshot meanwhile (on another
                    // branch, or before a rollback returned the head to our parent): our sequence number is stale. A conflict
                    // to retry with the next sequence number, not an invalid commit (Java #15126).
                    throw new CommitFailedException($"Sequence number {snapshot.SequenceNumber} of snapshot {snapshot.SnapshotId} is stale: {stale.Message}", stale);
                }
                catch (CommitStateUnknownException unknown) when (unknown.ServerResponded)
                {
                    // The catalog finished the request with an error, so its outcome is now fixed: look for our
                    // snapshot (IDs are random 63-bit values). Absent means not applied, which is a plain conflict.
                    if (!await AppliedAsync(snapshot.SnapshotId, unknown, ct))
                        throw new CommitFailedException($"Commit of snapshot {snapshot.SnapshotId} was not applied ({unknown.Message})", unknown);
                }
                _attemptArtifacts.Clear();
                await CleanupAsync(ct);
                return snapshot;
            }
            catch (CommitFailedException) when (attempt < policy.NumRetries)
            {
                _staleArtifacts.AddRange(_attemptArtifacts);
                _attemptArtifacts.Clear();
                var wait = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * (1 << Math.Min(attempt, 20)));
                await Task.Delay(wait + Random.Shared.Next(wait / 10 + 1), ct);
                await _table.RefreshAsync(ct);
            }
        }
    }

    /// <summary>
    /// <see cref="ApplyAsync"/>, treating a missing base file as a lost race when the table has moved: expiry or compaction
    /// deleted files of the snapshot this attempt started from, so it must refresh (and validate) like any conflict.
    /// </summary>
    private async Task<Snapshot> ApplyOrConflictAsync(CancellationToken ct)
    {
        try
        {
            return await ApplyAsync(ct);
        }
        catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException)
        {
            var before = Head(_table.Metadata);
            await _table.RefreshAsync(ct);
            if (Head(_table.Metadata) == before) throw;
            throw new CommitFailedException($"Files of snapshot {before} were removed while this {_operation} was being prepared; {_branch} has moved to {Head(_table.Metadata)}", e);
        }
    }

    /// <summary>
    /// Write this snapshot's files and return the commit instead of applying it, for
    /// <see cref="ICatalog.CommitTransactionAsync"/> across tables. On a conflict, re-stage from refreshed tables;
    /// files of an abandoned stage are left for orphan-file cleanup.
    /// </summary>
    public async Task<TableCommit> StageAsync(CancellationToken ct = default)
    {
        var snapshot = await ApplyAsync(ct);
        _attemptArtifacts.Clear();
        return new TableCommit(_table.Identifier,
            [new UpdateRequirement.AssertTableUuid(_table.Metadata.TableUuid), BranchRequirement(_table.Metadata)],
            [new MetadataUpdate.AddSnapshot(snapshot), SetBranch(snapshot.SnapshotId)]);
    }

    /// <summary>Move the target branch to the new snapshot, keeping its retention settings (Java's <c>SnapshotRef.builderFrom</c>).</summary>
    private MetadataUpdate.SetSnapshotRef SetBranch(long snapshotId)
    {
        if (!_table.Metadata.Refs.TryGetValue(_branch, out var existing)) return new(_branch, SnapshotRef.Branch(snapshotId));
        if (existing.Type != SnapshotRefType.Branch) throw new ArgumentException($"{_branch} is a tag, not a branch");
        return new(_branch, existing with { SnapshotId = snapshotId });
    }

    private async Task<long> LastSequenceNumberAsync(CancellationToken ct)
    {
        await _table.RefreshAsync(ct);
        return _table.Metadata.LastSequenceNumber;
    }

    private async Task<bool> AppliedAsync(long snapshotId, CommitStateUnknownException unknown, CancellationToken ct)
    {
        try
        {
            await _table.RefreshAsync(ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            throw new CommitStateUnknownException($"{unknown.Message}; resolving it failed: {e.Message}", unknown);
        }
        return _table.Metadata.SnapshotById(snapshotId) is not null;
    }

    /// <summary>Java's snapshot ID scheme: a random positive long derived from a UUID.</summary>
    public static long NewSnapshotId()
    {
        var bytes = Guid.NewGuid().ToByteArray();
        return (BitConverter.ToInt64(bytes, 0) ^ BitConverter.ToInt64(bytes, 8)) & long.MaxValue;
    }

    private async Task<Snapshot> ApplyAsync(CancellationToken ct)
    {
        var m = _table.Metadata;
        if (m.FormatVersion < 2) throw new NotSupportedException("Writing to v1 tables is not supported; upgrade the table to v2");
        var parent = Head(m) is { } headId ? m.SnapshotById(headId) : null;
        var snapshotId = NewSnapshotId();
        var sequenceNumber = m.NextSequenceNumber;
        var metaDir = m.Property("write.metadata.path", $"{m.Location}/metadata").TrimEnd('/');
        var commitUuid = Guid.NewGuid();

        var manifests = new List<ManifestFile>();
        long deletedFiles = 0, deletedRecords = 0, deletedSize = 0;
        var parentManifests = parent is null ? [] : await TableScan.ManifestsAsync(_table, parent, ct);

        // 0. Row-level deletes / conversion: decide which delete files to add and which to remove.
        var deleteChange = _rowDeletes is not null || _convertPositionDeletes || _addedDeleteFiles.Count > 0 || _deleteWhere is not null || _removeDeleteFiles.Count > 0
            ? await PlanDeleteChangeAsync(parent, parentManifests, commitUuid, ct)
            : DeleteChange.None;

        // 1. Carry forward (or rewrite, when deleting) the parent's manifests.
        var toRegroup = new List<ManifestFile>();
        var removedThisAttempt = new List<DataFile>();
        if (parent is not null)
        {
            var rewriteIndex = 0;
            foreach (var manifest in parentManifests)
            {
                if (_rewriteManifestsTarget is not null && manifest.Content == ManifestContent.Data)
                {
                    toRegroup.Add(manifest);
                    continue;
                }
                if (manifest.Content == ManifestContent.Deletes)
                {
                    manifests.Add(await RewriteDeleteManifestAsync(manifest, deleteChange.Removed, snapshotId, sequenceNumber, $"{metaDir}/{commitUuid}-d{rewriteIndex++}.avro", ct));
                    continue;
                }
                if (_deleteWhere is null)
                {
                    manifests.Add(manifest);
                    continue;
                }
                var spec = m.SpecById(manifest.PartitionSpecId);
                var entries = ManifestCodec.ReadManifest(await _table.IO.ReadAllAsync(manifest.ManifestPath, ct), spec, m.CurrentSchema, manifest).Entries
                    .Where(e => e.Status != ManifestEntryStatus.Deleted)
                    .ToList();
                var matched = entries.Where(e => _deleteWhere(e.DataFile)).ToList();
                if (matched.Count == 0)
                {
                    manifests.Add(manifest);
                    continue;
                }
                removedThisAttempt.AddRange(matched.Select(e => e.DataFile with { SpecId = manifest.PartitionSpecId }));
                var rewritten = entries.Select(e => matched.Contains(e)
                    ? e with { Status = ManifestEntryStatus.Deleted, SnapshotId = snapshotId }
                    : e with { Status = ManifestEntryStatus.Existing }).ToList();
                deletedFiles += matched.Count;
                deletedRecords += matched.Sum(e => e.DataFile.RecordCount);
                deletedSize += matched.Sum(e => e.DataFile.FileSizeInBytes);
                var path = $"{metaDir}/{commitUuid}-m{rewriteIndex++}.avro";
                manifests.Add(await WriteManifestAsync(path, spec, ManifestContent.Data, rewritten, snapshotId, sequenceNumber, ct));
                _attemptArtifacts.Add(path);
            }
        }

        if (_deleteWhere is not null) _removedAtStart ??= removedThisAttempt;

        // 1a. Rewrite manifests: all live data entries, sorted by partition, cut into target-sized manifests.
        var regroupIndex = 0;
        foreach (var bySpec in toRegroup.GroupBy(mf => mf.PartitionSpecId))
        {
            var spec = m.SpecById(bySpec.Key);
            var entries = new List<ManifestEntry>();
            long bytes = 0;
            foreach (var mf in bySpec)
            {
                bytes += mf.ManifestLength;
                entries.AddRange(ManifestCodec.ReadManifest(await _table.IO.ReadAllAsync(mf.ManifestPath, ct), spec, m.CurrentSchema, mf).Entries
                    .Where(e => e.Status != ManifestEntryStatus.Deleted)
                    .Select(e => e with { Status = ManifestEntryStatus.Existing }));
            }
            var perEntry = entries.Count == 0 ? 1 : Math.Max(1, bytes / entries.Count);
            var chunk = (int)Math.Max(1, _rewriteManifestsTarget!.Value / perEntry);
            var sorted = entries.OrderBy(e => string.Join('\u001f', e.DataFile.Partition.Select(v => v is byte[] b ? Convert.ToHexString(b) : Convert.ToString(v, CultureInfo.InvariantCulture) ?? "\0")), StringComparer.Ordinal).ToList();
            for (var i = 0; i < sorted.Count; i += chunk)
            {
                var path = $"{metaDir}/{commitUuid}-m-rewritten{regroupIndex++}.avro";
                manifests.Add(await WriteManifestAsync(path, spec, ManifestContent.Data, sorted.GetRange(i, Math.Min(chunk, sorted.Count - i)), snapshotId, sequenceNumber, ct));
                _attemptArtifacts.Add(path);
            }
        }
        if (_rewriteManifestsTarget is not null) _extraSummary["manifests-replaced"] = toRegroup.Count.ToString(CultureInfo.InvariantCulture);

        // 1b. New delete files: one delete manifest per partition spec.
        var deleteManifestIndex = 0;
        foreach (var group in deleteChange.Added.GroupBy(f => f.SpecId))
        {
            var path = $"{metaDir}/{commitUuid}-m-deletes{deleteManifestIndex++}.avro";
            var entries = group.Select(f => new ManifestEntry(ManifestEntryStatus.Added, null, null, null, f)).ToList();
            manifests.Add(await WriteManifestAsync(path, m.SpecById(group.Key), ManifestContent.Deletes, entries, snapshotId, sequenceNumber, ct));
            _attemptArtifacts.Add(path);
        }

        // 2. New manifests for added files, one per partition spec the files were written with (the default spec's
        //    first), written once and reused across retries.
        if (_added.Count > 0)
        {
            if (_writtenManifests is null)
            {
                var written = new List<ManifestFile>();
                foreach (var group in _added.GroupBy(f => f.SpecId).OrderBy(g => g.Key == m.DefaultSpecId ? 0 : 1).ThenBy(g => g.Key))
                {
                    var spec = m.PartitionSpecs.FirstOrDefault(p => p.SpecId == group.Key)
                        ?? throw new ValidationException($"Cannot add {group.First().FilePath}: partition spec {group.Key} is not a spec of the table");
                    var path = group.Key == m.DefaultSpecId ? $"{metaDir}/{commitUuid}-m-added.avro" : $"{metaDir}/{commitUuid}-m-added-spec{group.Key}.avro";
                    // Null snapshot ID and sequence numbers: inherited from the manifest list, so this file is valid for any attempt.
                    var entries = group.Select(f => new ManifestEntry(ManifestEntryStatus.Added, null, null, null, f)).ToList();
                    written.Add(await WriteManifestAsync(path, spec, ManifestContent.Data, entries, snapshotId, sequenceNumber, ct));
                    _writtenManifestPaths.Add(path);
                }
                _writtenManifests = written;
            }
            manifests.InsertRange(0, _writtenManifests.Select(w => w with { SequenceNumber = sequenceNumber, MinSequenceNumber = sequenceNumber, AddedSnapshotId = snapshotId }));
        }

        // 2b. Merge-append: compact small data manifests carried forward unchanged (plus this commit's new one).
        manifests = await MergeManifestsAsync(manifests, parentManifests, snapshotId, sequenceNumber, metaDir, commitUuid, ct);

        // 3. v3 row lineage: assign first_row_id to data manifests that lack one.
        long? firstRowId = null, addedRows = null;
        if (m.FormatVersion >= 3)
        {
            var next = m.NextRowId ?? 0;
            firstRowId = next;
            for (var i = 0; i < manifests.Count; i++)
            {
                if (manifests[i].Content != ManifestContent.Data || manifests[i].FirstRowId is not null) continue;
                manifests[i] = manifests[i] with { FirstRowId = next };
                next += manifests[i].ExistingRowsCount + manifests[i].AddedRowsCount;
            }
            addedRows = next - firstRowId;
        }

        var listPath = $"{metaDir}/snap-{snapshotId}-1-{commitUuid}.avro";
        await _table.IO.WriteAsync(listPath, ManifestCodec.WriteManifestList(m.FormatVersion, snapshotId, parent?.SnapshotId, sequenceNumber, manifests, firstRowId), overwrite: false, ct);
        _attemptArtifacts.Add(listPath);

        var deletesSomething = deletedFiles > 0 || deleteChange.Added.Count > 0;
        var operation = _operation switch
        {
            null when !deletesSomething => Operations.Append,
            null when _added.Count == 0 => Operations.Delete,
            null => Operations.Overwrite,
            Operations.Overwrite when deletedFiles == 0 => Operations.Append,
            _ => _operation,
        };
        return new Snapshot
        {
            SnapshotId = snapshotId,
            ParentSnapshotId = parent?.SnapshotId,
            SequenceNumber = sequenceNumber,
            TimestampMs = Math.Max(_clock.GetUtcNow().ToUnixTimeMilliseconds(), parent is null ? long.MinValue : parent.TimestampMs + 1),
            ManifestList = listPath,
            SchemaId = m.CurrentSchemaId,
            Summary = Summary(operation, parent, deletedFiles, deletedRecords, deletedSize, deleteChange),
            FirstRowId = firstRowId,
            AddedRows = addedRows,
        };
    }

    /// <summary>Delete files to add and (path, content offset) keys of live delete files to remove.</summary>
    private sealed record DeleteChange(IReadOnlyList<DataFile> Added, IReadOnlySet<(string Path, long? Offset)> Removed, IReadOnlyList<DataFile> RemovedFiles)
    {
        public static readonly DeleteChange None = new([], new HashSet<(string, long?)>(), []);
    }

    private async Task<DeleteChange> PlanDeleteChangeAsync(Snapshot? parent, IReadOnlyList<ManifestFile> parentManifests, Guid commitUuid, CancellationToken ct)
    {
        var m = _table.Metadata;
        if (_convertPositionDeletes && m.FormatVersion < 3) throw new NotSupportedException("Converting position deletes to deletion vectors requires a v3 table");
        if (parent is null)
        {
            if (_rowDeletes is { Count: > 0 }) throw new ValidationException("Cannot delete rows from a table with no snapshot");
            return _addedDeleteFiles.Count > 0 ? new DeleteChange(_addedDeleteFiles, new HashSet<(string, long?)>(), []) : DeleteChange.None;
        }

        var index = await DeleteIndex.LoadAsync(_table, parentManifests, ct);
        var live = (await TableScan.PlanFilesAsync(_table, parent.SnapshotId, null, ct)).ToDictionary(t => t.File.FilePath, StringComparer.Ordinal);
        var targets = new Dictionary<string, RoaringPositionBitmap>(StringComparer.Ordinal);
        foreach (var (path, bm) in _rowDeletes ?? [])
        {
            // Re-validated on every attempt: a concurrent commit may have removed the data file.
            if (!live.ContainsKey(path)) throw new ValidationException($"Cannot delete rows from {path}: it is not a live data file in snapshot {parent.SnapshotId}");
            var copy = new RoaringPositionBitmap();
            copy.Or(bm);
            targets[path] = copy;
        }

        var removed = new HashSet<(string, long?)>();
        var removedFiles = new List<DataFile>();
        void Remove(DataFile f)
        {
            if (removed.Add((f.FilePath, f.ContentOffset))) removedFiles.Add(f);
        }

        foreach (var f in _removeDeleteFiles)
        {
            var entry = index.All.Concat(index.EqualityDeletes).FirstOrDefault(e => e.File.FilePath == f.FilePath && e.File.ContentOffset == f.ContentOffset)
                ?? throw new ValidationException($"Delete file {f.FilePath} is no longer live in snapshot {parent.SnapshotId}: a concurrent commit rewrote or removed it");
            Remove(entry.File);
        }

        // Data files removed in this commit take their deletion vectors with them (no dangling DVs).
        if (_deleteWhere is not null)
        {
            foreach (var (path, task) in live)
            {
                if (!_deleteWhere(task.File)) continue;
                if (targets.ContainsKey(path)) throw new ValidationException($"{path} cannot be both removed and position-deleted in one commit");
                if (index.DeletionVectorFor(path) is { } dv) Remove(dv.File);
            }
        }

        if (m.FormatVersion >= 3)
        {
            if (_convertPositionDeletes)
            {
                foreach (var pd in index.All.Where(e => !e.IsDeletionVector))
                {
                    foreach (var (dataPath, positions) in await index.PositionDeletesAsync(pd.File.FilePath))
                    {
                        // Dangling deletes (data file already removed) or deletes older than the data file are dropped.
                        if (!live.TryGetValue(dataPath, out var task) || pd.DataSequenceNumber < task.DataSequenceNumber) continue;
                        if (!targets.TryGetValue(dataPath, out var bm)) targets[dataPath] = bm = new RoaringPositionBitmap();
                        bm.Or(positions);
                    }
                    Remove(pd.File);
                }
            }

            foreach (var (dataPath, bm) in targets)
            {
                var task = live[dataPath];
                // At most one DV per data file: fold the existing DV (or applicable v2 position deletes) in.
                if (index.DeletionVectorFor(dataPath) is { } existing)
                {
                    bm.Or(await index.ReadDeletionVectorAsync(existing.File, ct));
                    Remove(existing.File);
                }
                else if (await index.DeletedPositionsAsync(task.File, task.DataSequenceNumber, ct) is { } older)
                {
                    bm.Or(older);
                }
            }
            if (targets.Count == 0) return new DeleteChange(_addedDeleteFiles, removed, removedFiles);

            var ordered = targets.OrderBy(t => t.Key, StringComparer.Ordinal).ToList();
            var written = PuffinFile.Write(ordered.Select(t => DeletionVector.ToBlob(t.Key, t.Value)));
            var location = $"{DataDir(m)}/{commitUuid:N}-deletes.puffin";
            await _table.IO.WriteAsync(location, written.Bytes, overwrite: false, ct);
            _attemptArtifacts.Add(location);
            var added = ordered.Select((t, i) => new DataFile
            {
                Content = FileContent.PositionDeletes,
                FilePath = location,
                FileFormat = "PUFFIN",
                SpecId = live[t.Key].File.SpecId,
                Partition = live[t.Key].File.Partition,
                RecordCount = t.Value.Cardinality,
                FileSizeInBytes = written.Bytes.Length,
                ReferencedDataFile = t.Key,
                ContentOffset = written.Blobs[i].Offset,
                ContentSizeInBytes = written.Blobs[i].Length,
            }).ToList();
            return new DeleteChange([.. added, .. _addedDeleteFiles], removed, removedFiles);
        }

        // v2: one position delete file per data file.
        var v2Added = new List<DataFile>();
        foreach (var (dataPath, bm) in targets.OrderBy(t => t.Key, StringComparer.Ordinal))
        {
            var data = live[dataPath].File;
            var location = $"{DataDir(m)}/{commitUuid:N}-{v2Added.Count:D5}-deletes.parquet";
            v2Added.Add(await PositionDeleteFiles.WriteAsync(_table.IO, location, data.SpecId, data.Partition, bm.Positions().Select(p => (dataPath, p)), ct));
            _attemptArtifacts.Add(location);
        }
        return new DeleteChange([.. v2Added, .. _addedDeleteFiles], removed, removedFiles);
    }

    private static string DataDir(TableMetadata m) => m.Property("write.data.path", $"{m.Location}/data").TrimEnd('/');

    public const string ManifestMergeEnabled = "commit.manifest-merge.enabled";
    public const string ManifestMinCountToMerge = "commit.manifest.min-count-to-merge";
    public const string ManifestTargetSizeBytes = "commit.manifest.target-size-bytes";

    /// <summary>
    /// Java's merge-append: when a partition spec has at least <c>commit.manifest.min-count-to-merge</c> (100) data
    /// manifests, bin-pack them (newest first) into manifests of up to <c>commit.manifest.target-size-bytes</c> (8 MB).
    /// Only manifests carried forward unchanged from the parent, and this commit's own added manifest, are merged.
    /// Merged entries keep their data and file sequence numbers, snapshot IDs and v3 <c>first_row_id</c>s explicitly;
    /// entries deleted by earlier snapshots are dropped, as in Java.
    /// </summary>
    private async Task<List<ManifestFile>> MergeManifestsAsync(List<ManifestFile> manifests, IReadOnlyList<ManifestFile> parentManifests,
        long snapshotId, long sequenceNumber, string metaDir, Guid commitUuid, CancellationToken ct)
    {
        var m = _table.Metadata;
        if (m.Property(ManifestMergeEnabled, "true") != "true") return manifests;
        var minCount = int.Parse(m.Property(ManifestMinCountToMerge, "100"), CultureInfo.InvariantCulture);
        var target = long.Parse(m.Property(ManifestTargetSizeBytes, "8388608"), CultureInfo.InvariantCulture);
        var carried = parentManifests.Select(p => p.ManifestPath).ToHashSet(StringComparer.Ordinal);
        bool Mergeable(ManifestFile mf) => mf.Content == ManifestContent.Data && (carried.Contains(mf.ManifestPath) || _writtenManifestPaths.Contains(mf.ManifestPath));

        var result = new List<ManifestFile>();
        var mergeIndex = 0;
        foreach (var group in manifests.Where(Mergeable).GroupBy(mf => mf.PartitionSpecId))
        {
            var members = group.ToList(); // manifests are newest first
            if (members.Count < Math.Max(2, minCount)) { result.AddRange(members); continue; }
            var bins = new List<List<ManifestFile>>();
            foreach (var mf in members)
            {
                if (bins.Count == 0 || bins[^1].Sum(x => x.ManifestLength) + mf.ManifestLength > target) bins.Add([]);
                bins[^1].Add(mf);
            }
            var spec = m.SpecById(group.Key);
            foreach (var bin in bins)
            {
                if (bin.Count == 1) { result.Add(bin[0]); continue; }
                var entries = new List<ManifestEntry>();
                foreach (var mf in bin)
                {
                    var read = ManifestCodec.ReadManifest(await _table.IO.ReadAllAsync(mf.ManifestPath, ct), spec, m.CurrentSchema, mf).Entries;
                    foreach (var e in read)
                    {
                        if (e.Status == ManifestEntryStatus.Deleted) continue;
                        entries.Add(_writtenManifestPaths.Contains(mf.ManifestPath)
                            ? e with { Status = ManifestEntryStatus.Added, SnapshotId = snapshotId, SequenceNumber = null, FileSequenceNumber = null }
                            : e with { Status = ManifestEntryStatus.Existing });
                    }
                }
                var path = $"{metaDir}/{commitUuid}-m-merged{mergeIndex++}.avro";
                result.Add(await WriteManifestAsync(path, spec, ManifestContent.Data, entries, snapshotId, sequenceNumber, ct));
                _attemptArtifacts.Add(path);
            }
        }
        // Keep the original order otherwise: new/merged manifests first, then the rest as they were.
        var merged = result.ToHashSet();
        return [.. result, .. manifests.Where(mf => !Mergeable(mf) && !merged.Contains(mf))];
    }

    private async Task<ManifestFile> RewriteDeleteManifestAsync(ManifestFile manifest, IReadOnlySet<(string Path, long? Offset)> removed, long snapshotId, long sequenceNumber, string path, CancellationToken ct)
    {
        if (removed.Count == 0) return manifest;
        var m = _table.Metadata;
        var spec = m.SpecById(manifest.PartitionSpecId);
        var entries = ManifestCodec.ReadManifest(await _table.IO.ReadAllAsync(manifest.ManifestPath, ct), spec, m.CurrentSchema, manifest).Entries
            .Where(e => e.Status != ManifestEntryStatus.Deleted).ToList();
        if (!entries.Any(e => removed.Contains((e.DataFile.FilePath, e.DataFile.ContentOffset)))) return manifest;
        var rewritten = entries.Select(e => removed.Contains((e.DataFile.FilePath, e.DataFile.ContentOffset))
            ? e with { Status = ManifestEntryStatus.Deleted, SnapshotId = snapshotId }
            : e with { Status = ManifestEntryStatus.Existing }).ToList();
        _attemptArtifacts.Add(path);
        return await WriteManifestAsync(path, spec, ManifestContent.Deletes, rewritten, snapshotId, sequenceNumber, ct);
    }

    private async Task<ManifestFile> WriteManifestAsync(string path, PartitionSpec spec, ManifestContent content, IReadOnlyList<ManifestEntry> entries, long snapshotId, long sequenceNumber, CancellationToken ct)
    {
        var m = _table.Metadata;
        var bytes = ManifestCodec.WriteManifest(m.FormatVersion, m.CurrentSchema, spec, content, entries);
        await _table.IO.WriteAsync(path, bytes, overwrite: false, ct);
        var partitionType = spec.PartitionType(m.CurrentSchema);
        var live = entries.Where(e => e.Status != ManifestEntryStatus.Deleted).ToList();
        return new ManifestFile
        {
            ManifestPath = path,
            ManifestLength = bytes.Length,
            PartitionSpecId = spec.SpecId,
            Content = content,
            SequenceNumber = sequenceNumber,
            MinSequenceNumber = entries.Select(e => e.SequenceNumber ?? sequenceNumber).DefaultIfEmpty(sequenceNumber).Min(),
            AddedSnapshotId = snapshotId,
            AddedFilesCount = entries.Count(e => e.Status == ManifestEntryStatus.Added),
            ExistingFilesCount = entries.Count(e => e.Status == ManifestEntryStatus.Existing),
            DeletedFilesCount = entries.Count(e => e.Status == ManifestEntryStatus.Deleted),
            AddedRowsCount = entries.Where(e => e.Status == ManifestEntryStatus.Added).Sum(e => e.DataFile.RecordCount),
            ExistingRowsCount = entries.Where(e => e.Status == ManifestEntryStatus.Existing).Sum(e => e.DataFile.RecordCount),
            DeletedRowsCount = entries.Where(e => e.Status == ManifestEntryStatus.Deleted).Sum(e => e.DataFile.RecordCount),
            Partitions = PartitionSummaries(partitionType, live.Select(e => e.DataFile)),
        };
    }

    private static List<FieldSummary> PartitionSummaries(StructType partitionType, IEnumerable<DataFile> files)
    {
        var list = files.ToList();
        return partitionType.Fields.Select((f, i) =>
        {
            var values = list.Select(d => d.Partition.Count > i ? d.Partition[i] : null).ToList();
            var present = values.Where(v => v is not null).ToList();
            bool? containsNan = f.Type.TypeId is TypeId.Float or TypeId.Double ? present.Any(v => v is double d && double.IsNaN(d)) : null;
            if (present.Count == 0) return new FieldSummary(values.Count > 0, containsNan, null, null);
            var comparer = Comparer<object?>.Create((a, b) => a is string sa ? string.CompareOrdinal(sa, (string)b!) : ((IComparable)a!).CompareTo(b));
            return new FieldSummary(
                present.Count < values.Count,
                containsNan,
                Conversions.ToBytes(f.Type, present.Min(comparer)!),
                Conversions.ToBytes(f.Type, present.Max(comparer)!));
        }).ToList();
    }

    private Dictionary<string, string> Summary(string operation, Snapshot? parent, long deletedFiles, long deletedRecords, long deletedSize, DeleteChange deletes)
    {
        static long Prev(Snapshot? p, string key) => p?.Summary.TryGetValue(key, out var v) == true && long.TryParse(v, CultureInfo.InvariantCulture, out var n) ? n : 0;
        string S(long v) => v.ToString(CultureInfo.InvariantCulture);
        var addedRecords = _added.Sum(f => f.RecordCount);
        var addedSize = _added.Sum(f => f.FileSizeInBytes);
        var summary = new Dictionary<string, string>
        {
            ["operation"] = operation,
            ["added-data-files"] = S(_added.Count),
            ["added-records"] = S(addedRecords),
            ["added-files-size"] = S(addedSize),
            ["changed-partition-count"] = S(_added.Select(f => string.Join('|', f.Partition)).Distinct().Count()),
            ["total-data-files"] = S(Prev(parent, "total-data-files") + _added.Count - deletedFiles),
            ["total-records"] = S(Prev(parent, "total-records") + addedRecords - deletedRecords),
            ["total-files-size"] = S(Prev(parent, "total-files-size") + addedSize - deletedSize),
            ["total-delete-files"] = S(Prev(parent, "total-delete-files") + deletes.Added.Count - deletes.RemovedFiles.Count),
            ["total-position-deletes"] = S(Prev(parent, "total-position-deletes") + PositionRecords(deletes.Added) - PositionRecords(deletes.RemovedFiles)),
            ["total-equality-deletes"] = S(Prev(parent, "total-equality-deletes") + EqualityRecords(deletes.Added) - EqualityRecords(deletes.RemovedFiles)),
            ["engine-name"] = "iceberg-net",
        };
        if (deletedFiles > 0)
        {
            summary["deleted-data-files"] = S(deletedFiles);
            summary["deleted-records"] = S(deletedRecords);
            summary["removed-files-size"] = S(deletedSize);
        }
        if (deletes.Added.Count > 0)
        {
            summary["added-delete-files"] = S(deletes.Added.Count);
            if (PositionRecords(deletes.Added) > 0) summary["added-position-deletes"] = S(PositionRecords(deletes.Added));
            if (EqualityRecords(deletes.Added) > 0) summary["added-equality-deletes"] = S(EqualityRecords(deletes.Added));
            var dvs = deletes.Added.Count(f => f.FileFormat == "PUFFIN");
            if (dvs > 0) summary["added-dvs"] = S(dvs);
        }
        if (deletes.RemovedFiles.Count > 0)
        {
            summary["removed-delete-files"] = S(deletes.RemovedFiles.Count);
            if (PositionRecords(deletes.RemovedFiles) > 0) summary["removed-position-deletes"] = S(PositionRecords(deletes.RemovedFiles));
            if (EqualityRecords(deletes.RemovedFiles) > 0) summary["removed-equality-deletes"] = S(EqualityRecords(deletes.RemovedFiles));
            var dvs = deletes.RemovedFiles.Count(f => f.FileFormat == "PUFFIN");
            if (dvs > 0) summary["removed-dvs"] = S(dvs);
        }
        foreach (var (k, v) in _extraSummary) summary[k] = v;
        return summary;
    }

    private static long PositionRecords(IEnumerable<DataFile> files) => files.Where(f => f.Content == FileContent.PositionDeletes).Sum(f => f.RecordCount);
    private static long EqualityRecords(IEnumerable<DataFile> files) => files.Where(f => f.Content == FileContent.EqualityDeletes).Sum(f => f.RecordCount);

    /// <summary>After a successful commit, delete manifests/lists from failed attempts that the winning snapshot does not reference.</summary>
    private async Task CleanupAsync(CancellationToken ct)
    {
        var current = _table.Metadata.SnapshotById(Head(_table.Metadata)!.Value)!;
        var live = (await TableScan.ManifestsAsync(_table, current, ct)).Select(mf => mf.ManifestPath).Append(current.ManifestList!).ToHashSet();
        foreach (var path in _staleArtifacts.Where(p => !live.Contains(p)))
        {
            try { await _table.IO.DeleteAsync(path, ct); }
            catch (Exception e) when (e is not OperationCanceledException) { /* orphan-file cleanup will catch it */ }
        }
        _staleArtifacts.Clear();
    }
}

/// <summary>A commit's preconditions no longer hold on the current table (not retryable, unlike <see cref="CommitFailedException"/>).</summary>
public sealed class ValidationException(string message) : Exception(message);
