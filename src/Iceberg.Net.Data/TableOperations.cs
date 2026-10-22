using System.Globalization;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>Live data file in a snapshot, with the sequence number used for delete applicability.</summary>
public sealed record ScanTask(DataFile File, long DataSequenceNumber, string ManifestPath);

public static class TableScan
{
    public static async Task<IReadOnlyList<ManifestFile>> ManifestsAsync(Table table, Snapshot snapshot, CancellationToken ct = default)
    {
        if (snapshot.ManifestList is null)
            throw new NotSupportedException("v1 snapshots with an embedded 'manifests' list are not supported yet");
        return ManifestCodec.ReadManifestList(await table.IO.ReadAllAsync(snapshot.ManifestList, ct)).Manifests;
    }

    /// <summary>
    /// Plan the live data files of a snapshot (default: current). Phase-0 scope: tables with delete files
    /// (merge-on-read) are refused rather than silently returning deleted rows.
    /// </summary>
    public static async Task<IReadOnlyList<ScanTask>> PlanFilesAsync(Table table, long? snapshotId = null, Func<DataFile, bool>? filter = null, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = snapshotId is { } id ? m.SnapshotById(id) ?? throw new ArgumentException($"Unknown snapshot {id}") : m.CurrentSnapshot;
        if (snapshot is null) return [];

        var tasks = new List<ScanTask>();
        foreach (var manifest in await ManifestsAsync(table, snapshot, ct))
        {
            if (manifest.Content == ManifestContent.Deletes)
            {
                if (manifest.AddedFilesCount + manifest.ExistingFilesCount > 0)
                    throw new NotSupportedException("Merge-on-read delete files are not supported by the phase-0 reader");
                continue;
            }
            var spec = m.SpecById(manifest.PartitionSpecId);
            var contents = ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(manifest.ManifestPath, ct), spec, m.CurrentSchema, manifest);
            foreach (var e in contents.Entries)
            {
                if (e.Status == ManifestEntryStatus.Deleted) continue;
                if (filter is not null && !filter(e.DataFile)) continue;
                tasks.Add(new ScanTask(e.DataFile, e.SequenceNumber ?? 0, manifest.ManifestPath));
            }
        }
        return tasks;
    }

    /// <summary>Read all rows of a snapshot, projected onto the current schema (by field ID).</summary>
    public static async Task<List<object?[]>> ReadAllAsync(Table table, long? snapshotId = null, CancellationToken ct = default)
    {
        var rows = new List<object?[]>();
        foreach (var task in await PlanFilesAsync(table, snapshotId, null, ct))
        {
            if (task.File.FileFormat != "PARQUET") throw new NotSupportedException($"{task.File.FileFormat} data files are not supported");
            rows.AddRange(await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, table.Schema, ct));
        }
        return rows;
    }
}

/// <summary>Writes rows into Parquet files, one per partition, under <c>{location}/data/</c>.</summary>
public static class TableWriter
{
    public static async Task<IReadOnlyList<DataFile>> WriteAsync(Table table, IReadOnlyList<object?[]> rows, CancellationToken ct = default)
    {
        var schema = table.Schema;
        var spec = table.Spec;
        if (spec.Fields.Any(f => f.Transform is Transforms.UnknownTransform))
            throw new NotSupportedException("Cannot write with a partition spec containing unknown transforms");

        var positions = schema.Columns.Select((c, i) => (c.Id, i)).ToDictionary(x => x.Id, x => x.i);
        var groups = rows.GroupBy(
            r => spec.Partition(schema, id => positions.TryGetValue(id, out var p) ? r[p] : throw new NotSupportedException("Partitioning by nested columns is not supported yet")),
            PartitionComparer.Instance);

        var dataDir = table.Metadata.Property("write.data.path", $"{table.Location}/data").TrimEnd('/');
        var files = new List<DataFile>();
        foreach (var g in groups)
        {
            var partitionPath = spec.IsUnpartitioned ? "" : spec.PartitionPath(schema, g.Key) + "/";
            var location = $"{dataDir}/{partitionPath}{Guid.NewGuid():N}.parquet";
            files.Add(await ParquetFiles.WriteAsync(table.IO, location, schema, spec.SpecId, g.Key, g.ToList(), ct));
        }
        return files;
    }

    private sealed class PartitionComparer : IEqualityComparer<object?[]>
    {
        public static readonly PartitionComparer Instance = new();
        public bool Equals(object?[]? a, object?[]? b) => a!.Length == b!.Length && a.Zip(b).All(p => Equals(p.First, p.Second));
        public int GetHashCode(object?[] o) => o.Aggregate(17, (h, v) => HashCode.Combine(h, v));
    }
}

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
    private string _operation = Operations.Append;
    private readonly Dictionary<string, string> _extraSummary = [];
    private string? _writtenManifest;
    private ManifestFile? _writtenManifestFile;
    private readonly List<string> _staleArtifacts = [];

    private SnapshotProducer(Table table) => _table = table;

    public static SnapshotProducer Append(Table table) => new(table);

    /// <summary>Copy-on-write overwrite: whole files matching <paramref name="deleteWhere"/> are removed, then new files added.</summary>
    public static SnapshotProducer Overwrite(Table table, Func<DataFile, bool> deleteWhere) =>
        new(table) { _deleteWhere = deleteWhere, _operation = Operations.Overwrite };

    /// <summary>Remove whole data files without adding any (a metadata-only delete).</summary>
    public static SnapshotProducer DeleteFiles(Table table, Func<DataFile, bool> deleteWhere) =>
        new(table) { _deleteWhere = deleteWhere, _operation = Operations.Delete };

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

    public async Task<Snapshot> CommitAsync(CancellationToken ct = default)
    {
        var policy = CommitRetryPolicy.From(_table.Metadata);
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var snapshot = await ApplyAsync(ct);
                var requirements = new List<UpdateRequirement>
                {
                    new UpdateRequirement.AssertTableUuid(_table.Metadata.TableUuid),
                    new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, _table.Metadata.CurrentSnapshotId),
                };
                await _table.CommitAsync(requirements,
                [
                    new MetadataUpdate.AddSnapshot(snapshot),
                    new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, SnapshotRef.Branch(snapshot.SnapshotId)),
                ], ct);
                await CleanupAsync(ct);
                return snapshot;
            }
            catch (CommitFailedException) when (attempt < policy.NumRetries)
            {
                var wait = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * (1 << Math.Min(attempt, 20)));
                await Task.Delay(wait + Random.Shared.Next(wait / 10 + 1), ct);
                await _table.RefreshAsync(ct);
            }
        }
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
        var parent = m.CurrentSnapshot;
        var snapshotId = NewSnapshotId();
        var sequenceNumber = m.NextSequenceNumber;
        var metaDir = m.Property("write.metadata.path", $"{m.Location}/metadata").TrimEnd('/');
        var commitUuid = Guid.NewGuid();

        var manifests = new List<ManifestFile>();
        long deletedFiles = 0, deletedRecords = 0, deletedSize = 0;

        // 1. Carry forward (or rewrite, when deleting) the parent's manifests.
        if (parent is not null)
        {
            var rewriteIndex = 0;
            foreach (var manifest in await TableScan.ManifestsAsync(_table, parent, ct))
            {
                if (_deleteWhere is null || manifest.Content == ManifestContent.Deletes)
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
                var rewritten = entries.Select(e => matched.Contains(e)
                    ? e with { Status = ManifestEntryStatus.Deleted, SnapshotId = snapshotId }
                    : e with { Status = ManifestEntryStatus.Existing }).ToList();
                deletedFiles += matched.Count;
                deletedRecords += matched.Sum(e => e.DataFile.RecordCount);
                deletedSize += matched.Sum(e => e.DataFile.FileSizeInBytes);
                var path = $"{metaDir}/{commitUuid}-m{rewriteIndex++}.avro";
                manifests.Add(await WriteManifestAsync(path, spec, rewritten, snapshotId, sequenceNumber, ct));
                _staleArtifacts.Add(path);
            }
        }

        // 2. One new manifest for added files, written once and reused across retries.
        if (_added.Count > 0)
        {
            if (_writtenManifest is null)
            {
                _writtenManifest = $"{metaDir}/{commitUuid}-m-added.avro";
                // Null snapshot ID and sequence numbers: inherited from the manifest list, so this file is valid for any attempt.
                var entries = _added.Select(f => new ManifestEntry(ManifestEntryStatus.Added, null, null, null, f)).ToList();
                _writtenManifestFile = await WriteManifestAsync(_writtenManifest, m.DefaultSpec, entries, snapshotId, sequenceNumber, ct);
            }
            manifests.Insert(0, _writtenManifestFile! with { SequenceNumber = sequenceNumber, MinSequenceNumber = sequenceNumber, AddedSnapshotId = snapshotId });
        }

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
        _staleArtifacts.Add(listPath);

        var operation = _operation == Operations.Overwrite && deletedFiles == 0 ? Operations.Append : _operation;
        return new Snapshot
        {
            SnapshotId = snapshotId,
            ParentSnapshotId = parent?.SnapshotId,
            SequenceNumber = sequenceNumber,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ManifestList = listPath,
            SchemaId = m.CurrentSchemaId,
            Summary = Summary(operation, parent, deletedFiles, deletedRecords, deletedSize),
            FirstRowId = firstRowId,
            AddedRows = addedRows,
        };
    }

    private async Task<ManifestFile> WriteManifestAsync(string path, PartitionSpec spec, IReadOnlyList<ManifestEntry> entries, long snapshotId, long sequenceNumber, CancellationToken ct)
    {
        var m = _table.Metadata;
        var bytes = ManifestCodec.WriteManifest(m.FormatVersion, m.CurrentSchema, spec, ManifestContent.Data, entries);
        await _table.IO.WriteAsync(path, bytes, overwrite: false, ct);
        var partitionType = spec.PartitionType(m.CurrentSchema);
        var live = entries.Where(e => e.Status != ManifestEntryStatus.Deleted).ToList();
        return new ManifestFile
        {
            ManifestPath = path,
            ManifestLength = bytes.Length,
            PartitionSpecId = spec.SpecId,
            Content = ManifestContent.Data,
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

    private Dictionary<string, string> Summary(string operation, Snapshot? parent, long deletedFiles, long deletedRecords, long deletedSize)
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
            ["total-delete-files"] = S(Prev(parent, "total-delete-files")),
            ["total-position-deletes"] = S(Prev(parent, "total-position-deletes")),
            ["total-equality-deletes"] = S(Prev(parent, "total-equality-deletes")),
            ["engine-name"] = "iceberg-net",
        };
        if (deletedFiles > 0)
        {
            summary["deleted-data-files"] = S(deletedFiles);
            summary["deleted-records"] = S(deletedRecords);
            summary["removed-files-size"] = S(deletedSize);
        }
        foreach (var (k, v) in _extraSummary) summary[k] = v;
        return summary;
    }

    /// <summary>After a successful commit, delete manifests/lists from failed attempts that the winning snapshot does not reference.</summary>
    private async Task CleanupAsync(CancellationToken ct)
    {
        var current = _table.Metadata.CurrentSnapshot!;
        var live = (await TableScan.ManifestsAsync(_table, current, ct)).Select(mf => mf.ManifestPath).Append(current.ManifestList!).ToHashSet();
        foreach (var path in _staleArtifacts.Where(p => !live.Contains(p)))
        {
            try { await _table.IO.DeleteAsync(path, ct); }
            catch (Exception e) when (e is not OperationCanceledException) { /* orphan-file cleanup will catch it */ }
        }
        _staleArtifacts.Clear();
    }
}

public static class Maintenance
{
    /// <summary>
    /// Expire snapshots older than <paramref name="olderThanMs"/>, keeping at least
    /// <paramref name="retainLast"/> ancestors of main and every snapshot a ref points at.
    /// Metadata-only: unreachable files are left for an orphan-file sweep (deleting them safely needs
    /// the reachability diff Java's ExpireSnapshots computes; tracked in docs/roadmap.md).
    /// </summary>
    public static async Task<IReadOnlyList<long>> ExpireSnapshotsAsync(Table table, long olderThanMs, int retainLast = 1, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var keep = m.Ancestors(m.CurrentSnapshotId).Take(Math.Max(1, retainLast)).Select(s => s.SnapshotId).ToHashSet();
        keep.UnionWith(m.Refs.Values.Select(r => r.SnapshotId));
        var expire = m.Snapshots.Where(s => s.TimestampMs < olderThanMs && !keep.Contains(s.SnapshotId)).Select(s => s.SnapshotId).ToList();
        if (expire.Count == 0) return [];
        await table.CommitAsync(
            [new UpdateRequirement.AssertTableUuid(m.TableUuid), new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, m.CurrentSnapshotId)],
            [new MetadataUpdate.RemoveSnapshots(expire)], ct);
        return expire;
    }
}
