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

/// <summary>
/// v3 row lineage metadata columns (spec "Row Lineage"). A row's <c>_row_id</c> is the value stored in its data file
/// when present (rows carried through a rewrite keep their identity), otherwise the file's <c>first_row_id</c> plus
/// the row's position. <c>_last_updated_sequence_number</c> is likewise stored, or inherited from the file.
/// </summary>
public static class RowLineage
{
    public const int RowIdFieldId = 2147483540;
    public const int LastUpdatedSequenceNumberFieldId = 2147483539;

    public static readonly NestedField RowId = NestedField.Optional(RowIdFieldId, "_row_id", PrimitiveType.Long);
    public static readonly NestedField LastUpdatedSequenceNumber = NestedField.Optional(LastUpdatedSequenceNumberFieldId, "_last_updated_sequence_number", PrimitiveType.Long);

    public static Schema WithLineage(Schema schema) => new(schema.SchemaId, [.. schema.Columns, RowId, LastUpdatedSequenceNumber], schema.IdentifierFieldIds);

    public static bool IsReserved(int fieldId) => fieldId >= 2147483000;
}

public sealed record LineageRow(object?[] Row, long? RowId, long? LastUpdatedSequenceNumber);

/// <summary>Live data file in a snapshot, with the sequence number used for delete applicability.</summary>
public sealed record ScanTask(DataFile File, long DataSequenceNumber, string ManifestPath);

/// <summary>Planned data files and how much the filter pruned (manifests via partition summaries, files via partitions and metrics).</summary>
public sealed record ScanPlan(IReadOnlyList<ScanTask> Tasks, int ManifestsRead, int ManifestsSkipped, int FilesSkippedByPartition, int FilesSkippedByMetrics);

public static class TableScan
{
    public static async Task<IReadOnlyList<ManifestFile>> ManifestsAsync(Table table, Snapshot snapshot, CancellationToken ct = default)
    {
        if (snapshot.ManifestList is null)
            throw new NotSupportedException("v1 snapshots with an embedded 'manifests' list are not supported yet");
        return ManifestCodec.ReadManifestList(await table.IO.ReadAllAsync(snapshot.ManifestList, ct)).Manifests;
    }

    private static Snapshot? Resolve(TableMetadata m, long? snapshotId) =>
        snapshotId is { } id ? m.SnapshotById(id) ?? throw new ArgumentException($"Unknown snapshot {id}") : m.CurrentSnapshot;

    /// <summary>Plan the live data files of a snapshot (default: current).</summary>
    public static async Task<IReadOnlyList<ScanTask>> PlanFilesAsync(Table table, long? snapshotId = null, Func<DataFile, bool>? filter = null, CancellationToken ct = default) =>
        (await PlanAsync(table, Expression.True, snapshotId, filter, ct)).Tasks;

    /// <summary>Data files that may hold rows matching <paramref name="filter"/> (pruning only: rows still need filtering).</summary>
    public static async Task<IReadOnlyList<ScanTask>> PlanFilesAsync(Table table, Expression filter, long? snapshotId = null, CancellationToken ct = default) =>
        (await PlanAsync(table, filter, snapshotId, null, ct)).Tasks;

    /// <summary>
    /// Plan a scan (spec "Scan Planning"): skip manifests whose partition summaries cannot match the filter's inclusive
    /// projection, then files whose partition tuple or column metrics cannot match. <paramref name="fileFilter"/> is an
    /// extra caller predicate on files.
    /// </summary>
    public static async Task<ScanPlan> PlanAsync(Table table, Expression filter, long? snapshotId = null, Func<DataFile, bool>? fileFilter = null, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = Resolve(m, snapshotId);
        if (snapshot is null) return new ScanPlan([], 0, 0, 0, 0);

        var schema = m.CurrentSchema;
        var bound = Binder.Bind(schema, filter);
        var metrics = bound is AlwaysTrue ? null : new MetricsEvaluator(bound);
        var bySpec = new Dictionary<int, (ManifestEvaluator Manifest, Func<IReadOnlyList<object?>, bool> Partition)>();
        int read = 0, skipped = 0, byPartition = 0, byMetrics = 0;
        var tasks = new List<ScanTask>();
        foreach (var manifest in (await ManifestsAsync(table, snapshot, ct)).Where(mf => mf.Content == ManifestContent.Data))
        {
            var spec = m.SpecById(manifest.PartitionSpecId);
            if (!bySpec.TryGetValue(spec.SpecId, out var eval))
            {
                var projected = Projections.Inclusive(spec, schema, bound);
                bySpec[spec.SpecId] = eval = (new ManifestEvaluator(projected), RowEvaluator.Compile(projected));
            }
            if (!eval.Manifest.MightMatch(manifest.Partitions)) { skipped++; continue; }
            read++;
            var contents = ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(manifest.ManifestPath, ct), spec, schema, manifest);
            foreach (var e in contents.Entries)
            {
                if (e.Status == ManifestEntryStatus.Deleted) continue;
                if (fileFilter is not null && !fileFilter(e.DataFile)) continue;
                if (!eval.Partition(e.DataFile.Partition)) { byPartition++; continue; }
                if (metrics is not null && !metrics.MightMatch(e.DataFile)) { byMetrics++; continue; }
                tasks.Add(new ScanTask(e.DataFile, e.SequenceNumber ?? 0, manifest.ManifestPath));
            }
        }
        return new ScanPlan(tasks, read, skipped, byPartition, byMetrics);
    }

    /// <summary>The delete files (v2 position deletes, v3 deletion vectors) of a snapshot.</summary>
    public static async Task<DeleteIndex> DeletesAsync(Table table, long? snapshotId = null, CancellationToken ct = default)
    {
        var snapshot = Resolve(table.Metadata, snapshotId);
        return snapshot is null ? await DeleteIndex.LoadAsync(table, [], ct) : await DeleteIndex.LoadAsync(table, await ManifestsAsync(table, snapshot, ct), ct);
    }

    /// <summary>Read live rows with their v3 row lineage (<c>_row_id</c>, <c>_last_updated_sequence_number</c>).</summary>
    public static async Task<List<LineageRow>> ReadWithLineageAsync(Table table, long? snapshotId = null, CancellationToken ct = default)
    {
        var deletes = await DeletesAsync(table, snapshotId, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var schema = table.Schema;
        var projection = RowLineage.WithLineage(schema);
        var n = schema.Columns.Count;
        var result = new List<LineageRow>();
        foreach (var task in await PlanFilesAsync(table, snapshotId, null, ct))
        {
            var fileRows = await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, projection, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, schema, ct);
            for (var pos = 0; pos < fileRows.Count; pos++)
            {
                var row = fileRows[pos][..n];
                if (isDeleted?.Invoke(pos, row) == true) continue;
                var rowId = (long?)fileRows[pos][n] ?? (task.File.FirstRowId is { } first ? first + pos : null);
                var seq = (long?)fileRows[pos][n + 1] ?? (table.Metadata.FormatVersion >= 3 ? task.DataSequenceNumber : null);
                result.Add(new LineageRow(row, rowId, seq));
            }
        }
        return result;
    }

    /// <summary>
    /// Read all rows of a snapshot, projected onto the current schema (by field ID), with position deletes,
    /// deletion vectors and equality deletes applied (merge-on-read).
    /// </summary>
    public static Task<List<object?[]>> ReadAllAsync(Table table, long? snapshotId = null, CancellationToken ct = default) =>
        ReadAsync(table, Expression.True, snapshotId, ct);

    /// <summary>Rows of a snapshot matching <paramref name="filter"/>: pruned planning, deletes applied, then the filter on every row.</summary>
    public static async Task<List<object?[]>> ReadAsync(Table table, Expression filter, long? snapshotId = null, CancellationToken ct = default)
    {
        var bound = Binder.Bind(table.Schema, filter);
        var matches = bound is AlwaysTrue ? null : RowEvaluator.Compile(bound);
        var deletes = await DeletesAsync(table, snapshotId, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var rows = new List<object?[]>();
        foreach (var task in await PlanFilesAsync(table, filter, snapshotId, ct))
        {
            if (task.File.FileFormat != "PARQUET") throw new NotSupportedException($"{task.File.FileFormat} data files are not supported");
            var fileRows = await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, table.Schema, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, table.Schema, ct);
            for (var pos = 0; pos < fileRows.Count; pos++)
                if (isDeleted?.Invoke(pos, fileRows[pos]) != true && matches?.Invoke(fileRows[pos]) != false)
                    rows.Add(fileRows[pos]);
        }
        return rows;
    }
}

/// <summary>Writes rows into Parquet files, one per partition, under <c>{location}/data/</c>.</summary>
public static class TableWriter
{
    public static Task<IReadOnlyList<DataFile>> WriteAsync(Table table, IReadOnlyList<object?[]> rows, CancellationToken ct = default) =>
        WriteAsync(table, rows, null, ct);

    /// <summary>
    /// Write rows; with <paramref name="lineage"/> (v3), each row's <c>_row_id</c> and
    /// <c>_last_updated_sequence_number</c> are stored explicitly so rows carried through a rewrite keep their identity.
    /// </summary>
    public static async Task<IReadOnlyList<DataFile>> WriteAsync(Table table, IReadOnlyList<object?[]> rows, IReadOnlyList<(long? RowId, long? Seq)>? lineage, CancellationToken ct = default)
    {
        if (lineage is not null)
        {
            if (lineage.Count != rows.Count) throw new ArgumentException("One lineage entry per row is required");
            if (table.Metadata.FormatVersion < 3) lineage = null; // no row lineage before v3
        }
        var schema = table.Schema;
        var spec = table.Spec;
        if (spec.Fields.Any(f => f.Transform is Transforms.UnknownTransform))
            throw new NotSupportedException("Cannot write with a partition spec containing unknown transforms");

        var positions = schema.Columns.Select((c, i) => (c.Id, i)).ToDictionary(x => x.Id, x => x.i);
        var groups = rows.GroupBy(
            r => spec.Partition(schema, id => positions.TryGetValue(id, out var p) ? r[p] : throw new NotSupportedException("Partitioning by nested columns is not supported yet")),
            PartitionComparer.Instance);

        var dataDir = table.Metadata.Property("write.data.path", $"{table.Location}/data").TrimEnd('/');
        var index = new Dictionary<object?[], int>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < rows.Count; i++) index[rows[i]] = i;
        var files = new List<DataFile>();
        foreach (var g in groups)
        {
            var partitionPath = spec.IsUnpartitioned ? "" : spec.PartitionPath(schema, g.Key) + "/";
            var location = $"{dataDir}/{partitionPath}{Guid.NewGuid():N}.parquet";
            var groupRows = g.ToList();
            // Files are written in the table's default sort order (spec "Sorting"), and say so in sort_order_id.
            var order = table.Metadata.DefaultSortOrder;
            if (!order.IsUnsorted) groupRows = groupRows.OrderBy(r => r, new RowSorter(schema, order)).ToList(); // stable: ties keep input order
            DataFile written;
            if (lineage is null)
                written = await ParquetFiles.WriteAsync(table.IO, location, schema, spec.SpecId, g.Key, groupRows, ct);
            else
            {
                var withLineage = groupRows.Select(r => (object?[])[.. r, lineage[index[r]].RowId, lineage[index[r]].Seq]).ToList();
                written = await ParquetFiles.WriteAsync(table.IO, location, RowLineage.WithLineage(schema), spec.SpecId, g.Key, withLineage, ct);
            }
            files.Add(order.IsUnsorted ? written : written with { SortOrderId = order.OrderId });
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
    /// <summary>Null = derive from what the snapshot does (append / delete / overwrite).</summary>
    private string? _operation = Operations.Append;
    private readonly HashSet<string> _removePaths = new(StringComparer.Ordinal);
    private readonly List<DataFile> _addedDeleteFiles = [];
    private int? _retries;
    private readonly Dictionary<string, string> _extraSummary = [];
    private string? _writtenManifest;
    private ManifestFile? _writtenManifestFile;
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

    private readonly long? _startingSnapshotId;
    private Func<DataFile, bool>? _conflictFilter;

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
        foreach (var snapshot in m.Ancestors(m.CurrentSnapshotId).TakeWhile(sn => sn.SnapshotId != _startingSnapshotId))
        {
            if (snapshot.ManifestList is null) continue;
            foreach (var manifest in (await TableScan.ManifestsAsync(_table, snapshot, ct)).Where(mf => mf.Content == ManifestContent.Data && mf.AddedSnapshotId == snapshot.SnapshotId))
            {
                var entries = ManifestCodec.ReadManifest(await _table.IO.ReadAllAsync(manifest.ManifestPath, ct), m.SpecById(manifest.PartitionSpecId), m.CurrentSchema, manifest).Entries;
                if (entries.FirstOrDefault(e => e.Status == ManifestEntryStatus.Added && _conflictFilter(e.DataFile)) is { } conflict)
                    throw new ValidationException($"Concurrent snapshot {snapshot.SnapshotId} ({snapshot.Operation}) added {conflict.DataFile.FilePath}, which this {_operation} would remove; re-plan against the new table state");
            }
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
        if (_retries is { } r) policy = policy with { NumRetries = r };
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                if (attempt > 0) await ValidateNoConflictingDataAsync(ct);
                var snapshot = await ApplyAsync(ct);
                var requirements = new List<UpdateRequirement>
                {
                    new UpdateRequirement.AssertTableUuid(_table.Metadata.TableUuid),
                    new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, _table.Metadata.CurrentSnapshotId),
                };
                try
                {
                    await _table.CommitAsync(requirements,
                    [
                        new MetadataUpdate.AddSnapshot(snapshot),
                        new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, SnapshotRef.Branch(snapshot.SnapshotId)),
                    ], ct);
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
    /// Write this snapshot's files and return the commit instead of applying it, for
    /// <see cref="ICatalog.CommitTransactionAsync"/> across tables. On a conflict, re-stage from refreshed tables;
    /// files of an abandoned stage are left for orphan-file cleanup.
    /// </summary>
    public async Task<TableCommit> StageAsync(CancellationToken ct = default)
    {
        var snapshot = await ApplyAsync(ct);
        _attemptArtifacts.Clear();
        return new TableCommit(_table.Identifier,
            [new UpdateRequirement.AssertTableUuid(_table.Metadata.TableUuid), new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, _table.Metadata.CurrentSnapshotId)],
            [new MetadataUpdate.AddSnapshot(snapshot), new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, SnapshotRef.Branch(snapshot.SnapshotId))]);
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
        var parent = m.CurrentSnapshot;
        var snapshotId = NewSnapshotId();
        var sequenceNumber = m.NextSequenceNumber;
        var metaDir = m.Property("write.metadata.path", $"{m.Location}/metadata").TrimEnd('/');
        var commitUuid = Guid.NewGuid();

        var manifests = new List<ManifestFile>();
        long deletedFiles = 0, deletedRecords = 0, deletedSize = 0;
        var parentManifests = parent is null ? [] : await TableScan.ManifestsAsync(_table, parent, ct);

        // 0. Row-level deletes / conversion: decide which delete files to add and which to remove.
        var deleteChange = _rowDeletes is not null || _convertPositionDeletes || _addedDeleteFiles.Count > 0 || _deleteWhere is not null
            ? await PlanDeleteChangeAsync(parent, parentManifests, commitUuid, ct)
            : DeleteChange.None;

        // 1. Carry forward (or rewrite, when deleting) the parent's manifests.
        var toRegroup = new List<ManifestFile>();
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

        // 2. One new manifest for added files, written once and reused across retries.
        if (_added.Count > 0)
        {
            if (_writtenManifest is null)
            {
                _writtenManifest = $"{metaDir}/{commitUuid}-m-added.avro";
                // Null snapshot ID and sequence numbers: inherited from the manifest list, so this file is valid for any attempt.
                var entries = _added.Select(f => new ManifestEntry(ManifestEntryStatus.Added, null, null, null, f)).ToList();
                _writtenManifestFile = await WriteManifestAsync(_writtenManifest, m.DefaultSpec, ManifestContent.Data, entries, snapshotId, sequenceNumber, ct);
            }
            manifests.Insert(0, _writtenManifestFile! with { SequenceNumber = sequenceNumber, MinSequenceNumber = sequenceNumber, AddedSnapshotId = snapshotId });
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
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
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
        bool Mergeable(ManifestFile mf) => mf.Content == ManifestContent.Data && (carried.Contains(mf.ManifestPath) || mf.ManifestPath == _writtenManifest);

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
                        entries.Add(mf.ManifestPath == _writtenManifest
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

/// <summary>A commit's preconditions no longer hold on the current table (not retryable, unlike <see cref="CommitFailedException"/>).</summary>
public sealed class ValidationException(string message) : Exception(message);

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
