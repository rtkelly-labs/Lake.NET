using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

public enum ChangeType { Insert, Delete }

/// <summary>
/// One changed row, aligned to the table's current schema (Spark's <c>_change_type</c>, <c>_change_ordinal</c> and
/// <c>_commit_snapshot_id</c> columns).
/// </summary>
public sealed record ChangeRow(object?[] Row, ChangeType ChangeType, int ChangeOrdinal, long CommitSnapshotId);

public sealed record ChangeFeedOptions
{
    /// <summary>The branch whose history is read when no end snapshot is given (default <c>main</c>).</summary>
    public string Branch { get; init; } = SnapshotRef.Main;
    /// <summary>
    /// Drop a DELETE and an INSERT of an identical row within one snapshot: the rows a copy-on-write rewrite carried over
    /// unchanged. Off by default, as Spark's <c>changes</c> table; on, as <c>create_changelog_view</c>'s default.
    /// </summary>
    public bool RemoveCarryovers { get; init; }
}

/// <summary>
/// Row-level changes between two snapshots (Java's <c>IncrementalChangelogScan</c>, Spark's <c>changes</c> metadata
/// table). For each snapshot after <c>fromExclusive</c> up to <c>toInclusive</c> on the branch's ancestry, oldest
/// first: rows of data files it added are INSERTs (minus rows deletes of the same snapshot remove); rows of data files it
/// removed are DELETEs (with the deletes that applied before it); and rows of carried-over files that its new position
/// deletes, deletion vectors or equality deletes remove are DELETEs — for position deletes and DVs, exactly the rows
/// they reference. <c>replace</c> snapshots (compaction, manifest and delete rewrites) change no rows and are skipped, so
/// ordinals count only the snapshots that changed data.
/// </summary>
/// <remarks>
/// A copy-on-write overwrite reports the whole rewritten file: its untouched rows as DELETE and INSERT pairs, as Spark's
/// <c>changes</c> table does. <see cref="ChangeFeedOptions.RemoveCarryovers"/> removes those pairs (identical values in
/// every column), as Spark's <c>create_changelog_view</c> does by default; with duplicate rows in a table, a genuine
/// delete and re-insert of an identical row in one commit is removed as well — the same trade-off Spark makes.
/// </remarks>
public static class ChangeFeed
{
    public static async IAsyncEnumerable<ChangeRow> ChangesAsync(Table table, long? fromExclusive, long? toInclusive = null, ChangeFeedOptions? options = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        options ??= new ChangeFeedOptions();
        var m = table.Metadata;
        var end = toInclusive ?? (m.Refs.TryGetValue(options.Branch, out var r) ? r.SnapshotId : (long?)null);
        if (end is null) yield break;
        if (m.SnapshotById(end.Value) is null) throw new ArgumentException($"Unknown snapshot {end}");

        var range = new List<Snapshot>();
        var reached = fromExclusive is null;
        foreach (var s in m.Ancestors(end))
        {
            if (s.SnapshotId == fromExclusive) { reached = true; break; }
            range.Add(s);
        }
        if (!reached) throw new ArgumentException($"Starting snapshot {fromExclusive} is not an ancestor of end snapshot {end}");
        if (fromExclusive is null && range.Count > 0 && range[^1].ParentSnapshotId is { } missing)
            throw new ArgumentException($"Cannot read changes from the beginning: snapshot {missing} has expired");
        range.Reverse();

        var schema = m.CurrentSchema;
        var mapping = NameMapping.ForTable(m.Properties);
        var ordinal = 0;
        foreach (var snapshot in range.Where(s => s.Operation != Operations.Replace))
        {
            var changes = await SnapshotChangesAsync(table, snapshot, schema, mapping, ct);
            if (options.RemoveCarryovers) changes = RemoveCarryovers(changes);
            foreach (var (row, type) in changes) yield return new ChangeRow(row, type, ordinal, snapshot.SnapshotId);
            ordinal++;
        }
    }

    private static async Task<List<(object?[] Row, ChangeType Type)>> SnapshotChangesAsync(Table table, Snapshot snapshot, Schema schema, NameMapping? mapping, CancellationToken ct)
    {
        var m = table.Metadata;
        var manifests = await TableScan.ManifestsAsync(table, snapshot, ct);
        var parentManifests = snapshot.ParentSnapshotId is { } p && m.SnapshotById(p) is { } parent ? await TableScan.ManifestsAsync(table, parent, ct) : [];
        var after = await DeleteIndex.LoadAsync(table, manifests, ct);
        var before = await DeleteIndex.LoadAsync(table, parentManifests, ct);

        var addedData = new List<(DataFile File, long Seq)>();
        var removedData = new List<(DataFile File, long Seq)>();
        var addedDeletes = new List<DataFile>();
        foreach (var mf in manifests.Where(mf => mf.AddedSnapshotId == snapshot.SnapshotId))
        {
            var entries = ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(mf.ManifestPath, ct), m.SpecById(mf.PartitionSpecId), m.CurrentSchema, mf).Entries;
            foreach (var e in entries.Where(e => e.SnapshotId == snapshot.SnapshotId))
            {
                var file = e.DataFile with { SpecId = mf.PartitionSpecId };
                if (mf.Content == ManifestContent.Data)
                {
                    if (e.Status == ManifestEntryStatus.Added) addedData.Add((file, e.SequenceNumber ?? 0));
                    else if (e.Status == ManifestEntryStatus.Deleted) removedData.Add((file, e.SequenceNumber ?? 0));
                }
                else if (e.Status == ManifestEntryStatus.Added) addedDeletes.Add(file);
            }
        }

        var changes = new List<(object?[], ChangeType)>();
        foreach (var (file, seq) in removedData)
        {
            var rows = await DataFiles.ReadAsync(table.IO, file, schema, mapping, ct);
            var deleted = await before.RowFilterAsync(file, seq, schema, ct);
            for (var pos = 0; pos < rows.Count; pos++)
                if (deleted?.Invoke(pos, rows[pos]) != true) changes.Add((rows[pos], ChangeType.Delete));
        }

        if (addedDeletes.Count > 0)
        {
            // Data files carried over from the parent that one of the new delete files may apply to.
            var addedPaths = addedData.Select(a => a.File.FilePath).ToHashSet(StringComparer.Ordinal);
            var referenced = new HashSet<string>(StringComparer.Ordinal);
            var partitions = new HashSet<string>(StringComparer.Ordinal);
            var global = false;
            foreach (var d in addedDeletes)
            {
                if (d.Content == FileContent.EqualityDeletes)
                {
                    if (m.SpecById(d.SpecId).IsUnpartitioned) global = true;
                    else partitions.Add(CherryPicker.PartitionKey(d.SpecId, d.Partition));
                }
                else if (d.ReferencedDataFile is { } target) referenced.Add(target);
                else referenced.UnionWith((await after.PositionDeletesAsync(d.FilePath, d.FileFormat)).Keys);
            }
            foreach (var task in await TableScan.PlanFilesAsync(table, snapshot.SnapshotId, null, ct))
            {
                var file = task.File;
                if (addedPaths.Contains(file.FilePath)) continue;
                if (!global && !referenced.Contains(file.FilePath) && !partitions.Contains(CherryPicker.PartitionKey(file.SpecId, file.Partition))) continue;
                var now = await after.RowFilterAsync(file, task.DataSequenceNumber, schema, ct);
                if (now is null) continue;
                var was = await before.RowFilterAsync(file, task.DataSequenceNumber, schema, ct);
                var rows = await DataFiles.ReadAsync(table.IO, file, schema, mapping, ct);
                for (var pos = 0; pos < rows.Count; pos++)
                    if (now(pos, rows[pos]) && was?.Invoke(pos, rows[pos]) != true) changes.Add((rows[pos], ChangeType.Delete));
            }
        }

        foreach (var (file, seq) in addedData)
        {
            var rows = await DataFiles.ReadAsync(table.IO, file, schema, mapping, ct);
            var deleted = await after.RowFilterAsync(file, seq, schema, ct);
            for (var pos = 0; pos < rows.Count; pos++)
                if (deleted?.Invoke(pos, rows[pos]) != true) changes.Add((rows[pos], ChangeType.Insert));
        }
        return changes;
    }

    private static List<(object?[] Row, ChangeType Type)> RemoveCarryovers(List<(object?[] Row, ChangeType Type)> changes)
    {
        var inserts = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var (row, type) in changes)
            if (type == ChangeType.Insert) inserts[Key(row)] = inserts.GetValueOrDefault(Key(row)) + 1;
        var deletes = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var (row, type) in changes)
            if (type == ChangeType.Delete) deletes[Key(row)] = deletes.GetValueOrDefault(Key(row)) + 1;
        var cancel = inserts.Keys.Where(deletes.ContainsKey).ToDictionary(k => k, k => Math.Min(inserts[k], deletes[k]), StringComparer.Ordinal);
        var cancelInserts = new Dictionary<string, int>(cancel, StringComparer.Ordinal);
        var result = new List<(object?[], ChangeType)>();
        foreach (var (row, type) in changes)
        {
            var key = Key(row);
            var pending = type == ChangeType.Delete ? cancel : cancelInserts;
            if (pending.TryGetValue(key, out var n) && n > 0) { pending[key] = n - 1; continue; }
            result.Add((row, type));
        }
        return result;
    }

    private static string Key(object?[] row)
    {
        var sb = new StringBuilder();
        void Append(object? v)
        {
            switch (v)
            {
                case null: sb.Append('\u0000'); break;
                case byte[] b: sb.Append("b:").Append(Convert.ToHexString(b)); break;
                case string s: sb.Append("s:").Append(s.Length).Append(':').Append(s); break;
                case System.Collections.IDictionary d:
                    sb.Append('{');
                    foreach (System.Collections.DictionaryEntry e in d) { Append(e.Key); sb.Append('='); Append(e.Value); sb.Append(';'); }
                    sb.Append('}');
                    break;
                case System.Collections.IEnumerable list:
                    sb.Append('[');
                    foreach (var x in list) { Append(x); sb.Append(','); }
                    sb.Append(']');
                    break;
                case IFormattable f: sb.Append(v.GetType().Name).Append(':').Append(f.ToString(null, CultureInfo.InvariantCulture)); break;
                default: sb.Append(v); break;
            }
            sb.Append('\u001f');
        }
        foreach (var v in row) Append(v);
        return sb.ToString();
    }
}
