using Iceberg.Net.Catalog;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>How existing rows are removed by a delete, upsert or group replacement.</summary>
public enum DeleteStrategy
{
    /// <summary>From the table's <c>write.delete.mode</c> / <c>write.merge.mode</c> (copy-on-write unless set to merge-on-read).</summary>
    Default,
    /// <summary>Rewrite every data file that holds a deleted row, without it. Cheapest to read; most expensive to write.</summary>
    CopyOnWrite,
    /// <summary>Merge-on-read by position: v2 position delete files, v3 deletion vectors. Needs a scan to find positions.</summary>
    PositionDeletes,
    /// <summary>
    /// Merge-on-read by value: equality delete files holding the keys. No scan, so the cheapest write, but every read
    /// applies them until compaction. Only for key-based changes; partition source columns must be part of the key.
    /// PyIceberg cannot read tables with equality deletes, and format v4 forbids writing new ones.
    /// </summary>
    EqualityDeletes,
}

/// <param name="RowsDeleted">Null for equality deletes: they are written without scanning, so the count is unknown.</param>
public sealed record MutationResult(Snapshot Snapshot, long RowsInserted, long? RowsDeleted, int DataFilesRewritten, DeleteStrategy Strategy, int Attempts);

/// <summary>
/// Row-level write modes over Iceberg tables. Each call commits exactly one snapshot. On a concurrent commit the
/// whole change is re-planned against the new table state and retried (serializable: rows another writer added
/// are seen by the retry), using <c>commit.retry.*</c> properties for the retry budget.
/// Rows are <c>object?[]</c> aligned to the current schema's top-level columns (see <see cref="ParquetFiles"/>).
/// </summary>
public static class TableMutations
{
    public const string DeleteModeProperty = "write.delete.mode";
    public const string MergeModeProperty = "write.merge.mode";

    /// <summary>Pure insertion: append rows; existing rows are never read or changed.</summary>
    public static Task<MutationResult> InsertAsync(Table table, IReadOnlyList<object?[]> rows, CancellationToken ct = default) =>
        RunAsync(table, rows, null, DeleteStrategy.CopyOnWrite, ct);

    /// <summary>
    /// Insert-or-replace by key: every existing row whose key matches an incoming row is deleted and the incoming
    /// rows are inserted, atomically. <paramref name="keyColumns"/> defaults to the schema's identifier fields.
    /// Incoming keys must be unique and non-null.
    /// </summary>
    public static Task<MutationResult> UpsertAsync(Table table, IReadOnlyList<object?[]> rows, IReadOnlyList<string>? keyColumns = null,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var key = Key.Resolve(table.Schema, keyColumns, requireIdentifierDefault: true);
        var keys = key.KeysOf(rows, requireUnique: true, allowNull: false, what: "upsert key");
        return RunAsync(table, rows, new Matcher(key, keys, null), Resolve(table, strategy, MergeModeProperty), ct);
    }

    /// <summary>
    /// Group replacement: for every distinct group key present in <paramref name="rows"/>, delete all existing rows
    /// of that group, then insert <paramref name="rows"/> — e.g. "replace everything for client 42 and period
    /// 2024-09". Groups not present in the input are untouched. Null group values form their own group.
    /// </summary>
    public static Task<MutationResult> ReplaceGroupsAsync(Table table, IReadOnlyList<object?[]> rows, IReadOnlyList<string> groupColumns,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var key = Key.Resolve(table.Schema, groupColumns, requireIdentifierDefault: false);
        var keys = key.KeysOf(rows, requireUnique: false, allowNull: true, what: "group key");
        return RunAsync(table, rows, new Matcher(key, keys, null), Resolve(table, strategy, MergeModeProperty), ct);
    }

    /// <summary>Delete rows by key. <paramref name="keys"/> are tuples in <paramref name="keyColumns"/> order.</summary>
    public static Task<MutationResult> DeleteByKeysAsync(Table table, IReadOnlyList<string> keyColumns, IEnumerable<object?[]> keys,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var key = Key.Resolve(table.Schema, keyColumns, requireIdentifierDefault: false);
        var encoded = keys.Select(k => key.Encode(k)).ToHashSet(StringComparer.Ordinal);
        var sample = keys.ToList();
        return RunAsync(table, [], new Matcher(key, new KeySet(encoded, sample), null), Resolve(table, strategy, DeleteModeProperty), ct);
    }

    /// <summary>Delete rows matching a predicate over full rows. Equality deletes cannot express predicates.</summary>
    public static Task<MutationResult> DeleteWhereAsync(Table table, Func<object?[], bool> predicate,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var resolved = Resolve(table, strategy, DeleteModeProperty);
        if (resolved == DeleteStrategy.EqualityDeletes)
            throw new ArgumentException("Equality deletes delete by key; use DeleteByKeysAsync, or CopyOnWrite/PositionDeletes for predicates");
        return RunAsync(table, [], new Matcher(null, null, predicate), resolved, ct);
    }

    public static DeleteStrategy Resolve(Table table, DeleteStrategy strategy, string modeProperty) => strategy != DeleteStrategy.Default
        ? strategy
        : table.Metadata.Property(modeProperty, "copy-on-write") == "merge-on-read" ? DeleteStrategy.PositionDeletes : DeleteStrategy.CopyOnWrite;

    // ---------------------------------------------------------------------------------------------------------

    private sealed record KeySet(HashSet<string> Encoded, IReadOnlyList<object?[]> Tuples);

    private sealed class Key(IReadOnlyList<NestedField> fields, int[] columns)
    {
        public IReadOnlyList<NestedField> Fields { get; } = fields;
        public int[] Columns { get; } = columns;

        public static Key Resolve(Schema schema, IReadOnlyList<string>? names, bool requireIdentifierDefault)
        {
            IReadOnlyList<NestedField> fields;
            if (names is { Count: > 0 })
                fields = names.Select(n => schema.FindField(n) is { } f && schema.Columns.Contains(f) ? f : throw new ArgumentException($"Key column '{n}' is not a top-level column")).ToList();
            else if (requireIdentifierDefault && schema.IdentifierFieldIds.Count > 0)
                fields = schema.IdentifierFieldIds.Select(id => schema.FindField(id)!).ToList();
            else
                throw new ArgumentException("No key columns given and the schema has no identifier fields");
            if (fields.Any(f => !f.Type.IsPrimitive)) throw new ArgumentException("Key columns must be primitive");
            var cols = schema.Columns.ToList();
            return new Key(fields, fields.Select(f => cols.IndexOf(f)).ToArray());
        }

        public string FromRow(object?[] row) => EqualityKey.Encode(Fields, i => row[Columns[i]]);
        public string Encode(object?[] tuple) => EqualityKey.Encode(Fields, i => tuple[i]);
        public object?[] TupleOf(object?[] row) => Columns.Select(c => row[c]).ToArray();

        public KeySet KeysOf(IReadOnlyList<object?[]> rows, bool requireUnique, bool allowNull, string what)
        {
            var encoded = new HashSet<string>(StringComparer.Ordinal);
            var tuples = new List<object?[]>();
            foreach (var row in rows)
            {
                var tuple = TupleOf(row);
                if (!allowNull && tuple.Any(v => v is null)) throw new ArgumentException($"Null {what} [{string.Join(", ", tuple)}]");
                if (encoded.Add(Encode(tuple))) tuples.Add(tuple);
                else if (requireUnique) throw new ArgumentException($"Duplicate {what} [{string.Join(", ", tuple)}] in input");
            }
            return new KeySet(encoded, tuples);
        }
    }

    /// <summary>What to delete: rows whose key is in a set, or rows matching a predicate.</summary>
    private sealed record Matcher(Key? Key, KeySet? Keys, Func<object?[], bool>? Predicate)
    {
        public bool Matches(object?[] row) => Predicate?.Invoke(row) ?? Keys!.Encoded.Contains(Key!.FromRow(row));
    }

    private static async Task<MutationResult> RunAsync(Table table, IReadOnlyList<object?[]> inserts, Matcher? matcher, DeleteStrategy strategy, CancellationToken ct)
    {
        // Inserted rows don't depend on the table state, so their files are written once and reused by every attempt.
        IReadOnlyList<DataFile>? insertFiles = null;
        var policy = CommitRetryPolicy.From(table.Metadata);
        for (var attempt = 1; ; attempt++)
        {
            if (attempt > 1) await table.RefreshAsync(ct);
            insertFiles ??= inserts.Count > 0 ? await TableWriter.WriteAsync(table, inserts, ct) : [];
            var producer = SnapshotProducer.RowDelta(table).AddFiles(insertFiles).WithRetries(0);
            long? deleted = 0;
            var rewritten = 0;

            if (matcher is not null && table.Metadata.CurrentSnapshot is not null)
            {
                switch (strategy)
                {
                    case DeleteStrategy.EqualityDeletes:
                        await AddEqualityDeletesAsync(table, producer, matcher, ct);
                        deleted = null; // unknown without a scan; that is the point of equality deletes
                        break;
                    case DeleteStrategy.PositionDeletes:
                        foreach (var (file, positions, _) in await FindMatchesAsync(table, matcher, ct))
                        {
                            producer.DeletePositions(file.FilePath, positions);
                            deleted += positions.Count;
                        }
                        break;
                    case DeleteStrategy.CopyOnWrite:
                        foreach (var (file, positions, survivors) in await FindMatchesAsync(table, matcher, ct))
                        {
                            producer.RemoveDataFiles([file.FilePath]);
                            if (survivors.Count > 0) producer.AddFiles(await TableWriter.WriteAsync(table, survivors, ct));
                            deleted += positions.Count;
                            rewritten++;
                        }
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(strategy));
                }
            }

            try
            {
                var snapshot = await producer.CommitAsync(ct);
                return new MutationResult(snapshot, inserts.Count, deleted, rewritten, strategy, attempt);
            }
            catch (CommitFailedException) when (attempt <= policy.NumRetries)
            {
                var wait = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * (1 << Math.Min(attempt - 1, 20)));
                await Task.Delay(wait + Random.Shared.Next(wait / 10 + 1), ct);
            }
        }
    }

    /// <summary>Live rows of the current snapshot that the matcher selects, per data file (existing deletes applied).</summary>
    private static async Task<List<(DataFile File, List<long> Positions, List<object?[]> Survivors)>> FindMatchesAsync(Table table, Matcher matcher, CancellationToken ct)
    {
        var deletes = await TableScan.DeletesAsync(table, null, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var result = new List<(DataFile, List<long>, List<object?[]>)>();
        foreach (var task in await TableScan.PlanFilesAsync(table, null, null, ct))
        {
            var rows = await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, table.Schema, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, table.Schema, ct);
            var positions = new List<long>();
            var survivors = new List<object?[]>();
            for (var pos = 0; pos < rows.Count; pos++)
            {
                if (isDeleted?.Invoke(pos, rows[pos]) == true) continue;
                if (matcher.Matches(rows[pos])) positions.Add(pos);
                else survivors.Add(rows[pos]);
            }
            if (positions.Count > 0) result.Add((task.File, positions, survivors));
        }
        return result;
    }

    private static async Task AddEqualityDeletesAsync(Table table, SnapshotProducer producer, Matcher matcher, CancellationToken ct)
    {
        var key = matcher.Key ?? throw new InvalidOperationException("Equality deletes need a key");
        var schema = table.Schema;
        var spec = table.Spec;
        // An equality delete only applies within its partition (unless unpartitioned), so the partition of every
        // existing row with a given key must be derivable from the key itself.
        var keyIds = key.Fields.Select(f => f.Id).ToHashSet();
        if (spec.Fields.Any(f => !(f.Transform is Transforms.VoidTransform) && !keyIds.Contains(f.SourceId)))
            throw new ValidationException("Equality deletes require every partition source column to be part of the key; use PositionDeletes or CopyOnWrite");
        if (table.Metadata.Specs().Any(s => s.SpecId != spec.SpecId && !s.IsUnpartitioned))
            throw new ValidationException("Equality deletes on a table with several partition specs are not supported yet (rows may live under older specs)");

        var positions = key.Fields.Select((f, i) => (f.Id, i)).ToDictionary(x => x.Id, x => x.i);
        var dataDir = table.Metadata.Property("write.data.path", $"{table.Location}/data").TrimEnd('/');
        var byPartition = matcher.Keys!.Tuples.GroupBy(t => spec.Partition(schema, id => t[positions[id]]), PartitionComparer.Instance);
        var files = new List<DataFile>();
        foreach (var group in byPartition)
        {
            var location = $"{dataDir}/{Guid.NewGuid():N}-eq-deletes.parquet";
            files.Add(await EqualityDeleteFiles.WriteAsync(table.IO, location, spec.SpecId, group.Key, key.Fields, group, ct));
        }
        producer.AddDeleteFiles(files);
    }

    private static IEnumerable<Partitioning.PartitionSpec> Specs(this TableMetadata m) => m.PartitionSpecs;

    private sealed class PartitionComparer : IEqualityComparer<object?[]>
    {
        public static readonly PartitionComparer Instance = new();
        public bool Equals(object?[]? a, object?[]? b) => a!.Length == b!.Length && a.Zip(b).All(p => Equals(p.First, p.Second));
        public int GetHashCode(object?[] o) => o.Aggregate(17, (h, v) => HashCode.Combine(h, v));
    }
}
