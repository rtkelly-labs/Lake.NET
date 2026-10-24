using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
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
public static partial class TableMutations
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
        return RunAsync(table, rows, new Matcher(key, keys, null, key.FilterFor(keys.Tuples)), Resolve(table, strategy, MergeModeProperty), ct);
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
        return RunAsync(table, rows, new Matcher(key, keys, null, key.FilterFor(keys.Tuples)), Resolve(table, strategy, MergeModeProperty), ct);
    }

    /// <summary>Delete rows by key. <paramref name="keys"/> are tuples in <paramref name="keyColumns"/> order.</summary>
    public static Task<MutationResult> DeleteByKeysAsync(Table table, IReadOnlyList<string> keyColumns, IEnumerable<object?[]> keys,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var key = Key.Resolve(table.Schema, keyColumns, requireIdentifierDefault: false);
        var encoded = keys.Select(k => key.Encode(k)).ToHashSet(StringComparer.Ordinal);
        var sample = keys.ToList();
        return RunAsync(table, [], new Matcher(key, new KeySet(encoded, sample), null, key.FilterFor(sample)), Resolve(table, strategy, DeleteModeProperty), ct);
    }

    /// <summary>Delete rows matching a predicate over full rows. Equality deletes cannot express predicates.</summary>
    public static Task<MutationResult> DeleteWhereAsync(Table table, Func<object?[], bool> predicate,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var resolved = Resolve(table, strategy, DeleteModeProperty);
        if (resolved == DeleteStrategy.EqualityDeletes)
            throw new ArgumentException("Equality deletes delete by key; use DeleteByKeysAsync, or CopyOnWrite/PositionDeletes for predicates");
        return RunAsync(table, [], new Matcher(null, null, predicate, Expression.True), resolved, ct);
    }

    /// <summary>
    /// Delete rows matching a filter expression. Unlike the <see cref="Func{T, TResult}"/> overload, only data files
    /// whose partitions and column metrics can match are read.
    /// </summary>
    public static Task<MutationResult> DeleteWhereAsync(Table table, Expression filter,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var resolved = Resolve(table, strategy, DeleteModeProperty);
        if (resolved == DeleteStrategy.EqualityDeletes)
            throw new ArgumentException("Equality deletes delete by key; use DeleteByKeysAsync, or CopyOnWrite/PositionDeletes for predicates");
        var matches = RowEvaluator.Compile(Binder.Bind(table.Schema, filter));
        return RunAsync(table, [], new Matcher(null, null, row => matches(row), filter), resolved, ct);
    }

    /// <summary>
    /// Delete rows matching a filter expression and a row-level refinement. The filter prunes files by partition and
    /// metrics as in the <see cref="Expression"/> overload; a row is deleted only when it matches the filter and
    /// <paramref name="refine"/> returns true. Use this when only part of a predicate can be expressed as an
    /// <see cref="Expression"/> (a LINQ residual, say): pass the expressible part as the filter.
    /// </summary>
    public static Task<MutationResult> DeleteWhereAsync(Table table, Expression filter, Func<object?[], bool> refine,
        DeleteStrategy strategy = DeleteStrategy.Default, CancellationToken ct = default)
    {
        var resolved = Resolve(table, strategy, DeleteModeProperty);
        if (resolved == DeleteStrategy.EqualityDeletes)
            throw new ArgumentException("Equality deletes delete by key; use DeleteByKeysAsync, or CopyOnWrite/PositionDeletes for predicates");
        var matches = RowEvaluator.Compile(Binder.Bind(table.Schema, filter));
        return RunAsync(table, [], new Matcher(null, null, row => matches(row) && refine(row), filter), resolved, ct);
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

        /// <summary>
        /// A pruning filter implied by a key set: per key column, membership (or the min–max range for large sets),
        /// or-ed with "is null" when a key holds nulls. Floating-point columns are not pruned (NaN keys).
        /// </summary>
        public Expression FilterFor(IReadOnlyList<object?[]> tuples)
        {
            var filter = Expression.True;
            for (var j = 0; j < Fields.Count; j++)
            {
                var field = Fields[j];
                if (field.Type.TypeId is TypeId.Float or TypeId.Double) continue;
                var values = tuples.Select(t => t[j]).Where(v => v is not null).Distinct(ValueEquality.Instance).Cast<object>().ToList();
                var column = values.Count == 0 ? Expression.False
                    : values.Count <= 1000 ? Expr.In(field.Name, values)
                    : Expr.And(Expr.GreaterThanOrEqual(field.Name, values.Min(Comparer<object>.Create(ValueComparer.Compare))!),
                               Expr.LessThanOrEqual(field.Name, values.Max(Comparer<object>.Create(ValueComparer.Compare))!));
                if (tuples.Any(t => t[j] is null)) column = Expr.Or(column, Expr.IsNull(field.Name));
                filter = Expr.And(filter, column);
            }
            return filter;
        }

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
    /// <param name="Prune">Implied by the match (never narrower): data files it rules out are not read.</param>
    private sealed record Matcher(Key? Key, KeySet? Keys, Func<object?[], bool>? Predicate, Expression Prune)
    {
        public bool Matches(object?[] row) => Predicate?.Invoke(row) ?? Keys!.Encoded.Contains(Key!.FromRow(row));
    }

    private static async Task<MutationResult> RunAsync(Table table, IReadOnlyList<object?[]> inserts, Matcher? matcher, DeleteStrategy strategy, CancellationToken ct)
    {
        // Inserted rows don't depend on the table state, so their files are written once and reused by every attempt.
        IReadOnlyList<DataFile>? insertFiles = null;
        var (snapshot, (deleted, rewritten), attempts) = await RetryAsync(table, async () =>
        {
            insertFiles ??= inserts.Count > 0 ? await TableWriter.WriteAsync(table, inserts, ct) : [];
            var producer = SnapshotProducer.RowDelta(table).AddFiles(insertFiles).WithRetries(0);
            (long? Deleted, int Rewritten) outcome = (0, 0);
            if (matcher is not null && table.Metadata.CurrentSnapshot is not null)
                outcome = await PlanDeletesAsync(table, producer, matcher, strategy, ct);
            return (producer, outcome);
        }, ct);
        return new MutationResult(snapshot!, inserts.Count, deleted, rewritten, strategy, attempts);
    }

    /// <summary>
    /// The serializable re-plan loop shared by every mutation: <paramref name="plan"/> builds this attempt's change
    /// against the table's current state (a null producer means there is nothing to commit); on a lost race the table
    /// is refreshed and the change planned again from scratch, never replayed. A base file removed meanwhile by
    /// compaction and expiry counts as a lost race when the table has moved.
    /// </summary>
    private static async Task<(Snapshot? Snapshot, T Outcome, int Attempts)> RetryAsync<T>(Table table, Func<Task<(SnapshotProducer? Producer, T Outcome)>> plan, CancellationToken ct)
    {
        var policy = CommitRetryPolicy.From(table.Metadata);
        for (var attempt = 1; ; attempt++)
        {
            if (attempt > 1) await table.RefreshAsync(ct);
            SnapshotProducer? producer;
            T outcome;
            try
            {
                (producer, outcome) = await plan();
            }
            catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException && attempt <= policy.NumRetries)
            {
                // Compaction and expiry removed a file of the state this attempt planned against: re-plan if the table
                // has moved since, as for a lost commit race; otherwise the file is genuinely missing.
                var before = table.Metadata.CurrentSnapshotId;
                await table.RefreshAsync(ct);
                if (table.Metadata.CurrentSnapshotId == before) throw;
                await BackOffAsync(policy, attempt, ct);
                continue;
            }
            if (producer is null) return (null, outcome, attempt);

            try
            {
                return (await producer.CommitAsync(ct), outcome, attempt);
            }
            catch (CommitFailedException) when (attempt <= policy.NumRetries)
            {
                await BackOffAsync(policy, attempt, ct);
            }
        }
    }

    /// <summary>Adds this attempt's deletes to <paramref name="producer"/>; returns rows deleted (null if unknown) and files rewritten.</summary>
    private static async Task<(long? Deleted, int Rewritten)> PlanDeletesAsync(Table table, SnapshotProducer producer, Matcher matcher, DeleteStrategy strategy, CancellationToken ct)
    {
        long? deleted = 0;
        var rewritten = 0;
        switch (strategy)
        {
            case DeleteStrategy.EqualityDeletes:
                await AddEqualityDeletesAsync(table, producer, matcher, ct);
                deleted = null; // unknown without a scan; that is the point of equality deletes
                break;
            case DeleteStrategy.PositionDeletes:
                foreach (var m in await FindMatchesAsync(table, matcher.Prune, row => matcher.Matches(row) ? RowChange.Delete : null, ct))
                {
                    producer.DeletePositions(m.File.FilePath, m.Positions);
                    deleted += m.Positions.Count;
                }
                break;
            case DeleteStrategy.CopyOnWrite:
                foreach (var m in await FindMatchesAsync(table, matcher.Prune, row => matcher.Matches(row) ? RowChange.Delete : null, ct))
                {
                    await RewriteWithoutAsync(table, producer, m, ct);
                    deleted += m.Positions.Count;
                    rewritten++;
                }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(strategy));
        }
        return (deleted, rewritten);
    }

    /// <summary>Copy-on-write: remove the file and add its surviving rows, which keep their row identity (v3 row lineage).</summary>
    private static async Task RewriteWithoutAsync(Table table, SnapshotProducer producer, FileMatch m, CancellationToken ct)
    {
        producer.RemoveDataFiles([m.File.FilePath]);
        if (m.Survivors.Count > 0) producer.AddFiles(await TableWriter.WriteAsync(table, m.Survivors, m.SurvivorLineage.Count > 0 ? m.SurvivorLineage : null, ct));
    }

    private static Task BackOffAsync(CommitRetryPolicy policy, int attempt, CancellationToken ct)
    {
        var wait = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * (1 << Math.Min(attempt - 1, 20)));
        return Task.Delay(wait + Random.Shared.Next(wait / 10 + 1), ct);
    }

    /// <summary>What a mutation does to one live row: delete it (<see cref="Replacement"/> null) or replace it.</summary>
    private sealed record RowChange(object?[]? Replacement)
    {
        public static readonly RowChange Delete = new((object?[]?)null);
    }

    /// <summary>
    /// The live rows of one data file that a mutation changes (by position), the replacement rows with the lineage of
    /// the rows they replace, and the rows that survive with their own lineage (v3; empty before v3).
    /// </summary>
    private sealed record FileMatch(DataFile File, List<long> Positions, List<object?[]> Replacements, List<(long? RowId, long? Seq)> ReplacedLineage,
        List<object?[]> Survivors, List<(long? RowId, long? Seq)> SurvivorLineage);

    /// <summary>
    /// Live rows of the current snapshot (existing deletes applied) in the files <paramref name="prune"/> can reach,
    /// classified by <paramref name="change"/> (null keeps the row), per data file that has at least one change. On v3
    /// each row carries its lineage so a rewrite or an update can preserve <c>_row_id</c>.
    /// </summary>
    private static async Task<List<FileMatch>> FindMatchesAsync(Table table, Expression prune, Func<object?[], RowChange?> change, CancellationToken ct)
    {
        var deletes = await TableScan.DeletesAsync(table, null, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var v3 = table.Metadata.FormatVersion >= 3;
        // Columns, then lineage, then equality-delete columns the schema no longer has (deletes on them still apply).
        var projection = deletes.WithEqualityColumns(v3 ? RowLineage.WithLineage(table.Schema) : table.Schema);
        var n = table.Schema.Columns.Count;
        var result = new List<FileMatch>();
        foreach (var task in await TableScan.PlanFilesAsync(table, prune, null, ct))
        {
            var rows = await DataFiles.ReadAsync(table.IO, task.File, projection, mapping, ct);
            var isDeleted = deletes.IsEmpty ? null : await deletes.RowFilterAsync(task.File, task.DataSequenceNumber, projection, ct);
            var m = new FileMatch(task.File, [], [], [], [], []);
            for (var pos = 0; pos < rows.Count; pos++)
            {
                if (isDeleted?.Invoke(pos, rows[pos]) == true) continue;
                var row = rows[pos][..n];
                (long?, long?) lineage = v3
                    ? ((long?)rows[pos][n] ?? (task.File.FirstRowId is { } f ? f + pos : null), (long?)rows[pos][n + 1] ?? task.DataSequenceNumber)
                    : (null, null);
                if (change(row) is { } c)
                {
                    m.Positions.Add(pos);
                    if (c.Replacement is { } replacement)
                    {
                        m.Replacements.Add(replacement);
                        m.ReplacedLineage.Add(lineage);
                    }
                    continue;
                }
                m.Survivors.Add(row);
                if (v3) m.SurvivorLineage.Add(lineage);
            }
            if (m.Positions.Count > 0) result.Add(m);
        }
        return result;
    }

    private static async Task AddEqualityDeletesAsync(Table table, SnapshotProducer producer, Matcher matcher, CancellationToken ct)
    {
        var key = matcher.Key ?? throw new InvalidOperationException("Equality deletes need a key");
        var schema = table.Schema;
        var m = table.Metadata;
        // An equality delete applies only to data files of its own spec and partition (unless unpartitioned), so for
        // every spec that still has data, the partition of each existing row with a given key must follow from the
        // key itself; one delete file is written per (spec, partition).
        var live = m.CurrentSnapshot is { } current
            ? (await TableScan.ManifestsAsync(table, current, ct)).Where(mf => mf.Content == ManifestContent.Data).Select(mf => mf.PartitionSpecId).ToHashSet()
            : [];
        live.Add(m.DefaultSpecId);
        var specs = live.Select(m.SpecById).ToList();
        var keyIds = key.Fields.Select(f => f.Id).ToHashSet();
        foreach (var spec in specs)
            if (spec.Fields.Any(f => f.Transform is not Transforms.VoidTransform && !keyIds.Contains(f.SourceId)))
                throw new ValidationException($"Equality deletes require every partition source column to be part of the key (spec {spec.SpecId}); use PositionDeletes or CopyOnWrite");

        var positions = key.Fields.Select((f, i) => (f.Id, i)).ToDictionary(x => x.Id, x => x.i);
        var dataDir = m.Property("write.data.path", $"{table.Location}/data").TrimEnd('/');
        var files = new List<DataFile>();
        foreach (var spec in specs.Count > 1 && specs.Any(sp => sp.IsUnpartitioned) ? [specs.First(sp => sp.IsUnpartitioned)] : specs)
        {
            // An unpartitioned delete file applies to every partition of every spec, so one is enough.
            foreach (var group in matcher.Keys!.Tuples.GroupBy(t => spec.Partition(schema, id => t[positions[id]]), PartitionComparer.Instance))
            {
                var location = $"{dataDir}/{Guid.NewGuid():N}-eq-deletes.parquet";
                files.Add(await EqualityDeleteFiles.WriteAsync(table.IO, location, spec.SpecId, group.Key, key.Fields, group, ct));
            }
        }
        producer.AddDeleteFiles(files);
    }

    private sealed class ValueEquality : IEqualityComparer<object?>
    {
        public static readonly ValueEquality Instance = new();
        public new bool Equals(object? a, object? b) => a is byte[] x && b is byte[] y ? x.AsSpan().SequenceEqual(y) : object.Equals(a, b);
        public int GetHashCode(object? o) => o is byte[] b ? b.Length : o?.GetHashCode() ?? 0;
    }

    private sealed class PartitionComparer : IEqualityComparer<object?[]>
    {
        public static readonly PartitionComparer Instance = new();
        public bool Equals(object?[]? a, object?[]? b) => a!.Length == b!.Length && a.Zip(b).All(p => Equals(p.First, p.Second));
        public int GetHashCode(object?[] o) => o.Aggregate(17, (h, v) => HashCode.Combine(h, v));
    }
}
