using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.EntityFrameworkCore.Metadata;
using Iceberg.Net.Linq;
using Iceberg.Net.Linq.Query;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.EntityFrameworkCore.Storage;

/// <summary>
/// <c>SaveChanges</c>: the added, modified and deleted entities of each table become one MERGE by primary key, so
/// each table gets exactly one snapshot. Several tables are staged and committed in one catalog transaction when the
/// catalog supports it, and otherwise one after another (see docs/efcore.md for what that means for atomicity).
/// </summary>
internal sealed class IcebergDatabase(DatabaseDependencies dependencies, IcebergSession session) : Database(dependencies)
{
    public override int SaveChanges(IList<IUpdateEntry> entries) => Blocking.Run(() => SaveChangesAsync(entries));

    public override async Task<int> SaveChangesAsync(IList<IUpdateEntry> entries, CancellationToken ct = default)
    {
        var groups = entries.Where(e => e.EntityState is EntityState.Added or EntityState.Modified or EntityState.Deleted)
            .GroupBy(e => IcebergEntityMapping.For(e.EntityType))
            .OrderBy(g => g.Key.TableName, StringComparer.Ordinal)
            .ToList();
        if (groups.Count > 1)
        {
            if (session.Catalog.SupportsMultiTableTransactions) return await SaveAtomicallyAsync(groups, ct);
            if (session.Options.RequireAtomicSaveChanges)
                throw new NotSupportedException($"SaveChanges spans {groups.Count} tables ({string.Join(", ", groups.Select(g => g.Key.TableName))}), " +
                    $"but the {session.Catalog.Name} catalog cannot commit several tables atomically, and RequireAtomicSaveChanges is set. Nothing was saved");
        }

        var committed = new List<(IUpdateEntry Entry, IReadOnlyList<(IProperty, object)> Generated)>();
        var count = 0;
        foreach (var group in groups)
        {
            try
            {
                var save = await PrepareTableAsync(group.Key, group.ToList(), ct);
                try
                {
                    await TableMutations.MergeAsync(save.Table, save.Source, save.KeyColumns, save.Clauses, session.Options.DeleteStrategy, ct);
                }
                catch (Exception e) when (Translate(e, save) is { } translated)
                {
                    throw translated;
                }
                session.Forget(save.Table);
                committed.AddRange(save.Entries.Select(e => (e, save.Generated.TryGetValue(e, out var g) ? g : (IReadOnlyList<(IProperty, object)>)[])));
                count += save.Entries.Count;
            }
            catch when (committed.Count > 0)
            {
                // Earlier tables are committed and cannot be rolled back: mark their entities saved, so a retry of
                // SaveChanges only replays what failed.
                AcceptCommitted(committed);
                throw;
            }
        }
        foreach (var (entry, generated) in committed)
            foreach (var (property, value) in generated) entry.SetStoreGeneratedValue(property, value);
        return count;
    }

    /// <summary>
    /// Stage every table's MERGE, then commit them in one catalog transaction: all tables change or none does. After a
    /// lost race every MERGE is planned again, so its key and concurrency-token checks run against the state the
    /// transaction commits on. Nothing is accepted before the transaction succeeds.
    /// </summary>
    private async Task<int> SaveAtomicallyAsync(List<IGrouping<IcebergEntityMapping, IUpdateEntry>> groups, CancellationToken ct)
    {
        var saves = new List<TableSave>();
        var staged = new List<StagedCommit>();
        try
        {
            foreach (var group in groups)
            {
                var save = await PrepareTableAsync(group.Key, group.ToList(), ct);
                saves.Add(save);
                try
                {
                    staged.Add(await TableMutations.StageMergeAsync(save.Table, save.Source, save.KeyColumns, save.Clauses, session.Options.DeleteStrategy, ct));
                }
                catch (Exception e) when (Translate(e, save) is { } translated)
                {
                    throw translated;
                }
            }
        }
        catch
        {
            foreach (var s in staged) await s.DiscardAsync(CancellationToken.None);
            throw;
        }

        try
        {
            await StagedCommit.CommitAllAsync(staged, ct);
        }
        catch (Exception e) when (Translate(e, saves) is { } translated)
        {
            throw translated;
        }
        foreach (var save in saves)
        {
            session.Forget(save.Table);
            foreach (var (entry, generated) in save.Generated)
                foreach (var (property, value) in generated) entry.SetStoreGeneratedValue(property, value);
        }
        return saves.Sum(s => s.Entries.Count);
    }

    /// <summary>A core exception as EF Core's (null: let it through, e.g. one the MERGE's own checks threw).</summary>
    private static Exception? Translate(Exception e, params TableSave[] saves)
    {
        var what = string.Join(", ", saves.Select(s => $"{s.Mapping.EntityType.DisplayName()} to {s.Table.Identifier}"));
        var entries = saves.SelectMany(s => s.Entries).ToList();
        return e switch
        {
            CommitFailedException => new DbUpdateConcurrencyException($"Commit of {what} kept conflicting with concurrent commits: {e.Message}", e, entries),
            ValidationException or CommitStateUnknownException or ArgumentException => new DbUpdateException($"Saving {what} failed: {e.Message}", e, entries),
            _ => null,
        };
    }

    private static Exception? Translate(Exception e, List<TableSave> saves) => Translate(e, saves.ToArray());

    private static void AcceptCommitted(IEnumerable<(IUpdateEntry Entry, IReadOnlyList<(IProperty, object)> Generated)> committed)
    {
        foreach (var (entry, generated) in committed)
        {
            var ee = entry.ToEntityEntry();
            if (ee.State == EntityState.Deleted)
            {
                ee.State = EntityState.Detached;
                continue;
            }
            foreach (var (property, value) in generated) ee.Property(property).CurrentValue = value;
            ee.OriginalValues.SetValues(ee.CurrentValues);
            ee.State = EntityState.Unchanged;
        }
    }

    private enum Change { Insert, Update, Delete, Replace }

    private sealed record Item(Change Change, IReadOnlyList<IUpdateEntry> Entries, object?[] Row)
    {
        /// <summary>The entry whose original values describe the row expected in the table (the deleted one of a replace).</summary>
        public IUpdateEntry Expected => Entries.FirstOrDefault(e => e.EntityState == EntityState.Deleted) ?? Entries[0];
    }

    /// <summary>One table's part of a save: the MERGE's source rows, key and clauses, and the row versions it writes.</summary>
    private sealed record TableSave(IcebergEntityMapping Mapping, Table Table, IReadOnlyList<IUpdateEntry> Entries, List<object?[]> Source,
        List<string> KeyColumns, Action<MergeBuilder> Clauses, Dictionary<IUpdateEntry, IReadOnlyList<(IProperty, object)>> Generated);

    /// <summary>Load the table and build its MERGE. Its clauses make every check, so they hold for the state the commit lands on.</summary>
    private async Task<TableSave> PrepareTableAsync(IcebergEntityMapping mapping, List<IUpdateEntry> entries, CancellationToken ct)
    {
        if (mapping.KeyProperties.Count == 0)
            throw new InvalidOperationException($"{mapping.EntityType.DisplayName()} has no key; keyless entity types are read-only");
        var table = await session.LoadTableAsync(mapping, ct);
        var schema = table.Schema;
        var members = mapping.Members(table).ToDictionary(m => m.Member.Name);
        var position = schema.Columns.Select((c, i) => (c.Id, i)).ToDictionary(x => x.Id, x => x.i);
        int Pos(IReadOnlyProperty p) => position[members[p.Name].Field.Id];
        var keyPositions = mapping.KeyProperties.Select(Pos).ToArray();
        var keyColumns = mapping.KeyProperties.Select(p => members[p.Name].Field.Name).ToList();
        var mappedPositions = mapping.Properties.Select(Pos).ToArray();
        var tokens = mapping.Properties.Cast<IProperty>().Where(p => p.IsConcurrencyToken).ToList();
        var generatedTokens = tokens.Where(p => p.ValueGenerated == ValueGenerated.OnAddOrUpdate).ToList();
        foreach (var g in generatedTokens)
            if (ValueConversionsUnderlying(g.ClrType) is not (TypeCodeLong or TypeCodeInt))
                throw new NotSupportedException($"{mapping.EntityType.DisplayName()}.{g.Name}: generated concurrency tokens (row versions) must be int or long; Iceberg has no rowversion type");

        // One source row per key: a deleted entity and an added one with the same key replace the row.
        var generated = new Dictionary<IUpdateEntry, IReadOnlyList<(IProperty, object)>>();
        var byKey = new Dictionary<object?[], List<(IUpdateEntry Entry, object?[] Row)>>(RowComparer.Instance);
        foreach (var entry in entries)
        {
            var row = mapping.ToRow(table, entry.ToEntityEntry().Entity);
            if (generatedTokens.Count > 0 && entry.EntityState != EntityState.Deleted)
            {
                var values = new List<(IProperty, object)>();
                foreach (var g in generatedTokens)
                {
                    var next = entry.EntityState == EntityState.Added ? 1L : Convert.ToInt64(entry.GetOriginalValue(g) ?? 0L) + 1;
                    object value = ValueConversionsUnderlying(g.ClrType) == TypeCodeInt ? checked((int)next) : next;
                    row[Pos(g)] = members[g.Name].ToIceberg(value);
                    values.Add((g, value));
                }
                generated[entry] = values;
            }
            var key = keyPositions.Select(p => row[p]).ToArray();
            if (!byKey.TryGetValue(key, out var list)) byKey[key] = list = [];
            list.Add((entry, row));
        }
        var items = new Dictionary<object?[], Item>(ReferenceEqualityComparer.Instance);
        foreach (var (key, list) in byKey)
        {
            var states = list.Select(x => x.Entry.EntityState).OrderBy(s => s).ToList();
            Item item = states switch
            {
                // EF Core hands over a deleted entity replaced by an added one with the same key as one entry.
                [EntityState.Added] when list[0].Entry.SharedIdentityEntry is { } replaced => new(Change.Replace, [list[0].Entry, replaced], list[0].Row),
                [EntityState.Added] => new(Change.Insert, [list[0].Entry], list[0].Row),
                [EntityState.Modified] => new(Change.Update, [list[0].Entry], list[0].Row),
                [EntityState.Deleted] => new(Change.Delete, [list[0].Entry], list[0].Row),
                [EntityState.Deleted, EntityState.Added] or [EntityState.Added, EntityState.Deleted] =>
                    new(Change.Replace, list.Select(x => x.Entry).ToList(), list.First(x => x.Entry.EntityState == EntityState.Added).Row),
                _ => throw new DbUpdateException($"{mapping.EntityType.DisplayName()}: {list.Count} entities with key [{string.Join(", ", key)}] are being saved",
                    list.Select(x => x.Entry).ToList()),
            };
            items[item.Row] = item;
        }

        var expectsRows = items.Values.Any(i => i.Change != Change.Insert);
        long? required = null;
        if (session.Options.RequireUnchangedSnapshot && expectsRows && session.TryGetFirstRead(table, out var read))
        {
            required = read;
            if (table.Metadata.CurrentSnapshotId != read) throw Moved(mapping, table, read, items.Values);
        }

        // Checked inside the MERGE, so they hold for the table state the commit lands on (the core re-plans and
        // re-checks on every conflicting commit).
        void Expect(Item item, object?[] target)
        {
            if (item.Change == Change.Insert)
                throw new DbUpdateException($"Cannot insert {mapping.EntityType.DisplayName()} with key [{string.Join(", ", keyPositions.Select(p => target[p]))}]: " +
                    $"table {table.Identifier} already has a row with that key", item.Entries);
            if (required is not null && table.Metadata.CurrentSnapshotId != required) throw Moved(mapping, table, required, [item]);
            foreach (var token in tokens)
            {
                var original = members[token.Name].ToIceberg(item.Expected.GetOriginalValue(token));
                if (!RowComparer.ValueEquals(original, target[Pos(token)]))
                    throw new DbUpdateConcurrencyException(
                        $"{mapping.EntityType.DisplayName()} with key [{string.Join(", ", keyPositions.Select(p => target[p]))}]: concurrency token {token.Name} " +
                        "changed since the entity was read", item.Entries);
            }
        }

        var source = items.Keys.ToList();
        void Clauses(MergeBuilder m) => m
            .WhenMatchedDelete((target, src) =>
            {
                var item = items[src];
                Expect(item, target);
                return item.Change == Change.Delete;
            })
            .WhenMatchedUpdate((target, src) =>
            {
                // Columns the model does not map keep the table's values.
                var updated = (object?[])target.Clone();
                foreach (var p in mappedPositions) updated[p] = src[p];
                return updated;
            })
            .WhenNotMatchedInsert(src =>
            {
                var item = items[src];
                if (item.Change == Change.Insert) return true;
                throw new DbUpdateConcurrencyException(
                    $"{mapping.EntityType.DisplayName()} with key [{string.Join(", ", keyPositions.Select(p => src[p]))}] was {(item.Change == Change.Update ? "modified" : "deleted")} " +
                    $"in the context but table {table.Identifier} has no row with that key (deleted by another writer?)", item.Entries);
            });
        return new TableSave(mapping, table, entries, source, keyColumns, Clauses, generated);
    }

    private static DbUpdateConcurrencyException Moved(IcebergEntityMapping mapping, Table table, long? read, IEnumerable<Item> items) =>
        new($"Table {table.Identifier} ({mapping.EntityType.DisplayName()}) moved from snapshot {read?.ToString() ?? "none"} (read by this context) " +
            $"to {table.Metadata.CurrentSnapshotId}; RequireUnchangedSnapshot is set", items.SelectMany(i => i.Entries).ToList());

    private const int TypeCodeInt = 1, TypeCodeLong = 2;

    private static int ValueConversionsUnderlying(Type t) => (Nullable.GetUnderlyingType(t) ?? t) switch
    {
        var u when u == typeof(int) => TypeCodeInt,
        var u when u == typeof(long) => TypeCodeLong,
        _ => 0,
    };
}

/// <summary>Structural equality for rows and key tuples of physical values (byte arrays by content).</summary>
internal sealed class RowComparer : IEqualityComparer<object?[]>
{
    public static readonly RowComparer Instance = new();

    public static bool ValueEquals(object? a, object? b) => a is byte[] x && b is byte[] y ? x.AsSpan().SequenceEqual(y) : Equals(a, b);

    public bool Equals(object?[]? a, object?[]? b) => a!.Length == b!.Length && a.Zip(b).All(p => ValueEquals(p.First, p.Second));

    public int GetHashCode(object?[] o) => o.Aggregate(17, (h, v) => HashCode.Combine(h, v is byte[] b ? b.Length : v?.GetHashCode() ?? 0));
}
