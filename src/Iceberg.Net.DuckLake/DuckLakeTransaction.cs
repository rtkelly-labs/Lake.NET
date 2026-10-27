using System.Data.Common;
using System.Globalization;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

public sealed record DuckLakeColumnDefinition(string Name, DuckLakeType Type, bool Nullable = true);

/// <summary>A concurrent snapshot made a change the spec's conflict rules say this commit cannot be retried over.</summary>
public sealed class DuckLakeConflictException(string message) : Exception(message);

/// <summary>
/// The catalog database failed while committing, so whether the snapshot was written is unknown. Files written for
/// the transaction are kept: if the commit did land, they are part of the table.
/// </summary>
public sealed class DuckLakeCommitStateUnknownException(string message, Exception inner) : Exception(message, inner);

/// <summary>
/// One DuckLake snapshot in the making (spec "Writing Data"). Operations are staged against the snapshot that was
/// latest when the transaction started (its base): data files and delete files are written to storage as they are
/// staged, and catalog rows are written by <see cref="CommitAsync"/> in one database transaction that also inserts the
/// new <c>ducklake_snapshot</c> row. IDs (<c>next_catalog_id</c>, <c>next_file_id</c>, <c>next_row_id</c>) are taken
/// at commit, so losing the race for a snapshot ID to a non-conflicting snapshot costs a metadata retry only.
/// <para>
/// Limits: a delete must be the first data operation on its table in the transaction and the table must not have
/// been created or altered in it (an insert may follow, which is how UPDATE is expressed).
/// </para>
/// </summary>
public sealed class DuckLakeTransaction : IAsyncDisposable
{
    private readonly DuckLakeCatalog _catalog;
    private readonly List<Op> _ops = [];
    private readonly List<string> _written = [];
    private readonly Dictionary<(string Schema, string Table), PendingTable> _tables = [];
    private readonly Dictionary<string, PendingSchema> _schemas = new(StringComparer.Ordinal);
    private long? _base;
    private bool _finished;

    internal DuckLakeTransaction(DuckLakeCatalog catalog, string? author, string? commitMessage)
    {
        _catalog = catalog;
        Author = author;
        CommitMessage = commitMessage;
    }

    public string? Author { get; set; }
    public string? CommitMessage { get; set; }
    public string? CommitExtraInfo { get; set; }

    /// <summary>The snapshot staged operations were planned against (set by the first operation).</summary>
    public long? BaseSnapshotId => _base;

    // ---- staged state ----------------------------------------------------------------------------------------------

    private sealed class PendingSchema
    {
        public long? SchemaId;
        public required string Name;
        public required string Location;
        public bool Created, Dropped;
    }

    private sealed class PendingTable
    {
        public long? TableId; // null until commit for tables created in this transaction
        public required PendingSchema Schema;
        public required string Name;
        public required string Location;
        public required List<DuckLakeColumn> Columns;
        public long NextColumnId;
        public IReadOnlyList<DuckLakePartitionKey> PartitionKeys = [];
        public bool Created, Dropped, Altered, PartitionChanged, Inserted, Deleted;
        public Guid Uuid = Guid.NewGuid();
        public long? BaseTableId => Created ? null : TableId;
    }

    private abstract record Op;
    private sealed record CreateSchemaOp(PendingSchema Schema) : Op;
    private sealed record DropSchemaOp(PendingSchema Schema) : Op;
    private sealed record CreateTableOp(PendingTable Table) : Op;
    private sealed record DropTableOp(PendingTable Table) : Op;
    private sealed record AddColumnOp(PendingTable Table, DuckLakeColumn Column, long Order) : Op;
    private sealed record EndColumnsOp(PendingTable Table, IReadOnlyList<long> ColumnIds) : Op;
    private sealed record RenameColumnOp(PendingTable Table, DuckLakeColumn Column, string NewName, long Order, long? Parent) : Op;
    private sealed record SetPartitionOp(PendingTable Table, IReadOnlyList<DuckLakePartitionKey> Keys) : Op;
    private sealed record InsertOp(PendingTable Table, WrittenFile File) : Op;
    private sealed record DeleteOp(PendingTable Table, IReadOnlyList<FileDelete> Files, IReadOnlyDictionary<string, IReadOnlyList<long>> Inlined, long Count) : Op;

    private sealed record WrittenFile(string RelativePath, long RecordCount, long Size, long FooterSize, IReadOnlyList<DuckLakeColumnStats> Stats,
        IReadOnlyList<string?> PartitionValues);

    /// <summary>A data file's new delete state: a replacement delete file, or the whole file removed.</summary>
    private sealed record FileDelete(long DataFileId, long? OldDeleteFileId, long Deleted, long Total, WrittenDelete? NewFile);

    private sealed record WrittenDelete(string RelativePath, long Count, long Size, long FooterSize);

    private async Task<DbConnection> BeginAsync(CancellationToken ct)
    {
        if (_finished) throw new InvalidOperationException("The transaction has already been committed or disposed");
        var c = await _catalog.ConnectAsync(ct);
        _base ??= (await DuckLakeCatalog.LatestSnapshotAsync(c, null, ct)).SnapshotId;
        return c;
    }

    private async Task<PendingSchema> SchemaAsync(DbConnection c, string name, CancellationToken ct)
    {
        if (_schemas.TryGetValue(name, out var pending))
            return pending.Dropped ? throw new DuckLakeNotFoundException($"Schema {name} is dropped in this transaction") : pending;
        var row = DuckLakeCatalog.Find(await DuckLakeCatalog.SchemasAsync(c, null, _base!.Value, ct), s => s.Name, name, "schema");
        pending = new PendingSchema { SchemaId = row.SchemaId, Name = row.Name, Location = DuckLakeCatalog.Resolve(_catalog.DataPath, row.Path, row.Relative) };
        return _schemas[row.Name] = _schemas[name] = pending;
    }

    private async Task<PendingTable> TableAsync(DbConnection c, string schema, string table, CancellationToken ct)
    {
        var sc = await SchemaAsync(c, schema, ct);
        if (_tables.TryGetValue((sc.Name, table), out var pending))
            return pending.Dropped ? throw new DuckLakeNotFoundException($"Table {schema}.{table} is dropped in this transaction") : pending;
        var loaded = await _catalog.LoadTableAsync(c, null, sc.Name, table, _base!.Value, ct);
        var maxColumn = await Db.QueryAsync(c, null, "SELECT max(column_id) FROM ducklake_column WHERE table_id = @p0", r => Db.Long(r, 0), ct, null, loaded.TableId);
        pending = new PendingTable
        {
            TableId = loaded.TableId,
            Schema = sc,
            Name = loaded.Name,
            Location = loaded.Location,
            Columns = [.. loaded.Columns],
            NextColumnId = (maxColumn.FirstOrDefault() ?? 0) + 1,
            PartitionKeys = loaded.PartitionKeys,
            Uuid = loaded.TableUuid,
        };
        return _tables[(sc.Name, loaded.Name)] = _tables[(sc.Name, table)] = pending;
    }

    // ---- DDL -------------------------------------------------------------------------------------------------------

    public async Task CreateSchemaAsync(string name, CancellationToken ct = default)
    {
        await using var c = await BeginAsync(ct);
        var live = await DuckLakeCatalog.SchemasAsync(c, null, _base!.Value, ct);
        if (live.Any(s => s.Name == name) || _schemas.TryGetValue(name, out var p) && !p.Dropped)
            throw new InvalidOperationException($"Schema {name} already exists");
        var schema = new PendingSchema { Name = name, Location = _catalog.DataPath + PathSegment(name), Created = true };
        _schemas[name] = schema;
        _ops.Add(new CreateSchemaOp(schema));
    }

    /// <summary>Drop an empty schema (spec: "DROP SCHEMA is only allowed on empty schemas").</summary>
    public async Task DropSchemaAsync(string name, CancellationToken ct = default)
    {
        await using var c = await BeginAsync(ct);
        var schema = await SchemaAsync(c, name, ct);
        if (schema.Created) throw new InvalidOperationException($"Schema {name} was created in this transaction");
        var tables = (await DuckLakeCatalog.TablesAsync(c, null, _base!.Value, ct)).Where(t => t.SchemaId == schema.SchemaId)
            .Where(t => !(_tables.TryGetValue((schema.Name, t.Name), out var pt) && pt.Dropped)).ToList();
        if (tables.Count > 0 || _tables.Values.Any(t => t.Schema == schema && !t.Dropped))
            throw new InvalidOperationException($"Schema {name} is not empty");
        schema.Dropped = true;
        _ops.Add(new DropSchemaOp(schema));
    }

    public async Task CreateTableAsync(string schema, string name, IReadOnlyList<DuckLakeColumnDefinition> columns, CancellationToken ct = default)
    {
        if (columns.Count == 0) throw new ArgumentException("A table needs at least one column", nameof(columns));
        if (columns.Select(x => x.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count)
            throw new ArgumentException("Column names must be unique", nameof(columns));
        await using var c = await BeginAsync(ct);
        var sc = await SchemaAsync(c, schema, ct);
        if (!sc.Created && (await DuckLakeCatalog.TablesAsync(c, null, _base!.Value, ct)).Any(t => t.SchemaId == sc.SchemaId && t.Name == name)
            || _tables.TryGetValue((sc.Name, name), out var existing) && !existing.Dropped)
            throw new InvalidOperationException($"Table {schema}.{name} already exists");
        long next = 1;
        var assigned = columns.Select(d => DuckLakeColumn.Assign(d.Name, d.Type, d.Nullable, ref next)).ToList();
        var table = new PendingTable
        {
            Schema = sc, Name = name, Location = sc.Location + PathSegment(name), Columns = assigned, NextColumnId = next, Created = true,
        };
        _tables[(sc.Name, name)] = table;
        _ops.Add(new CreateTableOp(table));
    }

    public async Task DropTableAsync(string schema, string name, CancellationToken ct = default)
    {
        await using var c = await BeginAsync(ct);
        var t = await TableAsync(c, schema, name, ct);
        if (t.Created) throw new InvalidOperationException($"Table {schema}.{name} was created in this transaction");
        t.Dropped = true;
        _ops.Add(new DropTableOp(t));
    }

    public async Task AddColumnAsync(string schema, string table, DuckLakeColumnDefinition column, CancellationToken ct = default)
    {
        await using var c = await BeginAsync(ct);
        var t = await AlterableAsync(c, schema, table, ct);
        if (t.Columns.Any(x => string.Equals(x.Name, column.Name, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException($"Column {column.Name} already exists");
        if (!column.Nullable) throw new NotSupportedException("Adding a NOT NULL column needs a default value, which is not supported");
        var next = t.NextColumnId;
        var added = DuckLakeColumn.Assign(column.Name, column.Type, true, ref next);
        var order = next; // after every existing column_order, which never exceeds the IDs handed out
        t.NextColumnId = next;
        t.Columns.Add(added);
        _ops.Add(new AddColumnOp(t, added, order));
    }

    public async Task DropColumnAsync(string schema, string table, string column, CancellationToken ct = default)
    {
        await using var c = await BeginAsync(ct);
        var t = await AlterableAsync(c, schema, table, ct);
        var col = Column(t, column);
        if (t.Columns.Count == 1) throw new InvalidOperationException("Cannot drop the last column");
        if (t.PartitionKeys.Any(k => col.SelfAndDescendants().Any(x => x.Id == k.ColumnId)))
            throw new InvalidOperationException($"Column {column} is a partition key");
        t.Columns.Remove(col);
        _ops.Add(new EndColumnsOp(t, col.SelfAndDescendants().Select(x => x.Id).ToList()));
    }

    /// <summary>Rename a top-level column: a new version of its <c>ducklake_column</c> row, same <c>column_id</c>.</summary>
    public async Task RenameColumnAsync(string schema, string table, string column, string newName, CancellationToken ct = default)
    {
        await using var c = await BeginAsync(ct);
        var t = await AlterableAsync(c, schema, table, ct);
        var col = Column(t, column);
        if (t.Columns.Any(x => x != col && string.Equals(x.Name, newName, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException($"Column {newName} already exists");
        var orders = await Db.QueryAsync(c, null, $"SELECT column_order FROM ducklake_column WHERE table_id = @p0 AND column_id = @p1 AND {Db.Live}",
            r => Db.Long(r, 0), ct, _base, t.TableId, col.Id);
        var renamed = col with { Name = newName };
        t.Columns[t.Columns.IndexOf(col)] = renamed;
        _ops.Add(new RenameColumnOp(t, col, newName, orders.FirstOrDefault() ?? col.Id, null));
    }

    /// <summary>
    /// Set the partition keys new data is written with (spec <c>ducklake_partition_info</c>): transforms <c>identity</c>,
    /// <c>year</c>, <c>month</c>, <c>day</c>, <c>hour</c>. An empty list removes partitioning.
    /// </summary>
    public async Task SetPartitioningAsync(string schema, string table, IReadOnlyList<(string Column, string Transform)> keys, CancellationToken ct = default)
    {
        await using var c = await BeginAsync(ct);
        var t = await AlterableAsync(c, schema, table, ct, allowCreated: true);
        var resolved = keys.Select(k =>
        {
            var col = Column(t, k.Column);
            if (col.Type is not DuckLakePrimitive p) throw new NotSupportedException($"Cannot partition by nested column {k.Column}");
            DuckLakePartitioning.Validate(p, k.Transform);
            return new DuckLakePartitionKey(col.Id, k.Transform);
        }).ToList();
        t.PartitionKeys = resolved;
        t.PartitionChanged = true;
        _ops.Add(new SetPartitionOp(t, resolved));
    }

    private async Task<PendingTable> AlterableAsync(DbConnection c, string schema, string table, CancellationToken ct, bool allowCreated = false)
    {
        var t = await TableAsync(c, schema, table, ct);
        if (t.Inserted || t.Deleted) throw new InvalidOperationException($"Alter {schema}.{table} before writing data to it in the same transaction");
        if (t.Created && !allowCreated) throw new InvalidOperationException($"Table {schema}.{table} is created in this transaction: define its columns there");
        if (!t.Created) t.Altered = true;
        return t;
    }

    private static DuckLakeColumn Column(PendingTable t, string name) =>
        DuckLakeCatalog.Find(t.Columns, x => x.Name, name, $"column of {t.Schema.Name}.{t.Name}");

    /// <summary>A path segment for a schema or table directory: the name, percent-encoded where it is not a safe file name character.</summary>
    private static string PathSegment(string name) => Uri.EscapeDataString(name) + "/";

    // ---- data ------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Write <paramref name="rows"/> (values in table column order, see <see cref="DuckLakeValues"/>) as Parquet files —
    /// one per partition when the table is partitioned — and stage their registration.
    /// </summary>
    public async Task InsertAsync(string schema, string table, IEnumerable<object?[]> rows, CancellationToken ct = default)
    {
        PendingTable t;
        await using (var c = await BeginAsync(ct)) t = await TableAsync(c, schema, table, ct);
        var columns = t.Columns;
        var normalized = rows.Select(r =>
        {
            if (r.Length != columns.Count) throw new ArgumentException($"Row has {r.Length} values; {t.Schema.Name}.{t.Name} has {columns.Count} columns");
            var values = new object?[r.Length];
            for (var i = 0; i < r.Length; i++)
            {
                values[i] = DuckLakeValues.Normalize(columns[i].Type, r[i], columns[i].Name);
                if (values[i] is null && !columns[i].Nullable) throw new ArgumentException($"Column {columns[i].Name} is NOT NULL");
            }
            return values;
        }).ToList();
        if (normalized.Count == 0) return;
        var icebergSchema = DuckLakeTypes.ToIceberg(columns);
        var groups = t.PartitionKeys.Count == 0
            ? [(Values: (IReadOnlyList<string?>)[], Rows: normalized)]
            : DuckLakePartitioning.Split(columns, t.PartitionKeys, normalized);
        foreach (var (partitionValues, groupRows) in groups)
        {
            var directory = t.PartitionKeys.Count == 0 ? "" : DuckLakePartitioning.HivePath(columns, t.PartitionKeys, partitionValues);
            var relative = $"{directory}ducklake-{Guid.CreateVersion7()}.parquet";
            var location = t.Location + relative;
            _written.Add(location);
            var file = await ParquetFiles.WriteAsync(_catalog.IO, location, icebergSchema, 0, [], groupRows, _catalog.Options.Parquet, ct);
            var footer = await DuckLakeDeleteFiles.FooterSizeAsync(_catalog.IO, location, file.FileSizeInBytes, ct);
            var stats = new StatsCollector(columns);
            foreach (var row in groupRows) stats.Add(row);
            _ops.Add(new InsertOp(t, new WrittenFile(relative, groupRows.Count, file.FileSizeInBytes, footer, stats.Result(file.ColumnSizes), partitionValues)));
        }
        t.Inserted = true;
    }

    /// <summary>
    /// Delete the rows matching <paramref name="filter"/> (merge-on-read: spec "DELETE"). For each affected data file a
    /// new delete file holding every deleted position (earlier deletes included) replaces the file's current one; a
    /// data file losing all its rows is removed from the table instead. Inlined rows get their <c>end_snapshot</c> set.
    /// Returns the number of rows deleted.
    /// </summary>
    public async Task<long> DeleteAsync(string schema, string table, Expression filter, CancellationToken ct = default)
    {
        PendingTable t;
        DuckLakeTable loaded;
        DuckLakeScanPlan plan;
        await using (var c = await BeginAsync(ct))
        {
            t = await TableAsync(c, schema, table, ct);
            if (t.Created || t.Altered || t.PartitionChanged || t.Inserted || t.Deleted)
                throw new InvalidOperationException($"A delete must be the first change to {schema}.{table} in a transaction");
            loaded = await _catalog.LoadTableAsync(c, null, t.Schema.Name, t.Name, _base!.Value, ct);
            plan = await _catalog.PlanScanAsync(c, null, loaded, null, ct);
        }
        var byFile = new Dictionary<long, List<long>>();
        var inlined = new Dictionary<string, List<long>>();
        await foreach (var row in _catalog.ScanAsync(loaded, filter, ct))
        {
            if (row.DataFileId is { } f) (byFile.TryGetValue(f, out var l) ? l : byFile[f] = []).Add(row.Position);
            else (inlined.TryGetValue(row.InlinedTable!, out var l) ? l : inlined[row.InlinedTable!] = []).Add(row.RowId);
        }
        var deleteRows = await DeleteFileIdsAsync(loaded, ct);
        var files = new List<FileDelete>();
        foreach (var (fileId, positions) in byFile)
        {
            var task = plan.Files.Single(x => x.DataFileId == fileId);
            var all = new HashSet<long>(positions);
            if (await _catalog.DeletedPositionsAsync(plan, task, loaded.SnapshotId, ct) is { } earlier) all.UnionWith(earlier);
            deleteRows.TryGetValue(fileId, out var oldId);
            WrittenDelete? written = null;
            if (all.Count < task.RecordCount)
            {
                var relative = $"ducklake-{Guid.CreateVersion7()}-delete.parquet";
                var location = t.Location + relative;
                _written.Add(location);
                var (size, footer) = await DuckLakeDeleteFiles.WriteAsync(_catalog.IO, location, task.Location, all, ct);
                written = new WrittenDelete(relative, all.Count, size, footer);
            }
            files.Add(new FileDelete(fileId, task.DeleteFileLocation is null ? null : oldId, positions.Count, task.RecordCount, written));
        }
        var count = byFile.Values.Sum(p => (long)p.Count) + inlined.Values.Sum(p => (long)p.Count);
        if (count == 0) return 0;
        t.Deleted = true;
        _ops.Add(new DeleteOp(t, files, inlined.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<long>)kv.Value), count));
        return count;
    }

    private async Task<Dictionary<long, long>> DeleteFileIdsAsync(DuckLakeTable table, CancellationToken ct)
    {
        await using var c = await _catalog.ConnectAsync(ct);
        return (await DuckLakeCatalog.DeleteFilesAsync(c, null, table.TableId, table.SnapshotId, ct)).ToDictionary(kv => kv.Key, kv => kv.Value.Id);
    }

    /// <summary>UPDATE as the spec defines it: a delete of the matching rows and an insert of their new versions, in one snapshot.</summary>
    public async Task<long> UpdateAsync(string schema, string table, Expression filter, Func<object?[], object?[]> update, CancellationToken ct = default)
    {
        DuckLakeTable loaded;
        await using (var c = await BeginAsync(ct))
        {
            var t = await TableAsync(c, schema, table, ct);
            loaded = await _catalog.LoadTableAsync(c, null, t.Schema.Name, t.Name, _base!.Value, ct);
        }
        var matched = await _catalog.ReadAsync(loaded, filter, ct);
        var deleted = await DeleteAsync(schema, table, filter, ct);
        if (deleted != matched.Count) throw new InvalidOperationException($"Matched {matched.Count} rows but staged {deleted} deletes");
        await InsertAsync(schema, table, matched.Select(r => update((object?[])r.Clone())), ct);
        return deleted;
    }

    // ---- commit ----------------------------------------------------------------------------------------------------

    /// <summary>
    /// Write the catalog rows of every staged operation and the new snapshot in one database transaction. A lost race
    /// for the snapshot ID is retried after checking the winners' <c>changes_made</c> against this transaction's
    /// (spec "Conflict Resolution"); a logical conflict throws <see cref="DuckLakeConflictException"/> and removes the
    /// files this transaction wrote.
    /// </summary>
    public async Task<DuckLakeSnapshot> CommitAsync(CancellationToken ct = default)
    {
        if (_finished) throw new InvalidOperationException("The transaction has already been committed or disposed");
        if (_ops.Count == 0) throw new InvalidOperationException("Nothing to commit");
        _finished = true;
        Exception? last = null;
        for (var attempt = 0; attempt < Math.Max(1, _catalog.Options.MaxCommitAttempts); attempt++)
        {
            DuckLakeSnapshot snapshot;
            await using var c = await _catalog.ConnectAsync(ct);
            await using var tx = await c.BeginTransactionAsync(ct);
            try
            {
                snapshot = await ApplyAsync(c, tx, ct);
            }
            catch (DuckLakeConflictException)
            {
                await tx.RollbackAsync(CancellationToken.None);
                await CleanUpAsync();
                throw;
            }
            catch (DbException e)
            {
                // Lost the race for the snapshot ID (or another unique key): roll back and try again; anything else is
                // a real error, and nothing was committed, so the files go.
                last = e;
                await tx.RollbackAsync(CancellationToken.None);
                if (await SnapshotTakenAsync(e, ct)) continue;
                await CleanUpAsync();
                throw;
            }
            catch
            {
                await tx.RollbackAsync(CancellationToken.None);
                await CleanUpAsync();
                throw;
            }
            try
            {
                await tx.CommitAsync(ct);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                throw new DuckLakeCommitStateUnknownException($"Commit of snapshot {snapshot.SnapshotId} failed; it may or may not have been written", e);
            }
            return snapshot;
        }
        await CleanUpAsync();
        throw new DuckLakeConflictException($"Gave up after {_catalog.Options.MaxCommitAttempts} attempts: {last?.Message}");
    }

    private long? _attemptedSnapshot;

    private async Task<bool> SnapshotTakenAsync(DbException e, CancellationToken ct)
    {
        if (_attemptedSnapshot is not { } id) return false;
        await using var c = await _catalog.ConnectAsync(ct);
        var found = await Db.QueryAsync(c, null, "SELECT snapshot_id FROM ducklake_snapshot WHERE snapshot_id = @p0", r => Db.ReqLong(r, 0), ct, null, id);
        _ = e;
        return found.Count > 0;
    }

    private async Task CleanUpAsync()
    {
        foreach (var location in _written)
        {
            try { await _catalog.IO.DeleteAsync(location, CancellationToken.None); }
            catch (IOException) { /* best effort: an unreferenced file is an orphan, not corruption */ }
        }
        _written.Clear();
    }

    /// <summary>Abandon an uncommitted transaction, deleting the files it wrote.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_finished) return;
        _finished = true;
        await CleanUpAsync();
    }

    private ChangeSet Changes()
    {
        var set = new ChangeSet();
        foreach (var op in _ops)
        {
            switch (op)
            {
                case CreateSchemaOp s: set.CreatedSchemas.Add(s.Schema.Name); break;
                case DropSchemaOp s: set.DroppedSchemas[s.Schema.SchemaId!.Value] = s.Schema.Name; break;
                case CreateTableOp t: set.CreatedTables.Add((t.Table.Schema.Name, t.Table.Schema.SchemaId, t.Table.Name)); break;
                case DropTableOp t: set.DroppedTables.Add(t.Table.TableId!.Value); break;
                case AddColumnOp or EndColumnsOp or RenameColumnOp or SetPartitionOp when Base(op) is { } id: set.AlteredTables.Add(id); break;
                case InsertOp i when i.Table.BaseTableId is { } id: set.InsertedTables.Add(id); break;
                case DeleteOp d: set.DeletedTables.Add(d.Table.TableId!.Value); break;
            }
        }
        return set;

        static long? Base(Op op) => op switch
        {
            AddColumnOp a => a.Table.BaseTableId,
            EndColumnsOp e => e.Table.BaseTableId,
            RenameColumnOp r => r.Table.BaseTableId,
            SetPartitionOp p => p.Table.BaseTableId,
            _ => null,
        };
    }

    private async Task<DuckLakeSnapshot> ApplyAsync(DbConnection c, DbTransaction tx, CancellationToken ct)
    {
        var latest = await DuckLakeCatalog.LatestSnapshotAsync(c, tx, ct);
        var ours = Changes();
        if (latest.SnapshotId != _base)
        {
            var since = await Db.QueryAsync(c, tx, "SELECT snapshot_id, changes_made FROM ducklake_snapshot_changes WHERE snapshot_id > @p0 ORDER BY snapshot_id",
                r => (Db.ReqLong(r, 0), DuckLakeChange.Parse(Db.Str(r, 1))), ct, null, _base);
            foreach (var (id, theirs) in since)
                if (ours.ConflictWith(id, theirs) is { } reason) throw new DuckLakeConflictException(reason);
        }
        var snapshotId = latest.SnapshotId + 1;
        _attemptedSnapshot = snapshotId;
        var nextCatalogId = latest.NextCatalogId;
        var nextFileId = latest.NextFileId;
        var schemaChanged = _ops.Any(o => o is CreateSchemaOp or DropSchemaOp or CreateTableOp or DropTableOp or AddColumnOp or EndColumnsOp or RenameColumnOp or SetPartitionOp);
        var schemaVersion = latest.SchemaVersion + (schemaChanged ? 1 : 0);
        var dialect = _catalog.Options.Dialect;

        // Pending IDs are per attempt: a retry re-assigns them from the new latest snapshot.
        foreach (var s in _schemas.Values.Where(s => s.Created)) s.SchemaId = null;
        foreach (var t in _tables.Values.Where(t => t.Created)) t.TableId = null;
        var versionedTables = new HashSet<PendingTable>();
        _partitionIds.Clear();

        foreach (var op in _ops)
        {
            switch (op)
            {
                case CreateSchemaOp s:
                    s.Schema.SchemaId = nextCatalogId++;
                    await Exec("INSERT INTO ducklake_schema(schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) VALUES (@p0, @p1, @p2, NULL, @p3, @p4, @p5)",
                        s.Schema.SchemaId, dialect.Uuid(Guid.NewGuid()), snapshotId, s.Schema.Name, PathSegment(s.Schema.Name), true);
                    break;
                case DropSchemaOp s:
                    var live = await Db.QueryAsync(c, tx, $"SELECT count(*) FROM ducklake_table WHERE schema_id = @p0 AND {Db.Live}", r => Db.ReqLong(r, 0), ct, latest.SnapshotId, s.Schema.SchemaId);
                    if (live[0] > _ops.Count(o => o is DropTableOp d && d.Table.Schema == s.Schema))
                        throw new DuckLakeConflictException($"Schema {s.Schema.Name} gained tables concurrently");
                    await EndRows("ducklake_schema", "schema_id", s.Schema.SchemaId!.Value);
                    break;
                case CreateTableOp t:
                    var table = t.Table;
                    table.TableId = nextCatalogId++;
                    var schemaId = table.Schema.SchemaId ?? throw new InvalidOperationException($"Schema {table.Schema.Name} has no ID");
                    var clash = await Db.QueryAsync(c, tx, $"SELECT count(*) FROM ducklake_table WHERE schema_id = @p0 AND table_name = @p1 AND {Db.Live}",
                        r => Db.ReqLong(r, 0), ct, latest.SnapshotId, schemaId, table.Name);
                    if (clash[0] > 0) throw new DuckLakeConflictException($"Table {table.Schema.Name}.{table.Name} was created concurrently");
                    await Exec("INSERT INTO ducklake_table(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) VALUES (@p0, @p1, @p2, NULL, @p3, @p4, @p5, @p6)",
                        table.TableId, dialect.Uuid(table.Uuid), snapshotId, schemaId, table.Name, PathSegment(table.Name), true);
                    foreach (var col in table.Columns) await InsertColumn(table, col, null, col.Id);
                    versionedTables.Add(table);
                    break;
                case DropTableOp t:
                    var id = t.Table.TableId!.Value;
                    foreach (var name in new[] { "ducklake_table", "ducklake_partition_info", "ducklake_column", "ducklake_column_tag", "ducklake_data_file", "ducklake_delete_file" })
                        await EndRows(name, "table_id", id);
                    await EndRows("ducklake_tag", "object_id", id);
                    break;
                case AddColumnOp a:
                    await InsertColumn(a.Table, a.Column, null, a.Order);
                    versionedTables.Add(a.Table);
                    break;
                case EndColumnsOp e:
                    foreach (var colId in e.ColumnIds)
                        await Exec("UPDATE ducklake_column SET end_snapshot = @p0 WHERE table_id = @p1 AND column_id = @p2 AND end_snapshot IS NULL", snapshotId, e.Table.TableId, colId);
                    versionedTables.Add(e.Table);
                    break;
                case RenameColumnOp r:
                    await Exec("UPDATE ducklake_column SET end_snapshot = @p0 WHERE table_id = @p1 AND column_id = @p2 AND parent_column IS NULL AND end_snapshot IS NULL",
                        snapshotId, r.Table.TableId, r.Column.Id);
                    await InsertColumnRow(r.Table, r.Column with { Name = r.NewName }, r.Parent, r.Order);
                    versionedTables.Add(r.Table);
                    break;
                case SetPartitionOp p:
                    await EndRows("ducklake_partition_info", "table_id", p.Table.TableId!.Value);
                    if (p.Keys.Count > 0)
                    {
                        var partitionId = nextCatalogId++;
                        await Exec("INSERT INTO ducklake_partition_info(partition_id, table_id, begin_snapshot, end_snapshot) VALUES (@p0, @p1, @p2, NULL)", partitionId, p.Table.TableId, snapshotId);
                        for (var i = 0; i < p.Keys.Count; i++)
                            await Exec("INSERT INTO ducklake_partition_column(partition_id, table_id, partition_key_index, column_id, \"transform\") VALUES (@p0, @p1, @p2, @p3, @p4)",
                                partitionId, p.Table.TableId, i, p.Keys[i].ColumnId, p.Keys[i].Transform);
                        _partitionIds[p.Table] = partitionId;
                    }
                    else _partitionIds[p.Table] = null;
                    versionedTables.Add(p.Table);
                    break;
                case InsertOp i:
                    nextFileId = await InsertFile(i, nextFileId);
                    break;
                case DeleteOp d:
                    nextFileId = await Delete(d, nextFileId);
                    break;
            }
        }

        foreach (var t in versionedTables)
            await Exec("INSERT INTO ducklake_schema_versions(begin_snapshot, schema_version, table_id) VALUES (@p0, @p1, @p2)", snapshotId, schemaVersion, t.TableId);

        // Changes are recorded with the IDs just assigned (a table created here is named, not numbered).
        var final = Changes();
        foreach (var i in _ops.OfType<InsertOp>().Where(i => i.Table.Created)) final.InsertedTables.Add(i.Table.TableId!.Value);
        var changes = string.Join(",", final.Entries());
        var time = DateTimeOffset.UtcNow;
        await Exec("INSERT INTO ducklake_snapshot(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES (@p0, @p1, @p2, @p3, @p4)",
            snapshotId, dialect.TimestampTz(time), schemaVersion, nextCatalogId, nextFileId);
        await Exec("INSERT INTO ducklake_snapshot_changes(snapshot_id, changes_made, author, commit_message, commit_extra_info) VALUES (@p0, @p1, @p2, @p3, @p4)",
            snapshotId, changes, Author, CommitMessage, CommitExtraInfo);
        return new DuckLakeSnapshot(snapshotId, time, schemaVersion, nextCatalogId, nextFileId)
        {
            Changes = DuckLakeChange.Parse(changes), Author = Author, CommitMessage = CommitMessage, CommitExtraInfo = CommitExtraInfo,
        };

        Task<int> Exec(string sql, params object?[] args) => Db.ExecAsync(c, tx, sql, ct, args);

        Task<int> EndRows(string table, string key, long id) =>
            Exec($"UPDATE {table} SET end_snapshot = @p0 WHERE {key} = @p1 AND end_snapshot IS NULL", snapshotId, id);

        async Task InsertColumn(PendingTable table, DuckLakeColumn col, long? parent, long order)
        {
            await InsertColumnRow(table, col, parent, order);
            foreach (var child in col.Children) await InsertColumn(table, child, col.Id, child.Id);
        }

        Task<int> InsertColumnRow(PendingTable table, DuckLakeColumn col, long? parent, long order) =>
            Exec("INSERT INTO ducklake_column(column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, " +
                 "nulls_allowed, parent_column, default_value_type, default_value_dialect) VALUES (@p0, @p1, NULL, @p2, @p3, @p4, @p5, NULL, NULL, @p6, @p7, NULL, NULL)",
                col.Id, snapshotId, table.TableId, order, col.Name, col.Type.ColumnType, col.Nullable, parent);

        async Task<long> InsertFile(InsertOp op, long fileId)
        {
            var t = op.Table;
            var f = op.File;
            var stats = await Db.QueryAsync(c, tx, "SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats WHERE table_id = @p0",
                r => (Db.Long(r, 0) ?? 0, Db.Long(r, 1) ?? 0, Db.Long(r, 2) ?? 0), ct, null, t.TableId);
            long rowIdStart;
            if (stats.Count == 0)
            {
                // No stats row yet: row IDs continue after any rows already in files (DuckDB creates the row lazily too).
                var maxRow = await Db.QueryAsync(c, tx, "SELECT max(row_id_start + record_count) FROM ducklake_data_file WHERE table_id = @p0", r => Db.Long(r, 0), ct, null, t.TableId);
                rowIdStart = maxRow.FirstOrDefault() ?? 0;
                await Exec("INSERT INTO ducklake_table_stats(table_id, record_count, next_row_id, file_size_bytes) VALUES (@p0, @p1, @p2, @p3)",
                    t.TableId, f.RecordCount, rowIdStart + f.RecordCount, f.Size);
            }
            else
            {
                rowIdStart = stats[0].Item2;
                await Exec("UPDATE ducklake_table_stats SET record_count = record_count + @p0, next_row_id = next_row_id + @p0, file_size_bytes = file_size_bytes + @p1 WHERE table_id = @p2",
                    f.RecordCount, f.Size, t.TableId);
            }
            long? partitionId = t.PartitionKeys.Count == 0 ? null
                : _partitionIds.TryGetValue(t, out var pid) ? pid
                : (await Db.QueryAsync(c, tx, $"SELECT partition_id FROM ducklake_partition_info WHERE table_id = @p0 AND {Db.Live}", r => Db.Long(r, 0), ct, latest.SnapshotId, t.TableId)).FirstOrDefault();
            await Exec("INSERT INTO ducklake_data_file(data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, " +
                       "file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max) " +
                       "VALUES (@p0, @p1, @p2, NULL, @p0, @p3, @p4, 'parquet', @p5, @p6, @p7, @p8, @p9, NULL, NULL, NULL)",
                fileId, t.TableId, snapshotId, f.RelativePath, true, f.RecordCount, f.Size, f.FooterSize, rowIdStart, partitionId);
            foreach (var s in f.Stats)
            {
                await Exec("INSERT INTO ducklake_file_column_stats(data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats) " +
                           "VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, NULL)",
                    fileId, t.TableId, s.ColumnId, s.ColumnSizeBytes, s.ValueCount, s.NullCount, s.Min, s.Max, s.ContainsNan);
                await MergeTableColumnStats(t, s);
            }
            for (var i = 0; i < f.PartitionValues.Count; i++)
                await Exec("INSERT INTO ducklake_file_partition_value(data_file_id, table_id, partition_key_index, partition_value) VALUES (@p0, @p1, @p2, @p3)",
                    fileId, t.TableId, i, f.PartitionValues[i]);
            return fileId + 1;
        }

        async Task MergeTableColumnStats(PendingTable t, DuckLakeColumnStats s)
        {
            var type = t.Columns.SelectMany(x => x.SelfAndDescendants()).First(x => x.Id == s.ColumnId).Type as DuckLakePrimitive;
            var existing = await Db.QueryAsync(c, tx, "SELECT contains_null, contains_nan, min_value, max_value FROM ducklake_table_column_stats WHERE table_id = @p0 AND column_id = @p1",
                r => (Db.Bool(r, 0), Db.Bool(r, 1), Db.Str(r, 2), Db.Str(r, 3)), ct, null, t.TableId, s.ColumnId);
            if (existing.Count == 0)
            {
                // A table that already had rows without a stats row for this column (added column, older writer) has
                // unknown bounds: only a table this transaction just created starts from this file's exact bounds.
                var priorRows = !t.Created && (await Db.QueryAsync(c, tx, "SELECT count(*) FROM ducklake_data_file WHERE table_id = @p0 AND begin_snapshot < @p1",
                    r => Db.ReqLong(r, 0), ct, null, t.TableId, snapshotId))[0] > 0;
                await Exec("INSERT INTO ducklake_table_column_stats(table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) VALUES (@p0, @p1, @p2, @p3, @p4, @p5, NULL)",
                    t.TableId, s.ColumnId, priorRows || s.NullCount > 0, s.ContainsNan, priorRows ? null : s.Min, priorRows ? null : s.Max);
                return;
            }
            var (hasNull, hasNan, min, max) = existing[0];
            await Exec("UPDATE ducklake_table_column_stats SET contains_null = @p0, contains_nan = @p1, min_value = @p2, max_value = @p3 WHERE table_id = @p4 AND column_id = @p5",
                (hasNull ?? true) || s.NullCount > 0,
                hasNan is null || s.ContainsNan is null ? null : hasNan.Value || s.ContainsNan.Value,
                Widen(type, min, s.Min, lower: true), Widen(type, max, s.Max, lower: false), t.TableId, s.ColumnId);
        }

        async Task<long> Delete(DeleteOp op, long fileId)
        {
            var t = op.Table;
            var inlinedDeletes = DuckLakeCatalog.InlinedDeleteTable(t.TableId!.Value);
            if (op.Files.Count > 0 && await _catalog.TableExistsAsync(c, tx, inlinedDeletes, ct))
            {
                var ids = string.Join(", ", op.Files.Select(f => f.DataFileId.ToString(CultureInfo.InvariantCulture)));
                var concurrent = await Db.QueryAsync(c, tx, $"SELECT count(*) FROM {Db.Quote(inlinedDeletes)} WHERE begin_snapshot > @p0 AND file_id IN ({ids})",
                    r => Db.ReqLong(r, 0), ct, null, _base);
                if (concurrent[0] > 0) throw new DuckLakeConflictException($"Rows of table {t.TableId} were deleted (inlined) from the same data files concurrently");
            }
            foreach (var f in op.Files)
            {
                // The file's delete state must be what this delete was planned against: otherwise another snapshot
                // deleted from the same data file ("Two snapshots delete from the same underlying data file").
                var data = await Db.QueryAsync(c, tx, "SELECT end_snapshot FROM ducklake_data_file WHERE data_file_id = @p0", r => Db.Long(r, 0), ct, null, f.DataFileId);
                var current = await Db.QueryAsync(c, tx, "SELECT delete_file_id FROM ducklake_delete_file WHERE data_file_id = @p0 AND end_snapshot IS NULL",
                    r => Db.ReqLong(r, 0), ct, null, f.DataFileId);
                if (data.Count != 1 || data[0] is not null || (current.Count == 0 ? null : (long?)current[0]) != f.OldDeleteFileId)
                    throw new DuckLakeConflictException($"Data file {f.DataFileId} was deleted from or removed concurrently");
                if (f.OldDeleteFileId is { } old) await EndRows("ducklake_delete_file", "delete_file_id", old);
                if (f.NewFile is null)
                {
                    await EndRows("ducklake_data_file", "data_file_id", f.DataFileId);
                    continue;
                }
                await Exec("INSERT INTO ducklake_delete_file(delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, " +
                           "file_size_bytes, footer_size, encryption_key, partial_max) VALUES (@p0, @p1, @p2, NULL, @p3, @p4, @p5, 'parquet', @p6, @p7, @p8, NULL, NULL)",
                    fileId++, t.TableId, snapshotId, f.DataFileId, f.NewFile.RelativePath, true, f.NewFile.Count, f.NewFile.Size, f.NewFile.FooterSize);
            }
            foreach (var (inlinedTable, rowIds) in op.Inlined)
            {
                foreach (var chunk in rowIds.Chunk(500))
                {
                    var list = string.Join(", ", chunk.Select(id => id.ToString(CultureInfo.InvariantCulture)));
                    var updated = await Exec($"UPDATE {Db.Quote(inlinedTable)} SET end_snapshot = @p0 WHERE end_snapshot IS NULL AND row_id IN ({list})", snapshotId);
                    if (updated != chunk.Length) throw new DuckLakeConflictException($"Inlined rows of {inlinedTable} were deleted concurrently");
                }
            }
            await Exec("UPDATE ducklake_table_stats SET record_count = record_count - @p0 WHERE table_id = @p1", op.Count, t.TableId);
            return fileId;
        }
    }

    private readonly Dictionary<PendingTable, long?> _partitionIds = [];

    /// <summary>The wider of two bounds; unknown (null) when either is unknown or cannot be compared.</summary>
    private static string? Widen(DuckLakePrimitive? type, string? current, string? added, bool lower)
    {
        if (current is null) return null; // the table already has rows with unknown bounds
        if (added is null) return current; // nothing new to compare (all nulls / NaNs) keeps the current bound
        if (type is null || !DuckLakeValues.TryParse(type, current, out var a) || !DuckLakeValues.TryParse(type, added, out var b)) return null;
        var cmp = DuckLakeValues.Compare(b!, a!);
        return lower ? (cmp < 0 ? added : current) : (cmp > 0 ? added : current);
    }
}
