using System.Data.Common;
using System.Globalization;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;
using Microsoft.EntityFrameworkCore;

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
/// been created or altered in it (an insert may follow, which is how UPDATE is expressed); a table created in the
/// transaction takes its columns from its definition (only its partitioning can be set afterwards).
/// </para>
/// </summary>
public sealed class DuckLakeTransaction : IAsyncDisposable
{
    private readonly DuckLakeCatalog _catalog;
    private readonly List<Op> _ops = [];
    private readonly List<string> _written = [];
    private readonly Dictionary<(string Schema, string Table), PendingTable> _tables = [];
    private readonly Dictionary<string, PendingSchema> _schemas = new(StringComparer.Ordinal);
    private readonly Dictionary<PendingTable, long?> _partitionIds = [];
    private long? _base;
    private long? _attemptedSnapshot;
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
        /// <summary>The schema's path, names joined by <c>.</c> (nested schemas, 1.1).</summary>
        public required string Name;
        public required IReadOnlyList<string> Parts;
        public required string Location;
        public required string RelativePath;
        public PendingSchema? Parent;
        public long? ParentId;
        public Guid Uuid = Guid.NewGuid();
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
    /// <summary>A new version of one column row: renamed, or retyped (same <c>column_id</c>).</summary>
    private sealed record ReplaceColumnOp(PendingTable Table, DuckLakeColumn Column, long Order, long? Parent) : Op;
    private sealed record SetPartitionOp(PendingTable Table, IReadOnlyList<DuckLakePartitionKey> Keys) : Op;
    private sealed record InsertOp(PendingTable Table, WrittenFile File) : Op;
    private sealed record DeleteOp(PendingTable Table, IReadOnlyList<FileDelete> Files, IReadOnlyDictionary<string, IReadOnlyList<long>> Inlined, long Count) : Op;

    private sealed record WrittenFile(string RelativePath, long RecordCount, long Size, long FooterSize, long RowGroups, IReadOnlyList<DuckLakeColumnStats> Stats,
        IReadOnlyList<string?> PartitionValues);

    /// <summary>A data file's new delete state: a replacement delete file, or the whole file removed.</summary>
    private sealed record FileDelete(long DataFileId, long? OldDeleteFileId, long Deleted, long Total, WrittenDelete? NewFile);

    private sealed record WrittenDelete(string RelativePath, long Count, long Size, long FooterSize);

    private async Task<DuckLakeDbContext> BeginAsync(CancellationToken ct)
    {
        if (_finished) throw new InvalidOperationException("The transaction has already been committed or disposed");
        var db = _catalog.Context();
        _base ??= (await DuckLakeCatalog.LatestSnapshotAsync(db, ct)).SnapshotId;
        return db;
    }

    private async Task<PendingSchema> SchemaAsync(DuckLakeDbContext db, string name, CancellationToken ct)
    {
        if (_schemas.TryGetValue(name, out var pending))
            return pending.Dropped ? throw new DuckLakeNotFoundException($"Schema {name} is dropped in this transaction") : pending;
        var row = DuckLakeCatalog.Find(await _catalog.SchemasAsync(db, _base!.Value, ct), s => s.Name, name, "schema");
        if (_schemas.TryGetValue(row.Name, out pending) && pending.Dropped) throw new DuckLakeNotFoundException($"Schema {name} is dropped in this transaction");
        pending ??= new PendingSchema { SchemaId = row.SchemaId, Name = row.Name, Parts = row.Parts, Location = row.Location, RelativePath = "", ParentId = row.ParentId };
        return _schemas[row.Name] = _schemas[name] = pending;
    }

    private async Task<PendingTable> TableAsync(DuckLakeDbContext db, string schema, string table, CancellationToken ct)
    {
        var sc = await SchemaAsync(db, schema, ct);
        if (_tables.TryGetValue((sc.Name, table), out var pending))
            return pending.Dropped ? throw new DuckLakeNotFoundException($"Table {schema}.{table} is dropped in this transaction") : pending;
        if (sc.Created) throw new DuckLakeNotFoundException($"No table named '{table}' in schema {sc.Name}");
        var loaded = await _catalog.LoadTableAsync(db, sc.Name, table, _base!.Value, ct);
        var tableId = loaded.TableId;
        var maxColumn = await db.Columns.Where(c => c.TableId == tableId).MaxAsync(c => (long?)c.ColumnId, ct);
        pending = new PendingTable
        {
            TableId = loaded.TableId,
            Schema = sc,
            Name = loaded.Name,
            Location = loaded.Location,
            Columns = [.. loaded.Columns],
            NextColumnId = (maxColumn ?? 0) + 1,
            PartitionKeys = loaded.PartitionKeys,
            Uuid = loaded.TableUuid,
        };
        return _tables[(sc.Name, loaded.Name)] = _tables[(sc.Name, table)] = pending;
    }

    private void Require11(string feature)
    {
        if (!_catalog.Is11) throw new NotSupportedException($"{feature} needs DuckLake format version 1.1; this catalog is {_catalog.Version}");
    }

    // ---- DDL -------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Create a schema. <c>sales.europe</c> creates <c>europe</c> inside the existing (or just created) schema <c>sales</c>:
    /// nested schemas need format 1.1, and a name with a dot is always read as a path.
    /// </summary>
    public async Task CreateSchemaAsync(string name, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var parts = name.Split('.');
        if (parts.Any(string.IsNullOrEmpty)) throw new ArgumentException($"'{name}' is not a schema name or path", nameof(name));
        var live = await _catalog.SchemasAsync(db, _base!.Value, ct);
        if (live.Any(s => s.Name == name && !(_schemas.TryGetValue(s.Name, out var d) && d.Dropped)) || _schemas.TryGetValue(name, out var p) && !p.Dropped)
            throw new InvalidOperationException($"Schema {name} already exists");
        PendingSchema? parent = null;
        if (parts.Length > 1)
        {
            Require11("A nested schema");
            parent = await SchemaAsync(db, string.Join('.', parts[..^1]), ct);
        }
        var schema = new PendingSchema
        {
            Name = name,
            Parts = parts,
            Parent = parent,
            ParentId = parent?.SchemaId,
            Created = true,
            RelativePath = "",
            Location = "",
        };
        // A top-level schema's directory is its name; a nested one's is its UUID, relative to data_path (as DuckDB does).
        schema.RelativePath = parent is null ? PathSegment(parts[^1]) : schema.Uuid.ToString("D") + "/";
        schema.Location = _catalog.DataPath + schema.RelativePath;
        _schemas[name] = schema;
        _ops.Add(new CreateSchemaOp(schema));
    }

    /// <summary>Drop an empty schema (spec: "DROP SCHEMA is only allowed on empty schemas"; nested schemas count as contents).</summary>
    public async Task DropSchemaAsync(string name, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var schema = await SchemaAsync(db, name, ct);
        if (schema.Created) throw new InvalidOperationException($"Schema {name} was created in this transaction");
        var tables = (await DuckLakeCatalog.TablesAsync(db, _base!.Value, ct)).Where(t => t.SchemaId == schema.SchemaId)
            .Where(t => !(_tables.TryGetValue((schema.Name, t.Name), out var pt) && pt.Dropped)).ToList();
        var children = (await _catalog.SchemasAsync(db, _base!.Value, ct)).Where(s => s.ParentId == schema.SchemaId)
            .Where(s => !(_schemas.TryGetValue(s.Name, out var ps) && ps.Dropped)).ToList();
        if (tables.Count > 0 || children.Count > 0 || _tables.Values.Any(t => t.Schema == schema && !t.Dropped)
            || _schemas.Values.Any(s => s.Parent == schema && !s.Dropped))
            throw new InvalidOperationException($"Schema {name} is not empty");
        schema.Dropped = true;
        _ops.Add(new DropSchemaOp(schema));
    }

    public async Task CreateTableAsync(string schema, string name, IReadOnlyList<DuckLakeColumnDefinition> columns, CancellationToken ct = default)
    {
        if (columns.Count == 0) throw new ArgumentException("A table needs at least one column", nameof(columns));
        if (columns.Select(x => x.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count)
            throw new ArgumentException("Column names must be unique", nameof(columns));
        foreach (var c in columns) CheckColumn(c);
        await using var db = await BeginAsync(ct);
        var sc = await SchemaAsync(db, schema, ct);
        if (!sc.Created && (await DuckLakeCatalog.TablesAsync(db, _base!.Value, ct)).Any(t => t.SchemaId == sc.SchemaId && t.Name == name
                                                                                              && !(_tables.TryGetValue((sc.Name, t.Name), out var dropped) && dropped.Dropped))
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

    private void CheckColumn(DuckLakeColumnDefinition column)
    {
        // 1.1: user column names starting with _ducklake_ are reserved (inlined tables' system columns), ignoring case.
        if (column.Name.StartsWith("_ducklake_", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException($"Column names starting with _ducklake_ are reserved: {column.Name}");
        if (Uses(column.Type, "null")) Require11("The null type");
        if (!column.Nullable && Uses(column.Type, "null")) throw new ArgumentException($"Column {column.Name} of type null must allow nulls");

        static bool Uses(DuckLakeType t, string name) => t switch
        {
            DuckLakePrimitive p => p.Name == name,
            DuckLakeList l => Uses(l.Element, name),
            DuckLakeMap m => Uses(m.Key, name) || Uses(m.Value, name),
            DuckLakeStruct s => s.Fields.Any(f => Uses(f.Type, name)),
            _ => false,
        };
    }

    public async Task DropTableAsync(string schema, string name, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var t = await TableAsync(db, schema, name, ct);
        if (t.Created) throw new InvalidOperationException($"Table {schema}.{name} was created in this transaction");
        t.Dropped = true;
        _ops.Add(new DropTableOp(t));
    }

    public async Task AddColumnAsync(string schema, string table, DuckLakeColumnDefinition column, CancellationToken ct = default)
    {
        CheckColumn(column);
        await using var db = await BeginAsync(ct);
        var t = await AlterableAsync(db, schema, table, ct);
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
        await using var db = await BeginAsync(ct);
        var t = await AlterableAsync(db, schema, table, ct);
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
        CheckColumn(new DuckLakeColumnDefinition(newName, DuckLakeType.Int32));
        await using var db = await BeginAsync(ct);
        var t = await AlterableAsync(db, schema, table, ct);
        var col = Column(t, column);
        if (t.Columns.Any(x => x != col && string.Equals(x.Name, newName, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException($"Column {newName} already exists");
        var renamed = col with { Name = newName };
        t.Columns[t.Columns.IndexOf(col)] = renamed;
        _ops.Add(new ReplaceColumnOp(t, renamed, await OrderAsync(db, t, col, ct), null));
    }

    /// <summary>
    /// Change a top-level primitive column's type to one every existing value converts to without rewriting files
    /// (DuckLake allows only widening): <c>int8</c>→<c>int16</c>→<c>int32</c>→<c>int64</c>, <c>float32</c>→<c>float64</c>,
    /// and (1.1) <c>null</c> to any type.
    /// </summary>
    public async Task AlterColumnTypeAsync(string schema, string table, string column, DuckLakePrimitive newType, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var t = await AlterableAsync(db, schema, table, ct);
        var col = Column(t, column);
        if (col.Type is not DuckLakePrimitive from) throw new NotSupportedException($"Column {column} is nested; only primitive columns change type");
        if (!Widens(from.Name, newType.Name)) throw new InvalidOperationException($"Column {column}: {from} cannot become {newType} without rewriting data");
        var retyped = col with { Type = newType };
        t.Columns[t.Columns.IndexOf(col)] = retyped;
        _ops.Add(new ReplaceColumnOp(t, retyped, await OrderAsync(db, t, col, ct), null));

        static bool Widens(string from, string to) => (from, to) switch
        {
            ("null", _) => true,
            ("int8", "int16" or "int32" or "int64") or ("int16", "int32" or "int64") or ("int32", "int64") => true,
            ("float32", "float64") => true,
            _ => false,
        };
    }

    private async Task<long> OrderAsync(DuckLakeDbContext db, PendingTable t, DuckLakeColumn col, CancellationToken ct)
    {
        var tableId = t.TableId;
        var id = col.Id;
        return await db.Columns.LiveAt(_base!.Value).Where(c => c.TableId == tableId && c.ColumnId == id).Select(c => c.ColumnOrder).FirstOrDefaultAsync(ct) ?? col.Id;
    }

    /// <summary>
    /// Set the partition keys new data is written with (spec <c>ducklake_partition_info</c>): <c>identity</c>, <c>year</c>,
    /// <c>month</c>, <c>day</c>, <c>hour</c>, and (1.1) <c>epoch_year</c>, <c>epoch_month</c>, <c>epoch_day</c>,
    /// <c>epoch_hour</c>. An empty list removes partitioning.
    /// </summary>
    public async Task SetPartitioningAsync(string schema, string table, IReadOnlyList<(string Column, string Transform)> keys, CancellationToken ct = default)
    {
        if (keys.Any(k => k.Transform.StartsWith("epoch_", StringComparison.Ordinal))) Require11("An epoch_* partition transform");
        await using var db = await BeginAsync(ct);
        var t = await AlterableAsync(db, schema, table, ct, allowCreated: true);
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

    private async Task<PendingTable> AlterableAsync(DuckLakeDbContext db, string schema, string table, CancellationToken ct, bool allowCreated = false)
    {
        var t = await TableAsync(db, schema, table, ct);
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
        await using (var db = await BeginAsync(ct)) t = await TableAsync(db, schema, table, ct);
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
        // Columns of the null type hold no data: they are left out of the file (and read back as null).
        var stored = columns.Select((c, i) => (c, i)).Where(x => x.c.Type is not DuckLakePrimitive { Name: "null" }).ToList();
        var fileSchema = DuckLakeTypes.ToIceberg(stored.Select(x => x.c).ToList());
        var groups = t.PartitionKeys.Count == 0
            ? [(Values: (IReadOnlyList<string?>)[], Rows: normalized)]
            : DuckLakePartitioning.Split(columns, t.PartitionKeys, normalized);
        var groupRows = _catalog.Options.Parquet.RowGroupRows is > 0 and var g ? g : int.MaxValue;
        foreach (var (partitionValues, part) in groups)
        {
            var directory = t.PartitionKeys.Count == 0 ? "" : DuckLakePartitioning.HivePath(columns, t.PartitionKeys, partitionValues);
            var relative = $"{directory}ducklake-{Guid.CreateVersion7()}.parquet";
            var location = t.Location + relative;
            _written.Add(location);
            var fileRows = part.Select(r => stored.Select(x => r[x.i]).ToArray()).ToList();
            var file = await ParquetFiles.WriteAsync(_catalog.IO, location, fileSchema, 0, [], fileRows, _catalog.Options.Parquet, ct);
            var footer = await DuckLakeDeleteFiles.FooterSizeAsync(_catalog.IO, location, file.FileSizeInBytes, ct);
            var stats = new StatsCollector(columns);
            foreach (var row in part) stats.Add(row);
            var rowGroups = (part.Count + (long)groupRows - 1) / groupRows;
            _ops.Add(new InsertOp(t, new WrittenFile(relative, part.Count, file.FileSizeInBytes, footer, Math.Max(1, rowGroups), stats.Result(file.ColumnSizes), partitionValues)));
        }
        t.Inserted = true;
    }

    /// <summary>
    /// Delete the rows matching <paramref name="filter"/> (merge-on-read: spec "DELETE"). For each affected data file a
    /// new delete file holding every deleted position (earlier deletes included) replaces the file's current one; a
    /// data file losing all its rows is removed from the table instead. Inlined rows get their end snapshot set.
    /// Returns the number of rows deleted.
    /// </summary>
    public async Task<long> DeleteAsync(string schema, string table, Expression filter, CancellationToken ct = default)
    {
        PendingTable t;
        DuckLakeTable loaded;
        DuckLakeScanPlan plan;
        Dictionary<long, DeleteFileEntity> deleteRows;
        await using (var db = await BeginAsync(ct))
        {
            t = await TableAsync(db, schema, table, ct);
            if (t.Created || t.Altered || t.PartitionChanged || t.Inserted || t.Deleted)
                throw new InvalidOperationException($"A delete must be the first change to {schema}.{table} in a transaction");
            loaded = await _catalog.LoadTableAsync(db, t.Schema.Name, t.Name, _base!.Value, ct);
            plan = await _catalog.PlanScanAsync(db, loaded, null, ct);
            deleteRows = await DuckLakeCatalog.DeleteFilesAsync(db, loaded.TableId, loaded.SnapshotId, ct);
        }
        var byFile = new Dictionary<long, List<long>>();
        var inlined = new Dictionary<string, List<long>>();
        await foreach (var row in _catalog.ScanAsync(loaded, filter, ct))
        {
            if (row.DataFileId is { } f) (byFile.TryGetValue(f, out var l) ? l : byFile[f] = []).Add(row.Position);
            else (inlined.TryGetValue(row.InlinedTable!, out var l) ? l : inlined[row.InlinedTable!] = []).Add(row.RowId);
        }
        var files = new List<FileDelete>();
        foreach (var (fileId, positions) in byFile)
        {
            var task = plan.Files.Single(x => x.DataFileId == fileId);
            var all = new HashSet<long>(positions);
            if (await _catalog.DeletedPositionsAsync(plan, task, loaded.SnapshotId, ct) is { } earlier) all.UnionWith(earlier);
            WrittenDelete? written = null;
            if (all.Count < task.RecordCount)
            {
                var relative = $"ducklake-{Guid.CreateVersion7()}-delete.parquet";
                var location = t.Location + relative;
                _written.Add(location);
                var (size, footer) = await DuckLakeDeleteFiles.WriteAsync(_catalog.IO, location, task.Location, all, ct);
                written = new WrittenDelete(relative, all.Count, size, footer);
            }
            files.Add(new FileDelete(fileId, deleteRows.TryGetValue(fileId, out var old) ? old.DeleteFileId : null, positions.Count, task.RecordCount, written));
        }
        var count = byFile.Values.Sum(p => (long)p.Count) + inlined.Values.Sum(p => (long)p.Count);
        if (count == 0) return 0;
        t.Deleted = true;
        _ops.Add(new DeleteOp(t, files, inlined.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<long>)kv.Value), count));
        return count;
    }

    /// <summary>UPDATE as the spec defines it: a delete of the matching rows and an insert of their new versions, in one snapshot.</summary>
    public async Task<long> UpdateAsync(string schema, string table, Expression filter, Func<object?[], object?[]> update, CancellationToken ct = default)
    {
        DuckLakeTable loaded;
        await using (var db = await BeginAsync(ct))
        {
            var t = await TableAsync(db, schema, table, ct);
            loaded = await _catalog.LoadTableAsync(db, t.Schema.Name, t.Name, _base!.Value, ct);
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
            await using var db = _catalog.Context();
            await db.Database.OpenConnectionAsync(ct);
            await using var tx = await db.Database.BeginTransactionAsync(ct);
            try
            {
                snapshot = await ApplyAsync(db, ct);
            }
            catch (DuckLakeConflictException)
            {
                await tx.RollbackAsync(CancellationToken.None);
                await CleanUpAsync();
                throw;
            }
            catch (Exception e) when (e is DbException or DbUpdateException)
            {
                // Lost the race for the snapshot ID (a unique key): roll back and try again. Anything else is a real
                // error; nothing was committed, so the files go.
                last = e;
                await tx.RollbackAsync(CancellationToken.None);
                if (await SnapshotTakenAsync(ct)) continue;
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

    private async Task<bool> SnapshotTakenAsync(CancellationToken ct)
    {
        if (_attemptedSnapshot is not { } id) return false;
        await using var db = _catalog.Context();
        return await db.Snapshots.AnyAsync(s => s.SnapshotId == id, ct);
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
                case AddColumnOp or EndColumnsOp or ReplaceColumnOp or SetPartitionOp when Base(op) is { } id: set.AlteredTables.Add(id); break;
                case InsertOp i when i.Table.BaseTableId is { } id: set.InsertedTables.Add(id); break;
                case DeleteOp d: set.DeletedTables.Add(d.Table.TableId!.Value); break;
            }
        }
        return set;

        static long? Base(Op op) => op switch
        {
            AddColumnOp a => a.Table.BaseTableId,
            EndColumnsOp e => e.Table.BaseTableId,
            ReplaceColumnOp r => r.Table.BaseTableId,
            SetPartitionOp p => p.Table.BaseTableId,
            _ => null,
        };
    }

    private async Task<DuckLakeSnapshot> ApplyAsync(DuckLakeDbContext db, CancellationToken ct)
    {
        var latest = await DuckLakeCatalog.LatestSnapshotAsync(db, ct);
        var ours = Changes();
        if (latest.SnapshotId != _base)
        {
            var baseId = _base!.Value;
            var since = await db.SnapshotChanges.Where(c => c.SnapshotId > baseId).OrderBy(c => c.SnapshotId).ToListAsync(ct);
            foreach (var theirs in since)
                if (ours.ConflictWith(theirs.SnapshotId, DuckLakeChange.Parse(theirs.ChangesMade)) is { } reason) throw new DuckLakeConflictException(reason);
        }
        var snapshotId = latest.SnapshotId + 1;
        _attemptedSnapshot = snapshotId;
        var nextCatalogId = latest.NextCatalogId;
        var nextFileId = latest.NextFileId;
        var schemaChanged = _ops.Any(o => o is CreateSchemaOp or DropSchemaOp or CreateTableOp or DropTableOp or AddColumnOp or EndColumnsOp or ReplaceColumnOp or SetPartitionOp);
        var schemaVersion = latest.SchemaVersion + (schemaChanged ? 1 : 0);
        var v11 = _catalog.Is11;

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
                {
                    s.Schema.SchemaId = nextCatalogId++;
                    s.Schema.ParentId = s.Schema.Parent?.SchemaId ?? s.Schema.ParentId;
                    var parentId = s.Schema.ParentId;
                    var name = s.Schema.Parts[^1];
                    var live = db.Schemas.LiveAt(latest.SnapshotId).Where(x => x.SchemaName == name);
                    var clash = v11 ? await live.AnyAsync(x => x.ParentSchemaId == parentId, ct) : await live.AnyAsync(ct);
                    if (clash) throw new DuckLakeConflictException($"Schema {s.Schema.Name} was created concurrently");
                    db.Schemas.Add(new SchemaEntity
                    {
                        SchemaId = s.Schema.SchemaId.Value, SchemaUuid = s.Schema.Uuid, BeginSnapshot = snapshotId, SchemaName = name,
                        Path = s.Schema.RelativePath, PathIsRelative = true, ParentSchemaId = parentId,
                    });
                    break;
                }
                case DropSchemaOp s:
                {
                    var schemaId = s.Schema.SchemaId!.Value;
                    var tables = await db.Tables.LiveAt(latest.SnapshotId).CountAsync(x => x.SchemaId == schemaId, ct);
                    var children = v11 ? await db.Schemas.LiveAt(latest.SnapshotId).CountAsync(x => x.ParentSchemaId == schemaId, ct) : 0;
                    if (tables > _ops.Count(o => o is DropTableOp d && d.Table.Schema == s.Schema)
                        || children > _ops.Count(o => o is DropSchemaOp d && d.Schema.ParentId == schemaId))
                        throw new DuckLakeConflictException($"Schema {s.Schema.Name} gained tables or schemas concurrently");
                    await db.Schemas.Current().Where(x => x.SchemaId == schemaId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    break;
                }
                case CreateTableOp t:
                {
                    var table = t.Table;
                    table.TableId = nextCatalogId++;
                    var schemaId = table.Schema.SchemaId ?? throw new InvalidOperationException($"Schema {table.Schema.Name} has no ID");
                    var name = table.Name;
                    if (await db.Tables.LiveAt(latest.SnapshotId).AnyAsync(x => x.SchemaId == schemaId && x.TableName == name, ct)
                        && !_ops.Any(o => o is DropTableOp d && d.Table.Schema == table.Schema && d.Table.Name == name))
                        throw new DuckLakeConflictException($"Table {table.Schema.Name}.{table.Name} was created concurrently");
                    db.Tables.Add(new TableEntity
                    {
                        TableId = table.TableId.Value, TableUuid = table.Uuid, BeginSnapshot = snapshotId, SchemaId = schemaId, TableName = name,
                        Path = PathSegment(name), PathIsRelative = true,
                    });
                    foreach (var col in table.Columns) AddColumnRows(table, col, null, col.Id);
                    versionedTables.Add(table);
                    break;
                }
                case DropTableOp t:
                {
                    var id = t.Table.TableId!.Value;
                    await db.Tables.Current().Where(x => x.TableId == id).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    await db.PartitionInfos.Current().Where(x => x.TableId == id).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    await db.Columns.Current().Where(x => x.TableId == id).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    await db.DataFiles.Current().Where(x => x.TableId == id).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    await db.DeleteFiles.Current().Where(x => x.TableId == id).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    await Raw($"UPDATE ducklake_column_tag SET end_snapshot = @p0 WHERE table_id = @p1 AND end_snapshot IS NULL", snapshotId, id);
                    await Raw($"UPDATE ducklake_tag SET end_snapshot = @p0 WHERE object_id = @p1 AND end_snapshot IS NULL", snapshotId, id);
                    break;
                }
                case AddColumnOp a:
                    AddColumnRows(a.Table, a.Column, null, a.Order);
                    versionedTables.Add(a.Table);
                    break;
                case EndColumnsOp e:
                {
                    var tableId = e.Table.TableId!.Value;
                    var ids = e.ColumnIds.ToList();
                    await db.Columns.Current().Where(x => x.TableId == tableId && ids.Contains(x.ColumnId))
                        .ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    versionedTables.Add(e.Table);
                    break;
                }
                case ReplaceColumnOp r:
                {
                    var tableId = r.Table.TableId!.Value;
                    var columnId = r.Column.Id;
                    await db.Columns.Current().Where(x => x.TableId == tableId && x.ColumnId == columnId && x.ParentColumn == null)
                        .ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    db.Columns.Add(ColumnRow(r.Table, r.Column, r.Parent, r.Order));
                    versionedTables.Add(r.Table);
                    break;
                }
                case SetPartitionOp p:
                {
                    var tableId = p.Table.TableId!.Value;
                    await db.PartitionInfos.Current().Where(x => x.TableId == tableId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    if (p.Keys.Count > 0)
                    {
                        var partitionId = nextCatalogId++;
                        db.PartitionInfos.Add(new PartitionInfoEntity { PartitionId = partitionId, TableId = tableId, BeginSnapshot = snapshotId });
                        for (var i = 0; i < p.Keys.Count; i++)
                            db.PartitionColumns.Add(new PartitionColumnEntity
                            {
                                PartitionId = partitionId, TableId = tableId, PartitionKeyIndex = i, ColumnId = p.Keys[i].ColumnId, Transform = p.Keys[i].Transform,
                            });
                        _partitionIds[p.Table] = partitionId;
                    }
                    else _partitionIds[p.Table] = null;
                    versionedTables.Add(p.Table);
                    break;
                }
                case InsertOp i:
                    nextFileId = await InsertFile(i, nextFileId);
                    break;
                case DeleteOp d:
                    nextFileId = await Delete(d, nextFileId);
                    break;
            }
            await Flush();
        }

        foreach (var t in versionedTables)
            db.SchemaVersions.Add(new SchemaVersionEntity { BeginSnapshot = snapshotId, SchemaVersion = schemaVersion, TableId = t.TableId!.Value });

        // Changes are recorded with the IDs just assigned (a table created here is named, not numbered).
        var final = Changes();
        foreach (var i in _ops.OfType<InsertOp>().Where(i => i.Table.Created)) final.InsertedTables.Add(i.Table.TableId!.Value);
        var changes = string.Join(",", final.Entries());
        var time = DateTimeOffset.UtcNow;
        db.SnapshotChanges.Add(new SnapshotChangesEntity
        {
            SnapshotId = snapshotId, ChangesMade = changes, Author = Author, CommitMessage = CommitMessage, CommitExtraInfo = CommitExtraInfo,
        });
        db.Snapshots.Add(new SnapshotEntity
        {
            SnapshotId = snapshotId, SnapshotTime = time, SchemaVersion = schemaVersion, NextCatalogId = nextCatalogId, NextFileId = nextFileId,
        });
        await Flush();
        return new DuckLakeSnapshot(snapshotId, time, schemaVersion, nextCatalogId, nextFileId)
        {
            Changes = DuckLakeChange.Parse(changes), Author = Author, CommitMessage = CommitMessage, CommitExtraInfo = CommitExtraInfo,
        };

        async Task Flush()
        {
            await db.SaveChangesAsync(ct);
            db.ChangeTracker.Clear();
        }

        async Task Raw(string sql, params object?[] args)
        {
            var (c, t) = db.Raw();
            await Db.ExecAsync(c, t, sql, ct, args);
        }

        ColumnEntity ColumnRow(PendingTable table, DuckLakeColumn col, long? parent, long order) => new()
        {
            ColumnId = col.Id, BeginSnapshot = snapshotId, TableId = table.TableId!.Value, ColumnOrder = order, ColumnName = col.Name,
            ColumnType = col.Type.ColumnType, NullsAllowed = col.Nullable, ParentColumn = parent,
        };

        void AddColumnRows(PendingTable table, DuckLakeColumn col, long? parent, long order)
        {
            db.Columns.Add(ColumnRow(table, col, parent, order));
            foreach (var child in col.Children) AddColumnRows(table, child, col.Id, child.Id);
        }

        async Task<long> InsertFile(InsertOp op, long fileId)
        {
            var t = op.Table;
            var f = op.File;
            var tableId = t.TableId!.Value;
            var stats = await db.TableStats.FirstOrDefaultAsync(x => x.TableId == tableId, ct);
            long rowIdStart;
            if (stats is null)
            {
                // No stats row yet: row IDs continue after any rows already in files (DuckDB creates the row lazily too).
                rowIdStart = await db.DataFiles.Where(x => x.TableId == tableId).MaxAsync(x => x.RowIdStart + x.RecordCount, ct) ?? 0;
                db.TableStats.Add(new TableStatsEntity { TableId = tableId, RecordCount = f.RecordCount, NextRowId = rowIdStart + f.RecordCount, FileSizeBytes = f.Size });
            }
            else
            {
                rowIdStart = stats.NextRowId ?? 0;
                await db.TableStats.Where(x => x.TableId == tableId).ExecuteUpdateAsync(u => u
                    .SetProperty(x => x.RecordCount, x => (x.RecordCount ?? 0) + f.RecordCount)
                    .SetProperty(x => x.NextRowId, x => (x.NextRowId ?? 0) + f.RecordCount)
                    .SetProperty(x => x.FileSizeBytes, x => (x.FileSizeBytes ?? 0) + f.Size), ct);
            }
            long? partitionId = t.PartitionKeys.Count == 0 ? null
                : _partitionIds.TryGetValue(t, out var pid) ? pid
                : await db.PartitionInfos.LiveAt(latest.SnapshotId).Where(x => x.TableId == tableId).Select(x => (long?)x.PartitionId).FirstOrDefaultAsync(ct);
            db.DataFiles.Add(new DataFileEntity
            {
                DataFileId = fileId, TableId = tableId, BeginSnapshot = snapshotId, FileOrder = fileId, Path = f.RelativePath, PathIsRelative = true,
                FileFormat = "parquet", RecordCount = f.RecordCount, FileSizeBytes = f.Size, FooterSize = f.FooterSize, RowIdStart = rowIdStart,
                PartitionId = partitionId, RowGroupCount = v11 ? f.RowGroups : null,
            });
            foreach (var s in f.Stats)
            {
                var exact = v11 && s.Min is not null ? true : (bool?)null;
                db.FileColumnStats.Add(new FileColumnStatsEntity
                {
                    DataFileId = fileId, TableId = tableId, ColumnId = s.ColumnId, ColumnSizeBytes = s.ColumnSizeBytes, ValueCount = s.ValueCount,
                    NullCount = s.NullCount, MinValue = s.Min, MaxValue = s.Max, ContainsNan = s.ContainsNan, MinIsExact = exact, MaxIsExact = exact,
                });
                await MergeTableColumnStats(t, s);
            }
            for (var i = 0; i < f.PartitionValues.Count; i++)
                db.FilePartitionValues.Add(new FilePartitionValueEntity { DataFileId = fileId, TableId = tableId, PartitionKeyIndex = i, PartitionValue = f.PartitionValues[i] });
            return fileId + 1;
        }

        async Task MergeTableColumnStats(PendingTable t, DuckLakeColumnStats s)
        {
            var tableId = t.TableId!.Value;
            var type = t.Columns.SelectMany(x => x.SelfAndDescendants()).First(x => x.Id == s.ColumnId).Type as DuckLakePrimitive;
            var existing = await db.TableColumnStats.FirstOrDefaultAsync(x => x.TableId == tableId && x.ColumnId == s.ColumnId, ct);
            if (existing is null)
            {
                // A table that already had rows without a stats row for this column (added column, older writer) has
                // unknown bounds: only a table without earlier files starts from this file's exact bounds.
                var priorRows = !t.Created && await db.DataFiles.AnyAsync(x => x.TableId == tableId && x.BeginSnapshot < snapshotId, ct);
                var exact = v11 && !priorRows && s.Min is not null ? true : (bool?)null;
                db.TableColumnStats.Add(new TableColumnStatsEntity
                {
                    TableId = tableId, ColumnId = s.ColumnId, ContainsNull = priorRows || s.NullCount > 0, ContainsNan = s.ContainsNan,
                    MinValue = priorRows ? null : s.Min, MaxValue = priorRows ? null : s.Max, MinIsExact = exact, MaxIsExact = exact,
                });
                return;
            }
            var min = Widen(type, existing.MinValue, s.Min, lower: true);
            var max = Widen(type, existing.MaxValue, s.Max, lower: false);
            // A bound stays exact when it is one of the exact bounds it was chosen from.
            bool? MinExact() => min is null ? null : min == existing.MinValue ? existing.MinIsExact : true;
            bool? MaxExact() => max is null ? null : max == existing.MaxValue ? existing.MaxIsExact : true;
            var containsNull = (existing.ContainsNull ?? true) || s.NullCount > 0;
            bool? containsNan = existing.ContainsNan is null || s.ContainsNan is null ? null : existing.ContainsNan.Value || s.ContainsNan.Value;
            var q = db.TableColumnStats.Where(x => x.TableId == tableId && x.ColumnId == s.ColumnId);
            if (v11)
            {
                bool? minExact = MinExact(), maxExact = MaxExact();
                await q.ExecuteUpdateAsync(u => u.SetProperty(x => x.ContainsNull, containsNull).SetProperty(x => x.ContainsNan, containsNan)
                    .SetProperty(x => x.MinValue, min).SetProperty(x => x.MaxValue, max)
                    .SetProperty(x => x.MinIsExact, minExact).SetProperty(x => x.MaxIsExact, maxExact), ct);
            }
            else
            {
                await q.ExecuteUpdateAsync(u => u.SetProperty(x => x.ContainsNull, containsNull).SetProperty(x => x.ContainsNan, containsNan)
                    .SetProperty(x => x.MinValue, min).SetProperty(x => x.MaxValue, max), ct);
            }
        }

        async Task<long> Delete(DeleteOp op, long fileId)
        {
            var t = op.Table;
            var tableId = t.TableId!.Value;
            var inlinedDeletes = DuckLakeCatalog.InlinedDeleteTable(tableId);
            if (op.Files.Count > 0 && await DuckLakeCatalog.TableExistsAsync(db, inlinedDeletes, ct))
            {
                var ids = string.Join(", ", op.Files.Select(f => f.DataFileId.ToString(CultureInfo.InvariantCulture)));
                var (c, tx) = db.Raw();
                var concurrent = await Db.QueryAsync(c, tx, $"SELECT count(*) FROM {Db.Quote(inlinedDeletes)} WHERE begin_snapshot > @p0 AND file_id IN ({ids})",
                    r => Db.ReqLong(r, 0), ct, null, _base);
                if (concurrent[0] > 0) throw new DuckLakeConflictException($"Rows of table {tableId} were deleted (inlined) from the same data files concurrently");
            }
            foreach (var f in op.Files)
            {
                // The file's delete state must be what this delete was planned against: otherwise another snapshot
                // deleted from the same data file ("Two snapshots delete from the same underlying data file").
                var dataFileId = f.DataFileId;
                var data = await db.DataFiles.Where(x => x.DataFileId == dataFileId).Select(x => new { x.EndSnapshot }).ToListAsync(ct);
                var current = await db.DeleteFiles.Current().Where(x => x.DataFileId == dataFileId).Select(x => (long?)x.DeleteFileId).ToListAsync(ct);
                if (data.Count != 1 || data[0].EndSnapshot is not null || current.SingleOrDefault() != f.OldDeleteFileId)
                    throw new DuckLakeConflictException($"Data file {f.DataFileId} was deleted from or removed concurrently");
                if (f.OldDeleteFileId is { } old)
                    await db.DeleteFiles.Current().Where(x => x.DeleteFileId == old).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                if (f.NewFile is null)
                {
                    await db.DataFiles.Current().Where(x => x.DataFileId == dataFileId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                    continue;
                }
                db.DeleteFiles.Add(new DeleteFileEntity
                {
                    DeleteFileId = fileId++, TableId = tableId, BeginSnapshot = snapshotId, DataFileId = dataFileId, Path = f.NewFile.RelativePath,
                    PathIsRelative = true, Format = "parquet", DeleteCount = f.NewFile.Count, FileSizeBytes = f.NewFile.Size, FooterSize = f.NewFile.FooterSize,
                    RowGroupCount = v11 ? 1 : null,
                });
            }
            var (rowIdColumn, _, endColumn) = DuckLakeInlinedData.SystemColumns(v11);
            foreach (var (inlinedTable, rowIds) in op.Inlined)
            {
                foreach (var chunk in rowIds.Chunk(500))
                {
                    var list = string.Join(", ", chunk.Select(id => id.ToString(CultureInfo.InvariantCulture)));
                    var (c, tx) = db.Raw();
                    var updated = await Db.ExecAsync(c, tx,
                        $"UPDATE {Db.Quote(inlinedTable)} SET {endColumn} = @p0 WHERE {endColumn} IS NULL AND {rowIdColumn} IN ({list})", ct, snapshotId);
                    if (updated != chunk.Length) throw new DuckLakeConflictException($"Inlined rows of {inlinedTable} were deleted concurrently");
                }
            }
            await db.TableStats.Where(x => x.TableId == tableId).ExecuteUpdateAsync(u => u.SetProperty(x => x.RecordCount, x => x.RecordCount - op.Count), ct);
            return fileId;
        }
    }

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
