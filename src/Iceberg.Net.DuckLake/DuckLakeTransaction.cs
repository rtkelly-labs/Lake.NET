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
public sealed partial class DuckLakeTransaction : IAsyncDisposable
{
    private readonly DuckLakeCatalog _catalog;
    private readonly List<Op> _ops = [];
    private readonly List<string> _written = [];
    private readonly Dictionary<(string Schema, string Table), PendingTable> _tables = [];
    private readonly Dictionary<string, PendingSchema> _schemas = new(StringComparer.Ordinal);
    private readonly Dictionary<PendingTable, long?> _partitionIds = [];
    /// <summary>
    /// Tables a DELETE matched no rows in. Not written to <c>changes_made</c> (DuckDB writes nothing for them either),
    /// but they count for this transaction's conflict check: spec "Conflict Resolution", "A DELETE that matches zero
    /// rows still records a delete attempt".
    /// </summary>
    private readonly HashSet<PendingTable> _deleteAttempts = [];
    private long? _base;
    private long? _attemptedSnapshot;
    private bool _finished;
    /// <summary>Staging (lookups, scans for deletes): one span from the first operation to commit or disposal.</summary>
    private DuckLakeTelemetry.Scope? _stage;

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
        public IReadOnlyList<DuckLakeSortKey> SortKeys = [];
        public bool Created, Dropped, Altered, PartitionChanged, Inserted, Deleted, Compacted;
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
        IReadOnlyList<string?> PartitionValues)
    {
        /// <summary>A registered existing file: <see cref="RelativePath"/> is its absolute location.</summary>
        public bool Absolute { get; init; }
        /// <summary>A registered file's name mapping rows (without <c>mapping_id</c>), in DuckDB's order.</summary>
        public IReadOnlyList<NameMappingEntity>? Mapping { get; init; }
        /// <summary>The mapping's identity: files of one table with the same mapping share one <c>mapping_id</c>.</summary>
        public string? MappingKey => Mapping is null ? null
            : string.Join("\u0001", Mapping.Select(m => $"{m.ColumnId}\u0002{m.SourceName}\u0002{m.TargetFieldId}\u0002{m.ParentColumn}\u0002{m.IsPartition}"));
    }

    /// <summary>A data file's new delete state: a replacement delete file, or the whole file removed.</summary>
    /// <summary>
    /// A data file's new delete state: a replacement delete file, the whole file removed (neither), or (deletion
    /// inlining) <see cref="Inline"/> positions recorded in <c>ducklake_inlined_delete_{table}</c> beside its current
    /// delete file.
    /// </summary>
    private sealed record FileDelete(long DataFileId, long? OldDeleteFileId, long Deleted, long Total, WrittenDelete? NewFile, IReadOnlyList<long>? Inline = null);

    /// <summary>Rows written into the table's inlined data table (data inlining), each with the row ID it keeps or a new one.</summary>
    private sealed record InlineInsertOp(PendingTable Table, IReadOnlyList<(object?[] Values, long? RowId)> Rows) : Op;

    private sealed record WrittenDelete(string RelativePath, long Count, long Size, long FooterSize)
    {
        public string Format { get; init; } = "parquet";
    }

    private async Task<DuckLakeDbContext> BeginAsync(CancellationToken ct)
    {
        if (_finished) throw new InvalidOperationException("The transaction has already been committed or disposed");
        _stage ??= DuckLakeTelemetry.Start("ducklake.transaction.stage", _catalog);
        var db = _catalog.Context(_stage);
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
            SortKeys = loaded.SortKeys,
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
        var views = (await DuckLakeCatalog.ViewsAsync(db, _base!.Value, ct)).Count(v => v.SchemaId == schema.SchemaId
                                                                                       && !(_views.TryGetValue((schema.Name, v.Name), out var pv) && pv.Dropped));
        var schemaId = schema.SchemaId;
        var macros = (await db.Macros.LiveAt(_base!.Value).Where(m => m.SchemaId == schemaId).Select(m => m.MacroName ?? "").ToListAsync(ct))
            .Count(n => !(_macros.TryGetValue((schema.Name, n), out var pm) && pm.Dropped));
        if (tables.Count > 0 || children.Count > 0 || views > 0 || macros > 0 || _tables.Values.Any(t => t.Schema == schema && !t.Dropped)
            || _schemas.Values.Any(s => s.Parent == schema && !s.Dropped) || _views.Values.Any(v => v.Schema == schema && !v.Dropped)
            || _macros.Values.Any(m => m.Schema == schema && !m.Dropped))
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
        if (_views.TryGetValue((sc.Name, name), out var pendingView) && !pendingView.Dropped
            || !sc.Created && (await DuckLakeCatalog.ViewsAsync(db, _base!.Value, ct)).Any(v => v.SchemaId == sc.SchemaId && string.Equals(v.Name, name, StringComparison.OrdinalIgnoreCase)
                                                                                                && !(_views.TryGetValue((sc.Name, v.Name), out var pv) && (pv.Dropped || pv.Name != v.Name))))
            throw new InvalidOperationException($"{schema}.{name} already exists (a view)");
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
        if (t.Inserted || t.Deleted || t.Compacted) throw new InvalidOperationException($"Alter {schema}.{table} before writing data to it in the same transaction");
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
    public Task InsertAsync(string schema, string table, IEnumerable<object?[]> rows, CancellationToken ct = default) =>
        InsertAsync(schema, table, rows.Select(r => (r, (long?)null)), ct);

    /// <summary>
    /// Insert rows, each optionally with the row ID it keeps (an UPDATE's new row versions): those are written to the
    /// file's <c>_ducklake_internal_row_id</c> column, which takes precedence over <c>row_id_start</c> (spec
    /// <c>ducklake_data_file</c>), as DuckDB does.
    /// </summary>
    private async Task InsertAsync(string schema, string table, IEnumerable<(object?[] Row, long? RowId)> rows, CancellationToken ct)
    {
        PendingTable t;
        await using (var db = await BeginAsync(ct)) t = await TableAsync(db, schema, table, ct);
        var columns = t.Columns;
        var normalizers = columns.Select(c => DuckLakeValues.Normalizer(c.Type, c.Name)).ToArray();
        var required = columns.Select(c => !c.Nullable).ToArray();
        var normalized = new List<object?[]>(rows.TryGetNonEnumeratedCount(out var n) ? n : 0);
        var keepsRowIds = false;
        foreach (var (r, rowId) in rows)
        {
            if (r.Length != columns.Count) throw new ArgumentException($"Row has {r.Length} values; {t.Schema.Name}.{t.Name} has {columns.Count} columns");
            // A kept row ID rides along after the values (partitioning and statistics read values by column index).
            var values = new object?[rowId is null ? r.Length : r.Length + 1];
            for (var i = 0; i < r.Length; i++)
            {
                values[i] = normalizers[i](r[i]);
                if (values[i] is null && required[i]) throw new ArgumentException($"Column {columns[i].Name} is NOT NULL");
            }
            if (rowId is { } id)
            {
                values[^1] = id;
                keepsRowIds = true;
            }
            normalized.Add(values);
        }
        if (normalized.Count == 0) return;
        if (keepsRowIds && normalized.Any(v => v.Length == columns.Count)) throw new ArgumentException("Either every row keeps a row ID or none does");
        normalized = Sorted(t, normalized);
        // Data inlining: a small insert goes into the catalog database (not into partitioned tables: their rows belong in partition files).
        if (t.PartitionKeys.Count == 0 && normalized.Count <= await InliningLimitAsync(t, ct) && DuckLakeInlinedData.CanInline(_catalog.Store.Dialect, columns))
        {
            _ops.Add(new InlineInsertOp(t, normalized.Select(v => (v[..columns.Count], v.Length > columns.Count ? (long?)v[^1] : null)).ToList()));
            t.Inserted = true;
            return;
        }
        // Columns of the null type hold no data: they are left out of the file (and read back as null).
        var stored = columns.Select((c, i) => (c, i)).Where(x => x.c.Type is not DuckLakePrimitive { Name: "null" }).ToList();
        var allStored = stored.Count == columns.Count && !keepsRowIds;
        var fileSchema = DuckLakeTypes.ToIceberg(stored.Select(x => x.c).ToList());
        if (keepsRowIds)
        {
            fileSchema = new Schema(0, fileSchema.Columns.Append(
                NestedField.Optional(DuckLakeCatalog.RowIdFieldId, "_ducklake_internal_row_id", PrimitiveType.Long)));
            stored.Add((null!, columns.Count)); // the row ID, after the values
        }
        var storedColumns = stored.Where(x => x.c is not null).Select(x => x.c).ToList();
        var toFile = DuckLakeValues.FileRowConverter(storedColumns, toFile: true);
        var parquet = _catalog.Options.Parquet with { Metrics = false, Annotations = DuckLakeTypes.Annotations(storedColumns) };
        var groups = t.PartitionKeys.Count == 0
            ? [(Values: (IReadOnlyList<string?>)[], Rows: normalized)]
            : DuckLakePartitioning.Split(columns, t.PartitionKeys, normalized);
        foreach (var (partitionValues, part) in groups)
        {
            var directory = t.PartitionKeys.Count == 0 ? "" : DuckLakePartitioning.HivePath(columns, t.PartitionKeys, partitionValues);
            var relative = $"{directory}ducklake-{Guid.CreateVersion7()}.parquet";
            var location = t.Location + relative;
            _written.Add(location);
            var fileRows = allStored ? part : part.Select(r => stored.Select(x => r[x.i]).ToArray()).ToList();
            if (toFile is not null) fileRows = fileRows.Select(toFile).ToList();
            // DuckLake keeps its own statistics (StatsCollector), so the Iceberg metrics pass is skipped.
            var written = await ParquetFiles.WriteFileAsync(_catalog.IO, location, fileSchema, 0, [], fileRows, parquet, ct);
            var stats = new StatsCollector(columns);
            foreach (var row in part) stats.Add(row);
            _ops.Add(new InsertOp(t, new WrittenFile(relative, part.Count, written.File.FileSizeInBytes, written.FooterSize, written.RowGroups,
                stats.Result(written.File.ColumnSizes), partitionValues)));
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
            if (t.Created || t.Altered || t.PartitionChanged || t.Inserted || t.Deleted || t.Compacted)
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
        var inliningLimit = await InliningLimitAsync(t, ct);
        foreach (var (fileId, positions) in byFile)
        {
            var task = plan.Files.Single(x => x.DataFileId == fileId);
            // The replacement delete file holds its predecessor's positions and the new ones. Positions deleted by DuckDB's
            // deletion inlining stay in ducklake_inlined_delete_{table} and out of the file: readers apply both, and
            // DuckDB counts rows (record_count - delete_count - inlined deletions) assuming the two never overlap.
            var all = new HashSet<long>(positions);
            if (task.DeleteFileLocation is { } previous) all.UnionWith(await DuckLakeDeleteFiles.ReadPositionsAsync(_catalog.IO, previous, loaded.SnapshotId, ct));
            var inlinedDeleted = plan.InlinedDeletions.TryGetValue(fileId, out var inl) ? inl.Count : 0;
            var oldDeleteFile = deleteRows.TryGetValue(fileId, out var old) ? old.DeleteFileId : (long?)null;
            // Deletion inlining: a few positions of a file that keeps rows go to ducklake_inlined_delete_{table}, and its
            // delete file (if any) stays as it is.
            if (all.Count + inlinedDeleted < task.RecordCount && positions.Count <= inliningLimit)
            {
                files.Add(new FileDelete(fileId, oldDeleteFile, positions.Count, task.RecordCount, null, positions));
                continue;
            }
            WrittenDelete? written = null;
            if (all.Count + inlinedDeleted < task.RecordCount)
            {
                if (await WritesDeletionVectorsAsync(t, ct))
                {
                    // A bare vector: its snapshot is the delete file's begin_snapshot (spec ducklake_delete_file).
                    var relative = $"ducklake-{Guid.CreateVersion7()}-delete.puffin";
                    var location = t.Location + relative;
                    _written.Add(location);
                    var bitmap = new Puffin.RoaringPositionBitmap();
                    bitmap.AddRange(all);
                    var bytes = Puffin.DeletionVector.Encode(bitmap);
                    await _catalog.IO.WriteAsync(location, bytes, overwrite: false, ct);
                    written = new WrittenDelete(relative, all.Count, bytes.Length, 0) { Format = "puffin" };
                }
                else
                {
                    var relative = $"ducklake-{Guid.CreateVersion7()}-delete.parquet";
                    var location = t.Location + relative;
                    _written.Add(location);
                    var (size, footer) = await DuckLakeDeleteFiles.WriteAsync(_catalog.IO, location, task.Location, all, ct);
                    written = new WrittenDelete(relative, all.Count, size, footer);
                }
            }
            files.Add(new FileDelete(fileId, oldDeleteFile, positions.Count, task.RecordCount, written));
        }
        var count = byFile.Values.Sum(p => (long)p.Count) + inlined.Values.Sum(p => (long)p.Count);
        if (count == 0)
        {
            _deleteAttempts.Add(t);
            return 0;
        }
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
        var matched = new List<DuckLakeRow>();
        await foreach (var r in _catalog.ReadRowsAsync(loaded, filter, ct)) matched.Add(r);
        var deleted = await DeleteAsync(schema, table, filter, ct);
        if (deleted != matched.Count) throw new InvalidOperationException($"Matched {matched.Count} rows but staged {deleted} deletes");
        // The new versions keep their rows' IDs (spec "Row Lineage"; DuckDB does the same).
        await InsertAsync(schema, table, matched.Select(r => (update((object?[])r.Values.Clone()), (long?)r.RowId)), ct);
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
        if (_stage is { } stage)
        {
            stage.Succeeded();
            stage.Dispose();
        }
        using var scope = DuckLakeTelemetry.Start("ducklake.commit", _catalog);
        try
        {
            var snapshot = await CommitAttemptsAsync(scope, ct);
            scope.Snapshot(snapshot.SnapshotId);
            scope.Succeeded();
            return snapshot;
        }
        catch (Exception e)
        {
            scope.Failed(e);
            throw;
        }
    }

    private async Task<DuckLakeSnapshot> CommitAttemptsAsync(DuckLakeTelemetry.Scope scope, CancellationToken ct)
    {
        Exception? last = null;
        for (var attempt = 0; attempt < Math.Max(1, _catalog.Options.MaxCommitAttempts); attempt++)
        {
            scope.Retried(attempt + 1);
            DuckLakeSnapshot snapshot;
            await using var db = _catalog.Context(scope);
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
                if (await SnapshotTakenAsync(scope, ct))
                {
                    await Task.Delay(Backoff(attempt), ct);
                    continue;
                }
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

    /// <summary>Wait before retry <paramref name="attempt"/>: exponential with jitter, so writers that collided do not collide again in step.</summary>
    private TimeSpan Backoff(int attempt)
    {
        var o = _catalog.Options;
        var ms = Math.Min(o.MaxRetryWait.TotalMilliseconds, o.RetryWait.TotalMilliseconds * Math.Pow(1.5, attempt));
        return TimeSpan.FromMilliseconds(ms * (0.5 + Random.Shared.NextDouble()));
    }

    private async Task<bool> SnapshotTakenAsync(DuckLakeTelemetry.Scope scope, CancellationToken ct)
    {
        if (_attemptedSnapshot is not { } id) return false;
        await using var db = _catalog.Context(scope);
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
        _stage?.Dispose();
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
                case InlineInsertOp i when i.Table.BaseTableId is { } id: set.InlinedInsertTables.Add(id); break;
                case DeleteOp d:
                    // As DuckDB records them: deletes that write delete files or end data files, and deletes kept in the
                    // catalog (inlined rows ended, inlined deletions of file rows).
                    if (d.Files.Any(f => f.Inline is null)) set.DeletedTables.Add(d.Table.TableId!.Value);
                    if (d.Files.Any(f => f.Inline is not null) || d.Inlined.Count > 0) set.InlinedDeleteTables.Add(d.Table.TableId!.Value);
                    break;
                case MergeOp m: set.MergedTables.Add(m.Table.TableId!.Value); break;
                case RewriteOp r: set.RewrittenTables.Add(r.Table.TableId!.Value); break;
                case FlushOp f: set.FlushedTables.Add(f.Table.TableId!.Value); break;
                case var o when IsObjectOp(o): ObjectChanges(set, o); break;
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
        var latest = await db.Snapshots.OrderByDescending(s => s.SnapshotId).FirstOrDefaultAsync(ct) ?? throw new InvalidDataException("ducklake_snapshot is empty");
        var ours = Changes();
        foreach (var t in _deleteAttempts) ours.DeletedTables.Add(t.TableId!.Value);
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
        var schemaChanged = _ops.Any(o => o is CreateSchemaOp or DropSchemaOp or CreateTableOp or DropTableOp or AddColumnOp or EndColumnsOp or ReplaceColumnOp or SetPartitionOp
                                          || IsObjectOp(o));
        var schemaVersion = latest.SchemaVersion + (schemaChanged ? 1 : 0);
        var v11 = _catalog.Is11;
        var rows = new CatalogRows();

        // The snapshot row goes in first: it is the row concurrent committers race for (its primary key), so a loser
        // finds out before writing anything else (on PostgreSQL it waits for the winner, then fails at once). Its
        // counters follow from the staged operations alone; the ID assignment below must land on the same values.
        var time = DateTimeOffset.UtcNow;
        var snapshotRow = new SnapshotEntity
        {
            SnapshotId = snapshotId, SnapshotTime = time, SchemaVersion = schemaVersion,
            NextCatalogId = latest.NextCatalogId + _ops.Count(o => o is CreateSchemaOp or CreateTableOp or SetPartitionOp { Keys.Count: > 0 }) + ObjectCatalogIds(),
            NextFileId = latest.NextFileId + _ops.Sum(o => o switch
            {
                InsertOp or MergeOp => 1,
                DeleteOp d => d.Files.Count(f => f.NewFile is not null),
                RewriteOp r => r.Files.Count,
                FlushOp f => f.Files.Count + f.Files.Count(x => x.Deletes is not null) + f.Deletions.Count,
                _ => 0,
            }) + _ops.OfType<InlineInsertOp>().Select(i => i.Table).Distinct().Count()
              + _ops.OfType<InsertOp>().Where(i => i.File.Mapping is not null).Select(i => (i.Table, i.File.MappingKey)).Distinct().Count(),
        };
        await BulkInsert.InsertAsync(db, [snapshotRow], ct);

        // Pending IDs are per attempt: a retry re-assigns them from the new latest snapshot.
        foreach (var s in _schemas.Values.Where(s => s.Created)) s.SchemaId = null;
        foreach (var t in _tables.Values.Where(t => t.Created)) t.TableId = null;
        var versionedTables = new HashSet<PendingTable>();
        _partitionIds.Clear();

        // DDL, in order: rare, so one statement per change is fine. Rows are collected and written at the end.
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
                    rows.Schemas.Add(new SchemaEntity
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
                    rows.Tables.Add(new TableEntity
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
                    await db.SortInfos.Current().Where(x => x.TableId == id).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
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
                    // Spec ducklake_column_tag: "Dropping a column ends its active tag versions, including tags on child fields."
                    await db.ColumnTags.Current().Where(x => x.TableId == tableId && ids.Contains(x.ColumnId))
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
                    rows.Columns.Add(ColumnRow(r.Table, r.Column, r.Parent, r.Order));
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
                        rows.PartitionInfos.Add(new PartitionInfoEntity { PartitionId = partitionId, TableId = tableId, BeginSnapshot = snapshotId });
                        for (var i = 0; i < p.Keys.Count; i++)
                            rows.PartitionColumns.Add(new PartitionColumnEntity
                            {
                                PartitionId = partitionId, TableId = tableId, PartitionKeyIndex = i, ColumnId = p.Keys[i].ColumnId, Transform = p.Keys[i].Transform,
                            });
                        _partitionIds[p.Table] = partitionId;
                    }
                    else _partitionIds[p.Table] = null;
                    versionedTables.Add(p.Table);
                    break;
                }
                case var o when IsObjectOp(o):
                    nextCatalogId = await ApplyObjectAsync(db, o, snapshotId, latest.SnapshotId, nextCatalogId, rows, versionedTables, ct);
                    break;
            }
        }

        // Data: the state of every table written to, read in one query per catalog table; rows merged in memory.
        var inserts = _ops.OfType<InsertOp>().ToList();
        var inlineInserts = _ops.OfType<InlineInsertOp>().ToList();
        var deletes = _ops.OfType<DeleteOp>().ToList();
        // Tables receiving rows, in files or inlined.
        var receiving = inserts.Select(i => i.Table).Concat(inlineInserts.Select(i => i.Table)).Distinct().ToList();
        var dataTables = receiving.Select(t => t.TableId!.Value).Concat(deletes.Select(d => d.Table.TableId!.Value)).Distinct().ToList();
        var tableStats = dataTables.Count == 0 ? [] : await db.TableStats.Where(x => dataTables.Contains(x.TableId)).ToDictionaryAsync(x => x.TableId, ct);
        var newTableStats = new Dictionary<long, TableStatsEntity>();
        var statsDelta = new Dictionary<long, (long Records, long Rows, long Bytes)>();
        Dictionary<(long, long), TableColumnStatsEntity> columnStats = [];
        HashSet<long> tablesWithPriorFiles = [];
        if (receiving.Count > 0)
        {
            var insertTables = receiving.Select(t => t.TableId!.Value).ToList();
            var existingTables = receiving.Where(t => !t.Created).Select(t => t.TableId!.Value).ToList();
            columnStats = await db.TableColumnStats.Where(x => insertTables.Contains(x.TableId)).ToDictionaryAsync(x => (x.TableId, x.ColumnId), ct);
            if (existingTables.Count > 0)
            {
                tablesWithPriorFiles = (await db.DataFiles.Where(x => existingTables.Contains(x.TableId)).Select(x => x.TableId).Distinct().ToListAsync(ct)).ToHashSet();
                // No stats row yet: row IDs continue after any rows already in files (DuckDB creates the row lazily too).
                var unstated = existingTables.Where(id => !tableStats.ContainsKey(id)).ToList();
                if (unstated.Count > 0)
                    foreach (var x in await db.DataFiles.Where(f => unstated.Contains(f.TableId)).GroupBy(f => f.TableId)
                                 .Select(g => new { TableId = g.Key, End = g.Max(f => f.RowIdStart + f.RecordCount) }).ToListAsync(ct))
                        newTableStats[x.TableId] = new TableStatsEntity { TableId = x.TableId, RecordCount = 0, NextRowId = x.End ?? 0, FileSizeBytes = 0 };
            }
            var unpartitioned = inserts.Where(i => i.Table.PartitionKeys.Count > 0 && !_partitionIds.ContainsKey(i.Table)).Select(i => i.Table.TableId!.Value).Distinct().ToList();
            if (unpartitioned.Count > 0)
                foreach (var x in await db.PartitionInfos.LiveAt(latest.SnapshotId).Where(p => unpartitioned.Contains(p.TableId)).ToListAsync(ct))
                    _partitionIds.TryAdd(_tables.Values.First(t => t.TableId == x.TableId), x.PartitionId);
        }
        long NextRowId(long tableId)
        {
            var stats = tableStats.GetValueOrDefault(tableId) ?? newTableStats.GetValueOrDefault(tableId);
            return (stats?.NextRowId ?? 0) + statsDelta.GetValueOrDefault(tableId).Rows;
        }
        void AddDelta(long tableId, long records, long rowIds, long bytes)
        {
            var d = statsDelta.GetValueOrDefault(tableId);
            statsDelta[tableId] = (d.Records + records, d.Rows + rowIds, d.Bytes + bytes);
        }

        var mappingIds = new Dictionary<(PendingTable, string), long>();
        foreach (var i in inserts)
        {
            var t = i.Table;
            var f = i.File;
            var tableId = t.TableId!.Value;
            long? mappingId = null;
            if (f.Mapping is not null)
            {
                // As DuckDB: the mapping takes the next file ID, before its file's.
                if (!mappingIds.TryGetValue((t, f.MappingKey!), out var id))
                {
                    mappingIds[(t, f.MappingKey!)] = id = nextFileId++;
                    rows.ColumnMappings.Add(new ColumnMappingEntity { MappingId = id, TableId = tableId, Type = "map_by_name" });
                    rows.NameMappings.AddRange(f.Mapping.Select(m => new NameMappingEntity
                    {
                        MappingId = id, ColumnId = m.ColumnId, SourceName = m.SourceName, TargetFieldId = m.TargetFieldId, ParentColumn = m.ParentColumn, IsPartition = m.IsPartition,
                    }));
                }
                mappingId = id;
            }
            var fileId = nextFileId++;
            var rowIdStart = NextRowId(tableId);
            AddDelta(tableId, f.RecordCount, f.RecordCount, f.Size);
            rows.DataFiles.Add(new DataFileEntity
            {
                DataFileId = fileId, TableId = tableId, BeginSnapshot = snapshotId, FileOrder = fileId, Path = f.RelativePath, PathIsRelative = !f.Absolute, MappingId = mappingId,
                FileFormat = "parquet", RecordCount = f.RecordCount, FileSizeBytes = f.Size, FooterSize = f.FooterSize, RowIdStart = rowIdStart,
                PartitionId = t.PartitionKeys.Count == 0 ? null : _partitionIds.GetValueOrDefault(t), RowGroupCount = v11 ? f.RowGroups : null,
            });
            foreach (var s in f.Stats)
            {
                var exact = v11 && s.Min is not null ? true : (bool?)null;
                rows.FileColumnStats.Add(new FileColumnStatsEntity
                {
                    DataFileId = fileId, TableId = tableId, ColumnId = s.ColumnId, ColumnSizeBytes = s.ColumnSizeBytes, ValueCount = s.ValueCount,
                    NullCount = s.NullCount, MinValue = s.Min, MaxValue = s.Max, ContainsNan = s.ContainsNan, MinIsExact = exact, MaxIsExact = exact,
                });
                MergeColumnStats(t, s);
            }
            for (var k = 0; k < f.PartitionValues.Count; k++)
                rows.FilePartitionValues.Add(new FilePartitionValueEntity { DataFileId = fileId, TableId = tableId, PartitionKeyIndex = k, PartitionValue = f.PartitionValues[k] });
        }

        // Deletes before inlined inserts: an UPDATE's new inlined versions keep their rows' IDs, and the delete ends the old ones by ID.
        if (deletes.Count > 0) nextFileId = await DeleteAll(nextFileId);
        if (inlineInserts.Count > 0)
        {
            await InlineInsertsAsync();
            nextFileId += inlineInserts.Select(i => i.Table).Distinct().Count(); // DuckDB takes a file ID per inlining table
        }
        var merges = _ops.OfType<MergeOp>().ToList();
        var rewrites = _ops.OfType<RewriteOp>().ToList();
        if (merges.Count + rewrites.Count > 0) nextFileId = await CompactAll(nextFileId);
        var flushes = _ops.OfType<FlushOp>().ToList();
        if (flushes.Count > 0) nextFileId = await FlushAll(nextFileId);

        // Table statistics: existing rows by increment (one statement per table), new ones inserted.
        foreach (var (tableId, d) in statsDelta)
        {
            if (tableStats.ContainsKey(tableId))
            {
                await db.TableStats.Where(x => x.TableId == tableId).ExecuteUpdateAsync(u => u
                    .SetProperty(x => x.RecordCount, x => (x.RecordCount ?? 0) + d.Records)
                    .SetProperty(x => x.NextRowId, x => (x.NextRowId ?? 0) + d.Rows)
                    .SetProperty(x => x.FileSizeBytes, x => (x.FileSizeBytes ?? 0) + d.Bytes), ct);
                continue;
            }
            if (d.Rows == 0) continue; // a delete from a table with no stats row (inlined rows only) leaves it to the next insert
            var created = newTableStats.GetValueOrDefault(tableId) ?? new TableStatsEntity { TableId = tableId, RecordCount = 0, NextRowId = 0, FileSizeBytes = 0 };
            rows.TableStats.Add(new TableStatsEntity
            {
                TableId = tableId, RecordCount = created.RecordCount + d.Records, NextRowId = created.NextRowId + d.Rows, FileSizeBytes = created.FileSizeBytes + d.Bytes,
            });
        }
        // Column statistics of the tables inserted into: replaced as a whole (the table is not versioned).
        if (receiving.Count > 0)
        {
            var insertTables = receiving.Select(t => t.TableId!.Value).ToList();
            await db.TableColumnStats.Where(x => insertTables.Contains(x.TableId)).ExecuteDeleteAsync(ct);
            rows.TableColumnStats.AddRange(columnStats.Values);
        }

        foreach (var t in versionedTables)
            rows.SchemaVersions.Add(new SchemaVersionEntity { BeginSnapshot = snapshotId, SchemaVersion = schemaVersion, TableId = t.TableId!.Value });

        // Changes are recorded with the IDs just assigned (a table created here is named, not numbered).
        var final = Changes();
        foreach (var i in inserts.Where(i => i.Table.Created)) final.InsertedTables.Add(i.Table.TableId!.Value);
        foreach (var i in inlineInserts.Where(i => i.Table.Created)) final.InlinedInsertTables.Add(i.Table.TableId!.Value);
        // A flush that wrote delete files records a delete too, as DuckDB does (its own conflict check above did not count it).
        foreach (var f in flushes.Where(f => f.Deletions.Count > 0 || f.Files.Any(x => x.Deletes is not null))) final.DeletedTables.Add(f.Table.TableId!.Value);
        var changes = string.Join(",", final.Entries());
        if (nextCatalogId != snapshotRow.NextCatalogId || nextFileId != snapshotRow.NextFileId)
            throw new InvalidOperationException($"Snapshot {snapshotId}: assigned IDs up to {nextCatalogId}/{nextFileId}, recorded {snapshotRow.NextCatalogId}/{snapshotRow.NextFileId}");
        rows.SnapshotChanges.Add(new SnapshotChangesEntity
        {
            SnapshotId = snapshotId, ChangesMade = changes, Author = Author, CommitMessage = CommitMessage, CommitExtraInfo = CommitExtraInfo,
        });
        await rows.WriteAsync(db, ct);
        return new DuckLakeSnapshot(snapshotId, time, schemaVersion, nextCatalogId, nextFileId)
        {
            Changes = DuckLakeChange.Parse(changes), Author = Author, CommitMessage = CommitMessage, CommitExtraInfo = CommitExtraInfo,
        };

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
            rows.Columns.Add(ColumnRow(table, col, parent, order));
            foreach (var child in col.Children) AddColumnRows(table, child, col.Id, child.Id);
        }

        void MergeColumnStats(PendingTable t, DuckLakeColumnStats s)
        {
            var tableId = t.TableId!.Value;
            var type = t.Columns.SelectMany(x => x.SelfAndDescendants()).First(x => x.Id == s.ColumnId).Type as DuckLakePrimitive;
            if (!columnStats.TryGetValue((tableId, s.ColumnId), out var existing))
            {
                // A table that already had rows without a stats row for this column (added column, older writer) has
                // unknown bounds: only a table without earlier files starts from this file's exact bounds.
                var priorRows = tablesWithPriorFiles.Contains(tableId);
                var exact = v11 && !priorRows && s.Min is not null ? true : (bool?)null;
                columnStats[(tableId, s.ColumnId)] = new TableColumnStatsEntity
                {
                    TableId = tableId, ColumnId = s.ColumnId, ContainsNull = priorRows || s.NullCount > 0, ContainsNan = s.ContainsNan,
                    MinValue = priorRows ? null : s.Min, MaxValue = priorRows ? null : s.Max, MinIsExact = exact, MaxIsExact = exact,
                };
                return;
            }
            var min = Widen(type, existing.MinValue, s.Min, lower: true);
            var max = Widen(type, existing.MaxValue, s.Max, lower: false);
            // A bound stays exact when it is one of the exact bounds it was chosen from.
            existing.MinIsExact = !v11 || min is null ? null : min == existing.MinValue ? existing.MinIsExact : true;
            existing.MaxIsExact = !v11 || max is null ? null : max == existing.MaxValue ? existing.MaxIsExact : true;
            existing.MinValue = min;
            existing.MaxValue = max;
            existing.ContainsNull = (existing.ContainsNull ?? true) || s.NullCount > 0;
            existing.ContainsNan = existing.ContainsNan is null || s.ContainsNan is null ? null : existing.ContainsNan.Value || s.ContainsNan.Value;
        }

        // Inlined rows: into the inlined data table of the table's current schema version (created on first use, as
        // DuckDB does), with new row IDs from next_row_id unless they keep theirs (an UPDATE's new versions).
        async Task InlineInsertsAsync()
        {
            foreach (var group in inlineInserts.GroupBy(i => i.Table))
            {
                var t = group.Key;
                var tableId = t.TableId!.Value;
                var version = versionedTables.Contains(t) ? schemaVersion
                    : await db.SchemaVersions.Where(v => v.TableId == tableId).MaxAsync(v => (long?)v.SchemaVersion, ct) ?? latest.SchemaVersion;
                var name = await db.InlinedDataTables.Where(x => x.TableId == tableId && x.SchemaVersion == version).Select(x => x.TableName).FirstOrDefaultAsync(ct);
                if (name is null)
                {
                    name = $"ducklake_inlined_data_{tableId}_{version}";
                    await Raw(DuckLakeInlinedData.CreateTableSql(db.Dialect, v11, name, t.Columns));
                    rows.InlinedDataTables.Add(new InlinedDataTableEntity { TableId = tableId, TableName = name, SchemaVersion = version });
                }
                var data = new List<(long RowId, long Begin, long? End, object?[] Values)>();
                var stats = new StatsCollector(t.Columns);
                foreach (var (values, kept) in group.SelectMany(op => op.Rows))
                {
                    long rowId;
                    if (kept is { } k) rowId = k;
                    else
                    {
                        rowId = NextRowId(tableId);
                        AddDelta(tableId, 0, 1, 0);
                    }
                    data.Add((rowId, snapshotId, null, values));
                    stats.Add(values);
                }
                AddDelta(tableId, data.Count, 0, 0);
                await DuckLakeInlinedData.InsertAsync(db, name, t.Columns, data, ct);
                foreach (var s in stats.Result(null)) MergeColumnStats(t, s);
            }
        }

        async Task<long> DeleteAll(long fileId)
        {
            // The delete state of every data file deleted from, in two queries: each must still be what the delete was
            // planned against, or another snapshot deleted from the same data file ("Two snapshots delete from the same
            // underlying data file").
            var fileIds = deletes.SelectMany(d => d.Files).Select(f => f.DataFileId).ToList();
            var ended = fileIds.Count == 0 ? [] : (await db.DataFiles.Where(x => fileIds.Contains(x.DataFileId)).Select(x => new { x.DataFileId, x.EndSnapshot }).ToListAsync(ct))
                .ToDictionary(x => x.DataFileId, x => x.EndSnapshot);
            var current = fileIds.Count == 0 ? [] : (await db.DeleteFiles.Current().Where(x => fileIds.Contains(x.DataFileId)).Select(x => new { x.DataFileId, x.DeleteFileId }).ToListAsync(ct))
                .GroupBy(x => x.DataFileId).ToDictionary(g => g.Key, g => g.Select(x => x.DeleteFileId).ToList());
            var endDeleteFiles = new List<long>();
            var endDataFiles = new List<long>();
            var inlineDeletes = new List<(long TableId, long DataFileId, IReadOnlyList<long> Positions)>();
            foreach (var op in deletes)
            {
                var tableId = op.Table.TableId!.Value;
                if (op.Files.Count > 0) await CheckInlinedDeletionsAsync(tableId, op.Files);
                foreach (var f in op.Files)
                {
                    var now = current.GetValueOrDefault(f.DataFileId) ?? [];
                    long? currentDelete = now.Count == 1 ? now[0] : null;
                    if (!ended.TryGetValue(f.DataFileId, out var end) || end is not null || now.Count > 1 || currentDelete != f.OldDeleteFileId)
                        throw new DuckLakeConflictException($"Data file {f.DataFileId} was deleted from or removed concurrently");
                    if (f.Inline is { } positions)
                    {
                        inlineDeletes.Add((tableId, f.DataFileId, positions));
                        continue;
                    }
                    if (f.OldDeleteFileId is { } old) endDeleteFiles.Add(old);
                    if (f.NewFile is null)
                    {
                        endDataFiles.Add(f.DataFileId);
                        continue;
                    }
                    rows.DeleteFiles.Add(new DeleteFileEntity
                    {
                        DeleteFileId = fileId++, TableId = tableId, BeginSnapshot = snapshotId, DataFileId = f.DataFileId, Path = f.NewFile.RelativePath,
                        PathIsRelative = true, Format = f.NewFile.Format, DeleteCount = f.NewFile.Count, FileSizeBytes = f.NewFile.Size, FooterSize = f.NewFile.FooterSize,
                        RowGroupCount = v11 && f.NewFile.Format == "parquet" ? 1 : null,
                    });
                }
                var (rowIdColumn, beginColumn, endColumn) = DuckLakeInlinedData.SystemColumns(v11);
                foreach (var (inlinedTable, rowIds) in op.Inlined)
                {
                    foreach (var chunk in rowIds.Chunk(1000))
                    {
                        var list = string.Join(", ", chunk.Select(id => id.ToString(CultureInfo.InvariantCulture)));
                        var (c, tx) = db.Raw();
                        var updated = await Db.ExecAsync(c, tx,
                            $"UPDATE {Db.Quote(inlinedTable)} SET {endColumn} = @p0 WHERE {endColumn} IS NULL AND {beginColumn} < @p0 AND {rowIdColumn} IN ({list})", ct, snapshotId);
                        if (updated != chunk.Length) throw new DuckLakeConflictException($"Inlined rows of {inlinedTable} were deleted concurrently");
                    }
                }
                AddDelta(tableId, -op.Count, 0, 0);
            }
            if (endDeleteFiles.Count > 0)
                await db.DeleteFiles.Current().Where(x => endDeleteFiles.Contains(x.DeleteFileId)).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
            if (endDataFiles.Count > 0)
                await db.DataFiles.Current().Where(x => endDataFiles.Contains(x.DataFileId)).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
            foreach (var group in inlineDeletes.GroupBy(d => d.TableId))
            {
                await Raw(DuckLakeInlinedData.CreateDeleteTableSql(group.Key));
                await DuckLakeInlinedData.InsertDeletionsAsync(db, group.Key, group.SelectMany(d => d.Positions.Select(p => (d.DataFileId, p))).ToList(), snapshotId, ct);
            }
            return fileId;
        }

        // Compactions (spec "Conflict Resolution", Compaction): each input file must be as it was when the compaction
        // read it; merged inputs leave the catalog (their rows live on in the partial file) and are scheduled for
        // deletion, rewritten inputs end at this snapshot (older snapshots still read them).
        async Task<long> CompactAll(long fileId)
        {
            var inputIds = merges.SelectMany(m => m.Inputs).Concat(rewrites.SelectMany(r => r.Inputs.Select(i => i.DataFileId))).ToList();
            var state = (await db.DataFiles.Where(x => inputIds.Contains(x.DataFileId))
                    .Select(x => new { x.DataFileId, x.EndSnapshot, x.Path, x.PathIsRelative }).ToListAsync(ct))
                .ToDictionary(x => x.DataFileId);
            var current = (await db.DeleteFiles.Current().Where(x => inputIds.Contains(x.DataFileId)).Select(x => new { x.DataFileId, x.DeleteFileId }).ToListAsync(ct))
                .GroupBy(x => x.DataFileId).ToDictionary(g => g.Key, g => g.Select(x => x.DeleteFileId).ToList());
            var removed = new List<long>();
            var endData = new List<long>();
            var endDeletes = new List<long>();
            foreach (var m in merges)
            {
                var tableId = m.Table.TableId!.Value;
                await CheckInlinedDeletionsOfAsync(tableId, m.Inputs);
                foreach (var id in m.Inputs)
                {
                    if (!state.TryGetValue(id, out var f) || f.EndSnapshot is not null || current.ContainsKey(id))
                        throw new DuckLakeConflictException($"Data file {id} was deleted from, compacted or removed concurrently");
                    removed.Add(id);
                    rows.ScheduleDeletion(_catalog, id, DuckLakeCatalog.Resolve(m.Table.Location, f.Path, f.PathIsRelative ?? true));
                }
                AddFile(m.Table, fileId++, m.File, m.BeginSnapshot, m.PartialMax, m.RowIdStart, m.PartitionId);
            }
            foreach (var r in rewrites)
            {
                var tableId = r.Table.TableId!.Value;
                await CheckInlinedDeletionsOfAsync(tableId, r.Inputs.Select(i => i.DataFileId).ToList());
                foreach (var (id, deleteFile) in r.Inputs)
                {
                    var now = current.GetValueOrDefault(id) ?? [];
                    long? currentDelete = now.Count == 1 ? now[0] : null;
                    if (!state.TryGetValue(id, out var f) || f.EndSnapshot is not null || now.Count > 1 || currentDelete != deleteFile)
                        throw new DuckLakeConflictException($"Data file {id} was deleted from, compacted or removed concurrently");
                    endData.Add(id);
                    if (deleteFile is { } d) endDeletes.Add(d);
                }
                foreach (var (file, partitionId) in r.Files) AddFile(r.Table, fileId++, file, snapshotId, null, null, partitionId);
            }
            if (removed.Count > 0)
            {
                await db.FileColumnStats.Where(x => removed.Contains(x.DataFileId)).ExecuteDeleteAsync(ct);
                await db.FilePartitionValues.Where(x => removed.Contains(x.DataFileId)).ExecuteDeleteAsync(ct);
                await db.DataFiles.Where(x => removed.Contains(x.DataFileId)).ExecuteDeleteAsync(ct);
            }
            if (endDeletes.Count > 0)
                await db.DeleteFiles.Current().Where(x => endDeletes.Contains(x.DeleteFileId)).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
            if (endData.Count > 0)
                await db.DataFiles.Current().Where(x => endData.Contains(x.DataFileId)).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
            return fileId;
        }

        // Flushes: the inlined rows read at staging leave the inlined tables (rows inserted since stay), their files and
        // partial delete files come in; merged inlined deletions replace the data file's delete file.
        async Task<long> FlushAll(long fileId)
        {
            var (rowIdColumn, beginColumn, endColumn) = DuckLakeInlinedData.SystemColumns(v11);
            var baseId = _base!.Value;
            foreach (var f in flushes)
            {
                var t = f.Table;
                var tableId = t.TableId!.Value;
                foreach (var name in f.InlinedTables)
                {
                    var (c, tx) = db.Raw();
                    var deletedSince = await Db.QueryAsync(c, tx, $"SELECT count(*) FROM {Db.Quote(name)} WHERE {beginColumn} <= @p0 AND {endColumn} > @p0",
                        r => Db.ReqLong(r, 0), ct, null, baseId);
                    if (deletedSince[0] > 0) throw new DuckLakeConflictException($"Inlined rows of {name} were deleted while flushing them");
                    await Raw($"DELETE FROM {Db.Quote(name)} WHERE {beginColumn} <= @p0", baseId);
                }
                foreach (var flushed in f.Files)
                {
                    var dataFileId = fileId++;
                    AddFile(t, dataFileId, flushed.File, flushed.Begin, flushed.PartialMax, flushed.RowIdStart, null);
                    if (flushed.Deletes is { } d)
                        rows.DeleteFiles.Add(DeleteRow(tableId, fileId++, dataFileId, d, flushed.DeletesBegin, flushed.DeletesPartialMax));
                }
                if (f.Deletions.Count == 0) continue;
                var ids = f.Deletions.Select(d => d.DataFileId).ToList();
                await CheckInlinedDeletionsOfAsync(tableId, ids);
                var live = (await db.DataFiles.Where(x => ids.Contains(x.DataFileId) && x.EndSnapshot == null).Select(x => x.DataFileId).ToListAsync(ct)).ToHashSet();
                var current = (await db.DeleteFiles.Current().Where(x => ids.Contains(x.DataFileId)).Select(x => new { x.DataFileId, x.DeleteFileId }).ToListAsync(ct))
                    .GroupBy(x => x.DataFileId).ToDictionary(g => g.Key, g => g.Select(x => x.DeleteFileId).ToList());
                var replaced = new List<long>();
                foreach (var d in f.Deletions)
                {
                    var now = current.GetValueOrDefault(d.DataFileId) ?? [];
                    if (!live.Contains(d.DataFileId) || now.Count > 1 || (now.Count == 1 ? now[0] : (long?)null) != d.OldDeleteFileId)
                        throw new DuckLakeConflictException($"Data file {d.DataFileId} was deleted from, compacted or removed concurrently");
                    if (d.OldDeleteFileId is { } old)
                    {
                        replaced.Add(old);
                        rows.ScheduleDeletion(_catalog, old, d.OldDeleteLocation!);
                    }
                    rows.DeleteFiles.Add(DeleteRow(tableId, fileId++, d.DataFileId, d.File, d.Begin, d.PartialMax));
                }
                if (replaced.Count > 0) await db.DeleteFiles.Where(x => replaced.Contains(x.DeleteFileId)).ExecuteDeleteAsync(ct);
                await Raw($"DELETE FROM {Db.Quote(DuckLakeCatalog.InlinedDeleteTable(tableId))} WHERE begin_snapshot <= @p0 AND file_id IN " +
                          $"({string.Join(", ", ids.Select(i => i.ToString(CultureInfo.InvariantCulture)))})", baseId);
            }
            return fileId;
        }

        DeleteFileEntity DeleteRow(long tableId, long deleteFileId, long dataFileId, WrittenDelete d, long begin, long? partialMax) => new()
        {
            DeleteFileId = deleteFileId, TableId = tableId, BeginSnapshot = begin, DataFileId = dataFileId, Path = d.RelativePath, PathIsRelative = true,
            Format = "parquet", DeleteCount = d.Count, FileSizeBytes = d.Size, FooterSize = d.FooterSize, PartialMax = partialMax, RowGroupCount = v11 ? 1 : null,
        };

        void AddFile(PendingTable t, long fileId, WrittenFile f, long begin, long? partialMax, long? rowIdStart, long? partitionId)
        {
            var tableId = t.TableId!.Value;
            rows.DataFiles.Add(new DataFileEntity
            {
                DataFileId = fileId, TableId = tableId, BeginSnapshot = begin, FileOrder = fileId, Path = f.RelativePath, PathIsRelative = true,
                FileFormat = "parquet", RecordCount = f.RecordCount, FileSizeBytes = f.Size, FooterSize = f.FooterSize, RowIdStart = rowIdStart,
                PartitionId = partitionId, PartialMax = partialMax, RowGroupCount = v11 ? f.RowGroups : null,
            });
            foreach (var s in f.Stats)
            {
                var exact = v11 && s.Min is not null ? true : (bool?)null;
                rows.FileColumnStats.Add(new FileColumnStatsEntity
                {
                    DataFileId = fileId, TableId = tableId, ColumnId = s.ColumnId, ColumnSizeBytes = s.ColumnSizeBytes, ValueCount = s.ValueCount,
                    NullCount = s.NullCount, MinValue = s.Min, MaxValue = s.Max, ContainsNan = s.ContainsNan, MinIsExact = exact, MaxIsExact = exact,
                });
            }
            for (var k = 0; k < f.PartitionValues.Count; k++)
                rows.FilePartitionValues.Add(new FilePartitionValueEntity { DataFileId = fileId, TableId = tableId, PartitionKeyIndex = k, PartitionValue = f.PartitionValues[k] });
        }

        Task CheckInlinedDeletionsAsync(long tableId, IReadOnlyList<FileDelete> files) => CheckInlinedDeletionsOfAsync(tableId, files.Select(f => f.DataFileId).ToList());

        async Task CheckInlinedDeletionsOfAsync(long tableId, IReadOnlyList<long> files)
        {
            var inlinedDeletes = DuckLakeCatalog.InlinedDeleteTable(tableId);
            if (files.Count == 0 || !await DuckLakeCatalog.TableExistsAsync(db, inlinedDeletes, ct)) return;
            var ids = string.Join(", ", files.Select(f => f.ToString(CultureInfo.InvariantCulture)));
            var (c, tx) = db.Raw();
            var concurrent = await Db.QueryAsync(c, tx, $"SELECT count(*) FROM {Db.Quote(inlinedDeletes)} WHERE begin_snapshot > @p0 AND file_id IN ({ids})",
                r => Db.ReqLong(r, 0), ct, null, _base);
            if (concurrent[0] > 0) throw new DuckLakeConflictException($"Rows of table {tableId} were deleted (inlined) from the same data files concurrently");
        }
    }

    /// <summary>The catalog rows a commit inserts after its snapshot row, written with one multi-row statement per table.</summary>
    private sealed class CatalogRows
    {
        public readonly List<SchemaEntity> Schemas = [];
        public readonly List<TableEntity> Tables = [];
        public readonly List<ColumnEntity> Columns = [];
        public readonly List<PartitionInfoEntity> PartitionInfos = [];
        public readonly List<PartitionColumnEntity> PartitionColumns = [];
        public readonly List<DataFileEntity> DataFiles = [];
        public readonly List<FileColumnStatsEntity> FileColumnStats = [];
        public readonly List<FilePartitionValueEntity> FilePartitionValues = [];
        public readonly List<DeleteFileEntity> DeleteFiles = [];
        public readonly List<TableStatsEntity> TableStats = [];
        public readonly List<TableColumnStatsEntity> TableColumnStats = [];
        public readonly List<SchemaVersionEntity> SchemaVersions = [];
        public readonly List<SnapshotChangesEntity> SnapshotChanges = [];
        public readonly List<FileScheduledForDeletionEntity> FilesScheduled = [];
        public readonly List<InlinedDataTableEntity> InlinedDataTables = [];
        public readonly List<TagEntity> Tags = [];
        public readonly List<ColumnTagEntity> ColumnTags = [];
        public readonly List<ViewColumnTagEntity> ViewColumnTags = [];
        public readonly List<ViewEntity> Views = [];
        public readonly List<MacroEntity> Macros = [];
        public readonly List<MacroImplEntity> MacroImpls = [];
        public readonly List<MacroParameterEntity> MacroParameters = [];
        public readonly List<SortInfoEntity> SortInfos = [];
        public readonly List<SortExpressionEntity> SortExpressions = [];
        public readonly List<ColumnMappingEntity> ColumnMappings = [];
        public readonly List<NameMappingEntity> NameMappings = [];

        /// <summary>A file no snapshot reads any more, recorded as DuckDB does: relative to <c>data_path</c> when under it.</summary>
        public void ScheduleDeletion(DuckLakeCatalog catalog, long fileId, string location) =>
            FilesScheduled.Add(catalog.ScheduledFile(fileId, location, DateTimeOffset.UtcNow));

        public async Task WriteAsync(DuckLakeDbContext db, CancellationToken ct)
        {
            await BulkInsert.InsertAsync(db, Schemas, ct);
            await BulkInsert.InsertAsync(db, Tables, ct);
            await BulkInsert.InsertAsync(db, Columns, ct);
            await BulkInsert.InsertAsync(db, PartitionInfos, ct);
            await BulkInsert.InsertAsync(db, PartitionColumns, ct);
            await BulkInsert.InsertAsync(db, ColumnMappings, ct);
            await BulkInsert.InsertAsync(db, NameMappings, ct);
            await BulkInsert.InsertAsync(db, DataFiles, ct);
            await BulkInsert.InsertAsync(db, FileColumnStats, ct);
            await BulkInsert.InsertAsync(db, FilePartitionValues, ct);
            await BulkInsert.InsertAsync(db, DeleteFiles, ct);
            await BulkInsert.InsertAsync(db, TableStats, ct);
            await BulkInsert.InsertAsync(db, TableColumnStats, ct);
            await BulkInsert.InsertAsync(db, SchemaVersions, ct);
            await BulkInsert.InsertAsync(db, SnapshotChanges, ct);
            await BulkInsert.InsertAsync(db, FilesScheduled, ct);
            await BulkInsert.InsertAsync(db, InlinedDataTables, ct);
            await BulkInsert.InsertAsync(db, Tags, ct);
            await BulkInsert.InsertAsync(db, ColumnTags, ct);
            await BulkInsert.InsertAsync(db, ViewColumnTags, ct);
            await BulkInsert.InsertAsync(db, Views, ct);
            await BulkInsert.InsertAsync(db, Macros, ct);
            await BulkInsert.InsertAsync(db, MacroImpls, ct);
            await BulkInsert.InsertAsync(db, MacroParameters, ct);
            await BulkInsert.InsertAsync(db, SortInfos, ct);
            await BulkInsert.InsertAsync(db, SortExpressions, ct);
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
