using Iceberg.Net.Types;
using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Tags and comments, table renames, views, macros and sort orders: catalog rows only (no data), written as DuckDB 1.5.5
/// writes them. Each bumps the snapshot's <c>schema_version</c>; DDL that changes a table's columns or name also adds a
/// <c>ducklake_schema_versions</c> row, comments and sort orders do not (as DuckDB).
/// </summary>
public sealed partial class DuckLakeTransaction
{
    private readonly Dictionary<(string Schema, string View), PendingView> _views = [];
    private readonly Dictionary<(string Schema, string Macro), PendingMacro> _macros = [];

    private sealed class PendingView
    {
        public long? ViewId;
        public required PendingSchema Schema;
        public required string Name;
        public required string Sql;
        public required string Dialect;
        public IReadOnlyList<string>? Aliases;
        public Guid Uuid = Guid.NewGuid();
        public bool Created, Dropped;
    }

    private sealed class PendingMacro
    {
        public long? MacroId;
        public required PendingSchema Schema;
        public required string Name;
        public required bool IsTable;
        public IReadOnlyList<DuckLakeMacroImplementation> Implementations = [];
        public bool Created, Dropped;
    }

    private sealed record SetTagOp(PendingTable? Table, PendingView? View, string Key, string? Value) : Op;
    private sealed record SetColumnTagOp(PendingTable Table, long ColumnId, string Key, string? Value) : Op;
    private sealed record SetViewColumnTagOp(PendingView View, string Column, string Key, string? Value) : Op;
    private sealed record RenameTableOp(PendingTable Table, string OldName, string NewName) : Op;
    private sealed record SetSortOp(PendingTable Table, IReadOnlyList<DuckLakeSortKey> Keys) : Op;
    private sealed record CreateViewOp(PendingView View) : Op;
    private sealed record RenameViewOp(PendingView View, string OldName, string NewName) : Op;
    private sealed record DropViewOp(PendingView View) : Op;
    private sealed record CreateMacroOp(PendingMacro Macro) : Op;
    private sealed record DropMacroOp(PendingMacro Macro) : Op;

    private static bool IsObjectOp(Op op) => op is SetTagOp or SetColumnTagOp or SetViewColumnTagOp or RenameTableOp or SetSortOp or CreateViewOp
        or RenameViewOp or DropViewOp or CreateMacroOp or DropMacroOp;

    // ---- tags and comments -----------------------------------------------------------------------------------------

    /// <summary>Set (or with null, remove) a tag of a table; SQL comments are the <c>comment</c> key.</summary>
    public async Task SetTagAsync(string schema, string table, string key, string? value, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        _ops.Add(new SetTagOp(await TableAsync(db, schema, table, ct), null, key, value));
    }

    public Task SetCommentAsync(string schema, string table, string? comment, CancellationToken ct = default) => SetTagAsync(schema, table, "comment", comment, ct);

    /// <summary>Set (or with null, remove) a tag of a table's column (a nested field by its top-level column's name and the path, <c>a.b</c>).</summary>
    public async Task SetColumnTagAsync(string schema, string table, string column, string key, string? value, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var t = await TableAsync(db, schema, table, ct);
        var parts = column.Split('.');
        var col = Column(t, parts[0]);
        foreach (var part in parts[1..])
            col = DuckLakeCatalog.Find(col.Children, c => c.Name, part, $"field of {col.Name}");
        _ops.Add(new SetColumnTagOp(t, col.Id, key, value));
    }

    public Task SetColumnCommentAsync(string schema, string table, string column, string? comment, CancellationToken ct = default) =>
        SetColumnTagAsync(schema, table, column, "comment", comment, ct);

    /// <summary>Set (or with null, remove) a tag of a view.</summary>
    public async Task SetViewTagAsync(string schema, string view, string key, string? value, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        _ops.Add(new SetTagOp(null, await ViewAsync(db, schema, view, ct), key, value));
    }

    public Task SetViewCommentAsync(string schema, string view, string? comment, CancellationToken ct = default) => SetViewTagAsync(schema, view, "comment", comment, ct);

    /// <summary>Set (or remove) a tag of a view's column, named as the view's query produces it (1.1: <c>ducklake_view_column_tag</c>).</summary>
    public async Task SetViewColumnTagAsync(string schema, string view, string column, string key, string? value, CancellationToken ct = default)
    {
        Require11("A view column tag");
        await using var db = await BeginAsync(ct);
        _ops.Add(new SetViewColumnTagOp(await ViewAsync(db, schema, view, ct), column, key, value));
    }

    // ---- tables: rename, sort order ----------------------------------------------------------------------------------

    /// <summary>Rename a table: a new version of its <c>ducklake_table</c> row (same ID and files; DuckDB records <c>created_table</c> with the new name).</summary>
    public async Task RenameTableAsync(string schema, string table, string newName, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var t = await AlterableAsync(db, schema, table, ct, allowCreated: true);
        await RequireFreeNameAsync(db, t.Schema, newName, ct);
        var old = t.Name;
        _tables.Remove((t.Schema.Name, old));
        t.Name = newName;
        _tables[(t.Schema.Name, newName)] = t;
        // A table created here has not written anything yet (AlterableAsync): its directory follows its name.
        if (t.Created) t.Location = t.Schema.Location + PathSegment(newName);
        else
        {
            var index = _ops.FindIndex(o => o is RenameTableOp r && r.Table == t);
            if (index >= 0) _ops[index] = ((RenameTableOp)_ops[index]) with { NewName = newName };
            else _ops.Add(new RenameTableOp(t, old, newName));
        }
    }

    /// <summary>
    /// Set the order new data of a table is sorted in (an empty list removes it): DuckDB's <c>SET SORTED BY</c>. Keys that
    /// are column names order the files this library writes; other expressions are recorded for engines that evaluate them.
    /// </summary>
    public async Task SetSortOrderAsync(string schema, string table, IReadOnlyList<DuckLakeSortKey> keys, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var t = await AlterableAsync(db, schema, table, ct, allowCreated: true);
        t.SortKeys = keys;
        _ops.Add(new SetSortOp(t, keys));
    }

    /// <summary>
    /// Rows in the table's sort order, as far as its keys are column names (stable: rows equal on every key keep their
    /// order). Other keys end the usable prefix: sorting past them would not be the order the table asks for.
    /// </summary>
    private static List<object?[]> Sorted(PendingTable t, List<object?[]> rows)
    {
        var keys = new List<(int Index, bool Descending, bool NullsFirst)>();
        foreach (var k in t.SortKeys)
        {
            var name = k.Expression.Trim();
            if (name.Length > 1 && name[0] == '"' && name[^1] == '"') name = name[1..^1].Replace("\"\"", "\"");
            var index = t.Columns.FindIndex(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase) && c.Type is DuckLakePrimitive);
            if (index < 0) break;
            keys.Add((index, k.Descending, k.NullsFirst));
        }
        if (keys.Count == 0) return rows;
        return [.. rows.Order(Comparer<object?[]>.Create((a, b) =>
        {
            foreach (var (i, desc, nullsFirst) in keys)
            {
                var (x, y) = (a[i], b[i]);
                int c;
                if (x is null || y is null) c = x is null && y is null ? 0 : (x is null) == nullsFirst ? -1 : 1;
                else c = desc ? DuckLakeValues.Compare(y, x) : DuckLakeValues.Compare(x, y);
                if (c != 0) return c;
            }
            return 0;
        }))];
    }

    // ---- views -------------------------------------------------------------------------------------------------------

    /// <summary>
    /// Create a view: its SQL in <paramref name="dialect"/>, stored as given (refer to the lake's own catalog as
    /// <c>{DUCKLAKE_CATALOG}</c>, as DuckDB does, or leave names unqualified).
    /// </summary>
    public async Task CreateViewAsync(string schema, string name, string sql, IReadOnlyList<string>? columnAliases = null, string dialect = "duckdb",
        CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var sc = await SchemaAsync(db, schema, ct);
        await RequireFreeNameAsync(db, sc, name, ct);
        var view = new PendingView { Schema = sc, Name = name, Sql = sql, Dialect = dialect, Aliases = columnAliases, Created = true };
        _views[(sc.Name, name)] = view;
        _ops.Add(new CreateViewOp(view));
    }

    public async Task RenameViewAsync(string schema, string name, string newName, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var view = await ViewAsync(db, schema, name, ct);
        if (view.Created) throw new InvalidOperationException($"View {schema}.{name} is created in this transaction");
        await RequireFreeNameAsync(db, view.Schema, newName, ct);
        _views.Remove((view.Schema.Name, view.Name));
        var old = view.Name;
        view.Name = newName;
        _views[(view.Schema.Name, newName)] = view;
        var index = _ops.FindIndex(o => o is RenameViewOp r && r.View == view);
        if (index >= 0) _ops[index] = ((RenameViewOp)_ops[index]) with { NewName = newName };
        else _ops.Add(new RenameViewOp(view, old, newName));
    }

    public async Task DropViewAsync(string schema, string name, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var view = await ViewAsync(db, schema, name, ct);
        if (view.Created) throw new InvalidOperationException($"View {schema}.{name} is created in this transaction");
        view.Dropped = true;
        _ops.Add(new DropViewOp(view));
    }

    private async Task<PendingView> ViewAsync(DuckLakeDbContext db, string schema, string name, CancellationToken ct)
    {
        var sc = await SchemaAsync(db, schema, ct);
        if (_views.TryGetValue((sc.Name, name), out var pending))
            return pending.Dropped ? throw new DuckLakeNotFoundException($"View {schema}.{name} is dropped in this transaction") : pending;
        var row = DuckLakeCatalog.Find((await DuckLakeCatalog.ViewsAsync(db, _base!.Value, ct)).Where(v => v.SchemaId == sc.SchemaId), v => v.Name, name,
            $"view in schema {sc.Name}");
        pending = new PendingView
        {
            ViewId = row.ViewId, Schema = sc, Name = row.Name, Sql = row.Sql, Dialect = row.Dialect, Uuid = row.Uuid,
            Aliases = Corruption.Guard("ducklake_view.column_aliases", () => DuckLakeCatalog.ParseAliases(row.Aliases)),
        };
        return _views[(sc.Name, row.Name)] = pending;
    }

    /// <summary>Tables and views share a schema's names (as in DuckDB).</summary>
    private async Task RequireFreeNameAsync(DuckLakeDbContext db, PendingSchema sc, string name, CancellationToken ct)
    {
        bool Pending() => _tables.TryGetValue((sc.Name, name), out var t) && !t.Dropped || _views.TryGetValue((sc.Name, name), out var v) && !v.Dropped;
        bool Gone(string n) => _tables.TryGetValue((sc.Name, n), out var t) && (t.Dropped || t.Name != n) || _views.TryGetValue((sc.Name, n), out var v) && (v.Dropped || v.Name != n);
        if (Pending()) throw new InvalidOperationException($"{sc.Name}.{name} already exists");
        if (sc.Created) return;
        var live = (await DuckLakeCatalog.TablesAsync(db, _base!.Value, ct)).Where(t => t.SchemaId == sc.SchemaId).Select(t => t.Name)
            .Concat((await DuckLakeCatalog.ViewsAsync(db, _base!.Value, ct)).Where(v => v.SchemaId == sc.SchemaId).Select(v => v.Name));
        if (live.Any(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase) && !Gone(n))) throw new InvalidOperationException($"{sc.Name}.{name} already exists");
    }

    // ---- macros ------------------------------------------------------------------------------------------------------

    /// <summary>Create a macro with one or more implementations (all scalar or all table macros), stored as given.</summary>
    public async Task CreateMacroAsync(string schema, string name, IReadOnlyList<DuckLakeMacroImplementation> implementations, CancellationToken ct = default)
    {
        if (implementations.Count == 0) throw new ArgumentException("A macro needs an implementation", nameof(implementations));
        if (implementations.Select(i => i.IsTableMacro).Distinct().Count() > 1) throw new ArgumentException("A macro's implementations are all scalar or all table macros");
        await using var db = await BeginAsync(ct);
        var sc = await SchemaAsync(db, schema, ct);
        if (_macros.TryGetValue((sc.Name, name), out var p) && !p.Dropped
            || !sc.Created && await db.Macros.LiveAt(_base!.Value).AnyAsync(m => m.SchemaId == sc.SchemaId && m.MacroName == name, ct)
            && !(p?.Dropped ?? false))
            throw new InvalidOperationException($"Macro {schema}.{name} already exists");
        var macro = new PendingMacro { Schema = sc, Name = name, IsTable = implementations[0].IsTableMacro, Implementations = implementations, Created = true };
        _macros[(sc.Name, name)] = macro;
        _ops.Add(new CreateMacroOp(macro));
    }

    public async Task DropMacroAsync(string schema, string name, CancellationToken ct = default)
    {
        await using var db = await BeginAsync(ct);
        var sc = await SchemaAsync(db, schema, ct);
        if (_macros.TryGetValue((sc.Name, name), out var pending))
        {
            if (pending.Dropped || pending.Created) throw new InvalidOperationException($"Macro {schema}.{name} is created or dropped in this transaction");
        }
        else
        {
            var row = DuckLakeCatalog.Find(await db.Macros.LiveAt(_base!.Value).Where(m => m.SchemaId == sc.SchemaId).ToListAsync(ct), m => m.MacroName ?? "", name,
                $"macro in schema {sc.Name}");
            var macroId = row.MacroId;
            var isTable = await db.MacroImpls.AnyAsync(i => i.MacroId == macroId && i.Type == "table", ct);
            pending = _macros[(sc.Name, name)] = new PendingMacro { MacroId = row.MacroId, Schema = sc, Name = row.MacroName ?? name, IsTable = isTable };
        }
        pending.Dropped = true;
        _ops.Add(new DropMacroOp(pending));
    }

    // ---- commit ------------------------------------------------------------------------------------------------------

    private void ObjectChanges(ChangeSet set, Op op)
    {
        switch (op)
        {
            case SetTagOp { Table: { } t } when t.BaseTableId is { } id: set.AlteredTables.Add(id); break;
            case SetTagOp { View: { ViewId: { } id, Created: false } }: set.AlteredViews.Add(id); break;
            case SetColumnTagOp c when c.Table.BaseTableId is { } id: set.AlteredTables.Add(id); break;
            case SetViewColumnTagOp { View: { ViewId: { } id, Created: false } }: set.AlteredViews.Add(id); break;
            case SetSortOp s when s.Table.BaseTableId is { } id: set.AlteredTables.Add(id); break;
            case RenameTableOp r: set.RenamedTables.Add((r.Table.TableId!.Value, r.Table.Schema.Name, r.NewName)); break;
            case CreateViewOp v: set.CreatedViews.Add((v.View.Schema.Name, v.View.Schema.SchemaId, v.View.Name)); break;
            case RenameViewOp v: set.RenamedViews.Add((v.View.ViewId!.Value, v.View.Schema.Name, v.NewName)); break;
            case DropViewOp v: set.DroppedViews.Add(v.View.ViewId!.Value); break;
            case CreateMacroOp m: set.CreatedMacros.Add((m.Macro.Schema.Name, m.Macro.Schema.SchemaId, m.Macro.Name, m.Macro.IsTable)); break;
            case DropMacroOp m: set.DroppedMacros.Add((m.Macro.MacroId!.Value, m.Macro.IsTable)); break;
        }
    }

    /// <summary>Catalog IDs the object operations take (views, macros, sort orders), for the snapshot row written up front.</summary>
    private int ObjectCatalogIds() => _ops.Count(o => o is CreateViewOp or CreateMacroOp or SetSortOp { Keys.Count: > 0 });

    private async Task<long> ApplyObjectAsync(DuckLakeDbContext db, Op op, long snapshotId, long latest, long nextCatalogId, CatalogRows rows,
        HashSet<PendingTable> versionedTables, CancellationToken ct)
    {
        switch (op)
        {
            case SetTagOp tag:
            {
                var objectId = tag.Table?.TableId ?? tag.View?.ViewId ?? throw new InvalidOperationException("A tag needs a table or view");
                var key = tag.Key;
                await db.Tags.Current().Where(x => x.ObjectId == objectId && x.Key == key).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                if (tag.Value is not null) rows.Tags.Add(new TagEntity { ObjectId = objectId, BeginSnapshot = snapshotId, Key = key, Value = tag.Value });
                break;
            }
            case SetColumnTagOp tag:
            {
                var (tableId, columnId, key) = (tag.Table.TableId!.Value, tag.ColumnId, tag.Key);
                await db.ColumnTags.Current().Where(x => x.TableId == tableId && x.ColumnId == columnId && x.Key == key)
                    .ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                if (tag.Value is not null)
                    rows.ColumnTags.Add(new ColumnTagEntity { TableId = tableId, ColumnId = columnId, BeginSnapshot = snapshotId, Key = key, Value = tag.Value });
                break;
            }
            case SetViewColumnTagOp tag:
            {
                var (viewId, column, key) = (tag.View.ViewId!.Value, tag.Column, tag.Key);
                await db.ViewColumnTags.Current().Where(x => x.ViewId == viewId && x.ColumnName == column && x.Key == key)
                    .ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                if (tag.Value is not null)
                    rows.ViewColumnTags.Add(new ViewColumnTagEntity { ViewId = viewId, ColumnName = column, BeginSnapshot = snapshotId, Key = key, Value = tag.Value });
                break;
            }
            case RenameTableOp r:
            {
                var (tableId, oldName, schemaId) = (r.Table.TableId!.Value, r.OldName, r.Table.Schema.SchemaId!.Value);
                var current = await db.Tables.Current().Where(x => x.TableId == tableId && x.TableName == oldName).ToListAsync(ct);
                if (current.Count != 1) throw new DuckLakeConflictException($"Table {r.Table.Schema.Name}.{oldName} was renamed, altered or dropped concurrently");
                await RequireFreeAtCommitAsync(db, schemaId, r.NewName, latest, r.Table.Schema.Name, ct);
                await db.Tables.Current().Where(x => x.TableId == tableId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                var row = current[0];
                rows.Tables.Add(new TableEntity
                {
                    TableId = tableId, TableUuid = row.TableUuid, BeginSnapshot = snapshotId, SchemaId = row.SchemaId, TableName = r.NewName, Path = row.Path,
                    PathIsRelative = row.PathIsRelative,
                });
                versionedTables.Add(r.Table);
                break;
            }
            case SetSortOp s:
            {
                var tableId = s.Table.TableId!.Value;
                await db.SortInfos.Current().Where(x => x.TableId == tableId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                if (s.Keys.Count == 0) break;
                var sortId = nextCatalogId++;
                rows.SortInfos.Add(new SortInfoEntity { SortId = sortId, TableId = tableId, BeginSnapshot = snapshotId });
                for (var i = 0; i < s.Keys.Count; i++)
                    rows.SortExpressions.Add(new SortExpressionEntity
                    {
                        SortId = sortId, TableId = tableId, SortKeyIndex = i, Expression = s.Keys[i].Expression, Dialect = s.Keys[i].Dialect,
                        SortDirection = s.Keys[i].Descending ? "DESC" : "ASC", NullOrder = s.Keys[i].NullsFirst ? "NULLS_FIRST" : "NULLS_LAST",
                    });
                break;
            }
            case CreateViewOp c:
            {
                var view = c.View;
                view.ViewId = nextCatalogId++;
                var schemaId = view.Schema.SchemaId ?? throw new InvalidOperationException($"Schema {view.Schema.Name} has no ID");
                await RequireFreeAtCommitAsync(db, schemaId, view.Name, latest, view.Schema.Name, ct);
                rows.Views.Add(new ViewEntity
                {
                    ViewId = view.ViewId.Value, ViewUuid = view.Uuid, BeginSnapshot = snapshotId, SchemaId = schemaId, ViewName = view.Name,
                    Dialect = view.Dialect, Sql = view.Sql, ColumnAliases = DuckLakeCatalog.FormatAliases(view.Aliases),
                });
                break;
            }
            case RenameViewOp r:
            {
                var (viewId, oldName, schemaId) = (r.View.ViewId!.Value, r.OldName, r.View.Schema.SchemaId!.Value);
                var current = await db.Views.Current().Where(x => x.ViewId == viewId && x.ViewName == oldName).ToListAsync(ct);
                if (current.Count != 1) throw new DuckLakeConflictException($"View {r.View.Schema.Name}.{oldName} was renamed, altered or dropped concurrently");
                await RequireFreeAtCommitAsync(db, schemaId, r.NewName, latest, r.View.Schema.Name, ct);
                await db.Views.Current().Where(x => x.ViewId == viewId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                var row = current[0];
                rows.Views.Add(new ViewEntity
                {
                    ViewId = viewId, ViewUuid = row.ViewUuid, BeginSnapshot = snapshotId, SchemaId = row.SchemaId, ViewName = r.NewName, Dialect = row.Dialect,
                    Sql = row.Sql, ColumnAliases = row.ColumnAliases,
                });
                break;
            }
            case DropViewOp d:
            {
                var viewId = d.View.ViewId!.Value;
                if (await db.Views.Current().Where(x => x.ViewId == viewId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct) == 0)
                    throw new DuckLakeConflictException($"View {d.View.Schema.Name}.{d.View.Name} was dropped concurrently");
                await db.Tags.Current().Where(x => x.ObjectId == viewId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                if (db.Is11)
                    await db.ViewColumnTags.Current().Where(x => x.ViewId == viewId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct);
                break;
            }
            case CreateMacroOp c:
            {
                var macro = c.Macro;
                macro.MacroId = nextCatalogId++;
                var schemaId = macro.Schema.SchemaId ?? throw new InvalidOperationException($"Schema {macro.Schema.Name} has no ID");
                var name = macro.Name;
                if (await db.Macros.LiveAt(latest).AnyAsync(m => m.SchemaId == schemaId && m.MacroName == name, ct)
                    && !_ops.Any(o => o is DropMacroOp d && d.Macro.Schema == macro.Schema && d.Macro.Name == name))
                    throw new DuckLakeConflictException($"Macro {macro.Schema.Name}.{name} was created concurrently");
                rows.Macros.Add(new MacroEntity { SchemaId = schemaId, MacroId = macro.MacroId.Value, MacroName = name, BeginSnapshot = snapshotId });
                for (var i = 0; i < macro.Implementations.Count; i++)
                {
                    var impl = macro.Implementations[i];
                    rows.MacroImpls.Add(new MacroImplEntity { MacroId = macro.MacroId.Value, ImplId = i, Dialect = impl.Dialect, Sql = impl.Sql, Type = impl.IsTableMacro ? "table" : "scalar" });
                    for (var k = 0; k < impl.Parameters.Count; k++)
                    {
                        var p = impl.Parameters[k];
                        rows.MacroParameters.Add(new MacroParameterEntity
                        {
                            MacroId = macro.MacroId.Value, ImplId = i, ColumnId = k, ParameterName = p.Name, ParameterType = p.Type,
                            DefaultValue = p.Default ?? "NULL", DefaultValueType = p.Default is null ? "unknown" : p.DefaultType,
                        });
                    }
                }
                break;
            }
            case DropMacroOp d:
            {
                var macroId = d.Macro.MacroId!.Value;
                if (await db.Macros.Current().Where(x => x.MacroId == macroId).ExecuteUpdateAsync(u => u.SetProperty(x => x.EndSnapshot, snapshotId), ct) == 0)
                    throw new DuckLakeConflictException($"Macro {d.Macro.Schema.Name}.{d.Macro.Name} was dropped concurrently");
                break;
            }
        }
        return nextCatalogId;
    }

    /// <summary>A name taken by a table or view committed since staging is a conflict (unless this transaction ends it).</summary>
    private async Task RequireFreeAtCommitAsync(DuckLakeDbContext db, long schemaId, string name, long latest, string schemaName, CancellationToken ct)
    {
        var lower = name.ToLowerInvariant();
        var takenByTable = await db.Tables.LiveAt(latest).Where(x => x.SchemaId == schemaId && x.TableName != null).Select(x => new { x.TableId, x.TableName }).ToListAsync(ct);
        var takenByView = await db.Views.LiveAt(latest).Where(x => x.SchemaId == schemaId && x.ViewName != null).Select(x => new { x.ViewId, x.ViewName }).ToListAsync(ct);
        var endedHere = _ops.Select(o => o switch
        {
            DropTableOp d => d.Table.TableId,
            RenameTableOp r => r.Table.TableId,
            DropViewOp v => v.View.ViewId,
            RenameViewOp v => v.View.ViewId,
            _ => null,
        }).Where(id => id is not null).ToHashSet();
        if (takenByTable.Any(t => t.TableName!.ToLowerInvariant() == lower && !endedHere.Contains(t.TableId))
            || takenByView.Any(v => v.ViewName!.ToLowerInvariant() == lower && !endedHere.Contains(v.ViewId)))
            throw new DuckLakeConflictException($"{schemaName}.{name} was created concurrently");
    }
}
