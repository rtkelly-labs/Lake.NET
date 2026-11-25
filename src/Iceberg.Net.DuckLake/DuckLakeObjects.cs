using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// A key of a table's sort order (<c>ducklake_sort_expression</c>): a column name or SQL expression in
/// <paramref name="Dialect"/>. New data files are written sorted by the keys that are column names; DuckDB evaluates the rest.
/// </summary>
public sealed record DuckLakeSortKey(string Expression, bool Descending = false, bool NullsFirst = false, string Dialect = "duckdb");

/// <summary>A view (<c>ducklake_view</c>): a SQL definition, stored and returned, not executed by this library.</summary>
public sealed record DuckLakeView(long ViewId, Guid Uuid, string SchemaName, string Name, string Dialect, string Sql, IReadOnlyList<string>? ColumnAliases)
{
    public IReadOnlyDictionary<string, string?> Tags { get; init; } = new Dictionary<string, string?>();
    public string? Comment => Tags.GetValueOrDefault("comment");

    /// <summary>The definition with <c>{DUCKLAKE_CATALOG}</c> resolved to the name the lake is attached under (spec <c>ducklake_view</c>).</summary>
    public string SqlFor(string catalogName) => Sql.Replace(DuckLakeCatalog.CatalogPlaceholder, catalogName, StringComparison.Ordinal);
}

/// <summary>
/// A macro parameter (<c>ducklake_macro_parameters</c>). <paramref name="Type"/> is a DuckLake type or <c>unknown</c>;
/// <paramref name="Default"/> is the default's SQL text (null: none), <paramref name="DefaultType"/> its DuckLake type,
/// <c>expression</c> for a SQL expression, or <c>unknown</c>.
/// </summary>
public sealed record DuckLakeMacroParameter(string Name, string Type = "unknown", string? Default = null, string DefaultType = "unknown");

/// <summary>One implementation of a macro (<c>ducklake_macro_impl</c>): a scalar expression or a table query.</summary>
public sealed record DuckLakeMacroImplementation(string Sql, IReadOnlyList<DuckLakeMacroParameter> Parameters, bool IsTableMacro = false, string Dialect = "duckdb");

/// <summary>
/// A macro (<c>ducklake_macro</c>) with its implementations; stored and returned, not executed by this library. Unlike
/// a view's, a macro body's references are not rewritten: DuckDB 1.5.5 stores its own with the catalog name the lake was
/// attached under, and fails to load a body that holds <c>{DUCKLAKE_CATALOG}</c>.
/// </summary>
public sealed record DuckLakeMacro(long MacroId, string SchemaName, string Name, IReadOnlyList<DuckLakeMacroImplementation> Implementations)
{
    public bool IsTableMacro => Implementations.Count > 0 && Implementations[0].IsTableMacro;
}

public sealed partial class DuckLakeCatalog
{
    /// <summary>The placeholder DuckDB writes for the catalog's own name in view definitions.</summary>
    public const string CatalogPlaceholder = "{DUCKLAKE_CATALOG}";

    internal static async Task<IReadOnlyDictionary<string, string?>> TagsAsync(DuckLakeDbContext db, long objectId, long snapshot, CancellationToken ct) =>
        (await Guarded("ducklake_tag", () => db.Tags.LiveAt(snapshot).Where(t => t.ObjectId == objectId).ToListAsync(ct)))
        .GroupBy(t => t.Key).ToDictionary(g => g.Key, g => g.MaxBy(t => t.BeginSnapshot)!.Value);

    internal static async Task<IReadOnlyDictionary<long, IReadOnlyDictionary<string, string?>>> ColumnTagsAsync(DuckLakeDbContext db, long tableId, long snapshot,
        CancellationToken ct) =>
        (await Guarded("ducklake_column_tag", () => db.ColumnTags.LiveAt(snapshot).Where(t => t.TableId == tableId).ToListAsync(ct)))
        .GroupBy(t => t.ColumnId)
        .ToDictionary(g => g.Key, g => (IReadOnlyDictionary<string, string?>)g.GroupBy(t => t.Key).ToDictionary(k => k.Key, k => k.MaxBy(t => t.BeginSnapshot)!.Value));

    internal static async Task<IReadOnlyList<DuckLakeSortKey>> SortKeysAsync(DuckLakeDbContext db, long tableId, long snapshot, CancellationToken ct)
    {
        var sorts = await Guarded("ducklake_sort_info", () => db.SortInfos.LiveAt(snapshot).Where(s => s.TableId == tableId).Select(s => s.SortId).ToListAsync(ct));
        if (sorts.Count == 0) return [];
        if (sorts.Count > 1) throw new InvalidDataException($"Table {tableId} has {sorts.Count} live sort orders in snapshot {snapshot}");
        var sortId = sorts[0];
        return (await Guarded("ducklake_sort_expression", () => db.SortExpressions.Where(e => e.SortId == sortId).OrderBy(e => e.SortKeyIndex).ToListAsync(ct)))
            .Select(e => new DuckLakeSortKey(
                e.Expression ?? throw new InvalidDataException($"Sort {sortId} key {e.SortKeyIndex} has no expression"),
                e.SortDirection switch
                {
                    "DESC" => true,
                    "ASC" or null => false,
                    var d => throw new InvalidDataException($"Sort {sortId}: direction '{d}'"),
                },
                e.NullOrder switch
                {
                    "NULLS_FIRST" => true,
                    "NULLS_LAST" or null => false,
                    var n => throw new InvalidDataException($"Sort {sortId}: null order '{n}'"),
                },
                e.Dialect ?? "duckdb")).ToList();
    }

    internal sealed record ViewRow(long ViewId, Guid Uuid, long SchemaId, string Name, string Dialect, string Sql, string? Aliases);

    internal static async Task<List<ViewRow>> ViewsAsync(DuckLakeDbContext db, long snapshot, CancellationToken ct) =>
        (await Guarded("ducklake_view", () => db.Views.LiveAt(snapshot).OrderBy(v => v.ViewId).ToListAsync(ct)))
        .Select(v => new ViewRow(v.ViewId, v.ViewUuid ?? Guid.Empty, v.SchemaId, v.ViewName ?? throw new InvalidDataException($"View {v.ViewId} has no name"),
            v.Dialect ?? "duckdb", v.Sql ?? throw new InvalidDataException($"View {v.ViewId} has no SQL"), v.ColumnAliases)).ToList();

    /// <summary>Column aliases as DuckDB writes them: quoted names joined by commas (<c>"a","b"</c>), empty for none.</summary>
    internal static IReadOnlyList<string>? ParseAliases(string? text) =>
        string.IsNullOrEmpty(text) ? null : DuckLakeChange.SplitList(text);

    internal static string FormatAliases(IReadOnlyList<string>? aliases) =>
        aliases is null || aliases.Count == 0 ? "" : string.Join(",", aliases.Select(DuckLakeChange.QuoteName));

    public async Task<IReadOnlyList<string>> ListViewsAsync(string schema, long? snapshotId = null, CancellationToken ct = default)
    {
        await using var db = Context();
        var s = await ResolveSnapshotAsync(db, snapshotId, ct);
        var sc = Find(await SchemasAsync(db, s, ct), x => x.Name, schema, "schema");
        return (await ViewsAsync(db, s, ct)).Where(v => v.SchemaId == sc.SchemaId).Select(v => v.Name).ToList();
    }

    public async Task<DuckLakeView> LoadViewAsync(string schema, string view, long? snapshotId = null, CancellationToken ct = default)
    {
        await using var db = Context();
        var s = await ResolveSnapshotAsync(db, snapshotId, ct);
        var sc = Find(await SchemasAsync(db, s, ct), x => x.Name, schema, "schema");
        var v = Find((await ViewsAsync(db, s, ct)).Where(x => x.SchemaId == sc.SchemaId), x => x.Name, view, $"view in schema {sc.Name}");
        return new DuckLakeView(v.ViewId, v.Uuid, sc.Name, v.Name, v.Dialect, v.Sql, Corruption.Guard("ducklake_view.column_aliases", () => ParseAliases(v.Aliases)))
        {
            Tags = await TagsAsync(db, v.ViewId, s, ct),
        };
    }

    public async Task<IReadOnlyList<string>> ListMacrosAsync(string schema, long? snapshotId = null, CancellationToken ct = default)
    {
        await using var db = Context();
        var s = await ResolveSnapshotAsync(db, snapshotId, ct);
        var sc = Find(await SchemasAsync(db, s, ct), x => x.Name, schema, "schema");
        return await db.Macros.LiveAt(s).Where(m => m.SchemaId == sc.SchemaId).OrderBy(m => m.MacroId).Select(m => m.MacroName ?? "").ToListAsync(ct);
    }

    public async Task<DuckLakeMacro> LoadMacroAsync(string schema, string macro, long? snapshotId = null, CancellationToken ct = default)
    {
        await using var db = Context();
        var s = await ResolveSnapshotAsync(db, snapshotId, ct);
        var sc = Find(await SchemasAsync(db, s, ct), x => x.Name, schema, "schema");
        var m = Find(await Guarded("ducklake_macro", () => db.Macros.LiveAt(s).Where(x => x.SchemaId == sc.SchemaId).ToListAsync(ct)), x => x.MacroName ?? "", macro,
            $"macro in schema {sc.Name}");
        var macroId = m.MacroId;
        var impls = await Guarded("ducklake_macro_impl", () => db.MacroImpls.Where(i => i.MacroId == macroId).OrderBy(i => i.ImplId).ToListAsync(ct));
        var parameters = (await Guarded("ducklake_macro_parameters", () => db.MacroParameters.Where(p => p.MacroId == macroId).ToListAsync(ct)))
            .GroupBy(p => p.ImplId).ToDictionary(g => g.Key, g => g.OrderBy(p => p.ColumnId).ToList());
        return new DuckLakeMacro(macroId, sc.Name, m.MacroName ?? macro, impls.Select(i => new DuckLakeMacroImplementation(
            i.Sql ?? throw new InvalidDataException($"Macro {macroId} implementation {i.ImplId} has no SQL"),
            parameters.GetValueOrDefault(i.ImplId, []).Select(p => new DuckLakeMacroParameter(p.ParameterName ?? "", p.ParameterType ?? "unknown",
                p.DefaultValueType is null or "unknown" && p.DefaultValue is null or "NULL" ? null : p.DefaultValue, p.DefaultValueType ?? "unknown")).ToList(),
            i.Type switch
            {
                "table" => true,
                "scalar" or null => false,
                var k => throw new InvalidDataException($"Macro {macroId}: type '{k}'"),
            },
            i.Dialect ?? "duckdb")).ToList());
    }
}
