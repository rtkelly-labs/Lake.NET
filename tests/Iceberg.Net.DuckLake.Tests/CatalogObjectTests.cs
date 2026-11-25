using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Tags and comments, table renames, views, macros and sort orders: the catalog rows DuckDB 1.5.5 writes for them,
/// time travel over them, and their conflicts. On every catalog database and format version.
/// </summary>
public abstract class CatalogObjectTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected CatalogObjectTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    private bool Is11 => _lake.Version != DuckLakeCatalog.FormatVersion10;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private Task<List<object?[]>> SqlAsync(string sql) => _lake.SqlAsync(sql);

    private async Task<DuckLakeCatalog> LakeAsync()
    {
        var catalog = await _lake.CreateAsync();
        await CommitAsync(catalog, async tx =>
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("name", DuckLakeType.Varchar),
                new("point", DuckLakeType.Struct(("x", DuckLakeType.Int32)))]);
            await tx.InsertAsync("main", "t", [[1L, "a", new object?[] { 1 }]]);
        });
        return catalog;
    }

    private static async Task<DuckLakeSnapshot> CommitAsync(DuckLakeCatalog catalog, Func<DuckLakeTransaction, Task> work)
    {
        await using var tx = catalog.BeginTransaction();
        await work(tx);
        return await tx.CommitAsync();
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_table")]
    [Spec("ducklake: ducklake_view")]
    public async Task Repeated_table_and_view_renames_keep_only_the_final_version()
    {
        var catalog = await LakeAsync();
        var before = await CommitAsync(catalog, tx => tx.CreateViewAsync("main", "v", "SELECT 1"));
        await CommitAsync(catalog, async tx =>
        {
            await tx.RenameTableAsync("main", "t", "a");
            await tx.RenameTableAsync("main", "a", "b");
            await tx.RenameViewAsync("main", "v", "va");
            await tx.RenameViewAsync("main", "va", "vb");
        });
        Assert.Equal(["b"], await catalog.ListTablesAsync("main"));
        Assert.Equal(["vb"], await catalog.ListViewsAsync("main"));
        Assert.Single(await catalog.ReadAsync(await catalog.LoadTableAsync("main", "b")));
        Assert.Single(await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t", before.SnapshotId)));
        Assert.Equal("SELECT 1", (await catalog.LoadViewAsync("main", "v", before.SnapshotId)).Sql);
        Assert.Equal(2, (await SqlAsync("SELECT table_id FROM ducklake_table")).Count);
        Assert.Equal(2, (await SqlAsync("SELECT view_id FROM ducklake_view")).Count);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_tag")]
    [Spec("ducklake: ducklake_column_tag")]
    public async Task Comments_and_tags_are_versioned_like_DuckDBs()
    {
        var catalog = await LakeAsync();
        var s2 = await CommitAsync(catalog, async tx =>
        {
            await tx.SetCommentAsync("main", "t", "the table");
            await tx.SetColumnCommentAsync("main", "t", "name", "a name");
            await tx.SetColumnTagAsync("main", "t", "point.x", "unit", "m");
            await tx.SetTagAsync("main", "t", "owner", "etl");
        });
        Assert.Equal(["altered_table:1"], s2.Changes.Select(c => c.ToString()));
        Assert.Equal(2, s2.SchemaVersion); // bumped, without a ducklake_schema_versions row (as DuckDB)
        Assert.Equal([[1L]], await SqlAsync("SELECT count(*) FROM ducklake_schema_versions"));
        var s3 = await CommitAsync(catalog, async tx =>
        {
            await tx.SetCommentAsync("main", "t", "changed");
            await tx.SetTagAsync("main", "t", "owner", null);
        });
        Assert.Equal([["comment", "the table", 2L, s3.SnapshotId], ["comment", "changed", s3.SnapshotId, null], ["owner", "etl", 2L, s3.SnapshotId]],
            await SqlAsync("SELECT \"key\", \"value\", begin_snapshot, end_snapshot FROM ducklake_tag ORDER BY \"key\", begin_snapshot"));
        var now = await catalog.LoadTableAsync("main", "t");
        Assert.Equal("changed", now.Comment);
        Assert.False(now.Tags.ContainsKey("owner"));
        Assert.Equal("a name", now.ColumnComment("name"));
        Assert.Equal("m", now.ColumnTags[now.Column("point").Children[0].Id]["unit"]);
        var then = await catalog.LoadTableAsync("main", "t", 2);
        Assert.Equal(("the table", "etl"), (then.Comment, then.Tags["owner"]));
        Assert.Null((await catalog.LoadTableAsync("main", "t", 1)).Comment);
        // Dropping a column ends its tags, its children's included (spec ducklake_column_tag).
        var s4 = await CommitAsync(catalog, tx => tx.DropColumnAsync("main", "t", "point"));
        Assert.Equal([[s4.SnapshotId]], await SqlAsync("SELECT DISTINCT end_snapshot FROM ducklake_column_tag WHERE \"key\" = 'unit'"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_table")]
    [Spec("ducklake: ducklake_snapshot_changes")]
    public async Task Renaming_a_table_keeps_its_id_and_files()
    {
        var catalog = await LakeAsync();
        var s = await CommitAsync(catalog, tx => tx.RenameTableAsync("main", "t", "t2"));
        Assert.Equal(["created_table:\"main\".\"t2\""], s.Changes.Select(c => c.ToString())); // as DuckDB records a rename
        Assert.Equal([[1L, "t", 1L, s.SnapshotId], [1L, "t2", s.SnapshotId, null]],
            await SqlAsync("SELECT table_id, table_name, begin_snapshot, end_snapshot FROM ducklake_table ORDER BY begin_snapshot"));
        Assert.Equal([["t/"]], await SqlAsync("SELECT DISTINCT path FROM ducklake_table"));
        Assert.Single(await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t2")));
        Assert.Single(await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t", 1)));
        await Assert.ThrowsAsync<DuckLakeNotFoundException>(() => catalog.LoadTableAsync("main", "t"));
        // Two renames of one table: the second conflicts.
        await using var a = catalog.BeginTransaction();
        await a.RenameTableAsync("main", "t2", "x");
        await CommitAsync(catalog, tx => tx.RenameTableAsync("main", "t2", "y"));
        await Assert.ThrowsAsync<DuckLakeConflictException>(() => a.CommitAsync());
        // A created table renamed before commit lives under its final name.
        await CommitAsync(catalog, async tx =>
        {
            await tx.CreateTableAsync("main", "draft", [new("v", DuckLakeType.Int32)]);
            await tx.RenameTableAsync("main", "draft", "final");
            await tx.InsertAsync("main", "final", [[1]]);
        });
        Assert.All(_lake.Files().Where(f => f.Contains("final", StringComparison.Ordinal) || f.Contains("draft", StringComparison.Ordinal)),
            f => Assert.Contains("/final/", f.Replace('\\', '/'), StringComparison.Ordinal));
        Assert.Single(await catalog.ReadAsync(await catalog.LoadTableAsync("main", "final")));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_view")]
    [Spec("ducklake: ducklake_tag")]
    public async Task Views_are_created_commented_renamed_and_dropped()
    {
        var catalog = await LakeAsync();
        var created = await CommitAsync(catalog, async tx =>
        {
            await tx.CreateViewAsync("main", "v", "SELECT id, \"name\" AS n FROM {DUCKLAKE_CATALOG}.t WHERE (id > 1)");
            await tx.CreateViewAsync("main", "v2", "SELECT id, \"name\" FROM {DUCKLAKE_CATALOG}.t", ["a", "b"]);
        });
        Assert.Equal(["created_view:\"main\".\"v\"", "created_view:\"main\".\"v2\""], created.Changes.Select(c => c.ToString()));
        Assert.Equal([[2L, "v", "duckdb", ""], [3L, "v2", "duckdb", "\"a\",\"b\""]],
            await SqlAsync("SELECT view_id, view_name, dialect, column_aliases FROM ducklake_view ORDER BY view_id"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => CommitAsync(catalog, tx => tx.CreateViewAsync("main", "t", "SELECT 1")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => CommitAsync(catalog, tx => tx.CreateTableAsync("main", "v", [new("x", DuckLakeType.Int32)])));

        var commented = await CommitAsync(catalog, tx => tx.SetViewCommentAsync("main", "v", "a view"));
        Assert.Equal(["altered_view:2"], commented.Changes.Select(c => c.ToString()));
        var renamed = await CommitAsync(catalog, tx => tx.RenameViewAsync("main", "v2", "v3"));
        Assert.Equal(["created_view:\"main\".\"v3\""], renamed.Changes.Select(c => c.ToString()));
        var v = await catalog.LoadViewAsync("main", "v");
        Assert.Equal(("a view", "SELECT id, \"name\" AS n FROM lake.t WHERE (id > 1)"), (v.Comment, v.SqlFor("lake")));
        Assert.Equal(["a", "b"], (await catalog.LoadViewAsync("main", "v3")).ColumnAliases);
        Assert.Equal(["v", "v3"], await catalog.ListViewsAsync("main"));
        Assert.Equal(["v", "v2"], await catalog.ListViewsAsync("main", created.SnapshotId));

        await using (var tx = catalog.BeginTransaction())
        {
            if (Is11) await tx.SetViewColumnTagAsync("main", "v", "n", "comment", "view col");
            else await Assert.ThrowsAsync<NotSupportedException>(() => tx.SetViewColumnTagAsync("main", "v", "n", "comment", "x"));
            await tx.SetViewTagAsync("main", "v", "k", "1");
            await tx.CommitAsync();
        }
        await Assert.ThrowsAsync<InvalidOperationException>(() => CommitAsync(catalog, tx => tx.DropSchemaAsync("main"))); // views count as contents
        var dropped = await CommitAsync(catalog, tx => tx.DropViewAsync("main", "v"));
        Assert.Equal(["dropped_view:2"], dropped.Changes.Select(c => c.ToString()));
        Assert.Equal([["v3"]], (await catalog.ListViewsAsync("main")).Select(n => new object?[] { n }));
        Assert.Empty(await SqlAsync($"SELECT * FROM ducklake_tag WHERE object_id = 2 AND end_snapshot IS NULL"));
        if (Is11) Assert.Equal([[dropped.SnapshotId]], await SqlAsync("SELECT end_snapshot FROM ducklake_view_column_tag"));
        // Two creates of one name: the second conflicts.
        await using var a = catalog.BeginTransaction();
        await a.CreateViewAsync("main", "same", "SELECT 1");
        await CommitAsync(catalog, tx => tx.CreateViewAsync("main", "same", "SELECT 2"));
        await Assert.ThrowsAsync<DuckLakeConflictException>(() => a.CommitAsync());
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_macro")]
    [Spec("ducklake: ducklake_macro_impl")]
    [Spec("ducklake: ducklake_macro_parameters")]
    public async Task Macros_are_stored_as_DuckDB_stores_them()
    {
        var catalog = await LakeAsync();
        var s = await CommitAsync(catalog, async tx =>
        {
            await tx.CreateMacroAsync("main", "add2", [new("(a + b)", [new("a"), new("b", Default: "1", DefaultType: "int32")])]);
            await tx.CreateMacroAsync("main", "tm", [new("SELECT * FROM t WHERE (id = x)", [new("x")], IsTableMacro: true)]);
        });
        Assert.Equal(["created_scalar_macro:\"main\".\"add2\"", "created_table_macro:\"main\".\"tm\""], s.Changes.Select(c => c.ToString()));
        Assert.Equal([[2L, 0L, "a", "unknown", "NULL", "unknown"], [2L, 1L, "b", "unknown", "1", "int32"], [3L, 0L, "x", "unknown", "NULL", "unknown"]],
            await SqlAsync("SELECT macro_id, column_id, parameter_name, parameter_type, default_value, default_value_type FROM ducklake_macro_parameters ORDER BY macro_id, column_id"));
        var add2 = await catalog.LoadMacroAsync("main", "add2");
        Assert.Equal(("(a + b)", false, null, "1"), (add2.Implementations[0].Sql, add2.IsTableMacro, add2.Implementations[0].Parameters[0].Default,
            add2.Implementations[0].Parameters[1].Default));
        Assert.True((await catalog.LoadMacroAsync("main", "tm")).IsTableMacro);
        await Assert.ThrowsAsync<InvalidOperationException>(() => CommitAsync(catalog, tx => tx.CreateMacroAsync("main", "add2", [new("1", [])])));
        var dropped = await CommitAsync(catalog, tx => tx.DropMacroAsync("main", "add2"));
        Assert.Equal(["dropped_scalar_macro:2"], dropped.Changes.Select(c => c.ToString()));
        Assert.Equal(["tm"], await catalog.ListMacrosAsync("main"));
        Assert.Equal(["add2", "tm"], await catalog.ListMacrosAsync("main", s.SnapshotId));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_sort_info")]
    [Spec("ducklake: ducklake_sort_expression")]
    public async Task A_sort_order_is_recorded_and_orders_the_files_written()
    {
        var catalog = await LakeAsync();
        var s = await CommitAsync(catalog, tx => tx.SetSortOrderAsync("main", "t",
            [new("name", Descending: true), new("lower(\"name\")", NullsFirst: true), new("id")]));
        Assert.Equal(["altered_table:1"], s.Changes.Select(c => c.ToString()));
        Assert.Equal([[0L, "name", "duckdb", "DESC", "NULLS_LAST"], [1L, "lower(\"name\")", "duckdb", "ASC", "NULLS_FIRST"], [2L, "id", "duckdb", "ASC", "NULLS_LAST"]],
            await SqlAsync("SELECT sort_key_index, expression, dialect, sort_direction, null_order FROM ducklake_sort_info i JOIN ducklake_sort_expression e USING (sort_id) ORDER BY sort_key_index"));
        Assert.Equal(3, (await catalog.LoadTableAsync("main", "t")).SortKeys.Count);
        Assert.Empty((await catalog.LoadTableAsync("main", "t", 1)).SortKeys);
        // The insert is written sorted by the leading column keys (name descending; the expression ends the prefix).
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[5L, "b", null], [6L, "d", null], [7L, null, null], [8L, "a", null]]));
        var table = await catalog.LoadTableAsync("main", "t");
        var last = (await catalog.PlanScanAsync(table)).Files[^1];
        var inFile = (await Iceberg.Net.Data.ParquetFiles.ReadAsync(catalog.IO, last.Location, table.Schema)).Select(r => (string?)r[1]).ToList();
        Assert.Equal(["d", "b", "a", null], inFile);
        await CommitAsync(catalog, tx => tx.SetSortOrderAsync("main", "t", []));
        Assert.Empty((await catalog.LoadTableAsync("main", "t")).SortKeys);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_tag")]
    [Spec("ducklake: ducklake_macro_impl")]
    public async Task Expiry_removes_tag_versions_views_and_macros_no_snapshot_sees()
    {
        var catalog = await LakeAsync();
        await CommitAsync(catalog, async tx =>
        {
            await tx.SetCommentAsync("main", "t", "old");
            await tx.CreateViewAsync("main", "v", "SELECT 1");
            await tx.CreateMacroAsync("main", "m", [new("1", [new("p")])]);
            await tx.SetSortOrderAsync("main", "t", [new("id")]);
        });
        await CommitAsync(catalog, async tx =>
        {
            await tx.SetCommentAsync("main", "t", "new");
            await tx.DropViewAsync("main", "v");
            await tx.DropMacroAsync("main", "m");
            await tx.SetSortOrderAsync("main", "t", [new("name")]);
        });
        var latest = (await catalog.CurrentSnapshotAsync()).SnapshotId;
        await catalog.ExpireSnapshotsAsync((await catalog.ListSnapshotsAsync()).Select(s => s.SnapshotId).Where(id => id != latest).ToList());
        Assert.Equal([["new"]], await SqlAsync("SELECT \"value\" FROM ducklake_tag"));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_view"));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_macro"));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_macro_impl"));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_macro_parameters"));
        Assert.Equal([["name"]], await SqlAsync("SELECT expression FROM ducklake_sort_expression"));
        Assert.Equal("new", (await catalog.LoadTableAsync("main", "t")).Comment);
    }
}

public sealed class SqliteCatalogObjectTests10() : CatalogObjectTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteCatalogObjectTests11() : CatalogObjectTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresCatalogObjectTests10() : CatalogObjectTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresCatalogObjectTests11() : CatalogObjectTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
