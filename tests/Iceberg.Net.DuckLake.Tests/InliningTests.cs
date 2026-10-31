using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Writing inlined data (docs/ducklake.md, "Data inlining"): small inserts and deletes kept in the catalog database, as
/// DuckDB 1.5.5 writes them (table and column types per database, system columns per format version, change kinds,
/// counters), and flushing them to Parquet without changing what any snapshot reads. On every database and version.
/// </summary>
public abstract class InliningTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected InliningTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    private bool Is11 => _lake.Version != DuckLakeCatalog.FormatVersion10;
    private string RowId => Is11 ? "_ducklake_row_id" : "row_id";
    private string Begin => Is11 ? "_ducklake_begin_snapshot" : "begin_snapshot";
    private string End => Is11 ? "_ducklake_end_snapshot" : "end_snapshot";

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private Task<List<object?[]>> SqlAsync(string sql) => _lake.SqlAsync(sql);

    private async Task<DuckLakeCatalog> LakeAsync(int limit = 10)
    {
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { DataInliningRowLimit = limit });
        await using var tx = catalog.BeginTransaction();
        await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("v", DuckLakeType.Varchar)]);
        await tx.CommitAsync();
        return catalog;
    }

    private static async Task CommitAsync(DuckLakeCatalog catalog, Func<DuckLakeTransaction, Task> work)
    {
        await using var tx = catalog.BeginTransaction();
        await work(tx);
        await tx.CommitAsync();
    }

    private static async Task<Dictionary<long, string>> HistoryAsync(DuckLakeCatalog catalog)
    {
        var result = new Dictionary<long, string>();
        foreach (var s in await catalog.ListSnapshotsAsync())
        {
            if (s.SnapshotId < 1) continue;
            var rows = new List<string>();
            await foreach (var r in catalog.ReadRowsAsync(await catalog.LoadTableAsync("main", "t", s.SnapshotId))) rows.Add($"{r.RowId}:{string.Join("|", r.Values)}");
            result[s.SnapshotId] = string.Join(" ", rows.Order(StringComparer.Ordinal));
        }
        return result;
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_inlined_data_tables")]
    [Spec("ducklake: Insertion Inlining")]
    [Spec("ducklake: Type Encoding for Data Inlining")]
    public async Task A_small_insert_is_inlined_as_DuckDB_inlines_it()
    {
        var catalog = await LakeAsync();
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L, "a"], [2L, null]]));
        var snapshot = await catalog.CurrentSnapshotAsync();
        Assert.Equal(["inlined_insert:1"], snapshot.Changes.Select(c => c.ToString()));
        Assert.Equal(1, snapshot.NextFileId); // DuckDB takes a file ID for each table it inlines into
        Assert.Empty(_lake.Files());
        Assert.Equal([[1L, "ducklake_inlined_data_1_1", 1L]], await SqlAsync("SELECT table_id, table_name, schema_version FROM ducklake_inlined_data_tables"));
        Assert.Equal([[0L, 2L, null, 1L, "a"], [1L, 2L, null, 2L, null]],
            (await SqlAsync($"SELECT {RowId}, {Begin}, {End}, id, v FROM ducklake_inlined_data_1_1 ORDER BY 1")).Select(Text));
        Assert.Equal([[2L, 2L]], await SqlAsync("SELECT record_count, next_row_id FROM ducklake_table_stats"));
        Assert.Equal([[1L, "1", "2"], [2L, "a", "a"]], await SqlAsync("SELECT column_id, min_value, max_value FROM ducklake_table_column_stats ORDER BY column_id"));
        // Column types as DuckDB creates them: SQLite has BIGINT and VARCHAR; PostgreSQL keeps VARCHAR as BYTEA.
        var types = _lake.Backend == CatalogBackend.Sqlite
            ? (await SqlAsync("SELECT sql FROM sqlite_master WHERE name = 'ducklake_inlined_data_1_1'")).Single()[0] as string
            : string.Join(",", (await SqlAsync("SELECT data_type FROM information_schema.columns WHERE table_name = 'ducklake_inlined_data_1_1' ORDER BY ordinal_position")).Select(r => r[0]));
        Assert.Contains(_lake.Backend == CatalogBackend.Sqlite ? "\"v\" VARCHAR" : "bigint,bigint,bigint,bigint,bytea", types);
        var rows = new List<DuckLakeRow>();
        await foreach (var r in catalog.ReadRowsAsync(await catalog.LoadTableAsync("main", "t"))) rows.Add(r);
        Assert.Equal([(0L, 1L, "a"), (1L, 2L, (string?)null)], rows.Select(r => (r.RowId, (long)r.Values[0]!, (string?)r.Values[1])));

        // Over the limit: a file, with row IDs after the inlined ones.
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", Enumerable.Range(10, 11).Select(i => new object?[] { (long)i, "f" })));
        Assert.Equal([[2L]], await SqlAsync("SELECT row_id_start FROM ducklake_data_file"));
        Assert.Equal(13, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Count);
    }

    private static object?[] Text(object?[] row) => row.Select(v => v is byte[] b ? System.Text.Encoding.UTF8.GetString(b) : v).ToArray();

    [SkippableFact]
    [Spec("ducklake: Deletion Inlining")]
    [Spec("ducklake: DELETE")]
    public async Task Small_deletes_are_inlined_for_inlined_rows_and_file_rows()
    {
        var catalog = await LakeAsync(limit: 3);
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L, "a"], [2L, "b"]]));                     // 2: inlined
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", Enumerable.Range(10, 10).Select(i => new object?[] { (long)i, "f" }))); // 3: a file
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.In("id", 2L, 10L, 11L)));             // 4
        Assert.Equal(["inlined_delete:1"], (await catalog.CurrentSnapshotAsync()).Changes.Select(c => c.ToString()));
        Assert.Equal([[2L, 4L]], await SqlAsync($"SELECT id, {End} FROM ducklake_inlined_data_1_1 WHERE {End} IS NOT NULL"));
        // File ID 1: the inlined insert took file ID 0, as DuckDB's does.
        Assert.Equal([[1L, 0L, 4L], [1L, 1L, 4L]], await SqlAsync("SELECT file_id, row_id, begin_snapshot FROM ducklake_inlined_delete_1 ORDER BY row_id"));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_delete_file"));
        // Over the limit for one file: a delete file, which leaves the inlined deletions alone.
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.In("id", 12L, 13L, 14L, 15L)));
        Assert.Equal(["deleted_from_table:1"], (await catalog.CurrentSnapshotAsync()).Changes.Select(c => c.ToString()));
        Assert.Equal([[4L]], await SqlAsync("SELECT delete_count FROM ducklake_delete_file"));
        Assert.Equal([1L, 16L, 17L, 18L, 19L], (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Select(r => (long)r[0]!).Order());
        Assert.Equal(4, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t", 3))).Count - 8);
    }

    [SkippableFact]
    [Spec("ducklake: UPDATE")]
    [Spec("ducklake: ducklake_schema_versions")]
    public async Task Inlined_updates_keep_row_ids_and_a_schema_change_starts_a_new_inlined_table()
    {
        var catalog = await LakeAsync();
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L, "a"], [2L, "b"]]));
        await CommitAsync(catalog, tx => tx.UpdateAsync("main", "t", Expr.Equal("id", 2L), r => [r[0], "B"]));
        var rows = new List<DuckLakeRow>();
        await foreach (var r in catalog.ReadRowsAsync(await catalog.LoadTableAsync("main", "t"))) rows.Add(r);
        Assert.Equal([(0L, "a"), (1L, "B")], rows.Select(r => (r.RowId, (string)r.Values[1]!)).Order());
        await CommitAsync(catalog, tx => tx.AddColumnAsync("main", "t", new DuckLakeColumnDefinition("extra", DuckLakeType.Int32)));
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[3L, "c", 7]]));
        Assert.Equal([["ducklake_inlined_data_1_1"], ["ducklake_inlined_data_1_2"]], await SqlAsync("SELECT table_name FROM ducklake_inlined_data_tables ORDER BY schema_version"));
        Assert.Equal(["1|a|", "2|B|", "3|c|7"], (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Select(r => string.Join("|", r)).Order());
    }

    [SkippableFact]
    [Spec("ducklake: Flushing Inlined Data")]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task Flushing_moves_inlined_data_to_parquet_and_keeps_every_snapshot()
    {
        var catalog = await LakeAsync(limit: 3);
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L, "a"], [2L, "b"], [3L, "c"]]));
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[4L, "d"]]));
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.Equal("id", 2L)));
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", Enumerable.Range(100, 10).Select(i => new object?[] { (long)i, "p" })));
        await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.Equal("id", 100L)));
        var before = await HistoryAsync(catalog);

        Assert.Equal([new DuckLakeFlush("main", "t", 4)], await catalog.FlushInlinedDataAsync());
        Assert.Equal(["deleted_from_table:1", "inline_flush:1"], (await catalog.CurrentSnapshotAsync()).Changes.Select(c => c.ToString()));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_inlined_data_1_1"));
        Assert.Empty(await SqlAsync("SELECT * FROM ducklake_inlined_delete_1"));
        // The inlined rows' partial file begins at their first snapshot; both delete files are partial.
        Assert.Equal([[2L, 3L, 4L, 0L]], await SqlAsync("SELECT begin_snapshot, partial_max, record_count, row_id_start FROM ducklake_data_file WHERE partial_max IS NOT NULL"));
        Assert.Equal([[4L, 4L, 1L], [6L, 6L, 1L]], await SqlAsync("SELECT begin_snapshot, partial_max, delete_count FROM ducklake_delete_file ORDER BY begin_snapshot"));
        var after = await HistoryAsync(catalog);
        Assert.Equal(before, after.Where(kv => before.ContainsKey(kv.Key)));
        Assert.Equal(before[before.Keys.Max()], after[after.Keys.Max()]);
        Assert.Empty(await catalog.FlushInlinedDataAsync());
    }

    [SkippableFact]
    [Spec("ducklake: Configuring Data Inlining")]
    [Spec("ducklake: Persistent Override")]
    public async Task A_limit_persisted_in_the_lake_wins_over_the_option()
    {
        var catalog = await LakeAsync(limit: 0);
        await _lake.ExecAsync("INSERT INTO ducklake_metadata (\"key\", \"value\", \"scope\", scope_id) VALUES ('data_inlining_row_limit', '5', 'table', 1)");
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L, "a"]]));
        Assert.Empty(_lake.Files());
        await _lake.ExecAsync("UPDATE ducklake_metadata SET \"value\" = '0' WHERE \"key\" = 'data_inlining_row_limit'");
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[2L, "b"]]));
        Assert.Single(_lake.Files());
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    public async Task A_flush_conflicts_with_a_concurrent_inlined_delete_and_another_flush()
    {
        var catalog = await LakeAsync();
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[1L, "a"], [2L, "b"]]));
        await using (var flush = catalog.BeginTransaction())
        {
            Assert.NotNull(await flush.FlushInlinedDataAsync("main", "t", default));
            await CommitAsync(catalog, tx => tx.DeleteAsync("main", "t", Expr.Equal("id", 1L)));
            await Assert.ThrowsAsync<DuckLakeConflictException>(() => flush.CommitAsync());
        }
        await using (var flush = catalog.BeginTransaction())
        {
            Assert.NotNull(await flush.FlushInlinedDataAsync("main", "t", default));
            Assert.Single(await catalog.FlushInlinedDataAsync());
            await Assert.ThrowsAsync<DuckLakeConflictException>(() => flush.CommitAsync());
        }
        // An insert does not conflict with a flush: its rows stay inlined.
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[5L, "e"]]));
        await using (var flush = catalog.BeginTransaction())
        {
            Assert.NotNull(await flush.FlushInlinedDataAsync("main", "t", default));
            await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", [[6L, "f"]]));
            await flush.CommitAsync();
        }
        Assert.Equal([2L, 5L, 6L], (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Select(r => (long)r[0]!).Order());
        Assert.Single(await SqlAsync("SELECT * FROM ducklake_inlined_data_1_1"));
    }

    [SkippableFact]
    [Spec("ducklake: Type Encoding for Data Inlining")]
    [Spec("ducklake: Data Types")]
    public async Task Every_type_reads_back_the_same_inlined_and_flushed()
    {
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { DataInliningRowLimit = 100 });
        var columns = CrossEngineTests.InlinableTypes;
        await CommitAsync(catalog, tx => tx.CreateTableAsync("main", "t", columns));
        var rows = CrossEngineTests.InlinableRows().ToList();
        await CommitAsync(catalog, tx => tx.InsertAsync("main", "t", rows));
        Assert.Empty(_lake.Files());
        var table = await catalog.LoadTableAsync("main", "t");
        var expected = LakeJson.Rows(table.Columns, rows.Select(r => table.Columns.Select((c, i) => DuckLakeValues.Normalize(c.Type, r[i])).ToArray()));
        LakeJson.AssertSameRows(expected, LakeJson.Rows(table.Columns, await catalog.ReadAsync(table)));
        await catalog.FlushInlinedDataAsync();
        Assert.NotEmpty(_lake.Files());
        LakeJson.AssertSameRows(expected, LakeJson.Rows(table.Columns, await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))));
    }
}

public sealed class SqliteInliningTests10() : InliningTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteInliningTests11() : InliningTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresInliningTests10() : InliningTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresInliningTests11() : InliningTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
