using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Iceberg.Net.DuckLake.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// The catalog rows each operation writes (spec "Writing Data"), reads at a snapshot, and commit conflicts, on every
/// catalog database and format version (the subclasses at the end). No other engine.
/// </summary>
public abstract class CatalogTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected CatalogTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    private bool Is11 => _lake.Version != DuckLakeCatalog.FormatVersion10;
    private bool Sqlite => _lake.Backend == CatalogBackend.Sqlite;
    /// <summary>A boolean literal in the catalog database's SQL (DuckDB's SQLite catalogs keep booleans as BIGINT).</summary>
    private string True => Sqlite ? "1" : "TRUE";
    private string False => Sqlite ? "0" : "FALSE";

    public Task InitializeAsync() => Task.CompletedTask;

    /// <summary>Whatever a test did, the lake must still pass a deep validation.</summary>
    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private Task<List<object?[]>> SqlAsync(string sql) => _lake.SqlAsync(sql);

    private async Task<DuckLakeCatalog> WithTableAsync(int rows = 10, string table = "t")
    {
        var catalog = await _lake.CreateAsync();
        await using var tx = catalog.BeginTransaction();
        await tx.CreateTableAsync("main", table, [new("id", DuckLakeType.Int64), new("name", DuckLakeType.Varchar)]);
        if (rows > 0) await tx.InsertAsync("main", table, Enumerable.Range(0, rows).Select(i => new object?[] { (long)i, $"n{i}" }));
        await tx.CommitAsync();
        return catalog;
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_metadata")]
    [Spec("ducklake: Get Current Snapshot")]
    public async Task A_new_catalog_has_snapshot_0_and_the_main_schema()
    {
        var catalog = await _lake.CreateAsync();
        var snapshot = await catalog.CurrentSnapshotAsync();
        Assert.Equal((0L, 0L, 1L, 0L), (snapshot.SnapshotId, snapshot.SchemaVersion, snapshot.NextCatalogId, snapshot.NextFileId));
        Assert.Equal([new DuckLakeChange("created_schema", "\"main\"")], snapshot.Changes);
        Assert.Equal(["main"], await catalog.ListSchemasAsync());
        var meta = (await SqlAsync("SELECT \"key\", \"value\" FROM ducklake_metadata")).ToDictionary(r => (string)r[0]!, r => (string)r[1]!);
        Assert.Equal(_lake.Version, meta["version"]);
        Assert.Equal(_lake.DataPath, meta["data_path"]);
        Assert.Equal(Is11 ? 29 : 28, await _lake.TableCountAsync("ducklake_"));
        Assert.Equal(Is11 ? "1.1" : "1.0", catalog.Version);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_metadata")]
    public async Task Opening_refuses_other_format_versions_and_encrypted_catalogs()
    {
        _lake.Corrupted = true;
        await _lake.CreateAsync();
        await SqlAsync("UPDATE ducklake_metadata SET \"value\" = '0.3' WHERE \"key\" = 'version'");
        await Assert.ThrowsAsync<NotSupportedException>(() => _lake.OpenAsync());
        await SqlAsync($"UPDATE ducklake_metadata SET \"value\" = '{_lake.Version}' WHERE \"key\" = 'version'");
        await SqlAsync("UPDATE ducklake_metadata SET \"value\" = 'true' WHERE \"key\" = 'encrypted'");
        await Assert.ThrowsAsync<NotSupportedException>(() => _lake.OpenAsync());
    }

    [SkippableFact]
    [Spec("ducklake: Snapshot Creation")]
    [Spec("ducklake: CREATE SCHEMA")]
    [Spec("ducklake: CREATE TABLE")]
    [Spec("ducklake: ducklake_schema")]
    [Spec("ducklake: ducklake_table")]
    [Spec("ducklake: ducklake_schema_versions")]
    [Spec("ducklake: List Schemas")]
    [Spec("ducklake: List Tables")]
    public async Task Ddl_takes_catalog_ids_and_bumps_the_schema_version()
    {
        var catalog = await _lake.CreateAsync();
        DuckLakeSnapshot snapshot;
        await using (var tx = catalog.BeginTransaction("alice", "set up"))
        {
            tx.CommitExtraInfo = "{\"job\": 1}";
            await tx.CreateSchemaAsync("sales");
            await tx.CreateTableAsync("sales", "orders", [new("id", DuckLakeType.Int64, Nullable: false), new("items", DuckLakeType.List(DuckLakeType.Varchar))]);
            snapshot = await tx.CommitAsync();
        }
        Assert.Equal((1L, 1L, 3L, 0L), (snapshot.SnapshotId, snapshot.SchemaVersion, snapshot.NextCatalogId, snapshot.NextFileId));
        Assert.Equal("created_schema:\"sales\",created_table:\"sales\".\"orders\"", string.Join(",", snapshot.Changes));
        var stored = (await catalog.ListSnapshotsAsync())[^1];
        Assert.Equal(("alice", "set up", "{\"job\": 1}"), (stored.Author, stored.CommitMessage, stored.CommitExtraInfo));

        Assert.Equal([1L, "sales", "sales/", 1L], (await SqlAsync("SELECT schema_id, schema_name, path, path_is_relative FROM ducklake_schema WHERE schema_id = 1"))[0]);
        Assert.Equal([2L, 1L, "orders", "orders/", 1L, null], (await SqlAsync("SELECT table_id, schema_id, table_name, path, begin_snapshot, end_snapshot FROM ducklake_table"))[0]);
        var columns = await SqlAsync("SELECT column_id, column_name, column_type, nulls_allowed, parent_column FROM ducklake_column ORDER BY column_id");
        Assert.Equal([[1L, "id", "int64", 0L, null], [2L, "items", "list", 1L, null], [3L, "element", "varchar", 1L, 2L]], columns);
        Assert.Equal([[1L, 1L, 2L]], await SqlAsync("SELECT begin_snapshot, schema_version, table_id FROM ducklake_schema_versions"));

        var table = await catalog.LoadTableAsync("sales", "orders");
        Assert.Equal(_lake.DataPath + "sales/orders/", table.Location);
        Assert.Equal(["sales"], (await catalog.ListSchemasAsync()).Where(s => s != "main"));
        Assert.Equal(["orders"], await catalog.ListTablesAsync("sales"));
        // Names resolve case-insensitively when unambiguous, as DuckDB identifiers do.
        Assert.Equal(table.TableId, (await catalog.LoadTableAsync("SALES", "Orders")).TableId);
    }

    [SkippableFact]
    [Spec("ducklake: INSERT")]
    [Spec("ducklake: ducklake_data_file")]
    [Spec("ducklake: ducklake_file_column_stats")]
    [Spec("ducklake: ducklake_table_stats")]
    [Spec("ducklake: ducklake_table_column_stats")]
    public async Task Insert_registers_the_file_with_statistics_and_row_ids()
    {
        var catalog = await WithTableAsync(rows: 0);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[5L, "b"], [1L, null], [9L, "a"]]);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[20L, "z"]]);
            await tx.CommitAsync();
        }
        var files = await SqlAsync("SELECT data_file_id, begin_snapshot, record_count, row_id_start, path_is_relative, file_format, path, file_size_bytes, footer_size FROM ducklake_data_file ORDER BY data_file_id");
        Assert.Equal(2, files.Count);
        Assert.Equal([0L, 2L, 3L, 0L, 1L, "parquet"], files[0][..6]);
        Assert.Equal([1L, 3L, 1L, 3L, 1L, "parquet"], files[1][..6]);
        var location = _lake.DataPath + "main/t/" + (string)files[0][6]!;
        Assert.Equal(new FileInfo(location).Length, files[0][7]);
        Assert.True((long)files[0][8]! > 0 && (long)files[0][8]! < (long)files[0][7]!);

        // value_count counts non-null values, as DuckDB writes it.
        Assert.Equal([[0L, 1L, 3L, 0L, "1", "9", null], [0L, 2L, 2L, 1L, "a", "b", null]],
            await SqlAsync("SELECT data_file_id, column_id, value_count, null_count, min_value, max_value, contains_nan FROM ducklake_file_column_stats WHERE data_file_id = 0 ORDER BY column_id"));
        Assert.Equal([[1L, 4L, 4L]], (await SqlAsync("SELECT table_id, record_count, next_row_id, file_size_bytes FROM ducklake_table_stats")).Select(r => r[..3]));
        Assert.Equal([[1L, 0L, "1", "20"], [2L, 1L, "a", "z"]], await SqlAsync("SELECT column_id, contains_null, min_value, max_value FROM ducklake_table_column_stats ORDER BY column_id"));

        var table = await catalog.LoadTableAsync("main", "t");
        var rows = new List<DuckLakeRow>();
        await foreach (var r in catalog.ReadRowsAsync(table)) rows.Add(r);
        Assert.Equal([0L, 1L, 2L, 3L], rows.Select(r => r.RowId));
        Assert.Equal([5L, 1L, 9L, 20L], rows.Select(r => r.Values[0]));
        Assert.Equal(2L, (await catalog.CurrentSnapshotAsync()).NextFileId);
    }

    [SkippableFact]
    [Spec("ducklake: Nested Types-2")]
    [Spec("ducklake: ducklake_file_column_stats")]
    public async Task Nested_columns_have_statistics_on_their_children_as_in_the_spec_example()
    {
        // Spec "Type Encoding for Statistics", "Nested Types": the table and rows of its example, and its expected child bounds.
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "nested_types", [
                new("col_list", DuckLakeType.List(DuckLakeType.Int32)),
                new("col_struct", DuckLakeType.Struct(("a", DuckLakeType.Int32), ("b", DuckLakeType.Varchar))),
                new("col_map", DuckLakeType.Map(DuckLakeType.Varchar, DuckLakeType.Int32)),
            ]);
            await tx.InsertAsync("main", "nested_types", [
                [new[] { 1, 2, 3 }, new object?[] { 10, "hello" }, new Dictionary<string, int> { ["x"] = 1 }],
                [new[] { 4, 5, 6 }, new object?[] { 20, "world" }, new Dictionary<string, int> { ["y"] = 2 }],
            ]);
            await tx.CommitAsync();
        }
        var stats = await SqlAsync("SELECT c.column_name, s.min_value, s.max_value FROM ducklake_file_column_stats s " +
                                   "JOIN ducklake_column c ON c.column_id = s.column_id AND c.table_id = s.table_id ORDER BY s.column_id");
        Assert.Equal([["element", "1", "6"], ["a", "10", "20"], ["b", "hello", "world"], ["key", "x", "y"], ["value", "1", "2"]], stats);
        var rows = await catalog.ReadAsync(await catalog.LoadTableAsync("main", "nested_types"));
        Assert.Equal(new List<object?> { 4, 5, 6 }, rows[1][0]);
    }

    [SkippableFact]
    [Spec("ducklake: SELECT with File Pruning")]
    [Spec("ducklake: ducklake_file_column_stats")]
    public async Task File_statistics_prune_and_the_residual_filter_is_exact()
    {
        _lake.Corrupted = true;
        var catalog = await WithTableAsync(rows: 0);
        for (var f = 0; f < 4; f++)
        {
            await using var tx = catalog.BeginTransaction();
            await tx.InsertAsync("main", "t", Enumerable.Range(f * 100, 100).Select(i => new object?[] { (long)i, f == 3 ? null : $"n{i}" }));
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "t");
        var plan = await catalog.PlanScanAsync(table, Expr.And(Expr.GreaterThanOrEqual("id", 150L), Expr.LessThan("id", 160L)));
        Assert.Equal((1, 3), (plan.Files.Count, plan.PrunedFiles));
        Assert.Equal(10, (await catalog.ReadAsync(table, Expr.And(Expr.GreaterThanOrEqual("id", 150L), Expr.LessThan("id", 160L)))).Count);
        Assert.Equal(3, (await catalog.PlanScanAsync(table, Expr.NotNull("name"))).Files.Count);   // the all-null file goes
        Assert.Single((await catalog.PlanScanAsync(table, Expr.IsNull("name"))).Files);
        Assert.Empty((await catalog.PlanScanAsync(table, Expr.Equal("id", 10_000L))).Files);
        Assert.Equal(4, (await catalog.PlanScanAsync(table)).Files.Count);
        // Statistics this reader cannot interpret are no statistics: the file is read.
        await SqlAsync("UPDATE ducklake_file_column_stats SET min_value = 'garbage' WHERE data_file_id = 0");
        Assert.Equal(2, (await catalog.PlanScanAsync(table, Expr.LessThan("id", 150L))).Files.Count);
    }

    [SkippableFact]
    [Spec("ducklake: DELETE")]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task Deletes_replace_the_files_delete_file_and_remove_emptied_files()
    {
        var catalog = await WithTableAsync(rows: 10);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[100L, "other file"]]);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            Assert.Equal(2, await tx.DeleteAsync("main", "t", Expr.In("id", 1L, 2L)));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            Assert.Equal(2, await tx.DeleteAsync("main", "t", Expr.In("id", 3L, 100L)));
            await tx.CommitAsync();
        }
        var deletes = await SqlAsync("SELECT delete_file_id, data_file_id, begin_snapshot, end_snapshot, delete_count, format FROM ducklake_delete_file ORDER BY delete_file_id");
        // The second delete on file 0 ends the first delete file and writes one holding all three positions.
        Assert.Equal([[2L, 0L, 3L, 4L, 2L, "parquet"], [3L, 0L, 4L, null, 3L, "parquet"]], deletes);
        // File 1 lost its only row: it leaves the table instead of getting a delete file.
        Assert.Equal([[1L, 4L]], await SqlAsync("SELECT data_file_id, end_snapshot FROM ducklake_data_file WHERE end_snapshot IS NOT NULL"));
        Assert.Equal([[7L]], await SqlAsync("SELECT record_count FROM ducklake_table_stats"));
        Assert.Equal([["deleted_from_table:1"], ["deleted_from_table:1"]], await SqlAsync("SELECT changes_made FROM ducklake_snapshot_changes WHERE snapshot_id IN (3, 4)"));

        var table = await catalog.LoadTableAsync("main", "t");
        Assert.Equal([0L, 4L, 5L, 6L, 7L, 8L, 9L], (await catalog.ReadAsync(table)).Select(r => (long)r[0]!).Order());
        var before = await catalog.LoadTableAsync("main", "t", 3);
        Assert.Equal(9, (await catalog.ReadAsync(before)).Count);
    }

    [SkippableFact]
    [Spec("ducklake: UPDATE")]
    public async Task Update_is_a_delete_and_an_insert_in_one_snapshot()
    {
        var catalog = await WithTableAsync(rows: 5);
        DuckLakeSnapshot s;
        await using (var tx = catalog.BeginTransaction())
        {
            Assert.Equal(2, await tx.UpdateAsync("main", "t", Expr.GreaterThan("id", 2L), r => [r[0], "changed"]));
            s = await tx.CommitAsync();
        }
        Assert.Equal("inserted_into_table:1,deleted_from_table:1", string.Join(",", s.Changes));
        var rows = await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"));
        Assert.Equal(["n0", "n1", "n2", "changed", "changed"], rows.OrderBy(r => (long)r[0]!).Select(r => (string)r[1]!));
    }

    [SkippableFact]
    [Spec("ducklake: DROP TABLE")]
    [Spec("ducklake: DROP SCHEMA")]
    public async Task Drops_end_rows_and_old_snapshots_still_read()
    {
        var catalog = await WithTableAsync(rows: 3);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateSchemaAsync("s");
            await tx.CreateTableAsync("s", "x", [new("a", DuckLakeType.Int32)]);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.DropSchemaAsync("s")); // not empty
            await tx.DropTableAsync("s", "x");
            await tx.DropSchemaAsync("s");                                                      // empty once x goes, same snapshot
            await tx.DropTableAsync("main", "t");
            await tx.CommitAsync();
        }
        Assert.Empty(await catalog.ListTablesAsync("main"));
        Assert.Equal(["main"], await catalog.ListSchemasAsync());
        await Assert.ThrowsAsync<DuckLakeNotFoundException>(() => catalog.LoadTableAsync("main", "t"));
        Assert.Equal([[3L]], await SqlAsync("SELECT end_snapshot FROM ducklake_data_file"));
        Assert.Equal([[3L]], await SqlAsync("SELECT DISTINCT end_snapshot FROM ducklake_column WHERE table_id = 1"));
        // Time travel to before the drop still reads the table.
        Assert.Equal(3, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t", 2))).Count);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_column")]
    [Spec("ducklake: Show the Structure of a Table")]
    public async Task Schema_evolution_versions_columns_by_snapshot()
    {
        var catalog = await WithTableAsync(rows: 2);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.AddColumnAsync("main", "t", new("score", DuckLakeType.Float64));
            await tx.RenameColumnAsync("main", "t", "name", "label");
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DropColumnAsync("main", "t", "score");
            await tx.AddColumnAsync("main", "t", new("tags", DuckLakeType.List(DuckLakeType.Int32)));
            await tx.InsertAsync("main", "t", [[7L, "seven", new[] { 1, 2 }]]);
            await tx.CommitAsync();
        }
        var at2 = await catalog.LoadTableAsync("main", "t", 1);
        var at3 = await catalog.LoadTableAsync("main", "t", 2);
        var now = await catalog.LoadTableAsync("main", "t");
        Assert.Equal(["id", "name"], at2.Columns.Select(c => c.Name));
        Assert.Equal(["id", "label", "score"], at3.Columns.Select(c => c.Name));
        Assert.Equal(["id", "label", "tags"], now.Columns.Select(c => c.Name));
        // IDs are never reused: score was 3, tags takes 4 and its element 5.
        Assert.Equal([1L, 2L, 4L], now.Columns.Select(c => c.Id));
        Assert.Equal(5L, now.Columns[2].Children[0].Id);
        Assert.Equal(2L, now.Column("label").Id);
        var rows = (await catalog.ReadAsync(now)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal([null, null, new List<object?> { 1, 2 }], rows.Select(r => r[2]));
        Assert.Equal(["n0", "n1", "seven"], rows.Select(r => r[1]));
        Assert.Equal([2L, 3L], (await catalog.ListSnapshotsAsync()).Skip(2).Select(s => s.SchemaVersion));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_partition_column")]
    [Spec("ducklake: ducklake_file_partition_value")]
    public async Task Partitioned_inserts_write_one_file_per_partition()
    {
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "e", [new("id", DuckLakeType.Int32), new("ts", DuckLakeType.TimestampTz), new("k", DuckLakeType.Varchar)]);
            await tx.SetPartitioningAsync("main", "e", [("ts", "month"), ("k", "identity")]);
            await tx.InsertAsync("main", "e", [
                [1, new DateTimeOffset(2024, 1, 5, 0, 0, 0, TimeSpan.Zero), "a/b"],
                [2, new DateTimeOffset(2024, 1, 9, 0, 0, 0, TimeSpan.Zero), "a/b"],
                [3, new DateTimeOffset(2024, 2, 1, 0, 0, 0, TimeSpan.Zero), null],
            ]);
            await tx.CommitAsync();
        }
        Assert.Equal([[0L, 2L, "month"], [1L, 3L, "identity"]], await SqlAsync("SELECT partition_key_index, column_id, \"transform\" FROM ducklake_partition_column ORDER BY partition_key_index"));
        Assert.Equal([[0L, 0L, "1"], [0L, 1L, "a/b"], [1L, 0L, "2"], [1L, 1L, null]],
            await SqlAsync("SELECT data_file_id, partition_key_index, partition_value FROM ducklake_file_partition_value ORDER BY data_file_id, partition_key_index"));
        var paths = (await SqlAsync("SELECT path, partition_id FROM ducklake_data_file ORDER BY data_file_id")).ToList();
        Assert.StartsWith("month=1/k=a%2Fb/ducklake-", (string)paths[0][0]!);
        Assert.StartsWith("month=2/k=__HIVE_DEFAULT_PARTITION__/", (string)paths[1][0]!);
        Assert.All(paths, p => Assert.Equal(2L, p[1]));
        Assert.Throws<NotSupportedException>(() => DuckLakePartitioning.Validate(DuckLakeType.Int32, "truncate(3)"));
        Assert.Throws<ArgumentException>(() => DuckLakePartitioning.Validate(DuckLakeType.Date, "hour"));
    }

    [SkippableFact]
    [Spec("ducklake: Note on Paths")]
    public async Task Absolute_and_relative_paths_resolve_through_schema_and_table()
    {
        _lake.Corrupted = true;
        var catalog = await WithTableAsync(rows: 2);
        var elsewhere = Path.Combine(_lake.Root, "moved") + "/";
        Directory.CreateDirectory(elsewhere);
        File.Move(Directory.EnumerateFiles(_lake.DataPath + "main/t/").Single(), elsewhere + "f.parquet");
        await SqlAsync($"UPDATE ducklake_table SET path = '{elsewhere}', path_is_relative = {False}");
        await SqlAsync("UPDATE ducklake_data_file SET path = 'f.parquet'");
        Assert.Equal(2, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Count);
        await SqlAsync($"UPDATE ducklake_data_file SET path = '{elsewhere}f.parquet', path_is_relative = {False}");
        await SqlAsync($"UPDATE ducklake_table SET path = 'nowhere/', path_is_relative = {True}");
        Assert.Equal(2, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Count);
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    [Spec("ducklake: Data")]
    public async Task Concurrent_inserts_both_commit_after_a_metadata_retry()
    {
        var catalog = await WithTableAsync(rows: 0);
        var a = catalog.BeginTransaction();
        var b = catalog.BeginTransaction();
        await a.InsertAsync("main", "t", [[1L, "a"]]);
        await b.InsertAsync("main", "t", [[2L, "b"]]);
        var sa = await a.CommitAsync();
        var sb = await b.CommitAsync(); // based on the same snapshot as a: no logical conflict, so it lands on top
        Assert.Equal(sa.SnapshotId + 1, sb.SnapshotId);
        Assert.Equal([[0L, 0L], [1L, 1L]], await SqlAsync("SELECT data_file_id, row_id_start FROM ducklake_data_file ORDER BY data_file_id"));
        Assert.Equal(2, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Count);
    }

    [SkippableFact]
    [Spec("ducklake: Data")]
    public async Task An_insert_conflicting_with_a_concurrent_delete_fails_and_cleans_up_its_files()
    {
        var catalog = await WithTableAsync(rows: 5);
        var inserter = catalog.BeginTransaction();
        await inserter.InsertAsync("main", "t", [[9L, "late"]]);
        await using (var deleter = catalog.BeginTransaction())
        {
            await deleter.DeleteAsync("main", "t", Expr.Equal("id", 1L));
            await deleter.CommitAsync();
        }
        var before = _lake.Files().Count();
        await Assert.ThrowsAsync<DuckLakeConflictException>(() => inserter.CommitAsync());
        Assert.Equal(before - 1, _lake.Files().Count());
        Assert.Equal(4, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Count);
    }

    [SkippableFact]
    [Spec("ducklake: Data")]
    public async Task Deletes_conflict_only_on_the_same_data_file()
    {
        var catalog = await WithTableAsync(rows: 5);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", Enumerable.Range(10, 5).Select(i => new object?[] { (long)i, "second file" }));
            await tx.CommitAsync();
        }
        var first = catalog.BeginTransaction();
        var sameFile = catalog.BeginTransaction();
        var otherFile = catalog.BeginTransaction();
        await first.DeleteAsync("main", "t", Expr.Equal("id", 1L));
        await sameFile.DeleteAsync("main", "t", Expr.Equal("id", 2L));
        await otherFile.DeleteAsync("main", "t", Expr.Equal("id", 12L));
        await first.CommitAsync();
        await Assert.ThrowsAsync<DuckLakeConflictException>(() => sameFile.CommitAsync());
        await otherFile.CommitAsync();
        var rows = await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"));
        Assert.Equal([0L, 2L, 3L, 4L, 10L, 11L, 13L, 14L], rows.Select(r => (long)r[0]!).Order());
    }

    [SkippableFact]
    [Spec("ducklake: Tables / Views")]
    public async Task Concurrent_creation_of_the_same_table_conflicts()
    {
        var catalog = await _lake.CreateAsync();
        var a = catalog.BeginTransaction();
        var b = catalog.BeginTransaction();
        await a.CreateTableAsync("main", "same", [new("x", DuckLakeType.Int32)]);
        await b.CreateTableAsync("main", "same", [new("y", DuckLakeType.Int32)]);
        await a.CommitAsync();
        await Assert.ThrowsAsync<DuckLakeConflictException>(() => b.CommitAsync());
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_table_column_stats")]
    [Spec("ducklake: Snapshot Validity")]
    public async Task Table_statistics_only_widen()
    {
        var catalog = await WithTableAsync(rows: 0);
        foreach (var batch in new[] { new[] { 5L, 6L }, [1L], [3L] })
        {
            await using var tx = catalog.BeginTransaction();
            await tx.InsertAsync("main", "t", batch.Select(i => new object?[] { i, null }));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Equal("id", 1L));
            await tx.CommitAsync();
        }
        // Bounds stay valid upper/lower bounds after the delete (they are not narrowed, so next_file_id need not move for them).
        Assert.Equal([[1L, 0L, "1", "6"], [2L, 1L, null, null]], await SqlAsync("SELECT column_id, contains_null, min_value, max_value FROM ducklake_table_column_stats ORDER BY column_id"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_snapshot")]
    public async Task Time_travel_by_timestamp_and_unknown_snapshots()
    {
        var catalog = await WithTableAsync(rows: 1);
        var snapshots = await catalog.ListSnapshotsAsync();
        Assert.Equal(snapshots[^1].SnapshotId, (await catalog.SnapshotAsOfAsync(DateTimeOffset.UtcNow.AddMinutes(1))).SnapshotId);
        Assert.Equal(0, (await catalog.SnapshotAsOfAsync(snapshots[0].Time)).SnapshotId);
        await Assert.ThrowsAsync<DuckLakeNotFoundException>(() => catalog.SnapshotAsOfAsync(snapshots[0].Time.AddDays(-1)));
        await Assert.ThrowsAsync<DuckLakeNotFoundException>(() => catalog.LoadTableAsync("main", "t", 42));
        await Assert.ThrowsAsync<DuckLakeNotFoundException>(() => catalog.LoadTableAsync("main", "t", 0)); // before it existed
    }

    [SkippableFact]
    public async Task Abandoned_transactions_delete_their_files_and_commit_is_single_use()
    {
        var catalog = await WithTableAsync(rows: 1);
        var before = _lake.Files().Count();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[2L, "x"]]);
            Assert.Equal(before + 1, _lake.Files().Count());
        }
        Assert.Equal(before, _lake.Files().Count());
        var once = catalog.BeginTransaction();
        await once.InsertAsync("main", "t", [[3L, "y"]]);
        await once.CommitAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => once.CommitAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => catalog.BeginTransaction().CommitAsync()); // nothing staged
    }

    [SkippableFact]
    public async Task Rows_are_validated_against_the_table()
    {
        var catalog = await _lake.CreateAsync();
        await using var tx = catalog.BeginTransaction();
        await tx.CreateTableAsync("main", "v", [new("id", DuckLakeType.Int8, Nullable: false), new("d", DuckLakeType.Decimal(4, 1))]);
        await Assert.ThrowsAsync<ArgumentException>(() => tx.InsertAsync("main", "v", [[null, 1.5m]]));         // NOT NULL
        await Assert.ThrowsAsync<ArgumentException>(() => tx.InsertAsync("main", "v", [[300, 1.5m]]));          // int8 range
        await Assert.ThrowsAsync<ArgumentException>(() => tx.InsertAsync("main", "v", [[1]]));                  // arity
        await Assert.ThrowsAsync<ArgumentException>(() => tx.InsertAsync("main", "v", [[1, "not a number"]]));  // type
    }

    [SkippableFact]
    [Spec("ducklake: SELECT")]
    public async Task Untrusted_catalog_rows_fail_as_format_errors()
    {
        _lake.Corrupted = true;
        var catalog = await WithTableAsync(rows: 2);
        await SqlAsync("INSERT INTO ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size) " +
                       $"VALUES (90, 1, 1, NULL, 0, 'a.parquet', {True}, 'parquet', 1, 1, 1), (91, 1, 1, NULL, 0, 'b.parquet', {True}, 'parquet', 1, 1, 1)");
        var table = await catalog.LoadTableAsync("main", "t");
        // The Corruption contract: a format error (never a NullReference, cast or index exception).
        async Task FormatError(Func<Task> action) => Assert.True(Corruption.IsFormatError((await Record.ExceptionAsync(action))!));
        await FormatError(() => catalog.ReadAsync(table)); // two delete files for one data file
        await SqlAsync("DELETE FROM ducklake_delete_file");
        await SqlAsync("UPDATE ducklake_snapshot_changes SET changes_made = 'created_table:\"unterminated' WHERE snapshot_id = 1");
        await FormatError(() => catalog.ListSnapshotsAsync());
        await SqlAsync("UPDATE ducklake_snapshot_changes SET changes_made = NULL");
        if (Sqlite)
        {
            // Text where SQLite catalogs keep TIMESTAMPTZ (PostgreSQL's native type cannot hold a malformed value).
            await SqlAsync("UPDATE ducklake_snapshot SET snapshot_time = 'yesterday'");
            await FormatError(() => catalog.ListSnapshotsAsync());
            await SqlAsync("UPDATE ducklake_snapshot SET snapshot_time = '2026-01-01 00:00:00+00'");
        }
        await SqlAsync("UPDATE ducklake_column SET parent_column = 99 WHERE column_id = 2");
        await FormatError(() => catalog.LoadTableAsync("main", "t"));
        await SqlAsync("UPDATE ducklake_column SET parent_column = NULL, column_type = NULL WHERE column_id = 2");
        await FormatError(() => catalog.LoadTableAsync("main", "t"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    public async Task Files_this_library_cannot_read_correctly_are_refused()
    {
        _lake.Corrupted = true;
        var catalog = await WithTableAsync(rows: 2);
        var table = await catalog.LoadTableAsync("main", "t");
        await SqlAsync("UPDATE ducklake_data_file SET encryption_key = 'k'");
        await Assert.ThrowsAsync<NotSupportedException>(() => catalog.ReadAsync(table));
        await SqlAsync("UPDATE ducklake_data_file SET encryption_key = NULL, mapping_id = 1");
        await Assert.ThrowsAsync<NotSupportedException>(() => catalog.ReadAsync(table));
        await SqlAsync("UPDATE ducklake_data_file SET mapping_id = NULL, file_format = 'orc'");
        await Assert.ThrowsAsync<NotSupportedException>(() => catalog.ReadAsync(table));
    }

    [SkippableFact]
    [Spec("ducklake: Full Schema Creation Script")]
    public async Task The_dialect_matches_the_schema_DuckDB_creates()
    {
        await _lake.CreateAsync();
        if (Sqlite)
        {
            var columns = await SqlAsync("SELECT sql FROM sqlite_master WHERE name = 'ducklake_snapshot'");
            Assert.Equal("CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time VARCHAR, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)", columns[0][0]);
            return;
        }
        // The column types DuckDB 1.5.5 creates in PostgreSQL 16 (information_schema.columns.udt_name).
        async Task<string> Columns(string table) => string.Join(" ", (await SqlAsync(
                "SELECT column_name || ':' || udt_name FROM information_schema.columns " +
                $"WHERE table_schema = current_schema() AND table_name = '{table}' ORDER BY ordinal_position")).Select(r => (string)r[0]!));
        Assert.Equal("snapshot_id:int8 snapshot_time:timestamptz schema_version:int8 next_catalog_id:int8 next_file_id:int8", await Columns("ducklake_snapshot"));
        Assert.Equal("schema_id:int8 schema_uuid:uuid begin_snapshot:int8 end_snapshot:int8 schema_name:varchar path:varchar path_is_relative:bool"
                     + (Is11 ? " parent_schema_id:int8" : ""), await Columns("ducklake_schema"));
        Assert.Equal("table_id:int8 column_id:int8 contains_null:bool contains_nan:bool min_value:varchar max_value:varchar extra_stats:varchar"
                     + (Is11 ? " min_is_exact:bool max_is_exact:bool" : ""), await Columns("ducklake_table_column_stats"));
        var keys = (await SqlAsync("SELECT conrelid::regclass::text FROM pg_constraint WHERE contype = 'p' AND connamespace = current_schema()::regnamespace ORDER BY 1"))
            .Select(r => (string)r[0]!);
        Assert.Equal(["ducklake_data_file", "ducklake_delete_file", "ducklake_schema", "ducklake_snapshot", "ducklake_snapshot_changes"], keys);
    }
}

public sealed class SqliteCatalogTests10() : CatalogTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteCatalogTests11() : CatalogTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresCatalogTests10() : CatalogTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresCatalogTests11() : CatalogTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
