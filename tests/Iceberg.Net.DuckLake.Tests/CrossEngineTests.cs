using Iceberg.Net.Expressions;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// DuckDB's <c>ducklake</c> extension reads what this library writes, and this library reads what DuckDB writes, on
/// the same SQLite catalog and data directory.
/// </summary>
public abstract class CrossEngineTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected CrossEngineTests(CatalogBackend backend, string version)
    {
        Oracle = DuckLakeOracle.For(version);
        _lake = new TestLake(backend, version);
    }

    /// <summary>The DuckDB whose ducklake extension writes this class's format version.</summary>
    private DuckLakeOracle Oracle { get; }

    public Task InitializeAsync() => Task.CompletedTask;

    /// <summary>Whatever both engines did, the lake must still pass a deep validation.</summary>
    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    internal static readonly DuckLakeColumnDefinition[] AllTypes =
    [
        new("id", DuckLakeType.Int64, Nullable: false),
        new("i8", DuckLakeType.Int8),
        new("i16", DuckLakeType.Int16),
        new("i32", DuckLakeType.Int32),
        new("f32", DuckLakeType.Float32),
        new("f64", DuckLakeType.Float64),
        new("flag", DuckLakeType.Boolean),
        new("amount", DuckLakeType.Decimal(10, 2)),
        new("big", DuckLakeType.Decimal(38, 4)),
        new("day", DuckLakeType.Date),
        new("at", DuckLakeType.Time),
        new("ts", DuckLakeType.Timestamp),
        new("tstz", DuckLakeType.TimestampTz),
        new("name", DuckLakeType.Varchar),
        new("doc", DuckLakeType.Json),
        new("raw", DuckLakeType.Blob),
        new("uid", DuckLakeType.Uuid),
        new("tags", DuckLakeType.List(DuckLakeType.Varchar)),
        new("point", DuckLakeType.Struct(("x", DuckLakeType.Int32), ("label", DuckLakeType.Varchar))),
        new("attrs", DuckLakeType.Map(DuckLakeType.Varchar, DuckLakeType.Int64)),
    ];

    internal static object?[] AllTypesRow(long i) =>
    [
        i, (int)(sbyte)(i % 100), (int)(short)(i * 7), (int)(i * 1000), i / 3f, i / 7d, i % 2 == 0,
        decimal.Round(i * 1.25m, 2), decimal.Round(i * 1234567.891m, 4),
        new DateOnly(2024, 1, 1).AddDays((int)i), new TimeOnly(10, 30).Add(TimeSpan.FromSeconds(i)),
        new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc).AddMinutes(i), new DateTimeOffset(2024, 6, 1, 0, 0, 0, TimeSpan.Zero).AddHours(i),
        $"name-{i}-é", $"{{\"k\": {i}}}", new byte[] { (byte)i, 0xFF }, new Guid((int)i, 1, 2, [3, 4, 5, 6, 7, 8, 9, 10]),
        new List<object?> { $"t{i}", i % 3 == 0 ? null : "x" }, new object?[] { (int)i, $"p{i}" }, new Dictionary<object, object?> { [$"k{i}"] = i },
    ];

    private static object?[] NullsRow(long i) => [i, .. Enumerable.Repeat<object?>(null, AllTypes.Length - 1)];

    [SkippableFact]
    [Spec("ducklake: INSERT")]
    [Spec("ducklake: Data Types")]
    public async Task DuckDB_reads_every_type_this_library_writes()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction("tests", "create"))
        {
            await tx.CreateSchemaAsync("s");
            await tx.CreateTableAsync("s", "all_types", AllTypes);
            await tx.InsertAsync("s", "all_types", Enumerable.Range(0, 20).Select(i => AllTypesRow(i)).Append(NullsRow(99)));
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("s", "all_types");
        var ours = await catalog.ReadAsync(table);
        Assert.Equal(21, ours.Count);
        var theirs = await Oracle.RowsAsync(_lake, "SELECT * FROM lake.s.all_types");
        LakeJson.AssertSameRows(theirs, LakeJson.Rows(table.Columns, ours));
    }

    [SkippableFact]
    [Spec("ducklake: DELETE")]
    [Spec("ducklake: UPDATE")]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task DuckDB_reads_deletes_and_updates_and_counts_match()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("v", DuckLakeType.Varchar)]);
            await tx.InsertAsync("main", "t", Enumerable.Range(0, 100).Select(i => new object?[] { (long)i, $"v{i}" }));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            Assert.Equal(10, await tx.DeleteAsync("main", "t", Expr.LessThan("id", 10L)));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            // A second delete on the same data file replaces its delete file (at most one per data file per snapshot).
            Assert.Equal(5, await tx.DeleteAsync("main", "t", Expr.And(Expr.GreaterThanOrEqual("id", 50L), Expr.LessThan("id", 55L))));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            Assert.Equal(1, await tx.UpdateAsync("main", "t", Expr.Equal("id", 99L), r => [r[0], "updated"]));
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "t");
        var ours = await catalog.ReadAsync(table);
        Assert.Equal(85, ours.Count);
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.t"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(["[85]"], await Oracle.RowsAsync(_lake, "SELECT count(*) FROM lake.t"));
        Assert.Equal(["[\"updated\"]"], await Oracle.RowsAsync(_lake, "SELECT v FROM lake.t WHERE id = 99"));
        // The updated row keeps its row ID (stored in the new file's _ducklake_internal_row_id), in both engines.
        var rowIds = new List<DuckLakeRow>();
        await foreach (var r in catalog.ReadRowsAsync(table)) rowIds.Add(r);
        Assert.Equal(99L, rowIds.Single(r => (long)r.Values[0]! == 99L).RowId);
        Assert.Equal(["[99]"], await Oracle.RowsAsync(_lake, "SELECT rowid FROM lake.t WHERE id = 99"));
        // DuckDB's own filtered reads (its pruning uses the statistics written here) agree.
        Assert.Equal(["[40]"], await Oracle.RowsAsync(_lake, "SELECT count(*) FROM lake.t WHERE id >= 10 AND id < 55"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_snapshot")]
    public async Task Time_travel_agrees_with_DuckDB_at_every_snapshot()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int32)]);
            await tx.CommitAsync();
        }
        for (var batch = 0; batch < 3; batch++)
        {
            await using var tx = catalog.BeginTransaction();
            await tx.InsertAsync("main", "t", Enumerable.Range(batch * 10, 10).Select(i => new object?[] { i }));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Equal("id", 15));
            await tx.CommitAsync();
        }
        foreach (var snapshot in await catalog.ListSnapshotsAsync())
        {
            if (snapshot.SnapshotId < 1) continue;
            var table = await catalog.LoadTableAsync("main", "t", snapshot.SnapshotId);
            var ours = LakeJson.Rows(table.Columns, await catalog.ReadAsync(table));
            Assert.Equal(ours, await Oracle.RowsAsync(_lake, $"SELECT * FROM lake.t AT (VERSION => {snapshot.SnapshotId})"));
        }
    }

    /// <summary>Our rows of <c>main.t</c> at a snapshot, each led by its row ID, as the oracle renders <c>SELECT rowid, *</c>.</summary>
    private static async Task<List<string>> RowsWithIdsAsync(DuckLakeCatalog catalog, long snapshot)
    {
        var table = await catalog.LoadTableAsync("main", "t", snapshot);
        var rows = new List<string>();
        await foreach (var r in catalog.ReadRowsAsync(table))
            rows.Add(LakeJson.Canonical(new System.Text.Json.Nodes.JsonArray([System.Text.Json.Nodes.JsonValue.Create(r.RowId),
                .. table.Columns.Select((c, i) => LakeJson.Value(c.Type, r.Values[i]))]))!.ToJsonString());
        return rows.Order(StringComparer.Ordinal).ToList();
    }

    /// <summary>Both engines read every snapshot of <c>main.t</c> that is still there the same, row IDs included.</summary>
    private async Task AssertSameHistoryAsync(DuckLakeCatalog catalog)
    {
        var snapshots = (await catalog.ListSnapshotsAsync()).Select(s => s.SnapshotId).ToList();
        var checkedAny = false;
        foreach (var s in snapshots)
        {
            List<string> ours;
            try { ours = await RowsWithIdsAsync(catalog, s); }
            catch (DuckLakeNotFoundException) { continue; } // before the table existed
            LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, $"SELECT rowid, * FROM lake.t AT (VERSION => {s})"), ours);
            checkedAny = true;
        }
        Assert.True(checkedAny);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Maintenance_done_here_reads_the_same_in_DuckDB_which_cleans_up_after_it()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("v", DuckLakeType.Varchar)]);
            await tx.InsertAsync("main", "t", Enumerable.Range(0, 20).Select(i => new object?[] { (long)i, $"v{i}" }));
            await tx.CommitAsync();
        }
        for (var c = 0; c < 3; c++)
        {
            await using var tx = catalog.BeginTransaction();
            await tx.InsertAsync("main", "t", Enumerable.Range(20 + c * 3, 3).Select(i => new object?[] { (long)i, $"v{i}" }));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.LessThan("id", 19L)); // 19 of the first file's 20 rows
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.UpdateAsync("main", "t", Expr.Equal("id", 21L), r => [r[0], "updated"]); // keeps its row ID
            await tx.CommitAsync();
        }
        Assert.NotEmpty(await catalog.MergeAdjacentFilesAsync());
        Assert.NotEmpty(await catalog.RewriteDataFilesAsync());
        Assert.Equal(2, (await catalog.ExpireSnapshotsAsync([2, 3])).ExpiredSnapshots.Count);
        await AssertSameHistoryAsync(catalog);

        var scheduled = (await _lake.SqlAsync("SELECT path FROM ducklake_files_scheduled_for_deletion")).Select(r => _lake.DataPath + (string)r[0]!).ToList();
        Assert.NotEmpty(scheduled);
        Assert.All(scheduled, f => Assert.True(File.Exists(f), f));
        await Oracle.RunAsync(_lake, "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)");
        Assert.All(scheduled, f => Assert.False(File.Exists(f), f));
        Assert.Empty(await _lake.SqlAsync("SELECT * FROM ducklake_files_scheduled_for_deletion"));
        await AssertSameHistoryAsync(catalog);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task This_library_reads_DuckDBs_maintenance_and_cleans_up_after_it()
    {
        Oracle.Require();
        await Oracle.RunAsync(_lake,
            "CALL lake.set_option('data_inlining_row_limit', 0)",
            "CREATE TABLE lake.t(id BIGINT, v VARCHAR)",
            "INSERT INTO lake.t SELECT i, 'v' || i FROM range(20) r(i)",
            "INSERT INTO lake.t VALUES (20, 'v20'), (21, 'v21')",
            "INSERT INTO lake.t VALUES (22, 'v22')",
            "INSERT INTO lake.t VALUES (23, 'v23')",
            "DELETE FROM lake.t WHERE id < 15",
            "UPDATE lake.t SET v = 'updated' WHERE id = 22",
            "CALL ducklake_merge_adjacent_files('lake')",
            "CALL ducklake_rewrite_data_files('lake', delete_threshold => 0.5)",
            "CALL ducklake_expire_snapshots('lake', versions => [2, 3])");
        var catalog = await _lake.OpenAsync();
        Assert.DoesNotContain(2L, (await catalog.ListSnapshotsAsync()).Select(s => s.SnapshotId));
        await AssertSameHistoryAsync(catalog);
        var scheduled = await catalog.CleanupOldFilesAsync();
        Assert.NotEmpty(scheduled);
        Assert.All(scheduled, f => Assert.False(File.Exists(f), f));
        await AssertSameHistoryAsync(catalog);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_column")]
    public async Task Schema_evolution_written_here_reads_the_same_in_DuckDB()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int32), new("old", DuckLakeType.Varchar), new("gone", DuckLakeType.Int32)]);
            await tx.InsertAsync("main", "t", [[1, "a", 10], [2, "b", 20]]);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.RenameColumnAsync("main", "t", "old", "renamed");
            await tx.DropColumnAsync("main", "t", "gone");
            await tx.AddColumnAsync("main", "t", new("added", DuckLakeType.Float64));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[3, "c", 3.5]]);
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "t");
        Assert.Equal(["id", "renamed", "added"], table.Columns.Select(c => c.Name));
        var ours = await catalog.ReadAsync(table);
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.t"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(["[1,\"a\",null]", "[2,\"b\",null]", "[3,\"c\",3.5]"], LakeJson.Rows(table.Columns, ours));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_partition_info")]
    [Spec("ducklake: ducklake_file_partition_value")]
    public async Task Partitioned_writes_read_the_same_in_DuckDB()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "events", [new("id", DuckLakeType.Int64), new("region", DuckLakeType.Varchar), new("day", DuckLakeType.Date)]);
            await tx.SetPartitioningAsync("main", "events", [("region", "identity"), ("day", "year")]);
            await tx.InsertAsync("main", "events", Enumerable.Range(0, 30).Select(i => new object?[]
            {
                (long)i, i % 3 == 0 ? null : i % 3 == 1 ? "eu" : "us", new DateOnly(2023 + i % 2, 3, 1),
            }));
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "events");
        Assert.Equal(6, (await catalog.PlanScanAsync(table)).Files.Count);
        var ours = await catalog.ReadAsync(table);
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.events"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(["[5]"], await Oracle.RowsAsync(_lake, "SELECT count(*) FROM lake.events WHERE region = 'eu' AND year(day) = 2024"));
    }

    [SkippableFact]
    [Spec("ducklake: SELECT")]
    [Spec("ducklake: Data Inlining")]
    [Spec("ducklake: ducklake_inlined_data_tables")]
    [Spec("ducklake: ALTER TABLE Support")]
    [Spec("ducklake: SQLite Catalog")]
    [Spec("ducklake: PostgreSQL Catalog")]
    public async Task This_library_reads_what_DuckDB_writes_including_inlined_rows_deletes_and_alters()
    {
        Oracle.Require();
        await Oracle.RunAsync(_lake,
            "CREATE SCHEMA lake.s",
            "CREATE TABLE lake.s.t (id BIGINT, name VARCHAR, score DOUBLE, day DATE, ts TIMESTAMPTZ, amount DECIMAL(9,3), tags INT[], p STRUCT(a INT, b VARCHAR))",
            // Large enough to go to Parquet...
            "INSERT INTO lake.s.t SELECT i, 'n' || i, i / 2, DATE '2024-01-01' + i::INT, TIMESTAMPTZ '2024-01-01 00:00:00+00' + INTERVAL (i) MINUTE, i / 8, [i::INT, NULL], {'a': i::INT, 'b': 'b' || i} FROM range(1000) r(i)",
            // ...and small inserts, which DuckDB keeps inlined in the catalog (primitive columns only here).
            "INSERT INTO lake.s.t (id, name, score, day, ts, amount) VALUES (5000, 'inlined', 1.25, DATE '1999-12-31', TIMESTAMPTZ '1999-12-31 23:59:59.5+00', 12.345)",
            "INSERT INTO lake.s.t (id, name) VALUES (5001, 'it''s inlined')",
            "DELETE FROM lake.s.t WHERE id % 10 = 0 OR id = 5001",
            "UPDATE lake.s.t SET name = 'upd' WHERE id = 7",
            "ALTER TABLE lake.s.t ADD COLUMN extra VARCHAR",
            "INSERT INTO lake.s.t (id, extra) VALUES (6000, 'after alter')");
        var catalog = await _lake.OpenAsync();
        var table = await catalog.LoadTableAsync("s", "t");
        var ours = await catalog.ReadAsync(table);
        var theirs = await Oracle.RowsAsync(_lake, "SELECT * FROM lake.s.t");
        LakeJson.AssertSameRows(theirs, LakeJson.Rows(table.Columns, ours));

        // Pruned scans return exactly the filtered rows.
        var filtered = await catalog.ReadAsync(table, Expr.And(Expr.GreaterThanOrEqual("id", 100L), Expr.LessThan("id", 120L)));
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.s.t WHERE id >= 100 AND id < 120"), LakeJson.Rows(table.Columns, filtered));
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    public async Task Both_engines_write_to_the_same_lake_in_turn()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("who", DuckLakeType.Varchar)]);
            await tx.InsertAsync("main", "t", Enumerable.Range(0, 50).Select(i => new object?[] { (long)i, "dotnet" }));
            await tx.CommitAsync();
        }
        await Oracle.RunAsync(_lake,
            "CALL lake.set_option('data_inlining_row_limit', 0)",
            "INSERT INTO lake.t SELECT i, 'duckdb' FROM range(50, 100) r(i)",
            "DELETE FROM lake.t WHERE id % 7 = 0");
        await using (var tx = catalog.BeginTransaction())
        {
            // Deletes on top of DuckDB's delete files: the replacement holds both engines' positions.
            Assert.Equal(42, await tx.DeleteAsync("main", "t", Expr.Equal("who", "dotnet")));
            await tx.InsertAsync("main", "t", [[1000L, "dotnet-again"]]);
            await tx.CommitAsync();
        }
        await Oracle.RunAsync(_lake, "UPDATE lake.t SET who = 'duckdb-updated' WHERE id = 51");
        var table = await catalog.LoadTableAsync("main", "t");
        var ours = await catalog.ReadAsync(table);
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.t"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(44, ours.Count);
        var rowIds = new List<long>();
        await foreach (var r in catalog.ReadRowsAsync(table)) rowIds.Add(r.RowId);
        Assert.Equal(rowIds.Count, rowIds.Distinct().Count());
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_metadata")]
    public async Task DuckDB_attaches_a_catalog_created_here_and_this_library_opens_one_DuckDB_created()
    {
        Oracle.Require();
        await _lake.CreateAsync();
        var schemas = await Oracle.RowsAsync(_lake, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'lake'");
        Assert.Contains("[\"main\"]", schemas);

        using var other = new TestLake(_lake.Backend, _lake.Version);
        await Oracle.RunAsync(other, "CREATE TABLE lake.x (a INT)");
        var catalog = await other.OpenAsync();
        Assert.Equal(["main"], await catalog.ListSchemasAsync());
        Assert.Equal(["x"], await catalog.ListTablesAsync("main"));
    }

    [SkippableFact]
    [Spec("ducklake: Deletion Inlining")]
    [Spec("ducklake: Insertion Inlining")]
    [Spec("ducklake: Time Travel and Deletions")]
    [Spec("ducklake: Compaction")]
    public async Task Time_travel_agrees_with_DuckDB_across_its_inlining_flush_and_merge()
    {
        Oracle.Require();
        await Oracle.RunAsync(_lake,
            "CALL lake.set_option('data_inlining_row_limit', 10)",
            "CREATE TABLE lake.t (a INT, s VARCHAR)",
            "INSERT INTO lake.t SELECT i, 's' || i FROM range(100) r(i)",        // Parquet
            "DELETE FROM lake.t WHERE a < 2",                                   // inlined deletion
            "DELETE FROM lake.t WHERE a = 50",                                  // inlined deletion
            "INSERT INTO lake.t VALUES (1000, 'x'), (1001, 'y')",               // inlined insert
            "DELETE FROM lake.t WHERE a = 1000",                                // end_snapshot on an inlined row
            "UPDATE lake.t SET s = 'upd' WHERE a = 60",
            "CALL ducklake_flush_inlined_data('lake')",                         // partial delete and data files
            "INSERT INTO lake.t SELECT i, 'more' FROM range(200, 220) r(i)",
            "INSERT INTO lake.t SELECT i, 'more' FROM range(300, 320) r(i)",
            "CALL ducklake_merge_adjacent_files('lake')");                     // merged file with per-row snapshots
        // The state under test exists: partial data and delete files, and inlined deletions.
        var partials = (await _lake.SqlAsync("SELECT (SELECT count(*) FROM ducklake_data_file WHERE partial_max IS NOT NULL), " +
                                              "(SELECT count(*) FROM ducklake_delete_file WHERE partial_max IS NOT NULL)"))[0];
        var inlinedDeletes = await _lake.TableCountAsync("ducklake_inlined_delete_");
        Assert.True((long)partials[0]! > 0 && (long)partials[1]! > 0 && inlinedDeletes == 1,
            $"partial data {partials[0]}, partial delete {partials[1]}, inlined delete tables {inlinedDeletes}");
        var catalog = await _lake.OpenAsync();
        var compared = 0;
        foreach (var snapshot in await catalog.ListSnapshotsAsync())
        {
            if (snapshot.SnapshotId < 2) continue;
            compared++;
            var table = await catalog.LoadTableAsync("main", "t", snapshot.SnapshotId);
            LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, $"SELECT * FROM lake.t AT (VERSION => {snapshot.SnapshotId})"),
                LakeJson.Rows(table.Columns, await catalog.ReadAsync(table)));
        }
        Assert.True(compared >= 10, $"{compared} snapshots compared");
        // And this library's deletes on top of DuckDB's partial and inlined state agree with DuckDB afterwards.
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Or(Expr.LessThan("a", 5), Expr.GreaterThan("a", 310)));
            await tx.CommitAsync();
        }
        var now = await catalog.LoadTableAsync("main", "t");
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.t"), LakeJson.Rows(now.Columns, await catalog.ReadAsync(now)));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_partition_column")]
    [Spec("ducklake: SELECT with File Pruning")]
    public async Task This_library_reads_and_prunes_DuckDB_partitioned_tables()
    {
        Oracle.Require();
        await Oracle.RunAsync(_lake,
            "CALL lake.set_option('data_inlining_row_limit', 0)",
            "CREATE TABLE lake.p (id INT, region VARCHAR, ts TIMESTAMP)",
            "ALTER TABLE lake.p SET PARTITIONED BY (region, year(ts), month(ts))",
            "INSERT INTO lake.p SELECT i, ['eu', 'us', NULL][i % 3 + 1], TIMESTAMP '2023-11-01' + INTERVAL (i) DAY FROM range(120) r(i)");
        var catalog = await _lake.OpenAsync();
        var table = await catalog.LoadTableAsync("main", "p");
        Assert.Equal(["identity", "year", "month"], table.PartitionKeys.Select(k => k.Transform));
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.p"), LakeJson.Rows(table.Columns, await catalog.ReadAsync(table)));
        var filter = Expr.Equal("region", "eu");
        var plan = await catalog.PlanScanAsync(table, filter);
        Assert.True(plan.PrunedFiles > plan.Files.Count, $"{plan.PrunedFiles} pruned, {plan.Files.Count} read");
        LakeJson.AssertSameRows(await Oracle.RowsAsync(_lake, "SELECT * FROM lake.p WHERE region = 'eu'"),
            LakeJson.Rows(table.Columns, await catalog.ReadAsync(table, filter)));
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    [Spec("ducklake: Data")]
    public async Task A_commit_retries_over_DuckDB_inserts_and_refuses_over_DuckDB_deletes()
    {
        Oracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64)]);
            await tx.InsertAsync("main", "t", [[1L], [2L]]);
            await tx.CommitAsync();
        }
        var retried = catalog.BeginTransaction();
        await retried.InsertAsync("main", "t", [[10L]]);
        await Oracle.RunAsync(_lake, "INSERT INTO lake.t VALUES (20)");
        var snapshot = await retried.CommitAsync();
        Assert.Equal(3, snapshot.SnapshotId);

        var refused = catalog.BeginTransaction();
        await refused.InsertAsync("main", "t", [[30L]]);
        await Oracle.RunAsync(_lake, "DELETE FROM lake.t WHERE id = 1");
        await Assert.ThrowsAsync<DuckLakeConflictException>(() => refused.CommitAsync());
        Assert.Equal(["[10]", "[20]", "[2]"], await Oracle.RowsAsync(_lake, "SELECT id FROM lake.t"));
    }
}

public sealed class SqliteCrossEngineTests10() : CrossEngineTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresCrossEngineTests10() : CrossEngineTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteCrossEngineTests11() : CrossEngineTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresCrossEngineTests11() : CrossEngineTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
