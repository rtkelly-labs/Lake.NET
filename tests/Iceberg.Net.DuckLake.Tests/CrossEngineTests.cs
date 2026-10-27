using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// DuckDB's <c>ducklake</c> extension reads what this library writes, and this library reads what DuckDB writes, on
/// the same SQLite catalog and data directory.
/// </summary>
public sealed class CrossEngineTests : IDisposable
{
    private readonly TestLake _lake = new();

    public void Dispose() => _lake.Dispose();

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
        DuckLakeOracle.Require();
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
        var theirs = await DuckLakeOracle.RowsAsync(_lake, "SELECT * FROM lake.s.all_types");
        LakeJson.AssertSameRows(theirs, LakeJson.Rows(table.Columns, ours));
    }

    [SkippableFact]
    [Spec("ducklake: DELETE")]
    [Spec("ducklake: UPDATE")]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task DuckDB_reads_deletes_and_updates_and_counts_match()
    {
        DuckLakeOracle.Require();
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
        LakeJson.AssertSameRows(await DuckLakeOracle.RowsAsync(_lake, "SELECT * FROM lake.t"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(["[85]"], await DuckLakeOracle.RowsAsync(_lake, "SELECT count(*) FROM lake.t"));
        Assert.Equal(["[\"updated\"]"], await DuckLakeOracle.RowsAsync(_lake, "SELECT v FROM lake.t WHERE id = 99"));
        // DuckDB's own filtered reads (its pruning uses the statistics written here) agree.
        Assert.Equal(["[40]"], await DuckLakeOracle.RowsAsync(_lake, "SELECT count(*) FROM lake.t WHERE id >= 10 AND id < 55"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_snapshot")]
    public async Task Time_travel_agrees_with_DuckDB_at_every_snapshot()
    {
        DuckLakeOracle.Require();
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
            Assert.Equal(ours, await DuckLakeOracle.RowsAsync(_lake, $"SELECT * FROM lake.t AT (VERSION => {snapshot.SnapshotId})"));
        }
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_column")]
    public async Task Schema_evolution_written_here_reads_the_same_in_DuckDB()
    {
        DuckLakeOracle.Require();
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
        LakeJson.AssertSameRows(await DuckLakeOracle.RowsAsync(_lake, "SELECT * FROM lake.t"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(["[1,\"a\",null]", "[2,\"b\",null]", "[3,\"c\",3.5]"], LakeJson.Rows(table.Columns, ours));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_partition_info")]
    [Spec("ducklake: ducklake_file_partition_value")]
    public async Task Partitioned_writes_read_the_same_in_DuckDB()
    {
        DuckLakeOracle.Require();
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
        LakeJson.AssertSameRows(await DuckLakeOracle.RowsAsync(_lake, "SELECT * FROM lake.events"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(["[5]"], await DuckLakeOracle.RowsAsync(_lake, "SELECT count(*) FROM lake.events WHERE region = 'eu' AND year(day) = 2024"));
    }

    [SkippableFact]
    [Spec("ducklake: SELECT")]
    [Spec("ducklake: Data Inlining")]
    public async Task This_library_reads_what_DuckDB_writes_including_inlined_rows_deletes_and_alters()
    {
        DuckLakeOracle.Require();
        await DuckLakeOracle.RunAsync(_lake,
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
        var theirs = await DuckLakeOracle.RowsAsync(_lake, "SELECT * FROM lake.s.t");
        LakeJson.AssertSameRows(theirs, LakeJson.Rows(table.Columns, ours));

        // Pruned scans return exactly the filtered rows.
        var filtered = await catalog.ReadAsync(table, Expr.And(Expr.GreaterThanOrEqual("id", 100L), Expr.LessThan("id", 120L)));
        LakeJson.AssertSameRows(await DuckLakeOracle.RowsAsync(_lake, "SELECT * FROM lake.s.t WHERE id >= 100 AND id < 120"), LakeJson.Rows(table.Columns, filtered));
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    public async Task Both_engines_write_to_the_same_lake_in_turn()
    {
        DuckLakeOracle.Require();
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("who", DuckLakeType.Varchar)]);
            await tx.InsertAsync("main", "t", Enumerable.Range(0, 50).Select(i => new object?[] { (long)i, "dotnet" }));
            await tx.CommitAsync();
        }
        await DuckLakeOracle.RunAsync(_lake,
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
        await DuckLakeOracle.RunAsync(_lake, "UPDATE lake.t SET who = 'duckdb-updated' WHERE id = 51");
        var table = await catalog.LoadTableAsync("main", "t");
        var ours = await catalog.ReadAsync(table);
        LakeJson.AssertSameRows(await DuckLakeOracle.RowsAsync(_lake, "SELECT * FROM lake.t"), LakeJson.Rows(table.Columns, ours));
        Assert.Equal(44, ours.Count);
        var rowIds = new List<long>();
        await foreach (var r in catalog.ReadRowsAsync(table)) rowIds.Add(r.RowId);
        Assert.Equal(rowIds.Count, rowIds.Distinct().Count());
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_metadata")]
    public async Task DuckDB_attaches_a_catalog_created_here_and_this_library_opens_one_DuckDB_created()
    {
        DuckLakeOracle.Require();
        await _lake.CreateAsync();
        var schemas = await DuckLakeOracle.RowsAsync(_lake, "SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'lake'");
        Assert.Contains("[\"main\"]", schemas);

        using var other = new TestLake();
        await DuckLakeOracle.RunAsync(other, "CREATE TABLE lake.x (a INT)");
        var catalog = await other.OpenAsync();
        Assert.Equal(["main"], await catalog.ListSchemasAsync());
        Assert.Equal(["x"], await catalog.ListTablesAsync("main"));
    }
}
