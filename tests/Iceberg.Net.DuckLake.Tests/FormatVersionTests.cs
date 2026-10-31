using System.Text.RegularExpressions;
using Iceberg.Net.Data;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>The schema creation scripts of both format versions, as spec vectors.</summary>
public sealed class SpecSchemaTests
{
    private static Dictionary<string, List<(string Name, string Type)>> Parse(IEnumerable<string> statements) =>
        statements.Select(s => Regex.Match(s, @"CREATE TABLE (?:\{METADATA_CATALOG\}\.)?(\w+)\s*\((.*)\);?\s*$", RegexOptions.Singleline))
            .Where(m => m.Success)
            .ToDictionary(m => m.Groups[1].Value, m => m.Groups[2].Value.Split(',').Select(c =>
            {
                var parts = c.Trim().Split(' ', 2);
                return (parts[0].Trim('"'), parts[1].Replace(" PRIMARY KEY", "").Replace(" NOT NULL", "").Trim());
            }).ToList());

    private static Dictionary<string, List<(string Name, string Type)>> SpecScript(string version) =>
        Parse(File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "spec-vectors", $"schema-{version}.sql")).Where(l => l.StartsWith("CREATE TABLE")));

    [Theory]
    [Spec("ducklake: Full Schema Creation Script")]
    [InlineData("1.0")]
    [InlineData("1.1")]
    public void Every_dialect_creates_the_specs_tables_and_columns(string version)
    {
        var spec = SpecScript(version);
        Assert.Equal(version == "1.0" ? 28 : 29, spec.Count);
        foreach (var dialect in new[] { DuckLakeDialect.Sqlite, DuckLakeDialect.PostgreSql })
        {
            var ours = Parse(dialect.Ddl(version));
            Assert.Equal(spec.Keys.Order(), ours.Keys.Order());
            foreach (var (table, columns) in spec)
            {
                Assert.Equal(columns.Select(c => c.Name), ours[table].Select(c => c.Name));
                // PostgreSQL has every type the spec names; SQLite stores them the way DuckDB's SQLite scanner does.
                var expected = columns.Select(c => dialect == DuckLakeDialect.PostgreSql ? c.Type : c.Type switch
                {
                    "BOOLEAN" => "BIGINT",
                    "UUID" or "TIMESTAMP WITH TIME ZONE" => "VARCHAR",
                    var t => t,
                });
                Assert.Equal(expected, ours[table].Select(c => c.Type));
            }
        }
    }
}

/// <summary>What changed in 1.1, and that a 1.0 catalog refuses it; on both catalog databases.</summary>
public abstract class FormatVersionTests : IDisposable
{
    private readonly CatalogBackend _backend;
    private readonly List<TestLake> _lakes = [];

    protected FormatVersionTests(CatalogBackend backend) => _backend = backend;

    public void Dispose()
    {
        foreach (var l in _lakes) l.Dispose();
    }

    private TestLake Lake(string version = DuckLakeCatalog.FormatVersion11)
    {
        var lake = new TestLake(_backend, version);
        _lakes.Add(lake);
        return lake;
    }

    private bool Sqlite => _backend == CatalogBackend.Sqlite;

    [SkippableFact]
    [Spec("ducklake: ducklake_schema")]
    [Spec("ducklake: ducklake_snapshot_changes")]
    [Spec("ducklake: List Schemas")]
    public async Task Nested_schemas_have_a_parent_a_path_of_names_and_a_uuid_directory()
    {
        var lake = Lake();
        var catalog = await lake.CreateAsync();
        DuckLakeSnapshot created;
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateSchemaAsync("sales");
            await tx.CreateSchemaAsync("sales.europe");
            await tx.CreateTableAsync("sales.europe", "orders", [new("id", DuckLakeType.Int64)]);
            await tx.InsertAsync("sales.europe", "orders", [[1L], [2L]]);
            created = await tx.CommitAsync();
        }
        Assert.Equal("created_schema:\"sales\",created_schema:\"sales\".\"europe\",created_table:\"sales\".\"europe\".\"orders\",inserted_into_table:3",
            string.Join(",", created.Changes));
        Assert.Contains("sales.europe", await catalog.ListSchemasAsync());
        Assert.Equal(["orders"], await catalog.ListTablesAsync("sales.europe"));
        var rows = await lake.SqlAsync("SELECT schema_id, schema_name, parent_schema_id, path FROM ducklake_schema WHERE schema_id > 0 ORDER BY schema_id");
        Assert.Equal([1L, "sales", null, "sales/"], rows[0]);
        Assert.Equal([2L, "europe", 1L], rows[1][..3]);
        // A nested schema's relative path is relative to data_path (not its parent's) and named after its UUID.
        var uuid = (string)(await lake.SqlAsync("SELECT schema_uuid FROM ducklake_schema WHERE schema_id = 2"))[0][0]!;
        Assert.Equal(uuid + "/", rows[1][3]);
        var table = await catalog.LoadTableAsync("sales.europe", "orders");
        Assert.Equal(lake.DataPath + uuid + "/orders/", table.Location);
        Assert.Equal(2, (await catalog.ReadAsync(table)).Count);
        await using (var tx = catalog.BeginTransaction())
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.DropSchemaAsync("sales")); // not empty: it has a child
        }
    }

    [SkippableFact]
    [Spec("ducklake: Snapshot Creation")]
    public async Task A_1_0_catalog_refuses_what_needs_1_1()
    {
        var catalog = await Lake(DuckLakeCatalog.FormatVersion10).CreateAsync();
        await using var tx = catalog.BeginTransaction();
        await tx.CreateSchemaAsync("sales");
        await Assert.ThrowsAsync<NotSupportedException>(() => tx.CreateSchemaAsync("sales.europe"));
        await Assert.ThrowsAsync<NotSupportedException>(() => tx.CreateTableAsync("main", "t", [new("x", DuckLakeType.Null)]));
        await tx.CreateTableAsync("main", "t", [new("ts", DuckLakeType.Timestamp)]);
        await Assert.ThrowsAsync<NotSupportedException>(() => tx.SetPartitioningAsync("main", "t", [("ts", "epoch_month")]));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    [Spec("ducklake: ducklake_delete_file")]
    [Spec("ducklake: ducklake_file_column_stats")]
    [Spec("ducklake: ducklake_table_column_stats")]
    public async Task Row_group_counts_and_exact_bounds_are_recorded_in_1_1_only()
    {
        foreach (var version in new[] { DuckLakeCatalog.FormatVersion10, DuckLakeCatalog.FormatVersion11 })
        {
            var lake = Lake(version);
            var catalog = await lake.CreateAsync(new DuckLakeOptions { Parquet = new ParquetWriteOptions { RowGroupRows = 2 } });
            await using (var tx = catalog.BeginTransaction())
            {
                await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("none", DuckLakeType.Varchar)]);
                await tx.InsertAsync("main", "t", Enumerable.Range(0, 5).Select(i => new object?[] { (long)i, null }));
                await tx.CommitAsync();
            }
            await using (var tx = catalog.BeginTransaction())
            {
                await tx.DeleteAsync("main", "t", Expr.Equal("id", 3L));
                await tx.CommitAsync();
            }
            var plan = await catalog.PlanScanAsync(await catalog.LoadTableAsync("main", "t"));
            if (version == DuckLakeCatalog.FormatVersion10)
            {
                Assert.Null(plan.Files[0].RowGroupCount);
                continue;
            }
            Assert.Equal(3, plan.Files[0].RowGroupCount);
            Assert.Equal([[1L]], await lake.SqlAsync("SELECT row_group_count FROM ducklake_delete_file"));
            // Bounds computed from the rows are exact; a column with no bounds has unknown exactness (NULL).
            Assert.Equal([[1L, "0", "4", 1L, 1L], [2L, null, null, null, null]],
                await lake.SqlAsync("SELECT column_id, min_value, max_value, min_is_exact, max_is_exact FROM ducklake_file_column_stats ORDER BY column_id"));
            Assert.Equal([[1L, 1L, 1L], [2L, null, null]],
                await lake.SqlAsync("SELECT column_id, min_is_exact, max_is_exact FROM ducklake_table_column_stats ORDER BY column_id"));
        }
    }

    [SkippableFact]
    [Spec("ducklake: Primitive Types")]
    [Spec("ducklake: ducklake_column")]
    public async Task A_null_column_holds_nulls_until_it_is_given_a_type()
    {
        var lake = Lake();
        var catalog = await lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "signups", [new("id", DuckLakeType.Int32), new("referrer", DuckLakeType.Null)]);
            await tx.InsertAsync("main", "signups", [[1, null], [2, null]]);
            await Assert.ThrowsAsync<ArgumentException>(() => tx.InsertAsync("main", "signups", [[3, "x"]]));
            await tx.CommitAsync();
        }
        Assert.Equal([["null"]], await lake.SqlAsync("SELECT column_type FROM ducklake_column WHERE column_name = 'referrer'"));
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.AlterColumnTypeAsync("main", "signups", "referrer", DuckLakeType.Varchar);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "signups", [[3, "newsletter"]]);
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "signups");
        Assert.Equal(DuckLakeType.Varchar, table.Column("referrer").Type);
        Assert.Equal([null, null, "newsletter"], (await catalog.ReadAsync(table)).OrderBy(r => (int)r[0]!).Select(r => r[1]));
        var before = await catalog.LoadTableAsync("main", "signups", 1);
        Assert.Equal(DuckLakeType.Null, before.Column("referrer").Type);
        Assert.All(await catalog.ReadAsync(before), r => Assert.Null(r[1]));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_column")]
    public async Task Columns_widen_without_rewriting_files_and_never_narrow()
    {
        var catalog = await Lake().CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("n", DuckLakeType.Int16), new("f", DuckLakeType.Float32)]);
            await tx.InsertAsync("main", "t", [[(short)7, 1.5f]]);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.AlterColumnTypeAsync("main", "t", "n", DuckLakeType.Int64);
            await tx.AlterColumnTypeAsync("main", "t", "f", DuckLakeType.Float64);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[1L << 40, 2.25d]]);
            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.AlterColumnTypeAsync("main", "t", "n", DuckLakeType.Int32));
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[1L << 40, 2.25d]]);
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "t");
        var rows = (await catalog.ReadAsync(table)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal([7L, 1L << 40], rows.Select(r => r[0]));
        Assert.Equal([1.5d, 2.25d], rows.Select(r => r[1]));
        // Old files' statistics are read with the new type: pruning still works across the promotion.
        Assert.Single((await catalog.PlanScanAsync(table, Expr.GreaterThan("n", 100L))).Files);
        await using (var tx = catalog.BeginTransaction())
            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.AlterColumnTypeAsync("main", "t", "f", DuckLakeType.Varchar));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_partition_column")]
    [Spec("ducklake: ducklake_file_partition_value")]
    public async Task Epoch_and_bucket_transforms_store_icebergs_values()
    {
        var catalog = await Lake().CreateAsync();
        var ts = new DateTimeOffset(2024, 3, 15, 10, 0, 0, TimeSpan.Zero);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "clicks", [new("user_id", DuckLakeType.Int32), new("ts", DuckLakeType.TimestampTz)]);
            await tx.SetPartitioningAsync("main", "clicks", [("ts", "epoch_month"), ("user_id", "bucket(8)")]);
            await tx.InsertAsync("main", "clicks", [[42, ts]]);
            await tx.CommitAsync();
        }
        var lake = _lakes[^1];
        var values = (await lake.SqlAsync("SELECT partition_value FROM ducklake_file_partition_value ORDER BY partition_key_index")).Select(r => (string)r[0]!).ToList();
        var micros = IcebergTime.MicrosFromTimestamp(ts);
        Assert.Equal(Transform.Month.Apply(PrimitiveType.TimestampTz, micros)!.ToString(), values[0]); // months since 1970-01: 650
        Assert.Equal("650", values[0]);
        Assert.Equal(Transform.Bucket(8).Apply(PrimitiveType.Int, 42)!.ToString(), values[1]);
        Assert.Equal([["epoch_month"], ["bucket(8)"]], await lake.SqlAsync("SELECT \"transform\" FROM ducklake_partition_column ORDER BY partition_key_index"));
        Assert.Single(await catalog.ReadAsync(await catalog.LoadTableAsync("main", "clicks")));
        Assert.Throws<ArgumentException>(() => DuckLakePartitioning.Validate(DuckLakeType.Float64, "bucket(4)"));
        Assert.Throws<FormatException>(() => DuckLakePartitioning.Validate(DuckLakeType.Int32, "bucket(x)"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_inlined_data_tables")]
    public async Task Column_names_starting_with_ducklake_are_reserved()
    {
        var catalog = await Lake().CreateAsync();
        await using var tx = catalog.BeginTransaction();
        await Assert.ThrowsAsync<ArgumentException>(() => tx.CreateTableAsync("main", "t", [new("_DuckLake_row_id", DuckLakeType.Int64)]));
        await tx.CreateTableAsync("main", "t", [new("row_id", DuckLakeType.Int64), new("begin_snapshot", DuckLakeType.Int64)]); // fine in 1.1
    }

    /// <summary>
    /// Inlined rows as DuckDB lays them out in each database and version (the 1.1 rename of the system columns; text in
    /// SQLite; BYTEA for VARCHAR and native NUMERIC/BOOLEAN in PostgreSQL), read and deleted through this library.
    /// </summary>
    [SkippableTheory]
    [Spec("ducklake: Type Encoding for Data Inlining")]
    [Spec("ducklake: Insertion Inlining")]
    [Spec("ducklake: Read Inlined Data")]
    [InlineData("1.0")]
    [InlineData("1.1")]
    public async Task Inlined_rows_are_read_and_deleted_in_each_versions_layout(string version)
    {
        var lake = Lake(version);
        var catalog = await lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [
                new("id", DuckLakeType.Int64), new("name", DuckLakeType.Varchar), new("amount", DuckLakeType.Decimal(9, 2)),
                new("flag", DuckLakeType.Boolean), new("tags", DuckLakeType.List(DuckLakeType.Varchar)),
            ]);
            await tx.InsertAsync("main", "t", [[1L, "in a file", 1.5m, true, new[] { "f" }]]);
            await tx.CommitAsync();
        }
        var (rowId, begin, end) = DuckLakeInlinedData.SystemColumns(version == "1.1");
        var (text, number, boolean) = Sqlite ? ("VARCHAR", "VARCHAR", "BIGINT") : ("BYTEA", "NUMERIC(9,2)", "BOOLEAN");
        await lake.ExecAsync($"CREATE TABLE ducklake_inlined_data_1_1 ({rowId} BIGINT, {begin} BIGINT, {end} BIGINT, id BIGINT, name {text}, amount {number}, flag {boolean}, tags VARCHAR)");
        await lake.ExecAsync("INSERT INTO ducklake_inlined_data_tables (table_id, table_name, schema_version) VALUES (1, 'ducklake_inlined_data_1_1', 1)");
        string Text(string s) => Sqlite ? $"'{s.Replace("'", "''")}'" : $"'\\x{Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(s))}'::bytea";
        string Bool(bool b) => Sqlite ? (b ? "1" : "0") : (b ? "TRUE" : "FALSE");
        await lake.ExecAsync($"INSERT INTO ducklake_inlined_data_1_1 VALUES (1, 1, NULL, 2, {Text("it's inlined")}, '12.50', {Bool(false)}, '[''a, b'', NULL]')," +
                             $" (2, 1, NULL, 3, NULL, NULL, NULL, NULL)");
        var table = await catalog.LoadTableAsync("main", "t");
        var rows = new List<DuckLakeRow>();
        await foreach (var r in catalog.ReadRowsAsync(table)) rows.Add(r);
        var inlined = rows.Single(r => (long)r.Values[0]! == 2);
        Assert.Equal(1, inlined.RowId);
        Assert.Equal(["it's inlined", new IcebergDecimal(1250, 2), false, new List<object?> { "a, b", null }], inlined.Values[1..]);
        await using (var tx = catalog.BeginTransaction())
        {
            Assert.Equal(1, await tx.DeleteAsync("main", "t", Expr.Equal("id", 2L)));
            await tx.CommitAsync();
        }
        Assert.Equal([[1L, 2L], [2L, null]], await lake.SqlAsync($"SELECT {rowId}, {end} FROM ducklake_inlined_data_1_1 ORDER BY {rowId}"));
        Assert.Equal([1L, 3L], (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Select(r => (long)r[0]!).Order());
    }
}

public sealed class SqliteFormatVersionTests() : FormatVersionTests(CatalogBackend.Sqlite);
public sealed class PostgresFormatVersionTests() : FormatVersionTests(CatalogBackend.Postgres);
