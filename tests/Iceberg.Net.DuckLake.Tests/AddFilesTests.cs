using Iceberg.Net.Data;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;
using ParquetSharp;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Registering existing Parquet files (DuckDB's <c>ducklake_add_data_files</c>): a <c>map_by_name</c> column mapping,
/// footer statistics, absolute paths, as DuckDB 1.5.5 writes them. On every database and version.
/// </summary>
public abstract class AddFilesTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected AddFilesTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private string External(string name)
    {
        var path = Path.Combine(_lake.Root, "external", name);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        return path;
    }

    /// <summary>A file as other tools write it: plain columns, no field IDs.</summary>
    private string Plain(string name, long[] ids, string?[]? names = null, int[]? ints = null, bool namesFirst = false)
    {
        var path = External(name);
        var columns = new List<Column>();
        if (namesFirst && names is not null) columns.Add(new Column<string?>("name"));
        columns.Add(ints is null ? new Column<long>("id") : new Column<int>("id"));
        if (!namesFirst && names is not null) columns.Add(new Column<string?>("name"));
        using var writer = new ParquetFileWriter(path, columns.ToArray());
        using var group = writer.AppendRowGroup();
        void Names() { using var w = group.NextColumn().LogicalWriter<string?>(); w.WriteBatch(names!); }
        if (namesFirst && names is not null) Names();
        if (ints is null) { using var w = group.NextColumn().LogicalWriter<long>(); w.WriteBatch(ids); }
        else { using var w = group.NextColumn().LogicalWriter<int>(); w.WriteBatch(ints); }
        if (!namesFirst && names is not null) Names();
        writer.Close();
        return path;
    }

    private async Task<DuckLakeCatalog> LakeAsync(DuckLakeOptions? options = null)
    {
        var catalog = await _lake.CreateAsync(options);
        await using var tx = catalog.BeginTransaction();
        await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("name", DuckLakeType.Varchar)]);
        await tx.CommitAsync();
        return catalog;
    }

    private static async Task<long> AddAsync(DuckLakeCatalog catalog, string[] files, DuckLakeAddFileOptions? options = null)
    {
        await using var tx = catalog.BeginTransaction();
        var added = await tx.AddDataFilesAsync("main", "t", files, options);
        await tx.CommitAsync();
        return added;
    }

    private async Task<List<string>> RowsAsync(DuckLakeCatalog catalog, Expression? filter = null)
    {
        var table = await catalog.LoadTableAsync("main", "t");
        return (await catalog.ReadAsync(table, filter)).Select(r => $"{r[0]}:{r[1] ?? "null"}").Order(StringComparer.Ordinal).ToList();
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_file_column_stats")]
    public async Task Unknown_footer_null_counts_and_missing_defaults_do_not_prune_matching_rows()
    {
        var catalog = await LakeAsync();
        await _lake.ExecAsync("UPDATE ducklake_column SET initial_default = 'fallback' WHERE column_name = 'name'");
        var missing = Plain("missing.parquet", [1]);
        var unknown = External("unknown.parquet");
        await ParquetFiles.WriteAsync(_lake.IO, unknown,
            new Schema(0, [NestedField.Optional(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)]),
            0, [], [[2L, null], [3L, "present"]], new ParquetWriteOptions { Statistics = false });
        await AddAsync(catalog, [missing, unknown], new DuckLakeAddFileOptions { AllowMissing = true });
        Assert.Equal(["1:fallback", "2:null", "3:present"], await RowsAsync(catalog));
        Assert.Equal(["1:fallback"], await RowsAsync(catalog, Expr.Equal("name", "fallback")));
        Assert.Equal(["2:null"], await RowsAsync(catalog, Expr.IsNull("name")));
        Assert.Equal(["1:fallback", "3:present"], await RowsAsync(catalog, Expr.NotNull("name")));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_files_scheduled_for_deletion")]
    public async Task Registered_files_transfer_ownership_and_are_cleaned_up_after_expiry()
    {
        // Files outside data_path are deleted only when the options say so (the catalog is not trusted to name them).
        var catalog = await LakeAsync(new DuckLakeOptions { CleanupOutsideDataPath = true });
        var external = Plain("owned.parquet", [1, 2], ["a", "b"]);
        await AddAsync(catalog, [external]);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Equal("id", 1L));
            await tx.CommitAsync();
        }
        await catalog.RewriteDataFilesAsync(deleteThreshold: 0.1);
        Assert.True(File.Exists(external)); // older snapshots still own the original
        await catalog.ExpireSnapshotsAsync(DateTimeOffset.UtcNow.AddMinutes(1));
        var cautious = await _lake.OpenAsync();
        Assert.DoesNotContain(external, await cautious.CleanupOldFilesAsync(dryRun: true));
        Assert.DoesNotContain(external, await cautious.CleanupOldFilesAsync()); // the lake's own files go
        Assert.True(File.Exists(external));
        Assert.Equal([external], await catalog.CleanupOldFilesAsync());
        Assert.False(File.Exists(external));
        Assert.Equal(["2:b"], await RowsAsync(catalog));
    }

    [Fact]
    [Spec("ducklake: ducklake_column_mapping")]
    [Spec("ducklake: ducklake_name_mapping")]
    [Spec("ducklake: ducklake_data_file")]
    public async Task A_file_without_field_ids_is_registered_under_a_name_mapping_and_read_in_place()
    {
        var catalog = await LakeAsync();
        var file = Plain("a.parquet", [1, 2, 3], ["a", null, "c"]);
        Assert.Equal(3, await AddAsync(catalog, [file]));
        Assert.Equal(["1:a", "2:null", "3:c"], await RowsAsync(catalog));
        Assert.Empty(_lake.Files()); // nothing copied into the data path

        // As DuckDB: the mapping takes the next file ID, then the file; the path is absolute; inserted_into_table.
        var data = (await _lake.SqlAsync("SELECT data_file_id, mapping_id, path, path_is_relative, record_count, row_id_start FROM ducklake_data_file"))[0];
        Assert.Equal([1L, 0L, file, 0L, 3L, 0L], data.Select(v => v is bool or int or long or short ? (object)Convert.ToInt64(v) : v));
        Assert.Equal([0L, 1L, "map_by_name"], (await _lake.SqlAsync("SELECT mapping_id, table_id, type FROM ducklake_column_mapping"))[0].Select(v => v is string ? v : (object)Convert.ToInt64(v)));
        var names = await _lake.SqlAsync("SELECT column_id, source_name, target_field_id, parent_column, is_partition FROM ducklake_name_mapping ORDER BY column_id");
        Assert.Equal(["0 id 1  0", "1 name 2  0"], names.Select(r => string.Join(" ", r.Select(v => v is bool or long or int or short ? Convert.ToInt64(v).ToString() : v))));
        Assert.Equal(2L, Convert.ToInt64((await _lake.SqlAsync("SELECT next_file_id FROM ducklake_snapshot ORDER BY snapshot_id DESC"))[0][0]));
        Assert.Equal(["inserted_into_table:1"], (await catalog.CurrentSnapshotAsync()).Changes.Select(c => c.ToString()));

        // Footer statistics, as text: the file is pruned by them.
        var stats = await _lake.SqlAsync("SELECT column_id, value_count, null_count, min_value, max_value FROM ducklake_file_column_stats ORDER BY column_id");
        Assert.Equal(["1 3 0 1 3", "2 2 1 a c"], stats.Select(r => string.Join(" ", r)));
        Assert.Empty((await catalog.PlanScanAsync(await catalog.LoadTableAsync("main", "t"), Expr.GreaterThan("id", 3L))).Files);
    }

    [Fact]
    public async Task Columns_are_matched_by_name_whatever_their_order_or_field_ids()
    {
        var catalog = await LakeAsync();
        var reordered = Plain("b.parquet", [5], ["e"], namesFirst: true);
        // A file whose field IDs mean something else: id has field ID 2 and name field ID 1.
        var misleading = External("c.parquet");
        await ParquetFiles.WriteAsync(_lake.IO, misleading, new Schema(0, [NestedField.Optional(2, "id", PrimitiveType.Long), NestedField.Optional(1, "name", PrimitiveType.String)]),
            0, [], [[6L, "f"]], ParquetWriteOptions.Default);
        var narrow = Plain("d.parquet", [7], ints: [7]); // INT32 read into an int64 column, as DuckDB allows
        await AddAsync(catalog, [reordered, misleading], new DuckLakeAddFileOptions());
        await AddAsync(catalog, [narrow], new DuckLakeAddFileOptions { AllowMissing = true });
        Assert.Equal(["5:e", "6:f", "7:null"], await RowsAsync(catalog));
        // The two complete files have different field orders, so different mappings; reordered = name first.
        var mappings = await _lake.SqlAsync("SELECT mapping_id, column_id, source_name FROM ducklake_name_mapping ORDER BY mapping_id, column_id");
        Assert.Equal(3, mappings.Select(r => r[0]).Distinct().Count());
        Assert.Equal("name", mappings[0][2]);
    }

    [Fact]
    public async Task Files_with_one_layout_share_one_mapping_within_a_commit()
    {
        var catalog = await LakeAsync();
        await AddAsync(catalog, [Plain("a.parquet", [1], ["a"]), Plain("b.parquet", [2], ["b"])]);
        Assert.Single(await _lake.SqlAsync("SELECT mapping_id FROM ducklake_column_mapping"));
        Assert.Equal([0L, 0L], (await _lake.SqlAsync("SELECT mapping_id FROM ducklake_data_file ORDER BY data_file_id")).Select(r => Convert.ToInt64(r[0])));
        Assert.Equal([1L, 2L], (await _lake.SqlAsync("SELECT data_file_id FROM ducklake_data_file ORDER BY data_file_id")).Select(r => Convert.ToInt64(r[0])));
    }

    [Fact]
    public async Task Missing_extra_and_unreadable_columns_are_refused_unless_allowed()
    {
        var catalog = await LakeAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            var e = await Assert.ThrowsAsync<ArgumentException>(() => tx.AddDataFilesAsync("main", "t", [Plain("a.parquet", [1])]));
            Assert.Contains("\"name\" exists in table", e.Message);
        }
        var extra = External("extra.parquet");
        await ParquetFiles.WriteAsync(_lake.IO, extra, new Schema(0, [NestedField.Optional(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String),
            NestedField.Optional(3, "z", PrimitiveType.Int)]), 0, [], [[1L, "a", 9]], ParquetWriteOptions.Default);
        await using (var tx = catalog.BeginTransaction())
        {
            var e = await Assert.ThrowsAsync<ArgumentException>(() => tx.AddDataFilesAsync("main", "t", [extra]));
            Assert.Contains("\"z\" exists in file", e.Message);
        }
        var text = External("text.parquet");
        await ParquetFiles.WriteAsync(_lake.IO, text, new Schema(0, [NestedField.Optional(1, "id", PrimitiveType.String), NestedField.Optional(2, "name", PrimitiveType.String)]),
            0, [], [["x", "a"]], ParquetWriteOptions.Default);
        await using (var tx = catalog.BeginTransaction())
        {
            var e = await Assert.ThrowsAsync<ArgumentException>(() => tx.AddDataFilesAsync("main", "t", [text]));
            Assert.Contains("Failed to map column \"id\"", e.Message);
        }
        await AddAsync(catalog, [extra], new DuckLakeAddFileOptions { IgnoreExtraColumns = true });
        Assert.Equal(["1:a"], await RowsAsync(catalog));
    }

    [Fact]
    public async Task Hive_partition_columns_come_from_the_path()
    {
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("name", DuckLakeType.Varchar), new("year", DuckLakeType.Int32)]);
            await tx.CommitAsync();
        }
        var y2024 = Plain("year=2024/region=eu/a.parquet", [1, 2], ["a", "b"]);
        var none = Plain($"year={DuckLakePartitioning.HiveDefaultPartition}/b.parquet", [3], ["c"]);
        await AddAsync(catalog, [y2024, none], new DuckLakeAddFileOptions { HivePartitioning = true });
        var table = await catalog.LoadTableAsync("main", "t");
        Assert.Equal(["1:a:2024", "2:b:2024", "3:c:null"], (await catalog.ReadAsync(table)).Select(r => $"{r[0]}:{r[1]}:{r[2] ?? "null"}").Order(StringComparer.Ordinal));
        Assert.Equal(["year True"], (await _lake.SqlAsync("SELECT source_name, is_partition FROM ducklake_name_mapping WHERE target_field_id = 3 AND mapping_id = 0"))
            .Select(r => $"{r[0]} {Convert.ToBoolean(r[1])}"));
        Assert.Single(await catalog.ReadAsync(table, Expr.Equal("year", 2024) is var f ? Expr.And(f, Expr.Equal("id", 2L)) : null));
        Assert.Single((await catalog.PlanScanAsync(table, Expr.IsNull("year"))).Files); // pruned by the path value's statistics
    }

    [Fact]
    public async Task Added_rows_take_row_ids_and_can_be_deleted_updated_and_compacted()
    {
        var catalog = await LakeAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[100L, "written"]]);
            await tx.CommitAsync();
        }
        await AddAsync(catalog, [Plain("a.parquet", [1, 2, 3], ["a", "b", "c"])]);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Equal("id", 2L));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.UpdateAsync("main", "t", Expr.Equal("id", 3L), r => [r[0], "C"]);
            await tx.CommitAsync();
        }
        Assert.Equal(["100:written", "1:a", "3:C"], await RowsAsync(catalog));
        var ids = new List<long>();
        await foreach (var r in catalog.ReadRowsAsync(await catalog.LoadTableAsync("main", "t"))) ids.Add(r.RowId);
        Assert.Equal([0L, 1L, 3L], ids.Order());
        await catalog.RewriteDataFilesAsync(deleteThreshold: 0.1);
        Assert.Equal(["100:written", "1:a", "3:C"], await RowsAsync(catalog));
        // The rewrite of the file registered in place is written in the table's directory, under a relative path (not
        // under the registered file's absolute directory, appended to the table's).
        Assert.All(await _lake.SqlAsync("SELECT path FROM ducklake_data_file WHERE path_is_relative = " + (_lake.Backend == CatalogBackend.Sqlite ? "1" : "TRUE")),
            r => Assert.StartsWith("ducklake-", (string)r[0]!));
    }
}

public sealed class SqliteAddFilesTests10() : AddFilesTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteAddFilesTests11() : AddFilesTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresAddFilesTests10() : AddFilesTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresAddFilesTests11() : AddFilesTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
