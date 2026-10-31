using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary><see cref="DuckLakeValidator"/> finds each kind of damage, and nothing on a healthy lake.</summary>
public abstract class ValidatorTests : IDisposable
{
    private readonly TestLake _lake;

    protected ValidatorTests(CatalogBackend backend) => _lake = new TestLake(backend) { Corrupted = true };

    public void Dispose() => _lake.Dispose();

    private async Task<DuckLakeCatalog> HealthyAsync()
    {
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64)]);
            await tx.InsertAsync("main", "t", Enumerable.Range(0, 10).Select(i => new object?[] { (long)i }));
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[100L]]);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Equal("id", 3L));
            await tx.CommitAsync();
        }
        Assert.Empty(await DuckLakeValidator.ValidateAsync(catalog, new DuckLakeValidationOptions { Deep = true }));
        return catalog;
    }

    private async Task<string> ProblemsAfterAsync(DuckLakeCatalog catalog, string sql, bool deep = false)
    {
        await _lake.ExecAsync(sql);
        var problems = await DuckLakeValidator.ValidateAsync(catalog, new DuckLakeValidationOptions { Deep = deep });
        Assert.NotEmpty(problems);
        return string.Join("\n", problems);
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_snapshot")]
    public async Task Counters_that_do_not_cover_the_ids_in_use() =>
        Assert.Contains("next_file_id", await ProblemsAfterAsync(await HealthyAsync(), "UPDATE ducklake_snapshot SET next_file_id = 1 WHERE snapshot_id = (SELECT max(snapshot_id) FROM ducklake_snapshot)"));

    [SkippableFact]
    [Spec("ducklake: ducklake_snapshot")]
    public async Task A_row_no_retained_snapshot_can_see()
    {
        // Snapshot 1 removed by hand, without what expiry does: data file 0, ended at 2, is now visible nowhere.
        var catalog = await HealthyAsync();
        await _lake.ExecAsync("UPDATE ducklake_data_file SET end_snapshot = 2 WHERE data_file_id = 0");
        Assert.Contains("visible in no retained snapshot", await ProblemsAfterAsync(catalog, "DELETE FROM ducklake_snapshot WHERE snapshot_id = 1"));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task Two_delete_files_for_one_data_file_in_a_snapshot() =>
        Assert.Contains("has 2 delete files", await ProblemsAfterAsync(await HealthyAsync(),
            "INSERT INTO ducklake_delete_file (delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, format, delete_count) " +
            "SELECT 77, table_id, begin_snapshot, NULL, data_file_id, path, format, delete_count FROM ducklake_delete_file"));

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    public async Task Overlapping_row_ids() =>
        Assert.Contains("overlap", await ProblemsAfterAsync(await HealthyAsync(), "UPDATE ducklake_data_file SET row_id_start = 5 WHERE data_file_id = 1"));

    [SkippableFact]
    [Spec("ducklake: ducklake_data_file")]
    public async Task A_missing_file_or_a_wrong_size_or_row_count()
    {
        var catalog = await HealthyAsync();
        Assert.Contains("bytes, recorded as", await ProblemsAfterAsync(catalog, "UPDATE ducklake_data_file SET file_size_bytes = file_size_bytes + 1 WHERE data_file_id = 1"));
        await _lake.ExecAsync("UPDATE ducklake_data_file SET file_size_bytes = file_size_bytes - 1 WHERE data_file_id = 1");
        Assert.Contains("rows, recorded as", await ProblemsAfterAsync(catalog, "UPDATE ducklake_data_file SET record_count = 9 WHERE data_file_id = 1", deep: true));
        await _lake.ExecAsync("UPDATE ducklake_data_file SET record_count = 1 WHERE data_file_id = 1");
        foreach (var f in _lake.Files()) File.Delete(f);
        Assert.Contains("does not exist", string.Join("\n", await DuckLakeValidator.ValidateAsync(catalog)));
    }

    [SkippableFact]
    [Spec("ducklake: ducklake_delete_file")]
    public async Task A_delete_file_outliving_its_data_file() =>
        Assert.Contains("outside the life", await ProblemsAfterAsync(await HealthyAsync(), "UPDATE ducklake_data_file SET end_snapshot = 3 WHERE data_file_id = 0"));
}

public sealed class SqliteValidatorTests() : ValidatorTests(CatalogBackend.Sqlite);
public sealed class PostgresValidatorTests() : ValidatorTests(CatalogBackend.Postgres);
