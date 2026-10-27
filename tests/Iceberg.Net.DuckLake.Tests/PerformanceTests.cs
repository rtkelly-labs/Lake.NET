using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Xunit.Abstractions;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Catalog round trips do not grow with the size of the work: a commit, a scan plan and a delete take the same number
/// of catalog commands for 2 files as for 40, and for 3 columns as for 12. Against a remote PostgreSQL every command is
/// a network round trip (inside the commit, while the snapshot row is contended), so this is the latency budget that
/// matters; wall-clock numbers are in <c>tools/DuckLakeBench</c>.
/// </summary>
public abstract class PerformanceTests(CatalogBackend backend, ITestOutputHelper output) : IDisposable
{
    private readonly TestLake _lake = new(backend);

    public void Dispose() => _lake.Dispose();

    private static DuckLakeColumnDefinition[] Columns(int n) =>
        [new("id", DuckLakeType.Int64), new("part", DuckLakeType.Int32), .. Enumerable.Range(2, n - 2).Select(i => new DuckLakeColumnDefinition($"c{i}", DuckLakeType.Varchar))];

    private static object?[] Row(int id, int part, int columns) =>
        [(long)id, part, .. Enumerable.Range(2, columns - 2).Select(i => (object?)$"v{id}-{i}")];

    /// <summary>Catalog commands of one commit that inserts <paramref name="files"/> files (one per partition) of a table with <paramref name="columns"/> columns.</summary>
    private async Task<(long Stage, long Commit, long Plan, long PlanFiltered, long Delete)> MeasureAsync(DuckLakeCatalog catalog, string table, int files, int columns)
    {
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", table, Columns(columns));
            await tx.SetPartitioningAsync("main", table, [("part", "identity")]);
            await tx.CommitAsync();
        }
        using var spans = new SpanRecorder(_lake);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", table, Enumerable.Range(0, files * 3).Select(i => Row(i, i % files, columns)));
            await tx.CommitAsync();
        }
        var (stage, commit) = (spans.Commands("ducklake.transaction.stage"), spans.Commands("ducklake.commit"));
        // A second commit, so table statistics are merged into existing rows rather than created.
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", table, Enumerable.Range(0, files * 3).Select(i => Row(1000 + i, i % files, columns)));
            await tx.CommitAsync();
        }
        commit = Math.Max(commit, spans.Commands("ducklake.commit"));
        var loaded = await catalog.LoadTableAsync("main", table);
        Assert.Equal(files * 2, (await catalog.PlanScanAsync(loaded)).Files.Count);
        var plan = spans.Commands("ducklake.scan.plan");
        await catalog.PlanScanAsync(loaded, Expr.Equal("id", 5L));
        var planFiltered = spans.Commands("ducklake.scan.plan");
        await using (var tx = catalog.BeginTransaction())
        {
            Assert.Equal(files * 2, await tx.DeleteAsync("main", table, Expr.Or(Expr.LessThan("id", (long)files), Expr.GreaterThanOrEqual("id", 1000L + 2 * files))));
            await tx.CommitAsync();
        }
        var delete = spans.Commands("ducklake.commit");
        output.WriteLine($"{backend} {files} files x {columns} columns: stage {stage}, commit {commit}, plan {plan}, filtered plan {planFiltered}, delete commit {delete}");
        return (stage, commit, plan, planFiltered, delete);
    }

    [SkippableFact]
    [Spec("ducklake: INSERT")]
    [Spec("ducklake: DELETE")]
    [Spec("ducklake: SELECT")]
    public async Task Catalog_round_trips_do_not_grow_with_files_or_columns()
    {
        if (backend == CatalogBackend.Postgres) TestLake.RequirePostgres();
        var catalog = await _lake.CreateAsync();
        var small = await MeasureAsync(catalog, "small", files: 2, columns: 3);
        var large = await MeasureAsync(catalog, "large", files: 40, columns: 12);
        Assert.Equal(small, large);
    }
}

public sealed class SqlitePerformanceTests(ITestOutputHelper output) : PerformanceTests(CatalogBackend.Sqlite, output);
public sealed class PostgresPerformanceTests(ITestOutputHelper output) : PerformanceTests(CatalogBackend.Postgres, output);

/// <summary>
/// Many writers committing to one table at once all succeed: every lost race for the snapshot ID is retried (with
/// backoff) until it lands, snapshots stay gap-free and every row is there. (With the snapshot row written last,
/// PostgreSQL writers used to collide on <c>ducklake_data_file</c>'s key after doing all their work; it is now written
/// first, so a loser waits for the winner and retries at once.)
/// </summary>
public abstract class ConcurrentWriterTests(CatalogBackend backend) : IDisposable
{
    private readonly TestLake _lake = new(backend);

    public void Dispose() => _lake.Dispose();

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    public async Task Concurrent_writers_all_commit()
    {
        if (backend == CatalogBackend.Postgres) TestLake.RequirePostgres();
        const int writers = 6, commits = 15;
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { MaxCommitAttempts = 50 });
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("writer", DuckLakeType.Int32), new("n", DuckLakeType.Int32)]);
            await tx.CommitAsync();
        }
        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(async () =>
        {
            for (var i = 0; i < commits; i++)
            {
                await using var tx = catalog.BeginTransaction();
                await tx.InsertAsync("main", "t", [[w, i]]);
                await tx.CommitAsync();
            }
        })));
        var snapshots = await catalog.ListSnapshotsAsync();
        Assert.Equal(Enumerable.Range(0, 2 + writers * commits).Select(i => (long)i), snapshots.Select(s => s.SnapshotId));
        var rows = await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"));
        Assert.Equal(writers * commits, rows.Select(r => ((int)r[0]!, (int)r[1]!)).Distinct().Count());
        await DuckLakeValidator.AssertValidAsync(catalog, new DuckLakeValidationOptions { Deep = true });
    }
}

public sealed class SqliteConcurrentWriterTests() : ConcurrentWriterTests(CatalogBackend.Sqlite);
public sealed class PostgresConcurrentWriterTests() : ConcurrentWriterTests(CatalogBackend.Postgres);
