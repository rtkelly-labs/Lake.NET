// Wall-clock benchmarks of Iceberg.Net.DuckLake (docs/ducklake.md, "Performance").
//
//   dotnet run -c Release --project tools/DuckLakeBench [--postgres "<npgsql connection string to a server>"] [--scale n]
//
// Each scenario runs on a fresh lake in a temporary directory; with --postgres, each gets a fresh database there.
// --scale multiplies the row counts (default 1: about a million rows written in total). Prints a Markdown table.
using System.Diagnostics;
using Iceberg.Net.DuckLake;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Npgsql;

var postgres = Arg("--postgres");
var scale = double.TryParse(Arg("--scale"), System.Globalization.CultureInfo.InvariantCulture, out var s) ? s : 1;
var results = new List<(string Scenario, string Result)>();
var root = Path.Combine(Path.GetTempPath(), "ducklake-bench-" + Guid.NewGuid().ToString("N")[..8]);

DuckLakeColumnDefinition[] columns =
[
    new("id", DuckLakeType.Int64, Nullable: false), new("customer", DuckLakeType.Varchar), new("amount", DuckLakeType.Decimal(12, 2)),
    new("at", DuckLakeType.TimestampTz), new("day", DuckLakeType.Int32),
];
object?[] Row(long id) => [id, $"customer-{id % 1000}", (id % 10_000) / 100m, DateTimeOffset.UnixEpoch.AddSeconds(1_700_000_000 + id), (int)(id % 30)];

var n = (int)(1_000_000 * scale);
await Scenario("bulk insert", async lake =>
{
    await Create(lake, "t");
    var sw = Stopwatch.StartNew();
    await using (var tx = lake.BeginTransaction())
    {
        await tx.InsertAsync("main", "t", Enumerable.Range(0, n).Select(i => Row(i)));
        await tx.CommitAsync();
    }
    return $"{n:N0} rows in {sw.Elapsed.TotalSeconds:F2} s ({n / sw.Elapsed.TotalSeconds:N0} rows/s)";
});
await Scenario("partitioned insert (30 files)", async lake =>
{
    await Create(lake, "t", partitioned: true);
    var rows = n / 2;
    var sw = Stopwatch.StartNew();
    await using (var tx = lake.BeginTransaction())
    {
        await tx.InsertAsync("main", "t", Enumerable.Range(0, rows).Select(i => Row(i)));
        await tx.CommitAsync();
    }
    return $"{rows:N0} rows in {sw.Elapsed.TotalSeconds:F2} s ({rows / sw.Elapsed.TotalSeconds:N0} rows/s)";
});
await Scenario("small commits", async lake =>
{
    await Create(lake, "t");
    const int commits = 200;
    var latencies = new List<double>();
    for (var i = 0; i < commits; i++)
    {
        var sw = Stopwatch.StartNew();
        await using var tx = lake.BeginTransaction();
        await tx.InsertAsync("main", "t", [Row(i)]);
        await tx.CommitAsync();
        latencies.Add(sw.Elapsed.TotalMilliseconds);
    }
    return $"{commits} one-row commits: {Percentiles(latencies)}";
});
await Scenario("small commits, inlined (limit 10)", async lake =>
{
    await Create(lake, "t");
    const int commits = 200;
    var latencies = new List<double>();
    for (var i = 0; i < commits; i++)
    {
        var sw = Stopwatch.StartNew();
        await using var tx = lake.BeginTransaction();
        await tx.InsertAsync("main", "t", [Row(i)]);
        await tx.CommitAsync();
        latencies.Add(sw.Elapsed.TotalMilliseconds);
    }
    var table = await lake.LoadTableAsync("main", "t");
    var sw2 = Stopwatch.StartNew();
    var count = (await lake.ReadAsync(table)).Count;
    var read = sw2.Elapsed.TotalMilliseconds;
    sw2.Restart();
    await lake.FlushInlinedDataAsync();
    return $"{commits} one-row commits: {Percentiles(latencies)}; reading the {count} inlined rows {read:F0} ms; flushing them {sw2.Elapsed.TotalMilliseconds:F0} ms";
}, new DuckLakeOptions { DataInliningRowLimit = 10 });
await Scenario("scan planning after 1,000 commits", async lake =>
{
    await Create(lake, "t");
    for (var batch = 0; batch < 1000; batch += 50)
    {
        // 50 files per commit keeps the setup quick; the catalog then holds 1,000 live files.
        await using var tx = lake.BeginTransaction();
        for (var f = 0; f < 50; f++) await tx.InsertAsync("main", "t", Enumerable.Range((batch + f) * 100, 100).Select(i => Row(i)));
        await tx.CommitAsync();
    }
    var table = await lake.LoadTableAsync("main", "t");
    var full = await Time(10, () => lake.PlanScanAsync(table));
    var pruned = await Time(10, () => lake.PlanScanAsync(table, Expr.Equal("id", 4242L)));
    var plan = await lake.PlanScanAsync(table, Expr.Equal("id", 4242L));
    return $"1,000 files: full plan {Percentiles(full)}; point lookup {Percentiles(pruned)} ({plan.Files.Count} file kept)";
});
await Scenario("full scan", async lake =>
{
    await Create(lake, "t");
    await using (var tx = lake.BeginTransaction())
    {
        for (var f = 0; f < 10; f++) await tx.InsertAsync("main", "t", Enumerable.Range(f * (n / 10), n / 10).Select(i => Row(i)));
        await tx.CommitAsync();
    }
    var table = await lake.LoadTableAsync("main", "t");
    var sw = Stopwatch.StartNew();
    long count = 0;
    await foreach (var _ in lake.ReadRowsAsync(table)) count++;
    var all = sw.Elapsed.TotalSeconds;
    sw.Restart();
    var hits = (await lake.ReadAsync(table, Expr.Equal("id", 4242L))).Count;
    var lookup = sw.Elapsed.TotalMilliseconds;
    sw.Restart();
    long typed = 0;
    await foreach (var _ in lake.ReadAsync<Order>(table)) typed++;
    var typedSeconds = sw.Elapsed.TotalSeconds;
    return $"{count:N0} rows in 10 files: {all:F2} s ({count / all:N0} rows/s); as records (ReadAsync<T>) {typedSeconds:F2} s ({typed / typedSeconds:N0} rows/s); point lookup {lookup:F0} ms ({hits} row)";
});
await Scenario("delete 10% across 20 files", async lake =>
{
    await Create(lake, "t");
    var rows = n / 2;
    await using (var tx = lake.BeginTransaction())
    {
        for (var f = 0; f < 20; f++) await tx.InsertAsync("main", "t", Enumerable.Range(f * (rows / 20), rows / 20).Select(i => Row(i)));
        await tx.CommitAsync();
    }
    var sw = Stopwatch.StartNew();
    long deleted;
    await using (var tx = lake.BeginTransaction())
    {
        deleted = await tx.DeleteAsync("main", "t", Expr.Equal("day", 7));
        await tx.CommitAsync();
    }
    return $"{deleted:N0} of {rows:N0} rows in {sw.Elapsed.TotalSeconds:F2} s";
});
await Scenario("delete 10% across 20 files, deletion vectors (1.1)", async lake =>
{
    await Create(lake, "t");
    var rows = n / 2;
    await using (var tx = lake.BeginTransaction())
    {
        for (var f = 0; f < 20; f++) await tx.InsertAsync("main", "t", Enumerable.Range(f * (rows / 20), rows / 20).Select(i => Row(i)));
        await tx.CommitAsync();
    }
    var sw = Stopwatch.StartNew();
    long deleted;
    await using (var tx = lake.BeginTransaction())
    {
        deleted = await tx.DeleteAsync("main", "t", Expr.Equal("day", 7));
        await tx.CommitAsync();
    }
    var delete = sw.Elapsed.TotalSeconds;
    sw.Restart();
    long left = 0;
    await foreach (var _ in lake.ReadRowsAsync(await lake.LoadTableAsync("main", "t"))) left++;
    return $"{deleted:N0} of {rows:N0} rows in {delete:F2} s; reading the {left:N0} left {sw.Elapsed.TotalSeconds:F2} s";
}, new DuckLakeOptions { WriteDeletionVectors = true }, DuckLakeCatalog.FormatVersion11);
await Scenario("add 100 existing files", async lake =>
{
    await Create(lake, "t");
    var external = Path.Combine(root, "external-" + results.Count);
    Directory.CreateDirectory(external);
    var io = new LocalFileIO();
    Unsafe.NextId = 1;
    var schema = new Iceberg.Net.Types.Schema(0, columns.Select((c, i) => DuckLakeTypes.ToIceberg(DuckLakeColumn.Assign(c.Name, c.Type, c.Nullable, ref Unsafe.NextId))));
    var files = new List<string>();
    var perFile = n / 100;
    for (var f = 0; f < 100; f++)
    {
        var path = Path.Combine(external, $"part-{f}.parquet");
        await Iceberg.Net.Data.ParquetFiles.WriteAsync(io, path, schema, 0, [], Enumerable.Range(f * perFile, perFile)
            .Select(i => columns.Select((c, k) => DuckLakeValues.Normalize(c.Type, Row(i)[k])).ToArray()).ToList(), Iceberg.Net.Data.ParquetWriteOptions.Default);
        files.Add(path);
    }
    var sw = Stopwatch.StartNew();
    long added;
    await using (var tx = lake.BeginTransaction())
    {
        added = await tx.AddDataFilesAsync("main", "t", files);
        await tx.CommitAsync();
    }
    var register = sw.Elapsed.TotalSeconds;
    sw.Restart();
    long read = 0;
    await foreach (var _ in lake.ReadRowsAsync(await lake.LoadTableAsync("main", "t"))) read++;
    return $"{added:N0} rows registered in {register:F2} s (footers only); read through the name mapping in {sw.Elapsed.TotalSeconds:F2} s ({read / sw.Elapsed.TotalSeconds:N0} rows/s)";
});
await Scenario("insert unsigned, timetz and interval columns", async lake =>
{
    await using (var tx = lake.BeginTransaction())
    {
        await tx.CreateTableAsync("main", "w", [new("id", DuckLakeType.UInt64), new("ms", DuckLakeType.TimestampMs), new("tz", DuckLakeType.TimeTz), new("iv", DuckLakeType.Interval)]);
        await tx.CommitAsync();
    }
    var rows = n / 2;
    var sw = Stopwatch.StartNew();
    await using (var tx = lake.BeginTransaction())
    {
        await tx.InsertAsync("main", "w", Enumerable.Range(0, rows).Select(i => new object?[]
        {
            (ulong)i, DateTime.UnixEpoch.AddMilliseconds(i), new DuckLakeTimeTz(i % 86_400 * 1_000_000L, 3600), new DuckLakeInterval(i % 12, i % 30, i % 1000 * 1000L),
        }));
        await tx.CommitAsync();
    }
    var write = sw.Elapsed.TotalSeconds;
    sw.Restart();
    long read = 0;
    await foreach (var _ in lake.ReadRowsAsync(await lake.LoadTableAsync("main", "w"))) read++;
    return $"{rows:N0} rows in {write:F2} s ({rows / write:N0} rows/s); read {sw.Elapsed.TotalSeconds:F2} s ({read / sw.Elapsed.TotalSeconds:N0} rows/s)";
});
await Scenario("encrypted lake: insert, scan, delete", async lake =>
{
    await Create(lake, "t");
    var sw = Stopwatch.StartNew();
    await using (var tx = lake.BeginTransaction())
    {
        for (var f = 0; f < 10; f++) await tx.InsertAsync("main", "t", Enumerable.Range(f * (n / 10), n / 10).Select(i => Row(i)));
        await tx.CommitAsync();
    }
    var write = sw.Elapsed.TotalSeconds;
    sw.Restart();
    long count = 0;
    await foreach (var _ in lake.ReadRowsAsync(await lake.LoadTableAsync("main", "t"))) count++;
    var scan = sw.Elapsed.TotalSeconds;
    sw.Restart();
    long deleted;
    await using (var tx = lake.BeginTransaction())
    {
        deleted = await tx.DeleteAsync("main", "t", Expr.Equal("day", 7));
        await tx.CommitAsync();
    }
    return $"{n:N0} rows in {write:F2} s ({n / write:N0} rows/s); full scan {scan:F2} s ({count / scan:N0} rows/s); delete {deleted:N0} rows {sw.Elapsed.TotalSeconds:F2} s";
}, new DuckLakeOptions { Encrypted = true });
await Scenario("4 concurrent writers", async lake =>
{
    await Create(lake, "t");
    const int perWriter = 25;
    var sw = Stopwatch.StartNew();
    await Task.WhenAll(Enumerable.Range(0, 4).Select(w => Task.Run(async () =>
    {
        for (var i = 0; i < perWriter; i++)
        {
            await using var tx = lake.BeginTransaction();
            await tx.InsertAsync("main", "t", Enumerable.Range(w * 10_000 + i * 10, 10).Select(x => Row(x)));
            await tx.CommitAsync();
        }
    })));
    var snapshots = (await lake.ListSnapshotsAsync()).Count - 2;
    return $"{4 * perWriter} commits in {sw.Elapsed.TotalSeconds:F2} s ({4 * perWriter / sw.Elapsed.TotalSeconds:F0} commits/s), {snapshots} snapshots";
});

Console.WriteLine($"| Scenario ({(postgres is null ? "SQLite" : "PostgreSQL")} catalog, scale {scale}) | Result |");
Console.WriteLine("|---|---|");
foreach (var (scenario, result) in results) Console.WriteLine($"| {scenario} | {result} |");
Directory.Delete(root, recursive: true);
return;

async Task Scenario(string name, Func<DuckLakeCatalog, Task<string>> run, DuckLakeOptions? options = null, string version = DuckLakeCatalog.FormatVersion)
{
    var dir = Path.Combine(root, results.Count.ToString(System.Globalization.CultureInfo.InvariantCulture));
    Directory.CreateDirectory(dir);
    DuckLakeStore store;
    string? database = null;
    if (postgres is null) store = DuckLakeStore.Sqlite($"Data Source={Path.Combine(dir, "catalog.sqlite")}");
    else
    {
        database = "ducklake_bench_" + Guid.NewGuid().ToString("N")[..8];
        await using (var c = new NpgsqlConnection(postgres))
        {
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand($"CREATE DATABASE {database}", c);
            await cmd.ExecuteNonQueryAsync();
        }
        store = DuckLakeStore.PostgreSql(new NpgsqlConnectionStringBuilder(postgres) { Database = database }.ConnectionString);
    }
    try
    {
        var lake = await DuckLakeCatalog.CreateAsync(store, new LocalFileIO(), Path.Combine(dir, "data") + "/", options, version);
        Console.Error.WriteLine($"{name}...");
        results.Add((name, await run(lake)));
    }
    finally
    {
        if (database is not null)
        {
            NpgsqlConnection.ClearAllPools();
            await using var c = new NpgsqlConnection(postgres);
            await c.OpenAsync();
            await using var cmd = new NpgsqlCommand($"DROP DATABASE {database} WITH (FORCE)", c);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}

async Task Create(DuckLakeCatalog lake, string name, bool partitioned = false)
{
    await using var tx = lake.BeginTransaction();
    await tx.CreateTableAsync("main", name, columns);
    if (partitioned) await tx.SetPartitioningAsync("main", name, [("day", "identity")]);
    await tx.CommitAsync();
}

static async Task<List<double>> Time(int runs, Func<Task> run)
{
    await run(); // warm-up
    var times = new List<double>();
    for (var i = 0; i < runs; i++)
    {
        var sw = Stopwatch.StartNew();
        await run();
        times.Add(sw.Elapsed.TotalMilliseconds);
    }
    return times;
}

static string Percentiles(List<double> ms)
{
    var sorted = ms.Order().ToList();
    double P(double p) => sorted[Math.Min(sorted.Count - 1, (int)(p * sorted.Count))];
    return $"p50 {P(0.5):F1} ms, p95 {P(0.95):F1} ms";
}

string? Arg(string name) => args.SkipWhile(a => a != name).Skip(1).FirstOrDefault();

/// <summary>A row of the benchmark table, for <c>ReadAsync&lt;T&gt;</c>.</summary>
internal sealed record Order(long Id, string? Customer, decimal? Amount, DateTimeOffset? At, int? Day);

internal static class Unsafe
{
    public static long NextId = 1;
}
