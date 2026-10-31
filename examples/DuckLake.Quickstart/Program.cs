// A DuckLake from C#: a catalog database plus a directory of Parquet files, which DuckDB opens as it is.
//
//   dotnet run --project examples/DuckLake.Quickstart [directory] [postgres connection string]
//
// With a directory only, the catalog is a SQLite file in it; with an Npgsql connection string (to an empty database),
// the catalog is PostgreSQL. The program prints the ATTACH statement DuckDB opens the lake with.
using Iceberg.Net.DuckLake;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;

var root = Path.GetFullPath(args.Length > 0 ? args[0] : Path.Combine(Path.GetTempPath(), "ducklake-quickstart-" + Guid.NewGuid().ToString("N")[..8]));
Directory.CreateDirectory(root);
var catalogFile = Path.Combine(root, "catalog.sqlite");
var store = args.Length > 1 ? DuckLakeStore.PostgreSql(args[1]) : DuckLakeStore.Sqlite($"Data Source={catalogFile};Pooling=False");

// 1. Create the catalog (format 1.1): the DuckLake tables, snapshot 0 and the "main" schema.
DuckLakeCatalog lake;
try
{
    lake = await DuckLakeCatalog.OpenAsync(store, new LocalFileIO());
}
catch (Exception e) when (e is Microsoft.EntityFrameworkCore.DbUpdateException or System.Data.Common.DbException)
{
    lake = await DuckLakeCatalog.CreateAsync(store, new LocalFileIO(), Path.Combine(root, "data") + "/");
}
Console.WriteLine($"DuckLake {lake.Version} on {store.Dialect} at {root} (data path {lake.DataPath})");

// 2. One snapshot: a schema, a partitioned table with nested columns, and its first rows.
await using (var tx = lake.BeginTransaction(author: "quickstart", commitMessage: "create shop.orders"))
{
    await tx.CreateSchemaAsync("shop");
    await tx.CreateTableAsync("shop", "orders",
    [
        new("id", DuckLakeType.Int64, Nullable: false),
        new("customer", DuckLakeType.Varchar),
        new("amount", DuckLakeType.Decimal(10, 2)),
        new("placed_at", DuckLakeType.TimestampTz),
        new("items", DuckLakeType.List(DuckLakeType.Struct(("sku", DuckLakeType.Varchar), ("qty", DuckLakeType.Int32)))),
    ]);
    await tx.SetPartitioningAsync("shop", "orders", [("placed_at", "month")]);
    await tx.InsertAsync("shop", "orders", Enumerable.Range(1, 6).Select(i => new object?[]
    {
        (long)i,
        i % 2 == 0 ? "ada" : "grace",
        i * 12.5m,
        new DateTimeOffset(2026, 9 + i % 2, i, 10, 0, 0, TimeSpan.Zero),
        new List<object?> { new object?[] { $"sku-{i}", i } },
    }));
    var created = await tx.CommitAsync();
    Console.WriteLine($"snapshot {created.SnapshotId}: {string.Join(", ", created.Changes)}");
}

// 3. Merge-on-read delete and an UPDATE (delete + insert), each its own snapshot.
await using (var tx = lake.BeginTransaction(commitMessage: "cancel order 3"))
{
    await tx.DeleteAsync("shop", "orders", Expr.Equal("id", 3L));
    Console.WriteLine($"snapshot {(await tx.CommitAsync()).SnapshotId}: deleted order 3");
}
await using (var tx = lake.BeginTransaction(commitMessage: "discount ada"))
{
    var n = await tx.UpdateAsync("shop", "orders", Expr.Equal("customer", "ada"),
        row => [row[0], row[1], DuckLakeValues.Normalize(DuckLakeType.Decimal(10, 2), ((Iceberg.Net.Types.IcebergDecimal)row[2]!).ToDecimal() * 0.9m), row[3], row[4]]);
    Console.WriteLine($"snapshot {(await tx.CommitAsync()).SnapshotId}: updated {n} orders");
}

// 4. Schema evolution: add a column; existing files read it as NULL.
await using (var tx = lake.BeginTransaction(commitMessage: "add status"))
{
    await tx.AddColumnAsync("shop", "orders", new("status", DuckLakeType.Varchar));
    Console.WriteLine($"snapshot {(await tx.CommitAsync()).SnapshotId}: added column status");
}

// 5. Reads: the latest snapshot with a pruned, filtered scan, then time travel.
var orders = await lake.LoadTableAsync("shop", "orders");
var filter = Expr.LessThan("amount", 20m);
var plan = await lake.PlanScanAsync(orders, filter);
Console.WriteLine($"\nshop.orders @ {orders.SnapshotId}: {plan.Files.Count} file(s) to read, {plan.PrunedFiles} pruned by statistics for amount < 20");
foreach (var row in await lake.ReadAsync(orders, filter))
    Console.WriteLine($"  id={row[0]} customer={row[1]} amount={row[2]} status={row[5] ?? "NULL"}");

var first = await lake.LoadTableAsync("shop", "orders", snapshotId: 1);
Console.WriteLine($"\nas of snapshot 1: {(await lake.ReadAsync(first)).Count} rows, columns {string.Join(", ", first.Columns.Select(c => c.Name))}");

Console.WriteLine("\nsnapshots:");
foreach (var s in await lake.ListSnapshotsAsync())
    Console.WriteLine($"  {s.SnapshotId,2}  {s.Time:u}  schema v{s.SchemaVersion}  {string.Join(", ", s.Changes)}  {s.CommitMessage}");

// 6. Check the whole lake: the catalog's invariants, and every live file against what the catalog records.
var problems = await DuckLakeValidator.ValidateAsync(lake, new DuckLakeValidationOptions { Deep = true });
Console.WriteLine($"\nvalidation: {(problems.Count == 0 ? "OK" : string.Join("; ", problems))}");

Console.WriteLine($"\nOpen it in DuckDB:\n  ATTACH '{store.DuckDbAttach()}' AS lake;\n  SELECT * FROM lake.shop.orders;");
