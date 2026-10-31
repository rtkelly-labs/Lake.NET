using System.Diagnostics;
using Iceberg.Net.Data;
using Iceberg.Net.Data.ParquetNet;
using Iceberg.Net.IO;
using Iceberg.Net.Types;

// The same Iceberg rows written and read through ParquetFiles on each backend: best of three runs after a warm-up.
//   dotnet run -c Release --project tools/ParquetBackendBench [rows]
var rowCount = args.Length > 0 ? int.Parse(args[0], System.Globalization.CultureInfo.InvariantCulture) : 1_000_000;
var dir = Directory.CreateTempSubdirectory("parquet-backend-bench").FullName;
var io = new LocalFileIO();

var flat = new Schema(0,
[
    NestedField.Require(1, "id", PrimitiveType.Long),
    NestedField.Optional(2, "ts", PrimitiveType.TimestampTz),
    NestedField.Optional(3, "name", PrimitiveType.String),
    NestedField.Optional(4, "amount", new DecimalType(12, 2)),
    NestedField.Optional(5, "score", PrimitiveType.Double),
    NestedField.Optional(6, "key", PrimitiveType.Uuid),
]);
var nested = new Schema(0,
[
    NestedField.Require(1, "id", PrimitiveType.Long),
    NestedField.Optional(2, "tags", new ListType(3, PrimitiveType.String, false)),
    NestedField.Optional(4, "attrs", new MapType(5, PrimitiveType.String, 6, PrimitiveType.Long, false)),
    NestedField.Optional(7, "point", new StructType(NestedField.Optional(8, "x", PrimitiveType.Double), NestedField.Optional(9, "y", PrimitiveType.Double))),
]);
var random = new Random(42);
var flatRows = Enumerable.Range(0, rowCount).Select(i => new object?[]
{
    (long)i, 1_700_000_000_000_000L + i * 1_000L, i % 10 == 0 ? null : "name" + (i % 1000),
    new IcebergDecimal(random.Next(), 2), random.NextDouble(), Guid.NewGuid(),
}).ToList();
var nestedRows = Enumerable.Range(0, rowCount / 4).Select(i => new object?[]
{
    (long)i, i % 7 == 0 ? null : new List<object?> { "a" + (i % 10), "b" + (i % 100) },
    new Dictionary<object, object?> { ["k" + (i % 5)] = (long)i },
    new object?[] { random.NextDouble(), i % 3 == 0 ? null : random.NextDouble() },
}).ToList();

async Task<(double Write, double Read, long Bytes)> Run(Schema schema, List<object?[]> rows, string tag)
{
    var best = (Write: double.MaxValue, Read: double.MaxValue, Bytes: 0L);
    for (var round = 0; round < 4; round++)
    {
        var path = Path.Combine(dir, $"{tag}-{ParquetFiles.BackendName}-{round}.parquet");
        var sw = Stopwatch.StartNew();
        var file = await ParquetFiles.WriteAsync(io, path, schema, 0, [], rows, new ParquetWriteOptions { RowGroupRows = 250_000 });
        var write = sw.Elapsed.TotalMilliseconds;
        sw.Restart();
        var read = await ParquetFiles.ReadAsync(io, path, schema);
        var readMs = sw.Elapsed.TotalMilliseconds;
        if (read.Count != rows.Count) throw new InvalidOperationException($"read {read.Count} rows of {rows.Count}");
        if (round > 0) best = (Math.Min(best.Write, write), Math.Min(best.Read, readMs), file.FileSizeInBytes);
    }
    return best;
}

Console.WriteLine($"| Backend | Table | Write | Read | Size |");
Console.WriteLine($"|---|---|---|---|---|");
foreach (var use in new Action[] { ParquetNet.UseParquetSharp, ParquetNet.UseAsDefault })
{
    use();
    foreach (var (tag, schema, rows) in new[] { ("flat", flat, flatRows), ("nested", nested, nestedRows) })
    {
        var (write, read, bytes) = await Run(schema, rows, tag);
        Console.WriteLine($"| {ParquetFiles.BackendName} | {tag} ({rows.Count:N0} rows) | {write:N0} ms | {read:N0} ms | {bytes / 1024.0 / 1024:F1} MB |");
    }
}
Directory.Delete(dir, true);
