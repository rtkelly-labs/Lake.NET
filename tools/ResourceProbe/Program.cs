using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Types;

if (args.Length != 2 || !int.TryParse(args[0], out var count) || count <= 0)
{
    Console.Error.WriteLine("Usage: ResourceProbe <rows> <temporary-work-directory>");
    return 2;
}
var root = Path.Combine(Path.GetFullPath(args[1]), "resource-" + Guid.NewGuid().ToString("N"));
var results = new List<object>();
Directory.CreateDirectory(root);
try
{
    using var io = new CountingFileIO();
    using var catalog = new InMemoryCatalog(root, io);
    await catalog.CreateNamespaceAsync(Namespace.Of("probe"));
    var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("probe", "events"), new Schema(
        NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "payload", PrimitiveType.String)), FormatVersion: 3));
    // Deterministic moderately compressible data. This is a diagnostic workload, not a universal benchmark.
    var rows = new List<object?[]>();
    await Measure("input-materialization", () =>
    {
        var random = new Random(13);
        var payload = new byte[192];
        rows = Enumerable.Range(0, count).Select(i =>
        {
            random.NextBytes(payload);
            return new object?[] { (long)i, $"{i:D12}:" + Convert.ToBase64String(payload) };
        }).ToList();
        return Task.FromResult((long)rows.Count);
    });
    await Measure("insert", async () => { await TableMutations.InsertAsync(table, rows); return (long)rows.Count; });
    var metrics = new ScanMetrics();
    var beforeReadBytes = io.BytesRead;
    await Measure("projected-scan", async () =>
    {
        long seen = 0;
        await foreach (var row in TableScan.ReadRowsAsync(table, new ScanRequest { Columns = ["id"], Metrics = metrics })) seen++;
        if (seen != count) throw new InvalidOperationException($"Expected {count} rows, read {seen}");
        return seen;
    });
    var scanReadBytes = io.BytesRead - beforeReadBytes;
    var process = Process.GetCurrentProcess();
    var files = (await TableScan.PlanAsync(table, new ScanRequest())).Tasks;
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        runtime = RuntimeInformation.FrameworkDescription,
        os = RuntimeInformation.OSDescription,
        architecture = RuntimeInformation.ProcessArchitecture.ToString(),
        inputRows = count,
        payloadCharacters = 269,
        dataFileCount = files.Count,
        dataFileBytes = files.Sum(f => f.File.FileSizeInBytes),
        metrics.MaxBatchRows,
        metrics.RowGroupsRead,
        scanReadBytes,
        peakProcessWorkingSetBytes = process.PeakWorkingSet64 > 0 ? process.PeakWorkingSet64 : (long?)null,
        processWorkingSetBytes = process.WorkingSet64 > 0 ? process.WorkingSet64 : (long?)null,
        managedHeapBytes = GC.GetTotalMemory(false),
        phases = results,
    }, new JsonSerializerOptions { WriteIndented = true }));
}
finally { Directory.Delete(root, recursive: true); }
return 0;

async Task Measure(string phase, Func<Task<long>> run)
{
    var before = GC.GetTotalAllocatedBytes();
    var clock = Stopwatch.StartNew();
    var rows = await run();
    results.Add(new { phase, rows, elapsedMs = clock.Elapsed.TotalMilliseconds, allocatedBytes = GC.GetTotalAllocatedBytes() - before });
}

internal sealed class CountingFileIO : IFileIO
{
    private readonly LocalFileIO _inner = new();
    public long BytesRead { get; private set; }
    public async Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default)
    {
        var bytes = await _inner.ReadAllAsync(location, ct);
        BytesRead += bytes.Length;
        return bytes;
    }
    public async Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default)
    {
        var bytes = await _inner.ReadRangeAsync(location, offset, length, ct);
        BytesRead += bytes.Length;
        return bytes;
    }
    public Task<Stream> OpenReadAsync(string location, CancellationToken ct = default) => _inner.OpenReadAsync(location, ct);
    public Task<long> LengthAsync(string location, CancellationToken ct = default) => _inner.LengthAsync(location, ct);
    public Task<bool> ExistsAsync(string location, CancellationToken ct = default) => _inner.ExistsAsync(location, ct);
    public Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default) => _inner.WriteAsync(location, content, overwrite, ct);
    public Task DeleteAsync(string location, CancellationToken ct = default) => _inner.DeleteAsync(location, ct);
    public void Dispose() => _inner.Dispose();
}
