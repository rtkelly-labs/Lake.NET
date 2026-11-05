using System.Diagnostics;
using System.Text.Json;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;
using Microsoft.Coyote;
using Microsoft.Coyote.SystematicTesting;

// Temporary evaluation of Coyote on the MetastoreCatalog commit path (see CoyoteCommitRace.csproj).
// Two tests, run in order:
//   canary  - a catalog whose swap is a non-atomic check-then-set. Coyote must find the lost update; if it does not,
//             the setup is not exploring interleavings and the product result means nothing (exit 2).
//   product - the real InMemoryCatalog CAS with concurrent appenders and commit retries. Any bug fails (exit 1).
// Usage: CoyoteCommitRace <iterations> <report-dir>
//        CoyoteCommitRace baseline <runs>   (before rewriting: the same tests on the real thread pool)
if (args is ["baseline", var n])
{
    foreach (var racy in new[] { true, false })
    {
        CommitRace.ResetStats();
        int failures = 0, runs = int.Parse(n, System.Globalization.CultureInfo.InvariantCulture);
        var clock = Stopwatch.StartNew();
        for (var i = 0; i < runs; i++)
        {
            try { await CommitRace.RunAsync(racy); }
            catch (Exception) { failures++; }
        }
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            test = racy ? "canary" : "product",
            mode = "baseline",
            runs,
            failures,
            seconds = Math.Round(clock.Elapsed.TotalSeconds, 1),
            runs_with_retry = CommitRace.RunsWithRetry,
            max_attempts = CommitRace.MaxAttempts,
        }));
    }
    return 0;
}

var iterations = args.Length > 0 ? uint.Parse(args[0], System.Globalization.CultureInfo.InvariantCulture) : 1000;
var reportDir = args.Length > 1 ? args[1] : "coyote-reports";
Directory.CreateDirectory(reportDir);

var canary = Run("canary", () => CommitRace.RunAsync(racy: true), iterations, reportDir);
var product = Run("product", () => CommitRace.RunAsync(racy: false), iterations, reportDir);
if (canary.NumOfFoundBugs == 0)
{
    Console.WriteLine("canary not caught: interleavings are not being explored; the product result is void");
    return 2;
}
return product.NumOfFoundBugs == 0 ? 0 : 1;

static TestReport Run(string name, Func<Task> test, uint iterations, string reportDir)
{
    CommitRace.ResetStats();
    var config = Configuration.Create()
        .WithTestingIterations(iterations)
        .WithRandomGeneratorSeed(20261105)
        .WithMaxSchedulingSteps(5000)
        // An assembly that failed to rewrite must fail loudly, not degrade to random delays and report 0 bugs.
        .WithSystematicFuzzingFallbackEnabled(false)
        .WithPartiallyControlledConcurrencyAllowed(true)
        .WithUncontrolledInvocationStackTraceLoggingEnabled(true)
        .WithTelemetryEnabled(false);
    var engine = TestingEngine.Create(config, test);
    var clock = Stopwatch.StartNew();
    engine.Run();
    clock.Stop();
    var r = engine.TestReport;
    engine.TryEmitReports(reportDir, name, out _);
    var summary = new
    {
        test = name,
        iterations,
        seconds = Math.Round(clock.Elapsed.TotalSeconds, 1),
        iterations_per_second = Math.Round(r.NumOfExploredFairPaths / Math.Max(clock.Elapsed.TotalSeconds, 0.001), 1),
        explored = r.NumOfExploredFairPaths,
        bugs = r.NumOfFoundBugs,
        fair_steps = new[] { r.MinExploredFairSteps, r.MaxExploredFairSteps },
        controlled_operations = new[] { r.MinControlledOperations, r.MaxControlledOperations },
        concurrency_degree = new[] { r.MinConcurrencyDegree, r.MaxConcurrencyDegree },
        runs_with_retry = CommitRace.RunsWithRetry,
        max_attempts = CommitRace.MaxAttempts,
        uncontrolled = r.UncontrolledInvocations.OrderBy(s => s, StringComparer.Ordinal).ToArray(),
        internal_errors = r.InternalErrors.ToArray(),
        bug_reports = r.BugReports.ToArray(),
    };
    Console.WriteLine(JsonSerializer.Serialize(summary));
    return r;
}

internal static class CommitRace
{
    private const int Writers = 3;

    // Statistics only (did the CAS-failure/retry path run?); no test state is shared between iterations.
    private static int _runsWithRetry, _maxAttempts;
    public static int RunsWithRetry => Volatile.Read(ref _runsWithRetry);
    public static int MaxAttempts => Volatile.Read(ref _maxAttempts);

    public static void ResetStats()
    {
        Volatile.Write(ref _runsWithRetry, 0);
        Volatile.Write(ref _maxAttempts, 0);
    }

    public static async Task RunAsync(bool racy)
    {
        var io = new YieldingMemoryFileIO();
        using var catalog = racy ? new RacyCatalog(io) : new InMemoryCatalog("mem://warehouse", io);
        var id = TableIdentifier.Of("gen", "t");
        await catalog.CreateNamespaceAsync(Namespace.Of("gen"));
        await catalog.CreateTableAsync(new CreateTableRequest(id, new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long)]),
            Properties: new Dictionary<string, string>
            {
                ["commit.retry.num-retries"] = "10",
                ["commit.retry.min-wait-ms"] = "1",
                ["commit.retry.max-wait-ms"] = "2",
            }, FormatVersion: 2));

        var writers = Enumerable.Range(0, Writers).Select(w => Task.Run(async () =>
        {
            var table = await catalog.LoadTableAsync(id);
            var result = await TableMutations.InsertAsync(table, [new object?[] { (long)w * 10 }, new object?[] { (long)w * 10 + 1 }]);
            return result.Attempts;
        })).ToArray();
        try
        {
            var attempts = await Task.WhenAll(writers);
            if (attempts.Max() > 1) Interlocked.Increment(ref _runsWithRetry);
            int seen;
            while ((seen = Volatile.Read(ref _maxAttempts)) < attempts.Max()
                && Interlocked.CompareExchange(ref _maxAttempts, attempts.Max(), seen) != seen)
            {
            }
        }
        catch (CommitStateUnknownException e)
        {
            throw new InvalidOperationException($"commit state unknown on an in-process catalog: {e.Message}", e);
        }

        var m = (await catalog.LoadTableAsync(id)).Metadata;
        var chain = m.Ancestors(m.CurrentSnapshotId).ToList();
        Check(chain.Count == Writers, $"lost update: {chain.Count} snapshots in the current lineage after {Writers} successful appends");
        Check(chain.Select(s => s.SequenceNumber).Distinct().Count() == chain.Count, "duplicate sequence numbers in the lineage");
        Check(m.CurrentSnapshot?.Summary.GetValueOrDefault("total-records") == $"{Writers * 2}",
            $"total-records is {m.CurrentSnapshot?.Summary.GetValueOrDefault("total-records")}, expected {Writers * 2}");
        foreach (var s in chain)
            Check(s.ManifestList is { } ml && io.Contains(ml), $"snapshot {s.SnapshotId} manifest list is missing");
        // Hygiene: a writer that lost the swap deletes its metadata file (CommitFailedException path).
        var expected = m.MetadataLog.Select(e => e.MetadataFile).Append(m.MetadataFileLocation!).ToHashSet();
        var stray = io.Locations().Where(l => l.EndsWith(".metadata.json", StringComparison.Ordinal) && !expected.Contains(l)).ToList();
        Check(stray.Count == 0, $"metadata files outside the log: {string.Join(", ", stray)}");
    }

    private static void Check(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}

/// <summary>The canary: check-then-set with a scheduling point between, so a concurrent swap can be overwritten.</summary>
internal sealed class RacyCatalog(IFileIO io) : InMemoryCatalog("mem://warehouse", io)
{
    protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
    {
        if (await CurrentMetadataLocationAsync(id, ct) != expected)
            throw new CommitFailedException($"{id} changed concurrently");
        await Task.Yield();
        var now = await CurrentMetadataLocationAsync(id, ct);
        await base.SwapMetadataLocationAsync(id, now, newLocation, metadata, ct);
    }
}

/// <summary>Object store in memory, with a scheduling point before every operation.</summary>
internal sealed class YieldingMemoryFileIO : IFileIO
{
    private readonly Dictionary<string, byte[]> _files = new(StringComparer.Ordinal);
    private readonly object _lock = new();

    public bool Contains(string location)
    {
        lock (_lock) return _files.ContainsKey(location);
    }

    public List<string> Locations()
    {
        lock (_lock) return [.. _files.Keys];
    }

    private byte[] Get(string location)
    {
        lock (_lock) return _files.TryGetValue(location, out var b) ? b : throw new FileNotFoundException(location);
    }

    public async Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default)
    {
        await Task.Yield();
        return Get(location);
    }

    public async Task<Stream> OpenReadAsync(string location, CancellationToken ct = default)
    {
        await Task.Yield();
        return new MemoryStream(Get(location), writable: false);
    }

    public async Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default)
    {
        await Task.Yield();
        return Get(location).AsSpan((int)offset, length).ToArray();
    }

    public async Task<long> LengthAsync(string location, CancellationToken ct = default)
    {
        await Task.Yield();
        return Get(location).LongLength;
    }

    public async Task<bool> ExistsAsync(string location, CancellationToken ct = default)
    {
        await Task.Yield();
        return Contains(location);
    }

    public async Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default)
    {
        await Task.Yield();
        lock (_lock)
        {
            if (!overwrite && _files.ContainsKey(location)) throw new FileAlreadyExistsException(location);
            _files[location] = content.ToArray();
        }
    }

    public async Task DeleteAsync(string location, CancellationToken ct = default)
    {
        await Task.Yield();
        lock (_lock) _files.Remove(location);
    }

    public void Dispose()
    {
    }
}
