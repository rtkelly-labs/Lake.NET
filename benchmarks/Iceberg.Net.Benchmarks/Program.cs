using System.Globalization;
using System.Text.Json;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Iceberg.Net.Benchmarks;

/// <summary>
/// <c>-- --filter '*' --artifacts &lt;dir&gt;</c> runs the benchmarks (BenchmarkDotNet's own arguments);
/// <c>-- compare &lt;previous dir&gt; &lt;current dir&gt; [ratio]</c> compares two runs' JSON reports and exits 1 when a
/// benchmark's median time or allocated bytes grew by more than <c>ratio</c> (default 1.5: shared runners are noisy,
/// so only large regressions are flagged).
/// </summary>
public static class Program
{
    public static int Main(string[] args)
    {
        if (args is ["compare", var previous, var current, ..])
            return Compare(previous, current, args.Length > 3 ? double.Parse(args[3], CultureInfo.InvariantCulture) : 1.5);

        // In-process: no generated project to build (it would inherit the repository's central package management).
        var job = Job.Default.WithLaunchCount(1).WithWarmupCount(2).WithIterationCount(10).WithToolchain(InProcessEmitToolchain.Instance);
        var config = ManualConfig.Create(DefaultConfig.Instance).AddJob(job).AddExporter(JsonExporter.Full);
        var summaries = BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        return summaries.Any(s => s.HasCriticalValidationErrors || s.Reports.Any(r => !r.Success)) ? 1 : 0;
    }

    private sealed record Result(double MedianNs, long? AllocatedBytes);

    private static int Compare(string previousDir, string currentDir, double ratio)
    {
        var previous = Load(previousDir);
        var current = Load(currentDir);
        if (current.Count == 0)
        {
            Console.Error.WriteLine($"No benchmark results under {currentDir}");
            return 1;
        }

        var regressed = 0;
        Console.WriteLine("| Benchmark | Median | Previous | Ratio | Allocated | Previous | Ratio |");
        Console.WriteLine("|---|---|---|---|---|---|---|");
        foreach (var (name, now) in current.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            if (!previous.TryGetValue(name, out var before))
            {
                Console.WriteLine($"| {name} | {Time(now.MedianNs)} | – | new | {Bytes(now.AllocatedBytes)} | – | |");
                continue;
            }
            var timeRatio = now.MedianNs / before.MedianNs;
            double? allocRatio = now.AllocatedBytes is { } a && before.AllocatedBytes is { } b && b > 0 ? (double)a / b : null;
            var flagged = timeRatio > ratio || allocRatio > ratio;
            if (flagged) regressed++;
            Console.WriteLine($"| {name} | {Time(now.MedianNs)} | {Time(before.MedianNs)} | {Mark(timeRatio, ratio)} | " +
                $"{Bytes(now.AllocatedBytes)} | {Bytes(before.AllocatedBytes)} | {(allocRatio is { } r ? Mark(r, ratio) : "–")} |");
        }
        Console.WriteLine();
        Console.WriteLine(regressed == 0
            ? $"No benchmark slowed or allocated more than {ratio:0.##}× the previous run."
            : $"**{regressed} benchmark(s) regressed by more than {ratio:0.##}×.**");
        return regressed == 0 ? 0 : 1;
    }

    private static Dictionary<string, Result> Load(string dir)
    {
        var results = new Dictionary<string, Result>(StringComparer.Ordinal);
        if (!Directory.Exists(dir)) return results;
        foreach (var file in Directory.EnumerateFiles(dir, "*-report-full.json", SearchOption.AllDirectories))
        {
            using var doc = JsonDocument.Parse(File.ReadAllText(file));
            foreach (var b in doc.RootElement.GetProperty("Benchmarks").EnumerateArray())
            {
                if (!b.TryGetProperty("Statistics", out var stats) || stats.ValueKind != JsonValueKind.Object) continue;
                long? allocated = b.TryGetProperty("Memory", out var memory) && memory.ValueKind == JsonValueKind.Object
                    && memory.TryGetProperty("BytesAllocatedPerOperation", out var bytes) && bytes.ValueKind == JsonValueKind.Number
                    ? bytes.GetInt64() : null;
                var name = b.TryGetProperty("MethodTitle", out var title) ? title.GetString()! : b.GetProperty("Method").GetString()!;
                results[name] = new Result(stats.GetProperty("Median").GetDouble(), allocated);
            }
        }
        return results;
    }

    private static string Mark(double r, double ratio) => r > ratio ? $"**{r:0.00}× ⚠**" : $"{r:0.00}×";

    private static string Time(double ns) => ns switch
    {
        >= 1e9 => $"{ns / 1e9:0.00} s",
        >= 1e6 => $"{ns / 1e6:0.0} ms",
        >= 1e3 => $"{ns / 1e3:0.0} µs",
        _ => $"{ns:0} ns",
    };

    private static string Bytes(long? b) => b switch
    {
        null => "–",
        >= 1L << 20 => $"{b / (double)(1L << 20):0.0} MiB",
        >= 1L << 10 => $"{b / 1024.0:0.0} KiB",
        _ => $"{b} B",
    };
}
