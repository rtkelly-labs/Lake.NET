using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;
using Microsoft.Data.Sqlite;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>A DuckLake in a temporary directory: a SQLite catalog file and a data directory beside it.</summary>
public sealed class TestLake : IDisposable
{
    public TestLake()
    {
        Root = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", "ducklake-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Root);
    }

    public string Root { get; }
    public string CatalogFile => Path.Combine(Root, "catalog.sqlite");
    public string DataPath => Path.Combine(Root, "data") + "/";
    public LocalFileIO IO { get; } = new();

    /// <summary>Pooling off: DuckDB opens the same file between our calls, and a pooled handle would outlive the test.</summary>
    public SqliteConnection Connect() => new($"Data Source={CatalogFile};Pooling=False");

    public Task<DuckLakeCatalog> CreateAsync(DuckLakeOptions? options = null) => DuckLakeCatalog.CreateAsync(Connect, IO, DataPath, options);
    public Task<DuckLakeCatalog> OpenAsync(DuckLakeOptions? options = null) => DuckLakeCatalog.OpenAsync(Connect, IO, options);

    public IEnumerable<string> Files() => Directory.Exists(DataPath) ? Directory.EnumerateFiles(DataPath, "*", SearchOption.AllDirectories) : [];

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try { Directory.Delete(Root, recursive: true); }
        catch (IOException) { }
    }
}

/// <summary>
/// DuckDB's <c>ducklake</c> extension, the DuckLake reference implementation, through interop/ducklake_oracle.py.
/// Runs with <c>ICEBERG_DUCKDB_PYTHON</c> (else <c>ICEBERG_PYTHON</c>) when it has the interop <c>duckdb</c> group;
/// skips otherwise with the reason, and fails instead under <c>ICEBERG_DUCKDB_REQUIRED=1</c> (CI).
/// </summary>
public static class DuckLakeOracle
{
    private static readonly Lazy<string?> Skip = new(Probe);

    private static string? Python => Environment.GetEnvironmentVariable("ICEBERG_DUCKDB_PYTHON") is { Length: > 0 } p ? p
        : Environment.GetEnvironmentVariable("ICEBERG_PYTHON") is { Length: > 0 } q ? q : null;

    /// <summary>Skip unless the oracle is available; under ICEBERG_DUCKDB_REQUIRED=1 an unavailable oracle fails.</summary>
    public static void Require()
    {
        if (Environment.GetEnvironmentVariable("ICEBERG_DUCKDB_REQUIRED") == "1") Assert.True(Skip.Value is null, Skip.Value);
        Xunit.Skip.If(Skip.Value is not null, Skip.Value);
    }

    private static string? Probe()
    {
        if (Environment.GetEnvironmentVariable("ICEBERG_DUCKDB") == "0") return "DuckLake oracle disabled (ICEBERG_DUCKDB=0)";
        if (Python is null) return "DuckLake oracle unavailable: set ICEBERG_DUCKDB_PYTHON (or ICEBERG_PYTHON) to a Python with the interop 'duckdb' group";
        try
        {
            var probe = RunAsync(["probe"]).GetAwaiter().GetResult();
            return probe["available"]!.GetValue<bool>() ? null : $"DuckLake oracle unavailable: {probe["reason"]}";
        }
        catch (Exception e) when (e is InvalidOperationException or System.ComponentModel.Win32Exception or TimeoutException)
        {
            return $"DuckLake oracle unavailable: {e.Message}";
        }
    }

    /// <summary>Run statements against the lake attached as <c>lake</c>; every statement must succeed.</summary>
    public static async Task<List<JsonObject>> RunAsync(TestLake lake, params string[] statements)
    {
        var results = await TryRunAsync(lake, statements);
        foreach (var r in results)
            if (r["error"] is { } e) throw new InvalidOperationException($"DuckDB: {e}");
        Assert.Equal(statements.Length, results.Count);
        return results;
    }

    /// <summary>As <see cref="RunAsync(TestLake, string[])"/>, returning a failed statement's error instead of throwing.</summary>
    public static async Task<List<JsonObject>> TryRunAsync(TestLake lake, params string[] statements)
    {
        var job = Path.Combine(lake.Root, $"job-{Guid.NewGuid():N}.json");
        var exists = File.Exists(lake.CatalogFile);
        await File.WriteAllTextAsync(job, JsonSerializer.Serialize(new { catalog = lake.CatalogFile, data_path = exists ? null : lake.DataPath, statements }));
        var result = await RunAsync(["run", job]);
        return result.AsArray().Select(n => n!.AsObject()).ToList();
    }

    /// <summary>Rows of one query, each row's values as JSON text for order-insensitive comparison.</summary>
    public static async Task<List<string>> RowsAsync(TestLake lake, string sql) =>
        (await RunAsync(lake, sql))[0]["rows"]!.AsArray().Select(r => LakeJson.Canonical(r)!.ToJsonString()).Order(StringComparer.Ordinal).ToList();

    private static async Task<JsonNode> RunAsync(IEnumerable<string> args)
    {
        var psi = new ProcessStartInfo(Python!) { RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add(Path.Combine(TestEnvironment.RepoDir(), "interop", "ducklake_oracle.py"));
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi) ?? throw new InvalidOperationException($"cannot start {Python}");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(5));
        var output = await stdout;
        var marker = output.LastIndexOf("@@ORACLE@@", StringComparison.Ordinal);
        if (p.ExitCode != 0 || marker < 0) throw new InvalidOperationException($"ducklake_oracle.py failed ({p.ExitCode}): {await stderr}");
        return JsonNode.Parse(output[(marker + "@@ORACLE@@".Length)..])!;
    }
}
