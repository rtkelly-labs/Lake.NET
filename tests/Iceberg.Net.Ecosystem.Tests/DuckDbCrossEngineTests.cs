using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.Ecosystem.Tests;

/// <summary>
/// DuckDB's <c>iceberg</c> extension (interop/duckdb_oracle.py) as another independent reader. Runs with the Python
/// in <c>ICEBERG_DUCKDB_PYTHON</c> (else <c>ICEBERG_PYTHON</c>) when it has <c>duckdb</c> and the extension loads;
/// skips otherwise, with the reason (offline machines cannot download the extension; see docs/spark-and-athena.md).
/// </summary>
public static class DuckDbOracle
{
    private static readonly Lazy<string?> Skip = new(Probe);
    public static string? SkipReason => Skip.Value;

    private static string? Python => Environment.GetEnvironmentVariable("ICEBERG_DUCKDB_PYTHON") is { Length: > 0 } p ? p
        : Environment.GetEnvironmentVariable("ICEBERG_PYTHON") is { Length: > 0 } q ? q : null;

    private static string? Probe()
    {
        if (Environment.GetEnvironmentVariable("ICEBERG_DUCKDB") == "0") return "DuckDB oracle disabled (ICEBERG_DUCKDB=0)";
        if (Python is null) return "DuckDB oracle unavailable: set ICEBERG_DUCKDB_PYTHON (or ICEBERG_PYTHON) to a Python with the interop 'duckdb' group";
        try
        {
            var probe = RunAsync(["probe"]).GetAwaiter().GetResult();
            return probe["available"]!.GetValue<bool>() ? null : $"DuckDB oracle unavailable: {probe["reason"]}";
        }
        catch (Exception e) when (e is InvalidOperationException or System.ComponentModel.Win32Exception or TimeoutException)
        {
            return $"DuckDB oracle unavailable: {e.Message}";
        }
    }

    /// <summary>Each table's columns and rows as DuckDB reads them, keyed by metadata location.</summary>
    public static async Task<JsonObject> ReadAsync(IEnumerable<string> metadataLocations) => (await RunAsync(["read", .. metadataLocations])).AsObject();

    private static async Task<JsonNode> RunAsync(IEnumerable<string> args)
    {
        var psi = new ProcessStartInfo(Python!) { RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add(Path.Combine(TestEnvironment.RepoDir(), "interop", "duckdb_oracle.py"));
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi) ?? throw new InvalidOperationException($"cannot start {Python}");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(5));
        var output = await stdout;
        var marker = output.LastIndexOf("@@ORACLE@@", StringComparison.Ordinal);
        if (p.ExitCode != 0 || marker < 0) throw new InvalidOperationException($"duckdb_oracle.py failed ({p.ExitCode}): {await stderr}");
        return JsonNode.Parse(output[(marker + "@@ORACLE@@".Length)..])!;
    }
}

public sealed class DuckDbCrossEngineTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", "duckdb-" + Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "score", PrimitiveType.Double),
        NestedField.Optional(4, "day", PrimitiveType.Date),
    ], identifierFieldIds: [1]);

    public DuckDbCrossEngineTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, true);
    }

    private static object?[] Row(long id, string? name) => [id, name, id * 0.5, (int)(20_000 + id)];

    /// <summary>Row text both sides agree on: id|name|score|yyyy-MM-dd.</summary>
    private static string Ours(object?[] r) =>
        $"{r[0]}|{r[1] ?? "null"}|{((double)r[2]!).ToString("R", CultureInfo.InvariantCulture)}|{DateOnly.FromDayNumber(DateOnly.FromDateTime(new DateTime(1970, 1, 1)).DayNumber + (int)r[3]!):yyyy-MM-dd}";

    private static string Theirs(JsonNode r) =>
        $"{r[0]!.GetValue<long>()}|{r[1]?.GetValue<string>() ?? "null"}|{r[2]!.GetValue<double>().ToString("R", CultureInfo.InvariantCulture)}|{r[3]!.GetValue<string>()}";

    [SkippableTheory]
    [InlineData(2, DeleteStrategy.CopyOnWrite)]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(2, DeleteStrategy.EqualityDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)] // deletion vectors
    public async Task DuckDb_reads_upserts_and_deletes_under_every_mechanism(int formatVersion, DeleteStrategy strategy)
    {
        // CI sets ICEBERG_DUCKDB_REQUIRED=1, where the extension downloads: a load failure there is a failure, not a skip.
        if (Environment.GetEnvironmentVariable("ICEBERG_DUCKDB_REQUIRED") == "1") Assert.True(DuckDbOracle.SkipReason is null, DuckDbOracle.SkipReason);
        Skip.If(DuckDbOracle.SkipReason is not null, DuckDbOracle.SkipReason);
        var table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t_v{formatVersion}_{strategy}".ToLowerInvariant()), Schema,
            FormatVersion: formatVersion));
        await TableMutations.InsertAsync(table, Enumerable.Range(0, 20).Select(i => Row(i, $"n{i}")).ToList());
        await TableMutations.InsertAsync(table, Enumerable.Range(20, 10).Select(i => Row(i, null)).ToList());
        await TableMutations.UpsertAsync(table, [Row(3, "three"), Row(25, "twenty-five"), Row(100, "new")], strategy: strategy);
        await TableMutations.DeleteByKeysAsync(table, ["id"], [[5L], [21L]], strategy);

        var expected = (await TableScan.ReadAllAsync(table)).Select(Ours).Order(StringComparer.Ordinal).ToList();
        Assert.Equal(29, expected.Count);
        var result = (await DuckDbOracle.ReadAsync([table.MetadataLocation!]))[table.MetadataLocation!]!;
        Assert.True(result["error"] is null, result["error"]?.ToString());
        Assert.Equal(["id", "name", "score", "day"], result["columns"]!.AsArray().Select(c => c!.GetValue<string>()));
        Assert.Equal(expected, result["rows"]!.AsArray().Select(r => Theirs(r!)).Order(StringComparer.Ordinal).ToList());
    }
}
