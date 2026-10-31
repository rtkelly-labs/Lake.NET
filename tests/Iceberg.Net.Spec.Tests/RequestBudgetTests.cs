using System.Text.Json;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Iceberg.Net.Partitioning;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Storage requests per operation, gated exactly (docs/lake/verification/benchmarks.md: counts are deterministic,
/// wall-clock time is not). Every scenario's counts must equal <c>docs/lake/verification/request-budgets.json</c>; a
/// change that adds a request fails here until the budget is updated in the same pull request, with the reason in its
/// description. Regenerate: <c>LAKE_UPDATE_BUDGETS=1 dotnet test --filter RequestBudgetTests</c>.
/// </summary>
public sealed class RequestBudgetTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "lake-budgets", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "day", PrimitiveType.Date),
    ], identifierFieldIds: [1]);

    private static List<object?[]> Rows(int start, int count) =>
        Enumerable.Range(start, count).Select(i => new object?[] { (long)i, $"n{i}", 19_700 + i % 4 }).ToList();

    private static string BudgetFile()
    {
        for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
            if (File.Exists(Path.Combine(d.FullName, "Iceberg.Net.slnx")))
                return Path.Combine(d.FullName, "docs", "lake", "verification", "request-budgets.json");
        throw new InvalidOperationException("Iceberg.Net.slnx not found");
    }

    /// <summary>Each scenario: the operation measured runs after <c>setup</c>, on a fresh table.</summary>
    private async Task<IReadOnlyDictionary<string, long>> MeasureAsync(int formatVersion, Func<Table, Task> setup, Func<Table, Task> operation)
    {
        var metered = new MeteredFileIO(new LocalFileIO());
        using var catalog = new InMemoryCatalog(Path.Combine(_warehouse, Guid.NewGuid().ToString("N")), metered);
        await catalog.CreateNamespaceAsync(Namespace.Of("b"));
        var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("b"), "t"), Schema,
            PartitionSpec.BuilderFor(Schema).Bucket("id", 4).Build(), FormatVersion: formatVersion));
        await setup(table);
        table = await catalog.LoadTableAsync(table.Identifier);
        metered.Reset();
        await operation(table);
        return metered.Counts;
    }

    private static Func<Table, Task> Appends(int n) => async t =>
    {
        for (var i = 0; i < n; i++) await TableMutations.InsertAsync(t, Rows(i * 100, 100));
    };

    private async Task<Dictionary<string, IReadOnlyDictionary<string, long>>> ScenariosAsync() => new()
    {
        ["append_100_rows_4_buckets_v2"] = await MeasureAsync(2, Appends(0), t => TableMutations.InsertAsync(t, Rows(0, 100))),
        ["append_after_10_appends_v2"] = await MeasureAsync(2, Appends(10), t => TableMutations.InsertAsync(t, Rows(5000, 100))),
        ["full_scan_10_appends_v2"] = await MeasureAsync(2, Appends(10), async t => await TableScan.ReadAllAsync(t)),
        ["pruned_scan_one_bucket_10_appends_v2"] = await MeasureAsync(2, Appends(10), async t =>
        {
            await foreach (var _ in TableScan.ReadRowsAsync(t, new ScanRequest { Filter = Expr.Equal("id", 5L) })) { }
        }),
        ["upsert_10_keys_position_deletes_v2"] = await MeasureAsync(2, Appends(4), t =>
            TableMutations.UpsertAsync(t, Rows(10, 10), ["id"], DeleteStrategy.PositionDeletes)),
        ["upsert_10_keys_deletion_vectors_v3"] = await MeasureAsync(3, Appends(4), t =>
            TableMutations.UpsertAsync(t, Rows(10, 10), ["id"], DeleteStrategy.PositionDeletes)),
        ["upsert_10_keys_equality_deletes_v2"] = await MeasureAsync(2, Appends(4), t =>
            TableMutations.UpsertAsync(t, Rows(10, 10), ["id"], DeleteStrategy.EqualityDeletes)),
        ["expire_9_of_10_snapshots_v2"] = await MeasureAsync(2, Appends(10), t =>
            Maintenance.ExpireSnapshotsAsync(t, olderThanMs: long.MaxValue, retainLast: 1)),
    };

    [Fact]
    public async Task Every_scenario_makes_exactly_its_budgeted_storage_requests()
    {
        var measured = await ScenariosAsync();
        var path = BudgetFile();
        if (Environment.GetEnvironmentVariable("LAKE_UPDATE_BUDGETS") == "1")
        {
            var doc = new JsonObject
            {
                ["$comment"] = "Storage requests per operation (RequestBudgetTests). Exact: update in the PR that changes them, and say why.",
                ["scenarios"] = new JsonObject(measured.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv =>
                    KeyValuePair.Create(kv.Key, (JsonNode?)new JsonObject(kv.Value.Select(c => KeyValuePair.Create(c.Key, (JsonNode?)c.Value)))))),
            };
            await File.WriteAllTextAsync(path, doc.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) + "\n");
            return;
        }

        var budgets = JsonNode.Parse(await File.ReadAllTextAsync(path))!["scenarios"]!.AsObject();
        var problems = new List<string>();
        foreach (var (name, counts) in measured)
        {
            if (budgets[name] is not JsonObject budget)
            {
                problems.Add($"{name}: no budget (run with LAKE_UPDATE_BUDGETS=1 and commit the file)");
                continue;
            }
            foreach (var kind in counts.Keys.Union(budget.Select(b => b.Key)).Order(StringComparer.Ordinal))
            {
                var want = budget[kind]?.GetValue<long>() ?? 0;
                var got = counts.GetValueOrDefault(kind);
                if (want != got) problems.Add($"{name}: {kind} {got} (budget {want})");
            }
        }
        foreach (var (name, _) in budgets)
            if (!measured.ContainsKey(name)) problems.Add($"{name}: budgeted but no longer measured");
        Assert.True(problems.Count == 0, string.Join("\n", problems));
    }

    [Fact]
    public async Task The_counts_are_deterministic()
    {
        var a = await MeasureAsync(2, Appends(3), async t => await TableScan.ReadAllAsync(t));
        var b = await MeasureAsync(2, Appends(3), async t => await TableScan.ReadAllAsync(t));
        Assert.Equal(a, b);
    }
}
