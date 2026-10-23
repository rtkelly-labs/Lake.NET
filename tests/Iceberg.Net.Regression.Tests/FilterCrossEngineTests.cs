using System.Globalization;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Filtered reads agree with PyIceberg's <c>scan(row_filter=…)</c>, an independent implementation of binding,
/// projection through day/bucket/identity transforms, metrics pruning and row filtering.
/// Negations are only generated over non-null columns: Java (and we) treat <c>null != x</c> as true, while PyIceberg
/// filters rows with Arrow's three-valued logic and drops them — a known semantic difference, not a pruning bug.
/// </summary>
[Trait("Category", "Regression")]
[Spec("Scan Planning")]
[Spec("Partition Transforms")]
public sealed class FilterCrossEngineTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-filter", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public FilterCrossEngineTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private sealed record Filter(Expression Ours, JsonObject Json, string Text);

    private static readonly DateTimeOffset Base = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static Filter Leaf(Random rnd)
    {
        var column = new[] { "id", "name", "amount", "event_ts", "region", "qty" }[rnd.Next(6)];
        object Lit() => column switch
        {
            "id" => (long)rnd.Next(-3, 125),
            "name" => $"name-{rnd.Next(0, 130)}" is var n ? n[..Math.Min(n.Length, rnd.Next(5, 9))] : "",
            "amount" => rnd.Next(-5, 190) * 1.0,
            "event_ts" => Base.AddHours(rnd.Next(-10, 620)).ToString("yyyy-MM-ddTHH:mm:sszzz", CultureInfo.InvariantCulture),
            "region" => new[] { "eu", "us", "apac" }[rnd.Next(3)],
            _ => rnd.Next(-1, 11),
        };
        JsonNode ToJson(object v) => v switch { long l => l, int i => i, double d => d, string s => s, _ => throw new ArgumentException() };
        var nullable = column == "name";
        var ops = nullable ? new[] { "lt", "lt_eq", "gt", "gt_eq", "eq", "in", "starts_with", "is_null", "not_null" }
            : new[] { "lt", "lt_eq", "gt", "gt_eq", "eq", "not_eq", "in", "not_in" };
        var op = ops[rnd.Next(ops.Length)];
        if (op is "is_null" or "not_null")
            return new Filter(op == "is_null" ? Expr.IsNull(column) : Expr.NotNull(column), new JsonObject { ["op"] = op, ["term"] = column }, $"{column} {op}");
        if (op is "in" or "not_in")
        {
            var values = Enumerable.Range(0, rnd.Next(1, 4)).Select(_ => Lit()).Distinct().ToList();
            var e = op == "in" ? Expr.In(column, values) : Expr.NotIn(column, values);
            return new Filter(e, new JsonObject { ["op"] = op, ["term"] = column, ["values"] = new JsonArray(values.Select(ToJson).ToArray()) }, e.ToString()!);
        }
        var lit = Lit();
        if (op == "starts_with") lit = ((string)lit)[..Math.Min(((string)lit).Length, rnd.Next(1, 8))];
        Expression ours = op switch
        {
            "lt" => Expr.LessThan(column, lit), "lt_eq" => Expr.LessThanOrEqual(column, lit),
            "gt" => Expr.GreaterThan(column, lit), "gt_eq" => Expr.GreaterThanOrEqual(column, lit),
            "eq" => Expr.Equal(column, lit), "not_eq" => Expr.NotEqual(column, lit),
            _ => Expr.StartsWith(column, (string)lit),
        };
        var filter = new Filter(ours, new JsonObject { ["op"] = op, ["term"] = column, ["value"] = ToJson(lit) }, ours.ToString()!);
        return !nullable && rnd.Next(5) == 0
            ? new Filter(Expr.Not(filter.Ours), new JsonObject { ["op"] = "not", ["child"] = filter.Json }, $"not({filter.Text})")
            : filter;
    }

    private static Filter Random(Random rnd, int depth)
    {
        if (depth == 0 || rnd.Next(3) > 0) return Leaf(rnd);
        var (l, r) = (Random(rnd, depth - 1), Random(rnd, depth - 1));
        var and = rnd.Next(2) == 0;
        return new Filter(and ? Expr.And(l.Ours, r.Ours) : Expr.Or(l.Ours, r.Ours),
            new JsonObject { ["op"] = and ? "and" : "or", ["left"] = l.Json, ["right"] = r.Json }, $"({l.Text} {(and ? "and" : "or")} {r.Text})");
    }

    [SkippableTheory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Filtered_reads_match_pyiceberg(int formatVersion)
    {
        Require.LocalOracle();
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"f{formatVersion}"), CatalogConformanceTests.Schema, CatalogConformanceTests.Spec, FormatVersion: formatVersion));
        for (var b = 0; b < 4; b++) await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(b * 30, 30));
        await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 9 == 4, DeleteStrategy.PositionDeletes);

        var seed = int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_PBT_SEED"), out var s) ? s : Environment.TickCount;
        var rnd = new Random(seed);
        var ourPlanned = 0;
        var theirPlanned = 0;
        for (var run = 0; run < 25; run++)
        {
            var f = Random(rnd, 2);
            var ours = (await TableScan.ReadAsync(t, f.Ours)).Select(r => (long)r[0]!).Order().ToList();
            var py = await TestEnvironment.Current.OracleAsync("read-filter", t.MetadataLocation!, f.Json.ToJsonString());
            var theirs = py["ids"]!.AsArray().Select(i => i!.GetValue<long>()).ToList();
            Assert.True(ours.SequenceEqual(theirs), $"seed {seed}, filter {f.Text}: ours [{string.Join(",", ours)}] PyIceberg [{string.Join(",", theirs)}]");
            ourPlanned += (await TableScan.PlanAsync(t, f.Ours)).Tasks.Count;
            theirPlanned += py["files_planned"]!.GetValue<int>();
        }
        // Informational: both prune; neither may return different rows.
        Console.WriteLine($"v{formatVersion}: files planned — iceberg-dotnet {ourPlanned}, PyIceberg {theirPlanned}");
    }
}
