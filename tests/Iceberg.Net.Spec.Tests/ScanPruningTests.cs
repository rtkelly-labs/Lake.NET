using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;
using Xunit.Abstractions;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Pruning must never change results: for random filters over tables with every kind of partition transform, a
/// pruned read returns exactly what a full read filtered row by row returns — and it does prune.
/// ICEBERG_PBT_RUNS (default 40) × 2 filters per layout and format version; ICEBERG_PBT_SEED reproduces a run.
/// </summary>
[Spec("Scan Planning")]
[Spec("Partition Transforms")]
[Spec("Field-level Metrics and Statistics")]
public sealed class ScanPruningTests(ITestOutputHelper output) : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-prune", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "score", PrimitiveType.Double),
        NestedField.Optional(4, "ts", PrimitiveType.TimestampTz),
        NestedField.Optional(5, "n", PrimitiveType.Int),
        NestedField.Optional(6, "amount", new DecimalType(9, 2)),
        NestedField.Optional(7, "day", PrimitiveType.Date),
    ], identifierFieldIds: [1]);

    private const long BaseTs = 1_700_000_000_000_000L;
    private const long Hour = 3_600_000_000L;

    private static object?[] Row(int i) =>
    [
        (long)i,
        i % 9 == 0 ? null : $"{(char)('a' + i % 5)}{(char)('a' + i % 3)}-{i % 13}",
        i % 10 == 0 ? double.NaN : i % 11 == 0 ? null : i * 1.5 - 30,
        i % 15 == 0 ? null : BaseTs + i * 7 * Hour,
        i % 17 == 0 ? null : i % 23 - 5,
        i % 19 == 0 ? null : new IcebergDecimal(i * 37 - 1000, 2),
        19_700 + i / 8,
    ];

    public static TheoryData<string> Layouts() => new() { "unpartitioned", "identity", "day+bucket", "truncate", "month+truncate-int" };

    private static PartitionSpec SpecFor(string layout) => layout switch
    {
        "unpartitioned" => PartitionSpec.Unpartitioned,
        "identity" => PartitionSpec.BuilderFor(Schema).Identity("n").Build(),
        "day+bucket" => PartitionSpec.BuilderFor(Schema).Day("ts").Bucket("id", 4).Build(),
        "truncate" => PartitionSpec.BuilderFor(Schema).Truncate("name", 1).Identity("day").Build(),
        _ => PartitionSpec.BuilderFor(Schema).Month("ts").Truncate("n", 5).Build(),
    };

    private async Task<Table> Build(string layout, int formatVersion)
    {
        var catalog = new InMemoryCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("p"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("p", $"{layout}_v{formatVersion}".Replace('+', '_')), Schema, SpecFor(layout), FormatVersion: formatVersion));
        // Several commits so there are several manifests and files per partition, sorted by id within each batch.
        for (var b = 0; b < 4; b++)
            await TableMutations.InsertAsync(t, Enumerable.Range(b * 30, 30).Select(Row).ToList());
        await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 7 == 3, DeleteStrategy.PositionDeletes);
        return t;
    }

    private static Expression RandomExpression(Random rnd, int depth)
    {
        if (depth > 0 && rnd.Next(3) == 0)
        {
            var l = RandomExpression(rnd, depth - 1);
            var r = RandomExpression(rnd, depth - 1);
            return rnd.Next(3) switch { 0 => Expr.And(l, r), 1 => Expr.Or(l, r), _ => Expr.Not(l) };
        }
        var column = new[] { "id", "name", "score", "ts", "n", "amount", "day" }[rnd.Next(7)];
        if (rnd.Next(8) == 0) return rnd.Next(2) == 0 ? Expr.IsNull(column) : Expr.NotNull(column);
        if (column == "score" && rnd.Next(5) == 0) return rnd.Next(2) == 0 ? Expr.IsNaN(column) : Expr.NotNaN(column);
        object Lit() => column switch
        {
            "id" => (long)rnd.Next(-5, 125),
            "name" => $"{(char)('a' + rnd.Next(6))}{(char)('a' + rnd.Next(4))}-{rnd.Next(14)}"[..rnd.Next(1, 5)],
            "score" => rnd.Next(-40, 160) + 0.5 * rnd.Next(2),
            "ts" => BaseTs + rnd.Next(-20, 900) * Hour + rnd.Next(-1, 2),
            "n" => rnd.Next(-8, 20),
            "amount" => new IcebergDecimal(rnd.Next(-1100, 3500), 2),
            _ => 19_700 + rnd.Next(-2, 17),
        };
        if (column == "name" && rnd.Next(4) == 0)
            return rnd.Next(2) == 0 ? Expr.StartsWith(column, (string)Lit()) : Expr.NotStartsWith(column, (string)Lit());
        return rnd.Next(8) switch
        {
            0 => Expr.LessThan(column, Lit()),
            1 => Expr.LessThanOrEqual(column, Lit()),
            2 => Expr.GreaterThan(column, Lit()),
            3 => Expr.GreaterThanOrEqual(column, Lit()),
            4 => Expr.Equal(column, Lit()),
            5 => Expr.NotEqual(column, Lit()),
            6 => Expr.In(column, Enumerable.Range(0, rnd.Next(1, 5)).Select(_ => Lit())),
            _ => Expr.NotIn(column, Enumerable.Range(0, rnd.Next(1, 4)).Select(_ => Lit())),
        };
    }

    [Theory]
    [MemberData(nameof(Layouts))]
    public async Task Pruned_reads_return_exactly_the_matching_rows(string layout)
    {
        var runs = int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_PBT_RUNS"), out var n) ? n : 40;
        var seed = int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_PBT_SEED"), out var s) ? s : Environment.TickCount;
        var rnd = new Random(seed);
        foreach (var v in new[] { 2, 3 })
        {
            var t = await Build(layout, v);
            var all = await TableScan.ReadAllAsync(t);
            var allFiles = (await TableScan.PlanAsync(t, Expression.True)).Tasks.Count;
            long planned = 0, skipped = 0;
            for (var run = 0; run < runs * 2; run++)
            {
                var filter = RandomExpression(rnd, 2);
                var matches = RowEvaluator.Compile(Binder.Bind(Schema, filter));
                var expected = all.Where(r => matches(r)).Select(r => (long)r[0]!).Order().ToList();
                var actual = (await TableScan.ReadAsync(t, filter)).Select(r => (long)r[0]!).Order().ToList();
                if (!expected.SequenceEqual(actual))
                    Assert.Fail($"{layout} v{v} (ICEBERG_PBT_SEED={seed}) filter {filter}: expected [{string.Join(",", expected)}] got [{string.Join(",", actual)}]");
                var plan = await TableScan.PlanAsync(t, filter);
                planned += plan.Tasks.Count;
                skipped += allFiles - plan.Tasks.Count;
            }
            output.WriteLine($"{layout} v{v}: {allFiles} files; filters planned {planned} files and skipped {skipped}");
            Assert.True(skipped > 0, $"{layout}: random filters never pruned a file");
        }
    }

    [Fact]
    public async Task Selective_filters_skip_manifests_and_files()
    {
        var t = await Build("day+bucket", 2);
        var everything = await TableScan.PlanAsync(t, Expression.True);
        // ids 30–59 were the second commit (their own manifest); one id is in one bucket and one day.
        var plan = await TableScan.PlanAsync(t, Expr.Equal("id", 42L));
        Assert.True(plan.ManifestsSkipped + plan.FilesSkippedByPartition + plan.FilesSkippedByMetrics > 0);
        Assert.True(plan.Tasks.Count < everything.Tasks.Count / 4, $"{plan.Tasks.Count} of {everything.Tasks.Count} files planned");
        Assert.Single(await TableScan.ReadAsync(t, Expr.Equal("id", 42L)));

        var dayRange = await TableScan.PlanAsync(t, Expr.And(
            Expr.GreaterThanOrEqual("ts", BaseTs + 100 * 7 * Hour), Expr.LessThan("ts", BaseTs + 101 * 7 * Hour)));
        Assert.True(dayRange.FilesSkippedByPartition > 0);
        Assert.True(dayRange.Tasks.Count <= 4);
    }

    [Theory]
    [InlineData(DeleteStrategy.CopyOnWrite)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    public async Task Expression_deletes_only_touch_files_that_can_match(DeleteStrategy strategy)
    {
        var t = await Build("day+bucket", 3);
        var before = await TableScan.ReadAllAsync(t);
        var filter = Expr.And(Expr.GreaterThanOrEqual("id", 60L), Expr.LessThan("id", 64L));
        var result = await TableMutations.DeleteWhereAsync(t, filter, strategy);
        var after = await TableScan.ReadAllAsync(t);
        var matches = RowEvaluator.Compile(Binder.Bind(Schema, filter));
        Assert.Equal(before.Where(r => !matches(r)).Select(r => (long)r[0]!).Order(), after.Select(r => (long)r[0]!).Order());
        Assert.Equal(before.Count(r => matches(r)), result.RowsDeleted);
        if (strategy == DeleteStrategy.CopyOnWrite) Assert.True(result.DataFilesRewritten <= 4, $"{result.DataFilesRewritten} files rewritten for 4 rows");
        await TableValidator.AssertValidAsync(t, new ValidationOptions { Deep = true });
    }
}
