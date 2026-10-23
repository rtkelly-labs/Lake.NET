using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Reads, validates and writes on top of tables written by Spark 4 + Iceberg Java (the reference implementation;
/// regenerate with interop/spark_oracle.py fixtures). These are merge-on-read tables: v2 with position delete files,
/// v3 with deletion vectors and row lineage, each after DELETE, UPDATE and a second DELETE on the same files.
/// </summary>
public sealed class SparkFixtureTests : IDisposable
{
    private const string FixtureRoot = "file:///tmp/sentric-spark-fixture-root/warehouse";
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "fixtures", "spark");
    private static readonly JsonNode Expected = JsonNode.Parse(File.ReadAllText(Path.Combine(Dir, "expected.json")))!;

    private readonly string _copy = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-spark", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public SparkFixtureTests()
    {
        PrefixMappingFileIO.CopyTree(Dir, _copy);
        _catalog = new InMemoryCatalog(Path.Combine(_copy, "catalog"), new PrefixMappingFileIO(FixtureRoot, _copy));
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_copy)) Directory.Delete(_copy, true);
    }

    public static TheoryData<string> Tables() => new() { "mor_v2", "mor_v3" };

    private async Task<Table> Load(string name)
    {
        var ns = Namespace.Of("ref");
        if (!await _catalog.NamespaceExistsAsync(ns)) await _catalog.CreateNamespaceAsync(ns);
        return await _catalog.RegisterTableAsync(new TableIdentifier(ns, name), Expected[name]!["metadata_location"]!.GetValue<string>());
    }

    private static Dictionary<long, string?> ByKey(IEnumerable<object?[]> rows) => rows.ToDictionary(r => (long)r[0]!, r => (string?)r[1]);

    [Theory]
    [MemberData(nameof(Tables))]
    public async Task Reads_the_same_rows_as_spark(string name)
    {
        var table = await Load(name);
        var expected = Expected[name]!;
        Assert.Equal(expected["format_version"]!.GetValue<int>(), table.Metadata.FormatVersion);

        var rows = await TableScan.ReadAllAsync(table);
        var want = expected["rows"]!.AsArray().ToDictionary(r => r!["id"]!.GetValue<long>(), r => r!["name"]?.GetValue<string>());
        Assert.Equal(want.OrderBy(kv => kv.Key), ByKey(rows).OrderBy(kv => kv.Key));
    }

    [Theory]
    [MemberData(nameof(Tables))]
    public async Task Spark_tables_pass_deep_validation(string name)
    {
        var table = await Load(name);
        var report = await TableValidator.ValidateAsync(table, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());
    }

    [Fact]
    public async Task Row_lineage_matches_spark()
    {
        var table = await Load("mor_v3");
        var actual = (await TableScan.ReadWithLineageAsync(table)).ToDictionary(r => (long)r.Row[0]!, r => (r.RowId, r.LastUpdatedSequenceNumber));
        var expected = Expected["mor_v3"]!["row_lineage"]!.AsArray().ToDictionary(
            r => r!["id"]!.GetValue<long>(),
            r => ((long?)r!["row_id"]!.GetValue<long>(), (long?)r["last_updated_seq"]!.GetValue<long>()));
        Assert.Equal(expected.OrderBy(kv => kv.Key), actual.OrderBy(kv => kv.Key));
    }

    [Theory]
    [InlineData("mor_v2", DeleteStrategy.CopyOnWrite)]
    [InlineData("mor_v2", DeleteStrategy.PositionDeletes)]
    [InlineData("mor_v2", DeleteStrategy.EqualityDeletes)]
    [InlineData("mor_v3", DeleteStrategy.CopyOnWrite)]
    [InlineData("mor_v3", DeleteStrategy.PositionDeletes)]
    [InlineData("mor_v3", DeleteStrategy.EqualityDeletes)]
    public async Task Our_writes_on_top_of_spark_tables_stay_consistent(string name, DeleteStrategy strategy)
    {
        var table = await Load(name);
        var model = ByKey(await TableScan.ReadAllAsync(table));

        // Rows 4 and 11 are in different partitions; row 1 was already updated by Spark.
        await TableMutations.UpsertAsync(table, [[4L, "ours", "eu"], [11L, "ours", "us"], [1L, "ours-again", "us"], [100L, "new", "eu"]], ["id", "region"], strategy);
        foreach (var (k, v) in new[] { (4L, "ours"), (11L, "ours"), (1L, "ours-again"), (100L, "new") }) model[k] = v;
        await TableMutations.DeleteByKeysAsync(table, ["id", "region"], [[6L, "eu"], [7L, "us"]], strategy);
        model.Remove(6); model.Remove(7);

        var reloaded = await _catalog.LoadTableAsync(table.Identifier);
        Assert.Equal(model.OrderBy(kv => kv.Key), ByKey(await TableScan.ReadAllAsync(reloaded)).OrderBy(kv => kv.Key));
        var report = await TableValidator.ValidateAsync(reloaded, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());
    }

    [Fact]
    public async Task Copy_on_write_preserves_spark_row_ids_for_untouched_rows()
    {
        var table = await Load("mor_v3");
        var before = (await TableScan.ReadWithLineageAsync(table)).ToDictionary(r => (long)r.Row[0]!);

        await TableMutations.DeleteByKeysAsync(table, ["id", "region"], [[4L, "eu"]], DeleteStrategy.CopyOnWrite);
        await TableMutations.UpsertAsync(table, [[8L, "changed", "eu"]], ["id", "region"], DeleteStrategy.CopyOnWrite);

        var reloaded = await _catalog.LoadTableAsync(table.Identifier);
        var after = (await TableScan.ReadWithLineageAsync(reloaded)).ToDictionary(r => (long)r.Row[0]!);
        Assert.DoesNotContain(4L, after.Keys);

        // Rewritten-but-unchanged rows keep both identity and last-updated sequence number.
        foreach (var (id, row) in after.Where(kv => kv.Key != 8))
        {
            Assert.Equal(before[id].RowId, row.RowId);
            Assert.Equal(before[id].LastUpdatedSequenceNumber, row.LastUpdatedSequenceNumber);
        }
        // The upserted row is a new row version: new row ID, current sequence number.
        var seq = reloaded.Metadata.LastSequenceNumber;
        Assert.Equal(seq, after[8].LastUpdatedSequenceNumber);
        Assert.DoesNotContain(after[8].RowId, before.Values.Select(r => r.RowId));
        Assert.Equal(after.Count, after.Values.Select(r => r.RowId).Distinct().Count());
    }
}
