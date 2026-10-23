using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// UPDATE and MERGE output (the same operations the Spark fixture builds, see <see cref="SparkTablesFixture"/>) reads
/// identically in PyIceberg, column by column, on v2/v3 × copy-on-write/position deletes, partitioned and not.
/// </summary>
[Spec("Row-level Deletes")]
[Trait("Category", "Regression")]
public sealed class UpdateMergeCrossEngineTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-um", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;
    private int _n;

    private static readonly Schema Schema = new(0, CatalogConformanceTests.Schema.Columns, identifierFieldIds: [1]);

    public UpdateMergeCrossEngineTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    public static TheoryData<int, DeleteStrategy, bool> Matrix()
    {
        var data = new TheoryData<int, DeleteStrategy, bool>();
        foreach (var v in new[] { 2, 3 })
            foreach (var s in new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes })
                foreach (var partitioned in new[] { false, true })
                    data.Add(v, s, partitioned);
        return data;
    }

    private async Task<Table> Seeded(int formatVersion, bool partitioned)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t{_n++}"), Schema,
            partitioned ? CatalogConformanceTests.Spec : null, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(0, 30));
        return t;
    }

    private static void AssertSameRows(Dictionary<long, object?[]> ours, JsonArray theirs)
    {
        var py = theirs.ToDictionary(r => r!["id"]!.GetValue<long>(), r => r!.AsObject());
        Assert.Equal(ours.Keys.Order(), py.Keys.Order());
        foreach (var (id, row) in ours)
        {
            var p = py[id];
            Assert.Equal((string?)row[1], p["name"]?.GetValue<string>());
            Assert.Equal((double?)row[2], p["amount"]?.GetValue<double>());
            Assert.Equal((long?)row[3], p["event_ts"]?.GetValue<long>());
            Assert.Equal((string?)row[4], p["region"]?.GetValue<string>());
            Assert.Equal(row[5] is null ? null : Convert.ToInt64(row[5]), p["qty"]?.GetValue<long>());
        }
    }

    private async Task AssertPyIcebergAgrees(Table t)
    {
        t = await _catalog.LoadTableAsync(t.Identifier);
        await TableValidator.AssertValidAsync(t, new ValidationOptions { Deep = true, AllSnapshots = true });
        var ours = (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!);
        var py = await TestEnvironment.Current.OracleAsync("read-metadata", t.MetadataLocation!);
        AssertSameRows(ours, py["rows"]!.AsArray());
    }

    [SkippableTheory]
    [MemberData(nameof(Matrix))]
    public async Task Updates_read_identically_in_pyiceberg(int formatVersion, DeleteStrategy strategy, bool partitioned)
    {
        Require.LocalOracle();
        var t = await Seeded(formatVersion, partitioned);
        var r = await TableMutations.UpdateAsync(t, Expr.LessThan("id", 6L), u => u.Set("region", "apac").Set("name", row => $"moved-{row[0]}"), strategy);
        Assert.Equal(6, r.RowsUpdated);
        await TableMutations.UpdateAsync(t, Expr.Equal("region", "eu"), u => u.Set("qty", row => row[5] is int q ? q + 100 : null), strategy);
        await AssertPyIcebergAgrees(t);
    }

    [SkippableTheory]
    [MemberData(nameof(Matrix))]
    public async Task Merges_read_identically_in_pyiceberg(int formatVersion, DeleteStrategy strategy, bool partitioned)
    {
        Require.LocalOracle();
        var t = await Seeded(formatVersion, partitioned);
        var source = CatalogConformanceTests.Rows(20, 20).Select(row => { row[1] = $"src-{row[0]}"; return row; }).ToList();
        var r = await TableMutations.MergeAsync(t, source, ["id"], SparkTablesFixture.MergeClauses, strategy);
        // 20..29 match: 20 (qty 0) is deleted, 21..29 updated; 33..39 (qty > 2) inserted; the 10 odd ids < 20 are "us"
        // rows not in the source and are deleted.
        Assert.Equal((9L, 11L, 7L), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        await AssertPyIcebergAgrees(t);
    }
}
