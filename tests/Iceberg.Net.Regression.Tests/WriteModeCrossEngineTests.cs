using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Every write mode × delete strategy produces a table PyIceberg reads identically — except equality deletes,
/// which PyIceberg 0.12 does not implement and must refuse rather than return deleted rows.
/// </summary>
[Spec("Row-level Deletes")]
[Trait("Category", "Regression")]
public sealed class WriteModeCrossEngineTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-wm", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;
    private int _n;

    private static readonly Schema Schema = new(0, CatalogConformanceTests.Schema.Columns, identifierFieldIds: [1]);

    public WriteModeCrossEngineTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    public static TheoryData<int, DeleteStrategy> Matrix() => new()
    {
        { 2, DeleteStrategy.CopyOnWrite }, { 2, DeleteStrategy.PositionDeletes },
        { 3, DeleteStrategy.CopyOnWrite }, { 3, DeleteStrategy.PositionDeletes },
    };

    private async Task<Table> Seeded(int formatVersion)
    {
        // Unpartitioned so an "id" key also works for equality deletes.
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t{_n++}"), Schema, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(0, 30));
        return t;
    }

    private static async Task<Dictionary<long, string?>> PyIcebergNames(Table t)
    {
        var py = await TestEnvironment.Current.OracleAsync("read-metadata", t.MetadataLocation!);
        return py["rows"]!.AsArray().ToDictionary(r => r!["id"]!.GetValue<long>(), r => r!["name"]?.GetValue<string>());
    }

    private static async Task<Dictionary<long, string?>> OurNames(Table t) =>
        (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => (string?)r[1]);

    [SkippableTheory]
    [MemberData(nameof(Matrix))]
    public async Task Upsert_replace_and_delete_read_identically_in_pyiceberg(int formatVersion, DeleteStrategy strategy)
    {
        Require.LocalOracle();
        var t = await Seeded(formatVersion);
        await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(25, 10).Select(r => { r[1] = $"upserted-{r[0]}"; return r; }).ToList(), strategy: strategy);
        var group = CatalogConformanceTests.Rows(900, 2).Select(r => { r[4] = "eu"; return r; }).ToList();
        await TableMutations.ReplaceGroupsAsync(t, group, ["region"], strategy);
        await TableMutations.DeleteWhereAsync(t, row => (long)row[0]! % 7 == 1, strategy);

        var ours = await OurNames(t);
        Assert.Contains(900L, ours.Keys);
        Assert.DoesNotContain(0L, ours.Keys); // id 0 was "eu", replaced with the group
        Assert.Equal(ours, await PyIcebergNames(t));
    }

    [SkippableFact]
    public async Task Pyiceberg_refuses_equality_deletes_instead_of_returning_deleted_rows()
    {
        Require.LocalOracle();
        var t = await Seeded(2);
        await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(5, 1).Select(r => { r[1] = "new"; return r; }).ToList(), ["id"], DeleteStrategy.EqualityDeletes);
        Assert.Equal("new", (await OurNames(t))[5]);
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => PyIcebergNames(t));
        Assert.Contains("equality deletes", e.Message);
    }
}
