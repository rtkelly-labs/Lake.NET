using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Server-side scan planning against a server that implements it (Apache Gravitino's Iceberg REST service): tables we
/// write through it with every delete mechanism are planned by the server and read with the deletes it matched.
/// </summary>
[Trait("Category", "Regression")]
[Spec("rest: planTableScan")]
[Spec("Scan Planning")]
public sealed class ServerScanPlanningLiveTests
{
    private static readonly Schema Keyed = new(0, CatalogConformanceTests.Schema.Columns, identifierFieldIds: [1]);

    public static TheoryData<int, DeleteStrategy> Matrix() => new()
    {
        { 2, DeleteStrategy.CopyOnWrite }, { 2, DeleteStrategy.PositionDeletes }, { 2, DeleteStrategy.EqualityDeletes },
        { 3, DeleteStrategy.PositionDeletes }, { 3, DeleteStrategy.EqualityDeletes },
    };

    [SkippableTheory]
    [MemberData(nameof(Matrix))]
    public async Task Gravitino_plans_our_tables_and_we_read_its_plans(int formatVersion, DeleteStrategy strategy)
    {
        Require.Catalog("gravitino");
        var env = TestEnvironment.Current;
        using var catalog = (RestCatalog)await env.CatalogAsync("gravitino");
        Assert.True(catalog.SupportsScanPlanning);
        var ns = Namespace.Of(env.NewNamespace("plan"));
        await catalog.CreateNamespaceAsync(ns);
        try
        {
            var t = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, "t"), Keyed, CatalogConformanceTests.Spec, FormatVersion: formatVersion));
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(0, 30));
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(30, 20));
            var key = strategy == DeleteStrategy.EqualityDeletes ? new[] { "id", "event_ts", "region" } : ["id"];
            await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(5, 5).Select(r => { r[1] = $"upserted-{r[0]}"; return r; }).ToList(), key, strategy);
            await TableMutations.DeleteByKeysAsync(t, key, CatalogConformanceTests.Rows(12, 3).Select(r => key.Length == 1 ? new object?[] { r[0] } : [r[0], r[3], r[4]]), strategy);
            t = await catalog.LoadTableAsync(t.Identifier);

            foreach (var filter in new Expression?[] { null, Expr.LessThan("id", 20L), Expr.Equal("region", "eu"), Expr.And(Expr.GreaterThanOrEqual("id", 10L), Expr.NotEqual("region", "us")) })
            {
                var plan = await catalog.PlanTableScanAsync(t, new ScanPlanRequest(filter));
                // Every DV the client applies is for the task's own file (Gravitino's misattributed references are repaired).
                Assert.All(plan.Tasks, x => Assert.All(x.Deletes.Where(d => d.FileFormat == "PUFFIN"), d => Assert.Equal(x.DataFile.FilePath, d.ReferencedDataFile)));
                var theirs = await ServerSideScan.ReadAsync(t, plan, filter);
                var ours = filter is null ? await TableScan.ReadAllAsync(t) : await TableScan.ReadAsync(t, filter);
                Assert.Equal(SparkJson.Rows(t.Schema, ours).OrderBy(kv => kv.Key), SparkJson.Rows(t.Schema, theirs).OrderBy(kv => kv.Key));
                if (strategy != DeleteStrategy.CopyOnWrite && filter is null) Assert.Contains(plan.Tasks, x => x.Deletes.Count > 0);
            }
        }
        finally
        {
            foreach (var id in await catalog.ListTablesAsync(ns)) await catalog.DropTableAsync(id, purge: true);
            await catalog.DropNamespaceAsync(ns);
        }
    }
}
