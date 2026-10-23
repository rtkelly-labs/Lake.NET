using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Views through REST servers (the Java reference server, Polaris), and Trino reading views we create and creating
/// views we read — the cross-engine contract a view exists for.
/// </summary>
[Trait("Category", "Regression")]
[Spec("rest: createView")]
[Spec("rest: loadView")]
[Spec("rest: replaceView")]
[Spec("rest: dropView")]
[Spec("rest: listViews")]
[Spec("rest: renameView")]
[Spec("rest: viewExists")]
public sealed class ViewConformanceTests
{
    public static TheoryData<string> Catalogs => new() { "rest", "polaris", "gravitino", "nessie", "jdbc" };

    private static readonly Schema Out = new(0, [NestedField.Optional(1, "region", PrimitiveType.String), NestedField.Optional(2, "n", PrimitiveType.Long)]);

    [SkippableTheory]
    [MemberData(nameof(Catalogs))]
    public async Task View_lifecycle(string kind)
    {
        Require.Catalog(kind);
        var env = TestEnvironment.Current;
        using var catalog = await env.CatalogAsync(kind);
        var views = (IViewCatalog)catalog;
        var ns = Namespace.Of(env.NewNamespace($"{kind}_views"));
        await catalog.CreateNamespaceAsync(ns);
        var id = new TableIdentifier(ns, "by_region");
        try
        {
            var created = await views.CreateViewAsync(id, new ViewDefinition(Out, "SELECT region, count(*) AS n FROM events GROUP BY 1", "trino", ns.Levels.ToList()));
            Assert.Equal(1, created.Metadata.CurrentVersionId);
            Assert.True(await views.ViewExistsAsync(id));
            Assert.Contains(id, await views.ListViewsAsync(ns));

            var replaced = await views.ReplaceViewAsync(id, new ViewDefinition(Out, "SELECT region, sum(qty) AS n FROM events GROUP BY 1", "trino", ns.Levels.ToList()));
            Assert.Equal(2, replaced.Metadata.CurrentVersionId);
            Assert.Contains("sum(qty)", (await views.LoadViewAsync(id)).Sql("trino"));
            Assert.Equal(created.Metadata.ViewUuid, replaced.Metadata.ViewUuid);

            var renamed = new TableIdentifier(ns, "by_region2");
            await views.RenameViewAsync(id, renamed);
            Assert.False(await views.ViewExistsAsync(id));
            await views.DropViewAsync(renamed);
            Assert.False(await views.ViewExistsAsync(renamed));
        }
        finally
        {
            foreach (var v in await views.ListViewsAsync(ns)) await views.DropViewAsync(v);
            await catalog.DropNamespaceAsync(ns);
        }
    }

    [SkippableFact]
    public async Task Trino_queries_our_view_and_we_read_trinos()
    {
        Require.Trino();
        var env = TestEnvironment.Current;
        using var catalog = await env.CatalogAsync("rest");
        var views = (IViewCatalog)catalog;
        var ns = Namespace.Of(env.NewNamespace("trino_views"));
        await catalog.CreateNamespaceAsync(ns);
        try
        {
            var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, "events"), CatalogConformanceTests.Schema, CatalogConformanceTests.Spec));
            await TableMutations.InsertAsync(table, CatalogConformanceTests.Rows(0, 30));
            var sql = $"SELECT region, count(*) AS n FROM {TrinoOracle.Table(ns.ToString(), "events")} GROUP BY region";
            await views.CreateViewAsync(new TableIdentifier(ns, "ours"), new ViewDefinition(Out, sql, "trino", ns.Levels.ToList()));

            var rows = (await TrinoOracle.QueryAsync($"SELECT region, n FROM {TrinoOracle.Table(ns.ToString(), "ours")} ORDER BY region")).Rows;
            Assert.Equal([("eu", 15L), ("us", 15L)], rows.Select(r => (r[0]!.GetValue<string>(), r[1]!.GetValue<long>())));

            await TrinoOracle.QueryAsync($"CREATE VIEW {TrinoOracle.Table(ns.ToString(), "theirs")} AS SELECT id FROM {TrinoOracle.Table(ns.ToString(), "events")} WHERE qty > 5");
            var theirs = await views.LoadViewAsync(new TableIdentifier(ns, "theirs"));
            Assert.Contains("qty > 5", theirs.Sql("trino"));
            Assert.Equal("id", theirs.Metadata.CurrentSchema.Columns.Single().Name);
        }
        finally
        {
            foreach (var v in await views.ListViewsAsync(ns)) await views.DropViewAsync(v);
            foreach (var t in await catalog.ListTablesAsync(ns)) await catalog.DropTableAsync(t, purge: true);
            await catalog.DropNamespaceAsync(ns);
        }
    }
}
