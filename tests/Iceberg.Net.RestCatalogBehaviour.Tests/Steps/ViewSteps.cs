using Iceberg.Net.Catalog;
using Reqnroll;
using Xunit;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

[Binding]
public sealed class ViewSteps(CatalogWorld world)
{
    public const string Dialect = "spark";

    /// <summary>A view over the scenarios' table shape, resolved against <paramref name="defaultNamespace"/>.</summary>
    public static ViewDefinition Definition(string sql, string defaultNamespace) =>
        new(CatalogWorld.Schema, sql, Dialect, CatalogWorld.Ns(defaultNamespace).Levels);

    private async Task CreateAsync(string name, string sql) =>
        await (await world.CatalogAsync()).CreateViewAsync(CatalogWorld.Id(name), Definition(sql, CatalogWorld.Id(name).Namespace.ToString()));

    [Given(@"^the view ""([^""]*)"" as ""([^""]*)""$")]
    [When(@"^I create the view ""([^""]*)"" as ""([^""]*)""$")]
    public Task Create(string name, string sql) => CreateAsync(name, sql);

    [When(@"^I try to create the view ""([^""]*)"" as ""([^""]*)""$")]
    public Task TryCreate(string name, string sql) => world.AttemptAsync(() => CreateAsync(name, sql));

    [When(@"^I replace the view ""([^""]*)"" with ""([^""]*)""$")]
    public async Task Replace(string name, string sql) =>
        await (await world.CatalogAsync()).ReplaceViewAsync(CatalogWorld.Id(name), Definition(sql, CatalogWorld.Id(name).Namespace.ToString()));

    [When(@"^I rename the view ""([^""]*)"" to ""([^""]*)""$")]
    public async Task Rename(string from, string to) =>
        await (await world.CatalogAsync()).RenameViewAsync(CatalogWorld.Id(from), CatalogWorld.Id(to));

    [When(@"^I try to rename the view ""([^""]*)"" to ""([^""]*)""$")]
    public Task TryRename(string from, string to) => world.AttemptAsync(() => Rename(from, to));

    [When(@"^I drop the view ""([^""]*)""$")]
    public async Task Drop(string name) => await (await world.CatalogAsync()).DropViewAsync(CatalogWorld.Id(name));

    [When(@"^I try to load the view ""([^""]*)""$")]
    public Task TryLoad(string name) => world.AttemptAsync(async () => await (await world.CatalogAsync()).LoadViewAsync(CatalogWorld.Id(name)));

    [Then(@"^the view ""([^""]*)"" is at version (\d+) with SQL ""([^""]*)""$")]
    public async Task AtVersion(string name, int version, string sql)
    {
        var view = await (await world.CatalogAsync()).LoadViewAsync(CatalogWorld.Id(name));
        Assert.Equal(version, view.Metadata.CurrentVersionId);
        Assert.Equal(sql, view.Sql(Dialect));
    }

    [Then(@"^the view ""([^""]*)"" keeps (\d+) versions$")]
    public async Task Versions(string name, int count)
    {
        var view = await (await world.CatalogAsync()).LoadViewAsync(CatalogWorld.Id(name));
        Assert.Equal(count, view.Metadata.Versions.Count);
        Assert.Equal(count, view.Metadata.VersionLog.Count);
    }

    [Then(@"^the view ""([^""]*)"" has the columns (.*)$")]
    public async Task Columns(string name, string list) =>
        Assert.Equal(Gherkin.List(list), (await (await world.CatalogAsync()).LoadViewAsync(CatalogWorld.Id(name))).Metadata.CurrentSchema.Columns.Select(c => c.Name));

    [Then(@"^the views of ""([^""]*)"" are (.*)$")]
    public async Task ViewsOf(string ns, string list) =>
        Assert.Equal(Gherkin.List(list), (await (await world.CatalogAsync()).ListViewsAsync(CatalogWorld.Ns(ns))).Select(v => v.Name));

    [Then(@"^the view ""([^""]*)"" exists$")]
    public async Task Exists(string name) => Assert.True(await (await world.CatalogAsync()).ViewExistsAsync(CatalogWorld.Id(name)));

    [Then(@"^the view ""([^""]*)"" does not exist$")]
    public async Task DoesNotExist(string name) => Assert.False(await (await world.CatalogAsync()).ViewExistsAsync(CatalogWorld.Id(name)));
}
