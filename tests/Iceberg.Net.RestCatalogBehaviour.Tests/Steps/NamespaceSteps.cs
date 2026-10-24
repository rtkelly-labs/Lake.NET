using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Json;
using Reqnroll;
using Xunit;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

[Binding]
public sealed class NamespaceSteps(CatalogWorld world)
{
    // ---------- setup and actions ----------

    [Given(@"^the namespace ""([^""]*)""$")]
    public async Task GivenNamespace(string ns) => await (await world.CatalogAsync()).CreateNamespaceAsync(CatalogWorld.Ns(ns));

    [Given(@"^the namespace ""([^""]*)"" with properties:$")]
    [When(@"^I create the namespace ""([^""]*)"" with properties:$")]
    public async Task CreateWithProperties(string ns, DataTable properties) =>
        await (await world.CatalogAsync()).CreateNamespaceAsync(CatalogWorld.Ns(ns), Gherkin.Properties(properties));

    [Given(@"^the namespaces (.*)$")]
    public async Task GivenNamespaces(string list)
    {
        var catalog = await world.CatalogAsync();
        foreach (var ns in Gherkin.List(list)) await catalog.CreateNamespaceAsync(CatalogWorld.Ns(ns));
    }

    [Given(@"^the namespace ""([^""]*)"" holding the table ""([^""]*)""$")]
    public async Task HoldingTable(string ns, string table)
    {
        await GivenNamespace(ns);
        await (await world.CatalogAsync()).CreateTableAsync(new CreateTableRequest(new TableIdentifier(CatalogWorld.Ns(ns), table), CatalogWorld.Schema));
    }

    [Given(@"^the namespace ""([^""]*)"" holding the view ""([^""]*)""$")]
    public async Task HoldingView(string ns, string view)
    {
        await GivenNamespace(ns);
        await (await world.CatalogAsync()).CreateViewAsync(new TableIdentifier(CatalogWorld.Ns(ns), view), ViewSteps.Definition("SELECT 1", ns));
    }

    [Given(@"^the namespace ""([^""]*)"" holding the child namespace ""([^""]*)""$")]
    public async Task HoldingChild(string ns, string child)
    {
        await GivenNamespace(ns);
        await GivenNamespace($"{ns}.{child}");
    }

    [When(@"^I try to create the namespace ""([^""]*)""$")]
    public Task TryCreate(string ns) => world.AttemptAsync(() => GivenNamespace(ns));

    [When(@"^I drop the namespace ""([^""]*)""$")]
    public async Task Drop(string ns) => await (await world.CatalogAsync()).DropNamespaceAsync(CatalogWorld.Ns(ns));

    [When(@"^I try to drop the namespace ""([^""]*)""$")]
    public Task TryDrop(string ns) => world.SendAsync(HttpMethod.Delete, CatalogWorld.NsPath(ns));

    [When(@"^I load the namespace ""([^""]*)""$")]
    public Task Load(string ns) => world.SendAsync(HttpMethod.Get, CatalogWorld.NsPath(ns));

    [When(@"^I list the top-level namespaces (\d+) at a time$")]
    public async Task ListPaged(int size)
    {
        string? token = null;
        do
        {
            var status = await world.SendAsync(HttpMethod.Get, $"/v1/namespaces?pageSize={size}{(token is null ? "" : $"&pageToken={Uri.EscapeDataString(token)}")}");
            Assert.Equal(HttpStatusCode.OK, status);
            var json = world.Json!;
            world.Pages.Add(json["namespaces"]!.AsArray().Select(n => string.Join('.', n!.AsArray().Select(l => l!.GetValue<string>()))).ToList());
            token = json["next-page-token"]?.GetValue<string>();
            Assert.True(world.Pages.Count < 100, "Paging does not terminate");
        } while (!string.IsNullOrEmpty(token));
    }

    [When(@"^I update the properties of ""([^""]*)"", setting:$")]
    public void UpdateSetting(string ns, DataTable updates)
    {
        _update = (ns, Gherkin.Properties(updates));
    }

    private (string Ns, Dictionary<string, string> Updates)? _update;

    [When(@"^removing (.*)$")]
    public Task Removing(string list)
    {
        var (ns, updates) = _update ?? throw new InvalidOperationException("Say what to set first");
        var body = new JsonObject
        {
            ["updates"] = MetadataJson.StringMap(updates),
            ["removals"] = new JsonArray(Gherkin.List(list).Select(k => (JsonNode)k).ToArray()),
        };
        return world.SendAsync(HttpMethod.Post, $"{CatalogWorld.NsPath(ns)}/properties", body);
    }

    // ---------- outcomes ----------

    [Then(@"^the namespace ""([^""]*)"" exists$")]
    public async Task Exists(string ns) => Assert.True(await (await world.CatalogAsync()).NamespaceExistsAsync(CatalogWorld.Ns(ns)));

    [Then(@"^the namespace ""([^""]*)"" does not exist$")]
    public async Task DoesNotExist(string ns) => Assert.False(await (await world.CatalogAsync()).NamespaceExistsAsync(CatalogWorld.Ns(ns)));

    [Then(@"^the namespace ""([^""]*)"" has exactly the properties:$")]
    public async Task HasProperties(string ns, DataTable expected)
    {
        Assert.Equal(HttpStatusCode.OK, await world.SendAsync(HttpMethod.Get, CatalogWorld.NsPath(ns)));
        var json = world.Json!;
        Assert.Equal(CatalogWorld.Ns(ns).Levels, json["namespace"]!.AsArray().Select(l => l!.GetValue<string>()));
        var actual = MetadataJson.StringMapFrom(json["properties"]);
        Assert.Equal(Gherkin.Properties(expected).OrderBy(kv => kv.Key, StringComparer.Ordinal), actual.OrderBy(kv => kv.Key, StringComparer.Ordinal));
    }

    [Then(@"^the top-level namespaces are (.*)$")]
    public async Task TopLevel(string list) =>
        Assert.Equal(Gherkin.List(list), (await (await world.CatalogAsync()).ListNamespacesAsync()).Select(n => n.ToString()));

    [Then(@"^the children of ""([^""]*)"" are (.*)$")]
    public async Task Children(string parent, string list) =>
        Assert.Equal(Gherkin.List(list), (await (await world.CatalogAsync()).ListNamespacesAsync(CatalogWorld.Ns(parent))).Select(n => n.ToString()));

    [Then(@"^checking whether ""([^""]*)"" exists answers (\d+)$")]
    public async Task Head(string ns, int status) =>
        Assert.Equal((HttpStatusCode)status, await world.SendAsync(HttpMethod.Head, CatalogWorld.NsPath(ns)));

    [Then(@"^the pages hold (.*) namespaces$")]
    public void PageSizes(string sizes) => Assert.Equal(Numbers(sizes), world.Pages.Select(p => p.Count));

    [Then(@"^together the pages list (.*) in that order$")]
    public void PagesTogether(string list) => Assert.Equal(Gherkin.List(list), world.Pages.SelectMany(p => p));

    [Then(@"^the update reports updated (.*), removed (.*) and missing (.*)$")]
    public void UpdateReport(string updated, string removed, string missing)
    {
        Assert.Equal(HttpStatusCode.OK, world.Status);
        var json = world.Json!;
        static List<string> Set(JsonNode? n) => n?.AsArray().Select(x => x!.GetValue<string>()).Order(StringComparer.Ordinal).ToList() ?? [];
        Assert.Equal(Gherkin.List(updated).Order(StringComparer.Ordinal), Set(json["updated"]));
        Assert.Equal(Gherkin.List(removed).Order(StringComparer.Ordinal), Set(json["removed"]));
        Assert.Equal(Gherkin.List(missing).Order(StringComparer.Ordinal), Set(json["missing"]));
    }

    /// <summary>"2, 2 and 1" → [2, 2, 1].</summary>
    public static List<int> Numbers(string text) =>
        text.Replace(" and ", ",", StringComparison.Ordinal).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(s => int.Parse(s, System.Globalization.CultureInfo.InvariantCulture)).ToList();
}
