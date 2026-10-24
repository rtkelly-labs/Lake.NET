using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Reqnroll;
using Xunit;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

[Binding]
public sealed class TableSteps(CatalogWorld world)
{
    // ---------- setup ----------

    public async Task<Table> CreateAsync(string name, bool partitioned = false)
    {
        var catalog = await world.CatalogAsync();
        var table = await catalog.CreateTableAsync(new CreateTableRequest(CatalogWorld.Id(name), CatalogWorld.Schema,
            partitioned ? CatalogWorld.RegionSpec(CatalogWorld.Schema) : null));
        world.Observe(name, table);
        return table;
    }

    /// <summary>Write <paramref name="rows"/> as one append and note the files it added.</summary>
    public async Task<Snapshot> AppendAsync(Table table, string name, IReadOnlyList<object?[]> rows)
    {
        var files = await TableWriter.WriteAsync(table, rows);
        var snapshot = await SnapshotProducer.Append(table).AddFiles(files).CommitAsync();
        world.Observe(name, table, files.Select(f => f.FilePath));
        return snapshot;
    }

    public async Task<Table> LoadAsync(string name) => await (await world.CatalogAsync()).LoadTableAsync(CatalogWorld.Id(name));

    [Given(@"^the table ""([^""]*)""$")]
    public Task GivenTable(string name) => CreateAsync(name);

    [Given(@"^the tables (.*)$")]
    public async Task GivenTables(string list)
    {
        foreach (var name in Gherkin.List(list)) await CreateAsync(name);
    }

    [Given(@"^the table ""([^""]*)"" holding:$")]
    public async Task GivenTableHolding(string name, DataTable rows)
    {
        var table = await CreateAsync(name);
        await AppendAsync(table, name, Gherkin.Rows(rows, table.Schema));
    }

    [Given(@"^""([^""]*)"" also holds:$")]
    [When(@"^I append to ""([^""]*)"":$")]
    public async Task Append(string name, DataTable rows)
    {
        var table = await LoadAsync(name);
        await AppendAsync(table, name, Gherkin.Rows(rows, table.Schema));
    }

    // ---------- actions ----------

    [When(@"^I create the table ""([^""]*)"" partitioned by ""region""$")]
    public Task CreatePartitioned(string name) => CreateAsync(name, partitioned: true);

    [When(@"^I try to create the table ""([^""]*)""$")]
    public Task TryCreate(string name) => world.AttemptAsync(() => CreateAsync(name));

    [When(@"^I try to load the table ""([^""]*)""$")]
    public Task TryLoad(string name) => world.AttemptAsync(() => LoadAsync(name));

    [When(@"^I rename ""([^""]*)"" to ""([^""]*)""$")]
    public async Task Rename(string from, string to) =>
        await (await world.CatalogAsync()).RenameTableAsync(CatalogWorld.Id(from), CatalogWorld.Id(to));

    [When(@"^I try to rename ""([^""]*)"" to ""([^""]*)""$")]
    public Task TryRename(string from, string to) => world.AttemptAsync(() => Rename(from, to));

    [When(@"^I drop the table ""([^""]*)""$")]
    public Task Drop(string name) => DropAsync(name, purge: false);

    [When(@"^I drop the table ""([^""]*)"" with purge$")]
    public Task DropWithPurge(string name) => DropAsync(name, purge: true);

    private async Task DropAsync(string name, bool purge)
    {
        world.Observe(name, await LoadAsync(name));
        await (await world.CatalogAsync()).DropTableAsync(CatalogWorld.Id(name), purge);
    }

    [When(@"^I try to list the tables of ""([^""]*)""$")]
    public Task TryList(string ns) => world.AttemptAsync(async () => await (await world.CatalogAsync()).ListTablesAsync(CatalogWorld.Ns(ns)));

    [When(@"^I try to drop the table ""([^""]*)""$")]
    public Task TryDrop(string name) => world.AttemptAsync(async () => await (await world.CatalogAsync()).DropTableAsync(CatalogWorld.Id(name)));

    [When(@"^I register the (current|last) metadata file of ""([^""]*)"" as ""([^""]*)""$")]
    public async Task Register(string which, string source, string name)
    {
        var location = which == "current" ? (await LoadAsync(source)).MetadataLocation! : world.LastMetadataFile[source];
        var registered = await (await world.CatalogAsync()).RegisterTableAsync(CatalogWorld.Id(name), location);
        world.Observe(name, registered);
    }

    [When(@"^I try to register the (current|last) metadata file of ""([^""]*)"" as ""([^""]*)""$")]
    public Task TryRegister(string which, string source, string name) => world.AttemptAsync(() => Register(which, source, name));

    [When(@"^I unregister ""([^""]*)""$")]
    public async Task Unregister(string name)
    {
        world.Observe(name, await LoadAsync(name));
        Assert.Equal(HttpStatusCode.OK, await world.SendAsync(HttpMethod.Post, $"{CatalogWorld.TablePath(name)}/unregister"));
    }

    [When(@"^I list the tables of ""([^""]*)"" (\d+) at a time$")]
    public async Task ListPaged(string ns, int size)
    {
        string? token = null;
        do
        {
            var status = await world.SendAsync(HttpMethod.Get, $"{CatalogWorld.NsPath(ns)}/tables?pageSize={size}{(token is null ? "" : $"&pageToken={Uri.EscapeDataString(token)}")}");
            Assert.Equal(HttpStatusCode.OK, status);
            var json = world.Json!;
            world.Pages.Add(json["identifiers"]!.AsArray().Select(i => i!["name"]!.GetValue<string>()).ToList());
            token = json["next-page-token"]?.GetValue<string>();
            Assert.True(world.Pages.Count < 100, "Paging does not terminate");
        } while (!string.IsNullOrEmpty(token));
    }

    // ---------- outcomes ----------

    [Then(@"^the table ""([^""]*)"" exists$")]
    public async Task Exists(string name) => Assert.True(await (await world.CatalogAsync()).TableExistsAsync(CatalogWorld.Id(name)));

    [Then(@"^the table ""([^""]*)"" does not exist$")]
    public async Task DoesNotExist(string name) => Assert.False(await (await world.CatalogAsync()).TableExistsAsync(CatalogWorld.Id(name)));

    [Then(@"^the tables of ""([^""]*)"" are (.*)$")]
    public async Task TablesOf(string ns, string list) =>
        Assert.Equal(Gherkin.List(list), (await (await world.CatalogAsync()).ListTablesAsync(CatalogWorld.Ns(ns))).Select(t => t.Name));

    [Then(@"^the pages hold (.*) tables$")]
    public void PageSizes(string sizes) => Assert.Equal(NamespaceSteps.Numbers(sizes), world.Pages.Select(p => p.Count));

    [Then(@"^loading ""([^""]*)"" gives the columns (.*)$")]
    public async Task Columns(string name, string list) =>
        Assert.Equal(Gherkin.List(list), (await LoadAsync(name)).Schema.Columns.Select(c => c.Name));

    [Then(@"^loading ""([^""]*)"" gives a partition spec on ""([^""]*)"" with transform ""([^""]*)""$")]
    public async Task Partitioning(string name, string column, string transform)
    {
        var table = await LoadAsync(name);
        var field = Assert.Single(table.Spec.Fields);
        Assert.Equal(column, table.Schema.FindField(field.SourceId)!.Name);
        Assert.Equal(transform, field.Transform.ToString());
    }

    [Then(@"^""([^""]*)"" has no current snapshot$")]
    public async Task NoSnapshot(string name) => Assert.Null((await LoadAsync(name)).Metadata.CurrentSnapshot);

    [Then(@"^the table ""([^""]*)"" holds exactly:$")]
    [Then(@"^a fresh client reads exactly these rows from ""([^""]*)"":$")]
    public async Task HoldsExactly(string name, DataTable expected)
    {
        // A client of its own, so nothing cached by the writer's client can stand in for what the server holds.
        using var fresh = world.AuthRequired ? null : await world.Host.ConnectAsync();
        var table = fresh is null ? await LoadAsync(name) : await fresh.LoadTableAsync(CatalogWorld.Id(name));
        var rows = await TableScan.ReadAllAsync(table);
        Assert.Equal(Gherkin.Render(expected), Gherkin.Render(rows, table.Schema, expected.Header.ToList()));
    }

    [Then(@"^""([^""]*)"" holds (\d+) rows$")]
    public async Task RowCount(string name, int count) => Assert.Equal(count, (await TableScan.ReadAllAsync(await LoadAsync(name))).Count);

    [Then(@"^""([^""]*)"" has (\d+) snapshots?$")]
    public async Task SnapshotCount(string name, int count) => Assert.Equal(count, (await LoadAsync(name)).Metadata.Snapshots.Count);

    [Then(@"^the property ""([^""]*)"" of ""([^""]*)"" is ""([^""]*)""$")]
    public async Task Property(string key, string name, string value) =>
        Assert.Equal(value, (await LoadAsync(name)).Metadata.Properties.GetValueOrDefault(key));

    [Then(@"^the data and metadata files of ""([^""]*)"" still exist$")]
    public void FilesExist(string name)
    {
        var files = world.FilesOf[name];
        Assert.Contains(files, f => f.EndsWith(".parquet", StringComparison.Ordinal));
        foreach (var f in files) Assert.True(File.Exists(Local(f)), $"{f} is gone");
    }

    [Then(@"^none of the data or metadata files of ""([^""]*)"" exist$")]
    public void FilesGone(string name)
    {
        var files = world.FilesOf[name];
        Assert.Contains(files, f => f.EndsWith(".parquet", StringComparison.Ordinal));
        Assert.Contains(files, f => f.EndsWith(".metadata.json", StringComparison.Ordinal));
        foreach (var f in files) Assert.False(File.Exists(Local(f)), $"{f} is still there");
    }

    [Then(@"^""([^""]*)"" has the same metadata file as ""([^""]*)""$")]
    public async Task SameMetadata(string a, string b) => Assert.Equal((await LoadAsync(b)).MetadataLocation, (await LoadAsync(a)).MetadataLocation);

    [Then(@"^the response carries the last metadata file of ""([^""]*)""$")]
    public void UnregisterResponse(string name)
    {
        var json = world.Json!;
        Assert.Equal(world.LastMetadataFile[name], json["metadata-location"]!.GetValue<string>());
        Assert.NotNull(json["metadata"]?["table-uuid"]);
    }

    /// <summary>The local path of a file location (the warehouse is a local directory).</summary>
    public static string Local(string location) =>
        location.StartsWith("file:", StringComparison.Ordinal) ? new Uri(location).LocalPath : location;
}
