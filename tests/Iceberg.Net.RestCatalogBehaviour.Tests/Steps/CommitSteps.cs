using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;
using Reqnroll;
using Xunit;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

[Binding]
public sealed class CommitSteps(CatalogWorld world, TableSteps tables)
{
    private Table? _first, _second, _writer;
    private MutationResult? _firstResult, _secondResult;

    private static object?[] Row(Schema schema, long id, string name) =>
        schema.Columns.Select(c => c.Name switch { "id" => (object?)id, "name" => name, "region" => "eu", _ => null }).ToArray();

    // ---------- racing writers ----------

    [Given(@"^two writers that have both loaded ""([^""]*)""$")]
    public async Task TwoWriters(string name)
    {
        _first = await tables.LoadAsync(name);
        _second = await tables.LoadAsync(name);
    }

    [When(@"^the first writer appends:$")]
    public async Task FirstAppends(DataTable rows) => _firstResult = await TableMutations.InsertAsync(_first!, Gherkin.Rows(rows, _first!.Schema));

    [When(@"^the second writer appends:$")]
    public async Task SecondAppends(DataTable rows) => _secondResult = await TableMutations.InsertAsync(_second!, Gherkin.Rows(rows, _second!.Schema));

    [Then(@"^the second writer needed (\d+) attempts$")]
    public void SecondAttempts(int attempts)
    {
        Assert.Equal(1, _firstResult!.Attempts);
        Assert.Equal(attempts, _secondResult!.Attempts);
    }

    [Then(@"^the current snapshot of ""([^""]*)"" is the second writer's, whose parent is the first writer's$")]
    public async Task History(string name)
    {
        var current = (await tables.LoadAsync(name)).Metadata.CurrentSnapshot!;
        Assert.Equal(_secondResult!.Snapshot.SnapshotId, current.SnapshotId);
        Assert.Equal(_firstResult!.Snapshot.SnapshotId, current.ParentSnapshotId);
    }

    [When(@"^(\d+) writers that loaded the same version of ""([^""]*)"" each append (\d+) rows at once$")]
    public async Task Race(int writers, string name, int rows)
    {
        var loaded = await Task.WhenAll(Enumerable.Range(0, writers).Select(_ => tables.LoadAsync(name)));
        await Task.WhenAll(loaded.Select((t, w) =>
            TableMutations.InsertAsync(t, Enumerable.Range(0, rows).Select(i => Row(t.Schema, w * 100 + i, $"writer-{w}")).ToList())));
    }

    [Then(@"^""([^""]*)"" has (\d+) snapshots in one line of history$")]
    public async Task Linear(string name, int count)
    {
        var m = (await tables.LoadAsync(name)).Metadata;
        Assert.Equal(count, m.Snapshots.Count);
        Assert.Equal(count, m.Ancestors(m.CurrentSnapshotId).Count());
    }

    // ---------- stale requirements ----------

    [Given(@"^a writer that has loaded ""([^""]*)""$")]
    public async Task Writer(string name) => _writer = await tables.LoadAsync(name);

    [Given(@"^meanwhile another client appended to ""([^""]*)""$")]
    public async Task MeanwhileAppend(string name)
    {
        var table = await tables.LoadAsync(name);
        await tables.AppendAsync(table, name, [Row(table.Schema, 999, "meanwhile")]);
    }

    [Given(@"^meanwhile another client added a column ""([^""]*)"" to ""([^""]*)""$")]
    public async Task MeanwhileColumn(string column, string name) => await AddColumn(column, name);

    [When(@"^I add the optional string column ""([^""]*)"" to ""([^""]*)""$")]
    public async Task AddColumn(string column, string name)
    {
        var table = await tables.LoadAsync(name);
        await table.UpdateSchemaAsync(new Schema(0, [.. table.Schema.Columns, NestedField.Optional(table.Metadata.LastColumnId + 1, column, PrimitiveType.String)]));
        world.Observe(name, table);
    }

    public static UpdateRequirement Requirement(Table seen, string requirement) => requirement switch
    {
        "the main branch to be at the snapshot it saw" or "at the snapshot I saw" => new UpdateRequirement.AssertRefSnapshotId("main", seen.Metadata.CurrentSnapshotId),
        "the current schema to be the one it saw" or "on the schema I saw" => new UpdateRequirement.AssertCurrentSchemaId(seen.Metadata.CurrentSchemaId),
        _ => throw new ArgumentException($"Unknown requirement '{requirement}'"),
    };

    [When(@"^the writer commits a property change that requires (.*)$")]
    public Task WriterCommits(string requirement) => world.AttemptAsync(() =>
        _writer!.CommitAsync([Requirement(_writer, requirement)], [new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["owner"] = "writer" })]));

    [Then(@"^""([^""]*)"" has not changed since the meanwhile change$")]
    public async Task Unchanged(string name)
    {
        var table = await tables.LoadAsync(name);
        Assert.Equal(world.LastMetadataFile[name], table.MetadataLocation);
        Assert.False(table.Metadata.Properties.ContainsKey("owner"));
    }

    // ---------- schema evolution and time travel ----------

    [Given(@"^I remember the current snapshot of ""([^""]*)"" as ""([^""]*)""$")]
    public async Task RememberSnapshot(string name, string label) =>
        world.SnapshotIds[label] = (await tables.LoadAsync(name)).Metadata.CurrentSnapshotId!.Value;

    [Then(@"^reading ""([^""]*)"" as of snapshot ""([^""]*)"" gives the columns (.*) and the rows:$")]
    public Task ReadAsOf(string name, string label, string columns, DataTable rows) =>
        Read(name, SnapshotSelector.Id(world.SnapshotIds[label]), columns, rows);

    [Then(@"^reading ""([^""]*)"" now gives the columns (.*) and the rows:$")]
    public Task ReadNow(string name, string columns, DataTable rows) => Read(name, SnapshotSelector.Current, columns, rows);

    private async Task Read(string name, SnapshotSelector selector, string columns, DataTable expected)
    {
        var table = await tables.LoadAsync(name);
        var request = new ScanRequest { Snapshot = selector };
        var schema = TableScan.SchemaOf(table, request);
        Assert.Equal(Gherkin.List(columns), schema.Columns.Select(c => c.Name));
        var rows = new List<object?[]>();
        await foreach (var row in TableScan.ReadRowsAsync(table, request)) rows.Add(row);
        Assert.All(rows, r => Assert.Equal(schema.Columns.Count, r.Length));
        Assert.Equal(Gherkin.Render(expected), Gherkin.Render(rows, schema, expected.Header.ToList()));
    }

    // ---------- multi-table transactions ----------

    [Given(@"^I remember the state of ""([^""]*)"" and ""([^""]*)""$")]
    public async Task RememberState(string a, string b)
    {
        foreach (var name in new[] { a, b })
        {
            var table = await tables.LoadAsync(name);
            world.Remembered[name] = table;
            world.Observe(name, table);
        }
    }

    private static MetadataUpdate.SetProperties Set(string key, string value) => new(new Dictionary<string, string> { [key] = value });

    [When(@"^I commit one transaction that sets ""([^""]*)"" to ""([^""]*)"" on ""([^""]*)"" and ""([^""]*)"", requiring each to be (.*)$")]
    public Task Transaction(string key, string value, string a, string b, string requirement) => world.AttemptAsync(async () =>
        await (await world.CatalogAsync()).CommitTransactionAsync(new[] { a, b }.Select(n =>
            new TableCommit(CatalogWorld.Id(n), [Requirement(world.Remembered[n], requirement)], [Set(key, value)])).ToList()));

    [When(@"^I commit one transaction that sets ""([^""]*)"" to ""([^""]*)"" on ""([^""]*)"" and on the missing table ""([^""]*)""$")]
    public Task TransactionWithMissing(string key, string value, string a, string missing) => world.AttemptAsync(async () =>
        await (await world.CatalogAsync()).CommitTransactionAsync(
        [
            new TableCommit(CatalogWorld.Id(a), [new UpdateRequirement.AssertTableUuid(world.Remembered[a].Metadata.TableUuid)], [Set(key, value)]),
            new TableCommit(CatalogWorld.Id(missing), [], [Set(key, value)]),
        ]));

    [When(@"^I commit one transaction that appends a row with id (\d+) to ""([^""]*)"" and to ""([^""]*)""$")]
    public async Task TransactionAppend(long id, string a, string b)
    {
        var commits = new List<TableCommit>();
        foreach (var name in new[] { a, b })
        {
            var table = await tables.LoadAsync(name);
            var files = await TableWriter.WriteAsync(table, [Row(table.Schema, id, "in-transaction")]);
            commits.Add(await SnapshotProducer.Append(table).AddFiles(files).StageAsync());
        }
        await (await world.CatalogAsync()).CommitTransactionAsync(commits);
    }

    [Then(@"^""([^""]*)"" and ""([^""]*)"" have no property ""([^""]*)""$")]
    public async Task NoProperty(string a, string b, string key)
    {
        foreach (var name in new[] { a, b }) Assert.False((await tables.LoadAsync(name)).Metadata.Properties.ContainsKey(key), $"{name} has '{key}'");
    }

    [Then(@"^""([^""]*)"" has not changed since I remembered it$")]
    public async Task UnchangedSinceRemembered(string name) =>
        Assert.Equal(world.Remembered[name].MetadataLocation, (await tables.LoadAsync(name)).MetadataLocation);
}
