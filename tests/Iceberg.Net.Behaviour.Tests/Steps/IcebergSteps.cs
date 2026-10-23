using Reqnroll;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;
using Xunit;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.Behaviour.Tests.Steps;

/// <summary>Per-scenario state (Reqnroll context injection gives every scenario a fresh instance).</summary>
public sealed class IcebergWorld : IDisposable
{
    // Under the directory shared with the Spark oracle's container, so "Spark reads the same rows" can see the table.
    public string Warehouse { get; } = Path.Combine(SparkOracle.SharedRoot, "bdd", Guid.NewGuid().ToString("N"));
    public InMemoryCatalog Catalog { get; }
    public Table? Table { get; set; }
    public Table? SecondWriter { get; set; }
    public MutationResult? LastResult { get; set; }
    public MutationResult? SecondResult { get; set; }
    public Exception? Error { get; set; }
    public List<object?[]> DefaultRows { get; set; } = [];

    public IcebergWorld()
    {
        Catalog = new InMemoryCatalog(Warehouse);
        Catalog.CreateNamespaceAsync(Namespace.Of("bdd")).GetAwaiter().GetResult();
    }

    public Table T => Table ?? throw new InvalidOperationException("No table yet: start with 'Given a format vN table ...'");

    public void Dispose()
    {
        Catalog.Dispose();
        if (Directory.Exists(Warehouse)) Directory.Delete(Warehouse, recursive: true);
    }
}

[Binding]
public sealed class IcebergSteps(IcebergWorld world)
{
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "region", PrimitiveType.String),
    ], identifierFieldIds: [1]);

    // ---------- argument transformations ----------

    [StepArgumentTransformation(@"^(copy-on-write|position deletes|equality deletes)$")]
    public static DeleteStrategy ToStrategy(string text) => text switch
    {
        "copy-on-write" => DeleteStrategy.CopyOnWrite,
        "position deletes" => DeleteStrategy.PositionDeletes,
        "equality deletes" => DeleteStrategy.EqualityDeletes,
        _ => throw new ArgumentException(text),
    };

    private static List<object?[]> RowsOf(DataTable table) => table.Rows.Select(r => new object?[]
    {
        long.Parse(r["id"], System.Globalization.CultureInfo.InvariantCulture),
        r["name"] is "" or "null" ? null : r["name"],
        r["region"],
    }).ToList();

    private async Task Attempt(Func<Task> action)
    {
        try { await action(); }
        catch (Exception e) when (e is ArgumentException or ValidationException or NotSupportedException) { world.Error = e; }
    }

    // ---------- Given ----------

    [Given(@"^a format v(\d) table keyed by ""id""$")]
    public async Task GivenTable(int version) =>
        world.Table = await world.Catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("bdd", "events"), Schema, FormatVersion: version));

    [Given(@"^a format v(\d) table keyed by ""id"" and partitioned by ""region""$")]
    public async Task GivenPartitionedTable(int version) =>
        world.Table = await world.Catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("bdd", "events"), Schema,
            PartitionSpec.BuilderFor(Schema).Identity("region").Build(), FormatVersion: version));

    [Given(@"^the default rows are:$")]
    public void GivenDefaultRows(DataTable rows) => world.DefaultRows = RowsOf(rows);

    [Given(@"^a format v(\d) table keyed by ""id"" holding the default rows$")]
    public async Task GivenTableWithDefaults(int version)
    {
        await GivenTable(version);
        await TableMutations.InsertAsync(world.T, world.DefaultRows);
    }

    [Given(@"^the table contains the default rows$")]
    public Task GivenDefaults() => TableMutations.InsertAsync(world.T, world.DefaultRows);

    [Given(@"^the table contains:$")]
    public Task GivenRows(DataTable rows) => TableMutations.InsertAsync(world.T, RowsOf(rows));

    [Given(@"^a second writer that has already loaded the table$")]
    public async Task GivenSecondWriter() => world.SecondWriter = await world.Catalog.LoadTableAsync(world.T.Identifier);

    // ---------- When ----------

    [When(@"^I insert:$")]
    public async Task WhenInsert(DataTable rows) => world.LastResult = await TableMutations.InsertAsync(world.T, RowsOf(rows));

    [Given(@"^I upsert with (.*):$")]
    [When(@"^I upsert with (.*):$")]
    public Task WhenUpsert(DeleteStrategy strategy, DataTable rows) =>
        Attempt(async () => world.LastResult = await TableMutations.UpsertAsync(world.T, RowsOf(rows), strategy: strategy));

    [When(@"^the second writer upserts with (.*):$")]
    public async Task WhenSecondUpserts(DeleteStrategy strategy, DataTable rows) =>
        world.SecondResult = await TableMutations.UpsertAsync(world.SecondWriter!, RowsOf(rows), strategy: strategy);

    [When(@"^I replace the ""(.*)"" groups with (.*):$")]
    public Task WhenReplaceGroups(string column, DeleteStrategy strategy, DataTable rows) =>
        Attempt(async () => world.LastResult = await TableMutations.ReplaceGroupsAsync(world.T, RowsOf(rows), [column], strategy));

    [Given(@"^I delete keys (\d+) and (\d+) with (.*)$")]
    [When(@"^I delete keys (\d+) and (\d+) with (.*)$")]
    public Task WhenDeleteKeys(long a, long b, DeleteStrategy strategy) =>
        Attempt(async () => world.LastResult = await TableMutations.DeleteByKeysAsync(world.T, ["id"], [[a], [b]], strategy));

    [When(@"^I delete rows where region is ""(.*)"" with (.*)$")]
    public Task WhenDeleteWhere(string region, DeleteStrategy strategy) =>
        Attempt(async () => world.LastResult = await TableMutations.DeleteWhereAsync(world.T, row => (string?)row[2] == region, strategy));

    [When(@"^I drop the data files where region is ""(.*)""$")]
    public Task WhenDropFiles(string region) =>
        SnapshotProducer.DeleteFiles(world.T, f => (string?)f.Partition[0] == region).CommitAsync();

    [When(@"^I upgrade the table to format v(\d)$")]
    public Task WhenUpgrade(int version) => world.T.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(version)]);

    [When(@"^I convert position deletes to deletion vectors$")]
    public Task WhenConvert() => SnapshotProducer.ConvertPositionDeletesToDeletionVectors(world.T).CommitAsync();

    // ---------- Then ----------

    [Then(@"^the table contains exactly:$")]
    public async Task ThenContains(DataTable expected)
    {
        Assert.Null(world.Error);
        var actual = (await TableScan.ReadAllAsync(await world.Catalog.LoadTableAsync(world.T.Identifier)))
            .OrderBy(r => (long)r[0]!).Select(Describe).ToList();
        Assert.Equal(RowsOf(expected).OrderBy(r => (long)r[0]!).Select(Describe), actual);
    }

    private static string Describe(object?[] r) => $"{r[0]}|{r[1] ?? "null"}|{r[2]}";

    [Then(@"^the last snapshot operation is ""(.*)""$")]
    public async Task ThenOperation(string operation) =>
        Assert.Equal(operation, (await world.Catalog.LoadTableAsync(world.T.Identifier)).Metadata.CurrentSnapshot!.Operation);

    [Then(@"^the table has no delete files$")]
    public Task ThenNoDeleteFiles() => ThenDeleteFilesAre("none");

    [Then(@"^the table's delete files are (none|position delete files|deletion vectors|equality delete files)$")]
    public async Task ThenDeleteFilesAre(string kind)
    {
        var index = await TableScan.DeletesAsync(await world.Catalog.LoadTableAsync(world.T.Identifier));
        var files = index.All.Concat(index.EqualityDeletes).Select(e => e.File).ToList();
        switch (kind)
        {
            case "none": Assert.Empty(files); break;
            case "position delete files": Assert.NotEmpty(files); Assert.All(files, f => Assert.Equal((FileContent.PositionDeletes, "PARQUET"), (f.Content, f.FileFormat))); break;
            case "deletion vectors": Assert.NotEmpty(files); Assert.All(files, f => Assert.Equal((FileContent.PositionDeletes, "PUFFIN"), (f.Content, f.FileFormat))); break;
            case "equality delete files": Assert.NotEmpty(files); Assert.All(files, f => Assert.Equal(FileContent.EqualityDeletes, f.Content)); break;
        }
    }

    [Then(@"^every data file has at most one deletion vector$")]
    public async Task ThenOneDvPerFile()
    {
        // DeleteIndex.LoadAsync itself rejects a second DV for a data file; also check the count explicitly.
        var index = await TableScan.DeletesAsync(await world.Catalog.LoadTableAsync(world.T.Identifier));
        var dvs = index.All.Where(e => e.IsDeletionVector).ToList();
        Assert.Equal(dvs.Count, dvs.Select(e => e.File.ReferencedDataFile).Distinct().Count());
    }

    [Then(@"^the write fails with ""(.*)""$")]
    public void ThenFails(string message)
    {
        Assert.NotNull(world.Error);
        Assert.Contains(message, world.Error!.Message);
    }

    [Then(@"^the second writer needed (\d+) attempts$")]
    public void ThenAttempts(int attempts) => Assert.Equal(attempts, world.SecondResult!.Attempts);

    [Then(@"^PyIceberg reads the same rows$")]
    public async Task ThenPyIcebergAgrees()
    {
        Skip.If(TestEnvironment.Current.LocalOracleSkipReason is not null, TestEnvironment.Current.LocalOracleSkipReason);
        var table = await world.Catalog.LoadTableAsync(world.T.Identifier);
        var py = await TestEnvironment.Current.OracleAsync("read-metadata", table.MetadataLocation!);
        var theirs = py["rows"]!.AsArray().Select(r => $"{r!["id"]}|{r["name"]?.GetValue<string>() ?? "null"}|{r["region"]}").ToList();
        var ours = (await TableScan.ReadAllAsync(table)).OrderBy(r => (long)r[0]!).Select(Describe).ToList();
        Assert.Equal(ours, theirs);
    }

    [Then(@"^PyIceberg refuses the table$")]
    public async Task ThenPyIcebergRefuses()
    {
        Skip.If(TestEnvironment.Current.LocalOracleSkipReason is not null, TestEnvironment.Current.LocalOracleSkipReason);
        var table = await world.Catalog.LoadTableAsync(world.T.Identifier);
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => TestEnvironment.Current.OracleAsync("read-metadata", table.MetadataLocation!));
        Assert.Contains("equality deletes", e.Message);
    }

    /// <summary>Spark + Iceberg Java (the reference implementation) sees the same rows and, on v3, the same row lineage.</summary>
    [Then(@"^Spark reads the same rows$")]
    public async Task ThenSparkAgrees()
    {
        Skip.If(SparkOracle.SkipReason is not null, SparkOracle.SkipReason);
        var table = await world.Catalog.LoadTableAsync(world.T.Identifier);
        var spark = (await SparkOracle.ReadTablesAsync([table.MetadataLocation!]))[table.MetadataLocation!]!;
        Assert.True(spark["error"] is null, $"Spark failed: {spark["error"]}");
        var theirs = spark["rows"]!.AsArray().Select(r => $"{r!["id"]}|{r["name"]?.GetValue<string>() ?? "null"}|{r["region"]}").ToList();
        var ours = (await TableScan.ReadAllAsync(table)).OrderBy(r => (long)r[0]!).Select(Describe).ToList();
        Assert.Equal(ours, theirs);
        if (table.Metadata.FormatVersion >= 3)
        {
            var lineage = (await TableScan.ReadWithLineageAsync(table)).OrderBy(r => (long)r.Row[0]!).Select(r => $"{r.Row[0]}:{r.RowId}:{r.LastUpdatedSequenceNumber}");
            var sparkLineage = spark["row_lineage"]!.AsArray().Select(r => $"{r!["id"]}:{r["row_id"]}:{r["last_updated_seq"]}");
            Assert.Equal(lineage, sparkLineage);
        }
    }
}
