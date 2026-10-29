using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;
using Reqnroll;
using Xunit;

namespace Iceberg.Net.Behaviour.Tests.Steps;

[Binding]
public sealed class CommitRecoverySteps : IDisposable
{
    private UncertainCommitCatalog? _catalog;
    private string? _warehouse;
    private readonly List<Iceberg.Net.Catalog.Table> _tables = [];
    private readonly List<StagedCommit> _stages = [];
    private Exception? _error;
    private SnapshotProducer? _producer;
    private string _mode = "single";

    [Given(@"^a recovery table in format v(\d) using (single|transaction)$")]
    public async Task GivenRecoveryTable(int version, string mode)
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Iceberg.Net.slnx"))) root = root.Parent ?? throw new InvalidOperationException("Repository root missing");
        _warehouse = Path.Combine(root.FullName, "temp", "recovery-bdd", Guid.NewGuid().ToString("N"));
        _catalog = new UncertainCommitCatalog(_warehouse);
        _mode = mode;
        await _catalog.CreateNamespaceAsync(Namespace.Of("recovery"));
        for (var i = 0; i < (mode == "single" ? 1 : 2); i++)
        {
            var table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("recovery", "t" + i),
                new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long)]), FormatVersion: version));
            await TableMutations.InsertAsync(table, [[0L]]);
            _tables.Add(table);
        }
    }

    [When("an insert commits but its response is lost after another insert and snapshot expiry")]
    public async Task WhenResponseLost()
    {
        _catalog!.BeforeError = async ids =>
        {
            foreach (var id in ids)
            {
                var other = await _catalog.LoadTableAsync(id);
                await TableMutations.InsertAsync(other, [[2L]]);
                await Maintenance.ExpireAsync(other, long.MaxValue, retainLast: 1, cleanExpiredFiles: false);
            }
        };
        if (_mode == "transaction")
            foreach (var table in _tables)
                _stages.Add(await StagedCommit.StageAsync(table, async ct => SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, [[1L]], ct))));
        _catalog.Armed = true;
        _error = await Record.ExceptionAsync(async () =>
        {
            if (_mode == "single") await TableMutations.InsertAsync(_tables[0], [[1L]]);
            else await StagedCommit.CommitAllAsync(_stages);
        });
    }

    [Then("recovery reports an unknown outcome without replaying the input")]
    public async Task ThenUnknown()
    {
        Assert.IsType<CommitStateUnknownException>(_error);
        foreach (var table in _tables)
        {
            var fresh = await _catalog!.LoadTableAsync(table.Identifier);
            Assert.Equal(new long[] { 0, 1, 2 }, (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order());
            await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        }
    }

    [When("the caller cancels after the insert is committed but before its response arrives")]
    public async Task WhenCancelledAfterCommit()
    {
        using var cancel = new CancellationTokenSource();
        _catalog!.Failure = new OperationCanceledException(cancel.Token);
        _catalog.BeforeError = _ => { cancel.Cancel(); return Task.CompletedTask; };
        _producer = SnapshotProducer.Append(_tables[0]).AddFiles(await TableWriter.WriteAsync(_tables[0], [[1L]]));
        _catalog.Armed = true;
        _error = await Record.ExceptionAsync(() => _producer.CommitAsync(cancel.Token));
    }

    [Then("the cancelled producer remains unknown and cannot be replayed")]
    public async Task ThenCancelledProducerCannotReplay()
    {
        var error = Assert.IsType<CommitStateUnknownException>(_error);
        Assert.IsAssignableFrom<OperationCanceledException>(error.ResolutionFailure);
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => _producer!.CommitAsync());
    }

    [Then("PyIceberg reads the cancelled insert exactly once")]
    public async Task ThenCancelledOracle()
    {
        var fresh = await _catalog!.LoadTableAsync(_tables[0].Identifier);
        Assert.Equal(2, fresh.Metadata.Snapshots.Count);
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        var result = await TestEnvironment.Current.OracleAsync("read-metadata", fresh.MetadataLocation!);
        Assert.Equal(new long[] { 0, 1 }, result["rows"]!.AsArray().Select(row => row!["id"]!.GetValue<long>()).Order());
    }

    [Then("uncertain staged files cannot be discarded")]
    public async Task ThenPreserved()
    {
        foreach (var stage in _stages) await stage.DiscardAsync();
        Assert.All(_catalog!.UncertainFiles, path => Assert.True(File.Exists(path), path));
    }

    [Then("PyIceberg reads each recovery row exactly once")]
    public async Task ThenOracle()
    {
        foreach (var table in _tables)
        {
            var fresh = await _catalog!.LoadTableAsync(table.Identifier);
            var result = await TestEnvironment.Current.OracleAsync("read-metadata", fresh.MetadataLocation!);
            Assert.Equal(new long[] { 0, 1, 2 }, result["rows"]!.AsArray().Select(row => row!["id"]!.GetValue<long>()).Order());
        }
    }

    public void Dispose()
    {
        _catalog?.Dispose();
        if (_warehouse is not null && Directory.Exists(_warehouse)) Directory.Delete(_warehouse, true);
    }
}
