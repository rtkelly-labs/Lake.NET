using System.Text.Json;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Replays a TLC-produced schedule through real producers, catalog requirements and Parquet files.</summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("Metastore Tables")]
public sealed class FormalTraceReplayTests : IDisposable
{
    private readonly string _warehouse;

    public FormalTraceReplayTests()
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Iceberg.Net.slnx")))
            root = root.Parent ?? throw new InvalidOperationException("Cannot find repository root for local test artifacts");
        _warehouse = Path.Combine(root.FullName, "temp", "formal", "replay", Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long)]);

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Generated_append_conflict_trace_preserves_both_batches(int formatVersion)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "formal", "append-conflict.json");
        using var trace = JsonDocument.Parse(await File.ReadAllTextAsync(path));
        Assert.Equal("CommitRetry", trace.RootElement.GetProperty("model").GetString());
        using var catalog = new InMemoryCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("formal"));
        var initial = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("formal", "replay"), Schema, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(initial, [[0L]]);
        var writers = new[] { await catalog.LoadTableAsync(initial.Identifier), await catalog.LoadTableAsync(initial.Identifier) };
        var producers = new SnapshotProducer?[2];
        var files = new IReadOnlyList<DataFile>?[2];
        var attempts = new int[2];
        var committed = new List<long> { 0 };
        var parent = initial.Metadata.CurrentSnapshotId;

        foreach (var step in trace.RootElement.GetProperty("steps").EnumerateArray())
        {
            var writer = step.GetProperty("writer").GetInt32() - 1;
            Assert.InRange(writer, 0, 1);
            switch (step.GetProperty("action").GetString())
            {
                case "prepare":
                    files[writer] ??= await TableWriter.WriteAsync(writers[writer], [[(long)writer + 1]]);
                    // Expose individual attempts to the trace driver; the separate test below exercises the built-in loop.
                    producers[writer] = SnapshotProducer.Append(writers[writer]).AddFiles(files[writer]!).WithRetries(0);
                    attempts[writer]++;
                    break;
                case "commit":
                    var snapshot = await producers[writer]!.CommitAsync();
                    Assert.Equal(parent, snapshot.ParentSnapshotId);
                    parent = snapshot.SnapshotId;
                    committed.Add(writer + 1L);
                    break;
                case "conflict":
                    await Assert.ThrowsAsync<CommitFailedException>(() => producers[writer]!.CommitAsync());
                    break;
                case "retry":
                    await writers[writer].RefreshAsync();
                    break;
                default:
                    Assert.Fail($"Unsupported model action: {step}");
                    break;
            }

            // Prepared/uncommitted files must never become visible; each successful pointer swap is atomic.
            var fresh = await catalog.LoadTableAsync(initial.Identifier);
            var actual = (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order().ToArray();
            Assert.Equal(committed.Order(), actual);
            Assert.Equal(committed.Count, fresh.Metadata.Snapshots.Count);
            await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        }

        Assert.Equal([2, 1], attempts);
        Assert.Equal(trace.RootElement.GetProperty("expectedRows").EnumerateArray().Select(row => row.GetInt64()), committed.Order());
        Assert.All(files.SelectMany(f => f!), file => Assert.True(File.Exists(file.FilePath)));
    }

    private sealed class InterleavingCatalog(string warehouse) : InMemoryCatalog(warehouse)
    {
        public Func<Task>? BeforeNextSwap { get; set; }
        public int LostSwaps { get; private set; }

        protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
        {
            if (BeforeNextSwap is { } interfere)
            {
                BeforeNextSwap = null;
                await interfere();
            }
            try { await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct); }
            catch (CommitFailedException) { LostSwaps++; throw; }
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Automatic_append_retry_realises_the_generated_conflict_order(int formatVersion)
    {
        using var trace = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "formal", "append-conflict.json")));
        var commitOrder = trace.RootElement.GetProperty("steps").EnumerateArray()
            .Where(step => step.GetProperty("action").GetString() == "commit")
            .Select(step => step.GetProperty("writer").GetInt32()).ToArray();
        Assert.Equal([2, 1], commitOrder);
        using var catalog = new InterleavingCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("formal"));
        var first = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("formal", "automatic"), Schema,
            Properties: new Dictionary<string, string> { ["commit.retry.min-wait-ms"] = "0" }, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(first, [[0L]]);
        var second = await catalog.LoadTableAsync(first.Identifier);
        Snapshot? winningSnapshot = null;
        catalog.BeforeNextSwap = async () => winningSnapshot = (await TableMutations.InsertAsync(second, [[2L]])).Snapshot;

        var result = await TableMutations.InsertAsync(first, [[1L]]);

        Assert.Equal(1, catalog.LostSwaps);
        Assert.Equal(2, result.Attempts);
        Assert.NotNull(winningSnapshot);
        Assert.Equal(winningSnapshot.SnapshotId, result.Snapshot.ParentSnapshotId);
        var fresh = await catalog.LoadTableAsync(first.Identifier);
        Assert.Equal(new long[] { 0, 1, 2 }, (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order());
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }
}
