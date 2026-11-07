using BenchmarkDotNet.Attributes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Benchmarks;

/// <summary>
/// Macro benchmarks over a local warehouse and the in-memory catalog: scan planning over many manifests, decoding one
/// large manifest, writing and reading a million rows, and a commit that loses a race and retries. The job (iteration
/// counts, in-process toolchain) is set in <see cref="Program"/>.
/// </summary>
[MemoryDiagnoser]
public class TableBenchmarks
{
    private const int Appends = 200;
    private const int ManifestFiles = 2000;
    private const int Rows = 1_000_000;

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Require(2, "p", PrimitiveType.Int),
        NestedField.Optional(3, "s", PrimitiveType.String),
        NestedField.Optional(4, "d", PrimitiveType.Double),
    ]);

    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-bench", Guid.NewGuid().ToString("N"));
    private InMemoryCatalog _catalog = null!;
    private Table _manyManifests = null!;
    private Table _largeManifest = null!;
    private ManifestFile _largeManifestFile = null!;
    private Table _million = null!;
    private object?[][] _millionRows = null!;
    private int _tables;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        await _catalog.CreateNamespaceAsync(Namespace.Of("b"));
        var byP = PartitionSpec.BuilderFor(Schema).Identity("p").Build();

        // 200 appends of 50 rows each, one manifest per append, 20 partition values spread over them.
        _manyManifests = await CreateAsync("many_manifests", byP);
        for (var a = 0; a < Appends; a++)
            await TableMutations.InsertAsync(_manyManifests, Enumerable.Range(0, 50).Select(i => Row(a * 50L + i, a % 20)).ToList());

        // One append over 2000 partition values: one manifest with 2000 data-file entries.
        _largeManifest = await CreateAsync("large_manifest", byP);
        await TableMutations.InsertAsync(_largeManifest, Enumerable.Range(0, ManifestFiles).Select(i => Row(i, i)).ToList());
        _largeManifestFile = (await TableScan.ManifestsAsync(_largeManifest, _largeManifest.Metadata.CurrentSnapshot!)).Single();

        _millionRows = Enumerable.Range(0, Rows).Select(i => Row(i, i % 100)).ToArray();
        _million = await CreateAsync("million", spec: null);
        await TableMutations.InsertAsync(_million, _millionRows);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    [Benchmark(Description = "Plan a full scan over 200 manifests")]
    public async Task<int> PlanAllAsync()
    {
        await Task.Delay(5); // Deliberate slowdown: checks that the comparison step fails. Reverted next commit.
        return (await TableScan.PlanAsync(_manyManifests, Expression.True)).Tasks.Count;
    }

    [Benchmark(Description = "Plan a filtered scan over 200 manifests (partition pruning)")]
    public async Task<int> PlanFilteredAsync() => (await TableScan.PlanAsync(_manyManifests, Expr.Equal("p", 7))).Tasks.Count;

    [Benchmark(Description = "Decode a manifest of 2000 data files")]
    public async Task<int> DecodeManifestAsync() =>
        (await ManifestStore.ReadAsync(_largeManifest.IO, _largeManifest.Metadata, _largeManifestFile)).Entries.Count;

    [Benchmark(Description = "Write 1M rows")]
    public async Task<long> WriteMillionAsync()
    {
        var t = await CreateAsync($"write_{Interlocked.Increment(ref _tables)}", spec: null);
        return (await TableMutations.InsertAsync(t, _millionRows)).Snapshot.SnapshotId;
    }

    [Benchmark(Description = "Read 1M rows")]
    public async Task<int> ReadMillionAsync() => (await TableScan.ReadAsync(_million, Expression.True)).Count;

    [Benchmark(Description = "Commit that loses a race and retries")]
    public async Task<long> CommitWithRetryAsync()
    {
        var name = $"retry_{Interlocked.Increment(ref _tables)}";
        var winner = await CreateAsync(name, spec: null);
        var loser = await _catalog.LoadTableAsync(TableIdentifier.Of("b", name));
        var first = (await TableMutations.InsertAsync(winner, [Row(1, 1)])).Snapshot;
        // `loser` still holds the metadata from before the winner's commit, so its first attempt fails the
        // assert-ref-snapshot-id requirement, refreshes and commits on the new parent.
        var second = (await TableMutations.InsertAsync(loser, [Row(2, 2)])).Snapshot;
        if (second.ParentSnapshotId != first.SnapshotId) throw new InvalidOperationException("The second commit did not retry on the first");
        return second.SnapshotId;
    }

    private async Task<Table> CreateAsync(string name, PartitionSpec? spec) =>
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("b", name), Schema, spec,
            Properties: new Dictionary<string, string>
            {
                // Keep the retry backoff out of the measurement.
                ["commit.retry.min-wait-ms"] = "1",
                ["commit.retry.max-wait-ms"] = "1",
            },
            FormatVersion: 2));

    private static object?[] Row(long id, int p) => [id, p, $"row-{id}", id * 0.5];
}
