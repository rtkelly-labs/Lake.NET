using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

BenchmarkSwitcher.FromAssembly(typeof(DeletionVectorBenchmarks).Assembly).Run(args);

/// <summary>Deletion vectors: the Roaring64 codec Iceberg v3, DuckLake 1.1 and Delta share.</summary>
[MemoryDiagnoser]
public class DeletionVectorBenchmarks
{
    private RoaringPositionBitmap _positions = null!;
    private byte[] _blob = null!;

    [Params(1_000, 100_000)]
    public int Deleted { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(7);
        _positions = new RoaringPositionBitmap();
        _positions.AddRange(Enumerable.Range(0, Deleted).Select(_ => (long)random.Next(0, 10_000_000)));
        _blob = DeletionVector.Encode(_positions);
    }

    [Benchmark]
    public byte[] Encode() => DeletionVector.Encode(_positions);

    [Benchmark]
    public long Decode() => DeletionVector.Decode(_blob).Cardinality;
}

/// <summary>Table metadata JSON: parsed on every load and every commit.</summary>
[MemoryDiagnoser]
public class MetadataJsonBenchmarks
{
    private string _json = null!;

    [Params(10, 1_000)]
    public int Snapshots { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var dir = Path.Combine(Path.GetTempPath(), "lake-bench-meta", Guid.NewGuid().ToString("N"));
        using var catalog = new InMemoryCatalog(dir);
        catalog.CreateNamespaceAsync(Namespace.Of("b")).GetAwaiter().GetResult();
        var table = catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("b"), "t"),
            new Schema(NestedField.Require(1, "id", PrimitiveType.Long)))).GetAwaiter().GetResult();
        var metadata = table.Metadata;
        for (var i = 0; i < Snapshots; i++)
            metadata = TableMetadataBuilder.From(metadata).ApplyAll([
                new MetadataUpdate.AddSnapshot(new Snapshot
                {
                    SnapshotId = 1000 + i, ParentSnapshotId = i == 0 ? null : 999 + i, SequenceNumber = metadata.NextSequenceNumber,
                    TimestampMs = 1_700_000_000_000 + i, ManifestList = $"{metadata.Location}/metadata/snap-{i}.avro", SchemaId = 0,
                    Summary = new Dictionary<string, string> { ["operation"] = "append" },
                }),
                new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, SnapshotRef.Branch(1000 + i)),
            ]).Build(null);
        _json = MetadataJson.Write(metadata);
        Directory.Delete(dir, recursive: true);
    }

    [Benchmark]
    public TableMetadata Parse() => MetadataJson.Read(_json);
}

/// <summary>Writing and scanning Parquet data files through the table API, on local disk.</summary>
[MemoryDiagnoser]
public class WriteScanBenchmarks
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "lake-bench-data", Guid.NewGuid().ToString("N"));
    private InMemoryCatalog _catalog = null!;
    private Table _table = null!;
    private List<object?[]> _rows = null!;

    [GlobalSetup]
    public void Setup()
    {
        _catalog = new InMemoryCatalog(_dir);
        _catalog.CreateNamespaceAsync(Namespace.Of("b")).GetAwaiter().GetResult();
        _table = _catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(Namespace.Of("b"), "t"), new Schema(
            NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String),
            NestedField.Optional(3, "score", PrimitiveType.Double)))).GetAwaiter().GetResult();
        _rows = Enumerable.Range(0, 100_000).Select(i => new object?[] { (long)i, $"name-{i % 1000}", i * 0.5 }).ToList();
        TableMutations.InsertAsync(_table, _rows).GetAwaiter().GetResult();
        _table = _catalog.LoadTableAsync(_table.Identifier).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _catalog.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    [Benchmark]
    public async Task<int> Write100kRows() => (await TableWriter.WriteAsync(_table, _rows)).Count;

    [Benchmark]
    public async Task<int> Scan100kRows() => (await TableScan.ReadAllAsync(_table)).Count;
}
