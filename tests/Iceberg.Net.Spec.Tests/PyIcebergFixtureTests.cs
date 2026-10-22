using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Json;
using Iceberg.Net.Manifests;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Reads tables written by PyIceberg (interop/pyiceberg_oracle.py fixtures). PyIceberg's bucket
/// transform is the Rust iceberg-core implementation, so these also cross-check our transforms
/// against a second, independent implementation.
/// </summary>
public class PyIcebergFixtureTests
{
    private const string FixtureRoot = "file:///tmp/sentric-iceberg-fixture-root/warehouse";
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "fixtures", "pyiceberg");
    private static readonly JsonNode Expected = JsonNode.Parse(File.ReadAllText(Path.Combine(Dir, "expected.json")))!;

    /// <summary>Maps the absolute locations PyIceberg recorded onto the copied fixture directory.</summary>
    private sealed class RemappingFileIO : IFileIO
    {
        private readonly LocalFileIO _local = new();
        private static string Map(string l) => l.StartsWith(FixtureRoot, StringComparison.Ordinal) ? Dir + l[FixtureRoot.Length..] : l;
        public Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default) => _local.ReadAllAsync(Map(l), ct);
        public Task<Stream> OpenReadAsync(string l, CancellationToken ct = default) => _local.OpenReadAsync(Map(l), ct);
        public Task<byte[]> ReadRangeAsync(string l, long o, int n, CancellationToken ct = default) => _local.ReadRangeAsync(Map(l), o, n, ct);
        public Task<long> LengthAsync(string l, CancellationToken ct = default) => _local.LengthAsync(Map(l), ct);
        public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => _local.ExistsAsync(Map(l), ct);
        public Task WriteAsync(string l, ReadOnlyMemory<byte> c, bool o = false, CancellationToken ct = default) => throw new InvalidOperationException("fixtures are read-only");
        public Task DeleteAsync(string l, CancellationToken ct = default) => throw new InvalidOperationException("fixtures are read-only");
        public void Dispose() { }
    }

    private static async Task<Table> Load(string name)
    {
        var expected = Expected[$"interop.{name}"]!;
        var location = expected["metadata_location"]!.GetValue<string>();
        var io = new RemappingFileIO();
        var metadata = MetadataJson.Read(Encoding.UTF8.GetString(await io.ReadAllAsync(location)), location);
        return new Table(new InMemoryCatalog("/unused"), TableIdentifier.Of("interop", name), metadata, io);
    }

    [Fact]
    public async Task Partitioned_table_reads_the_same_rows_as_pyiceberg()
    {
        var table = await Load("partitioned");
        var expected = Expected["interop.partitioned"]!;
        var rows = await TableScan.ReadAllAsync(table);

        Assert.Equal(expected["row_count"]!.GetValue<int>(), rows.Count);
        Assert.Equal(
            expected["ids"]!.AsArray().Select(i => i!.GetValue<long>()).Order(),
            rows.Select(r => (long)r[0]!).Order());
        Assert.All(rows, r => Assert.Equal("eu", r[4])); // the 'us' partitions were deleted

        // Every column value, including timestamptz micros and nulls.
        var byId = rows.ToDictionary(r => (long)r[0]!);
        foreach (var e in expected["rows"]!.AsArray())
        {
            var row = byId[e!["id"]!.GetValue<long>()];
            Assert.Equal(e["name"]?.GetValue<string>(), row[1]);
            Assert.Equal(e["amount"]!.GetValue<double>(), row[2]);
            Assert.Equal(e["event_ts"]!.GetValue<long>(), row[3]);
            Assert.Equal(e["qty"]!.GetValue<int>(), row[5]);
        }
    }

    [Fact]
    public async Task Delete_snapshot_metadata_matches_spec_semantics()
    {
        var table = await Load("partitioned");
        var m = table.Metadata;
        Assert.Equal(3, m.Snapshots.Count);
        Assert.Equal(3, m.LastSequenceNumber);
        Assert.Equal(m.Snapshots.Select(s => s.SequenceNumber).Order(), m.Snapshots.Select(s => s.SequenceNumber));
        Assert.Equal(Metadata.Operations.Delete, m.CurrentSnapshot!.Operation);

        var manifests = await TableScan.ManifestsAsync(table, m.CurrentSnapshot);
        var entries = new List<ManifestEntry>();
        foreach (var mf in manifests)
            entries.AddRange(ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(mf.ManifestPath), m.SpecById(mf.PartitionSpecId), m.CurrentSchema, mf).Entries);

        // Deleted entries carry the deleting snapshot's ID; live entries keep their original data sequence numbers.
        Assert.All(entries.Where(e => e.Status == ManifestEntryStatus.Deleted), e => Assert.Equal(m.CurrentSnapshotId, e.SnapshotId));
        Assert.All(entries.Where(e => e.Status != ManifestEntryStatus.Deleted), e => Assert.InRange(e.SequenceNumber!.Value, 1, 2));
        Assert.Contains(entries, e => e.Status == ManifestEntryStatus.Deleted && (string)e.DataFile.Partition[2]! == "us");
    }

    [Fact]
    public async Task Our_transforms_agree_with_pyiceberg_partition_values()
    {
        var table = await Load("partitioned");
        var spec = table.Spec;
        foreach (var task in await TableScan.PlanFilesAsync(table, table.Metadata.Snapshots[0].SnapshotId))
        {
            var rows = await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, table.Schema);
            Assert.NotEmpty(rows);
            foreach (var row in rows)
            {
                var computed = spec.Partition(table.Schema, id => row[id - 1]);
                Assert.Equal(task.File.Partition, computed);
            }
        }
    }

    [Fact]
    public async Task Column_metrics_decode_with_single_value_serialisation()
    {
        var table = await Load("partitioned");
        var file = (await TableScan.PlanFilesAsync(table)).First().File;
        var rows = await ParquetFiles.ReadAsync(table.IO, file.FilePath, table.Schema);
        Assert.Equal(rows.Count, file.RecordCount);
        Assert.Equal(rows.Min(r => (long)r[0]!), Conversions.FromBytes(PrimitiveType.Long, file.LowerBounds![1]));
        Assert.Equal(rows.Max(r => (long)r[0]!), Conversions.FromBytes(PrimitiveType.Long, file.UpperBounds![1]));
        Assert.Equal(rows.Count, file.ValueCounts![1]);
    }

    [Fact]
    public async Task Schema_evolution_projects_missing_columns_as_null()
    {
        var table = await Load("evolved");
        Assert.Equal(1, table.Metadata.CurrentSchemaId);
        var rows = await TableScan.ReadAllAsync(table);
        Assert.Equal(6, rows.Count);
        var note = table.Schema.Columns.ToList().FindIndex(c => c.Name == "note");
        Assert.Equal(5, rows.Count(r => r[note] is null));
        Assert.Equal("added", rows.Single(r => (long)r[0]! == 100)[note]);
    }

    [Fact]
    public async Task Manifest_list_and_manifest_metadata_keys_are_present()
    {
        var table = await Load("partitioned");
        var list = ManifestCodec.ReadManifestList(await table.IO.ReadAllAsync(table.Metadata.CurrentSnapshot!.ManifestList!));
        Assert.Equal(table.Metadata.CurrentSnapshotId.ToString(), list.Metadata["snapshot-id"]);
        Assert.Equal("2", list.Metadata["format-version"]);
        var manifest = ManifestCodec.ReadManifest(await table.IO.ReadAllAsync(list.Manifests[0].ManifestPath), table.Spec, table.Schema);
        Assert.Equal("data", manifest.Metadata["content"]);
        Assert.True(ManifestCodec.EmbeddedSpec(manifest.Metadata).CompatibleWith(table.Spec));
    }
}
