using Iceberg.Net.Avro;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

public class AvroSpecTests
{
    [Theory]
    [InlineData(0L, new byte[] { 0x00 })]
    [InlineData(-1L, new byte[] { 0x01 })]
    [InlineData(1L, new byte[] { 0x02 })]
    [InlineData(-64L, new byte[] { 0x7f })]
    [InlineData(64L, new byte[] { 0x80, 0x01 })]
    public void Zigzag_varints_match_avro_spec(long value, byte[] encoded)
    {
        using var ms = new MemoryStream();
        AvroBinary.WriteLong(ms, value);
        Assert.Equal(encoded, ms.ToArray());
        ms.Position = 0;
        Assert.Equal(value, AvroBinary.ReadLong(ms));
    }

    [Fact]
    public void Schema_json_keeps_iceberg_properties()
    {
        var schema = ManifestCodec.ManifestEntrySchema(2, new StructType(NestedField.Optional(1000, "d", PrimitiveType.Date)));
        var json = AvroSchemaJson.ToJson(schema).ToJsonString();
        Assert.Contains("\"field-id\":102", json);
        Assert.Contains("\"logicalType\":\"map\"", json);
        Assert.Contains("\"element-id\":133", json);
        Assert.Contains("\"name\":\"k117_v118\"", json);
        var reparsed = AvroSchemaJson.Parse(json);
        Assert.Equal(json, AvroSchemaJson.ToJson(reparsed).ToJsonString());
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(9, 4)]
    [InlineData(18, 8)]
    [InlineData(38, 16)]
    public void Decimal_fixed_sizes_match_java(int precision, int bytes) => Assert.Equal(bytes, ManifestCodec.DecimalRequiredBytes(precision));

    [Fact]
    public void Manifest_round_trip_with_every_partition_type()
    {
        var schema = new Schema(
            NestedField.Require(1, "i", PrimitiveType.Int), NestedField.Optional(2, "s", PrimitiveType.String),
            NestedField.Optional(3, "u", PrimitiveType.Uuid), NestedField.Optional(4, "dec", new DecimalType(9, 2)),
            NestedField.Optional(5, "ts", PrimitiveType.Timestamp), NestedField.Optional(6, "b", PrimitiveType.Binary));
        var spec = PartitionSpec.BuilderFor(schema).Identity("i").Truncate("s", 2).Identity("u").Identity("dec").Hour("ts").Identity("b").Build();
        var uuid = Guid.NewGuid();
        var file = new DataFile
        {
            FilePath = "s3://b/t/data/f.parquet",
            Partition = [7, "ab", uuid, IcebergDecimal.Parse("-12.34"), 123, new byte[] { 9, 8 }],
            RecordCount = 3,
            FileSizeInBytes = 100,
            ValueCounts = new Dictionary<int, long> { [1] = 3, [2] = 3 },
            LowerBounds = new Dictionary<int, byte[]> { [1] = Conversions.ToBytes(PrimitiveType.Int, 1) },
            SplitOffsets = [4L],
            SortOrderId = 0,
            FirstRowId = 10,
        };
        var bytes = ManifestCodec.WriteManifest(3, schema, spec, ManifestContent.Data,
            [new ManifestEntry(ManifestEntryStatus.Added, null, null, null, file)]);

        var manifest = new ManifestFile { ManifestPath = "m.avro", ManifestLength = bytes.Length, PartitionSpecId = 0, AddedSnapshotId = 42, SequenceNumber = 5, FirstRowId = 10 };
        var entry = ManifestCodec.ReadManifest(bytes, spec, schema, manifest).Entries.Single();
        Assert.Equal(42, entry.SnapshotId);      // inherited
        Assert.Equal(5, entry.SequenceNumber);   // inherited
        Assert.Equal(5, entry.FileSequenceNumber);
        Assert.Equal(file.Partition, entry.DataFile.Partition);
        Assert.Equal(1, Conversions.FromBytes(PrimitiveType.Int, entry.DataFile.LowerBounds![1]));
        Assert.Equal(3, entry.DataFile.ValueCounts![2]);
        Assert.Equal(10, entry.DataFile.FirstRowId);
        Assert.Equal([4L], entry.DataFile.SplitOffsets);
    }

    [Fact]
    public void Deleted_entries_do_not_inherit_sequence_numbers()
    {
        var schema = new Schema(NestedField.Require(1, "i", PrimitiveType.Int));
        var file = new DataFile { FilePath = "f", RecordCount = 1, FileSizeInBytes = 1 };
        var bytes = ManifestCodec.WriteManifest(2, schema, PartitionSpec.Unpartitioned, ManifestContent.Data,
            [new ManifestEntry(ManifestEntryStatus.Deleted, 9, 1, 1, file), new ManifestEntry(ManifestEntryStatus.Existing, 8, 2, 2, file)]);
        var entries = ManifestCodec.ReadManifest(bytes, PartitionSpec.Unpartitioned, schema,
            new ManifestFile { ManifestPath = "m", ManifestLength = 1, PartitionSpecId = 0, AddedSnapshotId = 99, SequenceNumber = 7 }).Entries;
        Assert.Equal([1L, 2L], entries.Select(e => e.SequenceNumber!.Value));
        Assert.Equal([9L, 8L], entries.Select(e => e.SnapshotId!.Value));
    }

    [Fact]
    public void Manifest_list_round_trip()
    {
        var mf = new ManifestFile
        {
            ManifestPath = "s3://b/m0.avro", ManifestLength = 1234, PartitionSpecId = 1, Content = ManifestContent.Deletes,
            SequenceNumber = 4, MinSequenceNumber = 2, AddedSnapshotId = 77, AddedFilesCount = 1, ExistingFilesCount = 2, DeletedFilesCount = 3,
            AddedRowsCount = 10, ExistingRowsCount = 20, DeletedRowsCount = 30,
            Partitions = [new FieldSummary(true, null, [1, 0, 0, 0], [9, 0, 0, 0])],
        };
        var bytes = ManifestCodec.WriteManifestList(2, 77, 66, 4, [mf]);
        var back = ManifestCodec.ReadManifestList(bytes);
        Assert.Equal("66", back.Metadata["parent-snapshot-id"]);
        var r = back.Manifests.Single();
        Assert.Equal(mf with { Partitions = null }, r with { Partitions = null });
        Assert.Equal(mf.Partitions[0].UpperBound, r.Partitions![0].UpperBound);
        Assert.True(r.Partitions[0].ContainsNull);
    }

    [Fact]
    public void Container_handles_multiple_blocks()
    {
        var schema = new AvroRecord("r", [new AvroField("x", AvroSchema.Long, 1), new AvroField("m", new AvroMap(AvroSchema.String))]);
        var records = Enumerable.Range(0, 2500).Select(i => (object?)new object?[] { (long)i, new Dictionary<string, object?> { ["k"] = $"v{i}" } }).ToList();
        var bytes = AvroContainer.Write(schema, new Dictionary<string, string> { ["a"] = "b" }, records, blockSize: 1000);
        var c = AvroContainer.Read(bytes);
        Assert.Equal(2500, c.Records.Count);
        Assert.Equal(2499L, ((object?[])c.Records[^1]!)[0]);
        Assert.Equal("b", c.Metadata["a"]);
        Assert.Equal("deflate", c.Metadata["avro.codec"]);
    }

    [Fact]
    public void Single_value_serialisation_matches_spec_appendix_d()
    {
        Assert.Equal(new byte[] { 0x01, 0, 0, 0 }, Conversions.ToBytes(PrimitiveType.Int, 1));
        Assert.Equal(new byte[] { 0x01, 0, 0, 0, 0, 0, 0, 0 }, Conversions.ToBytes(PrimitiveType.Long, 1L));
        Assert.Equal("iceberg"u8.ToArray(), Conversions.ToBytes(PrimitiveType.String, "iceberg"));
        Assert.Equal(new byte[] { 0x01 }, Conversions.ToBytes(PrimitiveType.Boolean, true));
        // Decimals: unscaled two's complement, minimum big-endian bytes.
        Assert.Equal(new byte[] { 0x05, 0x8C }, Conversions.ToBytes(new DecimalType(9, 2), IcebergDecimal.Parse("14.20")));
        Assert.Equal(new byte[] { 0xFA, 0x74 }, Conversions.ToBytes(new DecimalType(9, 2), IcebergDecimal.Parse("-14.20")));
        var g = Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7");
        Assert.Equal(0xF7, Conversions.ToBytes(PrimitiveType.Uuid, g)[0]);
        Assert.Equal(g, Conversions.FromBytes(PrimitiveType.Uuid, Conversions.ToBytes(PrimitiveType.Uuid, g)));
        // int -> long promotion reads 4-byte bounds.
        Assert.Equal(7L, Conversions.FromBytes(PrimitiveType.Long, Conversions.ToBytes(PrimitiveType.Int, 7)));
    }
}
