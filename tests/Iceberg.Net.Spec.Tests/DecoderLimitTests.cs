using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using Iceberg.Net.Avro;
using Iceberg.Net.Manifests;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Hostile inputs that are well formed enough to pass the structural checks but would make a decoder allocate or loop
/// on a claimed size: each must fail fast with a format error. Random mutation in <see cref="CorruptionTests"/> rarely
/// reaches these, because each needs two coordinated fields (a schema and a count, a header and its runs).
/// </summary>
public sealed class DecoderLimitTests
{
    private static readonly byte[] Sync = new byte[16];

    /// <summary>An Avro object container with one block, built by hand so the count and size can lie.</summary>
    private static byte[] Container(string schemaJson, string codec, long count, byte[] block)
    {
        using var ms = new MemoryStream();
        ms.Write("Obj\u0001"u8);
        AvroBinary.WriteLong(ms, 2);
        AvroBinary.WriteString(ms, "avro.schema");
        AvroBinary.WriteString(ms, schemaJson);
        AvroBinary.WriteString(ms, "avro.codec");
        AvroBinary.WriteString(ms, codec);
        AvroBinary.WriteLong(ms, 0);
        ms.Write(Sync);
        AvroBinary.WriteLong(ms, count);
        AvroBinary.WriteLong(ms, block.Length);
        ms.Write(block);
        ms.Write(Sync);
        return ms.ToArray();
    }

    private static byte[] Deflate(int zeros)
    {
        using var ms = new MemoryStream();
        using (var d = new DeflateStream(ms, CompressionLevel.Optimal, leaveOpen: true))
        {
            var chunk = new byte[1 << 20];
            for (var written = 0; written < zeros; written += chunk.Length) d.Write(chunk, 0, Math.Min(chunk.Length, zeros - written));
        }
        return ms.ToArray();
    }

    [Spec("Avro")]
    [Theory]
    [InlineData("""{"type":"record","name":"r","fields":[]}""")]
    [InlineData("\"null\"")]
    public void An_avro_block_cannot_claim_billions_of_zero_byte_records(string schema)
    {
        var e = Assert.Throws<InvalidDataException>(() => AvroContainer.Read(Container(schema, "null", 1L << 40, [])));
        Assert.Contains("count", e.Message);
    }

    [Spec("Avro")]
    [Fact]
    public void An_avro_block_cannot_claim_more_records_than_its_bytes_hold() =>
        Assert.Throws<InvalidDataException>(() => AvroContainer.Read(Container("""{"type":"record","name":"r","fields":[{"name":"x","type":"long"}]}""", "null", 1000, [0, 0])));

    [Spec("Avro")]
    [Fact]
    public void A_deflate_block_cannot_inflate_past_the_block_limit()
    {
        var bomb = Deflate(AvroBinary.MaxBlockBytes + (1 << 20));
        Assert.True(bomb.Length < 1 << 20); // a small file that would inflate past the limit
        var e = Assert.Throws<InvalidDataException>(() => AvroContainer.Read(Container("""{"type":"record","name":"r","fields":[{"name":"x","type":"long"}]}""", "deflate", 1, bomb)));
        Assert.Contains("decompresses to more than", e.Message);
    }

    [Spec("Avro")]
    [Fact]
    public void A_fixed_size_from_the_file_cannot_size_an_allocation() =>
        Assert.Throws<InvalidDataException>(() => AvroContainer.Read(Container(
            """{"type":"record","name":"r","fields":[{"name":"f","type":{"type":"fixed","name":"f","size":2000000000}}]}""", "null", 1, [0])));

    [Spec("puffin: Compression codecs")]
    [Fact]
    public void A_zstd_blob_cannot_decompress_past_the_blob_limit()
    {
        using var compressed = new MemoryStream();
        using (var z = new ZstdSharp.CompressionStream(compressed, leaveOpen: true))
        {
            var chunk = new byte[1 << 20];
            for (var written = 0L; written <= PuffinFile.MaxDecompressedBlobBytes; written += chunk.Length) z.Write(chunk);
        }
        var file = PuffinFile.Write([new PuffinBlob("apache-datasketches-theta-v1", [1], compressed.ToArray())]);
        var contents = PuffinFile.Read(file.Bytes);
        // Written uncompressed: declare zstd on read, as a tampered footer would.
        var blob = contents.Blobs[0] with { CompressionCodec = "zstd" };
        var e = Assert.Throws<InvalidDataException>(() => contents.Payload(blob));
        Assert.Contains("decompresses to more than", e.ToString());
    }

    /// <summary>A Roaring64 bitmap with one 32-bit bitmap holding one run container.</summary>
    private static byte[] RunBitmap(ushort declaredCardinality, params (ushort Start, ushort Length)[] runs)
    {
        var bytes = new List<byte>();
        void U16(int v) { var b = new byte[2]; BinaryPrimitives.WriteUInt16LittleEndian(b, (ushort)v); bytes.AddRange(b); }
        void U32(uint v) { var b = new byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(b, v); bytes.AddRange(b); }
        bytes.AddRange(BitConverter.GetBytes(1L)); // one 32-bit bitmap
        U32(0); // its key
        U32(12347); // run cookie, one container
        bytes.Add(1); // run flags: container 0 is a run container
        U16(0); // container key
        U16(declaredCardinality - 1);
        U16(runs.Length);
        foreach (var (start, length) in runs) { U16(start); U16(length); }
        return [.. bytes];
    }

    [Spec("Deletion Vectors")]
    [Fact]
    public void A_valid_run_container_decodes()
    {
        var bitmap = RoaringPositionBitmap.Deserialize(RunBitmap(13, (2, 2), (10, 9)));
        Assert.Equal(13, bitmap.Cardinality);
    }

    [Spec("Deletion Vectors")]
    [Theory]
    [InlineData(4, 0, 65535)] // one 4-byte run claiming the whole container against a declared 4
    [InlineData(65535, 65535, 5)] // a run past the end of the container
    public void Run_containers_must_hold_exactly_their_declared_values(int declared, int start, int length) =>
        Assert.Throws<FormatException>(() => RoaringPositionBitmap.Deserialize(RunBitmap((ushort)declared, ((ushort)start, (ushort)length))));

    [Spec("Deletion Vectors")]
    [Fact]
    public void Overlapping_runs_are_refused() =>
        Assert.Throws<FormatException>(() => RoaringPositionBitmap.Deserialize(RunBitmap(6, (0, 2), (2, 2))));

    [Spec("Deletion Vectors")]
    [Fact]
    public void A_deletion_vector_cannot_hold_more_positions_than_its_manifest_entry_claims()
    {
        var positions = new RoaringPositionBitmap();
        for (var p = 0L; p < 100; p++) positions.Add(p);
        var encoded = DeletionVector.Encode(positions);
        Assert.Equal(100, DeletionVector.Decode(encoded, maxCardinality: 100).Cardinality);
        Assert.Throws<FormatException>(() => DeletionVector.Decode(encoded, maxCardinality: 10));
    }

    [Spec("Appendix D: Single-value serialization")]
    [Theory]
    [InlineData("boolean", 0)]
    [InlineData("int", 3)]
    [InlineData("long", 5)]
    [InlineData("float", 2)]
    [InlineData("double", 7)]
    [InlineData("uuid", 15)]
    public void A_bound_of_the_wrong_length_is_a_format_error(string type, int length)
    {
        var t = type switch
        {
            "boolean" => PrimitiveType.Boolean,
            "int" => PrimitiveType.Int,
            "long" => PrimitiveType.Long,
            "float" => PrimitiveType.Float,
            "double" => PrimitiveType.Double,
            _ => PrimitiveType.Uuid,
        };
        Assert.Throws<InvalidDataException>(() => Conversions.FromBytes(t, new byte[length]));
        Assert.Null(BoundSerialization.TryDecode(t, new byte[length])); // never prune on it
    }

    [Fact]
    public void Timestamps_outside_the_datetime_range_are_refused_not_wrapped()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => IcebergTime.TimestampFromMicros(long.MaxValue));
        Assert.Throws<ArgumentOutOfRangeException>(() => IcebergTime.TimestampFromMicros(long.MinValue / 2));
        Assert.Equal(new DateTime(1970, 1, 1, 0, 0, 1, DateTimeKind.Utc), IcebergTime.TimestampFromMicros(1_000_000));
    }
}
