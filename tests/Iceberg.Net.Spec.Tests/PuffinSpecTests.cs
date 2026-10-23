using System.Text.Json.Nodes;
using Iceberg.Net.Puffin;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Puffin, deletion vectors and Theta sketches against reference bytes produced by the official
/// libraries (Apache DataSketches, pyroaring/CRoaring, zlib) via interop/pyiceberg_oracle.py puffin-fixtures.
/// </summary>
public class PuffinSpecTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "fixtures", "puffin");
    private static readonly JsonNode Expected = JsonNode.Parse(File.ReadAllText(Path.Combine(Dir, "expected.json")))!;
    private static byte[] Fixture(string name) => File.ReadAllBytes(Path.Combine(Dir, name));

    private static IEnumerable<long> ExpectedPositions() => Expected["dv"]!["ranges"]!.AsArray()
        .SelectMany(r => Enumerable.Range(0, (int)(r![1]!.GetValue<long>() - r[0]!.GetValue<long>())).Select(i => r[0]!.GetValue<long>() + i));

    [Spec("puffin: apache-datasketches-theta-v1 blob type")]
    [Fact]
    public void Murmur3_x64_128_and_seed_hash_match_datasketches()
    {
        Assert.Equal((ushort)0x93CC, ThetaSketch.SeedHash(ThetaSketch.DefaultSeed));
        // Hash of "iceberg" as serialized by datasketches (single-item compact sketch, bytes 8..16).
        var sketch = new ThetaSketch();
        sketch.Update("iceberg"u8);
        Assert.Equal(Fixture("theta-single.bin"), sketch.Serialize());
        Assert.Equal(0x032E42766A92E119L, BitConverter.ToInt64(sketch.Serialize(), 8));
    }

    [Spec("puffin: apache-datasketches-theta-v1 blob type")]
    [Theory]
    [InlineData("empty", 0)]
    [InlineData("single", 1)]
    [InlineData("exact3", 3)]
    public void Exact_mode_sketches_are_byte_identical_to_datasketches(string name, int n)
    {
        var values = name == "single" ? ["iceberg"] : Enumerable.Range(0, n).Select(i => $"v{i}").ToArray();
        var sketch = new ThetaSketch();
        foreach (var v in values) sketch.Update(System.Text.Encoding.UTF8.GetBytes(v));
        Assert.Equal(Fixture($"theta-{name}.bin"), sketch.Serialize());
    }

    [Spec("puffin: apache-datasketches-theta-v1 blob type")]
    [Fact]
    public void Estimation_mode_sketch_from_datasketches_deserialises_and_estimates()
    {
        var e = Expected["theta"]!["est10k"]!;
        var theirs = ThetaSketch.Deserialize(Fixture("theta-est10k.bin"));
        Assert.Equal(e["retained"]!.GetValue<int>(), theirs.Retained);
        Assert.Equal(e["theta64"]!.GetValue<long>(), theirs.ThetaLong);
        Assert.Equal(e["estimate"]!.GetValue<double>(), theirs.Estimate, 6);

        // Our KMV sketch over the same values lands within the sketch's error bound (k=4096: ~1.6% RSE).
        var mine = new ThetaSketch();
        for (var i = 0; i < 10_000; i++) mine.Update(System.Text.Encoding.UTF8.GetBytes($"v{i}"));
        Assert.InRange(mine.Estimate, 10_000 * 0.95, 10_000 * 1.05);
        Assert.Equal(4096, mine.Retained);
        var back = ThetaSketch.Deserialize(mine.Serialize());
        Assert.Equal(mine.Estimate, back.Estimate, 9);
    }

    [Spec("puffin: apache-datasketches-theta-v1 blob type")]
    [Fact]
    public void Merged_sketches_estimate_the_union()
    {
        var a = new ThetaSketch();
        var b = new ThetaSketch();
        for (var i = 0; i < 6000; i++) a.Update(BitConverter.GetBytes((long)i));
        for (var i = 3000; i < 9000; i++) b.Update(BitConverter.GetBytes((long)i));
        a.Merge(b);
        Assert.InRange(a.Estimate, 9000 * 0.95, 9000 * 1.05);
    }

    [Spec("puffin: deletion-vector-v1 blob type")]
    [Spec("Deletion Vectors")]
    [Fact]
    public void Reads_deletion_vectors_written_by_pyroaring_including_run_containers()
    {
        var puffin = PuffinFile.Read(Fixture("dv.puffin"));
        Assert.Equal(2, puffin.Blobs.Count);
        foreach (var blob in puffin.Blobs)
        {
            Assert.Equal(PuffinBlobTypes.DeletionVectorV1, blob.Type);
            var dv = DeletionVector.Decode(puffin.Payload(blob));
            Assert.Equal(Expected["dv"]!["cardinality"]!.GetValue<long>(), dv.Cardinality);
            Assert.Equal(ExpectedPositions(), dv.Positions());
            Assert.Equal(blob.Properties![DeletionVector.CardinalityProperty], dv.Cardinality.ToString());
        }
    }

    [Spec("puffin: deletion-vector-v1 blob type")]
    [Fact]
    public void Deletion_vector_round_trip_and_crc_detection()
    {
        var bm = new RoaringPositionBitmap();
        bm.AddRange(ExpectedPositions());
        bm.Add(long.MaxValue >> 1);
        var blob = DeletionVector.Encode(bm);
        Assert.Equal(bm.Positions(), DeletionVector.Decode(blob).Positions());

        blob[^5] ^= 0xFF; // flip a byte inside the vector
        Assert.Throws<FormatException>(() => DeletionVector.Decode(blob));
        Assert.Throws<ArgumentOutOfRangeException>(() => bm.Add(-1));
    }

    [Spec("puffin: deletion-vector-v1 blob type")]
    [Fact]
    public void Bitmap_switches_container_types_at_4096_values()
    {
        var bm = new RoaringPositionBitmap();
        for (long i = 0; i < 4096; i++) bm.Add(i * 2);
        var arraySize = bm.Serialize().Length;
        bm.Add(1);
        var bitmapSize = bm.Serialize().Length;
        Assert.Equal(8 + 4 + 8 + 4 + 4 + 4096 * 2, arraySize);   // count, key, cookie+size, key/card, offset, array
        Assert.Equal(8 + 4 + 8 + 4 + 4 + 8192, bitmapSize);       // ... 65536-bit bitmap
        Assert.Equal(4097, RoaringPositionBitmap.Deserialize(bm.Serialize()).Cardinality);
    }

    [Spec("puffin: File structure")]
    [Spec("puffin: Footer Payload")]
    [Spec("puffin: BlobMetadata")]
    [Spec("puffin: Compression codecs")]
    [Fact]
    public void Puffin_container_round_trip_with_zstd_blob_and_offsets()
    {
        var theta = new ThetaSketch();
        theta.Update("x"u8);
        var written = PuffinFile.Write(
        [
            new PuffinBlob(PuffinBlobTypes.ThetaSketchV1, [1], theta.Serialize(), 42, 7, "zstd", new Dictionary<string, string> { ["ndv"] = "1" }),
            DeletionVector.ToBlob("s3://b/f.parquet", PositionsOf(1, 2, 3)),
        ]);
        Assert.Equal(4, written.Blobs[0].Offset);
        var read = PuffinFile.Read(written.Bytes);
        Assert.Equal("iceberg-net", read.Properties["created-by"]);
        Assert.Equal(theta.Serialize(), read.Payload(read.Blobs[0]));
        Assert.Equal("zstd", read.Blobs[0].CompressionCodec);
        Assert.Equal([1L, 2L, 3L], DeletionVector.Decode(read.Payload(read.Blobs[1])).Positions());
        Assert.Equal(-1, read.Blobs[1].SnapshotId);
        Assert.Throws<ArgumentException>(() => PuffinFile.Write([new PuffinBlob(PuffinBlobTypes.DeletionVectorV1, [], new byte[1], CompressionCodec: "zstd")]));
    }

    private static RoaringPositionBitmap PositionsOf(params long[] p)
    {
        var bm = new RoaringPositionBitmap();
        bm.AddRange(p);
        return bm;
    }
}
