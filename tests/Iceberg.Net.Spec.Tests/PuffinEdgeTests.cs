using System.Buffers.Binary;
using System.Text;
using Iceberg.Net.Puffin;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Edge cases the Stryker baseline showed were untested (mutants survived): Theta sketch estimation-mode invariants
/// and merge semantics, and the exact error each malformed Puffin input produces.
/// </summary>
[Spec("puffin: apache-datasketches-theta-v1 blob type")]
public sealed class PuffinEdgeTests
{
    private static ThetaSketch Sketch(int from, int count, int k = 1024)
    {
        var s = new ThetaSketch(k);
        for (var i = from; i < from + count; i++) s.Update(BitConverter.GetBytes((long)i));
        return s;
    }

    [Theory]
    [InlineData(15)]
    [InlineData(24)]
    [InlineData(0)]
    public void Nominal_entries_must_be_a_power_of_two_of_at_least_16(int k) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new ThetaSketch(k));

    [Fact]
    public void Estimation_mode_keeps_k_entries_below_a_falling_theta()
    {
        var s = new ThetaSketch(1024);
        var theta = s.ThetaLong;
        for (var i = 0; i < 100_000; i++)
        {
            s.Update(BitConverter.GetBytes((long)i));
            Assert.True(s.ThetaLong <= theta, "theta never rises");
            theta = s.ThetaLong;
            if (i % 997 == 0) Assert.True(s.Retained <= 1024);
        }
        Assert.Equal(1024, s.Retained);
        Assert.InRange(s.Estimate, 90_000, 110_000); // relative standard error ≈ 1/√k ≈ 3%
        s.Update(BitConverter.GetBytes(5L)); // duplicates change nothing
        Assert.Equal(theta, s.ThetaLong);
        var back = ThetaSketch.Deserialize(s.Serialize());
        Assert.Equal(s.ThetaLong, back.ThetaLong);
        Assert.Equal(1024, back.Retained);
    }

    [Fact]
    public void Empty_updates_are_ignored()
    {
        var s = new ThetaSketch();
        s.Update([]);
        Assert.True(s.IsEmpty);
        Assert.Equal(8, s.Serialize().Length);
    }

    [Fact]
    public void Merge_is_a_commutative_union_with_the_smaller_theta()
    {
        var a = Sketch(0, 60_000);
        var b = Sketch(60_000, 40_000);
        var small = Sketch(500_000, 10); // exact mode

        // A compact sketch does not record its nominal entries, so unions are built with an explicit k.
        var ab = new ThetaSketch(1024);
        ab.Merge(a);
        ab.Merge(b);
        var ba = new ThetaSketch(1024);
        ba.Merge(b);
        ba.Merge(a);
        Assert.Equal(ab.Serialize(), ba.Serialize());
        Assert.True(ab.ThetaLong <= Math.Min(a.ThetaLong, b.ThetaLong));
        Assert.True(ab.Retained <= 1024);
        Assert.InRange(ab.Estimate, 90_000, 110_000);

        var exactIntoEstimation = ThetaSketch.Deserialize(a.Serialize());
        exactIntoEstimation.Merge(small);
        Assert.Equal(a.ThetaLong, exactIntoEstimation.ThetaLong); // the exact sketch cannot raise theta
        var estimationIntoExact = ThetaSketch.Deserialize(small.Serialize());
        estimationIntoExact.Merge(a);
        Assert.Equal(exactIntoEstimation.Serialize(), estimationIntoExact.Serialize());

        var empty = new ThetaSketch(1024);
        var copy = ThetaSketch.Deserialize(a.Serialize());
        copy.Merge(empty);
        Assert.Equal(a.Serialize(), copy.Serialize());
        empty.Merge(small);
        Assert.False(empty.IsEmpty);
        Assert.Equal(10, empty.Retained);
        Assert.Throws<ArgumentException>(() => a.Merge(new ThetaSketch(1024, seed: 1)));
    }

    [Fact]
    public void Malformed_sketches_fail_with_specific_errors()
    {
        var good = Sketch(0, 10).Serialize();
        Assert.IsType<FormatException>(Record.Exception(() => ThetaSketch.Deserialize(good.AsSpan(0, 7))));
        var v2 = (byte[])good.Clone();
        v2[1] = 2;
        Assert.IsType<NotSupportedException>(Record.Exception(() => ThetaSketch.Deserialize(v2)));
        var family = (byte[])good.Clone();
        family[2] = 2;
        Assert.IsType<NotSupportedException>(Record.Exception(() => ThetaSketch.Deserialize(family)));
        Assert.IsType<FormatException>(Record.Exception(() => ThetaSketch.Deserialize(good, seed: 1))); // seed hash
    }

    private static byte[] Puffin(IReadOnlyDictionary<string, string>? properties = null, IReadOnlyDictionary<string, string>? blobProperties = null) =>
        PuffinFile.Write([new PuffinBlob(PuffinBlobTypes.ThetaSketchV1, [1], Sketch(0, 3).Serialize(), 1, 1, Properties: blobProperties)], properties).Bytes;

    [Fact]
    [Spec("puffin: Footer structure")]
    public void Malformed_puffin_files_fail_with_specific_errors()
    {
        var good = Puffin();
        var badMagic = (byte[])good.Clone();
        badMagic[0] = (byte)'X';
        Assert.Contains("bad magic", Assert.IsType<FormatException>(Record.Exception(() => PuffinFile.Read(badMagic))).Message);
        var badTail = (byte[])good.Clone();
        badTail[^1] = (byte)'X';
        Assert.IsType<FormatException>(Record.Exception(() => PuffinFile.Read(badTail)));
        Assert.IsType<FormatException>(Record.Exception(() => PuffinFile.Read(good[..15])));

        var lz4 = (byte[])good.Clone();
        lz4[^8] = 1; // footer flags bit 0: payload compressed
        Assert.IsType<NotSupportedException>(Record.Exception(() => PuffinFile.Read(lz4)));

        var hugeFooter = (byte[])good.Clone();
        BinaryPrimitives.WriteInt32LittleEndian(hugeFooter.AsSpan(hugeFooter.Length - 12), good.Length);
        Assert.Contains("footer", Assert.IsType<FormatException>(Record.Exception(() => PuffinFile.Read(hugeFooter))).Message);

        var contents = PuffinFile.Read(good);
        var outside = contents.Blobs[0] with { Offset = good.Length - 2 };
        Assert.Contains("outside", Assert.IsType<InvalidDataException>(Record.Exception(() => contents.Payload(outside))).Message);
        Assert.IsType<InvalidDataException>(Record.Exception(() => contents.Payload(contents.Blobs[0] with { Length = -1 })));
        Assert.IsType<InvalidDataException>(Record.Exception(() => contents.Payload(contents.Blobs[0] with { Offset = -1 })));
    }

    [Fact]
    [Spec("puffin: Footer Payload")]
    public void File_and_blob_properties_round_trip_and_default()
    {
        Assert.Equal("iceberg-net", PuffinFile.Read(Puffin()).Properties["created-by"]);
        var custom = PuffinFile.Read(Puffin(new Dictionary<string, string> { ["created-by"] = "test", ["x"] = "y" }));
        Assert.Equal("y", custom.Properties["x"]);
        Assert.Null(PuffinFile.Read(Puffin(blobProperties: new Dictionary<string, string>())).Blobs[0].Properties); // empty: omitted
        Assert.Equal("v", PuffinFile.Read(Puffin(blobProperties: new Dictionary<string, string> { ["k"] = "v" })).Blobs[0].Properties!["k"]);
        var noProps = Encoding.UTF8.GetString(Puffin(blobProperties: new Dictionary<string, string>()));
        Assert.DoesNotContain("\"k\"", noProps);
    }

    [Fact]
    [Spec("puffin: deletion-vector-v1 blob type")]
    public void Deletion_vectors_must_not_be_compressed() =>
        Assert.Throws<ArgumentException>(() => PuffinFile.Write([new PuffinBlob(PuffinBlobTypes.DeletionVectorV1, [], new byte[] { 1 }, CompressionCodec: "zstd")]));
}
