using Iceberg.Net.Puffin;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The Mumbling bitmap (apache/iceberg <c>format/mumbling-spec.md</c>, version 1), the encoding of v4 manifest
/// deletion vectors. The PFOR and container vectors are the spec's own examples; whole-bitmap vectors are assembled
/// from them by the spec's layout rules (the spec has no whole-bitmap examples).
/// </summary>
public sealed class MumblingBitmapTests
{
    private static byte[] Hex(string hex) => Convert.FromHexString(hex.Replace(" ", ""));

    public static TheoryData<string, byte[]> PforExamples() => new()
    {
        { "00 00 00", new byte[256] },
        { "00 00 05", Enumerable.Repeat((byte)5, 51).ToArray() },
        { "80 02 00 04 07 FF FE", [0, 0, 0, 0, 0xFF, 0, 0, 0xFE] },
        { "02 00 06 18", [6, 7, 8] },
        { "32 01 06 09 01 E0", [6, 34, 8, 7] },
    };

    [Theory]
    [MemberData(nameof(PforExamples))]
    public void Pfor_spec_examples_decode_and_encode_identically(string hex, byte[] values)
    {
        var bytes = Hex(hex);
        Assert.Equal(values, MumblingPfor.Decode(bytes, values.Length, out var consumed));
        Assert.Equal(bytes.Length, consumed);
        Assert.Equal(bytes, MumblingPfor.Encode(values)); // the spec's recommended width choice reproduces them
    }

    [Fact]
    public void Pfor_packs_most_significant_bit_first()
    {
        // "for bit width b = 2, the array [3, 2, 1, 2, 3] is stored as binary 1110 0110 1100 0000 or hex E6 C0"
        var encoded = Hex("02 00 00 E6 C0");
        Assert.Equal(new byte[] { 3, 2, 1, 2, 3 }, MumblingPfor.Decode(encoded, 5, out _));
    }

    [Fact]
    public void Pfor_round_trips_multiple_chunks_and_full_width_values()
    {
        var rnd = new Random(7);
        foreach (var n in new[] { 0, 1, 255, 256, 257, 700, 8192 })
        {
            foreach (var spread in new[] { 1, 4, 33, 256 })
            {
                var values = new byte[n];
                for (var i = 0; i < n; i++) values[i] = (byte)(rnd.Next(100) < 90 ? rnd.Next(Math.Min(spread, 4)) : rnd.Next(spread));
                var encoded = MumblingPfor.Encode(values);
                Assert.Equal(values, MumblingPfor.Decode(encoded, n, out var consumed));
                Assert.Equal(encoded.Length, consumed);
            }
        }
        // 8-bit values: stored as they are, m = 0, no exceptions.
        var wide = Enumerable.Range(0, 256).Select(i => (byte)(255 - i)).ToArray();
        var w = MumblingPfor.Encode(wide);
        Assert.Equal(new byte[] { 0x08, 0, 0 }, w[..3]);
        Assert.Equal(wide, w[3..]);
    }

    [Fact]
    public void Container_examples_from_the_spec()
    {
        // Sparse, descriptor 3, bytes 00 22 FF: positions 0, 34, 255. Header: version 1, cardinality 3, 1 container;
        // descriptors [3] as PFOR (b1 = 0, m = 3).
        var sparse = Hex("01 030000 0100 000003 0022FF");
        Assert.Equal([0, 34, 255], MumblingBitmap.Deserialize(sparse).Positions());
        Assert.Equal(sparse, MumblingBitmap.Of([0, 34, 255]).Serialize());

        // Dense, FF FF FF FF 80 00 ... : positions 0-32.
        var dense = Hex("01 210000 0100 000020 FFFFFFFF80" + new string('0', 54));
        Assert.Equal(Enumerable.Range(0, 33), MumblingBitmap.Deserialize(dense).Positions());
        Assert.Equal(dense, MumblingBitmap.Of(Enumerable.Range(0, 33)).Serialize());

        // Dense FF FF 00 ... 00 FF FF in the second container: 0-15 and 240-255, offset by 256. The empty first
        // container is still described (descriptor 0): PFOR [0, 0x20] = b1 0, b2 6, one exception at offset 1.
        var second = Hex("01 200000 0200 600100 01 80 FFFF" + new string('0', 56) + "FFFF");
        var expected = Enumerable.Range(256, 16).Concat(Enumerable.Range(256 + 240, 16));
        Assert.Equal(expected, MumblingBitmap.Deserialize(second).Positions());
        Assert.Equal(second, MumblingBitmap.Of(expected).Serialize());

        // Even positions: AA AA ... AA.
        var even = MumblingBitmap.Deserialize(Hex("01 800000 0100 000020" + string.Concat(Enumerable.Repeat("AA", 32))));
        Assert.Equal(Enumerable.Range(0, 128).Select(i => i * 2), even.Positions());
    }

    [Fact]
    public void Empty_and_largest_bitmaps()
    {
        var empty = new MumblingBitmap().Serialize();
        Assert.Equal(Hex("01 000000 0000"), empty);
        Assert.True(MumblingBitmap.Deserialize(empty).IsEmpty);

        var last = MumblingBitmap.Of([MumblingBitmap.MaxPosition]);
        var back = MumblingBitmap.Deserialize(last.Serialize());
        Assert.Equal([2_097_151], back.Positions());
        Assert.Throws<ArgumentOutOfRangeException>(() => new MumblingBitmap().Add(MumblingBitmap.MaxPosition + 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new MumblingBitmap().Add(-1));
    }

    [Fact]
    public void Random_bitmaps_round_trip()
    {
        var rnd = new Random(11);
        for (var run = 0; run < 50; run++)
        {
            var bm = new MumblingBitmap();
            var span = rnd.Next(1, MumblingBitmap.MaxPosition);
            var density = rnd.NextDouble();
            for (var i = rnd.Next(0, 5000); i > 0; i--) bm.Add(rnd.Next(span));
            // A dense run, so both container kinds appear.
            var start = rnd.Next(span) & ~0xFF;
            for (var p = start; p < Math.Min(start + 256, MumblingBitmap.MaxPosition); p++)
                if (rnd.NextDouble() < density) bm.Add(p);
            var back = MumblingBitmap.Deserialize(bm.Serialize());
            Assert.Equal(bm.Positions(), back.Positions());
            Assert.Equal(bm.Cardinality, back.Cardinality);
        }
    }

    [Theory]
    [InlineData("02 000000 0000", typeof(NotSupportedException))] // version 2
    [InlineData("01 000000 0120", typeof(FormatException))] // 8193 containers
    [InlineData("01 030000 0100 000003 2200FF", typeof(FormatException))] // sparse not ascending
    [InlineData("01 020000 0100 000003 0022FF", typeof(FormatException))] // cardinality disagrees
    [InlineData("01 030000 0100 000003 0022FF 00", typeof(FormatException))] // trailing byte
    [InlineData("01 030000 0100 000003 0022", typeof(EndOfStreamException))] // truncated container
    [InlineData("01 030000 0100 000043 0022FF", typeof(FormatException))] // descriptor type 010
    [InlineData("01 010000 0100 000020 8000000000000000000000000000000000000000000000000000000000000000", typeof(FormatException))] // dense with 1 bit
    [InlineData("01 010000 0100 09 00 00", typeof(FormatException))] // PFOR width 9
    [InlineData("01 010000 0100 37 00 00", typeof(FormatException))] // PFOR b2 > 8 - b1
    [InlineData("01 010000 01", typeof(EndOfStreamException))] // header truncated (5 bytes)
    public void Malformed_bitmaps_are_format_errors(string hex, Type error)
    {
        var e = Record.Exception(() => MumblingBitmap.Deserialize(Hex(hex)));
        Assert.NotNull(e);
        Assert.IsType(error, e);
    }

    [Fact]
    public void Dense_descriptor_low_bits_are_ignored()
    {
        // "implementations must ignore the least-significant bits for a dense container if they are set"
        var bytes = Hex("01 200000 0100 00003F FFFFFFFF" + new string('0', 56));
        Assert.Equal(Enumerable.Range(0, 32), MumblingBitmap.Deserialize(bytes).Positions());
    }

    /// <summary>Hostile input never reaches the <c>Corruption.Guard</c> fallback: every rejection is a deliberate check.</summary>
    [Fact]
    public void Corrupted_input_is_rejected_by_explicit_checks()
    {
        var sample = MumblingBitmap.Of(Enumerable.Range(0, 40).Concat([300, 301, 5000, 70_000]).Concat(Enumerable.Range(100_000, 300))).Serialize();
        var rnd = new Random(3);
        for (var run = 0; run < 5000; run++)
        {
            var b = (byte[])sample.Clone();
            switch (rnd.Next(3))
            {
                case 0: b[rnd.Next(b.Length)] ^= (byte)(1 << rnd.Next(8)); break;
                case 1: b = b[..rnd.Next(b.Length)]; break;
                default: b[rnd.Next(b.Length)] = (byte)rnd.Next(256); break;
            }
            try
            {
                MumblingBitmap.Deserialize(b);
            }
            catch (InvalidDataException e) when (e.InnerException is not null)
            {
                Assert.Fail($"run {run}: fell through to the guard: {e.InnerException.GetType().Name}: {e.InnerException.Message}");
            }
            catch (Exception e) when (e is FormatException or EndOfStreamException or NotSupportedException)
            {
            }
        }
    }
}
