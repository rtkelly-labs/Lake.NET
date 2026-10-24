using System.Buffers.Binary;
using System.Numerics;

namespace Iceberg.Net.Puffin;

/// <summary>
/// A Mumbling bitmap (apache/iceberg <c>format/mumbling-spec.md</c>, version 1): the bounded-size bitmap that v4
/// manifest deletion vectors use. Positions are 0 to 2,097,151, in up to 8,192 containers of 256 bits, each sparse
/// (0–31 sorted byte offsets) or dense (32 bytes, most significant bit first). The layout is a 6-byte header (version
/// <c>0x01</c>, 3-byte cardinality, 2-byte container count, little-endian), the container descriptors PFOR-encoded
/// (<see cref="MumblingPfor"/>), then the containers.
/// </summary>
/// <remarks>
/// <see cref="Deserialize"/> follows the <c>Corruption</c> contract and accepts only what the spec allows: a descriptor
/// type other than sparse/dense, a sparse container that is not strictly ascending, a dense container with fewer than
/// 32 bits set (it must be sparse), a cardinality that disagrees with the containers, and trailing bytes are all
/// format errors.
/// </remarks>
public sealed class MumblingBitmap
{
    public const int FormatVersion = 1;
    public const int MaxContainers = 8192;
    public const int ContainerBits = 256;
    /// <summary>The largest position a Mumbling bitmap can hold.</summary>
    public const int MaxPosition = MaxContainers * ContainerBits - 1;

    private const int HeaderSize = 6;
    private const int DenseBytes = 32;
    private const int DenseDescriptor = 0x20;

    private readonly SortedSet<int> _positions = [];

    public int Cardinality => _positions.Count;
    public bool IsEmpty => _positions.Count == 0;

    public static MumblingBitmap Of(IEnumerable<int> positions)
    {
        var bm = new MumblingBitmap();
        foreach (var p in positions) bm.Add(p);
        return bm;
    }

    public void Add(int position)
    {
        if (position is < 0 or > MaxPosition) throw new ArgumentOutOfRangeException(nameof(position), $"Mumbling bitmap positions are 0..{MaxPosition}");
        _positions.Add(position);
    }

    public bool Contains(int position) => _positions.Contains(position);

    /// <summary>Set positions, ascending.</summary>
    public IEnumerable<int> Positions() => _positions;

    public byte[] Serialize()
    {
        var containers = _positions.Count == 0 ? 0 : (_positions.Max >> 8) + 1;
        var counts = new int[containers];
        foreach (var p in _positions) counts[p >> 8]++;
        var descriptors = new byte[containers];
        for (var c = 0; c < containers; c++) descriptors[c] = (byte)(counts[c] < DenseBytes ? counts[c] : DenseDescriptor);

        using var ms = new MemoryStream();
        Span<byte> header = stackalloc byte[HeaderSize];
        header[0] = FormatVersion;
        header[1] = (byte)_positions.Count;
        header[2] = (byte)(_positions.Count >> 8);
        header[3] = (byte)(_positions.Count >> 16);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], (ushort)containers);
        ms.Write(header);
        ms.Write(MumblingPfor.Encode(descriptors));

        using var e = _positions.GetEnumerator();
        var more = e.MoveNext();
        Span<byte> dense = stackalloc byte[DenseBytes];
        for (var c = 0; c < containers; c++)
        {
            if (descriptors[c] == DenseDescriptor)
            {
                dense.Clear();
                for (; more && e.Current >> 8 == c; more = e.MoveNext()) dense[(e.Current & 0xFF) >> 3] |= (byte)(0x80 >> (e.Current & 7));
                ms.Write(dense);
            }
            else
            {
                for (; more && e.Current >> 8 == c; more = e.MoveNext()) ms.WriteByte((byte)e.Current);
            }
        }
        return ms.ToArray();
    }

    public static MumblingBitmap Deserialize(ReadOnlySpan<byte> data) => Corruption.Guard("Mumbling bitmap", data, DeserializeUnguarded);

    private static MumblingBitmap DeserializeUnguarded(ReadOnlySpan<byte> data)
    {
        if (data.Length < HeaderSize) throw new EndOfStreamException($"A Mumbling bitmap header is {HeaderSize} bytes, got {data.Length}");
        if (data[0] != FormatVersion) throw new NotSupportedException($"Mumbling bitmap version {data[0]} is not supported (only {FormatVersion})");
        var cardinality = data[1] | data[2] << 8 | data[3] << 16;
        int containers = BinaryPrimitives.ReadUInt16LittleEndian(data[4..]);
        if (containers > MaxContainers) throw new FormatException($"Mumbling container count {containers} exceeds {MaxContainers}");
        if (cardinality > containers * ContainerBits) throw new FormatException($"Mumbling cardinality {cardinality} exceeds what {containers} containers hold");

        var descriptors = MumblingPfor.Decode(data[HeaderSize..], containers, out var consumed);
        var pos = HeaderSize + consumed;
        var result = new MumblingBitmap();
        for (var c = 0; c < containers; c++)
        {
            var d = descriptors[c];
            var key = c << 8;
            switch (d >> 5)
            {
                case 0: // sparse: d & 0x1f sorted offsets
                {
                    var n = d & 0x1F;
                    var offsets = Take(data, ref pos, n, c);
                    for (var i = 0; i < n; i++)
                    {
                        if (i > 0 && offsets[i] <= offsets[i - 1]) throw new FormatException($"Mumbling sparse container {c} is not strictly ascending");
                        result._positions.Add(key | offsets[i]);
                    }
                    break;
                }
                case 1: // dense; the low descriptor bits must be ignored
                {
                    var bits = Take(data, ref pos, DenseBytes, c);
                    var set = 0;
                    for (var i = 0; i < DenseBytes; i++) set += BitOperations.PopCount(bits[i]);
                    if (set < DenseBytes) throw new FormatException($"Mumbling container {c} is dense with {set} bits set; fewer than 32 must be sparse");
                    for (var i = 0; i < DenseBytes; i++)
                        for (var b = 0; b < 8; b++)
                            if ((bits[i] & (0x80 >> b)) != 0) result._positions.Add(key | i << 3 | b);
                    break;
                }
                default:
                    throw new FormatException($"Mumbling descriptor 0x{d:x2} of container {c} has an unknown container type");
            }
            if (result._positions.Count > cardinality) throw new FormatException($"Mumbling bitmap holds more positions than its cardinality {cardinality}");
        }
        if (result._positions.Count != cardinality) throw new FormatException($"Mumbling cardinality {cardinality} but the containers hold {result._positions.Count}");
        if (pos != data.Length) throw new FormatException($"{data.Length - pos} bytes after the last Mumbling container");
        return result;
    }

    private static ReadOnlySpan<byte> Take(ReadOnlySpan<byte> data, ref int pos, int n, int container)
    {
        if (data.Length - pos < n) throw new EndOfStreamException($"Mumbling container {container} needs {n} bytes, {data.Length - pos} left");
        var s = data.Slice(pos, n);
        pos += n;
        return s;
    }
}

/// <summary>
/// The patched frame-of-reference encoding for unsigned bytes from the Mumbling spec (Appendix A): values in chunks of
/// 256, each chunk a 3-byte header (<c>b1</c> | <c>b2</c> &lt;&lt; 4, exception count <c>e</c>, offset <c>m</c>), the
/// low <c>b1</c> bits of every value minus <c>m</c>, the <c>e</c> exception positions, and the exceptions' remaining
/// <c>b2</c> bits. Bit-packing is most significant bit first, zero-padded to a byte.
/// </summary>
public static class MumblingPfor
{
    public const int ChunkSize = 256;

    public static byte[] Encode(ReadOnlySpan<byte> values)
    {
        using var ms = new MemoryStream();
        for (var start = 0; start < values.Length; start += ChunkSize)
            EncodeChunk(values.Slice(start, Math.Min(ChunkSize, values.Length - start)), ms);
        return ms.ToArray();
    }

    private static int Width(int v) => v == 0 ? 0 : 32 - BitOperations.LeadingZeroCount((uint)v);

    private static void EncodeChunk(ReadOnlySpan<byte> chunk, MemoryStream ms)
    {
        var n = chunk.Length;
        int m = 255;
        foreach (var v in chunk) m = Math.Min(m, v);
        var counts = new int[9];
        var maxWidth = 0;
        foreach (var v in chunk)
        {
            var w = Width(v - m);
            counts[w]++;
            maxWidth = Math.Max(maxWidth, w);
        }

        // The spec's recommended choice: the primary width with the smallest total size (the narrowest on a tie).
        int b1 = maxWidth, best = int.MaxValue;
        for (var b = 0; b <= maxWidth; b++)
        {
            var e = 0;
            for (var w = b + 1; w <= 8; w++) e += counts[w];
            if (e > byte.MaxValue) continue; // the exception count is one byte
            var size = Bytes(n * b) + e + Bytes(e * (maxWidth - b));
            if (size < best) (best, b1) = (size, b);
        }
        if (b1 == 8) m = 0; // 8-bit values are stored as they are
        var b2 = maxWidth - b1;
        var exceptions = new List<int>();
        for (var i = 0; i < n; i++)
            if (Width(chunk[i] - m) > b1) exceptions.Add(i);

        ms.WriteByte((byte)(b1 | b2 << 4));
        ms.WriteByte((byte)exceptions.Count);
        ms.WriteByte((byte)m);
        var primary = new BitWriter(Bytes(n * b1));
        for (var i = 0; i < n; i++) primary.Write(chunk[i] - m & (1 << b1) - 1, b1);
        ms.Write(primary.Bytes);
        foreach (var i in exceptions) ms.WriteByte((byte)i);
        var high = new BitWriter(Bytes(exceptions.Count * b2));
        foreach (var i in exceptions) high.Write(chunk[i] - m >> b1, b2);
        ms.Write(high.Bytes);
    }

    /// <summary>Decodes <paramref name="count"/> values; <paramref name="consumed"/> is the number of bytes they took.</summary>
    public static byte[] Decode(ReadOnlySpan<byte> data, int count, out int consumed)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        // Every chunk has a 3-byte header, so a count the data cannot hold is caught before allocating.
        if ((count + ChunkSize - 1) / ChunkSize > data.Length / 3) throw new EndOfStreamException($"PFOR data too short for {count} values");
        var values = new byte[count];
        var pos = 0;
        for (var start = 0; start < count; start += ChunkSize)
            pos += DecodeChunk(data[pos..], values.AsSpan(start, Math.Min(ChunkSize, count - start)));
        consumed = pos;
        return values;
    }

    private static int DecodeChunk(ReadOnlySpan<byte> data, Span<byte> values)
    {
        var n = values.Length;
        if (data.Length < 3) throw new EndOfStreamException("PFOR chunk header truncated");
        int b1 = data[0] & 0xF, b2 = data[0] >> 4, e = data[1], m = data[2];
        if (b1 > 8) throw new FormatException($"PFOR primary width {b1} exceeds 8");
        if (b2 > 8 - b1) throw new FormatException($"PFOR exception width {b2} exceeds 8 - {b1}");
        if (b1 == 8 && e != 0) throw new FormatException("PFOR chunk with 8-bit values cannot have exceptions");
        if (e > n) throw new FormatException($"PFOR chunk has {e} exceptions for {n} values");
        var primaryLength = Bytes(n * b1);
        var highLength = Bytes(e * b2);
        if (data.Length - 3 < primaryLength + e + highLength) throw new EndOfStreamException("PFOR chunk truncated");

        var primary = new BitReader(data.Slice(3, primaryLength));
        var decoded = new int[n];
        for (var i = 0; i < n; i++) decoded[i] = primary.Read(b1);
        var offsets = data.Slice(3 + primaryLength, e);
        var high = new BitReader(data.Slice(3 + primaryLength + e, highLength));
        foreach (var at in offsets)
        {
            if (at >= n) throw new FormatException($"PFOR exception offset {at} outside a chunk of {n}");
            decoded[at] |= high.Read(b2) << b1;
        }
        for (var i = 0; i < n; i++)
        {
            var v = decoded[i] + m;
            if (v > byte.MaxValue) throw new FormatException($"PFOR value {v} does not fit in a byte");
            values[i] = (byte)v;
        }
        return 3 + primaryLength + e + highLength;
    }

    private static int Bytes(int bits) => (bits + 7) / 8;

    private sealed class BitWriter(int length)
    {
        public byte[] Bytes { get; } = new byte[length];
        private int _bit;

        public void Write(int value, int width)
        {
            for (var i = width - 1; i >= 0; i--, _bit++)
                if ((value >> i & 1) != 0) Bytes[_bit >> 3] |= (byte)(0x80 >> (_bit & 7));
        }
    }

    private ref struct BitReader(ReadOnlySpan<byte> data)
    {
        private readonly ReadOnlySpan<byte> _data = data;
        private int _bit;

        public int Read(int width)
        {
            var v = 0;
            for (var i = 0; i < width; i++, _bit++) v = v << 1 | (_data[_bit >> 3] >> 7 - (_bit & 7) & 1);
            return v;
        }
    }
}
