using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace Iceberg.Net.Transforms;

/// <summary>
/// 32-bit Murmur3 (x86 variant, seed 0) and the per-type hashing rules from spec Appendix B.
/// Every Iceberg implementation must produce identical bucket numbers, so this is pinned by the
/// spec's test vectors in <c>BucketHashSpecTests</c>.
/// </summary>
public static class Murmur3
{
    private const uint C1 = 0xcc9e2d51;
    private const uint C2 = 0x1b873593;

    public static int HashBytes(ReadOnlySpan<byte> data)
    {
        uint h1 = 0;
        var nblocks = data.Length / 4;
        for (var i = 0; i < nblocks; i++)
        {
            var k1 = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i * 4, 4));
            k1 *= C1;
            k1 = BitOperations.RotateLeft(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            h1 = BitOperations.RotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        var tail = data[(nblocks * 4)..];
        uint t = 0;
        switch (tail.Length)
        {
            case 3: t ^= (uint)tail[2] << 16; goto case 2;
            case 2: t ^= (uint)tail[1] << 8; goto case 1;
            case 1:
                t ^= tail[0];
                t *= C1;
                t = BitOperations.RotateLeft(t, 15);
                t *= C2;
                h1 ^= t;
                break;
        }

        h1 ^= (uint)data.Length;
        h1 ^= h1 >> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >> 16;
        return unchecked((int)h1);
    }

    /// <summary>int and long both hash as 8 little-endian bytes, so promotion int to long keeps buckets stable.</summary>
    public static int HashLong(long value)
    {
        Span<byte> buf = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buf, value);
        return HashBytes(buf);
    }

    public static int HashInt(int value) => HashLong(value);

    /// <summary>float/double hash as doubleToLongBits(double(v)); -0.0 normalises to 0.0 and NaN to the canonical NaN.</summary>
    public static int HashDouble(double value)
    {
        if (value == 0.0) value = 0.0;
        if (double.IsNaN(value)) return HashLong(0x7ff8000000000000L);
        return HashLong(BitConverter.DoubleToInt64Bits(value));
    }

    public static int HashString(string value) => HashBytes(Encoding.UTF8.GetBytes(value));

    /// <summary>UUIDs hash as their 16 big-endian bytes. Guid.ToByteArray() is mixed-endian, so never use the default overload.</summary>
    public static int HashUuid(Guid value) => HashBytes(value.ToByteArray(bigEndian: true));

    /// <summary>Decimals hash the minimum-length two's-complement big-endian bytes of the unscaled value.</summary>
    public static int HashDecimal(BigInteger unscaled) => HashBytes(unscaled.ToByteArray(isUnsigned: false, isBigEndian: true));
}
