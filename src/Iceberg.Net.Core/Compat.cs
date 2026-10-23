using System.Buffers.Binary;
using System.Numerics;

namespace Iceberg.Net;

/// <summary>
/// The few BCL members Core and Avro use that netstandard2.0 lacks, behind one call each: on .NET 8 they forward to
/// the BCL, on netstandard2.0 they are implemented here. (Instance-style gaps are extension methods in
/// <c>Polyfills.cs</c>, so call sites stay unchanged.)
/// </summary>
internal static class Compat
{
    public static Dictionary<TKey, TValue> Copy<TKey, TValue>(IReadOnlyDictionary<TKey, TValue> source) where TKey : notnull =>
        source.ToDictionary(kv => kv.Key, kv => kv.Value);

#if NET6_0_OR_GREATER
    public static bool IsFinite(double d) => double.IsFinite(d);
    public static bool IsFinite(float f) => float.IsFinite(f);
    public static string ToHex(byte[] b) => Convert.ToHexString(b);
    public static byte[] FromHex(string s) => Convert.FromHexString(s);
    public static uint RotateLeft(uint v, int n) => BitOperations.RotateLeft(v, n);
    public static void WriteSingle(Span<byte> b, float f) => BinaryPrimitives.WriteSingleLittleEndian(b, f);
    public static void WriteDouble(Span<byte> b, double d) => BinaryPrimitives.WriteDoubleLittleEndian(b, d);
    public static float ReadSingle(ReadOnlySpan<byte> b) => BinaryPrimitives.ReadSingleLittleEndian(b);
    public static double ReadDouble(ReadOnlySpan<byte> b) => BinaryPrimitives.ReadDoubleLittleEndian(b);
    public static Guid GuidFromBigEndian(ReadOnlySpan<byte> b) => new(b, bigEndian: true);
    public static byte[] RandomBytes(int n) => System.Security.Cryptography.RandomNumberGenerator.GetBytes(n);
    public static BigInteger SignedBigEndian(ReadOnlySpan<byte> b) => new(b, isUnsigned: false, isBigEndian: true);
#else
    public static bool IsFinite(double d) => !double.IsNaN(d) && !double.IsInfinity(d);
    public static bool IsFinite(float f) => !float.IsNaN(f) && !float.IsInfinity(f);

    public static string ToHex(byte[] b)
    {
        var chars = new char[b.Length * 2];
        for (var i = 0; i < b.Length; i++)
        {
            chars[2 * i] = "0123456789ABCDEF"[b[i] >> 4];
            chars[2 * i + 1] = "0123456789ABCDEF"[b[i] & 0xF];
        }
        return new string(chars);
    }

    public static byte[] FromHex(string s)
    {
        if (s.Length % 2 != 0) throw new FormatException("Hex string has an odd length");
        var b = new byte[s.Length / 2];
        for (var i = 0; i < b.Length; i++) b[i] = (byte)(Nibble(s[2 * i]) << 4 | Nibble(s[2 * i + 1]));
        return b;
    }

    private static int Nibble(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => throw new FormatException($"'{c}' is not a hex digit"),
    };

    public static uint RotateLeft(uint v, int n) => (v << n) | (v >> (32 - n));

    public static void WriteSingle(Span<byte> b, float f) => BinaryPrimitives.WriteInt32LittleEndian(b, BitConverter.ToInt32(BitConverter.GetBytes(f), 0));
    public static void WriteDouble(Span<byte> b, double d) => BinaryPrimitives.WriteInt64LittleEndian(b, BitConverter.DoubleToInt64Bits(d));
    public static float ReadSingle(ReadOnlySpan<byte> b) => BitConverter.ToSingle(BitConverter.GetBytes(BinaryPrimitives.ReadInt32LittleEndian(b)), 0);
    public static double ReadDouble(ReadOnlySpan<byte> b) => BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(b));

    public static byte[] RandomBytes(int n)
    {
        var b = new byte[n];
        using var rng = System.Security.Cryptography.RandomNumberGenerator.Create();
        rng.GetBytes(b);
        return b;
    }

    public static Guid GuidFromBigEndian(ReadOnlySpan<byte> b)
    {
        if (b.Length != 16) throw new ArgumentException("A UUID is 16 bytes");
        var mixed = b.ToArray(); // Guid's byte[] constructor reads the first three fields little-endian
        Array.Reverse(mixed, 0, 4);
        Array.Reverse(mixed, 4, 2);
        Array.Reverse(mixed, 6, 2);
        return new Guid(mixed);
    }

    public static BigInteger SignedBigEndian(ReadOnlySpan<byte> b)
    {
        var little = b.ToArray();
        Array.Reverse(little);
        return new BigInteger(little);
    }
#endif
}
