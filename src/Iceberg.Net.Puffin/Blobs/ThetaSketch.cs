using System.Buffers.Binary;
using System.Numerics;

namespace Iceberg.Net.Puffin;

/// <summary>
/// Apache DataSketches Theta sketch, serialized in the compact, ordered, serial-version-3 layout that
/// Iceberg's <c>apache-datasketches-theta-v1</c> blob carries (as written by datasketches-java/cpp).
/// Values are hashed with MurmurHash3_x64_128 (seed 9001) over Iceberg's single-value binary
/// serialization, exactly as Iceberg's Spark ThetaSketchAgg does, so sketches merge across engines.
/// The update path is a K-minimum-values sketch: retained counts can differ from Java's QuickSelect
/// sketch in estimation mode, but the serialized form and estimator are the standard ones.
/// </summary>
public sealed class ThetaSketch
{
    public const long DefaultSeed = 9001;
    public const int DefaultNominalEntries = 4096;
    private const long MaxTheta = long.MaxValue;
    private const byte SerialVersion = 3;
    private const byte CompactFamily = 3;
    private const byte FlagReadOnly = 1 << 1, FlagEmpty = 1 << 2, FlagCompact = 1 << 3, FlagOrdered = 1 << 4;

    private readonly int _k;
    private readonly long _seed;
    private readonly SortedSet<long> _hashes = [];
    private long _theta = MaxTheta;
    private bool _empty = true;

    public ThetaSketch(int nominalEntries = DefaultNominalEntries, long seed = DefaultSeed)
    {
        if (nominalEntries < 16 || !BitOperations.IsPow2(nominalEntries)) throw new ArgumentOutOfRangeException(nameof(nominalEntries), "Must be a power of two ≥ 16");
        _k = nominalEntries;
        _seed = seed;
    }

    public bool IsEmpty => _empty;
    public long ThetaLong => _theta;
    public int Retained => _hashes.Count;
    public double Estimate => _theta == MaxTheta ? _hashes.Count : _hashes.Count / ((double)_theta / MaxTheta);

    /// <summary>Update with raw bytes (Iceberg: the value's single-value serialization). Empty input is ignored, as in DataSketches.</summary>
    public void Update(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return;
        _empty = false;
        var hash = (long)(Murmur3x64.Hash128(bytes, (ulong)_seed).H1 >> 1);
        if (hash == 0 || hash >= _theta) return;
        if (!_hashes.Add(hash) || _hashes.Count <= _k) return;
        // Keep the k smallest; the (k+1)-th becomes theta (strictly-less-than retention).
        _theta = _hashes.Max;
        _hashes.Remove(_theta);
    }

    public void Merge(ThetaSketch other)
    {
        if (other._seed != _seed) throw new ArgumentException("Cannot merge sketches with different seeds");
        if (other._empty) return;
        _empty = false;
        var theta = Math.Min(_theta, other._theta);
        foreach (var h in other._hashes) if (h < theta) _hashes.Add(h);
        _hashes.RemoveWhere(h => h >= theta);
        _theta = theta;
        while (_hashes.Count > _k)
        {
            _theta = _hashes.Max;
            _hashes.Remove(_theta);
        }
    }

    /// <summary>DataSketches seed hash: low 16 bits of MurmurHash3_x64_128(seed as 8 LE bytes, seed 0).</summary>
    public static ushort SeedHash(long seed)
    {
        Span<byte> b = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(b, seed);
        return (ushort)(Murmur3x64.Hash128(b, 0).H1 & 0xFFFF);
    }

    public byte[] Serialize()
    {
        var estimation = _theta < MaxTheta;
        var preLongs = _empty ? 1 : estimation ? 3 : _hashes.Count == 1 ? 1 : 2;
        var result = new byte[preLongs * 8 + (_empty ? 0 : _hashes.Count * 8)];
        result[0] = (byte)preLongs;
        result[1] = SerialVersion;
        result[2] = CompactFamily;
        result[5] = (byte)(FlagReadOnly | FlagCompact | FlagOrdered | (_empty ? FlagEmpty : 0));
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(6), SeedHash(_seed));
        if (preLongs >= 2) BinaryPrimitives.WriteInt32LittleEndian(result.AsSpan(8), _hashes.Count);
        if (preLongs == 3) BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(16), _theta);
        var at = preLongs * 8;
        foreach (var h in _hashes)
        {
            BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(at), h);
            at += 8;
        }
        return result;
    }

    public static ThetaSketch Deserialize(ReadOnlySpan<byte> data, long seed = DefaultSeed) =>
        Corruption.Guard("Theta sketch", data, d => DeserializeUnguarded(d, seed));

    private static ThetaSketch DeserializeUnguarded(ReadOnlySpan<byte> data, long seed)
    {
        if (data.Length < 8) throw new FormatException("Theta sketch is too short");
        var preLongs = data[0] & 0x3F;
        if (data[1] != SerialVersion) throw new NotSupportedException($"Theta serial version {data[1]} (only 3 is supported)");
        if (data[2] != CompactFamily) throw new NotSupportedException($"Theta family {data[2]} (only compact is supported)");
        var flags = data[5];
        if (BinaryPrimitives.ReadUInt16LittleEndian(data[6..]) != SeedHash(seed)) throw new FormatException("Theta sketch seed hash mismatch");
        var sketch = new ThetaSketch(DefaultNominalEntries, seed) { _empty = (flags & FlagEmpty) != 0 };
        if (sketch._empty) return sketch;
        int count;
        if (preLongs == 1) count = data.Length >= 16 ? 1 : 0;
        else count = BinaryPrimitives.ReadInt32LittleEndian(data[8..]);
        if (preLongs is < 1 or > 3 || count < 0 || preLongs * 8L + count * 8L > data.Length)
            throw new FormatException($"Theta sketch with {preLongs} preamble longs and {count} entries does not fit in {data.Length} bytes");
        if (preLongs == 3) sketch._theta = BinaryPrimitives.ReadInt64LittleEndian(data[16..]);
        for (var i = 0; i < count; i++) sketch._hashes.Add(BinaryPrimitives.ReadInt64LittleEndian(data[(preLongs * 8 + i * 8)..]));
        return sketch;
    }
}

/// <summary>MurmurHash3 x64 128-bit (Austin Appleby), as used by DataSketches.</summary>
public static class Murmur3x64
{
    private const ulong C1 = 0x87c37b91114253d5UL;
    private const ulong C2 = 0x4cf5ad432745937fUL;

    public static (ulong H1, ulong H2) Hash128(ReadOnlySpan<byte> data, ulong seed)
    {
        ulong h1 = seed, h2 = seed;
        var nblocks = data.Length / 16;
        for (var i = 0; i < nblocks; i++)
        {
            var k1 = BinaryPrimitives.ReadUInt64LittleEndian(data[(i * 16)..]);
            var k2 = BinaryPrimitives.ReadUInt64LittleEndian(data[(i * 16 + 8)..]);
            k1 *= C1; k1 = BitOperations.RotateLeft(k1, 31); k1 *= C2; h1 ^= k1;
            h1 = BitOperations.RotateLeft(h1, 27); h1 += h2; h1 = h1 * 5 + 0x52dce729;
            k2 *= C2; k2 = BitOperations.RotateLeft(k2, 33); k2 *= C1; h2 ^= k2;
            h2 = BitOperations.RotateLeft(h2, 31); h2 += h1; h2 = h2 * 5 + 0x38495ab5;
        }

        var tail = data[(nblocks * 16)..];
        ulong t1 = 0, t2 = 0;
        for (var i = tail.Length - 1; i >= 8; i--) t2 = (t2 << 8) | tail[i];
        for (var i = Math.Min(tail.Length, 8) - 1; i >= 0; i--) t1 = (t1 << 8) | tail[i];
        if (tail.Length > 8)
        {
            t2 *= C2; t2 = BitOperations.RotateLeft(t2, 33); t2 *= C1; h2 ^= t2;
        }
        if (tail.Length > 0)
        {
            t1 *= C1; t1 = BitOperations.RotateLeft(t1, 31); t1 *= C2; h1 ^= t1;
        }

        h1 ^= (ulong)data.Length; h2 ^= (ulong)data.Length;
        h1 += h2; h2 += h1;
        h1 = Fmix(h1); h2 = Fmix(h2);
        h1 += h2; h2 += h1;
        return (h1, h2);
    }

    private static ulong Fmix(ulong k)
    {
        k ^= k >> 33; k *= 0xff51afd7ed558ccdUL;
        k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53UL;
        k ^= k >> 33;
        return k;
    }
}
