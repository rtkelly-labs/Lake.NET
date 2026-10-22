using System.Buffers.Binary;
using System.Numerics;

namespace Iceberg.Net.Puffin;

/// <summary>
/// A set of row positions (non-negative longs) stored as Roaring bitmaps keyed by the high 32 bits, with
/// the Roaring64 "portable" serialization Iceberg's deletion vectors use: an 8-byte LE count of 32-bit
/// bitmaps, then for each a 4-byte LE key followed by a standard portable 32-bit Roaring bitmap.
/// Keys are limited to <see cref="int.MaxValue"/> for compatibility with Java's RoaringPositionBitmap.
/// </summary>
public sealed class RoaringPositionBitmap
{
    private readonly SortedDictionary<uint, Bitmap32> _bitmaps = [];

    public long Cardinality => _bitmaps.Values.Sum(b => (long)b.Cardinality);
    public bool IsEmpty => Cardinality == 0;

    public void Add(long position)
    {
        if (position < 0) throw new ArgumentOutOfRangeException(nameof(position), "Positions must be non-negative");
        var key = (uint)(position >> 32);
        if (key > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(position), "Position exceeds the Java-compatible range");
        if (!_bitmaps.TryGetValue(key, out var bm)) _bitmaps[key] = bm = new Bitmap32();
        bm.Add((uint)position);
    }

    public void AddRange(IEnumerable<long> positions)
    {
        foreach (var p in positions) Add(p);
    }

    public bool Contains(long position) =>
        position >= 0 && _bitmaps.TryGetValue((uint)(position >> 32), out var bm) && bm.Contains((uint)position);

    public void Or(RoaringPositionBitmap other)
    {
        foreach (var p in other.Positions()) Add(p);
    }

    public IEnumerable<long> Positions()
    {
        foreach (var (key, bm) in _bitmaps)
            foreach (var low in bm.Values())
                yield return ((long)key << 32) | low;
    }

    public byte[] Serialize()
    {
        using var ms = new MemoryStream();
        Span<byte> buf = stackalloc byte[8];
        var nonEmpty = _bitmaps.Where(kv => kv.Value.Cardinality > 0).ToList();
        BinaryPrimitives.WriteInt64LittleEndian(buf, nonEmpty.Count);
        ms.Write(buf);
        foreach (var (key, bm) in nonEmpty)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(buf, key);
            ms.Write(buf[..4]);
            bm.Serialize(ms);
        }
        return ms.ToArray();
    }

    public static RoaringPositionBitmap Deserialize(ReadOnlySpan<byte> data)
    {
        var result = new RoaringPositionBitmap();
        var count = BinaryPrimitives.ReadInt64LittleEndian(data);
        var pos = 8;
        long lastKey = -1;
        for (long i = 0; i < count; i++)
        {
            var key = BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]);
            pos += 4;
            if (key <= lastKey) throw new FormatException("Roaring64 keys must be strictly ascending");
            if (key > int.MaxValue) throw new FormatException($"Roaring64 key {key} exceeds the Java-compatible range");
            lastKey = key;
            var (bm, consumed) = Bitmap32.Deserialize(data[pos..]);
            pos += consumed;
            result._bitmaps[key] = bm;
        }
        return result;
    }

    /// <summary>
    /// A 32-bit Roaring bitmap: containers keyed by the high 16 bits, each an array (≤4096 values) or a
    /// 65536-bit bitmap. Serialized in the RoaringFormatSpec portable layout without run containers;
    /// run containers are accepted on read.
    /// </summary>
    internal sealed class Bitmap32
    {
        private const int ArrayMax = 4096;
        private const uint CookieNoRuns = 12346;
        private const ushort CookieRuns = 12347;

        private readonly SortedDictionary<ushort, Container> _containers = [];

        public int Cardinality => _containers.Values.Sum(c => c.Cardinality);

        public void Add(uint value)
        {
            var key = (ushort)(value >> 16);
            if (!_containers.TryGetValue(key, out var c)) _containers[key] = c = new Container();
            c.Add((ushort)value);
        }

        public bool Contains(uint value) => _containers.TryGetValue((ushort)(value >> 16), out var c) && c.Contains((ushort)value);

        public IEnumerable<uint> Values()
        {
            foreach (var (key, c) in _containers)
                foreach (var low in c.Values())
                    yield return ((uint)key << 16) | low;
        }

        public void Serialize(Stream s)
        {
            var containers = _containers.Where(kv => kv.Value.Cardinality > 0).ToList();
            Span<byte> b4 = stackalloc byte[4];
            Span<byte> b2 = stackalloc byte[2];
            BinaryPrimitives.WriteUInt32LittleEndian(b4, CookieNoRuns);
            s.Write(b4);
            BinaryPrimitives.WriteInt32LittleEndian(b4, containers.Count);
            s.Write(b4);
            foreach (var (key, c) in containers)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(b2, key);
                s.Write(b2);
                BinaryPrimitives.WriteUInt16LittleEndian(b2, (ushort)(c.Cardinality - 1));
                s.Write(b2);
            }
            // Offsets are relative to the start of this bitmap's serialization.
            var offset = 8 + 8 * containers.Count;
            foreach (var (_, c) in containers)
            {
                BinaryPrimitives.WriteInt32LittleEndian(b4, offset);
                s.Write(b4);
                offset += c.SerializedSize;
            }
            foreach (var (_, c) in containers) c.Serialize(s);
        }

        public static (Bitmap32 Bitmap, int Consumed) Deserialize(ReadOnlySpan<byte> data)
        {
            var cookie = BinaryPrimitives.ReadUInt32LittleEndian(data);
            int size, pos;
            byte[]? runFlags = null;
            bool hasOffsets;
            if ((cookie & 0xFFFF) == CookieRuns)
            {
                size = (int)(cookie >> 16) + 1;
                pos = 4;
                runFlags = data.Slice(pos, (size + 7) / 8).ToArray();
                pos += runFlags.Length;
                hasOffsets = size >= 4;
            }
            else if (cookie == CookieNoRuns)
            {
                size = BinaryPrimitives.ReadInt32LittleEndian(data[4..]);
                pos = 8;
                hasOffsets = true;
            }
            else throw new FormatException($"Unknown Roaring cookie {cookie}");

            var keys = new ushort[size];
            var cards = new int[size];
            for (var i = 0; i < size; i++)
            {
                keys[i] = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                cards[i] = BinaryPrimitives.ReadUInt16LittleEndian(data[(pos + 2)..]) + 1;
                pos += 4;
            }
            if (hasOffsets) pos += 4 * size;

            var bm = new Bitmap32();
            for (var i = 0; i < size; i++)
            {
                var c = new Container();
                if (runFlags is not null && (runFlags[i / 8] & (1 << (i % 8))) != 0)
                {
                    var runs = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                    pos += 2;
                    for (var r = 0; r < runs; r++)
                    {
                        var start = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                        var length = BinaryPrimitives.ReadUInt16LittleEndian(data[(pos + 2)..]);
                        pos += 4;
                        for (var v = 0; v <= length; v++) c.Add((ushort)(start + v));
                    }
                }
                else if (cards[i] > ArrayMax)
                {
                    c.SetBitmap(data.Slice(pos, 8192));
                    pos += 8192;
                }
                else
                {
                    for (var v = 0; v < cards[i]; v++) c.Add(BinaryPrimitives.ReadUInt16LittleEndian(data[(pos + 2 * v)..]));
                    pos += 2 * cards[i];
                }
                bm._containers[keys[i]] = c;
            }
            return (bm, pos);
        }

        private sealed class Container
        {
            private List<ushort>? _array = [];
            private ulong[]? _bits;
            private int _bitCount;

            public int Cardinality => _array?.Count ?? _bitCount;
            public int SerializedSize => _array is null ? 8192 : 2 * _array.Count;

            public void Add(ushort v)
            {
                if (_array is not null)
                {
                    var i = _array.BinarySearch(v);
                    if (i >= 0) return;
                    if (_array.Count < ArrayMax)
                    {
                        _array.Insert(~i, v);
                        return;
                    }
                    ToBitmap();
                }
                ref var word = ref _bits![v >> 6];
                var mask = 1UL << (v & 63);
                if ((word & mask) == 0)
                {
                    word |= mask;
                    _bitCount++;
                }
            }

            public bool Contains(ushort v) => _array is not null ? _array.BinarySearch(v) >= 0 : (_bits![v >> 6] & (1UL << (v & 63))) != 0;

            public IEnumerable<ushort> Values()
            {
                if (_array is not null)
                {
                    foreach (var v in _array) yield return v;
                    yield break;
                }
                for (var w = 0; w < 1024; w++)
                {
                    var word = _bits![w];
                    while (word != 0)
                    {
                        var bit = BitOperations.TrailingZeroCount(word);
                        yield return (ushort)((w << 6) | bit);
                        word &= word - 1;
                    }
                }
            }

            public void SetBitmap(ReadOnlySpan<byte> bytes)
            {
                _array = null;
                _bits = new ulong[1024];
                for (var w = 0; w < 1024; w++) _bits[w] = BinaryPrimitives.ReadUInt64LittleEndian(bytes[(w * 8)..]);
                _bitCount = _bits.Sum(BitOperations.PopCount);
            }

            private void ToBitmap()
            {
                _bits = new ulong[1024];
                foreach (var v in _array!) _bits[v >> 6] |= 1UL << (v & 63);
                _bitCount = _array.Count;
                _array = null;
            }

            public void Serialize(Stream s)
            {
                Span<byte> b = stackalloc byte[8];
                if (_array is not null)
                {
                    foreach (var v in _array)
                    {
                        BinaryPrimitives.WriteUInt16LittleEndian(b, v);
                        s.Write(b[..2]);
                    }
                    return;
                }
                foreach (var word in _bits!)
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(b, word);
                    s.Write(b);
                }
            }
        }
    }
}
