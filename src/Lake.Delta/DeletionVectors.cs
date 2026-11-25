using System.Buffers.Binary;
using Iceberg.Net.IO;
using Iceberg.Net.Puffin;

namespace Lake.Delta;

/// <summary>
/// Loads a deletion vector (PROTOCOL.md "Deletion Vectors") into Iceberg.Net.Puffin's <see cref="RoaringPositionBitmap"/>.
/// <list type="bullet">
/// <item>On disk (<c>u</c>, <c>p</c>): a file starting with version byte 1; at <c>offset</c> a record
/// <c>dataSize (4, BE) | bitmapData | CRC-32 (4, BE)</c>. With the portable magic, that record is byte for byte an Iceberg
/// <c>deletion-vector-v1</c> blob (DeltaDeletionVectorBytesTests), so <see cref="DeletionVector.Decode(ReadOnlySpan{byte}, long?)"/>
/// decodes it, CRC included.</item>
/// <item>Inline (<c>i</c>): the Z85 of <c>bitmapData</c> alone.</item>
/// </list>
/// <c>bitmapData</c> is a "RoaringBitmapArray" in one of delta-spark's two layouts: portable (magic 1681511377
/// little-endian, then the 64-bit Roaring portable format; the one PROTOCOL.md specifies) or native (magic 1681511376
/// big-endian, a big-endian bitmap count, then per bitmap a big-endian length and a 32-bit Roaring bitmap whose high 32
/// bits are its index; the layout of PROTOCOL.md's own inline example). Both are accepted.
/// </summary>
public static class DeletionVectors
{
    private const uint PortableMagic = 1681511377; // little-endian on disk: D1 D3 39 64
    private const uint NativeMagic = 1681511376;   // big-endian on disk:    64 39 D3 D0

    /// <summary>Resolve where a <c>u</c> or <c>p</c> deletion vector lives.</summary>
    public static string AbsolutePath(string tableRoot, DeletionVectorDescriptor dv) => dv.StorageType switch
    {
        "u" => UuidPath(tableRoot, dv.PathOrInlineDv),
        "p" => DeltaPaths.Absolute(tableRoot, dv.PathOrInlineDv),
        _ => throw new InvalidOperationException("An inline deletion vector has no path"),
    };

    private static string UuidPath(string tableRoot, string pathOrInlineDv)
    {
        if (pathOrInlineDv.Length < 20) throw new InvalidDataException($"Deletion vector path '{pathOrInlineDv}' is shorter than a 20-character Z85 UUID");
        var prefix = pathOrInlineDv[..^20];
        var uuid = Z85.DecodeUuid(pathOrInlineDv[^20..]);
        var name = $"deletion_vector_{uuid}.bin";
        return DeltaPaths.Join(tableRoot, prefix.Length == 0 ? name : prefix + "/" + name);
    }

    /// <summary>The rows <paramref name="dv"/> deletes, checked against its declared cardinality.</summary>
    public static Task<RoaringPositionBitmap> LoadAsync(IFileIO io, string tableRoot, DeletionVectorDescriptor dv, CancellationToken ct = default) =>
        Corruption.GuardAsync($"deletion vector {dv.UniqueId}", async () =>
        {
            RoaringPositionBitmap bitmap;
            if (dv.StorageType == "i")
            {
                bitmap = DecodeBitmapData(Z85.Decode(dv.PathOrInlineDv, dv.SizeInBytes), dv.Cardinality);
            }
            else
            {
                var path = AbsolutePath(tableRoot, dv);
                var offset = (long)(dv.Offset ?? 0);
                var recordLength = 4L + dv.SizeInBytes + 4;
                var fileLength = await io.LengthAsync(path, ct).ConfigureAwait(false);
                if (dv.SizeInBytes < 4 || offset + recordLength > fileLength)
                    throw new InvalidDataException($"Deletion vector record ({recordLength} bytes at {offset}) does not fit in {path} ({fileLength} bytes)");
                var record = await io.ReadRangeAsync(path, offset, (int)recordLength, ct).ConfigureAwait(false);
                bitmap = DecodeRecord(record, dv.SizeInBytes, dv.Cardinality);
            }
            if (bitmap.Cardinality != dv.Cardinality)
                throw new InvalidDataException($"Deletion vector holds {bitmap.Cardinality} rows; its descriptor says {dv.Cardinality}");
            return bitmap;
        });

    /// <summary>One on-disk record: <c>dataSize | bitmapData | CRC-32</c>.</summary>
    internal static RoaringPositionBitmap DecodeRecord(ReadOnlySpan<byte> record, int sizeInBytes, long maxCardinality)
    {
        if (record.Length < 12) throw new InvalidDataException("Deletion vector record is too short");
        var dataSize = BinaryPrimitives.ReadInt32BigEndian(record);
        if (dataSize != sizeInBytes) throw new InvalidDataException($"Deletion vector record says {dataSize} bytes; its descriptor says {sizeInBytes}");
        if (BinaryPrimitives.ReadUInt32LittleEndian(record[4..]) == PortableMagic)
            return DeletionVector.Decode(record, maxCardinality); // the Iceberg blob layout, CRC checked there
        var data = record.Slice(4, dataSize);
        var expected = BinaryPrimitives.ReadUInt32BigEndian(record[(4 + dataSize)..]);
        var actual = Crc32.Compute(data);
        if (expected != actual) throw new InvalidDataException($"Deletion vector CRC mismatch (expected {expected:x8}, computed {actual:x8})");
        return DecodeBitmapData(data, maxCardinality);
    }

    /// <summary><c>bitmapData</c>: a magic number, then the Roaring bitmaps in the layout it names.</summary>
    internal static RoaringPositionBitmap DecodeBitmapData(ReadOnlySpan<byte> data, long maxCardinality)
    {
        if (data.Length < 4) throw new InvalidDataException("Deletion vector bitmap is shorter than its magic number");
        if (BinaryPrimitives.ReadUInt32LittleEndian(data) == PortableMagic)
            return RoaringPositionBitmap.Deserialize(data[4..], maxCardinality);
        if (BinaryPrimitives.ReadUInt32BigEndian(data) == NativeMagic)
            return RoaringPositionBitmap.Deserialize(NativeToPortable(data[4..]), maxCardinality);
        throw new InvalidDataException($"Deletion vector magic {Convert.ToHexString(data[..4])} is neither the portable nor the native RoaringBitmapArray magic");
    }

    /// <summary>Re-frame the native layout as the 64-bit portable one: the bitmaps are the same bytes, keyed by index.</summary>
    private static byte[] NativeToPortable(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4) throw new InvalidDataException("Native deletion vector has no bitmap count");
        var count = BinaryPrimitives.ReadInt32BigEndian(data);
        if (count < 0 || count > (data.Length - 4) / 4) throw new InvalidDataException($"Native deletion vector bitmap count {count} is out of range");
        using var portable = new MemoryStream(data.Length + 8 + 4 * count);
        Span<byte> header = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(header, (ulong)count);
        portable.Write(header);
        var position = 4;
        for (var key = 0; key < count; key++)
        {
            if (position + 4 > data.Length) throw new InvalidDataException("Native deletion vector is truncated");
            var length = BinaryPrimitives.ReadInt32BigEndian(data[position..]);
            position += 4;
            if (length < 0 || position + length > data.Length) throw new InvalidDataException($"Native deletion vector bitmap {key} length {length} is out of range");
            BinaryPrimitives.WriteUInt32LittleEndian(header, (uint)key);
            portable.Write(header[..4]);
            portable.Write(data.Slice(position, length));
            position += length;
        }
        if (position != data.Length) throw new InvalidDataException("Native deletion vector has trailing bytes");
        return portable.ToArray();
    }
}
