using System.Buffers.Binary;
using System.Globalization;

namespace Iceberg.Net.Puffin;

/// <summary>
/// The v3 <c>deletion-vector-v1</c> blob: <c>length(4 BE) magic(D1 D3 39 64) vector CRC-32(4 BE)</c>, where
/// <c>length</c> covers magic + vector and the CRC is computed over the same bytes (java.util.zip.CRC32).
/// The blob must be stored uncompressed with <c>referenced-data-file</c> and <c>cardinality</c> properties
/// and snapshot ID / sequence number of -1 (both are inherited from the manifest entry).
/// </summary>
public static class DeletionVector
{
    public const string ReferencedDataFileProperty = "referenced-data-file";
    public const string CardinalityProperty = "cardinality";
    /// <summary>The reserved <c>_pos</c> metadata column; Java records it as the DV blob's field.</summary>
    public const int RowPositionFieldId = 2147483545;

    private static ReadOnlySpan<byte> Magic => [0xD1, 0xD3, 0x39, 0x64];

    public static byte[] Encode(RoaringPositionBitmap positions)
    {
        var vector = positions.Serialize();
        var result = new byte[4 + Magic.Length + vector.Length + 4];
        BinaryPrimitives.WriteInt32BigEndian(result, Magic.Length + vector.Length);
        Magic.CopyTo(result.AsSpan(4));
        vector.CopyTo(result.AsSpan(8));
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(8 + vector.Length), Crc32.Compute(result.AsSpan(4, Magic.Length + vector.Length)));
        return result;
    }

    public static RoaringPositionBitmap Decode(ReadOnlySpan<byte> blob) => Decode(blob, maxCardinality: null);

    /// <summary>
    /// Decode a deletion vector, refusing one that holds more than <paramref name="maxCardinality"/> positions (the
    /// manifest's <c>record_count</c> for it): the declared sizes are checked before any container is built.
    /// </summary>
    public static RoaringPositionBitmap Decode(ReadOnlySpan<byte> blob, long? maxCardinality) =>
        Corruption.Guard("deletion vector", blob, b => DecodeUnguarded(b, maxCardinality));

    private static RoaringPositionBitmap DecodeUnguarded(ReadOnlySpan<byte> blob, long? maxCardinality)
    {
        if (blob.Length < 12) throw new FormatException("Deletion vector is too short");
        var length = BinaryPrimitives.ReadInt32BigEndian(blob);
        if (length < Magic.Length || 4L + length + 4 > blob.Length) throw new FormatException("Deletion vector length is out of range");
        if (!blob.Slice(4, 4).SequenceEqual(Magic)) throw new FormatException("Deletion vector magic mismatch");
        var expected = BinaryPrimitives.ReadUInt32BigEndian(blob[(4 + length)..]);
        var actual = Crc32.Compute(blob.Slice(4, length));
        if (expected != actual) throw new FormatException($"Deletion vector CRC mismatch (expected {expected:x8}, computed {actual:x8})");
        return RoaringPositionBitmap.Deserialize(blob.Slice(8, length - Magic.Length), maxCardinality);
    }

    public static PuffinBlob ToBlob(string referencedDataFile, RoaringPositionBitmap positions) => new(
        PuffinBlobTypes.DeletionVectorV1,
        [RowPositionFieldId],
        Encode(positions),
        Properties: new Dictionary<string, string>
        {
            [ReferencedDataFileProperty] = referencedDataFile,
            [CardinalityProperty] = positions.Cardinality.ToString(CultureInfo.InvariantCulture),
        });
}

/// <summary>CRC-32 (IEEE 802.3, reflected 0xEDB88320) — identical to java.util.zip.CRC32 and zlib.crc32.</summary>
public static class Crc32
{
    private static readonly uint[] Table = BuildTable();

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var c = i;
            for (var k = 0; k < 8; k++) c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data) crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }
}
