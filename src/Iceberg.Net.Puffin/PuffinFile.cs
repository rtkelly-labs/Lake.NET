using System.Buffers.Binary;
using System.Text;
using System.Text.Json.Nodes;

namespace Iceberg.Net.Puffin;

/// <summary>Blob metadata as recorded in the Puffin footer (puffin-spec.md, "BlobMetadata").</summary>
public sealed record PuffinBlobMetadata(
    string Type,
    IReadOnlyList<int> Fields,
    long SnapshotId,
    long SequenceNumber,
    long Offset,
    long Length,
    string? CompressionCodec = null,
    IReadOnlyDictionary<string, string>? Properties = null);

/// <summary>A blob to write. Snapshot ID and sequence number are -1 for deletion vectors (assigned by inheritance).</summary>
public sealed record PuffinBlob(
    string Type,
    IReadOnlyList<int> Fields,
    ReadOnlyMemory<byte> Payload,
    long SnapshotId = -1,
    long SequenceNumber = -1,
    string? CompressionCodec = null,
    IReadOnlyDictionary<string, string>? Properties = null);

public static class PuffinBlobTypes
{
    public const string DeletionVectorV1 = "deletion-vector-v1";
    public const string ThetaSketchV1 = "apache-datasketches-theta-v1";
}

/// <summary>
/// Puffin container: <c>Magic Blob₁…Blobₙ Footer</c> where
/// <c>Footer = Magic FooterPayload FooterPayloadSize(int32 LE) Flags(4 bytes) Magic</c>.
/// Footer payload compression (flag bit 0, LZ4) is rejected on read and never written.
/// </summary>
public static class PuffinFile
{
    public static ReadOnlySpan<byte> Magic => "PFA1"u8;
    private const int FooterTrailerLength = 12; // size + flags + magic

    public sealed record Written(byte[] Bytes, IReadOnlyList<PuffinBlobMetadata> Blobs, long FooterSize);

    public static Written Write(IEnumerable<PuffinBlob> blobs, IReadOnlyDictionary<string, string>? properties = null)
    {
        using var ms = new MemoryStream();
        ms.Write(Magic);
        var metadata = new List<PuffinBlobMetadata>();
        foreach (var blob in blobs)
        {
            if (blob.Type == PuffinBlobTypes.DeletionVectorV1 && blob.CompressionCodec is not null)
                throw new ArgumentException("deletion-vector-v1 blobs must not be compressed");
            var stored = Compress(blob.CompressionCodec, blob.Payload.Span);
            var offset = ms.Position;
            ms.Write(stored);
            metadata.Add(new PuffinBlobMetadata(blob.Type, blob.Fields, blob.SnapshotId, blob.SequenceNumber, offset, stored.Length, blob.CompressionCodec, blob.Properties));
        }

        var footerStart = ms.Position;
        var payload = Encoding.UTF8.GetBytes(FooterJson(metadata, properties ?? new Dictionary<string, string> { ["created-by"] = "iceberg-net" }).ToJsonString());
        ms.Write(Magic);
        ms.Write(payload);
        Span<byte> trailer = stackalloc byte[FooterTrailerLength];
        BinaryPrimitives.WriteInt32LittleEndian(trailer, payload.Length);
        // trailer[4..8] = flags, all zero: footer payload uncompressed.
        Magic.CopyTo(trailer[8..]);
        ms.Write(trailer);
        return new Written(ms.ToArray(), metadata, ms.Length - footerStart);
    }

    public sealed record Contents(IReadOnlyList<PuffinBlobMetadata> Blobs, IReadOnlyDictionary<string, string> Properties, byte[] Bytes)
    {
        public byte[] Payload(PuffinBlobMetadata blob) => Corruption.Guard($"Puffin blob {blob.Type}", () =>
        {
            if (blob.Offset < 0 || blob.Length < 0 || blob.Offset + blob.Length > Bytes.Length)
                throw new InvalidDataException($"Puffin blob [{blob.Offset}, +{blob.Length}) lies outside the {Bytes.Length}-byte file");
            return Decompress(blob.CompressionCodec, Bytes.AsSpan((int)blob.Offset, (int)blob.Length));
        });
    }

    public static Contents Read(byte[] bytes) => Corruption.Guard("Puffin file", () => ReadUnguarded(bytes));

    private static Contents ReadUnguarded(byte[] bytes)
    {
        if (bytes.Length < Magic.Length * 2 + FooterTrailerLength || !bytes.AsSpan(0, 4).SequenceEqual(Magic) || !bytes.AsSpan(bytes.Length - 4).SequenceEqual(Magic))
            throw new FormatException("Not a Puffin file (bad magic)");
        var flags = bytes.AsSpan(bytes.Length - 8, 4);
        if ((flags[0] & 1) != 0) throw new NotSupportedException("Puffin footer payload is LZ4-compressed; not supported");
        var payloadSize = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - FooterTrailerLength, 4));
        var footerMagicAt = bytes.Length - FooterTrailerLength - payloadSize - Magic.Length;
        if (payloadSize < 0 || footerMagicAt < Magic.Length || !bytes.AsSpan(footerMagicAt, 4).SequenceEqual(Magic))
            throw new FormatException("Puffin footer is corrupt");
        var footer = JsonNode.Parse(bytes.AsSpan(footerMagicAt + 4, payloadSize))!;
        return new Contents(
            footer["blobs"]!.AsArray().Select(b => new PuffinBlobMetadata(
                b!["type"]!.GetValue<string>(),
                b["fields"]!.AsArray().Select(f => f!.GetValue<int>()).ToList(),
                b["snapshot-id"]!.GetValue<long>(),
                b["sequence-number"]!.GetValue<long>(),
                b["offset"]!.GetValue<long>(),
                b["length"]!.GetValue<long>(),
                b["compression-codec"]?.GetValue<string>(),
                b["properties"]?.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value!.GetValue<string>()))).ToList(),
            footer["properties"]?.AsObject().ToDictionary(kv => kv.Key, kv => kv.Value!.GetValue<string>()) ?? [],
            bytes);
    }

    private static JsonObject FooterJson(IEnumerable<PuffinBlobMetadata> blobs, IReadOnlyDictionary<string, string> properties)
    {
        static JsonObject Map(IReadOnlyDictionary<string, string> m) => new(m.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value)));
        return new JsonObject
        {
            ["blobs"] = new JsonArray(blobs.Select(b =>
            {
                var o = new JsonObject
                {
                    ["type"] = b.Type,
                    ["fields"] = new JsonArray(b.Fields.Select(f => (JsonNode)f).ToArray()),
                    ["snapshot-id"] = b.SnapshotId,
                    ["sequence-number"] = b.SequenceNumber,
                    ["offset"] = b.Offset,
                    ["length"] = b.Length,
                };
                if (b.CompressionCodec is not null) o["compression-codec"] = b.CompressionCodec;
                if (b.Properties is { Count: > 0 }) o["properties"] = Map(b.Properties);
                return (JsonNode)o;
            }).ToArray()),
            ["properties"] = Map(properties),
        };
    }

    private static byte[] Compress(string? codec, ReadOnlySpan<byte> data) => codec switch
    {
        null => data.ToArray(),
        "zstd" => new ZstdSharp.Compressor().Wrap(data).ToArray(),
        _ => throw new NotSupportedException($"Puffin codec '{codec}' (supported: none, zstd)"),
    };

    private static byte[] Decompress(string? codec, ReadOnlySpan<byte> data) => codec switch
    {
        null => data.ToArray(),
        "zstd" => new ZstdSharp.Decompressor().Unwrap(data).ToArray(),
        _ => throw new NotSupportedException($"Puffin codec '{codec}' (supported: none, zstd)"),
    };
}
