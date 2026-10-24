using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Avro;

/// <summary>
/// One encoding of manifests and manifest lists. v1–v3 have one, Avro (<see cref="AvroManifestFormat"/>); Iceberg v4's
/// Parquet root and leaf manifests would be a second. Callers go through <c>ManifestStore</c> (Iceberg.Net.Catalog),
/// which picks the format: by a file's leading bytes when reading, by the table's format version when writing.
/// </summary>
public interface IManifestFormat
{
    /// <summary>A short name for messages, e.g. <c>avro</c>.</summary>
    string Name { get; }

    /// <summary>Whether <paramref name="bytes"/> (a whole file) is in this format, from its magic bytes.</summary>
    bool Recognises(byte[] bytes);

    /// <summary>Whether this format writes manifests of tables at <paramref name="formatVersion"/>.</summary>
    bool Writes(int formatVersion);

    ManifestCodec.ManifestListContents ReadManifestList(byte[] bytes);

    /// <summary>A manifest's entries, decoded by field ID; with <paramref name="manifest"/>, inherited values are filled in.</summary>
    ManifestCodec.ManifestContents ReadManifest(byte[] bytes, PartitionSpec spec, Schema schema, ManifestFile? manifest);

    byte[] WriteManifest(int formatVersion, Schema schema, PartitionSpec spec, ManifestContent content, IEnumerable<ManifestEntry> entries);

    byte[] WriteManifestList(int formatVersion, long snapshotId, long? parentSnapshotId, long sequenceNumber, IEnumerable<ManifestFile> manifests, long? firstRowId);
}

/// <summary>v1–v3 manifests and manifest lists: Avro object containers, through <see cref="ManifestCodec"/>.</summary>
public sealed class AvroManifestFormat : IManifestFormat
{
    public static AvroManifestFormat Instance { get; } = new();

    private AvroManifestFormat() { }

    public string Name => "avro";

    public bool Recognises(byte[] bytes) => bytes.Length >= 4 && bytes[0] == (byte)'O' && bytes[1] == (byte)'b' && bytes[2] == (byte)'j' && bytes[3] == 1;

    public bool Writes(int formatVersion) => formatVersion is >= 1 and <= 3;

    public ManifestCodec.ManifestListContents ReadManifestList(byte[] bytes) => ManifestCodec.ReadManifestList(bytes);

    public ManifestCodec.ManifestContents ReadManifest(byte[] bytes, PartitionSpec spec, Schema schema, ManifestFile? manifest) =>
        ManifestCodec.ReadManifest(bytes, spec, schema, manifest);

    public byte[] WriteManifest(int formatVersion, Schema schema, PartitionSpec spec, ManifestContent content, IEnumerable<ManifestEntry> entries) =>
        ManifestCodec.WriteManifest(formatVersion, schema, spec, content, entries);

    public byte[] WriteManifestList(int formatVersion, long snapshotId, long? parentSnapshotId, long sequenceNumber, IEnumerable<ManifestFile> manifests, long? firstRowId) =>
        ManifestCodec.WriteManifestList(formatVersion, snapshotId, parentSnapshotId, sequenceNumber, manifests, firstRowId);
}
