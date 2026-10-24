using Iceberg.Net.Avro;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Manifests;

/// <summary>
/// The one place manifests and manifest lists are read from and written to storage. Every caller in the library goes
/// through it, so a second <see cref="IManifestFormat"/> (Iceberg v4's Parquet manifests) slots in here without touching
/// them. Reading picks the format by the file's leading bytes: an upgraded v4 table keeps its v1–v3 Avro manifests.
/// Writing picks it by the table's format version.
/// </summary>
public static class ManifestStore
{
    /// <summary>The formats tried, in order, when reading.</summary>
    public static IReadOnlyList<IManifestFormat> Formats { get; } = [AvroManifestFormat.Instance];

    /// <summary>The format whose magic bytes <paramref name="bytes"/> start with.</summary>
    public static IManifestFormat ForReading(byte[] bytes, string location)
    {
        foreach (var f in Formats)
            if (f.Recognises(bytes)) return f;
        if (bytes.Length >= 4 && bytes[0] == (byte)'P' && bytes[1] == (byte)'A' && bytes[2] == (byte)'R' && bytes[3] == (byte)'1')
            throw new NotSupportedException($"{location} is a Parquet manifest (Iceberg v4); only Avro manifests (v1–v3) are supported");
        // Unrecognised: let the Avro decoder report the corruption with its own, precise error.
        return AvroManifestFormat.Instance;
    }

    public static IManifestFormat ForWriting(int formatVersion) =>
        Formats.FirstOrDefault(f => f.Writes(formatVersion)) ?? throw new NotSupportedException($"Writing manifests of format version {formatVersion} is not supported");

    /// <summary>The manifests a snapshot's manifest list names.</summary>
    public static async Task<IReadOnlyList<ManifestFile>> ReadListAsync(IFileIO io, string manifestList, CancellationToken ct = default)
    {
        var bytes = await io.ReadAllAsync(manifestList, ct).ConfigureAwait(false);
        return ForReading(bytes, manifestList).ReadManifestList(bytes).Manifests;
    }

    /// <summary>
    /// A manifest's entries, partition values typed by <paramref name="spec"/> over <paramref name="schema"/>. Pass
    /// <paramref name="manifest"/> (its manifest-list entry) so ADDED entries inherit snapshot IDs, sequence numbers and
    /// v3 first row IDs, as the spec requires.
    /// </summary>
    public static async Task<ManifestCodec.ManifestContents> ReadAsync(IFileIO io, string path, PartitionSpec spec, Schema schema, ManifestFile? manifest = null,
        CancellationToken ct = default)
    {
        var bytes = await io.ReadAllAsync(path, ct).ConfigureAwait(false);
        return ForReading(bytes, path).ReadManifest(bytes, spec, schema, manifest);
    }

    /// <summary>A manifest of <paramref name="metadata"/>, with its own partition spec and the current schema.</summary>
    public static Task<ManifestCodec.ManifestContents> ReadAsync(IFileIO io, TableMetadata metadata, ManifestFile manifest, CancellationToken ct = default) =>
        ReadAsync(io, manifest.ManifestPath, metadata.SpecById(manifest.PartitionSpecId), metadata.CurrentSchema, manifest, ct);

    /// <summary>Write a manifest (never overwriting); returns its length in bytes.</summary>
    public static async Task<long> WriteAsync(IFileIO io, string path, int formatVersion, Schema schema, PartitionSpec spec, ManifestContent content,
        IEnumerable<ManifestEntry> entries, CancellationToken ct = default)
    {
        var bytes = ForWriting(formatVersion).WriteManifest(formatVersion, schema, spec, content, entries);
        await io.WriteAsync(path, bytes, overwrite: false, ct).ConfigureAwait(false);
        return bytes.Length;
    }

    /// <summary>Write a manifest list (never overwriting).</summary>
    public static async Task WriteListAsync(IFileIO io, string path, int formatVersion, long snapshotId, long? parentSnapshotId, long sequenceNumber,
        IEnumerable<ManifestFile> manifests, long? firstRowId, CancellationToken ct = default)
    {
        var bytes = ForWriting(formatVersion).WriteManifestList(formatVersion, snapshotId, parentSnapshotId, sequenceNumber, manifests, firstRowId);
        await io.WriteAsync(path, bytes, overwrite: false, ct).ConfigureAwait(false);
    }
}
