
namespace Iceberg.Net.Puffin;

/// <summary>
/// Writes Puffin containers with Iceberg's blob-type rules applied (for example, deletion vectors must be stored
/// uncompressed). Reading is the generic <see cref="PuffinFile.Read"/>: a reader must accept blobs of any type.
/// </summary>
public static class IcebergPuffinFile
{
    public static PuffinFile.Written Write(
        IEnumerable<PuffinBlob> blobs,
        IReadOnlyDictionary<string, string>? properties = null) =>
        PuffinFile.Write(blobs, properties, PuffinBlobTypes.Rules);
}
