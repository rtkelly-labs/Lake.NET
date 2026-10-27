
namespace Iceberg.Net.Puffin;

/// <summary>
/// The blob types Iceberg defines for Puffin files, with the container rules each one carries. The generic
/// <see cref="PuffinFile"/> does not know these names; <see cref="IcebergPuffinFile.Write"/> applies <see cref="Rules"/>.
/// </summary>
public static class PuffinBlobTypes
{
    public const string DeletionVectorV1 = "deletion-vector-v1";
    public const string ThetaSketchV1 = "apache-datasketches-theta-v1";

    /// <summary>Container rules for Iceberg's blob types.</summary>
    public static IReadOnlyList<PuffinBlobType> Rules { get; } =
    [
        new(DeletionVectorV1, Compressible: false),
        new(ThetaSketchV1),
    ];
}
