using Iceberg.Net.IO;

namespace Lake.Delta;

/// <summary>
/// Locations in a table. <c>add.path</c> and <c>remove.path</c> are RFC 2396 URIs: relative ones resolve against the
/// table root and are percent-decoded once (a partition directory <c>letter=%2F</c> is written <c>letter=%252F</c>);
/// absolute ones (with a scheme) are used as they are, decoded for <c>file:</c>.
/// </summary>
internal static class DeltaPaths
{
    public static string NormalizeRoot(string location) => location.Length > 1 ? location.TrimEnd('/') : location;

    public static string Join(string root, string relative) => root + "/" + relative.TrimStart('/');

    public static string LogDirectory(string root) => Join(root, "_delta_log");

    public static string Absolute(string root, string uriPath)
    {
        if (StorageScheme.Of(uriPath) is null) return Join(root, Uri.UnescapeDataString(uriPath));
        // file:/x (Hadoop) and file:///x: LocalFileIO decodes both; other schemes keep their URI form.
        return uriPath;
    }

    /// <summary>The key a logical file is reconciled under: its decoded path plus its DV's unique ID.</summary>
    public static (string Path, string? DvId) FileKey(string path, DeletionVectorDescriptor? dv) => (Uri.UnescapeDataString(path), dv?.UniqueId);
}
