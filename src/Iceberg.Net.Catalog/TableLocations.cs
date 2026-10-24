using Iceberg.Net.Metadata;

namespace Iceberg.Net.Catalog;

/// <summary>
/// Where a table's files may live, for the operations that delete them. A table owns its <c>location</c> and the
/// directories its <c>write.data.path</c>, <c>write.metadata.path</c> and <c>write.object-storage.path</c> properties
/// name. Metadata read from storage or a catalog can reference any path at all, so expiry, purge and orphan cleanup
/// only ever delete under these prefixes, and skip (and report) anything else.
/// </summary>
public static class TableLocations
{
    /// <summary>Table property: <c>false</c> means the table does not own its data files (Java's <c>gc.enabled</c>).</summary>
    public const string GcEnabled = "gc.enabled";

    /// <summary>Table properties naming directories the table writes files to, besides its location.</summary>
    public static readonly IReadOnlyList<string> PathProperties = ["write.data.path", "write.metadata.path", "write.object-storage.path"];

    /// <summary>
    /// Whether the table owns the files it references. Tables created by Spark's <c>snapshot</c> or <c>migrate</c>, or
    /// that adopted files in place, set <c>gc.enabled=false</c>: their data files belong to another table, so deleting
    /// them would corrupt it.
    /// </summary>
    public static bool IsGcEnabled(TableMetadata metadata) =>
        !string.Equals(metadata.Property(GcEnabled, "true").Trim(), "false", StringComparison.OrdinalIgnoreCase);

    /// <summary>The normalised directory prefixes (each ending in <c>/</c>) the table may delete under.</summary>
    public static IReadOnlyList<string> Prefixes(TableMetadata metadata) =>
        new[] { metadata.Location }.Concat(PathProperties.Select(p => metadata.Properties.GetValueOrDefault(p)))
            .OfType<string>().Where(l => l.Length > 0 && !HasDotSegment(Normalise(l)))
            .Select(l => Normalise(l).TrimEnd('/') + "/").Distinct(StringComparer.Ordinal).ToList();

    /// <summary>Whether <paramref name="path"/> is strictly below one of <paramref name="prefixes"/>, without dot segments.</summary>
    public static bool IsUnder(string path, IReadOnlyList<string> prefixes)
    {
        var p = Normalise(path);
        return !HasDotSegment(p) && prefixes.Any(prefix => p.Length > prefix.Length && p.StartsWith(prefix, StringComparison.Ordinal));
    }

    /// <summary>Split <paramref name="files"/> into those under the table's prefixes and those outside them.</summary>
    public static (List<string> Inside, List<string> Outside) Partition(TableMetadata metadata, IEnumerable<string> files)
    {
        var prefixes = Prefixes(metadata);
        var inside = new List<string>();
        var outside = new List<string>();
        foreach (var f in files) (IsUnder(f, prefixes) ? inside : outside).Add(f);
        return (inside, outside);
    }

    /// <summary>
    /// Refuse a location that is not safe to list and delete under: one with <c>.</c> or <c>..</c> segments, or one with
    /// no directory below a bucket, container or file-system root (<c>s3://bucket</c>, <c>/</c>, <c>file:///</c>).
    /// </summary>
    public static void RequireSafeRoot(string location)
    {
        var p = Normalise(location);
        if (HasDotSegment(p)) throw new ArgumentException($"Table location '{location}' has '.' or '..' segments", nameof(location));
        if (!PathPart(p).Split('/', '\\').Any(s => s.Length > 0))
            throw new ArgumentException($"Table location '{location}' is a bucket, container or file-system root", nameof(location));
    }

    /// <summary>
    /// Refuse a namespace level or table name that would change the shape of a path built from it: empty, <c>.</c>,
    /// <c>..</c>, or containing <c>/</c>, <c>\</c> or a control character.
    /// </summary>
    public static void RequireSafeName(string name, string what)
    {
        if (name.Length == 0 || name is "." or ".." || name.Any(c => c is '/' or '\\' || char.IsControl(c)))
            throw new ArgumentException($"{what} '{name}' cannot be used in a storage path", nameof(name));
    }

    /// <summary>One spelling per location: <c>s3a://</c>/<c>s3n://</c> as <c>s3://</c>, <c>file:</c> URIs as local paths.</summary>
    public static string Normalise(string location)
    {
        if (location.StartsWith("s3a://", StringComparison.Ordinal) || location.StartsWith("s3n://", StringComparison.Ordinal)) return "s3://" + location[6..];
        if (location.StartsWith("file://", StringComparison.Ordinal)) return new Uri(location).LocalPath;
        if (location.StartsWith("file:", StringComparison.Ordinal)) return Uri.UnescapeDataString(location["file:".Length..]);
        return location;
    }

    private static bool HasDotSegment(string normalised) => PathPart(normalised).Split('/', '\\').Any(s => s is "." or "..");

    /// <summary>The part after <c>scheme://authority</c>, or the whole of a local path.</summary>
    private static string PathPart(string normalised)
    {
        var scheme = normalised.IndexOf("://", StringComparison.Ordinal);
        if (scheme < 0) return normalised;
        var slash = normalised.IndexOf('/', scheme + 3);
        return slash < 0 ? "" : normalised[slash..];
    }
}
