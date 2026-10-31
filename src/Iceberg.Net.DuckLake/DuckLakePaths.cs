namespace Iceberg.Net.DuckLake;

/// <summary>
/// Confinement of paths read from catalog rows (untrusted, like every catalog row) and of the names that become path
/// segments. A path flagged relative (<c>path_is_relative</c>) must stay under the directory it is joined to: it may not
/// be absolute, carry a scheme, or have a <c>.</c> or <c>..</c> segment, also once percent-decoded (a <c>file://</c>
/// location is decoded before it reaches the file system). Deletion (scheduled-file cleanup, orphan cleanup) never
/// touches a location outside <c>data_path</c>, whatever a row says.
/// </summary>
internal static partial class DuckLakePaths
{
    /// <summary>A URI scheme (<c>s3:</c>, <c>file:</c>) or a drive letter (<c>C:</c>) at the start: an absolute location.</summary>
    [System.Text.RegularExpressions.GeneratedRegex("^[A-Za-z][A-Za-z0-9+.-]*:")]
    private static partial System.Text.RegularExpressions.Regex SchemeOrDrive();

    /// <summary>A relative path from a catalog row, refused (format error) unless it stays under its parent.</summary>
    public static string CheckRelative(string path, string what) =>
        IsConfined(path) ? path : throw new InvalidDataException($"{what}: relative path '{path}' escapes its directory");

    /// <summary>Whether <paramref name="path"/> is a relative path that cannot leave the directory it is joined to.</summary>
    public static bool IsConfined(string path)
    {
        if (path.Length == 0) return true;
        if (path.Contains('\0') || path[0] is '/' or '\\' || SchemeOrDrive().IsMatch(path)) return false;
        foreach (var segment in path.Split('/', '\\'))
        {
            var decoded = segment;
            // Decode until stable (bounded): %252e%252e decodes to %2e%2e, then to "..".
            for (var i = 0; i < 4 && decoded.Contains('%'); i++)
            {
                var next = Uri.UnescapeDataString(decoded);
                if (next == decoded) break;
                decoded = next;
            }
            // An encoded separator becomes a real one once decoded: check the parts it would split into too.
            if (decoded.Contains('\0') || decoded.Split('/', '\\').Any(part => part is "." or "..")) return false;
        }
        return true;
    }

    /// <summary>Whether <paramref name="location"/> lies under <paramref name="root"/> (which ends in <c>/</c>) without escaping it.</summary>
    public static bool IsUnder(string root, string location) =>
        location.Length > root.Length && location.StartsWith(root, StringComparison.Ordinal) && IsConfined(location[root.Length..]);

    /// <summary>An absolute location (rooted, a drive, or a URI) with no <c>.</c> or <c>..</c> segment after its root.</summary>
    public static bool IsCleanAbsolute(string location)
    {
        var rest = location[SchemeOrDrive().Match(location).Length..];
        var trimmed = rest.TrimStart('/', '\\');
        return trimmed.Length > 0 && trimmed.Length < rest.Length && IsConfined(trimmed);
    }

    /// <summary>
    /// A schema or table name that becomes a directory under <c>data_path</c>: <c>.</c>, <c>..</c>, and names with
    /// <c>/</c>, <c>\</c> or NUL are refused (the directory would not be the object's own). So is a name whose stored
    /// segment (<c>Uri.EscapeDataString(name)</c>) the path check would later refuse, such as <c>%2e%2e</c>, which decodes
    /// to <c>..</c>: it would commit and then fail every load of the object.
    /// </summary>
    public static void CheckName(string name, string what)
    {
        if (string.IsNullOrEmpty(name) || name is "." or ".." || name.Contains('/') || name.Contains('\\') || name.Contains('\0')
            || !IsConfined(Uri.EscapeDataString(name) + "/"))
            throw new ArgumentException($"'{name}' is not a valid {what} name: names may not be empty, '.' or '..' (also percent-encoded), or contain '/', '\\' or NUL", nameof(name));
    }
}
