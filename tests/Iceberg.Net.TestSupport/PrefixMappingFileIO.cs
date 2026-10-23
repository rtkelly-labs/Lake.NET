using Iceberg.Net.IO;

namespace Iceberg.Net.TestSupport;

/// <summary>
/// Maps absolute locations recorded by another engine (e.g. <c>file:///tmp/sentric-spark-fixture-root/warehouse</c>)
/// onto a local copy, for reads and writes, so fixture tables can be read and then written on top of without
/// rewriting their metadata. Locations outside the prefix pass through unchanged.
/// </summary>
public sealed class PrefixMappingFileIO(string fromPrefix, string toDirectory) : IFileIO
{
    private readonly LocalFileIO _local = new();
    private readonly string _to = toDirectory.TrimEnd('/');

    public string Map(string location)
    {
        // Java's Hadoop paths record "file:/tmp/x" where others write "file:///tmp/x": treat them as the same location.
        if (location.StartsWith("file:/", StringComparison.Ordinal) && !location.StartsWith("file://", StringComparison.Ordinal))
            location = "file://" + location["file:".Length..];
        return location.StartsWith(fromPrefix, StringComparison.Ordinal) ? _to + location[fromPrefix.Length..] : location;
    }

    public Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default) => _local.ReadAllAsync(Map(l), ct);
    public Task<Stream> OpenReadAsync(string l, CancellationToken ct = default) => _local.OpenReadAsync(Map(l), ct);
    public Task<byte[]> ReadRangeAsync(string l, long o, int n, CancellationToken ct = default) => _local.ReadRangeAsync(Map(l), o, n, ct);
    public Task<long> LengthAsync(string l, CancellationToken ct = default) => _local.LengthAsync(Map(l), ct);
    public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => _local.ExistsAsync(Map(l), ct);
    public Task WriteAsync(string l, ReadOnlyMemory<byte> c, bool o = false, CancellationToken ct = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Map(l))!);
        return _local.WriteAsync(Map(l), c, o, ct);
    }
    public Task DeleteAsync(string l, CancellationToken ct = default) => _local.DeleteAsync(Map(l), ct);
    public void Dispose() => _local.Dispose();

    /// <summary>Copy a directory tree (fixtures are read-only inputs; tests write to a copy).</summary>
    public static void CopyTree(string from, string to)
    {
        foreach (var dir in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(to, Path.GetRelativePath(from, dir)));
        Directory.CreateDirectory(to);
        foreach (var file in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
            File.Copy(file, Path.Combine(to, Path.GetRelativePath(from, file)), overwrite: true);
    }
}
