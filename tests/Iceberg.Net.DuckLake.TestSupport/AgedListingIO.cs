using Iceberg.Net.IO;

namespace Iceberg.Net.DuckLake.TestSupport;

/// <summary>
/// A store whose listing reports every object <paramref name="age"/> older than it is, and also returns
/// <paramref name="extra"/> locations (as a misbehaving or shared listing might). Object stores cannot backdate an object,
/// so orphan cleanup, which only touches files older than an hour, is tested through this on every backend.
/// </summary>
public sealed class AgedListingIO(IFileIO inner, TimeSpan age, IReadOnlyList<string>? extra = null) : IFileIO, ISupportsListing
{
    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var lister = inner as ISupportsListing ?? throw new NotSupportedException($"{inner.GetType().Name} cannot list");
        await foreach (var f in lister.ListAsync(prefix, ct)) yield return f with { LastModifiedUtc = f.LastModifiedUtc - age };
        foreach (var e in extra ?? []) yield return new StoredFile(e, 3, DateTime.UtcNow - age);
    }

    public Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default) => inner.ReadAllAsync(location, ct);
    public Task<Stream> OpenReadAsync(string location, CancellationToken ct = default) => inner.OpenReadAsync(location, ct);
    public Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default) => inner.ReadRangeAsync(location, offset, length, ct);
    public Task<long> LengthAsync(string location, CancellationToken ct = default) => inner.LengthAsync(location, ct);
    public Task<bool> ExistsAsync(string location, CancellationToken ct = default) => inner.ExistsAsync(location, ct);
    public Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default) => inner.WriteAsync(location, content, overwrite, ct);
    public Task DeleteAsync(string location, CancellationToken ct = default) => inner.DeleteAsync(location, ct);
    public void Dispose() { }
}
