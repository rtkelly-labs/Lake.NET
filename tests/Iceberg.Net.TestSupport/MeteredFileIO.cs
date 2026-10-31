using System.Collections.Concurrent;
using Iceberg.Net.IO;

namespace Iceberg.Net.TestSupport;

/// <summary>
/// Counts every storage request a FileIO receives, by kind. Request counts are deterministic where wall-clock time is
/// not, so they are what performance budgets gate on (docs/lake/verification/benchmarks.md; RequestBudgetTests).
/// </summary>
public sealed class MeteredFileIO(IFileIO inner) : IFileIO, ISupportsListing
{
    private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);
    private long _bytesRead, _bytesWritten;

    public long BytesRead => Interlocked.Read(ref _bytesRead);
    public long BytesWritten => Interlocked.Read(ref _bytesWritten);

    /// <summary>Requests by kind: read, range, open, length, exists, write, delete, list.</summary>
    public IReadOnlyDictionary<string, long> Counts => new SortedDictionary<string, long>(_counts, StringComparer.Ordinal);

    public long Total => _counts.Values.Sum();

    public void Reset()
    {
        _counts.Clear();
        Interlocked.Exchange(ref _bytesRead, 0);
        Interlocked.Exchange(ref _bytesWritten, 0);
    }

    private void Count(string kind) => _counts.AddOrUpdate(kind, 1, (_, n) => n + 1);

    public async Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default)
    {
        Count("read");
        var b = await inner.ReadAllAsync(location, ct);
        Interlocked.Add(ref _bytesRead, b.Length);
        return b;
    }

    public Task<Stream> OpenReadAsync(string location, CancellationToken ct = default)
    {
        Count("open");
        return inner.OpenReadAsync(location, ct);
    }

    public async Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default)
    {
        Count("range");
        var b = await inner.ReadRangeAsync(location, offset, length, ct);
        Interlocked.Add(ref _bytesRead, b.Length);
        return b;
    }

    public Task<long> LengthAsync(string location, CancellationToken ct = default)
    {
        Count("length");
        return inner.LengthAsync(location, ct);
    }

    public Task<bool> ExistsAsync(string location, CancellationToken ct = default)
    {
        Count("exists");
        return inner.ExistsAsync(location, ct);
    }

    public Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default)
    {
        Count("write");
        Interlocked.Add(ref _bytesWritten, content.Length);
        return inner.WriteAsync(location, content, overwrite, ct);
    }

    public Task DeleteAsync(string location, CancellationToken ct = default)
    {
        Count("delete");
        return inner.DeleteAsync(location, ct);
    }

    public IAsyncEnumerable<StoredFile> ListAsync(string prefix, CancellationToken ct = default)
    {
        Count("list");
        return inner is ISupportsListing l ? l.ListAsync(prefix, ct) : throw new NotSupportedException("The wrapped FileIO cannot list");
    }

    public void Dispose() => inner.Dispose();
}
