using Iceberg.Net.IO;

namespace Iceberg.Net.TestSupport;

public enum FaultKind
{
    /// <summary>The write never reaches storage (connection refused, 503 before upload).</summary>
    FailBeforeWrite,
    /// <summary>The write lands but the caller sees an error (timeout after the server stored it): the ambiguous case.</summary>
    FailAfterWrite,
}

/// <summary>
/// Wraps a FileIO and injects one fault at the N-th write (1-based), optionally only for paths matching a filter.
/// Counts writes so a clean run can discover how many fault points an operation has.
/// </summary>
public sealed class FaultInjectingFileIO(IFileIO inner) : IFileIO
{
    private int _writes;

    public int? FailAtWrite { get; set; }
    public FaultKind Kind { get; set; } = FaultKind.FailBeforeWrite;
    public Func<string, bool>? PathFilter { get; set; }
    public int Writes => _writes;
    public List<string> Written { get; } = [];
    public bool Fired { get; private set; }
    /// <summary>Observe the exact injected exception at the storage boundary, before it propagates.</summary>
    public Action<string, FaultKind, Exception>? OnFault { get; set; }

    public void Reset(int? failAt = null, FaultKind kind = FaultKind.FailBeforeWrite)
    {
        _writes = 0;
        FailAtWrite = failAt;
        Kind = kind;
        Fired = false;
        Written.Clear();
    }

    public async Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default)
    {
        if (PathFilter is not null && !PathFilter(location))
        {
            await inner.WriteAsync(location, content, overwrite, ct);
            return;
        }
        var n = Interlocked.Increment(ref _writes);
        if (n == FailAtWrite && !Fired)
        {
            Fired = true;
            if (Kind == FaultKind.FailAfterWrite)
            {
                await inner.WriteAsync(location, content, overwrite, ct);
                lock (Written) Written.Add(location);
            }
            var error = new IOException($"Injected {Kind} fault at write #{n}: {location}");
            OnFault?.Invoke(location, Kind, error);
            throw error;
        }
        await inner.WriteAsync(location, content, overwrite, ct);
        lock (Written) Written.Add(location);
    }

    public Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default) => inner.ReadAllAsync(l, ct);
    public Task<Stream> OpenReadAsync(string l, CancellationToken ct = default) => inner.OpenReadAsync(l, ct);
    public Task<byte[]> ReadRangeAsync(string l, long o, int n, CancellationToken ct = default) => inner.ReadRangeAsync(l, o, n, ct);
    public Task<long> LengthAsync(string l, CancellationToken ct = default) => inner.LengthAsync(l, ct);
    public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => inner.ExistsAsync(l, ct);
    public Task DeleteAsync(string l, CancellationToken ct = default) => inner.DeleteAsync(l, ct);
    public void Dispose() => inner.Dispose();
}
