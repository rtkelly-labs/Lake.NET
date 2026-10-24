namespace Iceberg.Net.IO;

/// <summary>
/// A read-only, seekable view of a stored object that fetches only the byte ranges that are read, through
/// <see cref="IFileIO.ReadRangeAsync"/>, and keeps them. A columnar reader on top of it (Parquet) then downloads the
/// footer and the column chunks it decodes rather than the whole file. Ranges can be fetched ahead with
/// <see cref="PrefetchAsync"/> (no blocking); a read of bytes not yet fetched fetches them synchronously, at least
/// <see cref="MinimumFetch"/> bytes at a time, on the thread pool so a synchronization context cannot deadlock it.
/// Not thread-safe, like any <see cref="Stream"/>.
/// </summary>
public sealed class RangeReadStream : Stream
{
    private readonly IFileIO _io;
    private readonly string _location;
    private readonly CancellationToken _ct;
    // Fetched ranges by offset, never overlapping.
    private readonly SortedList<long, byte[]> _ranges = [];
    private long _position;

    /// <param name="length">The object's size in bytes (a manifest's <c>file_size_in_bytes</c>, or <see cref="IFileIO.LengthAsync"/>).</param>
    public RangeReadStream(IFileIO io, string location, long length, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        _io = io ?? throw new ArgumentNullException(nameof(io));
        _location = location ?? throw new ArgumentNullException(nameof(location));
        Length = length;
        _ct = ct;
    }

    /// <summary>The smallest synchronous fetch, so a reader's small sequential reads do not become one request each.</summary>
    public int MinimumFetch { get; init; } = 8 * 1024;

    /// <summary>Bytes downloaded so far.</summary>
    public long BytesFetched { get; private set; }

    /// <summary>Range requests made so far.</summary>
    public int Requests { get; private set; }

    public override long Length { get; }
    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;

    public override long Position
    {
        get => _position;
        set => _position = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value));
    }

    /// <summary>Fetch <c>[offset, offset + count)</c> (clamped to the object) ahead of reading it; parts already fetched are not fetched again.</summary>
    public async Task PrefetchAsync(long offset, long count, CancellationToken ct = default)
    {
        var end = Math.Min(Length, offset + count);
        for (var at = Math.Max(0, offset); at < end;)
        {
            if (Covering(at) is { } hit)
            {
                at = hit.Offset + hit.Bytes.Length;
                continue;
            }
            var gapEnd = Math.Min(end, NextStart(at));
            var take = (int)Math.Min(gapEnd - at, int.MaxValue);
            await FetchAsync(at, take, ct).ConfigureAwait(false);
            at += take;
        }
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        var total = 0;
        while (buffer.Length > 0 && _position < Length)
        {
            if (Covering(_position) is not { } hit)
            {
                // Fetch the gap up to the next fetched range (or the end of the read), at least MinimumFetch bytes.
                var want = Math.Max(buffer.Length, MinimumFetch);
                var take = (int)Math.Min(Math.Min(want, Length - _position), NextStart(_position) - _position);
                var at = _position;
                Task.Run(() => FetchAsync(at, take, _ct), _ct).GetAwaiter().GetResult();
                continue;
            }
            var from = (int)(_position - hit.Offset);
            var n = Math.Min(buffer.Length, hit.Bytes.Length - from);
            hit.Bytes.AsSpan(from, n).CopyTo(buffer);
            buffer = buffer[n..];
            _position += n;
            total += n;
        }
        return total;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
    {
        await PrefetchAsync(_position, buffer.Length, ct).ConfigureAwait(false);
        return Read(buffer.Span);
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
        ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

    public override long Seek(long offset, SeekOrigin origin) => Position = origin switch
    {
        SeekOrigin.Begin => offset,
        SeekOrigin.Current => _position + offset,
        SeekOrigin.End => Length + offset,
        _ => throw new ArgumentOutOfRangeException(nameof(origin)),
    };

    private async Task FetchAsync(long offset, int count, CancellationToken ct)
    {
        var bytes = await _io.ReadRangeAsync(_location, offset, count, ct).ConfigureAwait(false);
        if (bytes.Length != count)
            throw new IOException($"Short read of {_location}: asked for {count} bytes at {offset}, got {bytes.Length} (is the recorded file size {Length} right?)");
        _ranges.Add(offset, bytes);
        BytesFetched += count;
        Requests++;
    }

    /// <summary>The fetched range containing <paramref name="at"/>, if any.</summary>
    private (long Offset, byte[] Bytes)? Covering(long at)
    {
        var keys = _ranges.Keys;
        int lo = 0, hi = keys.Count - 1, found = -1;
        while (lo <= hi) // last range starting at or before `at`
        {
            var mid = (lo + hi) >>> 1;
            if (keys[mid] <= at) { found = mid; lo = mid + 1; }
            else hi = mid - 1;
        }
        if (found < 0) return null;
        var bytes = _ranges.Values[found];
        return at < keys[found] + bytes.Length ? (keys[found], bytes) : null;
    }

    /// <summary>The start of the first fetched range after <paramref name="at"/>, or the object's length.</summary>
    private long NextStart(long at)
    {
        foreach (var k in _ranges.Keys)
            if (k > at) return k;
        return Length;
    }

    public override void Flush() { }
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
