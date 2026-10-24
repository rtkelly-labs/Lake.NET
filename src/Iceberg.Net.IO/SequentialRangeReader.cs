namespace Iceberg.Net.IO;

/// <summary>
/// Reads a stored object front to back in chunks of <see cref="ChunkSize"/> bytes through
/// <see cref="IFileIO.ReadRangeAsync"/>, keeping one chunk in flight ahead of the reader, and holding only the bytes
/// not yet consumed. For row-oriented formats read in order (Avro container blocks): memory is bounded by a chunk plus
/// the largest unit the reader asks for, fetching overlaps decoding, and a reader that stops early (a <c>Take</c>, a
/// cancelled scan) has fetched only the chunks it reached plus the one ahead. <see cref="RangeReadStream"/> is the
/// random-access counterpart for formats that seek (Parquet). Not thread-safe.
/// </summary>
public sealed class SequentialRangeReader : IAsyncDisposable
{
    private readonly IFileIO _io;
    private readonly string _location;
    private readonly CancellationTokenSource _cts;
    // Unconsumed bytes [_start, _start + _count) at _buffer[_head.._head + _count).
    private byte[] _buffer = [];
    private int _head;
    private long _start;
    private int _count;
    private Task<byte[]>? _ahead;
    private long _aheadOffset;

    /// <param name="length">The object's size in bytes (a manifest's <c>file_size_in_bytes</c>).</param>
    public SequentialRangeReader(IFileIO io, string location, long length, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        _io = io ?? throw new ArgumentNullException(nameof(io));
        _location = location ?? throw new ArgumentNullException(nameof(location));
        Length = length;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    }

    /// <summary>Bytes per request (the first request, and each one ahead). A larger <see cref="EnsureAsync"/> asks for more.</summary>
    public int ChunkSize { get; init; } = 1024 * 1024;

    public long Length { get; }

    /// <summary>Bytes downloaded so far (including a chunk fetched ahead and not yet consumed).</summary>
    public long BytesFetched { get; private set; }

    /// <summary>Range requests made so far.</summary>
    public int Requests { get; private set; }

    /// <summary>
    /// Make <c>[offset, offset + count)</c> available (clamped to the object) and return the buffer holding it from
    /// index <c>Index</c>; <c>Available</c> bytes from there are valid (at least <paramref name="count"/> unless the
    /// object ends first). Bytes before <paramref name="offset"/> are released: offsets never go backwards. The buffer
    /// is valid until the next call.
    /// </summary>
    public async ValueTask<(byte[] Buffer, int Index, int Available)> EnsureAsync(long offset, int count, CancellationToken ct = default)
    {
        if (offset < _start) throw new InvalidOperationException($"Sequential read went backwards: {offset} < {_start}");
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        Release(offset);
        var end = Math.Min(Length, offset + (long)count);
        while (_start + _count < end)
        {
            var at = _start + _count;
            byte[] bytes;
            if (_ahead is not null && _aheadOffset == at)
            {
                bytes = await _ahead.WaitAsync(ct).ConfigureAwait(false);
                _ahead = null;
            }
            else
            {
                await DropAheadAsync().ConfigureAwait(false);
                bytes = await FetchAsync(at, (int)Math.Min(Length - at, Math.Max(ChunkSize, end - at)), ct).ConfigureAwait(false);
            }
            Append(bytes);
        }
        // Keep one chunk in flight while the caller decodes what it has.
        var next = _start + _count;
        if (_ahead is null && next < Length)
        {
            _aheadOffset = next;
            _ahead = FetchAsync(next, (int)Math.Min(Length - next, ChunkSize), _cts.Token);
        }
        return (_buffer, _head, _count);
    }

    private async Task<byte[]> FetchAsync(long offset, int count, CancellationToken ct)
    {
        Requests++;
        var bytes = await _io.ReadRangeAsync(_location, offset, count, ct).ConfigureAwait(false);
        if (bytes.Length != count)
            throw new IOException($"Short read of {_location}: asked for {count} bytes at {offset}, got {bytes.Length} (is the recorded file size {Length} right?)");
        BytesFetched += count;
        return bytes;
    }

    private void Release(long offset)
    {
        var drop = offset - _start;
        if (drop <= 0) return;
        if (drop >= _count)
        {
            _head = _count = 0;
            _start = offset;
            return;
        }
        _head += (int)drop;
        _count -= (int)drop;
        _start = offset;
    }

    private void Append(byte[] bytes)
    {
        if (_count == 0)
        {
            _buffer = bytes;
            _head = 0;
            _count = bytes.Length;
            return;
        }
        if (_buffer.Length - _head - _count < bytes.Length)
        {
            // Compact the unconsumed bytes to the front, growing the buffer only when they and the new chunk do not fit.
            var needed = (long)_count + bytes.Length;
            var target = needed <= _buffer.Length ? _buffer : new byte[Math.Min(Math.Max(needed, _buffer.Length * 2L), Array.MaxLength)];
            Buffer.BlockCopy(_buffer, _head, target, 0, _count);
            _buffer = target;
            _head = 0;
        }
        Buffer.BlockCopy(bytes, 0, _buffer, _head + _count, bytes.Length);
        _count += bytes.Length;
    }

    /// <summary>Abandon the chunk in flight (a skip ahead of it): its result, or failure, is observed and dropped.</summary>
    private async Task DropAheadAsync()
    {
        if (_ahead is not { } ahead) return;
        _ahead = null;
        try { await ahead.ConfigureAwait(false); }
        catch (Exception e) when (e is not OutOfMemoryException) { /* not needed any more */ }
    }

    /// <summary>Cancels the chunk in flight, if any, and observes it.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_ahead is not null) _cts.Cancel();
        await DropAheadAsync().ConfigureAwait(false);
        _cts.Dispose();
    }
}
