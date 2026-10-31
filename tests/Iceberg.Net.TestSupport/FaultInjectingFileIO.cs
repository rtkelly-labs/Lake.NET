using System.Runtime.CompilerServices;
using Iceberg.Net.IO;

namespace Iceberg.Net.TestSupport;

public enum FaultKind
{
    /// <summary>The write never reaches storage (connection refused, 503 before upload).</summary>
    FailBeforeWrite,
    /// <summary>The write lands but the caller sees an error (timeout after the server stored it): the ambiguous case.</summary>
    FailAfterWrite,
    /// <summary>
    /// Half of the object is stored, then the write fails (a local or NFS file system, or a store without atomic PUT).
    /// The truncated object stays where it was written.
    /// </summary>
    TornWrite,
    /// <summary>The read fails (5xx, connection reset). Reads are <c>ReadAll</c>, <c>OpenRead</c>, <c>ReadRange</c>, <c>Length</c> and <c>List</c>.</summary>
    FailRead,
    /// <summary>
    /// The read stalls for <see cref="FaultInjectingFileIO.SlowReadDelay"/> before it proceeds, honouring the caller's
    /// cancellation token. <see cref="FaultInjectingFileIO.OnFault"/> runs first, so a test can cancel the operation at
    /// exactly this read.
    /// </summary>
    SlowRead,
    /// <summary>The delete fails and the object stays (403, 5xx, a lock on Windows).</summary>
    FailDelete,
}

/// <summary>
/// Wraps a FileIO and injects faults into writes, reads and deletes. Two modes:
/// <list type="bullet">
/// <item><b>At a point</b> (<see cref="Reset(int?, FaultKind)"/>): one fault at the N-th (1-based) call of the class the
/// kind targets — writes, reads or deletes. A clean run counts each class (<see cref="Writes"/>, <see cref="Reads"/>,
/// <see cref="Deletes"/>), so a test can enumerate every fault point of an operation.</item>
/// <item><b>Seeded</b> (<see cref="Randomly"/>): every call of a targeted class fails with a fixed probability, drawn
/// from a <see cref="Random"/> with the given seed, so a sequential run replays exactly.</item>
/// </list>
/// <see cref="PathFilter"/> limits both modes to matching paths (non-matching calls are neither faulted nor counted).
/// Calls made concurrently (parallel deletes) are numbered in arrival order: the N-th one is faulted, whichever it is.
/// </summary>
public sealed class FaultInjectingFileIO(IFileIO inner) : IFileIO, ISupportsListing
{
    private readonly object _gate = new();
    private int _writes, _reads, _deletes;
    private Random? _random;
    private double _rate;
    private FaultKind[] _randomKinds = [];

    public int? FailAtWrite { get; set; }
    public FaultKind Kind { get; set; } = FaultKind.FailBeforeWrite;
    public Func<string, bool>? PathFilter { get; set; }
    public int Writes => _writes;
    public int Reads => _reads;
    public int Deletes => _deletes;
    public List<string> Written { get; } = [];
    /// <summary>Every location this FileIO deleted (successfully), in order.</summary>
    public List<string> Deleted { get; } = [];
    /// <summary>The locations faulted so far.</summary>
    public List<string> Faulted { get; } = [];
    /// <summary>A point fault has fired (seeded mode: at least one has).</summary>
    public bool Fired { get; private set; }
    /// <summary>How long a <see cref="FaultKind.SlowRead"/> stalls.</summary>
    public TimeSpan SlowReadDelay { get; set; } = TimeSpan.FromSeconds(30);
    /// <summary>Runs when a fault fires, before it takes effect (e.g. cancel the operation at a slow read).</summary>
    public Action<FaultKind, string>? OnFault { get; set; }
    /// <summary>Seeded mode: stop injecting after this many faults.</summary>
    public int MaxFaults { get; set; } = int.MaxValue;

    /// <summary>The fault point of the kind's class (<see cref="FailAtWrite"/> also numbers reads and deletes).</summary>
    public int? FailAt { get => FailAtWrite; set => FailAtWrite = value; }

    /// <summary>Clear counters and arm one fault (or none, when <paramref name="failAt"/> is null).</summary>
    public void Reset(int? failAt = null, FaultKind kind = FaultKind.FailBeforeWrite)
    {
        lock (_gate)
        {
            _writes = _reads = _deletes = 0;
            FailAtWrite = failAt;
            Kind = kind;
            Fired = false;
            _random = null;
            Written.Clear();
            Deleted.Clear();
            Faulted.Clear();
        }
    }

    /// <summary>Disarm every fault, keeping the counters and logs.</summary>
    public void Disarm()
    {
        lock (_gate)
        {
            FailAtWrite = null;
            _random = null;
        }
    }

    /// <summary>
    /// Seeded mode: each call of a class one of <paramref name="kinds"/> targets fails with probability
    /// <paramref name="rate"/> (the kind among those of its class is drawn too), up to <see cref="MaxFaults"/>.
    /// </summary>
    public void Randomly(int seed, double rate, params FaultKind[] kinds)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rate);
        Reset();
        lock (_gate)
        {
            _random = new Random(seed);
            _rate = rate;
            _randomKinds = kinds.Length > 0 ? kinds : Enum.GetValues<FaultKind>();
        }
    }

    private enum Op { Write, Read, Delete }

    private static Op ClassOf(FaultKind kind) => kind switch
    {
        FaultKind.FailBeforeWrite or FaultKind.FailAfterWrite or FaultKind.TornWrite => Op.Write,
        FaultKind.FailRead or FaultKind.SlowRead => Op.Read,
        _ => Op.Delete,
    };

    /// <summary>Count the call and decide whether (and how) it is faulted.</summary>
    private FaultKind? Decide(Op op, string location)
    {
        if (PathFilter is not null && !PathFilter(location)) return null;
        lock (_gate)
        {
            var n = op switch { Op.Write => ++_writes, Op.Read => ++_reads, _ => ++_deletes };
            FaultKind? fault = null;
            if (_random is not null)
            {
                var eligible = _randomKinds.Where(k => ClassOf(k) == op).ToArray();
                // Always draw, so the sequence of decisions does not depend on which kinds are enabled.
                var roll = _random.NextDouble();
                var pick = _random.Next(Math.Max(1, eligible.Length));
                if (eligible.Length > 0 && Faulted.Count < MaxFaults && roll < _rate) fault = eligible[pick];
            }
            else if (!Fired && FailAtWrite == n && ClassOf(Kind) == op) fault = Kind;
            if (fault is null) return null;
            Fired = true;
            Faulted.Add(location);
            return fault;
        }
    }

    private void Fire(FaultKind kind, string location) => OnFault?.Invoke(kind, location);

    private IOException Injected(FaultKind kind, string location) => new($"Injected {kind} fault: {location}");

    public async Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default)
    {
        switch (Decide(Op.Write, location))
        {
            case null:
                break;
            case FaultKind.FailBeforeWrite:
                Fire(FaultKind.FailBeforeWrite, location);
                throw Injected(FaultKind.FailBeforeWrite, location);
            case FaultKind.FailAfterWrite:
                Fire(FaultKind.FailAfterWrite, location);
                await inner.WriteAsync(location, content, overwrite, ct);
                lock (_gate) Written.Add(location);
                throw Injected(FaultKind.FailAfterWrite, location);
            case var kind:
                Fire(kind.Value, location);
                await inner.WriteAsync(location, content[..(content.Length / 2)], overwrite, ct);
                lock (_gate) Written.Add(location);
                throw Injected(kind.Value, location);
        }
        await inner.WriteAsync(location, content, overwrite, ct);
        lock (_gate) Written.Add(location);
    }

    private async Task BeforeReadAsync(string location, CancellationToken ct)
    {
        if (Decide(Op.Read, location) is not { } kind) return;
        Fire(kind, location);
        if (kind == FaultKind.FailRead) throw Injected(kind, location);
        await Task.Delay(SlowReadDelay, ct);
    }

    public async Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default)
    {
        await BeforeReadAsync(l, ct);
        return await inner.ReadAllAsync(l, ct);
    }

    public async Task<Stream> OpenReadAsync(string l, CancellationToken ct = default)
    {
        await BeforeReadAsync(l, ct);
        return await inner.OpenReadAsync(l, ct);
    }

    public async Task<byte[]> ReadRangeAsync(string l, long o, int n, CancellationToken ct = default)
    {
        await BeforeReadAsync(l, ct);
        return await inner.ReadRangeAsync(l, o, n, ct);
    }

    public async Task<long> LengthAsync(string l, CancellationToken ct = default)
    {
        await BeforeReadAsync(l, ct);
        return await inner.LengthAsync(l, ct);
    }

    public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => inner.ExistsAsync(l, ct);

    public async Task DeleteAsync(string l, CancellationToken ct = default)
    {
        if (Decide(Op.Delete, l) is { } kind)
        {
            Fire(kind, l);
            throw Injected(kind, l);
        }
        await inner.DeleteAsync(l, ct);
        lock (_gate) Deleted.Add(l);
    }

    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (inner is not ISupportsListing lister) throw new NotSupportedException($"{inner.GetType().Name} cannot list files");
        await BeforeReadAsync(prefix, ct);
        await foreach (var f in lister.ListAsync(prefix, ct)) yield return f;
    }

    public void Dispose() => inner.Dispose();
}
