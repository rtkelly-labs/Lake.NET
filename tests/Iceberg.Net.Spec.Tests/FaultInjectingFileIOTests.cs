using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The fault model itself: each kind does what it says, at the point it says, and seeded runs replay.</summary>
[Spec("File System Operations")]
public sealed class FaultInjectingFileIOTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-faultio", Guid.NewGuid().ToString("N"));
    private readonly FaultInjectingFileIO _io = new(new LocalFileIO());

    public FaultInjectingFileIOTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        _io.Dispose();
        if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
    }

    private string P(string name) => Path.Combine(_dir, name);
    private static readonly byte[] Bytes = [.. Enumerable.Range(0, 100).Select(i => (byte)i)];

    [Fact]
    public async Task A_torn_write_leaves_a_truncated_object_and_fails()
    {
        _io.Reset(2, FaultKind.TornWrite);
        await _io.WriteAsync(P("a"), Bytes);
        await Assert.ThrowsAsync<IOException>(() => _io.WriteAsync(P("b"), Bytes));
        Assert.Equal(Bytes, await _io.ReadAllAsync(P("a")));
        Assert.Equal(50, (await _io.ReadAllAsync(P("b"))).Length);
        Assert.Equal([P("b")], _io.Faulted);
    }

    [Fact]
    public async Task Write_faults_land_or_not_as_their_kind_says()
    {
        _io.Reset(1, FaultKind.FailBeforeWrite);
        await Assert.ThrowsAsync<IOException>(() => _io.WriteAsync(P("before"), Bytes));
        Assert.False(File.Exists(P("before")));
        _io.Reset(1, FaultKind.FailAfterWrite);
        await Assert.ThrowsAsync<IOException>(() => _io.WriteAsync(P("after"), Bytes));
        Assert.Equal(Bytes, await File.ReadAllBytesAsync(P("after")));
    }

    [Fact]
    public async Task A_read_fault_fires_at_the_nth_read_of_any_kind()
    {
        await _io.WriteAsync(P("a"), Bytes);
        _io.Reset(3, FaultKind.FailRead);
        await _io.ReadAllAsync(P("a"));
        await _io.LengthAsync(P("a"));
        await Assert.ThrowsAsync<IOException>(() => _io.ReadRangeAsync(P("a"), 0, 10));
        Assert.Equal(10, (await _io.ReadRangeAsync(P("a"), 0, 10)).Length); // one fault only
        Assert.Equal(4, _io.Reads);
        Assert.Equal(0, _io.Writes);
    }

    [Fact]
    public async Task A_listing_is_a_read()
    {
        await _io.WriteAsync(P("a"), Bytes);
        _io.Reset(1, FaultKind.FailRead);
        await Assert.ThrowsAsync<IOException>(async () => { await foreach (var _ in _io.ListAsync(_dir + "/")) { } });
    }

    [Fact]
    public async Task A_slow_read_can_be_cancelled_at_exactly_that_read()
    {
        await _io.WriteAsync(P("a"), Bytes);
        using var cts = new CancellationTokenSource();
        _io.Reset(2, FaultKind.SlowRead);
        _io.OnFault = (_, _) => cts.Cancel();
        await _io.ReadAllAsync(P("a"), cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _io.ReadAllAsync(P("a"), cts.Token));
        Assert.True(_io.Fired);
    }

    [Fact]
    public async Task A_slow_read_without_cancellation_completes()
    {
        await _io.WriteAsync(P("a"), Bytes);
        _io.Reset(1, FaultKind.SlowRead);
        _io.SlowReadDelay = TimeSpan.FromMilliseconds(20);
        Assert.Equal(Bytes, await _io.ReadAllAsync(P("a")));
    }

    [Fact]
    public async Task A_delete_fault_keeps_the_object()
    {
        await _io.WriteAsync(P("a"), Bytes);
        await _io.WriteAsync(P("b"), Bytes);
        _io.Reset(1, FaultKind.FailDelete);
        await Assert.ThrowsAsync<IOException>(() => _io.DeleteAsync(P("a")));
        await _io.DeleteAsync(P("b"));
        Assert.True(File.Exists(P("a")));
        Assert.False(File.Exists(P("b")));
        Assert.Equal([P("b")], _io.Deleted);
    }

    [Fact]
    public async Task The_path_filter_limits_faults_and_counting()
    {
        _io.Reset(1, FaultKind.FailBeforeWrite);
        _io.PathFilter = p => p.EndsWith(".json", StringComparison.Ordinal);
        await _io.WriteAsync(P("data.parquet"), Bytes);
        await Assert.ThrowsAsync<IOException>(() => _io.WriteAsync(P("m.json"), Bytes));
        Assert.Equal(1, _io.Writes);
    }

    [Fact]
    public async Task Seeded_faults_replay_exactly()
    {
        async Task<List<string>> Run(int seed)
        {
            _io.PathFilter = null;
            _io.Randomly(seed, 0.3, FaultKind.FailBeforeWrite, FaultKind.FailRead, FaultKind.FailDelete);
            var outcomes = new List<string>();
            for (var i = 0; i < 40; i++)
            {
                var path = P($"s{seed}-{i}");
                try { await _io.WriteAsync(path, Bytes, overwrite: true); outcomes.Add("w"); } catch (IOException) { outcomes.Add("W!"); }
                try { await _io.ReadAllAsync(path); outcomes.Add("r"); } catch (Exception e) when (e is IOException) { outcomes.Add("R!"); }
                try { await _io.DeleteAsync(path); outcomes.Add("d"); } catch (Exception e) when (e is IOException) { outcomes.Add("D!"); }
            }
            return outcomes;
        }

        var first = await Run(7);
        var again = await Run(7);
        var other = await Run(8);
        Assert.Equal(first, again);
        Assert.NotEqual(first, other);
        Assert.Contains("W!", first);
        Assert.Contains("D!", first);
    }

    [Fact]
    public async Task Seeded_mode_stops_after_the_fault_budget()
    {
        _io.Randomly(1, 1.0, FaultKind.FailBeforeWrite);
        _io.MaxFaults = 2;
        for (var i = 0; i < 2; i++) await Assert.ThrowsAsync<IOException>(() => _io.WriteAsync(P($"x{i}"), Bytes));
        await _io.WriteAsync(P("x2"), Bytes);
        Assert.Equal(2, _io.Faulted.Count);
    }
}
