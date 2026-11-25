using Iceberg.Net.IO;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// What every FileIO must do, run against each one. Commits, scans and maintenance are written against
/// <see cref="IFileIO"/>, so a store that differs here differs everywhere: a listing that spells a key differently
/// makes orphan cleanup delete live files, and a short ranged read hands a decoder a truncated buffer.
/// </summary>
public abstract class FileIOConformanceTests : IDisposable
{
    private readonly List<string> _written = [];

    /// <summary>A fresh, empty prefix ending in '/' for this test.</summary>
    protected abstract string Root { get; }

    protected abstract IFileIO Create();

    /// <summary>Throws a skip when the store isn't available.</summary>
    protected virtual void RequireStore() { }

    private IFileIO? _io;
    private IFileIO Io => _io ??= Create();

    private string At(string name)
    {
        var location = Root + name;
        _written.Add(location);
        return location;
    }

    public void Dispose()
    {
        if (_io is null) return;
        foreach (var l in _written)
            try { _io.DeleteAsync(l).GetAwaiter().GetResult(); } catch (Exception) { /* best effort */ }
        _io.Dispose();
        GC.SuppressFinalize(this);
    }

    [SkippableFact]
    public async Task Exactly_one_of_many_concurrent_creates_wins()
    {
        RequireStore();
        var location = At("race.metadata.json");
        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(async i =>
        {
            try
            {
                await Io.WriteAsync(location, new[] { (byte)i });
                return (Won: true, Writer: i);
            }
            catch (FileAlreadyExistsException)
            {
                return (Won: false, Writer: i);
            }
        }));
        var winner = Assert.Single(results, r => r.Won);
        Assert.Equal(new[] { (byte)winner.Writer }, await Io.ReadAllAsync(location));
    }

    [SkippableFact]
    public async Task Overwrite_replaces_and_create_refuses()
    {
        RequireStore();
        var location = At("data.bin");
        await Io.WriteAsync(location, "one"u8.ToArray());
        await Assert.ThrowsAsync<FileAlreadyExistsException>(() => Io.WriteAsync(location, "two"u8.ToArray()));
        await Io.WriteAsync(location, "three"u8.ToArray(), overwrite: true);
        Assert.Equal("three"u8.ToArray(), await Io.ReadAllAsync(location));
    }

    [SkippableFact]
    public async Task A_missing_object_is_FileNotFound_from_every_read()
    {
        RequireStore();
        var location = Root + "missing.bin";
        await Assert.ThrowsAsync<FileNotFoundException>(() => Io.ReadAllAsync(location));
        await Assert.ThrowsAsync<FileNotFoundException>(async () => { await using var s = await Io.OpenReadAsync(location); await s.ReadExactlyAsync(new byte[1]); });
        await Assert.ThrowsAsync<FileNotFoundException>(() => Io.ReadRangeAsync(location, 0, 1));
        await Assert.ThrowsAsync<FileNotFoundException>(() => Io.LengthAsync(location));
        Assert.False(await Io.ExistsAsync(location));
    }

    [SkippableFact]
    public async Task Ranged_reads_are_exact_and_never_short()
    {
        RequireStore();
        var location = At("range.bin");
        var bytes = Enumerable.Range(0, 1000).Select(i => (byte)i).ToArray();
        await Io.WriteAsync(location, bytes);
        Assert.Equal(1000, await Io.LengthAsync(location));
        Assert.Equal(bytes[..1], await Io.ReadRangeAsync(location, 0, 1));
        Assert.Equal(bytes[990..], await Io.ReadRangeAsync(location, 990, 10));
        Assert.Equal(bytes, await Io.ReadRangeAsync(location, 0, 1000));
        // Past the end, a store may return fewer bytes than asked; the FileIO must not pass that on as if it were the range.
        await Assert.ThrowsAsync<EndOfStreamException>(() => Io.ReadRangeAsync(location, 995, 10));
    }

    [SkippableFact]
    public async Task Zero_byte_objects_round_trip()
    {
        RequireStore();
        var location = At("empty.bin");
        await Io.WriteAsync(location, Array.Empty<byte>());
        Assert.True(await Io.ExistsAsync(location));
        Assert.Equal(0, await Io.LengthAsync(location));
        Assert.Empty(await Io.ReadAllAsync(location));
    }

    [SkippableFact]
    public async Task Deleting_twice_is_not_an_error()
    {
        RequireStore();
        var location = At("gone.bin");
        await Io.WriteAsync(location, new byte[] { 1 });
        await Io.DeleteAsync(location);
        await Io.DeleteAsync(location);
        Assert.False(await Io.ExistsAsync(location));
    }

    public static TheoryData<string> AwkwardNames => new()
    {
        "with space.parquet",
        "plus+sign.parquet",
        "percent%20literal.parquet",
        "unicode-é-日本.parquet",
        "nested/dir/file.parquet",
        "equals=sign/k=v.parquet",
    };

    [SkippableTheory]
    [MemberData(nameof(AwkwardNames))]
    public async Task Listing_spells_every_location_exactly_as_it_was_written(string name)
    {
        // Orphan cleanup deletes what the listing returns and the metadata doesn't name; a listing that encodes or decodes
        // a key differently from how it was written would delete a live file.
        RequireStore();
        var location = At(name);
        await Io.WriteAsync(location, new byte[] { 7 });
        var listed = await ListAsync(Root);
        Assert.Contains(location, listed.Select(f => f.Location));
        Assert.Equal(new byte[] { 7 }, await Io.ReadAllAsync(listed.Single(f => f.Location == location).Location));
    }

    [SkippableFact]
    public async Task Listing_pages_through_every_object_with_sizes_and_times_and_stays_inside_its_prefix()
    {
        RequireStore();
        const int count = 1100; // more than one S3 page
        await Parallel.ForEachAsync(Enumerable.Range(0, count), new ParallelOptions { MaxDegreeOfParallelism = 16 },
            async (i, ct) => await Io.WriteAsync(At($"t/f{i:D5}.bin"), new byte[i % 7], ct: ct));
        var sibling = At("t2/f.bin"); // shares the "t" prefix as a string, not as a directory
        await Io.WriteAsync(sibling, new byte[] { 1 });

        var listed = await ListAsync(Root + "t/");
        Assert.Equal(count, listed.Count);
        Assert.DoesNotContain(sibling, listed.Select(f => f.Location));
        Assert.All(listed, f =>
        {
            var i = int.Parse(f.Location[^9..^4], System.Globalization.CultureInfo.InvariantCulture);
            Assert.Equal(i % 7, f.Size);
            // A missing timestamp must not read as "very old": orphan cleanup would take the file for a stale orphan.
            Assert.InRange(f.LastModifiedUtc, DateTime.UtcNow.AddDays(-1), DateTime.UtcNow.AddDays(1));
        });
    }

    [SkippableFact]
    public async Task A_cancelled_token_stops_reads_and_writes()
    {
        RequireStore();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var location = At("cancelled.bin");
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Io.WriteAsync(location, new byte[] { 1 }, ct: cts.Token));
        await Io.WriteAsync(location, new byte[] { 1 }, overwrite: true);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Io.ReadAllAsync(location, cts.Token));
    }

    private async Task<List<StoredFile>> ListAsync(string prefix)
    {
        var lister = Assert.IsAssignableFrom<ISupportsListing>(Io);
        var files = new List<StoredFile>();
        await foreach (var f in lister.ListAsync(prefix)) files.Add(f);
        return files;
    }
}

[Spec("File System Operations")]
public sealed class LocalFileIOConformanceTests : FileIOConformanceTests
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fileio-conformance", Guid.NewGuid().ToString("N"));

    protected override string Root => _dir.Replace('\\', '/') + "/";

    protected override IFileIO Create() => new LocalFileIO();
}

[Spec("File System Operations")]
[Trait("Category", "Regression")]
public sealed class S3FileIOConformanceTests : FileIOConformanceTests
{
    private readonly string _root = $"{TestEnvironment.Current.Warehouse.TrimEnd('/')}/fileio-conformance/{TestEnvironment.Current.RunId}/{Guid.NewGuid():N}/";

    protected override string Root => _root;

    protected override void RequireStore() => Require.Target();

    protected override IFileIO Create() => new S3FileIO(TestEnvironment.Current.S3Options);
}
