using Iceberg.Net.IO;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Which FileIO a location reaches. A scheme with no FileIO must be refused, never treated as a local path: a
/// <c>gs://</c> warehouse written to <c>./gs:/…</c> on local disk is a table no other engine can see.
/// </summary>
public sealed class FileIORoutingTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "fileio-routing-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    [Theory]
    [InlineData("s3://b/k", "s3")]
    [InlineData("S3A://b/k", "s3a")]
    [InlineData("gs://b/k", "gs")]
    [InlineData("abfss://c@a.dfs.core.windows.net/k", "abfss")]
    [InlineData("file:///tmp/x", "file")]
    [InlineData("file:/tmp/x", "file")]
    [InlineData("/tmp/x", null)]
    [InlineData("relative/x", null)]
    [InlineData("s3data/x", null)]
    [InlineData(@"C:\wh\t", null)]
    [InlineData("C:/wh/t", null)]
    [InlineData("1x://b/k", null)]
    public void Scheme_of_a_location(string location, string? scheme) =>
        Assert.Equal(scheme, StorageScheme.Of(location));

    public static TheoryData<string> Unsupported => new()
    {
        "gs://bucket/wh/t/metadata/v1.metadata.json",
        "abfss://container@account.dfs.core.windows.net/wh/t/data/f.parquet",
        "wasbs://container@account.blob.core.windows.net/wh/f",
        "hdfs://namenode:8020/wh/t/f",
        "oss://bucket/f",
        "http://example.com/f",
    };

    [Theory]
    [MemberData(nameof(Unsupported))]
    public async Task Resolving_FileIO_refuses_schemes_it_has_no_FileIO_for(string location)
    {
        var s3Created = false;
        using var io = new ResolvingFileIO(() => { s3Created = true; throw new InvalidOperationException("no S3 here"); });
        var cwdBefore = Directory.GetFileSystemEntries(Directory.GetCurrentDirectory()).Length;

        await Assert.ThrowsAsync<NotSupportedException>(() => io.WriteAsync(location, new byte[] { 1 }));
        await Assert.ThrowsAsync<NotSupportedException>(() => io.ReadAllAsync(location));
        await Assert.ThrowsAsync<NotSupportedException>(() => io.OpenReadAsync(location));
        await Assert.ThrowsAsync<NotSupportedException>(() => io.ReadRangeAsync(location, 0, 1));
        await Assert.ThrowsAsync<NotSupportedException>(() => io.LengthAsync(location));
        await Assert.ThrowsAsync<NotSupportedException>(() => io.ExistsAsync(location));
        await Assert.ThrowsAsync<NotSupportedException>(() => io.DeleteAsync(location));
        await Assert.ThrowsAsync<NotSupportedException>(async () => { await foreach (var _ in io.ListAsync(location)) { } });

        Assert.False(s3Created);
        Assert.Equal(cwdBefore, Directory.GetFileSystemEntries(Directory.GetCurrentDirectory()).Length);
    }

    [Theory]
    [MemberData(nameof(Unsupported))]
    public async Task Local_FileIO_refuses_other_schemes_instead_of_making_directories_named_after_them(string location)
    {
        using var io = new LocalFileIO();
        await Assert.ThrowsAsync<NotSupportedException>(() => io.WriteAsync(location, new byte[] { 1 }));
        await Assert.ThrowsAsync<NotSupportedException>(() => io.ExistsAsync(location));
    }

    [Fact]
    public async Task S3_schemes_go_to_S3_and_local_forms_stay_local()
    {
        var s3Calls = 0;
        using var io = new ResolvingFileIO(() => { s3Calls++; return new LocalFileIO(); });
        var bare = Path.Combine(_dir, "s3data", "a.bin");
        await io.WriteAsync(bare, new byte[] { 1 });
        await io.WriteAsync("file://" + Path.Combine(_dir, "b.bin"), new byte[] { 2 });
        Assert.Equal(0, s3Calls);
        Assert.True(File.Exists(bare));

        foreach (var scheme in new[] { "s3", "s3a", "s3n" })
            await Assert.ThrowsAnyAsync<Exception>(() => io.ExistsAsync($"{scheme}://bucket/k"));
        Assert.Equal(1, s3Calls); // created lazily, once
    }
}
