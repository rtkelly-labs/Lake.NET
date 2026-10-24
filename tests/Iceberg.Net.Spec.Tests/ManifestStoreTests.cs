using Iceberg.Net.Avro;
using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>ManifestStore: the one route manifests take to and from storage, and how it picks a format.</summary>
public sealed class ManifestStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-manifests", Guid.NewGuid().ToString("N"));
    private readonly LocalFileIO _io = new();

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    [Fact]
    public async Task Manifests_round_trip_through_the_store_and_formats_are_picked_by_magic_bytes()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long)]);
        var spec = PartitionSpec.Unpartitioned;
        var file = new DataFile { FilePath = "s3://b/t/data/a.parquet", FileFormat = "PARQUET", Partition = [], RecordCount = 3, FileSizeInBytes = 10 };
        var path = Path.Combine(_dir, "m.avro");
        var length = await ManifestStore.WriteAsync(_io, path, 2, schema, spec, ManifestContent.Data, [new ManifestEntry(ManifestEntryStatus.Added, 7, 1, 1, file)]);
        Assert.Equal(new FileInfo(path).Length, length);
        var read = await ManifestStore.ReadAsync(_io, path, spec, schema);
        Assert.Equal("s3://b/t/data/a.parquet", Assert.Single(read.Entries).DataFile.FilePath);
        // Never overwritten.
        await Assert.ThrowsAsync<FileAlreadyExistsException>(() => ManifestStore.WriteAsync(_io, path, 2, schema, spec, ManifestContent.Data, []));

        Assert.Same(AvroManifestFormat.Instance, ManifestStore.ForReading(await File.ReadAllBytesAsync(path), path));
        Assert.Same(AvroManifestFormat.Instance, ManifestStore.ForWriting(3));
        Assert.Throws<NotSupportedException>(() => ManifestStore.ForWriting(4));
        // A v4 Parquet manifest is named as such; anything else goes to the Avro decoder for its corruption error.
        Assert.Throws<NotSupportedException>(() => ManifestStore.ForReading("PAR1...."u8.ToArray(), "x"));
        var garbage = Path.Combine(_dir, "garbage.avro");
        await _io.WriteAsync(garbage, "not a manifest"u8.ToArray());
        var e = await Assert.ThrowsAnyAsync<Exception>(() => ManifestStore.ReadAsync(_io, garbage, spec, schema));
        Assert.True(Iceberg.Net.Corruption.IsFormatError(e), e.ToString());
    }
}
