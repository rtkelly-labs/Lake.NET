using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Failure handling in the metastore commit protocol (write new metadata file, then CAS the pointer).</summary>
public sealed class CommitProtocolTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));

    /// <summary>Fails the pointer swap on demand, either as a lost race or as an ambiguous error (e.g. a timeout).</summary>
    private sealed class FaultyCatalog(string warehouse) : InMemoryCatalog(warehouse)
    {
        public Exception? NextSwapFailure { get; set; }
        public bool ApplyBeforeFailing { get; set; }

        protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
        {
            if (NextSwapFailure is { } failure)
            {
                NextSwapFailure = null;
                if (ApplyBeforeFailing) await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
                throw failure;
            }
            await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static int MetadataFiles(Table t) => Directory.GetFiles(Path.Combine(t.Location, "metadata"), "*.metadata.json").Length;

    [Fact]
    public async Task Lost_race_deletes_the_orphaned_metadata_file()
    {
        using var catalog = new FaultyCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), LocalTableTests.Schema));
        catalog.NextSwapFailure = new CommitFailedException("lost the race");
        await Assert.ThrowsAsync<CommitFailedException>(() => t.SetPropertiesAsync(new Dictionary<string, string> { ["a"] = "b" }));
        Assert.Equal(1, MetadataFiles(t));
    }

    [Fact]
    public async Task Unknown_outcome_keeps_the_metadata_file_even_if_the_commit_landed()
    {
        using var catalog = new FaultyCatalog(_warehouse) { ApplyBeforeFailing = true };
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), LocalTableTests.Schema));
        catalog.NextSwapFailure = new TimeoutException("socket timeout after the server applied the update");
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => t.SetPropertiesAsync(new Dictionary<string, string> { ["a"] = "b" }));

        // The commit did land: deleting the new file would have corrupted the table.
        var reloaded = await catalog.LoadTableAsync(t.Identifier);
        Assert.Equal("b", reloaded.Metadata.Properties["a"]);
        Assert.Equal(2, MetadataFiles(t));
    }

    [Fact]
    public async Task Snapshot_producer_does_not_retry_unknown_outcomes()
    {
        using var catalog = new FaultyCatalog(_warehouse);
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), LocalTableTests.Schema));
        var files = await TableWriter.WriteAsync(t, LocalTableTests.Rows(0, 3));
        catalog.NextSwapFailure = new IOException("connection reset");
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => SnapshotProducer.Append(t).AddFiles(files).CommitAsync());
        // Data and manifest files are left in place for the caller to reconcile.
        Assert.All(files, f => Assert.True(File.Exists(f.FilePath)));
    }
}
