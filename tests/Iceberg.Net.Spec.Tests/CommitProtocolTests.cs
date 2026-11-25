using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Failure handling in the metastore commit protocol (write new metadata file, then CAS the pointer).</summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("Metastore Tables")]
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

/// <summary>Resolving unknown commit outcomes when the catalog did answer (HTTP 5xx, service errors).</summary>
public sealed class CommitResolutionTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));

    private sealed class UnknownOnceCatalog(string warehouse, bool apply, bool serverResponded) : InMemoryCatalog(warehouse)
    {
        public bool Armed { get; set; }

        protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
        {
            if (!Armed) { await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct); return; }
            Armed = false;
            if (apply) await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
            throw new CommitStateUnknownException("HTTP 500 from the catalog") { ServerResponded = serverResponded };
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static async Task<Table> Create(InMemoryCatalog c)
    {
        await c.CreateNamespaceAsync(Namespace.Of("db"));
        var t = await c.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), LocalTableTests.Schema));
        await TableMutations.InsertAsync(t, LocalTableTests.Rows(0, 3));
        return t;
    }

    [Fact]
    public async Task Positive_snapshot_evidence_resolves_a_service_error()
    {
        using var catalog = new UnknownOnceCatalog(_warehouse, apply: true, serverResponded: true);
        var t = await Create(catalog);
        catalog.Armed = true;
        var result = await TableMutations.InsertAsync(t, LocalTableTests.Rows(10, 2));

        var rows = await TableScan.ReadAllAsync(await catalog.LoadTableAsync(t.Identifier));
        Assert.Equal(5, rows.Count);                         // exactly once either way
        Assert.Equal(1, result.Attempts);
        await TableValidator.AssertValidAsync(await catalog.LoadTableAsync(t.Identifier), new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    public async Task Snapshot_absence_does_not_prove_failure_even_when_the_server_answered()
    {
        using var catalog = new UnknownOnceCatalog(_warehouse, apply: false, serverResponded: true);
        var table = await Create(catalog);
        catalog.Armed = true;
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => TableMutations.InsertAsync(table, LocalTableTests.Rows(10, 2)));
        Assert.Equal(3, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(table.Identifier))).Count);
    }

    [Fact]
    public async Task Timeouts_stay_unknown_because_the_request_may_still_apply()
    {
        using var catalog = new UnknownOnceCatalog(_warehouse, apply: false, serverResponded: false);
        var t = await Create(catalog);
        catalog.Armed = true;
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => TableMutations.InsertAsync(t, LocalTableTests.Rows(10, 2)));
    }
}
