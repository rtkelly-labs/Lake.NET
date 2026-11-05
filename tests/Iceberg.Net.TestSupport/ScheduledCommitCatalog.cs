using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.TestSupport;

/// <summary>Schedule a competing commit or definite CAS failure at the catalog boundary.</summary>
public sealed class ScheduledCommitCatalog(string warehouse) : InMemoryCatalog(warehouse)
{
    public Func<Task>? BeforeNextSwap { get; set; }
    public bool RejectSwaps { get; set; }
    public int SwapAttempts { get; set; }

    private async Task BeforeAsync()
    {
        if (BeforeNextSwap is { } schedule) { BeforeNextSwap = null; await schedule(); }
        SwapAttempts++;
        if (RejectSwaps) throw new CommitFailedException("Injected definite CAS failure");
    }

    protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
    {
        await BeforeAsync();
        await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
    }

    protected override async Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct)
    {
        await BeforeAsync();
        await base.SwapMetadataLocationsAsync(swaps, ct);
    }
}
