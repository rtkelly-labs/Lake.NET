using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.TestSupport;

/// <summary>Lose a response after an optional commit and a deterministic intervening maintenance schedule.</summary>
public sealed class UncertainCommitCatalog : InMemoryCatalog
{
    private readonly string _warehouse;
    public UncertainCommitCatalog(string warehouse) : base(warehouse) => _warehouse = warehouse;
    public bool Armed { get; set; }
    public bool ApplyBeforeError { get; set; } = true;
    public bool ServerResponded { get; set; } = true;
    public Exception? Failure { get; set; }
    public Func<IReadOnlyList<TableIdentifier>, Task>? BeforeError { get; set; }
    public List<string> UncertainFiles { get; } = [];
    public int ReadFailuresAfterError { get; set; }
    public int RecoveryReads { get; private set; }
    private int _remainingFailures;
    private bool _checking;

    protected override Task<string?> CurrentMetadataLocationAsync(TableIdentifier id, CancellationToken ct)
    {
        if (_checking)
        {
            RecoveryReads++;
            if (_remainingFailures-- > 0) throw new IOException("Injected status-check read failure");
        }
        return base.CurrentMetadataLocationAsync(id, ct);
    }

    private async Task FailAsync(IReadOnlyList<TableIdentifier> ids)
    {
        UncertainFiles.AddRange(Directory.GetFiles(_warehouse, "*", SearchOption.AllDirectories));
        if (BeforeError is { } intervene) await intervene(ids);
        _remainingFailures = ReadFailuresAfterError;
        _checking = true;
        throw Failure ?? new CommitStateUnknownException("Injected lost commit response") { ServerResponded = ServerResponded };
    }

    protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
    {
        if (!Armed) { await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct); return; }
        Armed = false;
        if (ApplyBeforeError) await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
        await FailAsync([id]);
    }

    protected override async Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct)
    {
        if (!Armed) { await base.SwapMetadataLocationsAsync(swaps, ct); return; }
        Armed = false;
        if (ApplyBeforeError) await base.SwapMetadataLocationsAsync(swaps, ct);
        await FailAsync(swaps.Select(swap => swap.Id).ToList());
    }
}
