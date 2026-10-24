using Microsoft.EntityFrameworkCore.Infrastructure;
using Iceberg.Net.Catalog;
using Iceberg.Net.EntityFrameworkCore.Infrastructure;
using Iceberg.Net.EntityFrameworkCore.Metadata;

namespace Iceberg.Net.EntityFrameworkCore.Storage;

/// <summary>Per-context state: the catalog and namespace from the options, and the snapshot each table was first read at.</summary>
internal sealed class IcebergSession(IDbContextOptions options)
{
    private readonly Dictionary<string, long?> _firstRead = new(StringComparer.Ordinal);

    public IcebergOptionsExtension Options { get; } = options.FindExtension<IcebergOptionsExtension>()
        ?? throw new InvalidOperationException("The context is not configured with UseIceberg");

    public ICatalog Catalog => Options.Catalog!;
    public Namespace Namespace => Options.Namespace!;

    /// <summary>The table, freshly loaded (every query and save sees the latest committed state).</summary>
    public async Task<Table> LoadTableAsync(IcebergEntityMapping mapping, CancellationToken ct)
    {
        try
        {
            return await Catalog.LoadTableAsync(mapping.Identifier(Namespace), ct);
        }
        catch (NoSuchTableException e)
        {
            throw new InvalidOperationException(
                $"Table {mapping.Identifier(Namespace)} for {mapping.EntityType.DisplayName()} does not exist; create it with Database.EnsureCreated() or the catalog", e);
        }
    }

    /// <summary>Remember the snapshot a tracking query read <paramref name="table"/> at (the first one counts).</summary>
    public void NoteRead(Table table)
    {
        lock (_firstRead) _firstRead.TryAdd(table.Identifier.ToString(), table.Metadata.CurrentSnapshotId);
    }

    /// <summary>The snapshot the context first read the table at, if it read it.</summary>
    public bool TryGetFirstRead(Table table, out long? snapshotId)
    {
        lock (_firstRead) return _firstRead.TryGetValue(table.Identifier.ToString(), out snapshotId);
    }

    public void Forget(Table table)
    {
        lock (_firstRead) _firstRead.Remove(table.Identifier.ToString());
    }
}
