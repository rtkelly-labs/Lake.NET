using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Iceberg.Net.Catalog;
using Iceberg.Net.EntityFrameworkCore.Metadata;
using Iceberg.Net.Linq;
using Iceberg.Net.Linq.Query;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.EntityFrameworkCore.Storage;

/// <summary>
/// <c>EnsureCreated</c> creates the namespace and every mapped table that does not exist yet (schema, field IDs,
/// identifier fields and partitioning from the model); existing tables are left as they are. <c>EnsureDeleted</c>
/// drops the model's tables with their data (purge) and leaves the namespace.
/// </summary>
internal sealed class IcebergDatabaseCreator(ICurrentDbContext current, IcebergSession session) : IDatabaseCreator
{
    private IEnumerable<IcebergEntityMapping> Mappings => current.Context.Model.GetEntityTypes().Select(IcebergEntityMapping.For);

    public bool EnsureCreated() => Blocking.Run(() => EnsureCreatedAsync());

    public async Task<bool> EnsureCreatedAsync(CancellationToken ct = default)
    {
        var created = false;
        var catalog = session.Catalog;
        if (!await catalog.NamespaceExistsAsync(session.Namespace, ct))
        {
            await catalog.CreateNamespaceAsync(session.Namespace, ct: ct);
            created = true;
        }
        foreach (var mapping in Mappings)
        {
            var id = mapping.Identifier(session.Namespace);
            if (await catalog.TableExistsAsync(id, ct)) continue;
            try
            {
                await catalog.CreateTableAsync(id, mapping.Schema, mapping.EntityType.DisplayName(), mapping.Spec, session.Options.TableProperties,
                    session.Options.FormatVersion ?? TableMetadata.DefaultFormatVersion, ct);
                created = true;
            }
            catch (AlreadyExistsException)
            {
                // created concurrently: fine
            }
        }
        return created;
    }

    public bool EnsureDeleted() => Blocking.Run(() => EnsureDeletedAsync());

    public async Task<bool> EnsureDeletedAsync(CancellationToken ct = default)
    {
        var deleted = false;
        if (!await session.Catalog.NamespaceExistsAsync(session.Namespace, ct)) return false;
        foreach (var mapping in Mappings)
        {
            var id = mapping.Identifier(session.Namespace);
            if (!await session.Catalog.TableExistsAsync(id, ct)) continue;
            await session.Catalog.DropTableAsync(id, purge: true, ct);
            deleted = true;
        }
        return deleted;
    }

    public bool CanConnect() => Blocking.Run(() => CanConnectAsync());

    public async Task<bool> CanConnectAsync(CancellationToken ct = default)
    {
        try
        {
            await session.Catalog.ListNamespacesAsync(null, ct);
            return true;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return false;
        }
    }
}
