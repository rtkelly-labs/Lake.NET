using System.Text;
using Microsoft.Extensions.Logging;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer;

/// <summary>A table as the server returns it: where its current metadata file is, and that file's contents.</summary>
public sealed record LoadedTable(TableIdentifier Id, string? MetadataLocation, TableMetadata Metadata);

/// <summary>A view as the server returns it.</summary>
public sealed record LoadedView(TableIdentifier Id, string MetadataLocation, ViewMetadata Metadata);

/// <summary>What a create-table request asks for (REST <c>CreateTableRequest</c>).</summary>
public sealed record CreateTableSpec(
    string Name,
    Schema Schema,
    PartitionSpec? Spec,
    SortOrder? WriteOrder,
    string? Location,
    IReadOnlyDictionary<string, string> Properties,
    bool StageCreate);

/// <summary>
/// The catalog semantics behind the REST endpoints, independent of HTTP. Commits work as a metastore catalog's do:
/// read the current metadata, check the requirements against it, apply the updates with
/// <see cref="TableMetadataBuilder"/>, write the next metadata file through <see cref="IFileIO"/> (never overwriting),
/// then compare-and-swap the pointer in the <see cref="ICatalogStore"/>. A lost swap is a
/// <see cref="CommitFailedException"/> (HTTP 409) and its file is deleted; any other failure after the write leaves
/// the file in place, because the swap may have happened.
/// </summary>
public sealed partial class CatalogService(ICatalogStore store, IFileIO io, RestServerOptions options)
{
    public ICatalogStore Store { get; } = store;
    public IFileIO FileIO { get; } = io;
    public Func<long> Clock { get; init; } = () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public ILogger? Logger { get; init; }

    // ---------- namespaces ----------

    public async Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string> properties, CancellationToken ct)
    {
        RequireNamespaceLevels(ns);
        if (await Store.LoadNamespaceAsync(ns, ct) is null)
            await RequireNamespaceRootAsync(ns, NamespaceRoot(ns, properties), properties.ContainsKey("location"), ct);
        await Store.CreateNamespaceAsync(ns, properties, ct);
    }

    public async Task<IReadOnlyDictionary<string, string>> LoadNamespaceAsync(Namespace ns, CancellationToken ct) =>
        await Store.LoadNamespaceAsync(ns, ct) ?? throw new NoSuchNamespaceException($"Namespace does not exist: {ns}");

    public async Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct) => await Store.LoadNamespaceAsync(ns, ct) is not null;

    public async Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent, CancellationToken ct)
    {
        if (parent is not null) await LoadNamespaceAsync(parent, ct);
        return (await Store.ListNamespacesAsync(parent, ct)).OrderBy(n => string.Join('\u001f', n.Levels), StringComparer.Ordinal).ToList();
    }

    /// <summary>Returns (updated, removed, missing) keys, as <c>UpdateNamespacePropertiesResponse</c>.</summary>
    public async Task<(IReadOnlyList<string> Updated, IReadOnlyList<string> Removed, IReadOnlyList<string> Missing)> UpdateNamespacePropertiesAsync(
        Namespace ns, IReadOnlyDictionary<string, string> updates, IReadOnlyList<string> removals, CancellationToken ct)
    {
        var both = removals.Where(updates.ContainsKey).ToList();
        if (both.Count > 0) throw new UnprocessableEntityException($"Properties cannot be both set and removed: {string.Join(", ", both)}");
        var current = await LoadNamespaceAsync(ns, ct);
        if (updates.ContainsKey("location") || removals.Contains("location"))
        {
            var after = current.Where(kv => !removals.Contains(kv.Key)).Concat(updates).GroupBy(kv => kv.Key).ToDictionary(g => g.Key, g => g.Last().Value);
            if (await Store.ListAsync(ns, CatalogEntryKind.Table, ct) is { Count: > 0 } || await Store.ListAsync(ns, CatalogEntryKind.View, ct) is { Count: > 0 })
                throw new BadRequestException($"Namespace {ns} has tables or views: its location cannot change");
            await RequireNamespaceRootAsync(ns, NamespaceRoot(ns, after), after.ContainsKey("location"), ct);
        }
        await Store.UpdateNamespacePropertiesAsync(ns, updates, removals, ct);
        return (updates.Keys.ToList(), removals.Where(current.ContainsKey).ToList(), removals.Where(k => !current.ContainsKey(k)).ToList());
    }

    public async Task DropNamespaceAsync(Namespace ns, CancellationToken ct)
    {
        await LoadNamespaceAsync(ns, ct);
        if ((await Store.ListAsync(ns, CatalogEntryKind.Table, ct)).Count > 0 || (await Store.ListAsync(ns, CatalogEntryKind.View, ct)).Count > 0)
            throw new NamespaceNotEmptyException($"Namespace is not empty: {ns} (it has tables or views)");
        if ((await Store.ListNamespacesAsync(ns, ct)).Count > 0)
            throw new NamespaceNotEmptyException($"Namespace is not empty: {ns} (it has child namespaces)");
        await Store.DropNamespaceAsync(ns, ct);
    }

    // ---------- tables ----------

    public async Task<IReadOnlyList<string>> ListAsync(Namespace ns, CatalogEntryKind kind, CancellationToken ct)
    {
        await LoadNamespaceAsync(ns, ct);
        return (await Store.ListAsync(ns, kind, ct)).Order(StringComparer.Ordinal).ToList();
    }

    public async Task<LoadedTable> LoadTableAsync(TableIdentifier id, CancellationToken ct)
    {
        var location = await Store.GetAsync(id, CatalogEntryKind.Table, ct) ?? throw await NotFoundAsync(id, "Table", ct);
        return new LoadedTable(id, location, await ReadMetadataAsync(location, ct));
    }

    public async Task<bool> TableExistsAsync(TableIdentifier id, CancellationToken ct) => await Store.GetAsync(id, CatalogEntryKind.Table, ct) is not null;

    /// <summary>NoSuchNamespace when the namespace is missing (Java answers the same), else NoSuchTable/NoSuchView.</summary>
    private async Task<Exception> NotFoundAsync(TableIdentifier id, string kind, CancellationToken ct) =>
        !await NamespaceExistsAsync(id.Namespace, ct)
            ? new NoSuchNamespaceException($"Namespace does not exist: {id.Namespace}")
            : kind == "View" ? new NoSuchViewException($"View does not exist: {id}") : new NoSuchTableException($"Table does not exist: {id}");

    /// <summary>The location a new table or view gets: under the namespace's <c>location</c> property, else under the warehouse.</summary>
    public async Task<string> DefaultLocationAsync(TableIdentifier id, CancellationToken ct)
    {
        var props = await LoadNamespaceAsync(id.Namespace, ct);
        if (props.TryGetValue("location", out var nsLocation) && nsLocation.Length > 0) return $"{nsLocation.TrimEnd('/')}/{id.Name}";
        if (string.IsNullOrEmpty(options.Warehouse)) throw new BadRequestException("The server has no warehouse configured: give the table a location");
        return $"{options.Warehouse.TrimEnd('/')}/{string.Join('/', id.Namespace.Levels)}/{id.Name}";
    }

    public async Task<LoadedTable> CreateTableAsync(Namespace ns, CreateTableSpec request, CancellationToken ct)
    {
        var id = new TableIdentifier(ns, request.Name);
        RequireEntryName(request.Name, "Table");
        await LoadNamespaceAsync(ns, ct);
        if (await Store.KindAsync(id, ct) is { } existing) throw new AlreadyExistsException($"{(existing == CatalogEntryKind.View ? "View" : "Table")} already exists: {id}");

        var props = new Dictionary<string, string>(request.Properties);
        var formatVersion = options.DefaultFormatVersion;
        if (props.Remove("format-version", out var fv))
            formatVersion = int.TryParse(fv, out var v) ? v : throw new BadRequestException($"Invalid format-version '{fv}'");
        var location = request.Location ?? await DefaultLocationAsync(id, ct);
        TableMetadata fresh;
        try
        {
            // Fresh field, partition-field and order IDs, as Java's server assigns them (the request's IDs are placeholders).
            fresh = TableMetadata.NewTable(request.Schema, request.Spec, request.WriteOrder, location, props, formatVersion, Clock);
        }
        catch (Exception e) when (e is ArgumentException or KeyNotFoundException or InvalidOperationException)
        {
            throw new BadRequestException(e.Message);
        }
        await RequireTableLocationsAsync(id, fresh, ct);
        if (request.StageCreate) return new LoadedTable(id, null, fresh); // nothing is written until the create commits

        var metadataLocation = MetastoreCatalog.NewMetadataLocation(fresh, null);
        var metadata = fresh with { MetadataFileLocation = metadataLocation };
        await WriteMetadataAsync(metadataLocation, metadata, ct);
        await SwapOrCleanUpAsync([new PointerSwap(id, null, metadataLocation)], CatalogEntryKind.Table, [metadataLocation], ct, onTaken: () => new AlreadyExistsException($"Table already exists: {id}"));
        return new LoadedTable(id, metadataLocation, metadata);
    }

    public async Task<LoadedTable> RegisterTableAsync(Namespace ns, string name, string metadataLocation, bool overwrite, CancellationToken ct)
    {
        var id = new TableIdentifier(ns, name);
        RequireEntryName(name, "Table");
        var nsProperties = await LoadNamespaceAsync(ns, ct);
        var current = await Store.GetAsync(id, CatalogEntryKind.Table, ct);
        if (await Store.KindAsync(id, ct) == CatalogEntryKind.View || current is not null && !overwrite)
            throw new AlreadyExistsException($"Table already exists: {id}");
        var nsRoot = NamespaceRoot(ns, nsProperties) ?? throw new BadRequestException("The server has no warehouse configured and the namespace has no location");
        // Checked before anything is read: the server reads with its own access.
        RequireAllowed(metadataLocation, $"Table {id} metadata location");
        if (!Root(metadataLocation).StartsWith(Root(nsRoot), StringComparison.Ordinal))
            throw new BadRequestException($"Table {id} metadata location {metadataLocation} is outside its namespace's location {nsRoot}");
        TableMetadata metadata;
        try
        {
            metadata = await ReadMetadataAsync(metadataLocation, ct);
        }
        catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException or InvalidDataException or System.Text.Json.JsonException)
        {
            throw new BadRequestException($"Cannot read table metadata at {metadataLocation} ({e.GetType().Name})");
        }
        await RequireTableLocationsAsync(id, metadata, ct);
        RequireOwnFiles(id, null, metadata);
        await Store.SwapAsync([new PointerSwap(id, current, metadataLocation)], CatalogEntryKind.Table, ct);
        return new LoadedTable(id, metadataLocation, metadata);
    }

    public async Task<LoadedTable> UnregisterTableAsync(TableIdentifier id, CancellationToken ct)
    {
        var table = await LoadTableAsync(id, ct);
        if (!await Store.DeleteAsync(id, CatalogEntryKind.Table, ct)) throw new NoSuchTableException($"Table does not exist: {id}");
        return table;
    }

    public async Task DropTableAsync(TableIdentifier id, bool purge, CancellationToken ct)
    {
        var table = await LoadTableAsync(id, ct);
        if (!await Store.DeleteAsync(id, CatalogEntryKind.Table, ct)) throw new NoSuchTableException($"Table does not exist: {id}");
        if (!purge) return;
        // Entry first, files second: a crash in between leaves orphans, never a table pointing at missing files.
        var m = table.Metadata;
        try
        {
            // Only the table's own directories, and data files only when the table owns them (gc.enabled), as the catalogs do.
            var files = await TableFiles.ReachableAsync(m, FileIO, m.Snapshots, ct, dataFiles: TableLocations.IsGcEnabled(m));
            files.UnionWith(m.MetadataLog.Select(l => l.MetadataFile).Append(table.MetadataLocation!));
            var (inside, outside) = TableLocations.Partition(m, files);
            if (outside.Count > 0) Logger?.LogWarning("Purging {Table}: skipped {Count} files outside its location", id, outside.Count);
            await TableFiles.DeleteAllAsync(FileIO, inside, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // The table is dropped; what could not be found or deleted is left to orphan-file cleanup (as Java does).
            Logger?.LogWarning(e, "Dropped {Table}, but purging its files failed", id);
        }
    }

    public async Task RenameAsync(TableIdentifier from, TableIdentifier to, CatalogEntryKind kind, CancellationToken ct)
    {
        RequireNamespaceLevels(to.Namespace);
        RequireEntryName(to.Name, kind == CatalogEntryKind.View ? "View" : "Table");
        if (await Store.GetAsync(from, kind, ct) is null) throw await NotFoundAsync(from, kind.ToString(), ct);
        await Store.RenameAsync(from, to, kind, ct);
    }

    /// <summary>Commit one table (REST <c>updateTable</c>); also commits a staged create (<c>assert-create</c>).</summary>
    public async Task<LoadedTable> CommitTableAsync(TableCommit commit, CancellationToken ct) => (await CommitAsync([commit], ct))[0];

    /// <summary>
    /// Commit several tables at once (REST <c>commitTransaction</c>): every requirement is checked and every metadata file
    /// written first, then all pointers move in one <see cref="ICatalogStore.SwapAsync"/>, or none do.
    /// </summary>
    public async Task<IReadOnlyList<LoadedTable>> CommitAsync(IReadOnlyList<TableCommit> commits, CancellationToken ct)
    {
        if (commits.Count == 0) throw new BadRequestException("No table changes to commit");
        if (commits.Select(c => c.Identifier).Distinct().Count() != commits.Count) throw new BadRequestException("A transaction may change each table once");
        var results = new List<LoadedTable>();
        var swaps = new List<PointerSwap>();
        var written = new List<string>();
        var resulting = new List<(TableIdentifier, TableMetadata, bool)>();
        try
        {
            foreach (var commit in commits)
            {
                var id = commit.Identifier;
                var currentLocation = await Store.GetAsync(id, CatalogEntryKind.Table, ct);
                var current = currentLocation is null ? null : await ReadMetadataAsync(currentLocation, ct);
                // Only a staged create (assert-create) may commit to a table that does not exist: anything else is a 404,
                // not a failed requirement.
                if (current is null && !commit.Requirements.Any(r => r is UpdateRequirement.AssertCreate)) throw await NotFoundAsync(id, "Table", ct);
                foreach (var requirement in commit.Requirements) requirement.Validate(current);

                TableMetadata updated;
                bool changed;
                if (current is null)
                {
                    // A staged create through the commit endpoint names the table as the create endpoint does.
                    RequireEntryName(id.Name, "Table");
                    await LoadNamespaceAsync(id.Namespace, ct);
                    if (await Store.KindAsync(id, ct) is not null) throw new CommitFailedException($"Requirement failed: {id} already exists");
                    updated = await BuildCreatedAsync(id, commit.Updates, ct);
                    await RequireTableLocationsAsync(id, updated, ct);
                    changed = true;
                    resulting.Add((id, updated, true));
                }
                else
                {
                    var builder = Apply(() => TableMetadataBuilder.From(current, Clock).ApplyAll(commit.Updates));
                    changed = builder.Changes.Count > 0;
                    updated = changed ? Apply(() => builder.Build(MetastoreCatalog.NewMetadataLocation(builder.Build(""), currentLocation))) : current;
                    var moved = changed && !TableDirectories(updated).SequenceEqual(TableDirectories(current));
                    if (moved) await RequireTableLocationsAsync(id, updated, ct);
                    resulting.Add((id, updated, moved));
                }

                if (!changed)
                {
                    results.Add(new LoadedTable(id, currentLocation, current!));
                    continue;
                }
                RequireOwnFiles(id, current, updated);
                var newLocation = updated.MetadataFileLocation ?? MetastoreCatalog.NewMetadataLocation(updated, currentLocation);
                updated = updated with { MetadataFileLocation = newLocation };
                await WriteMetadataAsync(newLocation, updated, ct);
                written.Add(newLocation);
                swaps.Add(new PointerSwap(id, currentLocation, newLocation));
                results.Add(new LoadedTable(id, newLocation, updated));
            }
            RequireDisjointInTransaction(resulting);
        }
        catch
        {
            await DeleteQuietlyAsync(written, ct); // nothing was swapped: the new files are unreferenced
            throw;
        }
        if (swaps.Count > 0) await SwapOrCleanUpAsync(swaps, CatalogEntryKind.Table, written, ct);
        return results;
    }

    /// <summary>
    /// The metadata of a staged create: fresh metadata from the create-shaped updates (schema, spec, order, location,
    /// format version, UUID), then every update applied on top (re-applying the create's own ones changes nothing).
    /// </summary>
    private async Task<TableMetadata> BuildCreatedAsync(TableIdentifier id, IReadOnlyList<MetadataUpdate> updates, CancellationToken ct)
    {
        var schema = updates.OfType<MetadataUpdate.AddSchema>().FirstOrDefault()?.Schema ?? throw new BadRequestException("A create must add a schema");
        var formatVersion = updates.OfType<MetadataUpdate.UpgradeFormatVersion>().LastOrDefault()?.FormatVersion ?? options.DefaultFormatVersion;
        var location = updates.OfType<MetadataUpdate.SetLocation>().LastOrDefault()?.Location ?? await DefaultLocationAsync(id, ct);
        return Apply(() =>
        {
            var fresh = TableMetadata.NewTable(schema, updates.OfType<MetadataUpdate.AddPartitionSpec>().FirstOrDefault()?.Spec,
                updates.OfType<MetadataUpdate.AddSortOrder>().FirstOrDefault()?.SortOrder, location, null, formatVersion, Clock, preserveFieldIds: true);
            if (updates.OfType<MetadataUpdate.AssignUuid>().FirstOrDefault() is { } uuid) fresh = fresh with { TableUuid = uuid.Uuid };
            fresh = fresh with { Schemas = [schema], CurrentSchemaId = schema.SchemaId, LastColumnId = Math.Max(fresh.LastColumnId, schema.HighestFieldId) };
            var built = TableMetadataBuilder.From(fresh, Clock).ApplyAll(updates).Build("");
            return built with { MetadataFileLocation = MetastoreCatalog.NewMetadataLocation(built, null) };
        });
    }

    /// <summary>Metadata the client's updates cannot produce (unknown IDs, downgrades, invalid snapshots) is a 400.</summary>
    private static T Apply<T>(Func<T> f)
    {
        try
        {
            return f();
        }
        catch (Exception e) when (e is ArgumentException or KeyNotFoundException or InvalidOperationException or NotSupportedException)
        {
            throw new BadRequestException(e.Message);
        }
    }

    private async Task SwapOrCleanUpAsync(IReadOnlyList<PointerSwap> swaps, CatalogEntryKind kind, IReadOnlyList<string> written, CancellationToken ct, Func<Exception>? onTaken = null)
    {
        try
        {
            await Store.SwapAsync(swaps, kind, ct);
        }
        catch (Exception e) when (e is CommitFailedException or NoSuchNamespaceException)
        {
            // Definitely lost: nothing points at the new files.
            await DeleteQuietlyAsync(written, ct);
            if (e is CommitFailedException && onTaken is not null) throw onTaken();
            throw;
        }
        // Any other failure: the store may have applied the swap, so the files stay (the client sees a 500: unknown).
    }

    private async Task DeleteQuietlyAsync(IEnumerable<string> files, CancellationToken ct)
    {
        foreach (var f in files)
        {
            try { await FileIO.DeleteAsync(f, ct); }
            catch (Exception e) when (e is not OperationCanceledException) { /* orphan cleanup will find it */ }
        }
    }

    private Task WriteMetadataAsync(string location, TableMetadata m, CancellationToken ct) =>
        FileIO.WriteAsync(location, Encoding.UTF8.GetBytes(MetadataJson.Write(m)), overwrite: false, ct);

    public async Task<TableMetadata> ReadMetadataAsync(string location, CancellationToken ct) =>
        MetastoreCatalog.DecodeMetadata(await FileIO.ReadAllAsync(location, ct), location);

    // ---------- views ----------

    public async Task<LoadedView> LoadViewAsync(TableIdentifier id, CancellationToken ct)
    {
        var location = await Store.GetAsync(id, CatalogEntryKind.View, ct) ?? throw await NotFoundAsync(id, "View", ct);
        return new LoadedView(id, location, ViewJson.Read(Encoding.UTF8.GetString(await FileIO.ReadAllAsync(location, ct)), location));
    }

    public async Task<bool> ViewExistsAsync(TableIdentifier id, CancellationToken ct) => await Store.GetAsync(id, CatalogEntryKind.View, ct) is not null;

    public async Task<LoadedView> CreateViewAsync(TableIdentifier id, string? location, Schema schema, ViewVersion version, IReadOnlyDictionary<string, string> properties, CancellationToken ct)
    {
        await LoadNamespaceAsync(id.Namespace, ct);
        if (await Store.KindAsync(id, ct) is { } existing) throw new AlreadyExistsException($"{(existing == CatalogEntryKind.View ? "View" : "Table")} already exists: {id}");
        if (version.Representations.Count == 0) throw new BadRequestException("A view version needs at least one representation");
        RequireEntryName(id.Name, "View");
        var metadata = ViewMetadata.Create(location ?? await DefaultLocationAsync(id, ct), schema, version, properties, Clock());
        await RequireEntryLocationsAsync(id, CatalogEntryKind.View, [metadata.Location], ct);
        return await WriteViewAsync(id, null, metadata, ct, onTaken: () => new AlreadyExistsException($"View already exists: {id}"));
    }

    public async Task<LoadedView> ReplaceViewAsync(TableIdentifier id, IReadOnlyList<ViewRequirement> requirements, IReadOnlyList<ViewUpdate> updates, CancellationToken ct)
    {
        var current = await LoadViewAsync(id, ct);
        foreach (var r in requirements) r.Validate(current.Metadata);
        var updated = Apply(() => ViewUpdate.ApplyAll(current.Metadata, updates, Clock()));
        if (updated.Location != current.Metadata.Location) await RequireEntryLocationsAsync(id, CatalogEntryKind.View, [updated.Location], ct);
        return await WriteViewAsync(id, current.MetadataLocation, updated, ct);
    }

    public async Task DropViewAsync(TableIdentifier id, CancellationToken ct)
    {
        if (!await Store.DeleteAsync(id, CatalogEntryKind.View, ct)) throw await NotFoundAsync(id, "View", ct);
    }

    private async Task<LoadedView> WriteViewAsync(TableIdentifier id, string? expected, ViewMetadata metadata, CancellationToken ct, Func<Exception>? onTaken = null)
    {
        var version = expected is not null && int.TryParse(Path.GetFileName(expected).Split('-')[0], out var v) ? v + 1 : 0;
        var location = $"{metadata.Location}/metadata/{version:D5}-{Guid.NewGuid()}.metadata.json";
        await FileIO.WriteAsync(location, Encoding.UTF8.GetBytes(ViewJson.Write(metadata)), overwrite: false, ct);
        await SwapOrCleanUpAsync([new PointerSwap(id, expected, location)], CatalogEntryKind.View, [location], ct, onTaken);
        return new LoadedView(id, location, metadata with { MetadataFileLocation = location });
    }
}
