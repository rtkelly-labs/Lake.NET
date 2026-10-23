using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Iceberg.Net.IO;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Catalog;

public sealed record Namespace(IReadOnlyList<string> Levels)
{
    public static Namespace Of(params string[] levels) => new(levels);
    public override string ToString() => string.Join('.', Levels);
    public bool Equals(Namespace? other) => other is not null && Levels.SequenceEqual(other.Levels);
    public override int GetHashCode() => Levels.Aggregate(0, (h, l) => HashCode.Combine(h, l));
}

public sealed record TableIdentifier(Namespace Namespace, string Name)
{
    public static TableIdentifier Of(string ns, string name) => new(Namespace.Of(ns.Split('.')), name);
    public override string ToString() => $"{Namespace}.{Name}";
}

public class NoSuchTableException(string message) : Exception(message);
public class NoSuchNamespaceException(string message) : Exception(message);
public class AlreadyExistsException(string message) : Exception(message);

/// <summary>A commit request: preconditions plus changes, the same shape as REST <c>CommitTableRequest</c>.</summary>
public sealed record TableCommit(TableIdentifier Identifier, IReadOnlyList<UpdateRequirement> Requirements, IReadOnlyList<MetadataUpdate> Updates);

public sealed record CreateTableRequest(
    TableIdentifier Identifier,
    Schema Schema,
    PartitionSpec? Spec = null,
    SortOrder? SortOrder = null,
    string? Location = null,
    IReadOnlyDictionary<string, string>? Properties = null,
    int FormatVersion = TableMetadata.DefaultFormatVersion);

public interface ICatalog : IDisposable
{
    string Name { get; }
    IFileIO FileIO { get; }

    Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default);
    Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default);
    Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default);
    Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default);

    Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default);
    Task<Table> CreateTableAsync(CreateTableRequest request, CancellationToken ct = default);
    Task<Table> LoadTableAsync(TableIdentifier id, CancellationToken ct = default);
    Task<bool> TableExistsAsync(TableIdentifier id, CancellationToken ct = default);
    Task<Table> RegisterTableAsync(TableIdentifier id, string metadataLocation, CancellationToken ct = default);
    Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default);
    /// <summary>
    /// Drop the catalog entry. With <paramref name="purge"/>, also delete every file the table references: metadata
    /// files, manifest lists, manifests, data, delete and statistics files (as Java's <c>dropTableData</c>).
    /// </summary>
    Task DropTableAsync(TableIdentifier id, bool purge = false, CancellationToken ct = default);

    /// <summary>
    /// Atomically validate <see cref="TableCommit.Requirements"/> against current metadata and apply
    /// <see cref="TableCommit.Updates"/>. Throws <see cref="CommitFailedException"/> on conflict (safe to
    /// refresh and retry) or <see cref="CommitStateUnknownException"/> when the outcome is unknown.
    /// </summary>
    Task<TableMetadata> CommitTableAsync(TableCommit commit, CancellationToken ct = default);

    /// <summary>
    /// Commit changes to several tables atomically (REST <c>commitTransaction</c>): every table's requirements are
    /// checked and either all changes apply or none do (<see cref="CommitFailedException"/>). Catalogs without
    /// multi-table atomicity (Glue) throw <see cref="NotSupportedException"/>.
    /// </summary>
    Task CommitTransactionAsync(IReadOnlyList<TableCommit> commits, CancellationToken ct = default);
}

/// <summary>A loaded table: identity, current metadata, and the catalog/FileIO to change it.</summary>
public sealed class Table(ICatalog catalog, TableIdentifier identifier, TableMetadata metadata, IFileIO io)
{
    public ICatalog Catalog { get; } = catalog;
    public TableIdentifier Identifier { get; } = identifier;
    public TableMetadata Metadata { get; private set; } = metadata;
    public IFileIO IO { get; } = io;

    public Schema Schema => Metadata.CurrentSchema;
    public PartitionSpec Spec => Metadata.DefaultSpec;
    public string Location => Metadata.Location;
    public string? MetadataLocation => Metadata.MetadataFileLocation;

    public async Task<Table> RefreshAsync(CancellationToken ct = default)
    {
        Metadata = (await Catalog.LoadTableAsync(Identifier, ct)).Metadata;
        return this;
    }

    public async Task<TableMetadata> CommitAsync(IReadOnlyList<UpdateRequirement> requirements, IReadOnlyList<MetadataUpdate> updates, CancellationToken ct = default)
    {
        Metadata = await Catalog.CommitTableAsync(new TableCommit(Identifier, requirements, updates), ct);
        return Metadata;
    }

    public Task<TableMetadata> SetPropertiesAsync(IReadOnlyDictionary<string, string> props, CancellationToken ct = default) =>
        CommitAsync([new UpdateRequirement.AssertTableUuid(Metadata.TableUuid)], [new MetadataUpdate.SetProperties(props)], ct);

    /// <summary>Make <paramref name="order"/> the default sort order (reusing an identical existing one); writers then sort by it.</summary>
    public Task<TableMetadata> ReplaceSortOrderAsync(SortOrder order, CancellationToken ct = default) =>
        CommitAsync(
            [new UpdateRequirement.AssertDefaultSortOrderId(Metadata.DefaultSortOrderId)],
            // Numbered with the next free order ID, as Java's client sends it (an identical existing order is reused).
            [new MetadataUpdate.AddSortOrder(order.IsUnsorted ? order : new SortOrder(Metadata.SortOrders.Max(o => o.OrderId) + 1, order.Fields)),
             new MetadataUpdate.SetDefaultSortOrder(MetadataUpdate.LastAdded)],
            ct);

    /// <summary>Add (or switch to an existing equivalent) schema; asserts the current schema has not moved.</summary>
    public Task<TableMetadata> UpdateSchemaAsync(Schema newSchema, CancellationToken ct = default) =>
        CommitAsync(
            [new UpdateRequirement.AssertCurrentSchemaId(Metadata.CurrentSchemaId), new UpdateRequirement.AssertLastAssignedFieldId(Metadata.LastColumnId)],
            [new MetadataUpdate.AddSchema(FreshSchemaId(newSchema)), new MetadataUpdate.SetCurrentSchema(MetadataUpdate.LastAdded)],
            ct);

    // A schema whose ID is already taken is sent with the next free ID, as Java's client does: some catalogs (Nessie)
    // reject an add-schema that reuses an ID rather than renumbering it.
    private Schema FreshSchemaId(Schema schema) =>
        Metadata.Schemas.Any(s => s.SchemaId == schema.SchemaId)
            ? new Schema(Metadata.Schemas.Max(s => s.SchemaId) + 1, schema.Columns, schema.IdentifierFieldIds)
            : schema;

    /// <summary>
    /// Evolve the partition spec, e.g. <c>table.UpdateSpecAsync(p => p.AddField("event_ts", Transform.Day).RemoveField("region"))</c>.
    /// New data is written under the new spec; existing files keep theirs. Re-applied on a concurrent change, as
    /// <see cref="UpdateSchemaAsync(Action{SchemaUpdate}, int, CancellationToken)"/>.
    /// </summary>
    public async Task<TableMetadata> UpdateSpecAsync(Action<PartitionSpecUpdate> changes, int retries = 4, CancellationToken ct = default)
    {
        for (var attempt = 0; ; attempt++)
        {
            var update = new PartitionSpecUpdate(Metadata);
            changes(update);
            var (requirements, updates) = update.ToCommit();
            try
            {
                return await CommitAsync(requirements, updates, ct);
            }
            catch (CommitFailedException) when (attempt < retries)
            {
                await RefreshAsync(ct);
            }
        }
    }

    /// <summary>
    /// Evolve the schema with <see cref="SchemaUpdate"/> operations, e.g.
    /// <c>table.UpdateSchemaAsync(s => s.AddColumn("region", PrimitiveType.String).RenameColumn("qty", "quantity"))</c>.
    /// If another writer changes the schema first, the operations are re-applied to the new schema (up to
    /// <paramref name="retries"/> times); an operation that no longer makes sense then fails with its own error.
    /// </summary>
    public async Task<TableMetadata> UpdateSchemaAsync(Action<SchemaUpdate> changes, int retries = 4, CancellationToken ct = default)
    {
        for (var attempt = 0; ; attempt++)
        {
            var update = new SchemaUpdate(Metadata);
            changes(update);
            var (requirements, updates) = update.ToCommit();
            try
            {
                return await CommitAsync(requirements, updates, ct);
            }
            catch (CommitFailedException) when (attempt < retries)
            {
                await RefreshAsync(ct);
            }
        }
    }
}

/// <summary>
/// Base for catalogs that store a pointer to the current metadata file (Glue, Hive, JDBC, in-memory).
/// The client does all metadata work; the catalog only provides an atomic compare-and-swap of the pointer.
/// </summary>
public abstract partial class MetastoreCatalog(string name, IFileIO io) : ICatalog
{
    public string Name { get; } = name;
    public IFileIO FileIO { get; } = io;
    protected Func<long> Clock { get; init; } = () => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public abstract Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default);
    public abstract Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default);
    public abstract Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default);
    public abstract Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default);
    public abstract Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default);
    public abstract Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default);

    /// <summary>Current metadata pointer, or null when the table does not exist.</summary>
    protected abstract Task<string?> CurrentMetadataLocationAsync(TableIdentifier id, CancellationToken ct);

    /// <summary>
    /// Atomically point the table at <paramref name="newLocation"/> if it still points at
    /// <paramref name="expectedLocation"/> (null = create). Throw <see cref="CommitFailedException"/> on a
    /// lost race; let other exceptions escape (they become <see cref="CommitStateUnknownException"/>).
    /// </summary>
    protected abstract Task SwapMetadataLocationAsync(TableIdentifier id, string? expectedLocation, string newLocation, TableMetadata metadata, CancellationToken ct);

    protected abstract Task RemoveTableEntryAsync(TableIdentifier id, CancellationToken ct);

    /// <summary>Default table location: <c>{warehouse}/{ns}.db/{table}</c> (the Hive/Glue convention Athena also uses).</summary>
    protected abstract Task<string> DefaultTableLocationAsync(TableIdentifier id, CancellationToken ct);

    public async Task<Table> CreateTableAsync(CreateTableRequest r, CancellationToken ct = default)
    {
        if (!await NamespaceExistsAsync(r.Identifier.Namespace, ct)) throw new NoSuchNamespaceException($"Namespace {r.Identifier.Namespace} does not exist");
        if (await CurrentMetadataLocationAsync(r.Identifier, ct) is not null) throw new AlreadyExistsException($"Table {r.Identifier} already exists");
        var location = r.Location ?? await DefaultTableLocationAsync(r.Identifier, ct);
        var fresh = TableMetadata.NewTable(r.Schema, r.Spec, r.SortOrder, location, r.Properties, r.FormatVersion, Clock);
        var metadata = await WriteAndSwapAsync(r.Identifier, null, loc => fresh with { MetadataFileLocation = loc }, ct);
        return new Table(this, r.Identifier, metadata, FileIO);
    }

    public async Task<Table> LoadTableAsync(TableIdentifier id, CancellationToken ct = default)
    {
        var location = await CurrentMetadataLocationAsync(id, ct) ?? throw new NoSuchTableException($"Table {id} does not exist");
        return new Table(this, id, await ReadMetadataAsync(location, ct), FileIO);
    }

    public async Task<bool> TableExistsAsync(TableIdentifier id, CancellationToken ct = default) => await CurrentMetadataLocationAsync(id, ct) is not null;

    public async Task<Table> RegisterTableAsync(TableIdentifier id, string metadataLocation, CancellationToken ct = default)
    {
        if (await CurrentMetadataLocationAsync(id, ct) is not null) throw new AlreadyExistsException($"Table {id} already exists");
        var metadata = await ReadMetadataAsync(metadataLocation, ct);
        await SwapMetadataLocationAsync(id, null, metadataLocation, metadata, ct);
        return new Table(this, id, metadata, FileIO);
    }

    public async Task DropTableAsync(TableIdentifier id, bool purge = false, CancellationToken ct = default)
    {
        var location = await CurrentMetadataLocationAsync(id, ct) ?? throw new NoSuchTableException($"Table {id} does not exist");
        var metadata = purge ? await ReadMetadataAsync(location, ct) : null;
        await RemoveTableEntryAsync(id, ct);
        if (metadata is not null)
        {
            // Entry first, files second: a crash in between leaves orphans, never a table pointing at missing files.
            var files = await TableFiles.ReachableAsync(metadata, FileIO, metadata.Snapshots, ct);
            files.UnionWith(metadata.MetadataLog.Select(l => l.MetadataFile).Append(location));
            await TableFiles.DeleteAllAsync(FileIO, files, ct);
        }
    }

    public async Task<TableMetadata> CommitTableAsync(TableCommit commit, CancellationToken ct = default)
    {
        var currentLocation = await CurrentMetadataLocationAsync(commit.Identifier, ct);
        var current = currentLocation is null ? null : await ReadMetadataAsync(currentLocation, ct);
        foreach (var req in commit.Requirements) req.Validate(current);
        if (current is null) throw new NoSuchTableException($"Table {commit.Identifier} does not exist");

        var builder = TableMetadataBuilder.From(current, Clock).ApplyAll(commit.Updates);
        if (builder.Changes.Count == 0) return current;
        return await WriteAndSwapAsync(commit.Identifier, current, builder.Build, ct);
    }

    public async Task CommitTransactionAsync(IReadOnlyList<TableCommit> commits, CancellationToken ct = default)
    {
        if (commits.Select(c => c.Identifier).Distinct().Count() != commits.Count) throw new ArgumentException("A transaction may change each table once");
        var swaps = new List<MetadataSwap>();
        try
        {
            foreach (var commit in commits)
            {
                var currentLocation = await CurrentMetadataLocationAsync(commit.Identifier, ct);
                var current = currentLocation is null ? null : await ReadMetadataAsync(currentLocation, ct);
                foreach (var req in commit.Requirements) req.Validate(current);
                if (current is null) throw new NoSuchTableException($"Table {commit.Identifier} does not exist");
                var builder = TableMetadataBuilder.From(current, Clock).ApplyAll(commit.Updates);
                if (builder.Changes.Count == 0) continue;
                var newLocation = NewMetadataLocation(builder.Build(""), current.MetadataFileLocation);
                var updated = builder.Build(newLocation);
                await FileIO.WriteAsync(newLocation, Encoding.UTF8.GetBytes(MetadataJson.Write(updated)), overwrite: false, ct);
                swaps.Add(new MetadataSwap(commit.Identifier, current.MetadataFileLocation!, newLocation, updated));
            }
            if (swaps.Count > 0) await SwapMetadataLocationsAsync(swaps, ct);
        }
        catch (Exception e) when (e is CommitFailedException or NoSuchTableException or NotSupportedException or ArgumentException)
        {
            // Nothing was swapped: the new metadata files are unreferenced.
            foreach (var swap in swaps) await TryDeleteAsync(swap.NewLocation, ct);
            throw;
        }
    }

    /// <summary>One pointer move of a multi-table commit.</summary>
    protected sealed record MetadataSwap(TableIdentifier Id, string ExpectedLocation, string NewLocation, TableMetadata Metadata);

    /// <summary>
    /// Move every pointer, or none (throwing <see cref="CommitFailedException"/>). The default refuses: a metastore
    /// needs a native multi-row transaction for this, which Glue does not have.
    /// </summary>
    protected virtual Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct) =>
        throw new NotSupportedException($"The {Name} catalog cannot commit several tables atomically");

    /// <summary>Write the next metadata file (never overwriting), then CAS the catalog pointer to it.</summary>
    private async Task<TableMetadata> WriteAndSwapAsync(TableIdentifier id, TableMetadata? @base, Func<string, TableMetadata> build, CancellationToken ct)
    {
        // build is pure, so call it once to learn the (possibly changed) location and again to stamp the file name.
        var newLocation = NewMetadataLocation(build(""), @base?.MetadataFileLocation);
        var updated = build(newLocation);

        await FileIO.WriteAsync(newLocation, Encoding.UTF8.GetBytes(MetadataJson.Write(updated)), overwrite: false, ct);
        try
        {
            await SwapMetadataLocationAsync(id, @base?.MetadataFileLocation, newLocation, updated, ct);
        }
        catch (CommitFailedException)
        {
            // We definitely lost: the new file is unreferenced and safe to remove.
            await TryDeleteAsync(newLocation, ct);
            throw;
        }
        catch (CommitStateUnknownException)
        {
            throw;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // We may have won. Never delete the file: a reader may already be following the pointer.
            throw new CommitStateUnknownException($"Commit to {id} failed with an unknown outcome; check {newLocation}", e)
            {
                // A service error response (as opposed to a timeout) means the call has finished.
                ServerResponded = e is Amazon.Runtime.AmazonServiceException,
            };
        }
        return updated;
    }

    protected async Task<TableMetadata> ReadMetadataAsync(string location, CancellationToken ct)
    {
        var bytes = await FileIO.ReadAllAsync(location, ct);
        if (location.EndsWith(".gz", StringComparison.Ordinal))
        {
            using var gz = new System.IO.Compression.GZipStream(new MemoryStream(bytes), System.IO.Compression.CompressionMode.Decompress);
            using var ms = new MemoryStream();
            await gz.CopyToAsync(ms, ct);
            bytes = ms.ToArray();
        }
        return MetadataJson.Read(Encoding.UTF8.GetString(bytes), location);
    }

    [GeneratedRegex(@"/(\d+)-[0-9a-fA-F-]+\.metadata\.json(\.gz)?$")]
    private static partial Regex VersionRe();

    /// <summary><c>{location}/metadata/{version:00000}-{uuid}.metadata.json</c>, the Java/PyIceberg naming.</summary>
    public static string NewMetadataLocation(TableMetadata m, string? previous)
    {
        var version = 0;
        if (previous is not null && VersionRe().Match(previous) is { Success: true } match)
            version = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture) + 1;
        var dir = m.Property("write.metadata.path", $"{m.Location}/metadata").TrimEnd('/');
        return $"{dir}/{version:D5}-{Guid.NewGuid()}.metadata.json";
    }

    private async Task TryDeleteAsync(string location, CancellationToken ct)
    {
        try { await FileIO.DeleteAsync(location, ct); }
        catch (Exception e) when (e is not OperationCanceledException) { /* best effort: orphan cleanup will find it */ }
    }

    public virtual void Dispose() => FileIO.Dispose();
}

/// <summary>Process-local catalog with a CAS pointer per table. The reference for catalog conformance tests.</summary>
public class InMemoryCatalog(string warehouse, IFileIO? io = null, string name = "memory") : MetastoreCatalog(name, io ?? new LocalFileIO()), IViewCatalog
{
    private readonly Dictionary<TableIdentifier, string> _views = [];

    public Task<IReadOnlyList<TableIdentifier>> ListViewsAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock) return Task.FromResult<IReadOnlyList<TableIdentifier>>(_views.Keys.Where(v => v.Namespace.Equals(ns)).ToList());
    }

    public async Task<View> CreateViewAsync(TableIdentifier id, ViewDefinition definition, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.ContainsKey(id.Namespace)) throw new NoSuchNamespaceException($"Namespace {id.Namespace} does not exist");
            if (_views.ContainsKey(id) || _tables.ContainsKey(id)) throw new AlreadyExistsException($"{id} already exists");
        }
        var metadata = ViewMetadata.Create(await DefaultTableLocationAsync(id, ct), definition.Schema, definition.ToVersion(), properties, Clock());
        return await WriteViewAsync(id, null, metadata, ct);
    }

    public async Task<View> LoadViewAsync(TableIdentifier id, CancellationToken ct = default)
    {
        string? location;
        lock (_lock) location = _views.GetValueOrDefault(id);
        if (location is null) throw new NoSuchTableException($"View {id} does not exist");
        return new View(id, await ViewFiles.ReadAsync(FileIO, location, ct));
    }

    public Task<bool> ViewExistsAsync(TableIdentifier id, CancellationToken ct = default)
    {
        lock (_lock) return Task.FromResult(_views.ContainsKey(id));
    }

    public async Task<View> ReplaceViewAsync(TableIdentifier id, ViewDefinition definition, CancellationToken ct = default)
    {
        var current = await LoadViewAsync(id, ct);
        return await WriteViewAsync(id, current.Metadata.MetadataFileLocation, current.Metadata.Replace(definition.Schema, definition.ToVersion(), Clock()), ct);
    }

    public Task DropViewAsync(TableIdentifier id, CancellationToken ct = default)
    {
        lock (_lock)
            if (!_views.Remove(id)) throw new NoSuchTableException($"View {id} does not exist");
        return Task.CompletedTask;
    }

    public Task RenameViewAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (_views.ContainsKey(to) || _tables.ContainsKey(to)) throw new AlreadyExistsException($"{to} already exists");
            if (!_views.Remove(from, out var loc)) throw new NoSuchTableException($"View {from} does not exist");
            _views[to] = loc;
        }
        return Task.CompletedTask;
    }

    private async Task<View> WriteViewAsync(TableIdentifier id, string? expected, ViewMetadata metadata, CancellationToken ct)
    {
        var location = await ViewFiles.WriteNextAsync(FileIO, metadata, expected, ct);
        lock (_lock)
        {
            if (_views.GetValueOrDefault(id) != expected) throw new CommitFailedException($"View {id} changed concurrently");
            _views[id] = location;
        }
        return new View(id, metadata with { MetadataFileLocation = location });
    }

    private readonly Dictionary<Namespace, Dictionary<string, string>> _namespaces = [];
    private readonly Dictionary<TableIdentifier, string> _tables = [];
    private readonly object _lock = new();

    public override Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.TryAdd(ns, new Dictionary<string, string>(properties ?? new Dictionary<string, string>())))
                throw new AlreadyExistsException($"Namespace {ns} already exists");
        }
        return Task.CompletedTask;
    }

    public override Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default)
    {
        lock (_lock)
        {
            var depth = parent?.Levels.Count ?? 0;
            IReadOnlyList<Namespace> result = _namespaces.Keys
                .Where(n => n.Levels.Count == depth + 1 && (parent is null || n.Levels.Take(depth).SequenceEqual(parent.Levels)))
                .ToList();
            return Task.FromResult(result);
        }
    }

    public override Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock) return Task.FromResult(_namespaces.ContainsKey(ns));
    }

    public override Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (_tables.Keys.Any(t => t.Namespace.Equals(ns))) throw new InvalidOperationException($"Namespace {ns} is not empty");
            _namespaces.Remove(ns);
        }
        return Task.CompletedTask;
    }

    public override Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock) return Task.FromResult<IReadOnlyList<TableIdentifier>>(_tables.Keys.Where(t => t.Namespace.Equals(ns)).ToList());
    }

    public override Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_tables.Remove(from, out var loc)) throw new NoSuchTableException($"Table {from} does not exist");
            if (!_tables.TryAdd(to, loc))
            {
                _tables[from] = loc;
                throw new AlreadyExistsException($"Table {to} already exists");
            }
        }
        return Task.CompletedTask;
    }

    protected override Task<string?> CurrentMetadataLocationAsync(TableIdentifier id, CancellationToken ct)
    {
        lock (_lock) return Task.FromResult(_tables.GetValueOrDefault(id));
    }

    protected override Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
    {
        lock (_lock)
        {
            var current = _tables.GetValueOrDefault(id);
            if (current != expected) throw new CommitFailedException($"Table {id} metadata moved from {expected ?? "(none)"} to {current ?? "(none)"}");
            _tables[id] = newLocation;
        }
        return Task.CompletedTask;
    }

    protected override Task RemoveTableEntryAsync(TableIdentifier id, CancellationToken ct)
    {
        lock (_lock) _tables.Remove(id);
        return Task.CompletedTask;
    }

    protected override Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct)
    {
        lock (_lock)
        {
            foreach (var swap in swaps)
                if (_tables.GetValueOrDefault(swap.Id) != swap.ExpectedLocation) throw new CommitFailedException($"Table {swap.Id} changed concurrently");
            foreach (var swap in swaps) _tables[swap.Id] = swap.NewLocation;
        }
        return Task.CompletedTask;
    }

    protected override Task<string> DefaultTableLocationAsync(TableIdentifier id, CancellationToken ct) =>
        Task.FromResult($"{warehouse.TrimEnd('/')}/{string.Join('/', id.Namespace.Levels)}.db/{id.Name}");
}
