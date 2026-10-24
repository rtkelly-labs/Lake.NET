using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.RestServer;

/// <summary>Tables and views share one name space per namespace: a name is either, never both.</summary>
public enum CatalogEntryKind { Table, View }

/// <summary>
/// One pointer move: <paramref name="Expected"/> is the metadata file the entry must still point at, or null for an
/// entry that must not exist yet (a staged create committed in a transaction).
/// </summary>
public sealed record PointerSwap(TableIdentifier Id, string? Expected, string NewLocation);

/// <summary>
/// Where the server keeps namespaces and the pointer from each table or view name to its current metadata file. The
/// server does all metadata work (requirements, updates, writing metadata files through <c>IFileIO</c>); a store only
/// has to move pointers atomically. <see cref="SwapAsync"/> is the one concurrency primitive: every pointer it is given
/// moves, or none does.
/// </summary>
public interface ICatalogStore
{
    /// <summary>Create the backing tables if needed. Called once at start-up.</summary>
    Task InitializeAsync(CancellationToken ct = default);

    /// <summary>Throws <see cref="AlreadyExistsException"/> if the namespace exists.</summary>
    Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string> properties, CancellationToken ct = default);

    /// <summary>The namespace's properties, or null when it does not exist.</summary>
    Task<IReadOnlyDictionary<string, string>?> LoadNamespaceAsync(Namespace ns, CancellationToken ct = default);

    /// <summary>Direct children of <paramref name="parent"/> (top-level namespaces when null), in no particular order.</summary>
    Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent, CancellationToken ct = default);

    /// <summary>Set and remove properties in one step. Throws <see cref="NoSuchNamespaceException"/>.</summary>
    Task UpdateNamespacePropertiesAsync(Namespace ns, IReadOnlyDictionary<string, string> updates, IReadOnlyCollection<string> removals, CancellationToken ct = default);

    /// <summary>Remove the namespace (the caller has checked it is empty). Throws <see cref="NoSuchNamespaceException"/>.</summary>
    Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default);

    /// <summary>Names of the tables or views directly in <paramref name="ns"/>.</summary>
    Task<IReadOnlyList<string>> ListAsync(Namespace ns, CatalogEntryKind kind, CancellationToken ct = default);

    /// <summary>The entry's current metadata location, or null when no entry of that kind holds the name.</summary>
    Task<string?> GetAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default);

    /// <summary>The kind of entry holding the name, or null.</summary>
    Task<CatalogEntryKind?> KindAsync(TableIdentifier id, CancellationToken ct = default);

    /// <summary>
    /// Move every pointer from its expected location to its new one, all or nothing. A pointer that moved, an entry that
    /// vanished, or a name that was taken (for an <see cref="PointerSwap.Expected"/> of null) is a
    /// <see cref="CommitFailedException"/>; nothing changes then. Creating in a namespace that does not exist is a
    /// <see cref="NoSuchNamespaceException"/>.
    /// </summary>
    Task SwapAsync(IReadOnlyList<PointerSwap> swaps, CatalogEntryKind kind, CancellationToken ct = default);

    /// <summary>Remove the entry; false when there was none.</summary>
    Task<bool> DeleteAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default);

    /// <summary>
    /// Move the entry to a new name, atomically. Throws <see cref="NoSuchTableException"/> (source missing),
    /// <see cref="AlreadyExistsException"/> (target name taken by a table or view) or
    /// <see cref="NoSuchNamespaceException"/> (target namespace missing).
    /// </summary>
    Task RenameAsync(TableIdentifier from, TableIdentifier to, CatalogEntryKind kind, CancellationToken ct = default);
}

/// <summary>
/// A process-local store: one lock, dictionaries. For tests and throwaway servers. Its server state (idempotency keys,
/// plans) is in memory too, shared by every server given this instance.
/// </summary>
public sealed class InMemoryCatalogStore : ICatalogStore, IServerStateStore
{
    private readonly InMemoryServerStateStore _state = new();

    public Task<IdempotencyClaim?> ClaimAsync(string principal, string key, string fingerprint, DateTimeOffset now, DateTimeOffset expiredBefore, CancellationToken ct = default) =>
        _state.ClaimAsync(principal, key, fingerprint, now, expiredBefore, ct);
    public Task CompleteAsync(string principal, string key, DateTimeOffset claimed, StoredResponse response, CancellationToken ct = default) =>
        _state.CompleteAsync(principal, key, claimed, response, ct);
    public Task ReleaseAsync(string principal, string key, DateTimeOffset claimed, CancellationToken ct = default) => _state.ReleaseAsync(principal, key, claimed, ct);
    public Task AddPlanAsync(PlanRecord plan, CancellationToken ct = default) => _state.AddPlanAsync(plan, ct);
    public Task<bool> FinishPlanAsync(string planId, string status, string? result, string? error, DateTimeOffset finished, CancellationToken ct = default) =>
        _state.FinishPlanAsync(planId, status, result, error, finished, ct);
    public Task<PlanRecord?> LoadPlanAsync(string planId, CancellationToken ct = default) => _state.LoadPlanAsync(planId, ct);
    public Task<bool> DeletePlanAsync(string planId, CancellationToken ct = default) => _state.DeletePlanAsync(planId, ct);
    public Task ExpireAsync(DateTimeOffset claimsBefore, DateTimeOffset plansFinishedBefore, DateTimeOffset plansSubmittedBefore, CancellationToken ct = default) =>
        _state.ExpireAsync(claimsBefore, plansFinishedBefore, plansSubmittedBefore, ct);
    public Task<(int Claims, int Plans)> CountAsync(CancellationToken ct = default) => _state.CountAsync(ct);

    private readonly object _lock = new();
    private readonly Dictionary<Namespace, Dictionary<string, string>> _namespaces = [];
    private readonly Dictionary<TableIdentifier, (CatalogEntryKind Kind, string Location)> _entries = [];

    public Task InitializeAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string> properties, CancellationToken ct = default)
    {
        lock (_lock)
            if (!_namespaces.TryAdd(ns, new Dictionary<string, string>(properties))) throw new AlreadyExistsException($"Namespace already exists: {ns}");
        return Task.CompletedTask;
    }

    public Task<IReadOnlyDictionary<string, string>?> LoadNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock)
            return Task.FromResult<IReadOnlyDictionary<string, string>?>(_namespaces.TryGetValue(ns, out var p) ? new Dictionary<string, string>(p) : null);
    }

    public Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent, CancellationToken ct = default)
    {
        var depth = parent?.Levels.Count ?? 0;
        lock (_lock)
            return Task.FromResult<IReadOnlyList<Namespace>>(_namespaces.Keys
                .Where(n => n.Levels.Count > depth && (parent is null || n.Levels.Take(depth).SequenceEqual(parent.Levels)))
                .Select(n => new Namespace(n.Levels.Take(depth + 1).ToList()))
                .Distinct()
                .ToList());
    }

    public Task UpdateNamespacePropertiesAsync(Namespace ns, IReadOnlyDictionary<string, string> updates, IReadOnlyCollection<string> removals, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_namespaces.TryGetValue(ns, out var props)) throw new NoSuchNamespaceException($"Namespace does not exist: {ns}");
            foreach (var k in removals) props.Remove(k);
            foreach (var (k, v) in updates) props[k] = v;
        }
        return Task.CompletedTask;
    }

    public Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        lock (_lock)
            if (!_namespaces.Remove(ns)) throw new NoSuchNamespaceException($"Namespace does not exist: {ns}");
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> ListAsync(Namespace ns, CatalogEntryKind kind, CancellationToken ct = default)
    {
        lock (_lock)
            return Task.FromResult<IReadOnlyList<string>>(_entries.Where(e => e.Key.Namespace.Equals(ns) && e.Value.Kind == kind).Select(e => e.Key.Name).ToList());
    }

    public Task<string?> GetAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default)
    {
        lock (_lock)
            return Task.FromResult(_entries.TryGetValue(id, out var e) && e.Kind == kind ? e.Location : null);
    }

    public Task<CatalogEntryKind?> KindAsync(TableIdentifier id, CancellationToken ct = default)
    {
        lock (_lock)
            return Task.FromResult(_entries.TryGetValue(id, out var e) ? e.Kind : (CatalogEntryKind?)null);
    }

    public Task SwapAsync(IReadOnlyList<PointerSwap> swaps, CatalogEntryKind kind, CancellationToken ct = default)
    {
        lock (_lock)
        {
            foreach (var s in swaps)
            {
                if (s.Expected is null)
                {
                    if (!_namespaces.ContainsKey(s.Id.Namespace)) throw new NoSuchNamespaceException($"Namespace does not exist: {s.Id.Namespace}");
                    if (_entries.ContainsKey(s.Id)) throw new CommitFailedException($"{s.Id} was created concurrently");
                }
                else if (!_entries.TryGetValue(s.Id, out var e) || e.Kind != kind || e.Location != s.Expected)
                    throw new CommitFailedException($"{kind} {s.Id} changed concurrently: it no longer points at {s.Expected}");
            }
            foreach (var s in swaps) _entries[s.Id] = (kind, s.NewLocation);
        }
        return Task.CompletedTask;
    }

    public Task<bool> DeleteAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default)
    {
        lock (_lock)
            return Task.FromResult(_entries.TryGetValue(id, out var e) && e.Kind == kind && _entries.Remove(id));
    }

    public Task RenameAsync(TableIdentifier from, TableIdentifier to, CatalogEntryKind kind, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_entries.TryGetValue(from, out var e) || e.Kind != kind) throw new NoSuchTableException($"{kind} does not exist: {from}");
            if (!_namespaces.ContainsKey(to.Namespace)) throw new NoSuchNamespaceException($"Namespace does not exist: {to.Namespace}");
            if (_entries.ContainsKey(to)) throw new AlreadyExistsException($"Table or view already exists: {to}");
            _entries.Remove(from);
            _entries[to] = e;
        }
        return Task.CompletedTask;
    }
}
