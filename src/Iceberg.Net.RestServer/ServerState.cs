using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestServer;

/// <summary>A finalized response kept for an <c>Idempotency-Key</c>, replayed byte for byte.</summary>
public sealed record StoredResponse(int Status, string? ContentType, string? ETag, byte[] Body);

/// <summary>
/// An <c>Idempotency-Key</c> someone holds: the request's fingerprint (method, path, body), when it was claimed, and
/// the final response once the request finished (null while it runs).
/// </summary>
public sealed record IdempotencyClaim(string Fingerprint, DateTimeOffset Claimed, StoredResponse? Response);

/// <summary>A principal has filled its retained idempotency-claim budget; existing keys remain replayable.</summary>
public sealed class IdempotencyCapacityException : Exception
{
    public IdempotencyCapacityException() : base("The principal's idempotency capacity is full; retry after existing claims expire") { }
}

/// <summary>A scan plan: <c>submitted</c>, <c>completed</c> (with its result JSON) or <c>failed</c> (with an error).</summary>
public sealed record PlanRecord(string PlanId, TableIdentifier Table, string Status, string? Result, string? Error, DateTimeOffset Created, DateTimeOffset? Finished);

/// <summary>
/// The state the server keeps between requests, beyond the catalog itself: <c>Idempotency-Key</c> claims and responses,
/// and scan plans by plan ID. Kept next to the catalog (<see cref="SqlCatalogStore"/> in two more tables,
/// <see cref="InMemoryCatalogStore"/> in memory), so every server instance over one database shares them. A custom
/// <see cref="ICatalogStore"/> that does not implement this gets process-local state.
/// </summary>
public interface IServerStateStore
{
    /// <summary>
    /// Claim <paramref name="key"/> for <paramref name="principal"/>: null when this call claimed it, else the existing
    /// claim (running or finished). A claim made before <paramref name="expiredBefore"/> no longer counts and is replaced.
    /// </summary>
    Task<IdempotencyClaim?> ClaimAsync(string principal, string key, string fingerprint, DateTimeOffset now, DateTimeOffset expiredBefore, CancellationToken ct = default);

    /// <summary>Atomically claim a key within a per-principal budget, including pending claims; never evict unexpired keys.</summary>
    Task<IdempotencyClaim?> ClaimAsync(string principal, string key, string fingerprint, DateTimeOffset now, DateTimeOffset expiredBefore,
        int maxClaimsPerPrincipal, CancellationToken ct = default) =>
        throw new NotSupportedException("The server state store must implement bounded idempotency claims");

    /// <summary>Record the final response of the claim made at <paramref name="claimed"/> (nothing happens if it was taken over).</summary>
    Task CompleteAsync(string principal, string key, DateTimeOffset claimed, StoredResponse response, CancellationToken ct = default);

    /// <summary>Drop the unfinished claim made at <paramref name="claimed"/>, so the next request with the key runs.</summary>
    Task ReleaseAsync(string principal, string key, DateTimeOffset claimed, CancellationToken ct = default);

    /// <summary>Add a new plan.</summary>
    Task AddPlanAsync(PlanRecord plan, CancellationToken ct = default);

    /// <summary>Finish a <c>submitted</c> plan; false when it is gone (cancelled) or already finished.</summary>
    Task<bool> FinishPlanAsync(string planId, string status, string? result, string? error, DateTimeOffset finished, CancellationToken ct = default);

    Task<PlanRecord?> LoadPlanAsync(string planId, CancellationToken ct = default);

    /// <summary>Forget a plan; false when there was none.</summary>
    Task<bool> DeletePlanAsync(string planId, CancellationToken ct = default);

    /// <summary>
    /// Drop idempotency claims made before <paramref name="claimsBefore"/>, plans finished before
    /// <paramref name="plansFinishedBefore"/>, and plans still submitted since before <paramref name="plansSubmittedBefore"/>.
    /// </summary>
    Task ExpireAsync(DateTimeOffset claimsBefore, DateTimeOffset plansFinishedBefore, DateTimeOffset plansSubmittedBefore, CancellationToken ct = default);

    /// <summary>(idempotency claims, plans) kept now, for tests and metrics.</summary>
    Task<(int Claims, int Plans)> CountAsync(CancellationToken ct = default);
}

/// <summary>Server state in this process's memory: one lock, two dictionaries.</summary>
public sealed class InMemoryServerStateStore : IServerStateStore
{
    private readonly object _lock = new();
    private readonly Dictionary<(string Principal, string Key), IdempotencyClaim> _claims = [];
    private readonly Dictionary<string, PlanRecord> _plans = new(StringComparer.Ordinal);

    public Task<IdempotencyClaim?> ClaimAsync(string principal, string key, string fingerprint, DateTimeOffset now, DateTimeOffset expiredBefore, CancellationToken ct = default) =>
        ClaimAsync(principal, key, fingerprint, now, expiredBefore, int.MaxValue, ct);

    public Task<IdempotencyClaim?> ClaimAsync(string principal, string key, string fingerprint, DateTimeOffset now, DateTimeOffset expiredBefore,
        int maxClaimsPerPrincipal, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxClaimsPerPrincipal);
        lock (_lock)
        {
            if (_claims.TryGetValue((principal, key), out var existing) && existing.Claimed >= expiredBefore) return Task.FromResult<IdempotencyClaim?>(existing);
            if (_claims.Count(c => c.Key.Principal == principal && c.Value.Claimed >= expiredBefore) >= maxClaimsPerPrincipal)
                throw new IdempotencyCapacityException();
            _claims[(principal, key)] = new IdempotencyClaim(fingerprint, now, null);
            return Task.FromResult<IdempotencyClaim?>(null);
        }
    }

    public Task CompleteAsync(string principal, string key, DateTimeOffset claimed, StoredResponse response, CancellationToken ct = default)
    {
        lock (_lock)
            if (_claims.TryGetValue((principal, key), out var c) && c.Claimed == claimed) _claims[(principal, key)] = c with { Response = response };
        return Task.CompletedTask;
    }

    public Task ReleaseAsync(string principal, string key, DateTimeOffset claimed, CancellationToken ct = default)
    {
        lock (_lock)
            if (_claims.TryGetValue((principal, key), out var c) && c.Claimed == claimed && c.Response is null) _claims.Remove((principal, key));
        return Task.CompletedTask;
    }

    public Task AddPlanAsync(PlanRecord plan, CancellationToken ct = default)
    {
        lock (_lock) _plans[plan.PlanId] = plan;
        return Task.CompletedTask;
    }

    public Task<bool> FinishPlanAsync(string planId, string status, string? result, string? error, DateTimeOffset finished, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (!_plans.TryGetValue(planId, out var p) || p.Status != "submitted") return Task.FromResult(false);
            _plans[planId] = p with { Status = status, Result = result, Error = error, Finished = finished };
            return Task.FromResult(true);
        }
    }

    public Task<PlanRecord?> LoadPlanAsync(string planId, CancellationToken ct = default)
    {
        lock (_lock) return Task.FromResult(_plans.GetValueOrDefault(planId));
    }

    public Task<bool> DeletePlanAsync(string planId, CancellationToken ct = default)
    {
        lock (_lock) return Task.FromResult(_plans.Remove(planId));
    }

    public Task ExpireAsync(DateTimeOffset claimsBefore, DateTimeOffset plansFinishedBefore, DateTimeOffset plansSubmittedBefore, CancellationToken ct = default)
    {
        lock (_lock)
        {
            foreach (var k in _claims.Where(kv => kv.Value.Claimed < claimsBefore).Select(kv => kv.Key).ToList()) _claims.Remove(k);
            foreach (var k in _plans.Where(kv => kv.Value.Finished is { } f ? f < plansFinishedBefore : kv.Value.Created < plansSubmittedBefore).Select(kv => kv.Key).ToList())
                _plans.Remove(k);
        }
        return Task.CompletedTask;
    }

    public Task<(int Claims, int Plans)> CountAsync(CancellationToken ct = default)
    {
        lock (_lock) return Task.FromResult((_claims.Count, _plans.Count));
    }
}
