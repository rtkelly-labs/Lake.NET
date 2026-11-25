using Iceberg.Net.Catalog;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Data;

/// <summary>
/// A change to one table that is planned and written (data files, delete files, manifests, manifest list) but not
/// committed. <see cref="Commit"/> is the <see cref="TableCommit"/> (requirements and updates) that would apply it, or
/// null when the change turned out to change nothing. Commit several at once, atomically, with
/// <see cref="CommitAllAsync"/>: when another writer wins a race, every staged change is planned again from scratch
/// against the refreshed tables (never replayed), so checks made while planning hold for the state the commit lands on.
/// </summary>
/// <remarks>
/// Made by <see cref="TableMutations.StageMergeAsync"/>, <see cref="TableMutations.StageUpdateAsync"/> or, for any
/// <see cref="SnapshotProducer"/>, <see cref="StageAsync(Table, Func{CancellationToken, Task{SnapshotProducer?}}, CancellationToken)"/>.
/// A stage that is not committed should be <see cref="DiscardAsync">discarded</see>.
/// </remarks>
public class StagedCommit
{
    private readonly Func<CancellationToken, Task<(SnapshotProducer? Producer, object? Outcome)>> _plan;
    private IReadOnlyList<string> _artifacts = [];

    internal StagedCommit(Table table, Func<CancellationToken, Task<(SnapshotProducer? Producer, object? Outcome)>> plan)
    {
        Table = table;
        _plan = plan;
    }

    /// <summary>The table, refreshed by every re-plan and after the commit (unless that read fails: the commit stands).</summary>
    public Table Table { get; }

    /// <summary>What committing the current plan sends to the catalog; null when the plan changes nothing.</summary>
    public TableCommit? Commit { get; private set; }

    /// <summary>The snapshot the current plan adds (committed once <see cref="IsCommitted"/>); null when it changes nothing.</summary>
    public Snapshot? Snapshot { get; private set; }

    /// <summary>How many times the change has been planned (1, plus one per lost race).</summary>
    public int Attempts { get; private set; }

    /// <summary>The commit went through (or there was nothing to commit).</summary>
    public bool IsCommitted { get; private set; }

    /// <summary>What the current plan found (e.g. row counts), as the staging method defines it.</summary>
    protected object? Outcome { get; private set; }

    /// <summary>
    /// Stage the snapshot <paramref name="plan"/> builds against <see cref="Table"/>'s current metadata. The plan runs
    /// again after every lost race, so it must build a new producer each time and read the table state it depends on
    /// (return null when there is nothing to commit). The producer's own retries are not used.
    /// </summary>
    public static async Task<StagedCommit> StageAsync(Table table, Func<CancellationToken, Task<SnapshotProducer?>> plan, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(plan);
        var staged = new StagedCommit(table, async c => (await plan(c), null));
        await staged.PlanAsync(ct);
        return staged;
    }

    /// <summary>
    /// Plan against the table's current metadata and write this attempt's files. A base file removed by compaction and
    /// expiry while planning counts as a lost race when the table has moved (as in every mutation): plan again.
    /// </summary>
    internal async Task PlanAsync(CancellationToken ct)
    {
        var policy = CommitRetryPolicy.From(Table.Metadata);
        for (var retry = 0; ; retry++)
        {
            Attempts++;
            SnapshotProducer? producer;
            try
            {
                (producer, Outcome) = await _plan(ct);
            }
            catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException && retry < policy.NumRetries)
            {
                var before = Table.Metadata.CurrentSnapshotId;
                await Table.RefreshAsync(ct);
                if (Table.Metadata.CurrentSnapshotId == before) throw;
                continue;
            }
            if (producer is null)
            {
                (Commit, Snapshot, _artifacts) = (null, null, []);
                return;
            }
            (Commit, Snapshot, _artifacts) = await producer.StageWithArtifactsAsync(ct);
            return;
        }
    }

    /// <summary>
    /// Delete the manifests, manifest list and delete files written for the current plan, which nothing references
    /// unless it was committed (then this does nothing). Data files are left for orphan-file cleanup, as for every
    /// lost attempt of a mutation. Best effort.
    /// </summary>
    public async Task DiscardAsync(CancellationToken ct = default)
    {
        if (IsCommitted) return;
        var artifacts = _artifacts;
        _artifacts = [];
        Commit = null;
        Snapshot = null;
        foreach (var path in artifacts)
        {
            try { await Table.IO.DeleteAsync(path, ct); }
            catch (Exception e) when (e is not OperationCanceledException) { /* orphan-file cleanup will find it */ }
        }
    }

    /// <summary>
    /// Commit every staged change atomically: all of them apply or none does. One table commits with
    /// <see cref="ICatalog.CommitTableAsync"/>; several need a catalog whose
    /// <see cref="ICatalog.SupportsMultiTableTransactions"/> is true (<see cref="ICatalog.CommitTransactionAsync"/>),
    /// otherwise <see cref="NotSupportedException"/>. On a conflict (<see cref="CommitFailedException"/>) every table is
    /// refreshed, every change planned again and the transaction retried with back-off, up to the largest
    /// <c>commit.retry.*</c> budget among the tables; then the last conflict is thrown. An exception thrown by a
    /// re-plan (a failed check, say) is thrown as it is. Whenever the outcome is a definite failure, the files written
    /// for the abandoned plans are deleted; after <see cref="CommitStateUnknownException"/> nothing is.
    /// </summary>
    public static async Task CommitAllAsync(IReadOnlyList<StagedCommit> staged, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(staged);
        if (staged.Count == 0) return;
        if (staged.Any(s => s.IsCommitted)) throw new InvalidOperationException("A staged commit was already committed");
        if (staged.Select(s => s.Table.Identifier).Distinct().Count() != staged.Count)
            throw new ArgumentException("A transaction may change each table once: stage one change per table");
        var catalog = staged[0].Table.Catalog;
        if (staged.Any(s => !ReferenceEquals(s.Table.Catalog, catalog)))
            throw new ArgumentException("Every staged change must belong to a table of the same catalog instance");

        var policies = staged.Select(s => CommitRetryPolicy.From(s.Table.Metadata)).ToList();
        var policy = new CommitRetryPolicy(policies.Max(p => p.NumRetries), policies.Max(p => p.MinWaitMs), policies.Max(p => p.MaxWaitMs));
        using var telemetry = IcebergTelemetry.StartCommit(string.Join(", ", staged.Select(s => s.Table.Identifier)), "transaction");
        var committing = false;
        try
        {
            for (var attempt = 1; ; attempt++)
            {
                if (attempt > 1)
                    foreach (var s in staged)
                    {
                        await s.Table.RefreshAsync(ct);
                        await s.PlanAsync(ct);
                    }
                var pending = staged.Where(s => s.Commit is not null).ToList();
                if (pending.Count == 0)
                {
                    foreach (var s in staged) s.IsCommitted = true;
                    telemetry.NothingToCommit();
                    return;
                }
                try
                {
                    committing = true;
                    await CommitOnceAsync(catalog, pending, ct);
                    committing = false;
                }
                catch (CommitFailedException conflict) when (attempt <= policy.NumRetries)
                {
                    committing = false;
                    // Definitely not applied: this attempt's files are unreferenced, and the re-plan writes new ones.
                    foreach (var s in pending) await s.DiscardAsync(ct);
                    var wait = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * (1 << Math.Min(attempt - 1, 20)));
                    telemetry.Retrying(attempt, conflict, TimeSpan.FromMilliseconds(wait));
                    await Task.Delay(wait + Random.Shared.Next(wait / 10 + 1), ct);
                    continue;
                }
                foreach (var s in staged) s.IsCommitted = true;
                // A transaction returns no metadata: load what was committed (a single commit already did). Best effort:
                // the transaction has committed, and throwing now would tell the caller it had not.
                if (pending.Count > 1)
                    foreach (var s in pending)
                    {
                        try { await s.Table.RefreshAsync(ct); }
                        catch (Exception e) when (e is not OperationCanceledException) { /* Table stays at its pre-commit metadata until refreshed */ }
                    }
                telemetry.Committed(pending[0].Snapshot!.SnapshotId, "transaction");
                return;
            }
        }
        catch (Exception e) when (telemetry.FailedWith(e) || !committing || e is CommitFailedException or NotSupportedException)
        {
            // A definite failure: a conflict with the retries spent, a catalog without transactions, or an error while
            // planning (a failed check). Nothing references the staged files. (FailedWith only records; it is false.)
            foreach (var s in staged) await s.DiscardAsync(CancellationToken.None);
            throw;
        }
    }

    private static async Task CommitOnceAsync(ICatalog catalog, List<StagedCommit> pending, CancellationToken ct)
    {
        try
        {
            if (pending.Count == 1)
                await pending[0].Table.CommitAsync(pending[0].Commit!.Requirements, pending[0].Commit!.Updates, ct);
            else if (!catalog.SupportsMultiTableTransactions)
                throw new NotSupportedException($"The {catalog.Name} catalog cannot commit several tables atomically " +
                    $"({string.Join(", ", pending.Select(s => s.Table.Identifier))})");
            else
                await catalog.CommitTransactionAsync(pending.Select(s => s.Commit!).ToList(), ct);
        }
        catch (ArgumentException stale) when (pending.Any(s => s.Table.Metadata.FormatVersion > 1))
        {
            // As in SnapshotProducer: another writer took a staged snapshot's sequence number (a commit on another
            // branch, or before a rollback). That is a conflict to re-plan, not an invalid commit.
            foreach (var s in pending) await s.Table.RefreshAsync(ct);
            var taken = pending.FirstOrDefault(s => s.Snapshot!.SequenceNumber <= s.Table.Metadata.LastSequenceNumber);
            if (taken is null) throw;
            throw new CommitFailedException($"Sequence number {taken.Snapshot!.SequenceNumber} of the snapshot staged for {taken.Table.Identifier} is stale: {stale.Message}", stale);
        }
        catch (CommitStateUnknownException unknown) when (unknown.ServerResponded)
        {
            // The catalog finished the request with an error, so its outcome is fixed: look for the staged snapshots
            // (IDs are random 63-bit values). All present: applied. None: not applied, a plain conflict.
            var applied = 0;
            try
            {
                foreach (var s in pending)
                {
                    await s.Table.RefreshAsync(ct);
                    if (s.Table.Metadata.SnapshotById(s.Snapshot!.SnapshotId) is not null) applied++;
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                throw new CommitStateUnknownException($"{unknown.Message}; resolving it failed: {e.Message}", unknown);
            }
            if (applied == pending.Count) return;
            if (applied == 0) throw new CommitFailedException($"The transaction was not applied ({unknown.Message})", unknown);
            throw;
        }
    }
}

/// <summary>A staged change with a typed result (e.g. the row counts of a staged MERGE).</summary>
public sealed class StagedCommit<TResult> : StagedCommit
{
    private readonly Func<Snapshot?, object?, int, TResult> _result;

    internal StagedCommit(Table table, Func<CancellationToken, Task<(SnapshotProducer? Producer, object? Outcome)>> plan,
        Func<Snapshot?, object?, int, TResult> result) : base(table, plan) => _result = result;

    /// <summary>The result of the current plan: final once <see cref="StagedCommit.IsCommitted"/> is true.</summary>
    public TResult Result => _result(Snapshot, Outcome, Attempts);
}
