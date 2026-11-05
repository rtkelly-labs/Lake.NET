using System.Diagnostics;
using System.Globalization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Data;

/// <summary>Read-only recovery. Missing evidence can never turn an uncertain write into a retryable conflict.</summary>
internal static class CommitRecovery
{
    private sealed record Policy(int Retries, int MinWaitMs, int MaxWaitMs, int TimeoutMs)
    {
        internal static Policy From(TableMetadata metadata)
        {
            int Read(string suffix, int fallback)
            {
                var key = "commit.status-check." + suffix;
                if (!int.TryParse(metadata.Property(key, fallback.ToString(CultureInfo.InvariantCulture)), NumberStyles.Integer,
                    CultureInfo.InvariantCulture, out var value) || value < 0)
                    throw new ArgumentException($"{key} must be a non-negative integer");
                return value;
            }
            var policy = new Policy(Read("num-retries", 3), Read("min-wait-ms", 1000), Read("max-wait-ms", 60_000), Read("total-timeout-ms", 1_800_000));
            if (policy.MinWaitMs > policy.MaxWaitMs) throw new ArgumentException("commit.status-check.min-wait-ms exceeds max-wait-ms");
            return policy;
        }
    }

    internal static async Task<bool> ConfirmAsync(IReadOnlyList<(Table Table, long SnapshotId)> commits,
        CommitStateUnknownException original, CancellationToken ct)
    {
        var identities = commits.Select(commit => commit.Table.Metadata.TableUuid).ToArray();
        Policy policy;
        try
        {
            var policies = commits.Select(commit => Policy.From(commit.Table.Metadata)).ToList();
            policy = new Policy(policies.Max(p => p.Retries), policies.Max(p => p.MinWaitMs), policies.Max(p => p.MaxWaitMs), policies.Min(p => p.TimeoutMs));
        }
        catch (Exception error)
        {
            throw Unresolved(original, error);
        }
        var started = Stopwatch.GetTimestamp();
        Exception? lastError = null;
        for (var attempt = 0; ; attempt++)
        {
            var confirmed = true;
            try
            {
                for (var i = 0; i < commits.Count; i++)
                {
                    var (table, snapshot) = commits[i];
                    ct.ThrowIfCancellationRequested();
                    try { await table.RefreshAsync(ct); }
                    catch (Exception error)
                    {
                        IcebergTelemetry.CommitStatusChecked(table.Identifier.ToString(), snapshot, attempt + 1, false, error);
                        throw;
                    }
                    var present = table.Metadata.TableUuid == identities[i] && table.Metadata.SnapshotById(snapshot) is not null;
                    IcebergTelemetry.CommitStatusChecked(table.Identifier.ToString(), snapshot, attempt + 1, present);
                    confirmed &= present;
                }
                return confirmed; // Absence is unknown, not a failed commit. Do not resubmit.
            }
            catch (Exception error)
            {
                lastError = error;
                if (error is OperationCanceledException || attempt >= policy.Retries) break;
            }
            var remaining = policy.TimeoutMs - Stopwatch.GetElapsedTime(started).TotalMilliseconds;
            var delay = Math.Min(policy.MaxWaitMs, policy.MinWaitMs * Math.Pow(2, Math.Min(attempt, 31)));
            if (remaining <= 0 || delay >= remaining) break;
            try { await Task.Delay(TimeSpan.FromMilliseconds(delay), ct); }
            catch (OperationCanceledException error) { lastError = error; break; }
            if (Stopwatch.GetElapsedTime(started).TotalMilliseconds >= policy.TimeoutMs) break;
        }
        throw Unresolved(original, lastError!);
    }

    private static CommitStateUnknownException Unresolved(CommitStateUnknownException original, Exception error) =>
        new($"{original.Message}; commit status remains unknown: {error.Message}", original)
        {
            ServerResponded = original.ServerResponded,
            ResolutionFailure = error,
        };
}
