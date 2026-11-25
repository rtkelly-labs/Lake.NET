using System.Diagnostics;
using System.Globalization;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Data;

public sealed record CommitRetryPolicy(int NumRetries = 4, int MinWaitMs = 100, int MaxWaitMs = 60_000)
{
    public int TotalTimeoutMs { get; init; } = 1_800_000;

    public static CommitRetryPolicy From(TableMetadata metadata)
    {
        int Read(string suffix, int fallback)
        {
            var key = "commit.retry." + suffix;
            if (!int.TryParse(metadata.Property(key, fallback.ToString(CultureInfo.InvariantCulture)), NumberStyles.Integer,
                CultureInfo.InvariantCulture, out var value) || value < 0)
                throw new ArgumentException($"{key} must be a non-negative integer");
            return value;
        }
        var policy = new CommitRetryPolicy(Read("num-retries", 4), Read("min-wait-ms", 100), Read("max-wait-ms", 60_000))
        {
            TotalTimeoutMs = Read("total-timeout-ms", 1_800_000),
        };
        policy.Validate();
        return policy;
    }

    internal void Validate()
    {
        if (NumRetries < 0 || MinWaitMs < 0 || MaxWaitMs < MinWaitMs || TotalTimeoutMs < 0)
            throw new ArgumentException("Commit retry counts/times must be non-negative and max wait must be at least min wait");
    }
}

/// <summary>One monotonic retry budget, passed explicitly through nested planning/producer layers.</summary>
internal sealed class CommitRetryBudget
{
    private readonly CommitRetryPolicy _policy;
    private readonly long _started = Stopwatch.GetTimestamp();
    private int _retries;
    private Exception? _lastConflict;

    internal CommitRetryBudget(CommitRetryPolicy policy)
    {
        policy.Validate();
        _policy = policy;
    }

    internal void CheckAttempt()
    {
        // The initial attempt is always allowed, including when the retry timeout is zero.
        if (!CanStartAttempt)
            throw new CommitFailedException("Commit retry elapsed-time budget exhausted before another attempt could finish planning", _lastConflict);
    }

    internal bool CanStartAttempt => _retries == 0 || Stopwatch.GetElapsedTime(_started).TotalMilliseconds < _policy.TotalTimeoutMs;

    internal async Task<TimeSpan?> RetryAsync(Exception conflict, CancellationToken ct)
    {
        _lastConflict = conflict;
        ct.ThrowIfCancellationRequested();
        var remaining = _policy.TotalTimeoutMs - Stopwatch.GetElapsedTime(_started).TotalMilliseconds;
        if (_retries >= _policy.NumRetries || remaining <= 0) return null;
        var basis = Math.Min(_policy.MaxWaitMs, _policy.MinWaitMs * Math.Pow(2, Math.Min(_retries, 31)));
        var wait = TimeSpan.FromMilliseconds(Math.Min(_policy.MaxWaitMs, basis + Random.Shared.NextDouble() * basis / 10));
        // Do not sleep until the deadline if the next backoff cannot fit inside it.
        if (wait.TotalMilliseconds >= remaining) return null;
        if (wait > TimeSpan.Zero) await Task.Delay(wait, ct);
        if (Stopwatch.GetElapsedTime(_started).TotalMilliseconds >= _policy.TotalTimeoutMs) return null;
        _retries++;
        return wait;
    }
}
