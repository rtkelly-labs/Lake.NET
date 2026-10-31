using System.Globalization;
using Iceberg.Net.IO;

namespace Lake.Delta;

/// <summary>When a commit happened, and where that time came from.</summary>
public sealed record DeltaCommitTimestamp(long Version, DateTimeOffset Timestamp, bool IsInCommitTimestamp);

/// <summary>
/// A Delta table on any <see cref="IFileIO"/> that can list. Read-only: snapshots at a version or a timestamp, and their
/// rows. Writes are not implemented yet.
/// </summary>
public sealed class DeltaTable
{
    private readonly IFileIO _io;

    public DeltaTable(string location, IFileIO io)
    {
        ArgumentException.ThrowIfNullOrEmpty(location);
        Location = DeltaPaths.NormalizeRoot(location);
        _io = io ?? throw new ArgumentNullException(nameof(io));
    }

    /// <summary>A table on the local filesystem.</summary>
    public static DeltaTable OpenLocal(string path) => new(path, new LocalFileIO());

    public string Location { get; }

    /// <summary>The snapshot at <paramref name="version"/>, or the latest when null.</summary>
    public async Task<DeltaSnapshot> GetSnapshotAsync(long? version = null, CancellationToken ct = default)
    {
        var listing = await DeltaLog.ListAsync(_io, Location, ct).ConfigureAwait(false);
        await DeltaLog.AddLastCheckpointHintAsync(_io, Location, listing, ct).ConfigureAwait(false);
        var latest = Latest(listing);
        var target = version ?? latest;
        if (target < 0 || target > latest)
            throw new DeltaVersionNotFoundException($"Version {target} of {Location} does not exist (latest is {latest})");

        // The newest complete checkpoint not after the target, with every later commit up to the target present.
        var checkpoint = listing.Checkpoints.Where(c => c.Version <= target).OrderByDescending(c => c.Version)
            .ThenBy(c => c.Kind == "multi-part" ? 1 : 0).FirstOrDefault();
        var from = checkpoint is null ? 0 : checkpoint.Version + 1;
        for (var v = from; v <= target; v++)
            if (!listing.Commits.ContainsKey(v))
                throw new DeltaVersionNotFoundException(checkpoint is null
                    ? $"Version {target} of {Location} cannot be reconstructed: commit {v} is missing and no checkpoint precedes it"
                    : $"Version {target} of {Location} cannot be reconstructed: commit {v} after checkpoint {checkpoint.Version} is missing");

        var state = new ReplayState();
        if (checkpoint is not null)
            state.ApplyVersion(await DeltaLog.ReadCheckpointAsync(_io, Location, checkpoint, ct).ConfigureAwait(false), $"checkpoint {checkpoint.Version}");
        DeltaCommitInfo? commitInfo = null;
        for (var v = from; v <= target; v++)
        {
            var actions = await DeltaLog.ReadJsonActionsAsync(_io, listing.Commits[v].Location, ct).ConfigureAwait(false);
            state.ApplyVersion(actions, $"commit {v}");
            if (v == target) commitInfo = actions.OfType<DeltaCommitInfo>().FirstOrDefault();
        }
        if (commitInfo is null && listing.Commits.TryGetValue(target, out var own))
            commitInfo = (await DeltaLog.ReadJsonActionsAsync(_io, own.Location, ct).ConfigureAwait(false)).OfType<DeltaCommitInfo>().FirstOrDefault();
        return new DeltaSnapshot(_io, Location, target, state, commitInfo);
    }

    private long Latest(LogListing listing)
    {
        var latest = Math.Max(listing.Commits.Count > 0 ? listing.Commits.Keys.Max() : -1, listing.Checkpoints.Count > 0 ? listing.Checkpoints.Max(c => c.Version) : -1);
        return latest >= 0 ? latest : throw new FileNotFoundException($"No Delta table at {Location}: _delta_log has no commits or checkpoints", DeltaPaths.LogDirectory(Location));
    }

    /// <summary>
    /// Each commit's time, oldest first, for the commits whose JSON is still in the log. With in-commit timestamps
    /// (<c>delta.enableInCommitTimestamps=true</c>), versions from <c>delta.inCommitTimestampEnablementVersion</c> on use
    /// <c>commitInfo.inCommitTimestamp</c>; earlier ones (and every commit otherwise) use the log file's modification time,
    /// made strictly increasing (impl: delta-spark does the same, the protocol leaves it open).
    /// </summary>
    public async Task<IReadOnlyList<DeltaCommitTimestamp>> GetCommitTimestampsAsync(CancellationToken ct = default)
    {
        var latest = await GetSnapshotAsync(null, ct).ConfigureAwait(false);
        var (ict, enablementVersion, _) = InCommitTimestamps(latest.Metadata);
        var listing = await DeltaLog.ListAsync(_io, Location, ct).ConfigureAwait(false);
        var result = new List<DeltaCommitTimestamp>(listing.Commits.Count);
        DateTimeOffset? previousFileTime = null;
        foreach (var (version, file) in listing.Commits)
        {
            if (ict && version >= enablementVersion)
            {
                var info = (await DeltaLog.ReadJsonActionsAsync(_io, file.Location, ct).ConfigureAwait(false)).OfType<DeltaCommitInfo>().FirstOrDefault();
                var ms = info?.InCommitTimestamp ?? throw new InvalidDataException($"Commit {version} has no commitInfo.inCommitTimestamp although in-commit timestamps are enabled from version {enablementVersion}");
                result.Add(new DeltaCommitTimestamp(version, DateTimeOffset.FromUnixTimeMilliseconds(ms), true));
            }
            else
            {
                var time = new DateTimeOffset(DateTime.SpecifyKind(file.LastModifiedUtc, DateTimeKind.Utc));
                time = new DateTimeOffset(time.UtcTicks - time.UtcTicks % TimeSpan.TicksPerMillisecond, TimeSpan.Zero);
                if (previousFileTime is { } p && time <= p) time = p.AddMilliseconds(1);
                previousFileTime = time;
                result.Add(new DeltaCommitTimestamp(version, time, false));
            }
        }
        return result;
    }

    /// <summary>
    /// The snapshot as of <paramref name="timestamp"/>: the latest version committed at or before it. With in-commit
    /// timestamps enabled part-way, a time at or after the enablement timestamp considers only versions from the
    /// enablement version on, and an earlier one only versions before it (PROTOCOL.md "In-Commit Timestamps").
    /// </summary>
    public async Task<DeltaSnapshot> GetSnapshotAsOfAsync(DateTimeOffset timestamp, CancellationToken ct = default)
    {
        var latest = await GetSnapshotAsync(null, ct).ConfigureAwait(false);
        var (ict, enablementVersion, enablementTimestamp) = InCommitTimestamps(latest.Metadata);
        IEnumerable<DeltaCommitTimestamp> candidates = await GetCommitTimestampsAsync(ct).ConfigureAwait(false);
        if (ict && enablementVersion > 0 && enablementTimestamp is { } since)
            candidates = timestamp >= since ? candidates.Where(c => c.Version >= enablementVersion) : candidates.Where(c => c.Version < enablementVersion);
        var match = candidates.Where(c => c.Timestamp <= timestamp).Select(c => (long?)c.Version).Max()
            ?? throw new DeltaVersionNotFoundException($"No version of {Location} was committed at or before {timestamp:O}");
        return match == latest.Version ? latest : await GetSnapshotAsync(match, ct).ConfigureAwait(false);
    }

    private static (bool Enabled, long EnablementVersion, DateTimeOffset? EnablementTimestamp) InCommitTimestamps(DeltaMetadata metadata)
    {
        var c = metadata.Configuration;
        if (!string.Equals(c.GetValueOrDefault("delta.enableInCommitTimestamps"), "true", StringComparison.OrdinalIgnoreCase)) return (false, 0, null);
        long version = 0;
        DateTimeOffset? since = null;
        if (c.TryGetValue("delta.inCommitTimestampEnablementVersion", out var v)
            && !long.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out version))
            throw new InvalidDataException($"delta.inCommitTimestampEnablementVersion '{v}' is not a number");
        if (c.TryGetValue("delta.inCommitTimestampEnablementTimestamp", out var t))
            since = long.TryParse(t, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ms)
                ? DateTimeOffset.FromUnixTimeMilliseconds(ms)
                : throw new InvalidDataException($"delta.inCommitTimestampEnablementTimestamp '{t}' is not a number");
        return (true, version, since);
    }

    /// <summary>The rows of the snapshot at <paramref name="version"/> (latest when null).</summary>
    public async IAsyncEnumerable<object?[]> ReadAsync(long? version = null, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var snapshot = await GetSnapshotAsync(version, ct).ConfigureAwait(false);
        await foreach (var row in snapshot.ReadAsync(ct).ConfigureAwait(false)) yield return row;
    }
}
