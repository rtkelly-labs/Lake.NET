using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Iceberg.Net.Diagnostics;

/// <summary>
/// Traces and metrics for scans, commits and catalog calls, on the BCL's <see cref="System.Diagnostics.ActivitySource"/>
/// and <see cref="System.Diagnostics.Metrics.Meter"/> (no OpenTelemetry dependency). Both are named
/// <see cref="Name"/>; wire them up with <c>AddSource("Iceberg.Net")</c> and <c>AddMeter("Iceberg.Net")</c>.
/// Nothing is recorded unless a listener subscribes. See docs/telemetry.md for every span, tag and instrument.
/// </summary>
/// <remarks>Call sites are one line each: start a scope with <c>using var</c>, report results on it.</remarks>
public static class IcebergTelemetry
{
    /// <summary>The <see cref="System.Diagnostics.ActivitySource"/> and <see cref="System.Diagnostics.Metrics.Meter"/> name.</summary>
    public const string Name = "Iceberg.Net";

    private static readonly string? Version = typeof(IcebergTelemetry).Assembly.GetName().Version?.ToString();

    public static ActivitySource ActivitySource { get; } = new(Name, Version);
    public static Meter Meter { get; } = new(Name, Version);

    /// <summary>Span and metric tag names.</summary>
    public static class Tags
    {
        public const string Table = "iceberg.table";
        public const string SnapshotId = "iceberg.snapshot.id";
        public const string Operation = "iceberg.operation";
        public const string Outcome = "iceberg.outcome";
        public const string CommitAttempts = "iceberg.commit.attempts";
        public const string CommitRetries = "iceberg.commit.retries";
        public const string ManifestsRead = "iceberg.scan.manifests.read";
        public const string ManifestsSkipped = "iceberg.scan.manifests.skipped";
        public const string FilesPlanned = "iceberg.scan.files.planned";
        public const string FilesPrunedByPartition = "iceberg.scan.files.pruned.partition";
        public const string FilesPrunedByMetrics = "iceberg.scan.files.pruned.metrics";
        public const string FilesRead = "iceberg.scan.files.read";
        public const string BytesRead = "iceberg.scan.bytes";
        public const string RowsReturned = "iceberg.scan.rows.returned";
        public const string RowsDeleted = "iceberg.scan.rows.deleted";
        public const string PruneReason = "iceberg.prune.reason";
        public const string RowKind = "iceberg.row.kind";
        public const string CatalogName = "iceberg.catalog.name";
        public const string CatalogType = "iceberg.catalog.type";
        public const string CatalogOperation = "iceberg.catalog.operation";
        /// <summary>OpenTelemetry semantic convention: the exception type of a failed operation.</summary>
        public const string ErrorType = "error.type";
    }

    /// <summary>Values of <see cref="Tags.Outcome"/>.</summary>
    public static class Outcomes
    {
        public const string Success = "success";
        /// <summary>The operation found nothing to change, so no snapshot was written.</summary>
        public const string NoOp = "noop";
        /// <summary>An optimistic-concurrency conflict that retries did not resolve (<c>CommitFailedException</c>).</summary>
        public const string Conflict = "conflict";
        /// <summary>The catalog may or may not have applied the commit (<c>CommitStateUnknownException</c>).</summary>
        public const string Unknown = "unknown";
        public const string Cancelled = "cancelled";
        public const string Error = "error";
    }

    private static readonly Counter<long> ManifestsCounter = Meter.CreateCounter<long>("iceberg.scan.manifests", "{manifest}", "Data manifests considered by scan planning, by iceberg.prune.reason (none = read)");
    private static readonly Counter<long> FilesPlannedCounter = Meter.CreateCounter<long>("iceberg.scan.files.planned", "{file}", "Data files left after scan planning");
    private static readonly Counter<long> FilesPrunedCounter = Meter.CreateCounter<long>("iceberg.scan.files.pruned", "{file}", "Data files skipped by scan planning, by iceberg.prune.reason");
    private static readonly Counter<long> BytesCounter = Meter.CreateCounter<long>("iceberg.scan.bytes", "By", "Size of the data files a scan opened");
    private static readonly Counter<long> RowsCounter = Meter.CreateCounter<long>("iceberg.scan.rows", "{row}", "Rows a scan returned or dropped as deleted, by iceberg.row.kind");
    private static readonly Histogram<double> PlanDuration = Meter.CreateHistogram<double>("iceberg.scan.planning.duration", "s", "Scan planning time");
    private static readonly Histogram<double> CommitDuration = Meter.CreateHistogram<double>("iceberg.commit.duration", "s", "Snapshot commit time including retries, by outcome");
    private static readonly Histogram<long> CommitAttempts = Meter.CreateHistogram<long>("iceberg.commit.attempts", "{attempt}", "Attempts a snapshot commit took");
    private static readonly Counter<long> CommitRetries = Meter.CreateCounter<long>("iceberg.commit.retries", "{retry}", "Snapshot commit retries after a conflict");
    private static readonly Histogram<double> CatalogDuration = Meter.CreateHistogram<double>("iceberg.catalog.duration", "s", "Catalog call time, by operation and outcome");

    /// <summary>The <see cref="Tags.Outcome"/> for an exception.</summary>
    public static string OutcomeOf(Exception e) => e switch
    {
        Metadata.CommitFailedException => Outcomes.Conflict,
        Metadata.CommitStateUnknownException => Outcomes.Unknown,
        OperationCanceledException => Outcomes.Cancelled,
        _ => Outcomes.Error,
    };

    /// <summary>Scan planning of one snapshot (span <c>iceberg.scan.plan</c>).</summary>
    public static PlanScope StartScanPlanning(string table, long snapshotId) => new(table, snapshotId);

    /// <summary>A scan reading rows (span <c>iceberg.scan</c>); disposed when the rows have been read or the reader stops.</summary>
    public static ScanScope StartScan(string table, long snapshotId) => new(table, snapshotId);

    /// <summary>A snapshot commit with its retries (span <c>iceberg.commit</c>).</summary>
    public static CommitScope StartCommit(string table, string? operation) => new(table, operation);

    /// <summary>
    /// One catalog call (span <c>iceberg.catalog.{operation}</c>, kind client). Call <see cref="CatalogScope.Succeeded"/>
    /// before returning; a scope disposed without it is recorded as an error (an exception escaped).
    /// </summary>
    public static CatalogScope StartCatalogCall(string catalogName, string catalogType, string operation, string? table = null) =>
        new(catalogName, catalogType, operation, table);

    private static double Seconds(long startTimestamp) => (Stopwatch.GetTimestamp() - startTimestamp) / (double)Stopwatch.Frequency;

    private static void SetError(Activity? activity, string outcome, Exception? e)
    {
        if (activity is null) return;
        activity.SetTag(Tags.Outcome, outcome);
        activity.SetStatus(ActivityStatusCode.Error, e?.Message);
        if (e is not null)
        {
            activity.SetTag(Tags.ErrorType, e.GetType().FullName);
            activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                ["exception.type"] = e.GetType().FullName,
                ["exception.message"] = e.Message,
            }));
        }
    }

    public sealed class PlanScope : IDisposable
    {
        private readonly long _start = Stopwatch.GetTimestamp();
        private readonly string _table;
        public Activity? Activity { get; }

        internal PlanScope(string table, long snapshotId)
        {
            _table = table;
            Activity = ActivitySource.StartActivity("iceberg.scan.plan");
            Activity?.SetTag(Tags.Table, table);
            Activity?.SetTag(Tags.SnapshotId, snapshotId);
        }

        /// <summary>What planning kept and pruned.</summary>
        public void Planned(int manifestsRead, int manifestsSkipped, int filesPlanned, int filesPrunedByPartition, int filesPrunedByMetrics)
        {
            var table = new KeyValuePair<string, object?>(Tags.Table, _table);
            ManifestsCounter.Add(manifestsRead, table, new(Tags.PruneReason, "none"));
            ManifestsCounter.Add(manifestsSkipped, table, new(Tags.PruneReason, "partition_summary"));
            FilesPlannedCounter.Add(filesPlanned, table);
            FilesPrunedCounter.Add(filesPrunedByPartition, table, new(Tags.PruneReason, "partition"));
            FilesPrunedCounter.Add(filesPrunedByMetrics, table, new(Tags.PruneReason, "metrics"));
            if (Activity is null) return;
            Activity.SetTag(Tags.ManifestsRead, manifestsRead);
            Activity.SetTag(Tags.ManifestsSkipped, manifestsSkipped);
            Activity.SetTag(Tags.FilesPlanned, filesPlanned);
            Activity.SetTag(Tags.FilesPrunedByPartition, filesPrunedByPartition);
            Activity.SetTag(Tags.FilesPrunedByMetrics, filesPrunedByMetrics);
        }

        public void Dispose()
        {
            PlanDuration.Record(Seconds(_start), new KeyValuePair<string, object?>(Tags.Table, _table));
            Activity?.Dispose();
        }
    }

    public sealed class ScanScope : IDisposable
    {
        private readonly string _table;
        private long _files, _bytes, _returned, _deleted;
        public Activity? Activity { get; }

        internal ScanScope(string table, long snapshotId)
        {
            _table = table;
            Activity = ActivitySource.StartActivity("iceberg.scan");
            Activity?.SetTag(Tags.Table, table);
            Activity?.SetTag(Tags.SnapshotId, snapshotId);
        }

        /// <summary>A data file is about to be read.</summary>
        public void FileOpened(long sizeInBytes)
        {
            _files++;
            _bytes += sizeInBytes;
            BytesCounter.Add(sizeInBytes, new KeyValuePair<string, object?>(Tags.Table, _table));
        }

        /// <summary>A batch was decoded: <paramref name="returned"/> rows kept, <paramref name="deleted"/> dropped by deletes.</summary>
        public void Rows(int returned, int deleted)
        {
            _returned += returned;
            _deleted += deleted;
            var table = new KeyValuePair<string, object?>(Tags.Table, _table);
            if (returned > 0) RowsCounter.Add(returned, table, new(Tags.RowKind, "returned"));
            if (deleted > 0) RowsCounter.Add(deleted, table, new(Tags.RowKind, "deleted"));
        }

        public void Dispose()
        {
            if (Activity is null) return;
            Activity.SetTag(Tags.FilesRead, _files);
            Activity.SetTag(Tags.BytesRead, _bytes);
            Activity.SetTag(Tags.RowsReturned, _returned);
            Activity.SetTag(Tags.RowsDeleted, _deleted);
            Activity.Dispose();
        }
    }

    /// <summary>
    /// One logical commit. Scopes nest: a commit started while another is open on the same async flow for the same table
    /// (a <c>SnapshotProducer</c> attempt inside a mutation's re-plan loop) reports into the outer one, so a mutation
    /// is one span with every attempt counted, however the retries are layered.
    /// </summary>
    public sealed class CommitScope : IDisposable
    {
        private static readonly AsyncLocal<CommitScope?> Ambient = new();

        private readonly long _start = Stopwatch.GetTimestamp();
        private readonly CommitScope? _outer, _previous;
        private readonly string _table;
        private string? _operation;
        private int _retries;
        private string? _outcome;
        private Exception? _error;
        public Activity? Activity { get; }

        internal CommitScope(string table, string? operation)
        {
            _table = table;
            _previous = Ambient.Value;
            if (_previous is { } outer && outer._table == table)
            {
                _outer = outer;
                outer._operation ??= operation;
                Activity = outer.Activity;
                return;
            }
            _operation = operation;
            Ambient.Value = this;
            Activity = ActivitySource.StartActivity("iceberg.commit");
            Activity?.SetTag(Tags.Table, table);
        }

        /// <summary>Attempt <paramref name="attempt"/> (1-based) lost a race; the next starts after <paramref name="backoff"/>.</summary>
        public void Retrying(int attempt, Exception conflict, TimeSpan? backoff = null)
        {
            if (_outer is { } outer) { outer.Retrying(attempt, conflict, backoff); return; }
            _retries++;
            CommitRetries.Add(1, new KeyValuePair<string, object?>(Tags.Table, _table), new KeyValuePair<string, object?>(Tags.Operation, _operation));
            var tags = new ActivityTagsCollection
            {
                ["iceberg.commit.attempt"] = _retries,
                [Tags.ErrorType] = conflict.GetType().FullName,
            };
            if (backoff is { } wait) tags["iceberg.commit.backoff_ms"] = (long)wait.TotalMilliseconds;
            Activity?.AddEvent(new ActivityEvent("iceberg.commit.retry", tags: tags));
        }

        /// <summary>The snapshot was committed.</summary>
        public void Committed(long snapshotId, string? operation)
        {
            if (_outer is { } outer) { outer.Committed(snapshotId, operation); return; }
            (_outcome, _error) = (Outcomes.Success, null);
            _operation ??= operation;
            Activity?.SetTag(Tags.SnapshotId, snapshotId);
        }

        /// <summary>There was nothing to commit (e.g. a delete that matched no rows).</summary>
        public void NothingToCommit()
        {
            if (_outer is { } outer) { outer.NothingToCommit(); return; }
            (_outcome, _error) = (Outcomes.NoOp, null);
        }

        /// <summary>
        /// Record that the commit failed with <paramref name="e"/>. Always returns false, so it can sit in an exception
        /// filter (<c>catch (Exception e) when (scope.FailedWith(e))</c>) without catching anything. An outer scope that
        /// retries afterwards overrides it.
        /// </summary>
        public bool FailedWith(Exception e)
        {
            if (_outer is { } outer) return outer.FailedWith(e);
            if (_outcome is Outcomes.Success) return false; // committed; a later cleanup error does not undo that
            (_outcome, _error) = (OutcomeOf(e), e);
            return false;
        }

        public void Dispose()
        {
            if (_outer is not null) return;
            Ambient.Value = _previous;
            var outcome = _outcome ?? Outcomes.Error;
            var tags = new TagList
            {
                { Tags.Table, _table },
                { Tags.Operation, _operation },
                { Tags.Outcome, outcome },
            };
            CommitDuration.Record(Seconds(_start), tags);
            CommitAttempts.Record(_retries + 1, tags);
            if (Activity is null) return;
            if (_operation is not null) Activity.SetTag(Tags.Operation, _operation);
            Activity.SetTag(Tags.CommitAttempts, _retries + 1);
            Activity.SetTag(Tags.CommitRetries, _retries);
            if (outcome is Outcomes.Success or Outcomes.NoOp)
            {
                Activity.SetTag(Tags.Outcome, outcome);
                Activity.SetStatus(ActivityStatusCode.Ok);
            }
            else SetError(Activity, outcome, _error);
            Activity.Dispose();
        }
    }

    public sealed class CatalogScope : IDisposable
    {
        private readonly long _start = Stopwatch.GetTimestamp();
        private readonly string _catalog, _type, _operation;
        private string? _outcome;
        public Activity? Activity { get; }

        internal CatalogScope(string catalog, string type, string operation, string? table)
        {
            (_catalog, _type, _operation) = (catalog, type, operation);
            Activity = ActivitySource.StartActivity("iceberg.catalog." + operation, ActivityKind.Client);
            if (Activity is null) return;
            Activity.SetTag(Tags.CatalogName, catalog);
            Activity.SetTag(Tags.CatalogType, type);
            Activity.SetTag(Tags.CatalogOperation, operation);
            if (table is not null) Activity.SetTag(Tags.Table, table);
        }

        /// <summary>The call returned normally.</summary>
        public void Succeeded()
        {
            _outcome = Outcomes.Success;
            Activity?.SetStatus(ActivityStatusCode.Ok);
        }

        /// <summary>The call returned <paramref name="result"/>; returns it, for <c>return scope.Succeeded(value);</c>.</summary>
        public T Succeeded<T>(T result)
        {
            Succeeded();
            return result;
        }

        /// <summary>As <see cref="CommitScope.FailedWith"/>: records the failure and returns false.</summary>
        public bool FailedWith(Exception e)
        {
            _outcome = OutcomeOf(e);
            SetError(Activity, _outcome, e);
            return false;
        }

        public void Dispose()
        {
            var outcome = _outcome ?? Outcomes.Error;
            if (_outcome is null) SetError(Activity, outcome, null);
            CatalogDuration.Record(Seconds(_start), new TagList
            {
                { Tags.CatalogName, _catalog },
                { Tags.CatalogType, _type },
                { Tags.CatalogOperation, _operation },
                { Tags.Outcome, outcome },
            });
            Activity?.Dispose();
        }
    }
}
