using System.Data.Common;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Traces and metrics of DuckLake commits, scan planning and scans, through the BCL's <see cref="ActivitySource"/> and
/// <see cref="Meter"/> named <see cref="Name"/> (as <c>IcebergTelemetry</c> does for Iceberg tables; docs/telemetry.md).
/// Every span counts the catalog database commands it ran (<see cref="Tags.CatalogCommands"/>): the number that
/// decides a DuckLake's latency against a remote catalog, and the one the performance tests hold constant.
/// </summary>
public static class DuckLakeTelemetry
{
    public const string Name = "Iceberg.Net.DuckLake";

    private static readonly string? Version = typeof(DuckLakeTelemetry).Assembly.GetName().Version?.ToString();

    public static ActivitySource ActivitySource { get; } = new(Name, Version);
    public static Meter Meter { get; } = new(Name, Version);

    public static class Tags
    {
        public const string Table = "ducklake.table";
        public const string SnapshotId = "ducklake.snapshot.id";
        /// <summary>The lake's <c>data_path</c>: tells lakes apart when one process opens several.</summary>
        public const string DataPath = "ducklake.data_path";
        public const string Outcome = "ducklake.outcome";
        public const string CatalogCommands = "ducklake.catalog.commands";
        public const string CommitAttempts = "ducklake.commit.attempts";
        public const string FilesPlanned = "ducklake.scan.files.planned";
        public const string FilesPruned = "ducklake.scan.files.pruned";
        public const string RowsReturned = "ducklake.scan.rows.returned";
        public const string ErrorType = "error.type";
    }

    public static class Outcomes
    {
        public const string Success = "success";
        public const string Conflict = "conflict";
        public const string Unknown = "unknown";
        public const string Cancelled = "cancelled";
        public const string Error = "error";
    }

    private static readonly Counter<long> Commands = Meter.CreateCounter<long>("ducklake.catalog.commands", "{command}", "Catalog database commands, by operation");
    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>("ducklake.operation.duration", "s", "Commit, scan planning and scan time, by operation and outcome");
    private static readonly Counter<long> FilesPlanned = Meter.CreateCounter<long>("ducklake.scan.files.planned", "{file}", "Data files left after scan planning");
    private static readonly Counter<long> FilesPruned = Meter.CreateCounter<long>("ducklake.scan.files.pruned", "{file}", "Data files ruled out by file statistics");
    private static readonly Counter<long> CommitRetries = Meter.CreateCounter<long>("ducklake.commit.retries", "{retry}", "Commits retried after losing the race for the snapshot ID");

    internal static string OutcomeOf(Exception e) => e switch
    {
        DuckLakeConflictException => Outcomes.Conflict,
        DuckLakeCommitStateUnknownException => Outcomes.Unknown,
        OperationCanceledException => Outcomes.Cancelled,
        _ => Outcomes.Error,
    };

    internal static Scope Start(string operation, DuckLakeCatalog catalog, string? table = null) => new(operation, catalog, table);

    /// <summary>One commit, plan or scan: its span, its duration and the catalog commands run on its behalf.</summary>
    internal sealed class Scope : IDisposable
    {
        private readonly long _start = Stopwatch.GetTimestamp();
        private readonly string _operation;
        private string _outcome = Outcomes.Error;
        private bool _disposed;
        public Activity? Activity { get; }
        public long CatalogCommands;

        internal Scope(string operation, DuckLakeCatalog catalog, string? table)
        {
            _operation = operation;
            Activity = ActivitySource.StartActivity(operation);
            if (Activity is null) return;
            Activity.SetTag(Tags.DataPath, catalog.DataPath);
            if (table is not null) Activity.SetTag(Tags.Table, table);
        }

        public void Snapshot(long snapshotId) => Activity?.SetTag(Tags.SnapshotId, snapshotId);

        public void Planned(int files, int pruned)
        {
            Activity?.SetTag(Tags.FilesPlanned, files);
            Activity?.SetTag(Tags.FilesPruned, pruned);
            FilesPlanned.Add(files);
            FilesPruned.Add(pruned);
        }

        public void Retried(int attempts)
        {
            Activity?.SetTag(Tags.CommitAttempts, attempts);
            if (attempts > 1) CommitRetries.Add(attempts - 1);
        }

        public void Rows(long rows) => Activity?.SetTag(Tags.RowsReturned, rows);

        public void Succeeded() => _outcome = Outcomes.Success;

        public void Failed(Exception e)
        {
            _outcome = OutcomeOf(e);
            if (Activity is null) return;
            Activity.SetStatus(ActivityStatusCode.Error, e.Message);
            Activity.SetTag(Tags.ErrorType, e.GetType().FullName);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            var op = new KeyValuePair<string, object?>("ducklake.operation", _operation);
            Commands.Add(CatalogCommands, op);
            Duration.Record((Stopwatch.GetTimestamp() - _start) / (double)Stopwatch.Frequency, op, new(Tags.Outcome, _outcome));
            if (Activity is null) return;
            Activity.SetTag(Tags.CatalogCommands, CatalogCommands);
            Activity.SetTag(Tags.Outcome, _outcome);
            Activity.Dispose();
        }
    }
}

/// <summary>Counts the commands EF Core runs for a context into the scope it was opened for.</summary>
internal sealed class DuckLakeCommandCounter : DbCommandInterceptor
{
    public static readonly DuckLakeCommandCounter Instance = new();

    private static void Count(CommandEventData e)
    {
        if (e.Context is DuckLakeDbContext { Scope: { } scope }) Interlocked.Increment(ref scope.CatalogCommands);
    }

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        Count(eventData);
        return result;
    }

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData,
        InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
    {
        Count(eventData);
        return ValueTask.FromResult(result);
    }

    public override InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        Count(eventData);
        return result;
    }

    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        Count(eventData);
        return ValueTask.FromResult(result);
    }

    public override InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        Count(eventData);
        return result;
    }

    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<object> result,
        CancellationToken cancellationToken = default)
    {
        Count(eventData);
        return ValueTask.FromResult(result);
    }
}
