using System.Data.Common;
using Microsoft.Data.Sqlite;
using Npgsql;

namespace Iceberg.Net.DuckLake;

/// <summary>Which catalog database failures are worth retrying.</summary>
internal static class DuckLakeFaults
{
    /// <summary>SQLite <c>SQLITE_BUSY</c> and <c>SQLITE_LOCKED</c>: another connection holds the lock.</summary>
    private const int SqliteBusy = 5, SqliteLocked = 6;

    /// <summary>
    /// A failure that left nothing behind and may pass on a second try: SQLite <c>BUSY</c>/<c>LOCKED</c>, PostgreSQL
    /// <c>40001</c> (serialization failure) and <c>40P01</c> (deadlock), or a provider's own transient flag (a dropped
    /// connection). Looked for through wrapping exceptions (EF Core's <c>DbUpdateException</c>).
    /// </summary>
    public static bool IsTransient(Exception? e)
    {
        for (var depth = 0; e is not null && depth < 8; e = e.InnerException, depth++)
        {
            switch (e)
            {
                case SqliteException { SqliteErrorCode: SqliteBusy or SqliteLocked }:
                case PostgresException { SqlState: PostgresErrorCodes.SerializationFailure or PostgresErrorCodes.DeadlockDetected }:
                    return true;
                case DbException { IsTransient: true }:
                    return true;
            }
        }
        return false;
    }

    /// <summary>Wait before retry <paramref name="attempt"/>: exponential with jitter, so writers that collided do not collide again in step.</summary>
    public static TimeSpan Backoff(DuckLakeOptions o, int attempt)
    {
        var ms = Math.Min(o.MaxRetryWait.TotalMilliseconds, o.RetryWait.TotalMilliseconds * Math.Pow(1.5, attempt));
        return TimeSpan.FromMilliseconds(ms * (0.5 + Random.Shared.NextDouble()));
    }
}
