using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Npgsql;

namespace Iceberg.Net.DuckLake.TestSupport;

/// <summary>Where <see cref="CatalogFaults"/> throws.</summary>
public enum FaultPoint
{
    /// <summary>Before the Nth catalog command (counted from arming) runs.</summary>
    BeforeCommand,
    /// <summary>After the Nth catalog command ran (its effects are in the open transaction).</summary>
    AfterCommand,
    /// <summary>Before <c>COMMIT</c> is sent: nothing is committed.</summary>
    BeforeCommit,
    /// <summary>After <c>COMMIT</c> succeeded: the commit landed but its acknowledgement is lost.</summary>
    AfterCommit,
}

/// <summary>
/// Fault injection for a lake's catalog database (gap D4), through EF Core's interceptors on the store's options hook
/// (<see cref="DuckLakeStore.Sqlite"/>'s <c>configure</c>, or <see cref="TestLake.ConfigureStore"/>). Armed, it counts
/// the catalog commands EF Core runs and the transactions it commits, and throws at the chosen point, as many times as
/// asked. Statements the library sends on EF's connection itself (multi-row inserts, inlined tables) are not counted:
/// faults land between them, around the EF commands that read and update the catalog in the same transaction.
/// </summary>
public sealed class CatalogFaults : IDbCommandInterceptor, IDbTransactionInterceptor
{
    private readonly object _lock = new();
    private FaultPoint? _point;
    private int _n;
    private int _remaining;
    private Func<Exception> _error = Generic;

    /// <summary>Catalog commands seen since arming.</summary>
    public int Commands { get; private set; }
    /// <summary>Commits attempted since arming.</summary>
    public int Commits { get; private set; }
    /// <summary>Faults thrown since arming.</summary>
    public int Fired { get; private set; }

    /// <summary>Throw <paramref name="error"/> (default <see cref="Generic"/>) at <paramref name="point"/>, <paramref name="times"/> times.</summary>
    public void Arm(FaultPoint point, int command = 1, Func<Exception>? error = null, int times = 1)
    {
        lock (_lock)
        {
            _point = point;
            _n = command;
            _remaining = times;
            _error = error ?? Generic;
            Commands = Commits = Fired = 0;
        }
    }

    public void Disarm()
    {
        lock (_lock) _point = null;
    }

    /// <summary>A database error that is not transient (a constraint, a lost connection the provider does not flag).</summary>
    public static Exception Generic() => new InjectedDbException("injected catalog failure");

    /// <summary>SQLite's <c>SQLITE_BUSY</c> ("database is locked").</summary>
    public static Exception SqliteBusy() => new SqliteException("database is locked", 5);

    /// <summary>PostgreSQL's <c>40001</c> serialization failure.</summary>
    public static Exception SerializationFailure() =>
        new PostgresException("could not serialize access due to concurrent update", "ERROR", "ERROR", PostgresErrorCodes.SerializationFailure);

    private void Command(bool before)
    {
        Exception? fault = null;
        lock (_lock)
        {
            if (_point is not { } point || _remaining == 0) return;
            if (before) Commands++;
            var hit = point == (before ? FaultPoint.BeforeCommand : FaultPoint.AfterCommand) && Commands >= _n;
            if (hit)
            {
                _remaining--;
                Fired++;
                fault = _error();
            }
        }
        if (fault is not null) throw fault;
    }

    private void Commit(bool before)
    {
        Exception? fault = null;
        lock (_lock)
        {
            if (_point is not { } point || _remaining == 0) return;
            if (before) Commits++;
            if (point == (before ? FaultPoint.BeforeCommit : FaultPoint.AfterCommit))
            {
                _remaining--;
                Fired++;
                fault = _error();
            }
        }
        if (fault is not null) throw fault;
    }

    // ---- commands ----------------------------------------------------------------------------------------------------

    public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        Command(before: true);
        return result;
    }

    public ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        Command(before: true);
        return ValueTask.FromResult(result);
    }

    public DbDataReader ReaderExecuted(DbCommand command, CommandExecutedEventData eventData, DbDataReader result)
    {
        After(result);
        return result;
    }

    public ValueTask<DbDataReader> ReaderExecutedAsync(DbCommand command, CommandExecutedEventData eventData, DbDataReader result, CancellationToken cancellationToken = default)
    {
        After(result);
        return ValueTask.FromResult(result);
    }

    private void After(IDisposable? result)
    {
        try
        {
            Command(before: false);
        }
        catch
        {
            result?.Dispose();
            throw;
        }
    }

    public InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        Command(before: true);
        return result;
    }

    public ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        Command(before: true);
        return ValueTask.FromResult(result);
    }

    public int NonQueryExecuted(DbCommand command, CommandExecutedEventData eventData, int result)
    {
        Command(before: false);
        return result;
    }

    public ValueTask<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        Command(before: false);
        return ValueTask.FromResult(result);
    }

    public InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        Command(before: true);
        return result;
    }

    public ValueTask<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<object> result,
        CancellationToken cancellationToken = default)
    {
        Command(before: true);
        return ValueTask.FromResult(result);
    }

    public object? ScalarExecuted(DbCommand command, CommandExecutedEventData eventData, object? result)
    {
        Command(before: false);
        return result;
    }

    public ValueTask<object?> ScalarExecutedAsync(DbCommand command, CommandExecutedEventData eventData, object? result, CancellationToken cancellationToken = default)
    {
        Command(before: false);
        return ValueTask.FromResult(result);
    }

    // ---- transactions ------------------------------------------------------------------------------------------------

    public InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        Commit(before: true);
        return result;
    }

    public ValueTask<InterceptionResult> TransactionCommittingAsync(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result,
        CancellationToken cancellationToken = default)
    {
        Commit(before: true);
        return ValueTask.FromResult(result);
    }

    public void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) => Commit(before: false);

    public Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
    {
        Commit(before: false);
        return Task.CompletedTask;
    }

    private sealed class InjectedDbException(string message) : DbException(message);
}
