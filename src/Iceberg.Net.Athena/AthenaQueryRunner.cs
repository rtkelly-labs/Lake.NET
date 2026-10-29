using Amazon.Athena;
using Amazon.Athena.Model;
using System.Diagnostics;

namespace Iceberg.Net.Athena;

/// <summary>Where Athena runs queries: a workgroup, an output location (unless the workgroup enforces one) and the default database.</summary>
public sealed record AthenaQueryOptions
{
    public string? WorkGroup { get; init; }
    public string? OutputLocation { get; init; }
    /// <summary>Data catalog for unqualified names (default <c>AwsDataCatalog</c>).</summary>
    public string? Catalog { get; init; }
    public string? Database { get; init; }
    /// <summary>First poll interval; it doubles up to <see cref="MaxPollInterval"/>.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(500);
    public TimeSpan MaxPollInterval { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(30);
}

/// <summary>A finished query: its rows as strings (Athena's result format), column names, and what it scanned.</summary>
public sealed record AthenaQueryResult(string QueryExecutionId, IReadOnlyList<string> Columns, IReadOnlyList<IReadOnlyList<string?>> Rows, long? DataScannedBytes,
    long? EngineExecutionTimeMs);

public sealed class AthenaQueryFailedException(string queryExecutionId, string state, string? reason, string sql)
    : Exception($"Athena query {queryExecutionId} {state}: {reason}\n{sql}")
{
    public string QueryExecutionId { get; } = queryExecutionId;
    public string State { get; } = state;
    public string? Reason { get; } = reason;
    public int? ErrorCategory { get; init; }
    public int? ErrorType { get; init; }
    /// <summary>A service hint that a new execution might succeed, not permission to replay a mutation.</summary>
    public bool? Retryable { get; init; }
    public string? ErrorMessage { get; init; }
}

/// <summary>Submission may have reached Athena. Recover using the same request token and identical parameters.</summary>
public sealed class AthenaSubmissionUnknownException(string token, Exception inner)
    : Exception($"Athena submission outcome is unknown; recover with client request token {token}", inner)
{
    public string ClientRequestToken { get; } = token;
}

/// <summary>A local wait expired; the server-side execution may still be running.</summary>
public sealed class AthenaQueryTimedOutException(string id, TimeSpan timeout)
    : TimeoutException($"Athena query {id} did not finish within {timeout}; resume polling this execution")
{
    public string QueryExecutionId { get; } = id;
}

/// <summary>
/// Runs Athena SQL (<c>StartQueryExecution</c>, then <c>GetQueryExecution</c> with backoff until it finishes, then every
/// page of <c>GetQueryResults</c>). DDL, OPTIMIZE and VACUUM return no rows.
/// </summary>
public sealed class AthenaQueryRunner(IAmazonAthena athena, AthenaQueryOptions options)
{
    private void ValidatePollingOptions()
    {
        if (options.PollInterval < TimeSpan.Zero || options.MaxPollInterval < options.PollInterval || options.Timeout < TimeSpan.Zero)
            throw new ArgumentException("Athena poll intervals and timeout must be non-negative, and max poll interval must be at least the initial interval");
    }

    public Task<string> StartAsync(string sql, CancellationToken ct = default) => StartAsync(sql, Guid.NewGuid().ToString("N"), ct);

    /// <summary>Submit or recover the same submission; persist the token and all request parameters before calling.</summary>
    public async Task<string> StartAsync(string sql, string clientRequestToken, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sql);
        ValidatePollingOptions(); // Do not submit SQL that the configured runner cannot subsequently wait for.
        if (clientRequestToken is null || clientRequestToken.Length is < 32 or > 128)
            throw new ArgumentException("Athena client request tokens must contain 32 to 128 characters", nameof(clientRequestToken));
        ct.ThrowIfCancellationRequested();
        var request = new StartQueryExecutionRequest
        {
            QueryString = sql,
            WorkGroup = options.WorkGroup,
            ClientRequestToken = clientRequestToken,
            ResultConfiguration = options.OutputLocation is null ? null : new ResultConfiguration { OutputLocation = options.OutputLocation },
            QueryExecutionContext = options.Database is null && options.Catalog is null ? null
                : new QueryExecutionContext { Database = options.Database, Catalog = options.Catalog },
        };
        try { return (await athena.StartQueryExecutionAsync(request, ct)).QueryExecutionId; }
        catch (Exception error) when (error is IOException or HttpRequestException or TimeoutException or OperationCanceledException
            or Amazon.Runtime.AmazonUnmarshallingException
            || error is AmazonAthenaException service && ((int)service.StatusCode is >= 500 or 429 || (int)service.StatusCode == 0))
        {
            throw new AthenaSubmissionUnknownException(clientRequestToken, error);
        }
    }

    /// <summary>Wait for a query to finish; throws <see cref="AthenaQueryFailedException"/> if it failed or was cancelled.</summary>
    public async Task<QueryExecution> WaitAsync(string queryExecutionId, string sql = "", CancellationToken ct = default)
    {
        ValidatePollingOptions();
        var started = Stopwatch.GetTimestamp();
        var delay = options.PollInterval;
        var firstPoll = true;
        while (true)
        {
            if (!firstPoll && Stopwatch.GetElapsedTime(started) >= options.Timeout)
                throw new AthenaQueryTimedOutException(queryExecutionId, options.Timeout);
            firstPoll = false;
            var execution = (await athena.GetQueryExecutionAsync(new GetQueryExecutionRequest { QueryExecutionId = queryExecutionId }, ct)).QueryExecution;
            var state = execution.Status.State;
            if (state == QueryExecutionState.SUCCEEDED) return execution;
            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED)
                throw new AthenaQueryFailedException(queryExecutionId, state.Value, execution.Status.StateChangeReason, sql)
                {
                    ErrorCategory = execution.Status.AthenaError?.ErrorCategory,
                    ErrorType = execution.Status.AthenaError?.ErrorType,
                    Retryable = execution.Status.AthenaError?.Retryable,
                    ErrorMessage = execution.Status.AthenaError?.ErrorMessage,
                };
            var remaining = options.Timeout - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero) throw new AthenaQueryTimedOutException(queryExecutionId, options.Timeout);
            await Task.Delay(TimeSpan.FromTicks(Math.Min(Math.Min(delay.Ticks, remaining.Ticks), TimeSpan.FromMilliseconds(int.MaxValue).Ticks)), ct);
            delay = TimeSpan.FromTicks(delay.Ticks + Math.Min(delay.Ticks, options.MaxPollInterval.Ticks - delay.Ticks));
        }
    }

    /// <summary>Run <paramref name="sql"/> to completion and read all its rows.</summary>
    public async Task<AthenaQueryResult> RunAsync(string sql, CancellationToken ct = default)
    {
        var id = await StartAsync(sql, ct);
        return await ResumeAsync(id, sql, ct);
    }

    /// <summary>Execute with a durable submission identity; this does not resubmit a failed execution.</summary>
    public async Task<AthenaQueryResult> RunAsync(string sql, string clientRequestToken, CancellationToken ct = default) =>
        await ResumeAsync(await StartAsync(sql, clientRequestToken, ct), sql, ct);

    /// <summary>Resume an existing execution after a restart or local timeout; never starts another query.</summary>
    public async Task<AthenaQueryResult> ResumeAsync(string id, string sql = "", CancellationToken ct = default)
    {
        var execution = await WaitAsync(id, sql, ct);
        var columns = new List<string>();
        var rows = new List<IReadOnlyList<string?>>();
        string? token = null;
        var first = true;
        do
        {
            var page = await athena.GetQueryResultsAsync(new GetQueryResultsRequest { QueryExecutionId = id, NextToken = token }, ct);
            if (first && page.ResultSet?.ResultSetMetadata?.ColumnInfo is { } info) columns.AddRange(info.Select(c => c.Name));
            var pageRows = page.ResultSet?.Rows ?? [];
            // A query's (StatementType DML: SELECT, INSERT, ...) first row repeats the column names; DDL and UTILITY
            // results (SHOW, DESCRIBE, ...) have none, so a first row that merely equals the names is data there.
            var skip = first && execution.StatementType != StatementType.DDL && execution.StatementType != StatementType.UTILITY && pageRows.Count > 0
                && columns.Count > 0 && Cells(pageRows[0]).SequenceEqual(columns) ? 1 : 0;
            rows.AddRange(pageRows.Skip(skip).Select(r => (IReadOnlyList<string?>)Cells(r).ToList()));
            token = page.NextToken;
            first = false;
        } while (!string.IsNullOrEmpty(token));
        return new AthenaQueryResult(id, columns, rows, execution.Statistics?.DataScannedInBytes, execution.Statistics?.EngineExecutionTimeInMillis);
    }

    // A NULL is a datum without VarCharValue; the SDK leaves an empty row's Data list null.
    private static IEnumerable<string?> Cells(Row row) => (row.Data ?? []).Select(d => d.VarCharValue);
}
