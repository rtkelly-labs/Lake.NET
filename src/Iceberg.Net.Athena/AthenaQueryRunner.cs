using Amazon.Athena;
using Amazon.Athena.Model;

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
}

/// <summary>
/// Runs Athena SQL (<c>StartQueryExecution</c>, then <c>GetQueryExecution</c> with backoff until it finishes, then every
/// page of <c>GetQueryResults</c>). DDL, OPTIMIZE and VACUUM return no rows.
/// </summary>
public sealed class AthenaQueryRunner(IAmazonAthena athena, AthenaQueryOptions options)
{
    public async Task<string> StartAsync(string sql, CancellationToken ct = default)
    {
        var request = new StartQueryExecutionRequest
        {
            QueryString = sql,
            WorkGroup = options.WorkGroup,
            ClientRequestToken = Guid.NewGuid().ToString("N"),
            ResultConfiguration = options.OutputLocation is null ? null : new ResultConfiguration { OutputLocation = options.OutputLocation },
            QueryExecutionContext = options.Database is null && options.Catalog is null ? null
                : new QueryExecutionContext { Database = options.Database, Catalog = options.Catalog },
        };
        return (await athena.StartQueryExecutionAsync(request, ct)).QueryExecutionId;
    }

    /// <summary>Wait for a query to finish; throws <see cref="AthenaQueryFailedException"/> if it failed or was cancelled.</summary>
    public async Task<QueryExecution> WaitAsync(string queryExecutionId, string sql = "", CancellationToken ct = default)
    {
        var deadline = DateTimeOffset.UtcNow + options.Timeout;
        var delay = options.PollInterval;
        while (true)
        {
            var execution = (await athena.GetQueryExecutionAsync(new GetQueryExecutionRequest { QueryExecutionId = queryExecutionId }, ct)).QueryExecution;
            var state = execution.Status.State;
            if (state == QueryExecutionState.SUCCEEDED) return execution;
            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED)
                throw new AthenaQueryFailedException(queryExecutionId, state.Value, execution.Status.StateChangeReason, sql);
            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException($"Athena query {queryExecutionId} still {state} after {options.Timeout}");
            await Task.Delay(delay, ct);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, options.MaxPollInterval.Ticks));
        }
    }

    /// <summary>Run <paramref name="sql"/> to completion and read all its rows.</summary>
    public async Task<AthenaQueryResult> RunAsync(string sql, CancellationToken ct = default)
    {
        var id = await StartAsync(sql, ct);
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
            // A SELECT's first row repeats the column names (DDL and DML results have no header).
            var skip = first && pageRows.Count > 0 && columns.Count > 0 && pageRows[0].Data.Select(d => d.VarCharValue).SequenceEqual(columns) ? 1 : 0;
            rows.AddRange(pageRows.Skip(skip).Select(r => (IReadOnlyList<string?>)r.Data.Select(d => (string?)d.VarCharValue).ToList()));
            token = page.NextToken;
            first = false;
        } while (!string.IsNullOrEmpty(token));
        return new AthenaQueryResult(id, columns, rows, execution.Statistics?.DataScannedInBytes, execution.Statistics?.EngineExecutionTimeInMillis);
    }
}
