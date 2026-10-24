using Amazon.Athena;
using Amazon.Athena.Model;
using Iceberg.Net.Athena;
using Iceberg.Net.IO;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// <see cref="AthenaQueryRunner"/> with the real AWS SDK client against moto's Athena: the request it sends (as moto
/// records it), polling through QUEUED and RUNNING (moto's state manager) — moto refuses GetQueryResults before
/// SUCCEEDED, as Athena does — reading results moto was given (with Athena's header row and NULLs), across pages
/// when moto runs the paging shim, and a stopped query. moto executes no SQL; what Athena's engine makes of the
/// generated SQL is <see cref="AthenaSqlOnTrinoTests"/>' job (and AWS-mode <see cref="AthenaTests"/>').
/// </summary>
[Spec("Scan Planning")]
[Trait("Category", "Regression")]
[Collection(MotoServiceStateCollection.Name)]
public sealed class AthenaRunnerEmulationTests
{
    private static readonly AthenaTable Events = new("analytics", "events");
    private readonly string _output = $"{TestEnvironment.Current.Warehouse.TrimEnd('/')}/athena-results/{TestEnvironment.Current.RunId}/";

    private static void RequireMoto()
    {
        Skip.If(TestEnvironment.Current.Target != Target.Emulation, "moto's Athena is emulation only (AthenaTests run the real service)");
        Require.Target();
    }

    /// <summary>The SDK client, counting GetQueryResults pages.</summary>
    private sealed class CountingAthena : AmazonAthenaClient
    {
        public CountingAthena() : base(AwsServiceClients.AthenaConfig()) { }
        public int ResultPages { get; private set; }

        public override Task<GetQueryResultsResponse> GetQueryResultsAsync(GetQueryResultsRequest request, CancellationToken ct = default)
        {
            ResultPages++;
            return base.GetQueryResultsAsync(request, ct);
        }
    }

    private AthenaQueryRunner Runner(IAmazonAthena athena) => new(athena, new AthenaQueryOptions
    {
        WorkGroup = "primary", OutputLocation = _output, Catalog = "AwsDataCatalog", Database = "analytics",
        PollInterval = TimeSpan.FromMilliseconds(10), MaxPollInterval = TimeSpan.FromMilliseconds(40), Timeout = TimeSpan.FromMinutes(1),
    });

    [SkippableFact]
    public async Task Runner_waits_for_the_query_and_reads_every_row_without_the_header()
    {
        RequireMoto();
        const int count = 2500; // two and a half of Athena's 1000-row pages
        var expected = Enumerable.Range(0, count).Select(i => (IReadOnlyList<string?>)[i.ToString(System.Globalization.CultureInfo.InvariantCulture), i % 9 == 0 ? null : $"name, \"{i}\""]).ToList();
        var paging = (await MotoApi.ShimsAsync()).Contains("athena-get-query-results-paging");
        await MotoApi.QueueAthenaResultAsync(["id", "name"], expected);
        await MotoApi.SetManualTransitionAsync("athena::execution", 1); // QUEUED → RUNNING → SUCCEEDED, one step per poll
        try
        {
            using var athena = new CountingAthena();
            var sql = AthenaSql.Select(Events, ["id", "name"], AthenaSql.AsOfVersion(42), "\"name\" <> " + AthenaSql.String("it's"));

            var result = await Runner(athena).RunAsync(sql);

            Assert.Equal(["id", "name"], result.Columns);
            Assert.Equal(expected, result.Rows);
            Assert.Equal(paging ? 3 : 1, athena.ResultPages);

            // What Athena (moto) received.
            var execution = (await athena.GetQueryExecutionAsync(new GetQueryExecutionRequest { QueryExecutionId = result.QueryExecutionId })).QueryExecution;
            Assert.Equal(sql, execution.Query);
            Assert.Equal("primary", execution.WorkGroup);
            Assert.Equal(QueryExecutionState.SUCCEEDED, execution.Status.State);
            Assert.StartsWith(_output, execution.ResultConfiguration.OutputLocation);
            // moto writes the result as Athena does, a CSV under the output location (header included).
            using var io = new S3FileIO(TestEnvironment.Current.S3Options);
            var csv = System.Text.Encoding.UTF8.GetString(await io.ReadAllAsync(execution.ResultConfiguration.OutputLocation)).TrimEnd('\n').Split('\n');
            Assert.Equal(count + 1, csv.Length);
            await io.DeleteAsync(execution.ResultConfiguration.OutputLocation);
        }
        finally
        {
            await MotoApi.UnsetTransitionAsync("athena::execution");
        }
    }

    [SkippableFact]
    public async Task Maintenance_statements_finish_with_no_rows()
    {
        RequireMoto();
        using var athena = AwsServiceClients.Athena();
        foreach (var sql in new[] { AthenaSql.Optimize(Events, "event_date >= DATE '2026-10-01'"), AthenaSql.Vacuum(Events) })
        {
            var result = await Runner(athena).RunAsync(sql);
            Assert.Empty(result.Columns);
            Assert.Empty(result.Rows);
            Assert.Equal(sql, (await athena.GetQueryExecutionAsync(new GetQueryExecutionRequest { QueryExecutionId = result.QueryExecutionId })).QueryExecution.Query);
        }
    }

    [SkippableFact]
    public async Task A_stopped_query_raises_with_its_state()
    {
        RequireMoto();
        await MotoApi.SetManualTransitionAsync("athena::execution", 5); // keep it QUEUED long enough to stop
        try
        {
            using var athena = AwsServiceClients.Athena();
            var runner = Runner(athena);
            var sql = AthenaSql.Vacuum(Events);
            var id = await runner.StartAsync(sql);
            await athena.StopQueryExecutionAsync(new StopQueryExecutionRequest { QueryExecutionId = id });

            var e = await Assert.ThrowsAsync<AthenaQueryFailedException>(() => runner.WaitAsync(id, sql));
            Assert.Equal("CANCELLED", e.State);
            Assert.Equal(id, e.QueryExecutionId);
            Assert.Contains("VACUUM", e.Message);
        }
        finally
        {
            await MotoApi.UnsetTransitionAsync("athena::execution");
        }
    }
}
