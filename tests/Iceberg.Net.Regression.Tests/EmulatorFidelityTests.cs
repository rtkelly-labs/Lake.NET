using Amazon.Glue;
using Amazon.Glue.Model;
using Iceberg.Net.Athena;
using Ath = Amazon.Athena.Model;
using Iceberg.Net.IO;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// The storage/catalog guarantees Iceberg commits rely on. These run unchanged in both modes:
/// passing against AWS proves the behaviour is real; passing in emulation proves the emulator
/// (plus emulation/moto_server.py shims) reproduces it. A failure in only one mode is a fidelity gap.
/// </summary>
[Spec("File System Operations")]
[Spec("Metastore Tables")]
[Trait("Category", "Regression")]
[Collection(MotoServiceStateCollection.Name)] // the Athena case uses moto's process-wide result queue
public sealed class EmulatorFidelityTests
{
    private static string Location(string name) => $"{TestEnvironment.Current.Warehouse.TrimEnd('/')}/fidelity/{TestEnvironment.Current.RunId}/{name}";

    [SkippableFact]
    public async Task S3_conditional_create_rejects_existing_objects()
    {
        Require.Target();
        using var io = new S3FileIO(TestEnvironment.Current.S3Options);
        var loc = Location($"{Guid.NewGuid():N}.metadata.json");
        await io.WriteAsync(loc, "first"u8.ToArray());
        await Assert.ThrowsAsync<FileAlreadyExistsException>(() => io.WriteAsync(loc, "second"u8.ToArray()));
        Assert.Equal("first"u8.ToArray(), await io.ReadAllAsync(loc));
        await io.DeleteAsync(loc);
    }

    [SkippableFact]
    public async Task S3_ranged_reads_and_lengths()
    {
        Require.Target();
        using var io = new S3FileIO(TestEnvironment.Current.S3Options);
        var loc = Location($"{Guid.NewGuid():N}.bin");
        var bytes = Enumerable.Range(0, 4096).Select(i => (byte)i).ToArray();
        await io.WriteAsync(loc, bytes);
        Assert.Equal(4096, await io.LengthAsync(loc));
        Assert.Equal(bytes[100..164], await io.ReadRangeAsync(loc, 100, 64));
        Assert.True(await io.ExistsAsync(loc));
        await io.DeleteAsync(loc);
        Assert.False(await io.ExistsAsync(loc));
    }

    [SkippableFact]
    public async Task Glue_update_table_enforces_version_id()
    {
        Require.Target();
        var env = TestEnvironment.Current;
        var config = new AmazonGlueConfig { RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(env.Region) };
        if (env.GlueEndpoint is not null)
        {
            config.ServiceURL = env.GlueEndpoint;
            config.AuthenticationRegion = env.Region;
        }
        using var glue = new AmazonGlueClient(config);
        var db = env.NewNamespace("fidelity");
        await glue.CreateDatabaseAsync(new CreateDatabaseRequest { DatabaseInput = new DatabaseInput { Name = db } });
        try
        {
            TableInput Input(string v) => new() { Name = "t", Parameters = new Dictionary<string, string> { ["v"] = v } };
            await glue.CreateTableAsync(new CreateTableRequest { DatabaseName = db, TableInput = Input("0") });
            var stale = (await glue.GetTableAsync(new GetTableRequest { DatabaseName = db, Name = "t" })).Table.VersionId;

            await glue.UpdateTableAsync(new UpdateTableRequest { DatabaseName = db, TableInput = Input("1"), VersionId = stale });
            await Assert.ThrowsAsync<ConcurrentModificationException>(() =>
                glue.UpdateTableAsync(new UpdateTableRequest { DatabaseName = db, TableInput = Input("2"), VersionId = stale }));

            var final = (await glue.GetTableAsync(new GetTableRequest { DatabaseName = db, Name = "t" })).Table;
            Assert.Equal("1", final.Parameters["v"]);
        }
        finally
        {
            await glue.DeleteTableAsync(new DeleteTableRequest { DatabaseName = db, Name = "t" });
            await glue.DeleteDatabaseAsync(new DeleteDatabaseRequest { Name = db });
        }
    }

    /// <summary>
    /// Athena returns GetQueryResults in pages of at most 1000 rows (a SELECT's header row counting as one) with a
    /// NextToken, and refuses a larger MaxResults. moto returns everything at once without the shim.
    /// </summary>
    [SkippableFact]
    public async Task Athena_get_query_results_pages_at_1000_rows()
    {
        var env = TestEnvironment.Current;
        using var athena = AwsServiceClients.Athena();
        string id;
        if (env.Target == Target.Emulation)
        {
            Require.Target();
            Skip.IfNot((await MotoApi.ShimsAsync()).Contains("athena-get-query-results-paging"),
                $"moto at {env.S3Endpoint} was started before emulation/moto_server.py had the Athena paging shim: restart it (emulation/up.sh)");
            await MotoApi.QueueAthenaResultAsync(["x"], Enumerable.Range(1, 2500).Select(i => (IReadOnlyList<string?>)[i.ToString(System.Globalization.CultureInfo.InvariantCulture)]));
            id = (await athena.StartQueryExecutionAsync(new Ath.StartQueryExecutionRequest { QueryString = "SELECT x FROM UNNEST(sequence(1, 2500)) AS t(x)", WorkGroup = "primary" })).QueryExecutionId;
        }
        else
        {
            Require.Athena();
            var runner = new AthenaQueryRunner(athena, new AthenaQueryOptions { WorkGroup = env.AthenaWorkGroup, OutputLocation = env.AthenaOutput });
            id = await runner.StartAsync("SELECT x FROM UNNEST(sequence(1, 2500)) AS t(x)");
            await runner.WaitAsync(id);
        }

        var pages = new List<Ath.GetQueryResultsResponse>();
        string? token = null;
        do
        {
            var page = await athena.GetQueryResultsAsync(new Ath.GetQueryResultsRequest { QueryExecutionId = id, NextToken = token });
            pages.Add(page);
            token = page.NextToken;
        } while (token is not null);

        Assert.Equal([1000, 1000, 501], pages.Select(p => p.ResultSet.Rows.Count)); // 2500 rows + the header
        Assert.Equal("x", pages[0].ResultSet.Rows[0].Data[0].VarCharValue);
        Assert.Equal("2500", pages[2].ResultSet.Rows[^1].Data[0].VarCharValue);
        var tooMany = pages[0].ResultSet.Rows.Count + 1; // not a constant: the SDK's analyzer refuses MaxResults = 1001 at compile time
        await Assert.ThrowsAsync<Ath.InvalidRequestException>(() =>
            athena.GetQueryResultsAsync(new Ath.GetQueryResultsRequest { QueryExecutionId = id, MaxResults = tooMany }));
    }
}
