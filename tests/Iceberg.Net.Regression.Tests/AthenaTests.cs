using Amazon.Athena;
using Amazon.Athena.Model;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// AWS-only: the production consumer. Athena (Trino-based, Iceberg v2) must read what we write
/// through Glue, and we must read what Athena writes. There is no emulator for Athena; these are
/// the tests that justify an AWS run.
/// </summary>
[Spec("Scan Planning")]
[Trait("Category", "Aws")]
public sealed class AthenaTests
{
    private static async Task<List<List<string?>>> QueryAsync(string sql)
    {
        var env = TestEnvironment.Current;
        using var athena = new AmazonAthenaClient(Amazon.RegionEndpoint.GetBySystemName(env.Region));
        var start = await athena.StartQueryExecutionAsync(new StartQueryExecutionRequest
        {
            QueryString = sql,
            WorkGroup = env.AthenaWorkGroup,
            ResultConfiguration = env.AthenaOutput is null ? null : new ResultConfiguration { OutputLocation = env.AthenaOutput },
        });
        while (true)
        {
            var status = (await athena.GetQueryExecutionAsync(new GetQueryExecutionRequest { QueryExecutionId = start.QueryExecutionId })).QueryExecution.Status;
            if (status.State == QueryExecutionState.SUCCEEDED) break;
            if (status.State == QueryExecutionState.FAILED || status.State == QueryExecutionState.CANCELLED)
                throw new InvalidOperationException($"Athena query {status.State}: {status.StateChangeReason}\n{sql}");
            await Task.Delay(1000);
        }
        var results = await athena.GetQueryResultsAsync(new GetQueryResultsRequest { QueryExecutionId = start.QueryExecutionId });
        return results.ResultSet.Rows.Skip(1).Select(r => r.Data.Select(d => (string?)d.VarCharValue).ToList()).ToList();
    }

    [SkippableFact]
    public async Task Athena_reads_dotnet_written_table()
    {
        Require.Athena();
        var env = TestEnvironment.Current;
        using var catalog = await env.CatalogAsync("glue");
        var ns = Namespace.Of(env.NewNamespace("athena_read"));
        await catalog.CreateNamespaceAsync(ns);
        var id = new TableIdentifier(ns, "events");
        try
        {
            var table = await catalog.CreateTableAsync(new CreateTableRequest(id, CatalogConformanceTests.Schema, CatalogConformanceTests.Spec));
            await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, CatalogConformanceTests.Rows(0, 50))).CommitAsync();
            await SnapshotProducer.Overwrite(table, f => (string?)f.Partition[2] == "us").CommitAsync();

            var rows = await QueryAsync($"SELECT count(*), sum(id), count(DISTINCT region) FROM \"{ns}\".\"events\"");
            Assert.Equal(["25", "600", "1"], rows.Single());
            // Partition pruning on our day transform: the 2024-01-01 partition has ids 0..4 (5h apart), even ids only.
            var day = await QueryAsync($"SELECT count(*) FROM \"{ns}\".\"events\" WHERE event_ts >= TIMESTAMP '2024-01-01 00:00:00 UTC' AND event_ts < TIMESTAMP '2024-01-02 00:00:00 UTC'");
            Assert.Equal("3", day.Single()[0]);
        }
        finally
        {
            await catalog.DropTableAsync(id, purge: true);
            await catalog.DropNamespaceAsync(ns);
        }
    }

    [SkippableFact]
    public async Task Dotnet_reads_athena_written_table()
    {
        Require.Athena();
        var env = TestEnvironment.Current;
        using var catalog = await env.CatalogAsync("glue");
        var ns = env.NewNamespace("athena_write");
        await catalog.CreateNamespaceAsync(Namespace.Of(ns));
        var location = $"{env.Warehouse.TrimEnd('/')}/athena/{ns}/t";
        try
        {
            await QueryAsync($"CREATE TABLE \"{ns}\".t (id bigint, name string) LOCATION '{location}' TBLPROPERTIES ('table_type'='ICEBERG')");
            await QueryAsync($"INSERT INTO \"{ns}\".t VALUES (1, 'a'), (2, 'b'), (3, NULL)");
            await QueryAsync($"DELETE FROM \"{ns}\".t WHERE id = 2"); // Athena writes positional delete files (merge-on-read)

            var table = await catalog.LoadTableAsync(new TableIdentifier(Namespace.Of(ns), "t"));
            Assert.Equal(2, table.Metadata.FormatVersion);
            // Athena's position delete files are applied (merge-on-read): row 2 is gone.
            Assert.Equal([1L, 3L], (await TableScan.ReadAllAsync(table)).Select(r => (long)r[0]!).Order());
            Assert.Equal(3, (await TableScan.ReadAllAsync(table, table.Metadata.CurrentSnapshot!.ParentSnapshotId)).Count);
            Assert.Contains((await TableScan.DeletesAsync(table)).All, e => !e.IsDeletionVector);

            // We write on top of Athena's deletes, and Athena still reads the result.
            // Delete id = 1 by position, found by reading Athena's data files directly.
            var target = new Dictionary<string, IEnumerable<long>>();
            foreach (var task in await TableScan.PlanFilesAsync(table))
            {
                var fileRows = await ParquetFiles.ReadAsync(table.IO, task.File.FilePath, table.Schema);
                var pos = fileRows.FindIndex(r => (long)r[0]! == 1);
                if (pos >= 0) target[task.File.FilePath] = [pos];
            }
            await SnapshotProducer.DeleteRows(table, target).CommitAsync();
            Assert.Equal([3L], (await TableScan.ReadAllAsync(table)).Select(r => (long)r[0]!));
            var count = await QueryAsync($"SELECT count(*) FROM \"{ns}\".t");
            Assert.Equal("1", count.Single()[0]);
        }
        finally
        {
            await QueryAsync($"DROP TABLE IF EXISTS \"{ns}\".t");
            await catalog.DropNamespaceAsync(Namespace.Of(ns));
        }
    }
}
