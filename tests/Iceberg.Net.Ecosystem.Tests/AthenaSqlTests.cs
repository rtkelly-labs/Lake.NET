using Amazon;
using Amazon.Athena;
using Amazon.Athena.Model;
using Amazon.Runtime;
using Iceberg.Net.Athena;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Ecosystem.Tests;

/// <summary>
/// Athena SQL generation, and the query runner against an overridden SDK client. Real Athena runs are the AWS-only
/// <c>AthenaTests</c> in the regression suite (skipped without an AWS target).
/// </summary>
public sealed class AthenaSqlTests
{
    private static readonly AthenaTable Events = new("analytics", "events");

    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String, "display name"),
        NestedField.Optional(3, "event_ts", PrimitiveType.Timestamp),
        NestedField.Optional(4, "amount", new DecimalType(38, 2)),
        NestedField.Optional(5, "tags", new ListType(8, PrimitiveType.String, false)),
        NestedField.Optional(6, "attrs", new MapType(9, PrimitiveType.String, 10, PrimitiveType.Int, false)),
        NestedField.Optional(7, "loc", new StructType(NestedField.Optional(11, "lat", PrimitiveType.Double), NestedField.Optional(12, "lon", PrimitiveType.Double))),
    ]);

    [Fact]
    public void Identifiers_and_literals_are_quoted_for_trino_and_hive()
    {
        Assert.Equal("\"analytics\".\"events\"", Events.Dml);
        Assert.Equal("\"AwsDataCatalog\".\"analytics\".\"events\"", (Events with { Catalog = "AwsDataCatalog" }).Dml);
        Assert.Equal("`analytics`.`events`", Events.Ddl);
        Assert.Equal("\"we\"\"ird\"", AthenaSql.Identifier("we\"ird"));
        Assert.Equal("'it''s'", AthenaSql.String("it's"));
        Assert.Throws<ArgumentException>(() => AthenaSql.DdlIdentifier("a`b"));
        Assert.Throws<ArgumentException>(() => AthenaSql.DdlString("s3://b/it's"));
        Assert.Throws<ArgumentException>(() => AthenaSql.DdlString(@"a\b"));
    }

    [Fact]
    public void Create_table_from_an_iceberg_schema_and_partition_spec()
    {
        var spec = PartitionSpec.BuilderFor(Schema).Day("event_ts").Bucket("id", 16).Truncate("name", 4).Build();
        var sql = AthenaSql.CreateTable(Events, Schema, "s3://lake/analytics/events", spec, new AthenaIcebergProperties
        {
            VacuumMaxSnapshotAgeSeconds = 86400, OptimizeRewriteDeleteFileThreshold = 10, WriteCompression = "zstd",
        });
        Assert.Equal("""
            CREATE TABLE `analytics`.`events` (
              `id` bigint,
              `name` string COMMENT 'display name',
              `event_ts` timestamp,
              `amount` decimal(38,2),
              `tags` array<string>,
              `attrs` map<string,int>,
              `loc` struct<lat:double,lon:double>)
            PARTITIONED BY (day(`event_ts`), bucket(16, `id`), truncate(4, `name`))
            LOCATION 's3://lake/analytics/events'
            TBLPROPERTIES ('table_type'='ICEBERG', 'optimize_rewrite_delete_file_threshold'='10', 'vacuum_max_snapshot_age_seconds'='86400', 'write_compression'='zstd')
            """.ReplaceLineEndings("\n"), sql);
    }

    [Fact]
    public void Types_athena_cannot_declare_are_refused()
    {
        foreach (var type in new IcebergType[] { PrimitiveType.TimestampTz, PrimitiveType.Uuid, PrimitiveType.Time, new FixedType(16), PrimitiveType.TimestampNs })
            Assert.Throws<NotSupportedException>(() => AthenaSql.DdlType(type));
        Assert.Equal("CREATE TABLE IF NOT EXISTS `analytics`.`events` (\n  `id` bigint)\nLOCATION 's3://x/'\nTBLPROPERTIES ('table_type'='ICEBERG')",
            AthenaSql.CreateTable(Events, new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long)]), "s3://x/", ifNotExists: true));
    }

    [Fact]
    public void Maintenance_statements()
    {
        Assert.Equal("OPTIMIZE \"analytics\".\"events\" REWRITE DATA USING BIN_PACK", AthenaSql.Optimize(Events));
        Assert.Equal("OPTIMIZE \"analytics\".\"events\" REWRITE DATA USING BIN_PACK WHERE event_date >= DATE '2026-10-01'",
            AthenaSql.Optimize(Events, "event_date >= DATE '2026-10-01'"));
        Assert.Equal("VACUUM \"analytics\".\"events\"", AthenaSql.Vacuum(Events));
        Assert.Equal("ALTER TABLE `analytics`.`events` SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds'='259200', 'vacuum_min_snapshots_to_keep'='3')",
            AthenaSql.SetTableProperties(Events, new AthenaIcebergProperties { VacuumMaxSnapshotAgeSeconds = 259200, VacuumMinSnapshotsToKeep = 3 }));
        Assert.Equal("ALTER TABLE `analytics`.`events` UNSET TBLPROPERTIES ('write_compression')", AthenaSql.UnsetTableProperties(Events, ["write_compression"]));
        Assert.Throws<ArgumentException>(() => AthenaSql.SetTableProperties(Events, new Dictionary<string, string>()));
    }

    [Fact]
    public void Time_travel_and_metadata_tables()
    {
        Assert.Equal("SELECT * FROM \"analytics\".\"events\" FOR TIMESTAMP AS OF TIMESTAMP '2026-10-01 11:00:00.000000 UTC'",
            AthenaSql.Select(Events, timeTravel: AthenaSql.AsOf(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.FromHours(1)))));
        Assert.Equal("SELECT \"id\", \"name\" FROM \"analytics\".\"events\" FOR VERSION AS OF 949530903748831860 WHERE id > 5",
            AthenaSql.Select(Events, ["id", "name"], AthenaSql.AsOfVersion(949530903748831860), "id > 5"));
        Assert.Equal("\"analytics\".\"events$snapshots\"", AthenaSql.MetadataTable(Events, "snapshots"));
        Assert.Throws<ArgumentException>(() => AthenaSql.MetadataTable(Events, "secrets"));
    }

    private sealed class FakeAthena() : AmazonAthenaClient(new BasicAWSCredentials("test", "test"), RegionEndpoint.EUWest1)
    {
        public FakeCalls Calls { get; } = new();
        public override Task<StartQueryExecutionResponse> StartQueryExecutionAsync(StartQueryExecutionRequest request, CancellationToken ct = default) =>
            Calls.Answer<StartQueryExecutionResponse>(request);
        public override Task<GetQueryExecutionResponse> GetQueryExecutionAsync(GetQueryExecutionRequest request, CancellationToken ct = default) =>
            Calls.Answer<GetQueryExecutionResponse>(request);
        public override Task<GetQueryResultsResponse> GetQueryResultsAsync(GetQueryResultsRequest request, CancellationToken ct = default) =>
            Calls.Answer<GetQueryResultsResponse>(request);
    }

    private static Row Row(params string?[] values) => new() { Data = values.Select(v => new Datum { VarCharValue = v }).ToList() };

    [Fact]
    public async Task Runner_polls_until_done_and_reads_every_page_without_the_header()
    {
        var athena = new FakeAthena();
        var id = Guid.NewGuid().ToString();
        var states = new Queue<QueryExecutionState>([QueryExecutionState.QUEUED, QueryExecutionState.RUNNING, QueryExecutionState.SUCCEEDED]);
        var columns = new ResultSetMetadata { ColumnInfo = [new ColumnInfo { Name = "id" }, new ColumnInfo { Name = "name" }] };
        athena.Calls
            .On<StartQueryExecutionRequest, StartQueryExecutionResponse>(_ => new StartQueryExecutionResponse { QueryExecutionId = id })
            .On<GetQueryExecutionRequest, GetQueryExecutionResponse>(_ => new GetQueryExecutionResponse
            {
                QueryExecution = new QueryExecution
                {
                    Status = new QueryExecutionStatus { State = states.Dequeue() },
                    Statistics = new QueryExecutionStatistics { DataScannedInBytes = 1234, EngineExecutionTimeInMillis = 56 },
                },
            })
            .On<GetQueryResultsRequest, GetQueryResultsResponse>(r => r.NextToken is null
                ? new GetQueryResultsResponse { NextToken = "p2", ResultSet = new ResultSet { ResultSetMetadata = columns, Rows = [Row("id", "name"), Row("1", "a")] } }
                : new GetQueryResultsResponse { ResultSet = new ResultSet { ResultSetMetadata = columns, Rows = [Row("2", null)] } });
        var runner = new AthenaQueryRunner(athena, new AthenaQueryOptions
        {
            WorkGroup = "wg", OutputLocation = "s3://results/", Database = "analytics", PollInterval = TimeSpan.FromMilliseconds(1),
        });

        var result = await runner.RunAsync(AthenaSql.Select(Events, ["id", "name"]));

        Assert.Equal(["id", "name"], result.Columns);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal(["1", "a"], result.Rows[0]);
        Assert.Equal(["2", null], result.Rows[1]);
        Assert.Equal(1234, result.DataScannedBytes);
        var start = Assert.Single(athena.Calls.Sent<StartQueryExecutionRequest>());
        Assert.Equal("wg", start.WorkGroup);
        Assert.Equal("s3://results/", start.ResultConfiguration.OutputLocation);
        Assert.Equal("analytics", start.QueryExecutionContext.Database);
        Assert.Equal(3, athena.Calls.Sent<GetQueryExecutionRequest>().Count());
    }

    [Fact]
    public async Task Runner_raises_on_a_failed_query()
    {
        var athena = new FakeAthena();
        athena.Calls
            .On<StartQueryExecutionRequest, StartQueryExecutionResponse>(_ => new StartQueryExecutionResponse { QueryExecutionId = Guid.NewGuid().ToString() })
            .On<GetQueryExecutionRequest, GetQueryExecutionResponse>(_ => new GetQueryExecutionResponse
            {
                QueryExecution = new QueryExecution { Status = new QueryExecutionStatus { State = QueryExecutionState.FAILED, StateChangeReason = "ICEBERG_COMMIT_ERROR" } },
            });
        var runner = new AthenaQueryRunner(athena, new AthenaQueryOptions { WorkGroup = "wg", PollInterval = TimeSpan.FromMilliseconds(1) });
        var e = await Assert.ThrowsAsync<AthenaQueryFailedException>(() => runner.RunAsync(AthenaSql.Vacuum(Events)));
        Assert.Equal("FAILED", e.State);
        Assert.Contains("ICEBERG_COMMIT_ERROR", e.Message);
        Assert.Contains("VACUUM", e.Message);
    }
}
