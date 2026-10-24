using System.Text.Json;
using Amazon.EMRServerless;
using Amazon.Glue;
using Iceberg.Net.IO;
using Iceberg.Net.Spark;
using Emr = Amazon.EMRServerless.Model;
using Glue = Amazon.Glue.Model;

namespace Iceberg.Net.Ecosystem.Tests;

/// <summary>
/// The EMR Serverless and Glue runners against faked SDK clients: what they stage, the StartJobRun requests they send,
/// and how service states map. (A real run needs AWS; the driver itself runs in Spark in <see cref="SparkProcedureOracleTests"/>.)
/// </summary>
public sealed class SparkJobRunnerTests : IDisposable
{
    private readonly string _staging = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly SparkScriptStager _stager;
    private static readonly SparkSqlScript Script = new SparkSqlScript()
        .Add(new IcebergProcedures("glue").RewriteDataFiles(new SparkTable("glue", "db", "t")))
        .Add(new IcebergProcedures("glue").ExpireSnapshots(new SparkTable("glue", "db", "t"), new ExpireSnapshotsOptions { RetainLast = 3 }));

    public SparkJobRunnerTests() => _stager = new SparkScriptStager(new LocalFileIO(), _staging);

    public void Dispose()
    {
        if (Directory.Exists(_staging)) Directory.Delete(_staging, true);
    }

    [Fact]
    public async Task Emr_serverless_submits_the_staged_driver_with_the_statements_and_catalog_conf()
    {
        var client = new FakeEmrServerless(); var fake = client.Calls;
        var states = new Queue<string>(["PENDING", "RUNNING", "SUCCESS"]);
        fake.On<Emr.StartJobRunRequest, Emr.StartJobRunResponse>(_ => new Emr.StartJobRunResponse { JobRunId = "run1", ApplicationId = "app" })
            .On<Emr.GetJobRunRequest, Emr.GetJobRunResponse>(_ => new Emr.GetJobRunResponse { JobRun = new Emr.JobRun { State = new Amazon.EMRServerless.JobRunState(states.Dequeue()) } });
        var runner = new EmrServerlessSqlJobRunner(client, _stager, new EmrServerlessJobOptions
        {
            ApplicationId = "app", ExecutionRoleArn = "arn:aws:iam::1:role/r", StagingPrefix = _staging,
            Spark = SparkCatalogConfig.Glue("glue", "s3://lake/wh"), ExtraSparkSubmitParameters = "--conf spark.executor.cores=2", LogUri = "s3://logs/",
        });

        var run = await runner.RunAsync(Script, "compact-db-t", pollInterval: TimeSpan.FromMilliseconds(1));

        Assert.Equal(SparkJobState.Succeeded, run.State);
        var request = Assert.Single(fake.Sent<Emr.StartJobRunRequest>());
        Assert.Equal("app", request.ApplicationId);
        Assert.Equal("compact-db-t", request.Name);
        Assert.Equal(_stager.DriverLocation, request.JobDriver.SparkSubmit.EntryPoint);
        Assert.Equal(["--statements", run.StatementsLocation!], request.JobDriver.SparkSubmit.EntryPointArguments);
        Assert.Contains("--conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog", request.JobDriver.SparkSubmit.SparkSubmitParameters);
        Assert.EndsWith("--conf spark.executor.cores=2", request.JobDriver.SparkSubmit.SparkSubmitParameters);
        Assert.Equal("s3://logs/", request.ConfigurationOverrides.MonitoringConfiguration.S3MonitoringConfiguration.LogUri);
        Assert.Equal(Script.Statements, JsonSerializer.Deserialize<string[]>(await File.ReadAllTextAsync(run.StatementsLocation!)));
        Assert.True(File.Exists(_stager.DriverLocation));
        Assert.Equal(3, fake.Sent<Emr.GetJobRunRequest>().Count());
    }

    [Fact]
    public async Task A_failed_run_raises_with_the_service_details()
    {
        var client = new FakeEmrServerless(); var fake = client.Calls;
        fake.On<Emr.StartJobRunRequest, Emr.StartJobRunResponse>(_ => new Emr.StartJobRunResponse { JobRunId = "run2" })
            .On<Emr.GetJobRunRequest, Emr.GetJobRunResponse>(_ => new Emr.GetJobRunResponse { JobRun = new Emr.JobRun { State = Amazon.EMRServerless.JobRunState.FAILED, StateDetails = "Table not found" } });
        var runner = new EmrServerlessSqlJobRunner(client, _stager, new EmrServerlessJobOptions
        {
            ApplicationId = "app", ExecutionRoleArn = "r", StagingPrefix = _staging, Spark = SparkCatalogConfig.Glue("glue", "s3://lake/wh"),
        });
        var e = await Assert.ThrowsAsync<SparkJobFailedException>(() => runner.RunAsync(Script, pollInterval: TimeSpan.FromMilliseconds(1)));
        Assert.Equal(SparkJobState.Failed, e.Run.State);
        Assert.Contains("Table not found", e.Message);
    }

    [Theory]
    [InlineData("SUBMITTED", SparkJobState.Pending)]
    [InlineData("SCHEDULED", SparkJobState.Pending)]
    [InlineData("RUNNING", SparkJobState.Running)]
    [InlineData("SUCCESS", SparkJobState.Succeeded)]
    [InlineData("FAILED", SparkJobState.Failed)]
    [InlineData("CANCELLED", SparkJobState.Cancelled)]
    public async Task Emr_states_map(string state, SparkJobState expected)
    {
        var client = new FakeEmrServerless(); var fake = client.Calls;
        fake.On<Emr.GetJobRunRequest, Emr.GetJobRunResponse>(_ => new Emr.GetJobRunResponse { JobRun = new Emr.JobRun { State = new Amazon.EMRServerless.JobRunState(state) } });
        var runner = new EmrServerlessSqlJobRunner(client, _stager, new EmrServerlessJobOptions
        {
            ApplicationId = "app", ExecutionRoleArn = "r", StagingPrefix = _staging, Spark = SparkCatalogConfig.Glue("glue", "s3://lake/wh"),
        });
        Assert.Equal(expected, (await runner.GetAsync("x")).State);
    }

    [Fact]
    public async Task Glue_creates_the_job_once_then_starts_runs_with_the_statements_argument()
    {
        var client = new FakeGlue(); var fake = client.Calls;
        Glue.Job? job = null;
        fake.On<Glue.GetJobRequest, Glue.GetJobResponse>(r => job is null ? throw new Glue.EntityNotFoundException("no job") : new Glue.GetJobResponse { Job = job })
            .On<Glue.CreateJobRequest, Glue.CreateJobResponse>(r =>
            {
                job = new Glue.Job { Name = r.Name, Command = r.Command, Role = r.Role, DefaultArguments = r.DefaultArguments, GlueVersion = r.GlueVersion };
                return new Glue.CreateJobResponse { Name = r.Name };
            })
            .On<Glue.StartJobRunRequest, Glue.StartJobRunResponse>(_ => new Glue.StartJobRunResponse { JobRunId = "jr_1" })
            .On<Glue.GetJobRunRequest, Glue.GetJobRunResponse>(_ => new Glue.GetJobRunResponse { JobRun = new Glue.JobRun { JobRunState = Amazon.Glue.JobRunState.SUCCEEDED } });
        var runner = new GlueSqlJobRunner(client, _stager, new GlueJobOptions
        {
            JobName = "iceberg-sql", StagingPrefix = _staging, Spark = SparkCatalogConfig.Glue("glue", "s3://lake/wh"), RoleArn = "arn:aws:iam::1:role/glue",
        });

        await runner.EnsureJobAsync();
        await runner.EnsureJobAsync(); // already points at this driver: no update
        var run = await runner.RunAsync(Script, pollInterval: TimeSpan.FromMilliseconds(1));

        var create = Assert.Single(fake.Sent<Glue.CreateJobRequest>());
        Assert.Equal("glueetl", create.Command.Name);
        Assert.Equal(_stager.DriverLocation, create.Command.ScriptLocation);
        Assert.Equal("5.0", create.GlueVersion);
        Assert.Equal("iceberg", create.DefaultArguments["--datalake-formats"]);
        Assert.Contains(" --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions", create.DefaultArguments["--conf"]);
        Assert.Empty(fake.Sent<Glue.UpdateJobRequest>());
        var start = Assert.Single(fake.Sent<Glue.StartJobRunRequest>());
        Assert.Equal("iceberg-sql", start.JobName);
        Assert.Equal(run.StatementsLocation, start.Arguments["--statements"]);
        Assert.Equal(SparkJobState.Succeeded, run.State);
    }

    [Theory]
    [InlineData("STARTING", SparkJobState.Pending)]
    [InlineData("RUNNING", SparkJobState.Running)]
    [InlineData("SUCCEEDED", SparkJobState.Succeeded)]
    [InlineData("TIMEOUT", SparkJobState.Failed)]
    [InlineData("ERROR", SparkJobState.Failed)]
    [InlineData("STOPPED", SparkJobState.Cancelled)]
    public async Task Glue_states_map(string state, SparkJobState expected)
    {
        var client = new FakeGlue(); var fake = client.Calls;
        fake.On<Glue.GetJobRunRequest, Glue.GetJobRunResponse>(_ => new Glue.GetJobRunResponse { JobRun = new Glue.JobRun { JobRunState = new Amazon.Glue.JobRunState(state) } });
        var runner = new GlueSqlJobRunner(client, _stager, new GlueJobOptions { JobName = "j", StagingPrefix = _staging, Spark = SparkCatalogConfig.Glue("glue", "s3://w") });
        Assert.Equal(expected, (await runner.GetAsync("x")).State);
    }
}
