using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon.EMRServerless;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Spark;
using Iceberg.Net.Types;
using Emr = Amazon.EMRServerless.Model;
using Glue = Amazon.Glue.Model;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// The EMR Serverless and Glue job runners with the real AWS SDK clients against moto, end to end: the driver and
/// statements staged in moto S3, the job (Glue) created and the run started on moto, polled to a terminal state —
/// and then executed. moto runs nothing, so <see cref="SparkJobServiceEmulator"/> reads the run back from moto and
/// spark-submits exactly what it holds (the staged s3:// driver and statements) against the Glue catalog in moto,
/// the way the service would. This library then checks what Spark did to the table.
/// moto's run states are canned (EMR: SUCCESS at once; Glue: STARTING → RUNNING → SUCCEEDED), so the link from the
/// Spark outcome to the reported state is the one thing only AWS can show.
/// </summary>
[Spec("Scan Planning")]
[Trait("Category", "Regression")]
[Collection(MotoServiceStateCollection.Name)]
public sealed class ManagedSparkJobTests : IAsyncLifetime
{
    private const string SparkCatalog = "glue";
    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)]);

    private readonly TestEnvironment _env = TestEnvironment.Current;
    private readonly Namespace _ns = Namespace.Of(TestEnvironment.Current.NewNamespace("sparkjob"));
    private readonly string _staging = $"{TestEnvironment.Current.Warehouse.TrimEnd('/')}/jobs/{TestEnvironment.Current.RunId}-{Guid.NewGuid().ToString("N")[..8]}/";
    private ICatalog? _catalog;
    private S3FileIO? _io;
    private readonly List<string> _applications = [];

    public async Task InitializeAsync()
    {
        if (_env.Target != Target.Emulation || _env.SkipReason is not null) return;
        _catalog = await _env.CatalogAsync("glue");
        await _catalog.CreateNamespaceAsync(_ns);
        _io = new S3FileIO(_env.S3Options);
    }

    public async Task DisposeAsync()
    {
        if (_applications.Count > 0)
        {
            using var emr = AwsServiceClients.EmrServerless();
            foreach (var id in _applications)
            {
                await emr.StopApplicationAsync(new Emr.StopApplicationRequest { ApplicationId = id });
                await emr.DeleteApplicationAsync(new Emr.DeleteApplicationRequest { ApplicationId = id });
            }
        }
        if (_io is not null)
            await foreach (var staged in _io.ListAsync(_staging)) await _io.DeleteAsync(staged.Location); // driver and statements
        if (_catalog is null) return;
        foreach (var id in await _catalog.ListTablesAsync(_ns)) await _catalog.DropTableAsync(id, purge: true);
        await _catalog.DropNamespaceAsync(_ns);
        _catalog.Dispose();
        _io?.Dispose();
    }

    private static void RequireEmulation() =>
        Skip.If(TestEnvironment.Current.Target != Target.Emulation, "moto-backed job runs are emulation only; on AWS they need an EMR application / Glue role");

    private SparkCatalogConfig Catalog() => SparkCatalogConfig.Glue(SparkCatalog, _env.Warehouse.TrimEnd('/') + "/glue");

    /// <summary>A table with three appends (three small data files) for rewrite_data_files to compact.</summary>
    private async Task<(Table Table, SparkTable Spark)> FragmentedTableAsync(string name)
    {
        var table = await _catalog!.CreateTableAsync(new CreateTableRequest(new TableIdentifier(_ns, name), Schema, FormatVersion: 2));
        for (var batch = 0; batch < 3; batch++)
            await TableMutations.InsertAsync(table, Enumerable.Range(batch * 10, 10).Select(i => new object?[] { (long)i, i % 4 == 0 ? null : $"n;{i}'" }).ToList());
        return (await _catalog.LoadTableAsync(table.Identifier), SparkTable.From(SparkCatalog, table.Identifier));
    }

    private static async Task<List<(long Id, string? Name)>> RowsAsync(Table table) =>
        (await TableScan.ReadAllAsync(table)).Select(r => ((long)r[0]!, (string?)r[1])).OrderBy(r => r.Item1).ToList();

    private static SparkSqlScript Maintenance(SparkTable t)
    {
        var p = new IcebergProcedures(SparkCatalog);
        return new SparkSqlScript()
            .Add(p.RewriteDataFiles(t, new RewriteDataFilesOptions { MinInputFiles = 2 }))
            .Add(p.ExpireSnapshots(t, new ExpireSnapshotsOptions { OlderThan = DateTimeOffset.UtcNow.AddDays(1), RetainLast = 1 }))
            .Add($"SELECT count(*) AS n, sum(id) AS s FROM {t.QualifiedName} WHERE name LIKE '%;%'");
    }

    /// <summary>The table after the maintenance script: same rows, one compacted file, one snapshot, fsck clean.</summary>
    private async Task AssertMaintainedAsync(Table before, List<(long Id, string? Name)> expected, JsonArray results)
    {
        Assert.Equal(3, results[0]!["rows"]![0]!["rewritten_data_files_count"]!.GetValue<int>());
        Assert.Equal(1, results[0]!["rows"]![0]!["added_data_files_count"]!.GetValue<int>());
        Assert.True(results[1]!["rows"]![0]!["deleted_manifest_lists_count"]!.GetValue<long>() >= 3, results[1]!.ToJsonString());
        var withSemicolon = expected.Where(r => r.Item2 is not null).ToList();
        Assert.Equal(withSemicolon.Count, results[2]!["rows"]![0]!["n"]!.GetValue<long>());
        Assert.Equal(withSemicolon.Sum(r => r.Item1), results[2]!["rows"]![0]!["s"]!.GetValue<long>());

        var after = await _catalog!.LoadTableAsync(before.Identifier);
        Assert.Equal("replace", after.Metadata.CurrentSnapshot!.Operation);
        Assert.Single(after.Metadata.Snapshots);
        Assert.Single(await TableScan.PlanFilesAsync(after));
        Assert.Equal(expected, await RowsAsync(after));
        var report = await TableValidator.ValidateAsync(after, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());
    }

    private async Task<string> CreateEmrApplicationAsync(IAmazonEMRServerless emr)
    {
        var id = (await emr.CreateApplicationAsync(new Emr.CreateApplicationRequest
        {
            Name = "iceberg-regression-" + _env.RunId, ReleaseLabel = "emr-7.5.0", Type = "SPARK", ClientToken = Guid.NewGuid().ToString("N"),
        })).ApplicationId;
        _applications.Add(id);
        return id;
    }

    [SkippableFact]
    public async Task Emr_serverless_run_stages_submits_and_its_driver_maintains_the_table()
    {
        RequireEmulation();
        Skip.If(SparkJobServiceEmulator.SkipReason is not null, SparkJobServiceEmulator.SkipReason);
        using var emr = AwsServiceClients.EmrServerless();
        var applicationId = await CreateEmrApplicationAsync(emr);
        var (table, spark) = await FragmentedTableAsync("events");
        var expected = await RowsAsync(table);
        var stager = new SparkScriptStager(_io!, _staging);
        var runner = new EmrServerlessSqlJobRunner(emr, stager, new EmrServerlessJobOptions
        {
            ApplicationId = applicationId, ExecutionRoleArn = "arn:aws:iam::123456789012:role/emr-iceberg", StagingPrefix = _staging, Spark = Catalog(),
            ExtraSparkSubmitParameters = "--conf spark.executor.cores=1", ExecutionTimeoutMinutes = 30, LogUri = _staging + "logs/",
            Tags = new() { ["purpose"] = "iceberg-regression" },
        });
        var script = Maintenance(spark);

        var submitted = await runner.SubmitAsync(script, "compact-events");

        // What moto recorded is what EMR would run: the staged driver with --statements, the catalog as --conf.
        var recorded = (await emr.GetJobRunAsync(new Emr.GetJobRunRequest { ApplicationId = applicationId, JobRunId = submitted.RunId })).JobRun;
        Assert.Equal("compact-events", recorded.Name);
        Assert.Equal(stager.DriverLocation, recorded.JobDriver.SparkSubmit.EntryPoint);
        Assert.Equal(["--statements", submitted.StatementsLocation!], recorded.JobDriver.SparkSubmit.EntryPointArguments);
        Assert.Contains($"--conf spark.sql.catalog.{SparkCatalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog", recorded.JobDriver.SparkSubmit.SparkSubmitParameters);
        Assert.EndsWith("--conf spark.executor.cores=1", recorded.JobDriver.SparkSubmit.SparkSubmitParameters);
        Assert.Equal(_staging + "logs/", recorded.ConfigurationOverrides.MonitoringConfiguration.S3MonitoringConfiguration.LogUri);
        Assert.Equal("iceberg-regression", recorded.Tags["purpose"]);
        Assert.Equal(SparkScriptStager.DriverScript, Encoding.UTF8.GetString(await _io!.ReadAllAsync(stager.DriverLocation)));
        Assert.Equal(script.Statements, JsonSerializer.Deserialize<string[]>(await _io.ReadAllAsync(submitted.StatementsLocation!)));

        var results = await SparkJobServiceEmulator.RunEmrServerlessJobAsync(emr, applicationId, submitted.RunId);
        var finished = await runner.WaitAsync(submitted.RunId, TimeSpan.FromMinutes(1), TimeSpan.FromMilliseconds(50));

        Assert.Equal(SparkJobState.Succeeded, finished.State);
        await AssertMaintainedAsync(table, expected, results);
    }

    [SkippableFact]
    public async Task Emr_serverless_cancel_reaches_the_service_and_the_run_reads_cancelled()
    {
        RequireEmulation();
        Require.Target();
        using var emr = AwsServiceClients.EmrServerless();
        var applicationId = await CreateEmrApplicationAsync(emr);
        var runner = new EmrServerlessSqlJobRunner(emr, new SparkScriptStager(_io!, _staging), new EmrServerlessJobOptions
        {
            ApplicationId = applicationId, ExecutionRoleArn = "arn:aws:iam::123456789012:role/emr-iceberg", StagingPrefix = _staging, Spark = Catalog(),
        });

        var run = await runner.SubmitAsync(new SparkSqlScript().Add("SELECT 1"));
        await runner.CancelAsync(run.RunId);

        Assert.Equal(SparkJobState.Cancelled, (await runner.GetAsync(run.RunId)).State);
        var e = await Assert.ThrowsAsync<Amazon.EMRServerless.Model.ResourceNotFoundException>(() => runner.GetAsync("00000000000000000"));
        Assert.Contains("00000000000000000", e.Message);
    }

    [SkippableFact]
    public async Task A_failing_statement_fails_the_staged_job()
    {
        RequireEmulation();
        Skip.If(SparkJobServiceEmulator.SkipReason is not null, SparkJobServiceEmulator.SkipReason);
        using var emr = AwsServiceClients.EmrServerless();
        var applicationId = await CreateEmrApplicationAsync(emr);
        var runner = new EmrServerlessSqlJobRunner(emr, new SparkScriptStager(_io!, _staging), new EmrServerlessJobOptions
        {
            ApplicationId = applicationId, ExecutionRoleArn = "arn:aws:iam::123456789012:role/emr-iceberg", StagingPrefix = _staging, Spark = Catalog(),
        });
        var missing = new SparkTable(SparkCatalog, _ns.ToString(), "missing");

        var run = await runner.SubmitAsync(new SparkSqlScript().Add("SELECT 1 AS one").Add(new IcebergProcedures(SparkCatalog).RewriteDataFiles(missing)));

        var e = await Assert.ThrowsAsync<SparkJobEmulationException>(() => SparkJobServiceEmulator.RunEmrServerlessJobAsync(emr, applicationId, run.RunId));
        // The driver ran the first statement, then the procedure failed the job (spark-submit exits non-zero).
        Assert.Contains("[1/2] SELECT 1 AS one", e.Log);
        Assert.Contains("TABLE_OR_VIEW_NOT_FOUND", e.Log);
        Assert.Contains(".missing", e.Log);
        Assert.DoesNotContain("@@RESULTS@@", e.Log);
        Assert.Contains("[2/2]", e.Log);
    }

    [SkippableFact]
    public async Task Glue_job_is_created_and_its_run_maintains_the_table()
    {
        RequireEmulation();
        Skip.If(SparkJobServiceEmulator.SkipReason is not null, SparkJobServiceEmulator.SkipReason);
        using var glue = AwsServiceClients.Glue();
        var jobName = "iceberg-sql-" + _env.RunId + "-" + Guid.NewGuid().ToString("N")[..6];
        var (table, spark) = await FragmentedTableAsync("events_glue");
        var expected = await RowsAsync(table);
        var options = new GlueJobOptions { JobName = jobName, StagingPrefix = _staging, Spark = Catalog(), RoleArn = "arn:aws:iam::123456789012:role/glue-iceberg", TimeoutMinutes = 30 };
        var stager = new SparkScriptStager(_io!, _staging);
        var runner = new GlueSqlJobRunner(glue, stager, options);
        try
        {
            // moto 5.2.4 has no UpdateJob, so repointing an existing job at a new driver is covered by SparkJobRunnerTests only.
            await runner.EnsureJobAsync();
            await runner.EnsureJobAsync(); // exists and already points at this driver: no change
            var job = (await glue.GetJobAsync(new Glue.GetJobRequest { JobName = jobName })).Job;
            Assert.Equal(stager.DriverLocation, job.Command.ScriptLocation);
            Assert.Equal("glueetl", job.Command.Name);
            Assert.Equal("5.0", job.GlueVersion);
            Assert.Equal("arn:aws:iam::123456789012:role/glue-iceberg", job.Role);
            Assert.Equal("iceberg", job.DefaultArguments["--datalake-formats"]);
            Assert.StartsWith("spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog --conf ", job.DefaultArguments["--conf"]);

            await MotoApi.SetManualTransitionAsync("glue::job_run", 2);
            var submitted = await runner.SubmitAsync(Maintenance(spark));
            Assert.Equal(SparkJobState.Pending, (await runner.GetAsync(submitted.RunId)).State); // STARTING

            var results = await SparkJobServiceEmulator.RunGlueJobAsync(glue, jobName, submitted.RunId);
            var seen = new List<SparkJobState>();
            SparkJobRun current;
            do
            {
                current = await runner.GetAsync(submitted.RunId);
                seen.Add(current.State);
            } while (!current.IsFinished);

            Assert.Equal([SparkJobState.Running, SparkJobState.Succeeded], seen.Distinct());
            await AssertMaintainedAsync(table, expected, results);
        }
        finally
        {
            await MotoApi.UnsetTransitionAsync("glue::job_run");
            await glue.DeleteJobAsync(new Glue.DeleteJobRequest { JobName = jobName });
        }
    }
}
