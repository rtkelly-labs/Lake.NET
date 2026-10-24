using Amazon.EMRServerless;
using Amazon.EMRServerless.Model;
using EmrJobRunState = Amazon.EMRServerless.JobRunState;

namespace Iceberg.Net.Spark;

/// <summary>EMR Serverless application and role a <see cref="EmrServerlessSqlJobRunner"/> submits to.</summary>
public sealed record EmrServerlessJobOptions
{
    public required string ApplicationId { get; init; }
    public required string ExecutionRoleArn { get; init; }
    /// <summary>Where the driver and statements are written, e.g. <c>s3://bucket/iceberg-jobs/</c>.</summary>
    public required string StagingPrefix { get; init; }
    /// <summary>Catalog and extension settings; EMR 7.x images ship Iceberg (<c>spark.jars</c> otherwise).</summary>
    public required SparkCatalogConfig Spark { get; init; }
    /// <summary>Further <c>sparkSubmitParameters</c>, e.g. <c>--conf spark.executor.cores=4</c>.</summary>
    public string? ExtraSparkSubmitParameters { get; init; }
    public long? ExecutionTimeoutMinutes { get; init; }
    /// <summary>S3 prefix for driver and executor logs.</summary>
    public string? LogUri { get; init; }
    public Dictionary<string, string>? Tags { get; init; }
}

/// <summary>
/// Runs a <see cref="SparkSqlScript"/> on EMR Serverless (<c>StartJobRun</c>, Spark submit driver): stages the driver and
/// statements with <see cref="SparkScriptStager"/>, then submits the driver with <c>--statements</c>.
/// </summary>
public sealed class EmrServerlessSqlJobRunner(IAmazonEMRServerless emr, SparkScriptStager stager, EmrServerlessJobOptions options) : ISparkSqlJobRunner
{
    public async Task<SparkJobRun> SubmitAsync(SparkSqlScript script, string? runName = null, CancellationToken ct = default)
    {
        var driver = await stager.StageDriverAsync(ct);
        var statements = await stager.StageStatementsAsync(script, ct);
        var request = BuildRequest(driver, statements, runName);
        var response = await emr.StartJobRunAsync(request, ct);
        return new SparkJobRun(response.JobRunId, SparkJobState.Pending, null, statements);
    }

    /// <summary>The <c>StartJobRun</c> request for staged files (public so callers can inspect or adapt it).</summary>
    public StartJobRunRequest BuildRequest(string driverLocation, string statementsLocation, string? runName = null)
    {
        var submit = options.Spark.ToSparkSubmitParameters();
        if (!string.IsNullOrWhiteSpace(options.ExtraSparkSubmitParameters)) submit += " " + options.ExtraSparkSubmitParameters.Trim();
        var request = new StartJobRunRequest
        {
            ApplicationId = options.ApplicationId,
            ExecutionRoleArn = options.ExecutionRoleArn,
            Name = runName ?? "iceberg-net-sql",
            ClientToken = Guid.NewGuid().ToString("N"),
            JobDriver = new JobDriver
            {
                SparkSubmit = new SparkSubmit
                {
                    EntryPoint = driverLocation,
                    EntryPointArguments = ["--statements", statementsLocation],
                    SparkSubmitParameters = submit,
                },
            },
            ExecutionTimeoutMinutes = options.ExecutionTimeoutMinutes,
            Tags = options.Tags,
        };
        if (options.LogUri is { } logs)
            request.ConfigurationOverrides = new ConfigurationOverrides
            {
                MonitoringConfiguration = new MonitoringConfiguration { S3MonitoringConfiguration = new S3MonitoringConfiguration { LogUri = logs } },
            };
        return request;
    }

    public async Task<SparkJobRun> GetAsync(string runId, CancellationToken ct = default)
    {
        var run = (await emr.GetJobRunAsync(new GetJobRunRequest { ApplicationId = options.ApplicationId, JobRunId = runId }, ct)).JobRun;
        return new SparkJobRun(runId, Map(run.State), run.StateDetails);
    }

    public Task CancelAsync(string runId, CancellationToken ct = default) =>
        emr.CancelJobRunAsync(new CancelJobRunRequest { ApplicationId = options.ApplicationId, JobRunId = runId }, ct);

    internal static SparkJobState Map(EmrJobRunState? state) => state?.Value switch
    {
        "SUCCESS" => SparkJobState.Succeeded,
        "FAILED" => SparkJobState.Failed,
        "CANCELLED" => SparkJobState.Cancelled,
        "RUNNING" or "CANCELLING" => SparkJobState.Running,
        _ => SparkJobState.Pending, // SUBMITTED, PENDING, SCHEDULED, QUEUED
    };
}
