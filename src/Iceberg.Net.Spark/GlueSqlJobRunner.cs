using Amazon.Glue;
using Amazon.Glue.Model;
using GlueJobRunState = Amazon.Glue.JobRunState;

namespace Iceberg.Net.Spark;

/// <summary>An AWS Glue Spark job (Glue 4.0+, <c>--datalake-formats iceberg</c>) whose script is the staged driver.</summary>
public sealed record GlueJobOptions
{
    /// <summary>The job to start; <see cref="GlueSqlJobRunner.EnsureJobAsync"/> creates it if missing.</summary>
    public required string JobName { get; init; }
    public required string StagingPrefix { get; init; }
    public required SparkCatalogConfig Spark { get; init; }
    /// <summary>The job's IAM role (needed only to create the job).</summary>
    public string? RoleArn { get; init; }
    public string GlueVersion { get; init; } = "5.0";
    public string WorkerType { get; init; } = "G.1X";
    public int NumberOfWorkers { get; init; } = 2;
    /// <summary>Run timeout in minutes (Glue's default is 48 hours).</summary>
    public int? TimeoutMinutes { get; init; }
}

/// <summary>
/// Runs a <see cref="SparkSqlScript"/> as an AWS Glue job run (<c>StartJobRun</c>): the job's script is the staged driver,
/// and each run passes its statements file as <c>--statements</c>.
/// </summary>
public sealed class GlueSqlJobRunner(IAmazonGlue glue, SparkScriptStager stager, GlueJobOptions options) : ISparkSqlJobRunner
{
    /// <summary>Default arguments of the job: Iceberg on, the catalog configuration.</summary>
    public Dictionary<string, string> DefaultArguments() => new()
    {
        ["--datalake-formats"] = "iceberg",
        ["--conf"] = options.Spark.ToGlueConfArgument(),
        ["--job-language"] = "python",
    };

    /// <summary>Create the job (script = the staged driver) if it does not exist, or point it at the current driver.</summary>
    public async Task EnsureJobAsync(CancellationToken ct = default)
    {
        var driver = await stager.StageDriverAsync(ct);
        var command = new JobCommand { Name = "glueetl", PythonVersion = "3", ScriptLocation = driver };
        try
        {
            var existing = (await glue.GetJobAsync(new GetJobRequest { JobName = options.JobName }, ct)).Job;
            if (existing.Command?.ScriptLocation == driver) return;
            await glue.UpdateJobAsync(new UpdateJobRequest
            {
                JobName = options.JobName,
                JobUpdate = new JobUpdate
                {
                    Role = existing.Role,
                    Command = command,
                    GlueVersion = existing.GlueVersion ?? options.GlueVersion,
                    DefaultArguments = existing.DefaultArguments ?? DefaultArguments(),
                    WorkerType = existing.WorkerType,
                    NumberOfWorkers = existing.NumberOfWorkers,
                },
            }, ct);
        }
        catch (EntityNotFoundException)
        {
            await glue.CreateJobAsync(new CreateJobRequest
            {
                Name = options.JobName,
                Role = options.RoleArn ?? throw new InvalidOperationException($"Glue job {options.JobName} does not exist; set RoleArn to create it"),
                Command = command,
                GlueVersion = options.GlueVersion,
                WorkerType = options.WorkerType,
                NumberOfWorkers = options.NumberOfWorkers,
                DefaultArguments = DefaultArguments(),
            }, ct);
        }
    }

    public async Task<SparkJobRun> SubmitAsync(SparkSqlScript script, string? runName = null, CancellationToken ct = default)
    {
        var statements = await stager.StageStatementsAsync(script, ct);
        var response = await glue.StartJobRunAsync(BuildRequest(statements), ct);
        return new SparkJobRun(response.JobRunId, SparkJobState.Pending, null, statements);
    }

    /// <summary>The <c>StartJobRun</c> request for a staged statements file.</summary>
    public StartJobRunRequest BuildRequest(string statementsLocation) => new()
    {
        JobName = options.JobName,
        Arguments = new Dictionary<string, string> { ["--statements"] = statementsLocation },
        Timeout = options.TimeoutMinutes,
    };

    public async Task<SparkJobRun> GetAsync(string runId, CancellationToken ct = default)
    {
        var run = (await glue.GetJobRunAsync(new GetJobRunRequest { JobName = options.JobName, RunId = runId }, ct)).JobRun;
        return new SparkJobRun(runId, Map(run.JobRunState), run.ErrorMessage);
    }

    public Task CancelAsync(string runId, CancellationToken ct = default) =>
        glue.BatchStopJobRunAsync(new BatchStopJobRunRequest { JobName = options.JobName, JobRunIds = [runId] }, ct);

    internal static SparkJobState Map(GlueJobRunState? state) => state?.Value switch
    {
        "SUCCEEDED" => SparkJobState.Succeeded,
        "FAILED" or "TIMEOUT" or "ERROR" or "EXPIRED" => SparkJobState.Failed,
        "STOPPED" => SparkJobState.Cancelled,
        "RUNNING" or "STOPPING" => SparkJobState.Running,
        _ => SparkJobState.Pending, // STARTING, WAITING
    };
}
