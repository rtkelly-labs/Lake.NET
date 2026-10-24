using System.Diagnostics;
using System.Text.Json.Nodes;
using Amazon.EMRServerless;
using Amazon.Glue;
using Emr = Amazon.EMRServerless.Model;
using Glue = Amazon.Glue.Model;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// What EMR Serverless or Glue would do with a job run that moto accepted (moto records runs but executes nothing):
/// read the run back from moto through the SDK, and <c>spark-submit</c> exactly what it holds — the entry point /
/// script and its arguments, which are the <c>s3://</c> URIs the runner staged in moto S3, and the Spark
/// configuration — in the Spark oracle image (Spark 4.0.1 + Iceberg 1.12).
///
/// The emulator adds only what the managed service's environment provides: Iceberg's AWS integration
/// (<c>iceberg-aws-bundle</c>, which EMR 7 and Glue's <c>--datalake-formats iceberg</c> ship), an <c>s3://</c>
/// file system (EMRFS there; S3A on moto here), and endpoint overrides that send each Iceberg catalog's Glue and S3
/// calls to moto. The jars come from <c>emulation/up.sh --spark</c>.
/// </summary>
internal static class SparkJobServiceEmulator
{
    private static readonly string[] AwsJars =
    [
        "iceberg-aws-bundle-1.12.0.jar", "hadoop-aws-3.4.1.jar", "s3-transfer-manager-2.54.17.jar", "apache-client-2.54.17.jar",
    ];

    private static string JarDirectory => Path.GetDirectoryName(Environment.GetEnvironmentVariable("ICEBERG_SPARK_JAR")
        ?? Path.Combine(TestEnvironment.RepoDir(), "emulation", ".cache", SparkOracle.JarName))!;

    private static readonly Lazy<string?> Skip = new(() =>
        TestEnvironment.Current.Target != Target.Emulation ? "the job service emulator runs in emulation only (on AWS the service runs the job)"
        : TestEnvironment.Current.SkipReason
        ?? SparkOracle.SkipReason
        ?? (AwsJars.FirstOrDefault(j => !File.Exists(Path.Combine(JarDirectory, j))) is { } missing
            ? $"Spark job emulation unavailable: {Path.Combine(JarDirectory, missing)} missing (run emulation/up.sh --spark)"
            : null));

    public static string? SkipReason => Skip.Value;

    /// <summary>Run an EMR Serverless job run as EMR would: <c>spark-submit {sparkSubmitParameters} {entryPoint} {entryPointArguments}</c>.</summary>
    public static async Task<JsonArray> RunEmrServerlessJobAsync(IAmazonEMRServerless emr, string applicationId, string jobRunId)
    {
        var run = (await emr.GetJobRunAsync(new Emr.GetJobRunRequest { ApplicationId = applicationId, JobRunId = jobRunId })).JobRun;
        var submit = run.JobDriver?.SparkSubmit ?? throw new InvalidOperationException($"EMR job run {jobRunId} has no sparkSubmit driver");
        // EMR Serverless splits sparkSubmitParameters on whitespace into spark-submit options.
        var parameters = (submit.SparkSubmitParameters ?? "").Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return await SparkSubmitAsync(parameters, submit.EntryPoint, submit.EntryPointArguments ?? []);
    }

    /// <summary>
    /// Run a Glue job run as Glue would: the job's script with the job's default arguments overlaid by the run's,
    /// passed to the script as <c>--name value</c> pairs (with <c>--JOB_NAME</c> and <c>--JOB_RUN_ID</c>); the
    /// <c>--conf</c> argument becomes Spark configuration. Refuses what Glue could not run as an Iceberg Spark job.
    /// </summary>
    public static async Task<JsonArray> RunGlueJobAsync(IAmazonGlue glue, string jobName, string runId)
    {
        var job = (await glue.GetJobAsync(new Glue.GetJobRequest { JobName = jobName })).Job;
        var run = (await glue.GetJobRunAsync(new Glue.GetJobRunRequest { JobName = jobName, RunId = runId })).JobRun;
        if (job.Command?.Name != "glueetl") throw new InvalidOperationException($"Glue job {jobName} is a {job.Command?.Name} job, not a Spark (glueetl) job");
        if (job.Command.PythonVersion != "3") throw new InvalidOperationException($"Glue job {jobName} needs PythonVersion 3, has {job.Command.PythonVersion}");
        var arguments = new SortedDictionary<string, string>(job.DefaultArguments ?? [], StringComparer.Ordinal);
        foreach (var (k, v) in run.Arguments ?? []) arguments[k] = v;
        if (!(arguments.GetValueOrDefault("--datalake-formats") ?? "").Split(',').Contains("iceberg"))
            throw new InvalidOperationException($"Glue job {jobName} does not set --datalake-formats iceberg: Glue would run it without Iceberg");
        string[] conf = arguments.TryGetValue("--conf", out var c) ? ("--conf " + c).Split(' ', StringSplitOptions.RemoveEmptyEntries) : [];
        arguments["--JOB_NAME"] = jobName;
        arguments["--JOB_RUN_ID"] = runId;
        return await SparkSubmitAsync(conf, job.Command.ScriptLocation, arguments.SelectMany(a => new[] { a.Key, a.Value }).ToList());
    }

    private static async Task<JsonArray> SparkSubmitAsync(IReadOnlyList<string> jobParameters, string entryPoint, IEnumerable<string> entryPointArguments)
    {
        if (SkipReason is not null) throw new InvalidOperationException(SkipReason);
        var env = TestEnvironment.Current;
        var endpoint = env.S3Endpoint!;
        var catalogs = new List<string>();
        for (var i = 0; i + 1 < jobParameters.Count; i++)
            if (jobParameters[i] == "--conf" && jobParameters[i + 1].Split('=', 2) is [var key, "org.apache.iceberg.spark.SparkCatalog"] && key.StartsWith("spark.sql.catalog.", StringComparison.Ordinal))
                catalogs.Add(key["spark.sql.catalog.".Length..]);
        var service = new List<string>
        {
            "spark.ui.enabled=false", "spark.sql.shuffle.partitions=2", "spark.log.level=ERROR",
            // The service's s3:// file system (EMRFS, Glue's S3 connector): S3A on moto.
            "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem", $"spark.hadoop.fs.s3a.endpoint={endpoint}",
            "spark.hadoop.fs.s3a.path.style.access=true", $"spark.hadoop.fs.s3a.endpoint.region={env.Region}",
        };
        foreach (var catalog in catalogs)
            service.AddRange([
                $"spark.sql.catalog.{catalog}.glue.endpoint={env.GlueEndpoint}", $"spark.sql.catalog.{catalog}.s3.endpoint={endpoint}",
                $"spark.sql.catalog.{catalog}.s3.path-style-access=true", $"spark.sql.catalog.{catalog}.client.region={env.Region}",
            ]);
        var jars = string.Join(':', new[] { SparkOracle.JarName }.Concat(AwsJars).Select(j => "/jars/" + j));
        List<string> args =
        [
            "run", "--rm", "--network", "host", "-e", "SPARK_LOCAL_IP=127.0.0.1",
            // The job's execution role: moto accepts any credentials.
            "-e", "AWS_ACCESS_KEY_ID=" + (Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID") ?? "test"),
            "-e", "AWS_SECRET_ACCESS_KEY=" + (Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY") ?? "test"),
            "-e", "AWS_REGION=" + env.Region,
            "-v", $"{JarDirectory}:/jars:ro",
            SparkOracle.Image, "/opt/spark/bin/spark-submit", "--master", "local[2]",
            // On the launcher's class path, so spark-submit itself can fetch an s3:// entry point.
            "--driver-class-path", jars,
            .. service.SelectMany(s => new[] { "--conf", s }),
            .. jobParameters,
            entryPoint, .. entryPointArguments,
        ];
        var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(10));
        var output = await stdout;
        var marker = output.LastIndexOf("@@RESULTS@@", StringComparison.Ordinal);
        if (process.ExitCode != 0 || marker < 0)
            throw new SparkJobEmulationException(process.ExitCode,
                string.Join('\n', (await stderr + "\n" + output).Split('\n').Where(l => !l.StartsWith("\tat ", StringComparison.Ordinal))));
        return JsonNode.Parse(output[(marker + "@@RESULTS@@".Length)..])!.AsArray();
    }
}

/// <summary>The emulated job failed: the job run would have ended FAILED on the service.</summary>
internal sealed class SparkJobEmulationException(int exitCode, string log)
    : Exception($"Spark job failed (exit {exitCode}):\n{string.Join('\n', log.Split('\n').TakeLast(60))}")
{
    /// <summary>The driver's and Spark's whole output, without Java stack frames.</summary>
    public string Log { get; } = log;
}
