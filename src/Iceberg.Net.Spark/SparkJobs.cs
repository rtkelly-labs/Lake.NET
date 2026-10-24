using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Iceberg.Net.IO;

namespace Iceberg.Net.Spark;

/// <summary>Statements run in order by one Spark job (see <c>Scripts/iceberg_sql_driver.py</c>).</summary>
public sealed class SparkSqlScript
{
    private readonly List<string> _statements = [];

    public SparkSqlScript() { }
    public SparkSqlScript(IEnumerable<string> statements) { foreach (var s in statements) Add(s); }

    public IReadOnlyList<string> Statements => _statements;

    public SparkSqlScript Add(string statement)
    {
        ArgumentException.ThrowIfNullOrEmpty(statement);
        _statements.Add(statement.Trim().TrimEnd(';'));
        return this;
    }

    public SparkSqlScript Add(SparkProcedureCall call) => Add(call.ToSql());

    /// <summary>The JSON array of statements the driver reads.</summary>
    public string ToJson() => JsonSerializer.Serialize(_statements);

    public override string ToString() => string.Join(";\n", _statements) + ";";
}

/// <summary>Spark configuration (<c>--conf</c>) for an Iceberg catalog and the Iceberg SQL extensions (needed for CALL).</summary>
public sealed class SparkCatalogConfig
{
    public const string Extensions = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
    private readonly SortedDictionary<string, string> _conf = new(StringComparer.Ordinal) { ["spark.sql.extensions"] = Extensions };

    public IReadOnlyDictionary<string, string> Conf => _conf;

    public SparkCatalogConfig Set(string key, string value)
    {
        _conf[key] = value;
        return this;
    }

    private SparkCatalogConfig Catalog(string name, IEnumerable<KeyValuePair<string, string>> properties)
    {
        Set($"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog");
        foreach (var (k, v) in properties) Set($"spark.sql.catalog.{name}.{k}", v);
        return this;
    }

    /// <summary>An AWS Glue Data Catalog as Spark catalog <paramref name="name"/> (S3FileIO).</summary>
    public static SparkCatalogConfig Glue(string name, string warehouse) => new SparkCatalogConfig().Catalog(name,
    [
        new("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
        new("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
        new("warehouse", warehouse),
    ]);

    /// <summary>An Iceberg REST catalog (Polaris, Glue/S3 Tables REST endpoints with SigV4, …).</summary>
    public static SparkCatalogConfig Rest(string name, Uri uri, string? warehouse = null, IReadOnlyDictionary<string, string>? properties = null)
    {
        var props = new List<KeyValuePair<string, string>> { new("type", "rest"), new("uri", uri.ToString()) };
        if (warehouse is not null) props.Add(new("warehouse", warehouse));
        if (properties is not null) props.AddRange(properties);
        return new SparkCatalogConfig().Catalog(name, props);
    }

    /// <summary>A Hadoop (file-system) catalog: local tests and the Spark oracle.</summary>
    public static SparkCatalogConfig Hadoop(string name, string warehouse) =>
        new SparkCatalogConfig().Catalog(name, [new("type", "hadoop"), new("warehouse", warehouse)]);

    /// <summary><c>--conf k=v --conf …</c> (EMR Serverless <c>sparkSubmitParameters</c>, spark-submit).</summary>
    public IEnumerable<string> ToArguments()
    {
        foreach (var (k, v) in _conf)
        {
            if (k.Any(char.IsWhiteSpace) || v.Any(char.IsWhiteSpace))
                throw new ArgumentException($"Spark conf '{k}' contains whitespace, which job parameters cannot carry");
            yield return "--conf";
            yield return $"{k}={v}";
        }
    }

    public string ToSparkSubmitParameters() => string.Join(" ", ToArguments());

    /// <summary>
    /// The value of a Glue job's single <c>--conf</c> argument: Glue accepts one, so further settings are chained as
    /// <c>k1=v1 --conf k2=v2</c> (the documented way to pass several).
    /// </summary>
    public string ToGlueConfArgument() => string.Join(" --conf ", ToArguments().Where((_, i) => i % 2 == 1));
}

public enum SparkJobState { Pending, Running, Succeeded, Failed, Cancelled }

/// <summary>A submitted job: its run ID on the service, its state, and the details the service gave.</summary>
public sealed record SparkJobRun(string RunId, SparkJobState State, string? Details = null, string? StatementsLocation = null)
{
    public bool IsFinished => State is SparkJobState.Succeeded or SparkJobState.Failed or SparkJobState.Cancelled;
}

/// <summary>Runs a <see cref="SparkSqlScript"/> as a Spark job on a managed service.</summary>
public interface ISparkSqlJobRunner
{
    /// <summary>Stage the script and start a run.</summary>
    Task<SparkJobRun> SubmitAsync(SparkSqlScript script, string? runName = null, CancellationToken ct = default);
    Task<SparkJobRun> GetAsync(string runId, CancellationToken ct = default);
    Task CancelAsync(string runId, CancellationToken ct = default);
}

public static class SparkJobRunnerExtensions
{
    /// <summary>Poll until the run finishes; throws <see cref="TimeoutException"/> after <paramref name="timeout"/>.</summary>
    public static async Task<SparkJobRun> WaitAsync(this ISparkSqlJobRunner runner, string runId, TimeSpan? timeout = null, TimeSpan? pollInterval = null,
        CancellationToken ct = default)
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromHours(2));
        var interval = pollInterval ?? TimeSpan.FromSeconds(15);
        while (true)
        {
            var run = await runner.GetAsync(runId, ct);
            if (run.IsFinished) return run;
            if (DateTimeOffset.UtcNow >= deadline) throw new TimeoutException($"Spark job {runId} still {run.State} after {timeout}");
            await Task.Delay(interval, ct);
        }
    }

    /// <summary>Submit, wait, and throw <see cref="SparkJobFailedException"/> unless the run succeeded.</summary>
    public static async Task<SparkJobRun> RunAsync(this ISparkSqlJobRunner runner, SparkSqlScript script, string? runName = null, TimeSpan? timeout = null,
        TimeSpan? pollInterval = null, CancellationToken ct = default)
    {
        var started = await runner.SubmitAsync(script, runName, ct);
        var run = await runner.WaitAsync(started.RunId, timeout, pollInterval, ct);
        return run.State == SparkJobState.Succeeded ? run with { StatementsLocation = started.StatementsLocation } : throw new SparkJobFailedException(run);
    }
}

public sealed class SparkJobFailedException(SparkJobRun run) : Exception($"Spark job {run.RunId} ended {run.State}: {run.Details}")
{
    public SparkJobRun Run { get; } = run;
}

/// <summary>
/// Writes the driver script and a run's statements under a staging prefix (e.g. <c>s3://bucket/iceberg-jobs/</c>).
/// The driver's name carries a hash of its content, so it is written once per version and never overwritten.
/// </summary>
public sealed class SparkScriptStager(IFileIO io, string stagingPrefix)
{
    public static string DriverScript { get; } = LoadDriver();

    private static string LoadDriver()
    {
        using var stream = typeof(SparkScriptStager).Assembly.GetManifestResourceStream("Iceberg.Net.Spark.iceberg_sql_driver.py")
            ?? throw new InvalidOperationException("The embedded Spark SQL driver is missing");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    private string Prefix => stagingPrefix.EndsWith('/') ? stagingPrefix : stagingPrefix + "/";

    /// <summary>The driver's file name: <c>iceberg_sql_driver-{content hash}.py</c>.</summary>
    public static string DriverFileName { get; } = "iceberg_sql_driver-" +
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(DriverScript)))[..12].ToLowerInvariant() + ".py";

    public string DriverLocation => Prefix + DriverFileName;

    public async Task<string> StageDriverAsync(CancellationToken ct = default)
    {
        if (!await io.ExistsAsync(DriverLocation, ct)) await io.WriteAsync(DriverLocation, Encoding.UTF8.GetBytes(DriverScript), overwrite: true, ct);
        return DriverLocation;
    }

    /// <summary>Write <paramref name="script"/>'s statements to a new file; returns its location.</summary>
    public async Task<string> StageStatementsAsync(SparkSqlScript script, CancellationToken ct = default)
    {
        if (script.Statements.Count == 0) throw new ArgumentException("The script has no statements", nameof(script));
        var location = $"{Prefix}statements/{DateTime.UtcNow.ToString("yyyyMMddTHHmmss", CultureInfo.InvariantCulture)}-{Guid.NewGuid():N}.json";
        await io.WriteAsync(location, Encoding.UTF8.GetBytes(script.ToJson()), overwrite: false, ct);
        return location;
    }
}
