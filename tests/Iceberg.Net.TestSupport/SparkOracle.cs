using System.Diagnostics;
using System.Text.Json.Nodes;

namespace Iceberg.Net.TestSupport;

/// <summary>
/// Spark 4 + Iceberg Java (the reference implementation) as an oracle, run as <c>docker run apache/spark</c> with the
/// Iceberg runtime jar (fetched once into emulation/.cache by emulation/up.sh --spark). Tables to be read must live
/// under <see cref="SharedRoot"/>, which is mounted into the container at the same absolute path, so the
/// <c>file://</c> locations in their metadata resolve identically on both sides.
/// JVM start-up dominates, so <see cref="ReadTablesAsync"/> describes many tables per invocation.
/// </summary>
public static class SparkOracle
{
    public static string Image => Environment.GetEnvironmentVariable("ICEBERG_SPARK_IMAGE") ?? "apache/spark:4.0.1";
    public const string JarName = "iceberg-spark-runtime-4.0_2.13-1.12.0.jar";

    /// <summary>Host directory shared with the container at the same path.</summary>
    public static string SharedRoot { get; } = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-spark-oracle");

    private static readonly Lazy<string?> Skip = new(ComputeSkipReason);
    public static string? SkipReason => Skip.Value;

    private static string Jar => Environment.GetEnvironmentVariable("ICEBERG_SPARK_JAR")
        ?? Path.Combine(TestEnvironment.RepoDir(), "emulation", ".cache", JarName);

    private static string? ComputeSkipReason()
    {
        if (Environment.GetEnvironmentVariable("ICEBERG_SPARK") == "0") return "Spark oracle disabled (ICEBERG_SPARK=0)";
        if (!File.Exists(Jar)) return $"Spark oracle unavailable: {Jar} missing (run emulation/up.sh --spark)";
        try
        {
            var (code, _, _) = Run("docker", ["image", "inspect", Image], TimeSpan.FromSeconds(30)).GetAwaiter().GetResult();
            return code == 0 ? null : $"Spark oracle unavailable: image {Image} not present (docker pull {Image})";
        }
        catch (Exception e) when (e is System.ComponentModel.Win32Exception or InvalidOperationException or TimeoutException)
        {
            return $"Spark oracle unavailable: docker not usable ({e.Message})";
        }
    }

    /// <summary>Describe each table (rows, snapshots, delete files, and row lineage for v3) as Spark reads it.</summary>
    public static async Task<JsonObject> ReadTablesAsync(IEnumerable<string> metadataLocations)
    {
        if (SkipReason is not null) throw new InvalidOperationException(SkipReason);
        Directory.CreateDirectory(SharedRoot);
        var interop = Path.Combine(TestEnvironment.RepoDir(), "interop");
        string[] args =
        [
            "run", "--rm", "--user", "root", "--network", "none", "--hostname", "localhost", "-e", "SPARK_LOCAL_IP=127.0.0.1",
            "-v", $"{Path.GetDirectoryName(Jar)}:/jars:ro", "-v", $"{interop}:/interop:ro", "-v", $"{SharedRoot}:{SharedRoot}",
            Image, "/opt/spark/bin/spark-submit", "--jars", $"/jars/{Path.GetFileName(Jar)}", "/interop/spark_oracle.py",
            "read-tables", .. metadataLocations,
        ];
        var (code, stdout, stderr) = await Run("docker", args, TimeSpan.FromMinutes(10));
        var marker = stdout.LastIndexOf("@@ORACLE@@", StringComparison.Ordinal);
        if (code != 0 || marker < 0)
            throw new InvalidOperationException($"Spark oracle failed ({code}): {string.Join('\n', stderr.Split('\n').TakeLast(30))}");
        return JsonNode.Parse(stdout[(marker + "@@ORACLE@@".Length)..])!.AsObject();
    }

    private static async Task<(int Code, string Stdout, string Stderr)> Run(string file, IEnumerable<string> args, TimeSpan timeout)
    {
        var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(timeout);
        return (p.ExitCode, await stdout, await stderr);
    }
}
