using Xunit;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;

namespace Iceberg.Net.TestSupport;

public enum Target { Emulation, Aws }

/// <summary>
/// Where the regression suite runs. Everything is driven by environment variables so the same binaries
/// run against the local emulators (default) and against a real AWS account. See docs/regression-suite.md.
/// </summary>
public sealed class TestEnvironment
{
    public static readonly TestEnvironment Current = new();

    public Target Target { get; }
    public string Region { get; }
    public string? S3Endpoint { get; }
    public string? GlueEndpoint { get; }
    public Uri? RestUri { get; }
    /// <summary>AWS mode: Glue Iceberg REST endpoint warehouse (account ID) for the "rest" catalog kind.</summary>
    public string? GlueRestWarehouse { get; }
    public string Warehouse { get; }
    public string? AthenaWorkGroup { get; }
    public string? AthenaOutput { get; }
    public string RunId { get; } = $"{DateTime.UtcNow:yyyyMMddHHmm}_{Guid.NewGuid().ToString("N")[..6]}";

    /// <summary>Why the suite (or a capability) cannot run, or null when it can.</summary>
    public string? SkipReason { get; }
    public string? RestSkipReason { get; }
    /// <summary>Apache Polaris (emulation/up.sh --polaris): a second REST server implementation.</summary>
    public string JdbcDatabase { get; } = Env("ICEBERG_JDBC_SQLITE") ?? Path.Combine(Path.GetTempPath(), "iceberg-dotnet-jdbc", "catalog.db");
    public Uri PolarisUri { get; } = new(Env("ICEBERG_POLARIS_URI") ?? "http://localhost:8182/api/catalog");
    public string? PolarisSkipReason { get; }
    /// <summary>Apache Gravitino's Iceberg REST server (emulation/up.sh --gravitino): a third REST server, with scan planning.</summary>
    public Uri GravitinoUri { get; } = new(Env("ICEBERG_GRAVITINO_URI") ?? "http://localhost:9001/iceberg");
    public string? GravitinoSkipReason { get; } = "Gravitino only runs in emulation";
    /// <summary>Project Nessie's Iceberg REST API (emulation/up.sh --nessie): a fourth REST server.</summary>
    public Uri NessieUri { get; } = new(Env("ICEBERG_NESSIE_URI") ?? "http://localhost:19120/iceberg");
    public string? NessieSkipReason { get; } = "Nessie only runs in emulation";

    /// <summary>Lakekeeper (emulation/up.sh --lakekeeper): a fifth REST server, and the one not built on Iceberg Java.</summary>
    public Uri LakekeeperUri { get; } = new(Env("ICEBERG_LAKEKEEPER_URI") ?? "http://localhost:8283/catalog");
    public string? LakekeeperSkipReason { get; } = "Lakekeeper only runs in emulation";
    public string? OracleSkipReason { get; }
    /// <summary>Oracle availability for tests that only use the local filesystem (no emulator or AWS).</summary>
    public string? LocalOracleSkipReason { get; }
    public string? AthenaSkipReason { get; }

    private static string? Env(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : null;

    private TestEnvironment()
    {
        Target = Env("ICEBERG_TEST_TARGET")?.ToLowerInvariant() == "aws" ? Target.Aws : Target.Emulation;
        Region = Env("AWS_REGION") ?? "eu-west-1";
        var required = Env("ICEBERG_REQUIRE_TARGET") == "1";

        if (Target == Target.Emulation)
        {
            S3Endpoint = Env("ICEBERG_S3_ENDPOINT") ?? $"http://localhost:{Env("MOTO_PORT") ?? "5000"}";
            GlueEndpoint = Env("ICEBERG_GLUE_ENDPOINT") ?? S3Endpoint;
            RestUri = new Uri(Env("ICEBERG_REST_URI") ?? "http://localhost:8181");
            Warehouse = Env("ICEBERG_WAREHOUSE") ?? "s3://iceberg-regression/warehouse";
            // Emulators accept any credentials; set them process-wide so the SDK chain and the Python oracle agree.
            foreach (var (k, v) in new[] { ("AWS_ACCESS_KEY_ID", "test"), ("AWS_SECRET_ACCESS_KEY", "test"), ("AWS_REGION", Region) })
                if (Env(k) is null) Environment.SetEnvironmentVariable(k, v);
            Environment.SetEnvironmentVariable("ICEBERG_S3_ENDPOINT", S3Endpoint);
            Environment.SetEnvironmentVariable("ICEBERG_GLUE_ENDPOINT", GlueEndpoint);
            Environment.SetEnvironmentVariable("ICEBERG_REST_URI", RestUri.ToString());

            SkipReason = Reachable(new Uri(S3Endpoint)) ? null : $"S3/Glue emulator not reachable at {S3Endpoint} (run emulation/up.sh)";
            RestSkipReason = SkipReason ?? (Reachable(RestUri) ? null : $"REST catalog fixture not reachable at {RestUri}");
            PolarisSkipReason = SkipReason ?? (Reachable(PolarisUri) ? null : $"Polaris not reachable at {PolarisUri} (run emulation/up.sh --polaris)");
            GravitinoSkipReason = SkipReason ?? (Reachable(GravitinoUri) ? null : $"Gravitino not reachable at {GravitinoUri} (run emulation/up.sh --gravitino)");
            NessieSkipReason = SkipReason ?? (Reachable(NessieUri) ? null : $"Nessie not reachable at {NessieUri} (run emulation/up.sh --nessie)");
            LakekeeperSkipReason = SkipReason ?? (Reachable(LakekeeperUri) ? null : $"Lakekeeper not reachable at {LakekeeperUri} (run emulation/up.sh --lakekeeper)");
            AthenaSkipReason = "Athena only runs with ICEBERG_TEST_TARGET=aws";
        }
        else
        {
            Warehouse = Env("ICEBERG_WAREHOUSE") ?? "";
            RestUri = Env("ICEBERG_REST_URI") is { } rest ? new Uri(rest) : null;
            AthenaWorkGroup = Env("ICEBERG_ATHENA_WORKGROUP");
            AthenaOutput = Env("ICEBERG_ATHENA_OUTPUT");
            SkipReason = Warehouse.StartsWith("s3://", StringComparison.Ordinal) ? null : "ICEBERG_WAREHOUSE=s3://bucket/prefix is required for AWS runs";
            // The Glue/S3 Tables REST endpoints need SigV4 request signing, which RestCatalogOptions.Handler can supply but the suite does not yet.
            // REST against AWS: the Glue Iceberg REST endpoint (SigV4) for an account, or any REST URI.
            GlueRestWarehouse = Env("ICEBERG_GLUE_REST_WAREHOUSE");
            RestSkipReason = SkipReason ?? (RestUri is null && GlueRestWarehouse is null ? "Set ICEBERG_GLUE_REST_WAREHOUSE (account id) or ICEBERG_REST_URI for REST tests against AWS" : null);
            PolarisSkipReason = "Polaris runs in emulation only";
            AthenaSkipReason = SkipReason ?? (AthenaWorkGroup is null ? "Set ICEBERG_ATHENA_WORKGROUP (and optionally ICEBERG_ATHENA_OUTPUT)" : null);
        }

        Environment.SetEnvironmentVariable("ICEBERG_WAREHOUSE", Warehouse);
        // The JDBC catalog's SQLite database, shared with the PyIceberg oracle's SqlCatalog through this variable.
        Directory.CreateDirectory(Path.GetDirectoryName(JdbcDatabase)!);
        Environment.SetEnvironmentVariable("ICEBERG_JDBC_SQLITE", JdbcDatabase);
        LocalOracleSkipReason = OracleCommand() is null ? "PyIceberg oracle unavailable: set ICEBERG_PYTHON to an interpreter with interop/pyproject.toml installed, or install uv" : null;
        OracleSkipReason = SkipReason ?? LocalOracleSkipReason;
        if (required && SkipReason is not null) throw new InvalidOperationException($"ICEBERG_REQUIRE_TARGET=1 but: {SkipReason}");
    }

    internal static bool Reachable(Uri uri)
    {
        try
        {
            using var client = new TcpClient();
            return client.ConnectAsync(uri.Host, uri.Port).Wait(TimeSpan.FromSeconds(2));
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <summary>Glue database names must be lowercase [a-z0-9_].</summary>
    public string NewNamespace(string purpose) => $"regr_{purpose}_{RunId}".ToLowerInvariant();

    public S3FileIOOptions S3Options => new()
    {
        Endpoint = S3Endpoint,
        PathStyleAccess = S3Endpoint is not null,
        Region = Region,
    };

    public IReadOnlyDictionary<string, string> S3Properties => S3Endpoint is null
        ? new Dictionary<string, string> { ["client.region"] = Region }
        : new Dictionary<string, string> { ["s3.endpoint"] = S3Endpoint, ["s3.path-style-access"] = "true", ["client.region"] = Region };

    /// <summary>
    /// Catalog kinds a test assembly hosts itself, e.g. "dotnet-rest" (this repository's REST server, started by the
    /// regression suite). They store data in the same S3 as the other kinds, so they skip with <see cref="SkipReason"/>.
    /// </summary>
    public static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Func<Task<ICatalog>>> HostedCatalogs = new();

    public async Task<ICatalog> CatalogAsync(string kind) => kind switch
    {
        _ when HostedCatalogs.TryGetValue(kind, out var connect) => await connect(),
        "glue" => GlueCatalog.Create(new S3FileIO(S3Options), new GlueCatalogOptions { Endpoint = GlueEndpoint, Region = Region, Warehouse = Warehouse.TrimEnd('/') + "/glue" }),
        "rest" when GlueRestWarehouse is not null && RestUri is null => await RestCatalog.ConnectAsync(RestCatalogOptions.ForGlue(Region, GlueRestWarehouse)),
        "rest" => await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = RestUri!, Properties = S3Properties },
            props => new S3FileIO(S3FileIOOptions.FromProperties(props))),
        // Bootstrapped by emulation/polaris_setup.py. No credential vending (no STS): clients bring S3 credentials.
        "polaris" => await RestCatalog.ConnectAsync(new RestCatalogOptions
            {
                Uri = PolarisUri, Warehouse = "regression", Credential = "root:s3cr3t", Scope = "PRINCIPAL_ROLE:ALL",
                AccessDelegation = null, Properties = S3Properties,
            },
            props => new S3FileIO(S3FileIOOptions.FromProperties(props)), "polaris"),
        // Nessie's Iceberg REST API (prefix "main" from its config); no request signing, clients bring S3 credentials.
        "nessie" => await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = NessieUri, AccessDelegation = null, Properties = S3Properties },
            props => new S3FileIO(S3FileIOOptions.FromProperties(props)), "nessie"),
        // Lakekeeper's warehouse "regression" (emulation/lakekeeper_setup.py); no STS, so clients bring S3 credentials.
        "lakekeeper" => await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = LakekeeperUri, Warehouse = "regression", AccessDelegation = null, Properties = S3Properties },
            props => new S3FileIO(S3FileIOOptions.FromProperties(props)), "lakekeeper"),
        // Gravitino's Iceberg REST service (in-memory backend, S3FileIO on moto); clients bring S3 credentials.
        "gravitino" => await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = GravitinoUri, AccessDelegation = null, Properties = S3Properties },
            props => new S3FileIO(S3FileIOOptions.FromProperties(props)), "gravitino"),
        // Java JdbcCatalog / PyIceberg SqlCatalog tables in SQLite, data in S3 (moto or AWS); catalog name "regression".
        "jdbc" => await JdbcAsync(),
        _ => throw new ArgumentException(kind),
    };

    private async Task<ICatalog> JdbcAsync()
    {
        var catalog = new JdbcCatalog(() => new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={JdbcDatabase};Default Timeout=30"),
            Warehouse.TrimEnd('/') + "/jdbc", new S3FileIO(S3Options), "regression", views: true);
        await catalog.InitializeAsync();
        return catalog;
    }

    /// <summary>
    /// Whether a catalog serves the table's snapshot history and metadata log. Nessie keeps history as Nessie commits:
    /// the metadata it serves carries only the current snapshot and an empty metadata-log.
    /// </summary>
    public static bool KeepsSnapshotHistory(string kind) => kind != "nessie";

    public string? CatalogSkipReason(string kind) => kind switch { "rest" => RestSkipReason, "polaris" => PolarisSkipReason, "gravitino" => GravitinoSkipReason, "nessie" => NessieSkipReason, "lakekeeper" => LakekeeperSkipReason, _ => SkipReason };

    // ---------- PyIceberg oracle ----------

    /// <summary>The iceberg-dotnet directory (the first ancestor of the test output containing interop/).</summary>
    public static string RepoDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "interop"))) dir = dir.Parent;
        return dir is null ? throw new DirectoryNotFoundException("interop/ not found above test output") : dir.FullName;
    }

    private static string InteropDir() => Path.Combine(RepoDir(), "interop");

    /// <summary>The interpreter (or uv) and the arguments that run the oracle script.</summary>
    private static (string File, string[] Prefix)? OracleCommand()
    {
        var script = Path.Combine(InteropDir(), "pyiceberg_oracle.py");
        if (Env("ICEBERG_PYTHON") is { } python) return (python, ["-I", script]);
        var uv = (Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator).Select(p => Path.Combine(p, "uv")).FirstOrDefault(File.Exists);
        return uv is null ? null : (uv, ["run", "--quiet", "--project", InteropDir(), "python", "-I", script]);
    }

    public async Task<JsonNode> OracleAsync(params string[] args) => JsonNode.Parse(await OracleRawAsync(args))!;

    /// <summary>Run the PyIceberg oracle and return its stdout (a JSON document).</summary>
    public async Task<string> OracleRawAsync(params string[] args)
    {
        var (file, prefix) = OracleCommand() ?? throw new InvalidOperationException(OracleSkipReason);
        // ArgumentList quotes each argument correctly (JSON arguments contain quotes and spaces).
        var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in prefix.Concat(args)) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(5));
        if (p.ExitCode != 0) throw new InvalidOperationException($"PyIceberg oracle failed ({p.ExitCode}): {await stderr}");
        return await stdout;
    }
}

/// <summary>Skips (with the reason) when the configured target or capability is unavailable, instead of failing.</summary>
public static class Require
{
    public static void Target() => Skip.If(TestEnvironment.Current.SkipReason is not null, TestEnvironment.Current.SkipReason);
    public static void Catalog(string kind) => Skip.If(TestEnvironment.Current.CatalogSkipReason(kind) is not null, TestEnvironment.Current.CatalogSkipReason(kind));
    public static void Oracle() => Skip.If(TestEnvironment.Current.OracleSkipReason is not null, TestEnvironment.Current.OracleSkipReason);
    public static void LocalOracle() => Skip.If(TestEnvironment.Current.LocalOracleSkipReason is not null, TestEnvironment.Current.LocalOracleSkipReason);
    public static void Trino() => Skip.If(TrinoOracle.SkipReason is not null, TrinoOracle.SkipReason);
    public static void Spark() => Skip.If(SparkOracle.SkipReason is not null, SparkOracle.SkipReason);
    public static void Athena() => Skip.If(TestEnvironment.Current.AthenaSkipReason is not null, TestEnvironment.Current.AthenaSkipReason);
}
