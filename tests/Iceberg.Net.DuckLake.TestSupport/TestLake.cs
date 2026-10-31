using System.Data.Common;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Iceberg.Net.IO;
using Iceberg.Net.TestSupport;
using Microsoft.Data.Sqlite;
using Npgsql;
using Xunit;

namespace Iceberg.Net.DuckLake.TestSupport;

/// <summary>The catalog databases DuckLake is tested on.</summary>
public enum CatalogBackend
{
    Sqlite,
    Postgres,
}

/// <summary>Where a test lake's data files live.</summary>
public enum StorageBackend
{
    /// <summary>A temporary directory (<see cref="LocalFileIO"/>).</summary>
    Local,
    /// <summary>
    /// A prefix of the <c>iceberg-regression</c> bucket on the S3 emulator (<see cref="S3FileIO"/>, path-style, test
    /// credentials) at <c>ICEBERG_S3_ENDPOINT</c> (default <c>http://localhost:5000</c>, moto from <c>emulation/up.sh</c>);
    /// skipped when unreachable, failed instead under <c>ICEBERG_DUCKLAKE_S3_REQUIRED=1</c>.
    /// </summary>
    S3,
}

/// <summary>
/// A DuckLake in a temporary directory: a catalog database (a SQLite file beside the data, or a fresh PostgreSQL
/// database) and a data directory. The PostgreSQL server comes from <c>ICEBERG_DUCKLAKE_POSTGRES</c> (an Npgsql
/// connection string; <c>emulation/up.sh</c> starts one on port 5434); without it, PostgreSQL lakes skip, and fail
/// instead under <c>ICEBERG_DUCKLAKE_POSTGRES_REQUIRED=1</c> (CI). The database is created on first use, inside the
/// test, so a skip is reported as one.
/// </summary>
public sealed class TestLake : IDisposable
{
    public const string PostgresVariable = "ICEBERG_DUCKLAKE_POSTGRES";

    private DuckLakeStore? _store;
    private string? _database;

    public TestLake(CatalogBackend backend = CatalogBackend.Sqlite, string version = DuckLakeCatalog.FormatVersion, StorageBackend storage = StorageBackend.Local)
    {
        Backend = backend;
        Version = version;
        Storage = storage;
        var id = Guid.NewGuid().ToString("N");
        Root = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", "ducklake-" + id);
        Directory.CreateDirectory(Root);
        DataPath = storage == StorageBackend.S3 ? $"s3://{S3Bucket}/ducklake-tests/{id}/data/" : Path.Combine(Root, "data") + "/";
    }

    public CatalogBackend Backend { get; }
    /// <summary>The format version <see cref="CreateAsync"/> creates.</summary>
    public string Version { get; }
    public StorageBackend Storage { get; }
    /// <summary>A local directory for the SQLite catalog and scratch files, whatever the storage.</summary>
    public string Root { get; }
    public string DataPath { get; }

    public const string S3Bucket = "iceberg-regression";
    public static string S3Endpoint => Environment.GetEnvironmentVariable("ICEBERG_S3_ENDPOINT") is { Length: > 0 } e ? e : "http://localhost:5000";

    private IFileIO? _io;

    /// <summary>The lake's storage; for S3, the first use skips the test when the emulator is unreachable.</summary>
    public IFileIO IO => _io ??= Storage switch
    {
        StorageBackend.S3 => CreateS3(),
        _ => new LocalFileIO(),
    };

    private static readonly Lazy<string?> S3Skip = new(() =>
    {
        try
        {
            using var client = new System.Net.Sockets.TcpClient();
            var uri = new Uri(S3Endpoint);
            return client.ConnectAsync(uri.Host, uri.Port).Wait(TimeSpan.FromSeconds(2)) ? null : $"S3 emulator not reachable at {S3Endpoint} (run emulation/up.sh)";
        }
        catch (Exception e) when (e is System.Net.Sockets.SocketException or AggregateException or UriFormatException)
        {
            return $"S3 emulator not reachable at {S3Endpoint}: {e.Message}";
        }
    });

    private static S3FileIO CreateS3()
    {
        if (Environment.GetEnvironmentVariable("ICEBERG_DUCKLAKE_S3_REQUIRED") == "1") Assert.True(S3Skip.Value is null, S3Skip.Value);
        Skip.If(S3Skip.Value is not null, S3Skip.Value);
        return new S3FileIO(new S3FileIOOptions
        {
            Endpoint = S3Endpoint, PathStyleAccess = true, Region = "eu-west-1", AccessKeyId = "test", SecretAccessKey = "test",
        });
    }
    public string SqliteFile => Path.Combine(Root, "catalog.sqlite");

    public DuckLakeStore Store => _store ??= Backend switch
    {
        // Pooling off: DuckDB opens the same file between our calls, and a pooled handle would outlive the test.
        CatalogBackend.Sqlite => DuckLakeStore.Sqlite($"Data Source={SqliteFile};Pooling=False", ConfigureStore),
        _ => DuckLakeStore.PostgreSql(CreateDatabase(), ConfigureStore),
    };

    /// <summary>EF Core options for the store (interceptors such as <see cref="CatalogFaults"/>); set before first use of <see cref="Store"/>.</summary>
    public Action<Microsoft.EntityFrameworkCore.DbContextOptionsBuilder>? ConfigureStore { get; set; }

    /// <summary>Whether the catalog database already exists (DuckDB then attaches without DATA_PATH).</summary>
    public bool CatalogExists => Backend == CatalogBackend.Sqlite ? File.Exists(SqliteFile) : _catalogCreated;

    private bool _catalogCreated;

    /// <summary>The <c>ATTACH</c> string DuckDB uses for this lake's catalog.</summary>
    public string Attach => Store.DuckDbAttach();

    public async Task<DuckLakeCatalog> CreateAsync(DuckLakeOptions? options = null, string? version = null)
    {
        var catalog = await DuckLakeCatalog.CreateAsync(Store, IO, DataPath, options, version ?? Version);
        _catalogCreated = true;
        return catalog;
    }

    public Task<DuckLakeCatalog> OpenAsync(DuckLakeOptions? options = null) => DuckLakeCatalog.OpenAsync(Store, IO, options);

    /// <summary>Call after DuckDB created the catalog (so the next oracle job attaches without DATA_PATH).</summary>
    public void MarkCreated() => _catalogCreated = true;

    /// <summary>A raw connection to the catalog database, for assertions on the rows themselves.</summary>
    public DbConnection Connect() => Backend == CatalogBackend.Sqlite
        ? new SqliteConnection(Store.ConnectionString)
        : new NpgsqlConnection(Store.ConnectionString);

    /// <summary>Rows of a query, with booleans as 0/1 and UUIDs and timestamps as text so both databases compare alike.</summary>
    public async Task<List<object?[]>> SqlAsync(string sql)
    {
        await using var c = Connect();
        await c.OpenAsync();
        await using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        await using var r = await cmd.ExecuteReaderAsync();
        var rows = new List<object?[]>();
        while (await r.ReadAsync())
        {
            var row = new object?[r.FieldCount];
            for (var i = 0; i < row.Length; i++)
                row[i] = r.IsDBNull(i) ? null : r.GetValue(i) switch
                {
                    bool b => b ? 1L : 0L,
                    int n => (long)n,
                    Guid g => g.ToString("D"),
                    var v => v,
                };
            rows.Add(row);
        }
        return rows;
    }

    /// <summary>Execute a statement (corrupting the catalog on purpose, in tests).</summary>
    public async Task ExecAsync(string sql) => await SqlAsync(sql);

    /// <summary>The number of tables in the catalog database whose names start with <paramref name="prefix"/>.</summary>
    public async Task<long> TableCountAsync(string prefix) => (long)(await SqlAsync(Backend == CatalogBackend.Sqlite
        ? $"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name LIKE '{prefix}%'"
        : $"SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE '{prefix}%'"))[0][0]!;

    /// <summary>Set by tests that corrupt the catalog on purpose: <see cref="ValidateAsync"/> then skips it.</summary>
    public bool Corrupted { get; set; }

    /// <summary>Deep-validate the lake (<see cref="DuckLakeValidator"/>) if a catalog was created and not corrupted on purpose.</summary>
    public async Task ValidateAsync()
    {
        if (Corrupted || !CatalogExists || _store is null) return;
        var catalog = await OpenAsync();
        await DuckLakeValidator.AssertValidAsync(catalog, new DuckLakeValidationOptions { Deep = true });
    }

    /// <summary>The objects under <see cref="DataPath"/>, on whichever storage.</summary>
    public IEnumerable<string> Files()
    {
        if (Storage == StorageBackend.Local) return Directory.Exists(DataPath) ? Directory.EnumerateFiles(DataPath, "*", SearchOption.AllDirectories) : [];
        var files = new List<string>();
        var list = ((ISupportsListing)IO).ListAsync(DataPath).GetAsyncEnumerator();
        try
        {
            while (list.MoveNextAsync().AsTask().GetAwaiter().GetResult()) files.Add(list.Current.Location);
        }
        finally
        {
            list.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        return files;
    }

    private static readonly Lazy<string?> PostgresSkip = new(ProbePostgres);

    /// <summary>Skip (or, when required, fail) unless the PostgreSQL server is reachable.</summary>
    public static void RequirePostgres()
    {
        if (Environment.GetEnvironmentVariable("ICEBERG_DUCKLAKE_POSTGRES_REQUIRED") == "1") Assert.True(PostgresSkip.Value is null, PostgresSkip.Value);
        Skip.If(PostgresSkip.Value is not null, PostgresSkip.Value);
    }

    private static string? ProbePostgres()
    {
        if (Environment.GetEnvironmentVariable(PostgresVariable) is not { Length: > 0 } cs)
            return $"PostgreSQL catalog unavailable: set {PostgresVariable} (e.g. Host=localhost;Port=5434;Username=postgres;Password=postgres)";
        try
        {
            using var c = new NpgsqlConnection(cs);
            c.Open();
            return null;
        }
        catch (Exception e) when (e is NpgsqlException or InvalidOperationException or System.Net.Sockets.SocketException)
        {
            return $"PostgreSQL catalog unavailable at {PostgresVariable}: {e.Message}";
        }
    }

    private string CreateDatabase()
    {
        RequirePostgres();
        var admin = Environment.GetEnvironmentVariable(PostgresVariable)!;
        _database = "ducklake_" + Guid.NewGuid().ToString("N")[..16];
        using (var c = new NpgsqlConnection(admin))
        {
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = $"CREATE DATABASE {_database}";
            cmd.ExecuteNonQuery();
        }
        return new NpgsqlConnectionStringBuilder(admin) { Database = _database, Pooling = false }.ConnectionString;
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        if (_database is not null)
        {
            try
            {
                using var c = new NpgsqlConnection(Environment.GetEnvironmentVariable(PostgresVariable));
                c.Open();
                using var cmd = c.CreateCommand();
                cmd.CommandText = $"DROP DATABASE IF EXISTS {_database} WITH (FORCE)";
                cmd.ExecuteNonQuery();
            }
            catch (NpgsqlException) { /* best effort */ }
        }
        if (Storage == StorageBackend.S3 && _io is not null)
        {
            try
            {
                foreach (var f in Files()) _io.DeleteAsync(f).GetAwaiter().GetResult();
            }
            catch (Exception e) when (e is IOException or Amazon.Runtime.AmazonServiceException or HttpRequestException) { /* best effort */ }
        }
        _io?.Dispose();
        try { Directory.Delete(Root, recursive: true); }
        catch (IOException) { }
    }
}

/// <summary>
/// DuckDB's <c>ducklake</c> extension, the DuckLake reference implementation, through interop/ducklake_oracle.py.
/// Two oracles: <c>ICEBERG_DUCKDB_PYTHON</c> (else <c>ICEBERG_PYTHON</c>), the interop <c>duckdb</c> group (DuckDB 1.5,
/// format 1.0), and <c>ICEBERG_DUCKLAKE11_PYTHON</c>, a Python with DuckDB 2.0 (format 1.1). Each skips when unavailable
/// and fails instead under <c>ICEBERG_DUCKDB_REQUIRED=1</c> / <c>ICEBERG_DUCKLAKE11_REQUIRED=1</c> (CI).
/// </summary>
public sealed class DuckLakeOracle
{
    public static readonly DuckLakeOracle V10 = new(DuckLakeCatalog.FormatVersion10,
        () => Environment.GetEnvironmentVariable("ICEBERG_DUCKDB_PYTHON") is { Length: > 0 } p ? p
            : Environment.GetEnvironmentVariable("ICEBERG_PYTHON") is { Length: > 0 } q ? q : null,
        "ICEBERG_DUCKDB_REQUIRED", "ICEBERG_DUCKDB_PYTHON (or ICEBERG_PYTHON) to a Python with the interop 'duckdb' group");

    public static readonly DuckLakeOracle V11 = new(DuckLakeCatalog.FormatVersion11,
        () => Environment.GetEnvironmentVariable("ICEBERG_DUCKLAKE11_PYTHON") is { Length: > 0 } p ? p : null,
        "ICEBERG_DUCKLAKE11_REQUIRED", "ICEBERG_DUCKLAKE11_PYTHON to a Python with duckdb 2.0 (its ducklake extension writes format 1.1)");

    public static DuckLakeOracle For(string version) => version == DuckLakeCatalog.FormatVersion10 ? V10 : V11;

    private readonly Func<string?> _python;
    private readonly string _required;
    private readonly string _hint;
    private readonly Lazy<string?> _skip;

    private DuckLakeOracle(string version, Func<string?> python, string required, string hint)
    {
        Version = version;
        _python = python;
        _required = required;
        _hint = hint;
        _skip = new Lazy<string?>(Probe);
    }

    /// <summary>The DuckLake format version this oracle's DuckDB writes.</summary>
    public string Version { get; }

    /// <summary>Skip unless the oracle is available; under its REQUIRED variable an unavailable oracle fails.</summary>
    public void Require()
    {
        if (Environment.GetEnvironmentVariable(_required) == "1") Assert.True(_skip.Value is null, _skip.Value);
        Skip.If(_skip.Value is not null, _skip.Value);
    }

    private string? Probe()
    {
        if (Environment.GetEnvironmentVariable("ICEBERG_DUCKDB") == "0") return "DuckLake oracle disabled (ICEBERG_DUCKDB=0)";
        if (_python() is null) return $"DuckLake {Version} oracle unavailable: set {_hint}";
        try
        {
            var probe = RunAsync(["probe"]).GetAwaiter().GetResult();
            if (!probe["available"]!.GetValue<bool>()) return $"DuckLake {Version} oracle unavailable: {probe["reason"]}";
            var format = probe["format"]!.GetValue<string>();
            return format == Version ? null : $"The DuckLake oracle at {_python()} writes format {format}, not {Version}";
        }
        catch (Exception e) when (e is InvalidOperationException or System.ComponentModel.Win32Exception or TimeoutException)
        {
            return $"DuckLake {Version} oracle unavailable: {e.Message}";
        }
    }

    /// <summary>Run statements against the lake attached as <c>lake</c>; every statement must succeed.</summary>
    public async Task<List<JsonObject>> RunAsync(TestLake lake, params string[] statements)
    {
        var results = await TryRunAsync(lake, statements);
        foreach (var r in results)
            if (r["error"] is { } e) throw new InvalidOperationException($"DuckDB: {e}");
        Assert.Equal(statements.Length, results.Count);
        return results;
    }

    /// <summary>As <see cref="RunAsync(TestLake, string[])"/>, returning a failed statement's error instead of throwing.</summary>
    public async Task<List<JsonObject>> TryRunAsync(TestLake lake, params string[] statements)
    {
        var job = Path.Combine(lake.Root, $"job-{Guid.NewGuid():N}.json");
        var create = !lake.CatalogExists;
        await File.WriteAllTextAsync(job, JsonSerializer.Serialize(new { attach = lake.Attach, data_path = create ? lake.DataPath : null, statements }));
        var result = await RunAsync(["run", job]);
        if (create) lake.MarkCreated();
        return result.AsArray().Select(n => n!.AsObject()).ToList();
    }

    /// <summary>Rows of one query, each row's values as JSON text for order-insensitive comparison.</summary>
    public async Task<List<string>> RowsAsync(TestLake lake, string sql) =>
        (await RunAsync(lake, sql))[0]["rows"]!.AsArray().Select(r => LakeJson.Canonical(r)!.ToJsonString()).Order(StringComparer.Ordinal).ToList();

    private async Task<JsonNode> RunAsync(IEnumerable<string> args)
    {
        var python = _python()!;
        var psi = new ProcessStartInfo(python) { RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add(Path.Combine(TestEnvironment.RepoDir(), "interop", "ducklake_oracle.py"));
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi) ?? throw new InvalidOperationException($"cannot start {python}");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(5));
        var output = await stdout;
        var marker = output.LastIndexOf("@@ORACLE@@", StringComparison.Ordinal);
        if (p.ExitCode != 0 || marker < 0) throw new InvalidOperationException($"ducklake_oracle.py failed ({p.ExitCode}): {await stderr}");
        return JsonNode.Parse(output[(marker + "@@ORACLE@@".Length)..])!;
    }
}
