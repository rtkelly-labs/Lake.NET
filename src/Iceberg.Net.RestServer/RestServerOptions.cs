namespace Iceberg.Net.RestServer;

/// <summary>
/// Server configuration, bound from the <c>Iceberg</c> configuration section (appsettings.json, environment variables
/// such as <c>Iceberg__Warehouse</c>, or command-line arguments such as <c>--Iceberg:Warehouse=s3://bucket/wh</c>).
/// </summary>
public sealed class RestServerOptions
{
    public const string Section = "Iceberg";

    /// <summary>Root under which tables without a location (and without a namespace <c>location</c>) are placed: a local path or <c>s3://bucket/prefix</c>.</summary>
    public string Warehouse { get; set; } = "";

    /// <summary><c>sqlite</c> (default) or <c>memory</c>.</summary>
    public string Store { get; set; } = "sqlite";

    /// <summary>SQLite database file for the <c>sqlite</c> store; defaults to <c>catalog.db</c> in the working directory.</summary>
    public string? SqlitePath { get; set; }

    /// <summary>The <c>catalog_name</c> column value, so several catalogs can share one database (as Java's JdbcCatalog allows).</summary>
    public string CatalogName { get; set; } = "rest";

    /// <summary>Optional URL prefix: routes become <c>/v1/{prefix}/...</c> and <c>/v1/config</c> returns it as an override.</summary>
    public string? Prefix { get; set; }

    /// <summary>Format version of tables created without a <c>format-version</c> property.</summary>
    public int DefaultFormatVersion { get; set; } = 2;

    /// <summary><c>/v1/config</c> <c>defaults</c>: applied under the client's own properties.</summary>
    public Dictionary<string, string> Defaults { get; set; } = [];

    /// <summary><c>/v1/config</c> <c>overrides</c>: applied over the client's own properties.</summary>
    public Dictionary<string, string> Overrides { get; set; } = [];

    /// <summary>
    /// <c>config</c> returned with every loaded table, e.g. <c>s3.endpoint</c> and <c>s3.path-style-access</c> so clients
    /// reach the same object store. Not credential vending: whatever is here goes to every authenticated client.
    /// </summary>
    public Dictionary<string, string> TableConfig { get; set; } = [];

    /// <summary>The server's own FileIO properties for <c>s3://</c> locations (<c>s3.endpoint</c>, <c>client.region</c>, keys); local paths need none.</summary>
    public Dictionary<string, string> FileIO { get; set; } = [];

    public RestServerAuthOptions Auth { get; set; } = new();

    /// <summary>Role-based authorization: grants per principal. Off (everything allowed) while no grant is configured.</summary>
    public RestServerAuthorizationOptions Authorization { get; set; } = new();

    /// <summary>Server-side scan planning (<c>plan</c>, <c>plan/{plan-id}</c>, <c>tasks</c>).</summary>
    public ScanPlanningOptions ScanPlanning { get; set; } = new();

    /// <summary>Storage credentials vended with loaded tables (STS AssumeRole). Off while no <c>RoleArn</c> is configured.</summary>
    public CredentialVendingOptions CredentialVending { get; set; } = new();

    /// <summary><c>Idempotency-Key</c> deduplication of mutating requests.</summary>
    public IdempotencyOptions Idempotency { get; set; } = new();
}

/// <summary>
/// Static grants: principal → grant strings <c>{level}:{scope}</c>, where level is <c>read</c>, <c>write</c> or
/// <c>admin</c> (each includes the ones before it) and scope is <c>*</c> or a dotted namespace or table path
/// (<c>sales</c>, <c>sales.orders</c>), covering everything beneath it. The principal <c>*</c> grants to everyone
/// authenticated. A principal is the token's <c>sub</c>: the OAuth2 client ID, <c>static-token</c> for static tokens,
/// <c>anonymous</c> when authentication is off.
/// </summary>
public sealed class RestServerAuthorizationOptions
{
    public Dictionary<string, List<string>> Grants { get; set; } = [];

    public bool Enabled => Grants.Count > 0;
}

public sealed class ScanPlanningOptions
{
    /// <summary>Serve the planning endpoints and advertise them in <c>/v1/config</c>.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// <c>scan-planning-mode</c> returned in every loaded table's <c>config</c>, when set: <c>server</c> makes Iceberg
    /// Java (1.12+) clients plan their scans here; <c>client</c> (or unset) leaves planning to them.
    /// </summary>
    public string? Mode { get; set; }

    /// <summary>
    /// A scan whose surviving manifests hold more data files than this (by the manifest list's counts) is answered with
    /// plan tasks, each a group of manifests holding about this many files, which clients fetch one by one.
    /// </summary>
    public int FilesPerPlanTask { get; set; } = 5000;

    /// <summary>Plan in the background: <c>planTableScan</c> answers <c>submitted</c> and clients poll <c>plan/{plan-id}</c>.</summary>
    public bool Async { get; set; }

    /// <summary>How long a plan's result stays available to <c>plan/{plan-id}</c> after it finished.</summary>
    public TimeSpan PlanLifetime { get; set; } = TimeSpan.FromMinutes(10);
}

/// <summary>
/// Vended credentials: STS <c>AssumeRole</c> on <see cref="RoleArn"/> with a session policy limited to the table's
/// location (read-only for principals without write access), returned as <c>storage-credentials</c> and in the table
/// <c>config</c> when the client sends <c>X-Iceberg-Access-Delegation: vended-credentials</c>.
/// </summary>
public sealed class CredentialVendingOptions
{
    public string? RoleArn { get; set; }

    /// <summary>STS endpoint; default <c>https://sts.{Region}.amazonaws.com</c> (an emulator's URL in tests).</summary>
    public string? StsEndpoint { get; set; }

    public string Region { get; set; } = "us-east-1";

    public string? ExternalId { get; set; }

    public int DurationSeconds { get; set; } = 3600;

    /// <summary>
    /// The server's own credentials for calling STS. Unset: the FileIO's <c>s3.access-key-id</c> /
    /// <c>s3.secret-access-key</c>, else the AWS SDK's default credential chain.
    /// </summary>
    public string? AccessKeyId { get; set; }

    public string? SecretAccessKey { get; set; }

    public string? SessionToken { get; set; }

    public bool Enabled => !string.IsNullOrEmpty(RoleArn);
}

public sealed class IdempotencyOptions
{
    public bool Enabled { get; set; } = true;

    /// <summary>Advertised as <c>idempotency-key-lifetime</c>; responses are kept at least this long (plus a minute's grace).</summary>
    public TimeSpan Lifetime { get; set; } = TimeSpan.FromMinutes(30);
}

/// <summary>
/// Bearer-token authentication. With nothing configured the server is open. Any of <see cref="Tokens"/>,
/// <see cref="Clients"/> or <see cref="Authority"/> switches authentication on for every endpoint except the token
/// endpoint.
/// </summary>
public sealed class RestServerAuthOptions
{
    /// <summary>Static bearer tokens accepted as-is (the <c>token</c> client property).</summary>
    public List<string> Tokens { get; set; } = [];

    /// <summary>
    /// OAuth2 clients for the (deprecated, but widely used) <c>POST /v1/oauth/tokens</c> client-credentials endpoint:
    /// client ID → secret. Clients configure <c>credential = id:secret</c>.
    /// </summary>
    public Dictionary<string, string> Clients { get; set; } = [];

    /// <summary>HMAC-SHA256 key (at least 32 bytes of text) signing the tokens the server issues; random per process when unset.</summary>
    public string? SigningKey { get; set; }

    /// <summary>Issuer of the server's own tokens.</summary>
    public string Issuer { get; set; } = "iceberg-net-rest";

    /// <summary>Lifetime of issued tokens.</summary>
    public TimeSpan TokenLifetime { get; set; } = TimeSpan.FromHours(1);

    /// <summary>An external OpenID Connect issuer whose JWTs are also accepted (keys from its discovery document).</summary>
    public string? Authority { get; set; }

    /// <summary>The audience external JWTs must carry; unchecked when unset.</summary>
    public string? Audience { get; set; }

    /// <summary>Allow an <c>http://</c> <see cref="Authority"/> (tests, local IdPs).</summary>
    public bool RequireHttpsMetadata { get; set; } = true;

    public bool Enabled => Tokens.Count > 0 || Clients.Count > 0 || Authority is not null;
}
