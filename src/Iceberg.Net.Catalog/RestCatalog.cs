using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.IO;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Catalog;

public sealed record RestCatalogOptions
{
    /// <summary>Catalog base URI, e.g. <c>http://localhost:8181</c> (the client appends <c>/v1/...</c>).</summary>
    public required Uri Uri { get; init; }
    public string? Warehouse { get; init; }
    /// <summary>Static bearer token (<c>token</c> property).</summary>
    public string? Token { get; init; }
    /// <summary>OAuth2 client credentials as <c>client_id:client_secret</c> (<c>credential</c> property).</summary>
    public string? Credential { get; init; }
    public string Scope { get; init; } = "catalog";
    /// <summary>Token endpoint; defaults to the deprecated in-spec <c>/v1/oauth/tokens</c>. Point at your IdP in production.</summary>
    public Uri? OAuth2ServerUri { get; init; }
    /// <summary>Ask the server to vend storage credentials (<c>X-Iceberg-Access-Delegation</c>).</summary>
    public string? AccessDelegation { get; init; } = "vended-credentials";
    /// <summary>Extra client-side properties; server <c>defaults</c> are applied under these, <c>overrides</c> on top.</summary>
    public IReadOnlyDictionary<string, string> Properties { get; init; } = new Dictionary<string, string>();
    /// <summary>Hook for request signing (e.g. a SigV4 handler for the Glue/S3 Tables REST endpoints).</summary>
    public HttpMessageHandler? Handler { get; init; }

    /// <summary>
    /// The AWS Glue Iceberg REST endpoint for <paramref name="warehouse"/> (an account ID, or
    /// <c>{account}:s3tablescatalog/{bucket}</c> for S3 Tables through Glue), signed with SigV4 (<c>glue</c>) using
    /// <paramref name="credentials"/> or the SDK's default credential chain.
    /// </summary>
    public static RestCatalogOptions ForGlue(string region, string warehouse, Amazon.Runtime.AWSCredentials? credentials = null) => new()
    {
        Uri = new Uri($"https://glue.{region}.amazonaws.com/iceberg"),
        Warehouse = warehouse,
        Handler = new SigV4Handler(credentials ?? Amazon.Runtime.Credentials.DefaultAWSCredentialsIdentityResolver.GetCredentials(), region, "glue"),
        Properties = new Dictionary<string, string> { ["client.region"] = region },
    };

    /// <summary>The S3 Tables Iceberg REST endpoint for a table bucket ARN, signed with SigV4 (<c>s3tables</c>).</summary>
    public static RestCatalogOptions ForS3Tables(string region, string tableBucketArn, Amazon.Runtime.AWSCredentials? credentials = null) => new()
    {
        Uri = new Uri($"https://s3tables.{region}.amazonaws.com/iceberg"),
        Warehouse = tableBucketArn,
        Handler = new SigV4Handler(credentials ?? Amazon.Runtime.Credentials.DefaultAWSCredentialsIdentityResolver.GetCredentials(), region, "s3tables"),
        Properties = new Dictionary<string, string> { ["client.region"] = region },
    };
}

/// <summary>
/// Iceberg REST catalog client (open-api/rest-catalog-open-api.yaml). The server owns commit
/// semantics: the client sends requirements + updates and the server applies them atomically.
/// </summary>
public sealed class RestCatalog : ICatalog
{
    private readonly HttpClient _http;
    private readonly RestCatalogOptions _options;
    private string _prefix = "";
    private string? _token;

    public string Name { get; }
    public IFileIO FileIO { get; private set; }
    public IReadOnlyDictionary<string, string> Properties { get; private set; } = new Dictionary<string, string>();
    /// <summary>Endpoints advertised by <c>GET /v1/config</c>; empty means "assume the default set".</summary>
    public IReadOnlyList<string> Endpoints { get; private set; } = [];

    private readonly Func<IReadOnlyDictionary<string, string>, IFileIO> _ioFactory;

    private RestCatalog(RestCatalogOptions options, Func<IReadOnlyDictionary<string, string>, IFileIO> ioFactory, string name)
    {
        _options = options;
        _ioFactory = ioFactory;
        Name = name;
        _http = options.Handler is null ? new HttpClient() : new HttpClient(options.Handler);
        _http.BaseAddress = new Uri(options.Uri.ToString().TrimEnd('/') + "/");
        _token = options.Token;
        FileIO = new LocalFileIO();
    }

    /// <summary>Connect: authenticate, fetch <c>/v1/config</c>, merge properties and build FileIO from them.</summary>
    public static async Task<RestCatalog> ConnectAsync(
        RestCatalogOptions options,
        Func<IReadOnlyDictionary<string, string>, IFileIO>? ioFactory = null,
        string name = "rest",
        CancellationToken ct = default)
    {
        ioFactory ??= props => new ResolvingFileIO(() => new S3FileIO(S3FileIOOptions.FromProperties(props)));
        var catalog = new RestCatalog(options, ioFactory, name);
        await catalog.InitialiseAsync(ct);
        return catalog;
    }

    private async Task InitialiseAsync(CancellationToken ct)
    {
        if (_token is null && _options.Credential is not null) _token = await FetchTokenAsync(ct);
        var query = _options.Warehouse is null ? "" : $"?warehouse={Uri.EscapeDataString(_options.Warehouse)}";
        var config = await SendAsync(HttpMethod.Get, $"v1/config{query}", null, ct);
        var merged = new Dictionary<string, string>();
        foreach (var (k, v) in MetadataJson.StringMapFrom(config?["defaults"])) merged[k] = v;
        foreach (var (k, v) in _options.Properties) merged[k] = v;
        foreach (var (k, v) in MetadataJson.StringMapFrom(config?["overrides"])) merged[k] = v;
        Properties = merged;
        Endpoints = config?["endpoints"]?.AsArray().Select(e => e!.GetValue<string>()).ToList() ?? [];
        _prefix = merged.TryGetValue("prefix", out var p) && p.Length > 0 ? p.Trim('/') + "/" : "";
        FileIO = _ioFactory(merged);
    }

    private async Task<string> FetchTokenAsync(CancellationToken ct)
    {
        var parts = _options.Credential!.Split(':', 2);
        var form = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["scope"] = _options.Scope,
        };
        if (parts.Length == 2)
        {
            form["client_id"] = parts[0];
            form["client_secret"] = parts[1];
        }
        else form["client_secret"] = parts[0];
        var uri = _options.OAuth2ServerUri ?? new Uri(_http.BaseAddress!, "v1/oauth/tokens");
        using var resp = await _http.PostAsync(uri, new FormUrlEncodedContent(form), ct);
        var body = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode) throw new UnauthorizedAccessException($"OAuth2 token request failed ({(int)resp.StatusCode}): {body}");
        return JsonNode.Parse(body)!["access_token"]!.GetValue<string>();
    }

    /// <summary>Multi-level namespaces are joined with the 0x1F unit separator, then URL-encoded (spec).</summary>
    public static string EncodeNamespace(Namespace ns) => Uri.EscapeDataString(string.Join('\u001f', ns.Levels));

    private string NsPath(Namespace ns) => $"v1/{_prefix}namespaces/{EncodeNamespace(ns)}";
    private string TablePath(TableIdentifier id) => $"{NsPath(id.Namespace)}/tables/{Uri.EscapeDataString(id.Name)}";

    public async Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["namespace"] = new JsonArray(ns.Levels.Select(l => (JsonNode)l).ToArray()),
            ["properties"] = MetadataJson.StringMap(properties ?? new Dictionary<string, string>()),
        };
        await SendAsync(HttpMethod.Post, $"v1/{_prefix}namespaces", body, ct, conflict: m => new AlreadyExistsException(m));
    }

    public async Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default)
    {
        var result = new List<Namespace>();
        string? pageToken = null;
        do
        {
            var q = new List<string>();
            if (parent is not null) q.Add($"parent={EncodeNamespace(parent)}");
            if (pageToken is not null) q.Add($"pageToken={Uri.EscapeDataString(pageToken)}");
            var resp = await SendAsync(HttpMethod.Get, $"v1/{_prefix}namespaces{(q.Count > 0 ? "?" + string.Join('&', q) : "")}", null, ct);
            result.AddRange(resp!["namespaces"]!.AsArray().Select(n => new Namespace(n!.AsArray().Select(l => l!.GetValue<string>()).ToList())));
            pageToken = resp["next-page-token"]?.GetValue<string>();
        } while (!string.IsNullOrEmpty(pageToken));
        return result;
    }

    public async Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default)
    {
        try
        {
            await SendAsync(HttpMethod.Get, NsPath(ns), null, ct);
            return true;
        }
        catch (NoSuchNamespaceException)
        {
            return false;
        }
    }

    public Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default) => SendAsync(HttpMethod.Delete, NsPath(ns), null, ct);

    public async Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default)
    {
        var result = new List<TableIdentifier>();
        string? pageToken = null;
        do
        {
            var resp = await SendAsync(HttpMethod.Get, $"{NsPath(ns)}/tables{(pageToken is null ? "" : $"?pageToken={Uri.EscapeDataString(pageToken)}")}", null, ct);
            result.AddRange(resp!["identifiers"]!.AsArray().Select(i => new TableIdentifier(
                new Namespace(i!["namespace"]!.AsArray().Select(l => l!.GetValue<string>()).ToList()), i["name"]!.GetValue<string>())));
            pageToken = resp["next-page-token"]?.GetValue<string>();
        } while (!string.IsNullOrEmpty(pageToken));
        return result;
    }

    public async Task<Table> CreateTableAsync(CreateTableRequest r, CancellationToken ct = default)
    {
        var props = new Dictionary<string, string>(r.Properties ?? new Dictionary<string, string>())
        {
            ["format-version"] = r.FormatVersion.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        var body = new JsonObject
        {
            ["name"] = r.Identifier.Name,
            ["schema"] = TypeJson.SchemaToJson(r.Schema),
            ["properties"] = MetadataJson.StringMap(props),
        };
        if (r.Location is not null) body["location"] = r.Location;
        if (r.Spec is not null) body["partition-spec"] = TypeJson.SpecToJson(r.Spec);
        if (r.SortOrder is not null) body["write-order"] = TypeJson.SortOrderToJson(r.SortOrder);
        var resp = await SendAsync(HttpMethod.Post, $"{NsPath(r.Identifier.Namespace)}/tables", body, ct, conflict: m => new AlreadyExistsException(m));
        return ToTable(r.Identifier, resp!);
    }

    public async Task<Table> LoadTableAsync(TableIdentifier id, CancellationToken ct = default) =>
        ToTable(id, (await SendAsync(HttpMethod.Get, TablePath(id), null, ct))!);

    public async Task<bool> TableExistsAsync(TableIdentifier id, CancellationToken ct = default)
    {
        try
        {
            await SendAsync(HttpMethod.Head, TablePath(id), null, ct);
            return true;
        }
        catch (NoSuchTableException)
        {
            return false;
        }
    }

    public async Task<Table> RegisterTableAsync(TableIdentifier id, string metadataLocation, CancellationToken ct = default)
    {
        var body = new JsonObject { ["name"] = id.Name, ["metadata-location"] = metadataLocation };
        return ToTable(id, (await SendAsync(HttpMethod.Post, $"{NsPath(id.Namespace)}/register", body, ct, conflict: m => new AlreadyExistsException(m)))!);
    }

    public Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        static JsonObject Id(TableIdentifier t) => new()
        {
            ["namespace"] = new JsonArray(t.Namespace.Levels.Select(l => (JsonNode)l).ToArray()),
            ["name"] = t.Name,
        };
        return SendAsync(HttpMethod.Post, $"v1/{_prefix}tables/rename", new JsonObject { ["source"] = Id(from), ["destination"] = Id(to) }, ct,
            conflict: m => new AlreadyExistsException(m));
    }

    public Task DropTableAsync(TableIdentifier id, bool purge = false, CancellationToken ct = default) =>
        SendAsync(HttpMethod.Delete, $"{TablePath(id)}?purgeRequested={(purge ? "true" : "false")}", null, ct);

    public async Task<TableMetadata> CommitTableAsync(TableCommit commit, CancellationToken ct = default)
    {
        var body = new JsonObject
        {
            ["requirements"] = new JsonArray(commit.Requirements.Select(r => (JsonNode)UpdateJson.ToJson(r)).ToArray()),
            ["updates"] = new JsonArray(commit.Updates.Select(u => (JsonNode)UpdateJson.ToJson(u)).ToArray()),
        };
        var resp = await SendAsync(HttpMethod.Post, TablePath(commit.Identifier), body, ct,
            conflict: m => new CommitFailedException(m), commit: true);
        return MetadataJson.FromJson(resp!["metadata"]!) with { MetadataFileLocation = resp["metadata-location"]?.GetValue<string>() };
    }

    private Table ToTable(TableIdentifier id, JsonNode loadResult)
    {
        var metadata = MetadataJson.FromJson(loadResult["metadata"]!) with { MetadataFileLocation = loadResult["metadata-location"]?.GetValue<string>() };
        // Per-table config (and vended credentials) override catalog properties for this table's IO.
        var tableConfig = MetadataJson.StringMapFrom(loadResult["config"]);
        var creds = loadResult["storage-credentials"]?.AsArray()
            .Where(c => metadata.Location.StartsWith(c!["prefix"]!.GetValue<string>(), StringComparison.Ordinal))
            .OrderByDescending(c => c!["prefix"]!.GetValue<string>().Length)
            .FirstOrDefault();
        var io = FileIO;
        if (tableConfig.Count > 0 || creds is not null)
        {
            var props = new Dictionary<string, string>(Properties);
            foreach (var (k, v) in tableConfig) props[k] = v;
            if (creds is not null) foreach (var (k, v) in MetadataJson.StringMapFrom(creds["config"])) props[k] = v;
            io = _ioFactory(props);
        }
        return new Table(this, id, metadata, io);
    }

    private async Task<JsonNode?> SendAsync(
        HttpMethod method,
        string path,
        JsonNode? body,
        CancellationToken ct,
        Func<string, Exception>? conflict = null,
        bool commit = false)
    {
        using var req = new HttpRequestMessage(method, path);
        if (_token is not null) req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
        if (_options.AccessDelegation is not null) req.Headers.Add("X-Iceberg-Access-Delegation", _options.AccessDelegation);
        if (body is not null) req.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        // Spec 1.11+: lets a server deduplicate a mutation it already applied; servers that predate it ignore the header.
        if (method != HttpMethod.Get && method != HttpMethod.Head) req.Headers.Add("Idempotency-Key", Guid.NewGuid().ToString());

        HttpResponseMessage resp;
        try
        {
            resp = await _http.SendAsync(req, ct);
        }
        catch (HttpRequestException e) when (commit)
        {
            throw new CommitStateUnknownException($"Commit request to {path} failed in flight", e);
        }

        using (resp)
        {
            var text = resp.Content is null ? "" : await resp.Content.ReadAsStringAsync(ct);
            if (resp.IsSuccessStatusCode) return text.Length == 0 ? null : JsonNode.Parse(text);

            var message = ErrorMessage(text) ?? $"{(int)resp.StatusCode} {resp.ReasonPhrase}";
            var type = ErrorType(text);
            throw resp.StatusCode switch
            {
                HttpStatusCode.NotFound when type == "NoSuchTableException" || (type is null && path.Contains("/tables/", StringComparison.Ordinal)) =>
                    new NoSuchTableException(message),
                HttpStatusCode.NotFound => new NoSuchNamespaceException(message),
                HttpStatusCode.Conflict when conflict is not null => conflict(message),
                HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden => new UnauthorizedAccessException(message),
                // On commit, 500/502/503/504 mean the commit may or may not have been applied (Java 1.10: #13352, #13619).
                HttpStatusCode.InternalServerError or HttpStatusCode.BadGateway or HttpStatusCode.ServiceUnavailable or HttpStatusCode.GatewayTimeout when commit =>
                    new CommitStateUnknownException(message) { ServerResponded = true },
                _ => new HttpRequestException($"{method} {path} failed: {message}", null, resp.StatusCode),
            };
        }
    }

    private static JsonNode? ParseOrNull(string text)
    {
        try { return text.Length == 0 ? null : JsonNode.Parse(text); }
        catch (System.Text.Json.JsonException) { return null; }
    }

    private static string? ErrorMessage(string text) => ParseOrNull(text)?["error"]?["message"]?.GetValue<string>();
    private static string? ErrorType(string text) => ParseOrNull(text)?["error"]?["type"]?.GetValue<string>();

    public void Dispose()
    {
        _http.Dispose();
        FileIO.Dispose();
    }
}
