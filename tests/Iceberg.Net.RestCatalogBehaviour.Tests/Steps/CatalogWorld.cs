using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Partitioning;
using Iceberg.Net.RestServer.Tests;
using Iceberg.Net.Types;
using Xunit;
using Table = Iceberg.Net.Catalog.Table;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

/// <summary>
/// Per-scenario state (Reqnroll context injection gives every scenario a fresh instance): one in-process server, the
/// clients connected to it, and what the last attempt observed. The server starts on first use, so a Given that
/// configures it (authentication, grants) must come before anything that touches the catalog.
/// </summary>
public sealed class CatalogWorld
{
    /// <summary>Every table in these scenarios has this shape unless a step says otherwise.</summary>
    public static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "region", PrimitiveType.String),
    ]);

    public static PartitionSpec RegionSpec(Schema schema) => PartitionSpec.BuilderFor(schema).Identity("region").Build();

    public Dictionary<string, string> Settings { get; } = new();
    /// <summary>Principal → secret, for servers that require authentication. The observer ("root") holds <c>admin:*</c>.</summary>
    public Dictionary<string, string> Secrets { get; } = new();
    public bool AuthRequired => Secrets.Count > 0;

    private ServerHost? _host;
    private RestCatalog? _catalog;
    private readonly Dictionary<string, (RestCatalog Client, StatusRecorder Recorder)> _clients = new();

    public ServerHost Host => _host ??= ServerHost.Start("memory", Settings);

    /// <summary>The catalog as an all-powerful observer sees it (anonymous without auth, "root" with it).</summary>
    public async Task<RestCatalog> CatalogAsync() =>
        _catalog ??= AuthRequired ? (await ClientAsync("root")).Client : await Host.ConnectAsync();

    /// <summary>A client authenticated as <paramref name="principal"/> (OAuth2 client credentials), recording every status it receives.</summary>
    public async Task<(RestCatalog Client, StatusRecorder Recorder)> ClientAsync(string principal)
    {
        if (_clients.TryGetValue(principal, out var existing)) return existing;
        var recorder = new StatusRecorder();
        var client = await Host.ConnectAsync(credential: $"{principal}:{Secrets[principal]}", wrap: inner => { recorder.InnerHandler = inner; return recorder; });
        return _clients[principal] = (client, recorder);
    }

    // ---------- tables remembered by the scenario ----------

    /// <summary>The pages of the last paged listing.</summary>
    public List<List<string>> Pages { get; } = [];

    /// <summary>Tables as some writer loaded them earlier (stale on purpose), by "ns.name".</summary>
    public Dictionary<string, Table> Remembered { get; } = new();
    /// <summary>Snapshot IDs a scenario names ("before").</summary>
    public Dictionary<string, long> SnapshotIds { get; } = new();
    /// <summary>Every file the scenario saw a table reference (data, manifest lists, metadata), by "ns.name".</summary>
    public Dictionary<string, HashSet<string>> FilesOf { get; } = new();
    /// <summary>The metadata file a table pointed at when last observed, by "ns.name".</summary>
    public Dictionary<string, string> LastMetadataFile { get; } = new();

    /// <summary>Note every file <paramref name="table"/> references now, so later steps can check they still (or no longer) exist.</summary>
    public void Observe(string name, Table table, IEnumerable<string>? dataFiles = null)
    {
        if (!FilesOf.TryGetValue(name, out var files)) FilesOf[name] = files = new HashSet<string>(StringComparer.Ordinal);
        foreach (var f in dataFiles ?? []) files.Add(f);
        foreach (var s in table.Metadata.Snapshots) if (s.ManifestList is { } list) files.Add(list);
        foreach (var m in table.Metadata.MetadataLog) files.Add(m.MetadataFile);
        if (table.MetadataLocation is { } current)
        {
            files.Add(current);
            LastMetadataFile[name] = current;
        }
    }

    // ---------- outcomes ----------

    private Exception? _error;
    private bool _errorObserved = true;

    /// <summary>
    /// Run something the scenario expects may be refused, keeping the exception for a Then step. A refusal nobody
    /// looked at fails the next attempt (and the scenario), so an unexpected error is never swallowed.
    /// </summary>
    public async Task AttemptAsync(Func<Task> action)
    {
        ThrowUnobserved();
        _error = null;
        try
        {
            await action();
        }
        catch (Exception e)
        {
            _error = e;
            _errorObserved = false;
        }
    }

    public Exception TakeError()
    {
        Assert.True(_error is not null, "Expected the attempt to be refused, but it succeeded");
        _errorObserved = true;
        return _error!;
    }

    public void AssertSucceeded()
    {
        if (_error is { } e && !_errorObserved)
        {
            _errorObserved = true;
            throw new Xunit.Sdk.XunitException($"Expected the attempt to succeed, but it failed: {e}");
        }
    }

    public void ThrowUnobserved()
    {
        if (_error is { } e && !_errorObserved)
        {
            _errorObserved = true;
            throw new Xunit.Sdk.XunitException($"An earlier step failed and no step checked it: {e}");
        }
    }

    // ---------- raw HTTP (behaviour only observable on the wire) ----------

    public HttpStatusCode Status { get; private set; }
    public HttpResponseHeaders? Headers { get; private set; }
    public string Body { get; private set; } = "";
    public JsonNode? Json => Body.Length == 0 ? null : JsonNode.Parse(Body);

    /// <summary>
    /// Send a request as the observer (with "root"'s bearer token when the server requires authentication, none when
    /// <paramref name="anonymous"/>), keeping status, headers and body.
    /// </summary>
    public async Task<HttpStatusCode> SendAsync(HttpMethod method, string path, JsonNode? body = null, bool anonymous = false,
        string? bearer = null, Action<HttpRequestMessage>? configure = null)
    {
        using var http = Host.Http();
        using var request = new HttpRequestMessage(method, path);
        if (body is not null) request.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        bearer ??= AuthRequired && !anonymous ? await TokenAsync("root") : null;
        if (bearer is not null) request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", bearer);
        configure?.Invoke(request);
        using var response = await http.SendAsync(request);
        Status = response.StatusCode;
        Headers = response.Headers;
        Body = await response.Content.ReadAsStringAsync();
        return Status;
    }

    public async Task<string> TokenAsync(string principal)
    {
        using var http = Host.Http();
        using var form = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = principal,
            ["client_secret"] = Secrets[principal],
            ["scope"] = "catalog",
        });
        using var response = await http.PostAsync("/v1/oauth/tokens", form);
        response.EnsureSuccessStatusCode();
        return JsonNode.Parse(await response.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
    }

    // ---------- naming ----------

    /// <summary>"sales.staging" → the namespace of those levels.</summary>
    public static Namespace Ns(string dotted) => Namespace.Of(dotted.Split('.'));

    /// <summary>"sales.staging.events" → namespace "sales.staging", name "events".</summary>
    public static TableIdentifier Id(string dotted)
    {
        var dot = dotted.LastIndexOf('.');
        return new TableIdentifier(Ns(dotted[..dot]), dotted[(dot + 1)..]);
    }

    public static string NsPath(string dotted) => $"/v1/namespaces/{RestCatalog.EncodeNamespace(Ns(dotted))}";
    public static string TablePath(string dotted) => $"{NsPath(Id(dotted).Namespace.ToString())}/tables/{Uri.EscapeDataString(Id(dotted).Name)}";
    public static string ViewPath(string dotted) => $"{NsPath(Id(dotted).Namespace.ToString())}/views/{Uri.EscapeDataString(Id(dotted).Name)}";

    public static JsonObject Identifier(string dotted) => new()
    {
        ["namespace"] = new JsonArray(Id(dotted).Namespace.Levels.Select(l => (JsonNode)l).ToArray()),
        ["name"] = Id(dotted).Name,
    };

    public async Task DisposeAsync()
    {
        foreach (var (client, _) in _clients.Values) client.Dispose();
        if (_catalog is not null && !_clients.Values.Any(c => ReferenceEquals(c.Client, _catalog))) _catalog.Dispose();
        if (_host is not null) await _host.DisposeAsync();
    }
}

/// <summary>Remembers the status of every response a client receives, so a refusal can be told apart (401 vs 403).</summary>
public sealed class StatusRecorder : DelegatingHandler
{
    private readonly List<HttpStatusCode> _statuses = [];

    public IReadOnlyList<HttpStatusCode> Statuses
    {
        get { lock (_statuses) return _statuses.ToList(); }
    }

    public HttpStatusCode? Last
    {
        get { lock (_statuses) return _statuses.Count == 0 ? null : _statuses[^1]; }
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var response = await base.SendAsync(request, cancellationToken);
        lock (_statuses) _statuses.Add(response.StatusCode);
        return response;
    }
}
