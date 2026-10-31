using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// The authorization matrix: principal × operation × resource → status, for every route the server maps. Each row
/// starts a fresh server, seeds it as <c>root</c>, then sends its request as each principal in turn (least privileged
/// first, so a successful destructive request runs last). <c>D</c> is a refusal by the authorizer (403 naming the
/// missing privilege); a number is the exact status of a request that got past authorization, a 403 included (the
/// signer's own refusal). <see cref="Every_route_the_server_maps_is_in_the_matrix"/> fails for a route no row covers.
/// </summary>
public sealed class AuthorizationMatrixTests
{
    /// <summary>In the order every row runs, with their grants.</summary>
    private static readonly string[] Principals = ["nobody", "reader", "writer", "owner", "root"];

    private static readonly Dictionary<string, string> Settings = new()
    {
        ["Auth:Clients:nobody"] = "nobody-secret",
        ["Auth:Clients:reader"] = "reader-secret",
        ["Auth:Clients:writer"] = "writer-secret",
        ["Auth:Clients:owner"] = "owner-secret",
        ["Auth:Clients:root"] = "root-secret",
        ["Auth:SigningKey"] = "an-authorization-matrix-signing-key-32-bytes",
        ["Authorization:Grants:reader:0"] = "read:sales",
        ["Authorization:Grants:writer:0"] = "write:sales",
        ["Authorization:Grants:owner:0"] = "admin:sales",
        ["Authorization:Grants:root:0"] = "admin:*",
        // Every optional route on: plan tasks (one manifest per task), credentials and remote signing.
        ["ScanPlanning:FilesPerPlanTask"] = "1",
        ["RemoteSigning:Enabled"] = "true",
        ["RemoteSigning:Region"] = "eu-west-1",
        ["RemoteSigning:AccessKeyId"] = "AKIDEXAMPLE",
        ["RemoteSigning:SecretAccessKey"] = "secret",
    };

    private sealed class StubVendor : IStorageCredentialVendor
    {
        public Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<VendedCredential>>([new VendedCredential(location, new Dictionary<string, string> { ["s3.access-key-id"] = write ? "W" : "R" }, null)]);
    }

    /// <summary>What the seed leaves behind for the rows to use.</summary>
    private sealed record Seeded(string PlanId, string PlanTask, IReadOnlyDictionary<string, string> Registrable, string OrdersMetadata);

    private sealed record Row(string Route, string Case, string Expect, Func<Seeded, string, (HttpMethod Method, string Path, JsonNode? Body)> Request);

    private const string T = "/v1/namespaces/sales/tables/orders";

    private static JsonObject Obj(string json) => JsonNode.Parse(json)!.AsObject();

    private static JsonObject SetProperties => Obj("""{"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}""");

    private static JsonObject ViewBody(string name) => Obj($$$"""
        {"name":"{{{name}}}","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":false,"type":"long"}]},
         "view-version":{"version-id":1,"timestamp-ms":1700000000000,"schema-id":0,"summary":{"engine-name":"test"},
           "representations":[{"type":"sql","sql":"SELECT 1 AS x","dialect":"trino"}],"default-namespace":["sales"]},
         "properties":{}}
        """);

    private static JsonObject Rename(string fromNs, string from, string toNs, string to) =>
        Obj($$$"""{"source":{"namespace":["{{{fromNs}}}"],"name":"{{{from}}}"},"destination":{"namespace":["{{{toNs}}}"],"name":"{{{to}}}"}}""");

    private static JsonObject Sign(string method) =>
        Obj($$$"""{"region":"eu-west-1","uri":"https://bucket.s3.eu-west-1.amazonaws.com/wh/x","method":"{{{method}}}","headers":{}}""");

    //                                                                         nobody reader writer owner root
    private static readonly Row[] Rows =
    [
        new("GET /v1/config", "config", "200 200 200 200 200", (_, _) => (HttpMethod.Get, "/v1/config", null)),

        new("GET /v1/namespaces", "list top level", "200 200 200 200 200", (_, _) => (HttpMethod.Get, "/v1/namespaces", null)),
        new("GET /v1/namespaces", "list under hr", "D D D D 200", (_, _) => (HttpMethod.Get, "/v1/namespaces?parent=hr", null)),
        new("POST /v1/namespaces", "create under sales", "D D D 200 200",
            (_, p) => (HttpMethod.Post, "/v1/namespaces", Obj($$$"""{"namespace":["sales","ns_{{{p}}}"],"properties":{}}"""))),
        new("POST /v1/namespaces", "create at the top", "D D D D 200",
            (_, p) => (HttpMethod.Post, "/v1/namespaces", Obj($$$"""{"namespace":["top_{{{p}}}"],"properties":{}}"""))),
        new("GET /v1/namespaces/{namespace}", "load sales", "D 200 200 200 200", (_, _) => (HttpMethod.Get, "/v1/namespaces/sales", null)),
        new("GET /v1/namespaces/{namespace}", "load hr", "D D D D 200", (_, _) => (HttpMethod.Get, "/v1/namespaces/hr", null)),
        new("HEAD /v1/namespaces/{namespace}", "exists sales", "D 204 204 204 204", (_, _) => (HttpMethod.Head, "/v1/namespaces/sales", null)),
        new("DELETE /v1/namespaces/{namespace}", "drop sales.empty", "D D D 204 404",
            (_, _) => (HttpMethod.Delete, "/v1/namespaces/sales%1Fempty", null)),
        new("POST /v1/namespaces/{namespace}/properties", "set properties", "D D D 200 200",
            (_, _) => (HttpMethod.Post, "/v1/namespaces/sales/properties", Obj("""{"updates":{"owner":"x"},"removals":[]}"""))),

        new("GET /v1/namespaces/{namespace}/tables", "list sales", "D 200 200 200 200", (_, _) => (HttpMethod.Get, "/v1/namespaces/sales/tables", null)),
        new("POST /v1/namespaces/{namespace}/tables", "create in sales", "D D 200 200 200",
            (_, p) => (HttpMethod.Post, "/v1/namespaces/sales/tables", Obj($$$"""{"name":"t_{{{p}}}","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":false,"type":"long"}]}}"""))),
        new("POST /v1/namespaces/{namespace}/tables", "create in hr", "D D D D 200",
            (_, p) => (HttpMethod.Post, "/v1/namespaces/hr/tables", Obj($$$"""{"name":"t_{{{p}}}","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":false,"type":"long"}]}}"""))),
        new("POST /v1/namespaces/{namespace}/register", "register", "D D 200 200 200",
            (s, p) => (HttpMethod.Post, "/v1/namespaces/sales/register", new JsonObject { ["name"] = $"reg_{p}", ["metadata-location"] = s.Registrable[p] })),
        new("POST /v1/namespaces/{namespace}/register", "register with overwrite", "D D D 200 200",
            (s, _) => (HttpMethod.Post, "/v1/namespaces/sales/register", new JsonObject { ["name"] = "orders", ["metadata-location"] = s.OrdersMetadata, ["overwrite"] = true })),
        new("GET /v1/namespaces/{namespace}/tables/{table}", "load sales.orders", "D 200 200 200 200", (_, _) => (HttpMethod.Get, T, null)),
        new("GET /v1/namespaces/{namespace}/tables/{table}", "load hr.people", "D D D D 200", (_, _) => (HttpMethod.Get, "/v1/namespaces/hr/tables/people", null)),
        new("HEAD /v1/namespaces/{namespace}/tables/{table}", "exists", "D 204 204 204 204", (_, _) => (HttpMethod.Head, T, null)),
        new("POST /v1/namespaces/{namespace}/tables/{table}", "commit", "D D 200 200 200", (_, _) => (HttpMethod.Post, T, SetProperties)),
        new("DELETE /v1/namespaces/{namespace}/tables/{table}", "drop", "D D D 204 404", (_, _) => (HttpMethod.Delete, "/v1/namespaces/sales/tables/dropme", null)),
        new("POST /v1/namespaces/{namespace}/tables/{table}/unregister", "unregister", "D D D 200 404",
            (_, _) => (HttpMethod.Post, "/v1/namespaces/sales/tables/dropme/unregister", null)),
        new("POST /v1/namespaces/{namespace}/tables/{table}/metrics", "report metrics", "D 204 204 204 204", (_, _) => (HttpMethod.Post, T + "/metrics", new JsonObject())),

        new("POST /v1/namespaces/{namespace}/tables/{table}/plan", "plan", "D 200 200 200 200", (_, _) => (HttpMethod.Post, T + "/plan", new JsonObject())),
        // A plan ID is root's: nobody else can fetch or cancel it, whatever they may read.
        new("GET /v1/namespaces/{namespace}/tables/{table}/plan/{plan-id}", "fetch root's plan", "D 404 404 404 200",
            (s, _) => (HttpMethod.Get, $"{T}/plan/{s.PlanId}", null)),
        new("DELETE /v1/namespaces/{namespace}/tables/{table}/plan/{plan-id}", "cancel root's plan", "D 404 404 404 204",
            (s, _) => (HttpMethod.Delete, $"{T}/plan/{s.PlanId}", null)),
        // A plan task is self-contained and checked against the table on use: it names only manifests of a table the
        // caller can read, so anyone who may read the table may use it (and could have planned it themselves).
        new("POST /v1/namespaces/{namespace}/tables/{table}/tasks", "root's plan task", "D 200 200 200 200",
            (s, _) => (HttpMethod.Post, T + "/tasks", new JsonObject { ["plan-task"] = s.PlanTask })),
        new("POST /v1/namespaces/{namespace}/tables/{table}/tasks", "a sales task on hr's route", "D D D D 404",
            (s, _) => (HttpMethod.Post, "/v1/namespaces/hr/tables/people/tasks", new JsonObject { ["plan-task"] = s.PlanTask })),

        new("GET /v1/namespaces/{namespace}/tables/{table}/credentials", "credentials", "D 200 200 200 200", (_, _) => (HttpMethod.Get, T + "/credentials", null)),
        // Past authorization the signer refuses on its own (the URI is not under this local table): 403, not D.
        new("POST /v1/namespaces/{namespace}/tables/{table}/sign", "sign a read", "D 403 403 403 403", (_, _) => (HttpMethod.Post, T + "/sign", Sign("GET"))),
        new("POST /v1/namespaces/{namespace}/tables/{table}/sign", "sign a write", "D D 403 403 403", (_, _) => (HttpMethod.Post, T + "/sign", Sign("PUT"))),

        // Rename needs admin on the source and write on the destination: owner administers sales but cannot write hr.
        new("POST /v1/tables/rename", "into a namespace without write", "D D D D 204", (_, _) => (HttpMethod.Post, "/v1/tables/rename", Rename("sales", "ren", "hr", "ren"))),
        new("POST /v1/tables/rename", "out of a namespace without admin", "D D D D 204", (_, _) => (HttpMethod.Post, "/v1/tables/rename", Rename("hr", "people", "sales", "people"))),
        new("POST /v1/tables/rename", "within sales", "D D D 204 404", (_, _) => (HttpMethod.Post, "/v1/tables/rename", Rename("sales", "ren", "sales", "ren2"))),
        new("POST /v1/transactions/commit", "commit sales.orders", "D D 204 204 204", (_, _) => (HttpMethod.Post, "/v1/transactions/commit",
            new JsonObject { ["table-changes"] = new JsonArray(Change("sales", "orders")) })),
        new("POST /v1/transactions/commit", "commit sales.orders and hr.people", "D D D D 204", (_, _) => (HttpMethod.Post, "/v1/transactions/commit",
            new JsonObject { ["table-changes"] = new JsonArray(Change("sales", "orders"), Change("hr", "people")) })),

        new("GET /v1/namespaces/{namespace}/views", "list views", "D 200 200 200 200", (_, _) => (HttpMethod.Get, "/v1/namespaces/sales/views", null)),
        new("POST /v1/namespaces/{namespace}/views", "create view", "D D 200 200 200", (_, p) => (HttpMethod.Post, "/v1/namespaces/sales/views", ViewBody($"v_{p}"))),
        new("POST /v1/namespaces/{namespace}/views", "create view in hr", "D D D D 200", (_, p) => (HttpMethod.Post, "/v1/namespaces/hr/views", ViewBody($"v_{p}"))),
        new("GET /v1/namespaces/{namespace}/views/{view}", "load view", "D 200 200 200 200", (_, _) => (HttpMethod.Get, "/v1/namespaces/sales/views/v", null)),
        new("HEAD /v1/namespaces/{namespace}/views/{view}", "view exists", "D 204 204 204 204", (_, _) => (HttpMethod.Head, "/v1/namespaces/sales/views/v", null)),
        new("POST /v1/namespaces/{namespace}/views/{view}", "replace view", "D D 200 200 200", (_, _) => (HttpMethod.Post, "/v1/namespaces/sales/views/v", SetProperties)),
        new("DELETE /v1/namespaces/{namespace}/views/{view}", "drop view", "D D D 204 404", (_, _) => (HttpMethod.Delete, "/v1/namespaces/sales/views/vdrop", null)),
        new("POST /v1/views/rename", "view into a namespace without write", "D D D D 204", (_, _) => (HttpMethod.Post, "/v1/views/rename", Rename("sales", "vren", "hr", "vren"))),
        new("POST /v1/views/rename", "view within sales", "D D D 204 404", (_, _) => (HttpMethod.Post, "/v1/views/rename", Rename("sales", "vren", "sales", "vren2"))),
    ];

    /// <summary>Mapped routes no row covers, each with the reason.</summary>
    private static readonly Dictionary<string, string> Exempt = new()
    {
        ["POST /v1/oauth/tokens"] = "anonymous by design: it is how a principal authenticates (AuthTests, JwtTests)",
        ["GET /openapi/{documentName}.json"] = "the public description of these routes (OpenApiConformanceTests)",
    };

    private static JsonObject Change(string ns, string name)
    {
        var change = SetProperties;
        change["identifier"] = Obj($$$"""{"namespace":["{{{ns}}}"],"name":"{{{name}}}"}""");
        return change;
    }

    public static TheoryData<string> Cases()
    {
        var data = new TheoryData<string>();
        foreach (var row in Rows) data.Add($"{row.Route} | {row.Case}");
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Each_principal_gets_the_status_its_grants_imply(string name)
    {
        var row = Rows.Single(r => $"{r.Route} | {r.Case}" == name);
        await using var host = ServerHost.Start(settings: Settings, services: s => s.AddSingleton<IStorageCredentialVendor>(new StubVendor()));
        using var http = host.Http();
        var seeded = await SeedAsync(host, http);
        var expected = row.Expect.Split(' ');
        Assert.Equal(Principals.Length, expected.Length);

        var failures = new List<string>();
        for (var i = 0; i < Principals.Length; i++)
        {
            var principal = Principals[i];
            var token = await TokenAsync(http, principal);
            var (method, path, body) = row.Request(seeded, principal);
            var (status, message) = await SendAsync(http, method, path, token, body);
            var denied = status == HttpStatusCode.Forbidden && (method == HttpMethod.Head || message.Contains(" may not ", StringComparison.Ordinal));
            var actual = denied ? "D" : ((int)status).ToString(System.Globalization.CultureInfo.InvariantCulture);
            if (actual != expected[i]) failures.Add($"{principal}: expected {expected[i]}, got {actual} {message}");
        }
        Assert.True(failures.Count == 0, $"{name}\n{string.Join('\n', failures)}");
    }

    [Fact]
    public async Task Every_route_the_server_maps_is_in_the_matrix()
    {
        await using var host = ServerHost.Start(settings: Settings, services: s => s.AddSingleton<IStorageCredentialVendor>(new StubVendor()));
        _ = host.Http();
        var mapped = host.Service<EndpointDataSource>().Endpoints.OfType<RouteEndpoint>()
            .SelectMany(e => (e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? ["*"]).Select(m => $"{m} {e.RoutePattern.RawText!.TrimEnd('/')}"))
            .ToHashSet(StringComparer.Ordinal);
        var covered = Rows.Select(r => r.Route).Concat(Exempt.Keys).ToHashSet(StringComparer.Ordinal);
        Assert.True(mapped.SetEquals(covered),
            $"Routes without a matrix row: {string.Join(", ", mapped.Except(covered))}\nRows for routes that are not mapped: {string.Join(", ", covered.Except(mapped))}");
    }

    /// <summary>
    /// An <c>Idempotency-Key</c> belongs to the principal that used it: another principal sending the same key and body
    /// is authorized and runs afresh, and never receives the first principal's stored response.
    /// </summary>
    [Fact]
    public async Task An_idempotency_key_replayed_by_another_principal_is_not_replayed_to_them()
    {
        await using var host = ServerHost.Start(settings: Settings, services: s => s.AddSingleton<IStorageCredentialVendor>(new StubVendor()));
        using var http = host.Http();
        await SeedAsync(host, http);
        var key = Guid.NewGuid().ToString();
        var create = Obj("""{"namespace":["sales","replayed"],"properties":{}}""");

        async Task<(HttpStatusCode, string, bool)> Send(string principal)
        {
            using var req = new HttpRequestMessage(HttpMethod.Post, "/v1/namespaces") { Content = new StringContent(create.ToJsonString(), Encoding.UTF8, "application/json") };
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await TokenAsync(http, principal));
            req.Headers.Add("Idempotency-Key", key);
            using var resp = await http.SendAsync(req);
            return (resp.StatusCode, await resp.Content.ReadAsStringAsync(), resp.Headers.Contains("Idempotent-Replayed"));
        }

        Assert.Equal((HttpStatusCode.OK, false), (await Send("owner")) is var (s1, _, r1) ? (s1, r1) : default);
        // The owner's own retry is a replay.
        Assert.Equal((HttpStatusCode.OK, true), (await Send("owner")) is var (s2, _, r2) ? (s2, r2) : default);
        // A reader with the same key and body is authorized afresh: refused, not handed the owner's 200.
        var (readerStatus, readerBody, readerReplayed) = await Send("reader");
        Assert.Equal(HttpStatusCode.Forbidden, readerStatus);
        Assert.False(readerReplayed);
        Assert.Contains("reader may not", readerBody, StringComparison.Ordinal);
        // An admin with the same key runs the request again: the namespace exists now.
        var (rootStatus, _, rootReplayed) = await Send("root");
        Assert.Equal(HttpStatusCode.Conflict, rootStatus);
        Assert.False(rootReplayed);
    }

    private static async Task<Seeded> SeedAsync(ServerHost host, HttpClient http)
    {
        using var root = await host.ConnectAsync(credential: "root:root-secret");
        foreach (var ns in new[] { Namespace.Of("sales"), Namespace.Of("hr"), Namespace.Of("sales", "empty") }) await root.CreateNamespaceAsync(ns);
        var orders = await root.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        // Two appends, two manifests: with one manifest per task, a plan answers with plan tasks.
        await SnapshotProducer.Append(orders).AddFiles(await TableWriter.WriteAsync(orders, ScanPlanningTests.Rows(0, 3))).CommitAsync();
        orders = await root.LoadTableAsync(orders.Identifier);
        await SnapshotProducer.Append(orders).AddFiles(await TableWriter.WriteAsync(orders, ScanPlanningTests.Rows(3, 3))).CommitAsync();
        var schema = new Schema(0, [NestedField.Optional(1, "x", PrimitiveType.Long)]);
        foreach (var name in new[] { "dropme", "ren" }) await root.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", name), schema));
        await root.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("hr", "people"), schema));
        foreach (var name in new[] { "v", "vdrop", "vren" })
            await root.CreateViewAsync(TableIdentifier.Of("sales", name), new ViewDefinition(schema, "SELECT 1 AS x", "trino", ["sales"]));

        // A metadata file per principal that a register can adopt: its table dropped (not purged) so nothing occupies its directory.
        var registrable = new Dictionary<string, string>();
        foreach (var p in Principals)
        {
            var t = await root.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", $"reg_{p}"), schema));
            registrable[p] = t.Metadata.MetadataFileLocation!;
            await root.DropTableAsync(t.Identifier);
        }

        var token = await TokenAsync(http, "root");
        using var req = new HttpRequestMessage(HttpMethod.Post, T + "/plan") { Content = new StringContent("{}", Encoding.UTF8, "application/json") };
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var resp = await http.SendAsync(req);
        var plan = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        var current = await root.LoadTableAsync(orders.Identifier);
        return new Seeded(plan["plan-id"]!.GetValue<string>(), plan["plan-tasks"]![0]!.GetValue<string>(), registrable, current.Metadata.MetadataFileLocation!);
    }

    private static async Task<string> TokenAsync(HttpClient http, string principal)
    {
        using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = principal, ["client_secret"] = $"{principal}-secret",
        }));
        resp.EnsureSuccessStatusCode();
        return JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>();
    }

    private static async Task<(HttpStatusCode Status, string Message)> SendAsync(HttpClient http, HttpMethod method, string path, string token, JsonNode? body)
    {
        using var req = new HttpRequestMessage(method, path);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        if (body is not null) req.Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        string message;
        try { message = JsonNode.Parse(text)?["error"]?["message"]?.GetValue<string>() ?? ""; }
        catch (System.Text.Json.JsonException) { message = ""; }
        return (resp.StatusCode, message);
    }
}
