using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Idempotency keys and plan IDs live in the catalog's store (<see cref="IServerStateStore"/>), so two server instances
/// over one database (behind a load balancer, say) honour each other's keys and answer each other's plan IDs. Run over
/// one SQLite file and over one shared in-memory store.
/// </summary>
[Spec("rest: getConfig")]
[Spec("rest: fetchPlanningResult")]
public sealed class SharedStateTests
{
    /// <summary>Two servers over one SQLite file (and warehouse), or over one in-memory store.</summary>
    private sealed class Pair : IAsyncDisposable
    {
        private readonly string _root = Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests", Guid.NewGuid().ToString("N"));
        public ServerHost One { get; }
        public ServerHost Two { get; }

        public Pair(string store, IReadOnlyDictionary<string, string>? settings = null)
        {
            ICatalogStore? shared = store == "memory" ? new InMemoryCatalogStore() : null;
            One = ServerHost.Start(store, settings, shared, _root);
            Two = ServerHost.Start(store, settings, shared, _root);
        }

        public async ValueTask DisposeAsync()
        {
            await One.DisposeAsync();
            await Two.DisposeAsync();
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
        }
    }

    public static TheoryData<string> Stores => new() { "sqlite", "memory" };

    private static async Task<(HttpStatusCode Status, JsonNode? Body, bool Replayed)> SendAsync(HttpClient http, HttpMethod method, string path, string key, JsonNode? body = null)
    {
        using var req = new HttpRequestMessage(method, path);
        req.Headers.Add("Idempotency-Key", key);
        if (body is not null) req.Content = ProtocolTests.JsonBody(body);
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text), resp.Headers.Contains("Idempotent-Replayed"));
    }

    private static JsonObject CreateNs(string name, string note = "x") =>
        new() { ["namespace"] = new JsonArray(name), ["properties"] = new JsonObject { ["note"] = note } };

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task A_key_used_on_one_server_is_replayed_by_the_other(string store)
    {
        await using var pair = new Pair(store);
        using var one = pair.One.Http();
        using var two = pair.Two.Http();
        var key = Guid.NewGuid().ToString();

        var first = await SendAsync(one, HttpMethod.Post, "/v1/namespaces", key, CreateNs("a"));
        Assert.Equal(HttpStatusCode.OK, first.Status);
        Assert.Equal(HttpStatusCode.NoContent, (await ProtocolTests.SendAsync(two, HttpMethod.Delete, "/v1/namespaces/a")).Status);
        var replay = await SendAsync(two, HttpMethod.Post, "/v1/namespaces", key, CreateNs("a"));
        Assert.True(replay.Replayed);
        Assert.True(JsonNode.DeepEquals(first.Body, replay.Body));
        Assert.Equal(HttpStatusCode.NotFound, (await ProtocolTests.SendAsync(one, HttpMethod.Get, "/v1/namespaces/a")).Status);
        Assert.Equal(HttpStatusCode.UnprocessableEntity, (await SendAsync(two, HttpMethod.Post, "/v1/namespaces", key, CreateNs("a", "other"))).Status);

        // Duplicates racing on both servers run once.
        var raced = Guid.NewGuid().ToString();
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => SendAsync(i % 2 == 0 ? one : two, HttpMethod.Post, "/v1/namespaces", raced, CreateNs("once"))));
        Assert.All(results, r => Assert.Equal(HttpStatusCode.OK, r.Status));
        Assert.Equal(7, results.Count(r => r.Replayed));
        Assert.True(await pair.One.Service<IdempotencyStore>().CountAsync() >= 2);
    }

    /// <summary>Sends to the first server until a commit's response is lost, then to the second, as a load balancer might.</summary>
    private sealed class FailOver(HttpMessageHandler first, HttpMessageHandler second) : HttpMessageHandler
    {
        private readonly HttpMessageInvoker _first = new(first);
        private readonly HttpMessageInvoker _second = new(second);
        public bool Switched { get; private set; }
        public List<string?> CommitKeys { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var isCommit = request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath.EndsWith("/tables/events", StringComparison.Ordinal);
            if (isCommit) CommitKeys.Add(request.Headers.TryGetValues("Idempotency-Key", out var k) ? k.Single() : null);
            var response = await (Switched ? _second : _first).SendAsync(request, ct);
            if (isCommit && !Switched)
            {
                Switched = true;
                response.Dispose();
                throw new HttpRequestException("connection reset (simulated)");
            }
            return response;
        }
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task A_commit_whose_response_was_lost_is_retried_on_the_other_server_without_applying_twice(string store)
    {
        await using var pair = new Pair(store);
        var failover = new FailOver(pair.One.Factory.Server.CreateHandler(), pair.Two.Factory.Server.CreateHandler());
        using var catalog = await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = new Uri("http://localhost/"), Handler = failover, AccessDelegation = null },
            _ => new IO.LocalFileIO());
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "events"), ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 5))).CommitAsync();
        Assert.True(failover.Switched);
        Assert.Equal(2, failover.CommitKeys.Count);
        Assert.Equal(failover.CommitKeys[0], failover.CommitKeys[1]);
        var reloaded = await catalog.LoadTableAsync(t.Identifier);
        Assert.Single(reloaded.Metadata.Snapshots);
        Assert.Equal(5, (await TableScan.ReadAllAsync(reloaded)).Count);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Plan_ids_are_answered_and_cancelled_by_any_server(string store)
    {
        await using var pair = new Pair(store);
        using var catalog = await pair.One.ConnectAsync();
        await ScanPlanningTests.TableWithDeletesAsync(catalog, "shared", 2, "position");
        using var one = pair.One.Http();
        using var two = pair.Two.Http();
        const string path = "/v1/namespaces/db/tables/shared/plan";

        var (_, planned) = await ProtocolTests.SendAsync(one, HttpMethod.Post, path, new JsonObject { ["filter"] = ExpressionJson.ToJson(Expr.LessThan("id", 20L)) });
        var planId = planned!["plan-id"]!.GetValue<string>();
        var (status, fetched) = await ProtocolTests.SendAsync(two, HttpMethod.Get, $"{path}/{planId}");
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("completed", fetched!["status"]!.GetValue<string>());
        Assert.True(JsonNode.DeepEquals(planned["file-scan-tasks"], fetched["file-scan-tasks"]));
        Assert.True(JsonNode.DeepEquals(planned["delete-files"], fetched["delete-files"]));
        // Planned for another table: not found there.
        ProtocolTests.AssertError(HttpStatusCode.NotFound, "NoSuchPlanIdException", await ProtocolTests.SendAsync(two, HttpMethod.Get, $"/v1/namespaces/db/tables/other/plan/{planId}"));

        Assert.Equal(HttpStatusCode.NoContent, (await ProtocolTests.SendAsync(two, HttpMethod.Delete, $"{path}/{planId}")).Status);
        ProtocolTests.AssertError(HttpStatusCode.NotFound, "NoSuchPlanIdException", await ProtocolTests.SendAsync(one, HttpMethod.Get, $"{path}/{planId}"));
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task An_asynchronous_plan_started_on_one_server_is_polled_on_the_other(string store)
    {
        await using var pair = new Pair(store, new Dictionary<string, string> { ["ScanPlanning:Async"] = "true" });
        using var catalog = await pair.One.ConnectAsync();
        var table = await ScanPlanningTests.TableWithDeletesAsync(catalog, "async", 3, "position");
        using var one = pair.One.Http();
        using var two = pair.Two.Http();
        const string path = "/v1/namespaces/db/tables/async/plan";
        var (_, submitted) = await ProtocolTests.SendAsync(one, HttpMethod.Post, path, new JsonObject());
        Assert.Equal("submitted", submitted!["status"]!.GetValue<string>());
        var planId = submitted["plan-id"]!.GetValue<string>();
        JsonNode? result = null;
        for (var i = 0; i < 200; i++)
        {
            (_, result) = await ProtocolTests.SendAsync(two, HttpMethod.Get, $"{path}/{planId}");
            if (result!["status"]!.GetValue<string>() != "submitted") break;
            await Task.Delay(25);
        }
        Assert.Equal("completed", result!["status"]!.GetValue<string>());
        var plan = ServerScanPlan.FromCompletedResponse(result, table.Metadata);
        Assert.Equal((await TableScan.ReadAllAsync(table)).Select(r => (long)r[0]!).Order(),
            (await ServerSideScan.ReadAsync(table, plan)).Select(r => (long)r[0]!).Order());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Idempotency_capacity_is_shared_and_existing_keys_still_replay(string store)
    {
        await using var pair = new Pair(store, new Dictionary<string, string> { ["Idempotency:MaxClaimsPerPrincipal"] = "1" });
        using var one = pair.One.Http();
        using var two = pair.Two.Http();
        Assert.Equal(HttpStatusCode.OK, (await SendAsync(one, HttpMethod.Post, "/v1/namespaces", "first", CreateNs("first"))).Status);
        Assert.Equal(HttpStatusCode.TooManyRequests, (await SendAsync(two, HttpMethod.Post, "/v1/namespaces", "second", CreateNs("second"))).Status);
        Assert.True((await SendAsync(two, HttpMethod.Post, "/v1/namespaces", "first", CreateNs("first"))).Replayed);
        Assert.Equal(HttpStatusCode.NotFound, (await ProtocolTests.SendAsync(one, HttpMethod.Get, "/v1/namespaces/second")).Status);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task Concurrent_claims_obey_the_budget_and_other_principals_are_independent(string store)
    {
        var root = Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var one = StateStore(store, root);
            var two = store == "memory" ? one : StateStore(store, root);
            await ((ICatalogStore)one).InitializeAsync();
            var now = DateTimeOffset.UtcNow;
            async Task<bool> Claim(int i)
            {
                try
                {
                    await (i % 2 == 0 ? one : two).ClaimAsync("p", i.ToString(), "f", now, now.AddHours(-1), 2);
                    return true;
                }
                catch (IdempotencyCapacityException) { return false; }
            }
            Assert.Equal(2, (await Task.WhenAll(Enumerable.Range(0, 16).Select(Claim))).Count(x => x));
            Assert.Null(await two.ClaimAsync("other", "separate", "f", now, now.AddHours(-1), 2));
            Assert.Null(await one.ClaimAsync("p", "after-expiry", "f", now.AddHours(2), now.AddHours(1), 2));
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            Directory.Delete(root, recursive: true);
        }
    }

    private static IServerStateStore StateStore(string store, string root) => store switch
    {
        "memory" => new InMemoryCatalogStore(),
        _ => SqlCatalogStore.Sqlite(Path.Combine(root, "state.db")),
    };

    /// <summary>The <see cref="IServerStateStore"/> contract, the same for both stores.</summary>
    [Theory]
    [MemberData(nameof(Stores))]
    public async Task The_state_store_contract(string store)
    {
        var root = Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var state = StateStore(store, root);
            await ((ICatalogStore)state).InitializeAsync();
            var t0 = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000);
            var later = t0.AddMinutes(10);

            // Claim, see the running claim, finish it, replay it.
            Assert.Null(await state.ClaimAsync("p", "k", "f1", t0, t0.AddHours(-1)));
            var running = await state.ClaimAsync("p", "k", "f1", t0.AddSeconds(1), t0.AddHours(-1));
            Assert.Equal(("f1", t0, (StoredResponse?)null), (running!.Fingerprint, running.Claimed, running.Response));
            Assert.Null(await state.ClaimAsync("other", "k", "f1", t0, t0.AddHours(-1))); // keys are per principal
            await state.CompleteAsync("p", "k", t0, new StoredResponse(409, "application/json", "\"e\"", [1, 2, 3]));
            var done = await state.ClaimAsync("p", "k", "f1", later, t0.AddHours(-1));
            Assert.Equal(409, done!.Response!.Status);
            Assert.Equal(new byte[] { 1, 2, 3 }, done.Response.Body);
            Assert.Equal("\"e\"", done.Response.ETag);
            // A finished claim is not released; one older than the cut-off is replaced.
            await state.ReleaseAsync("p", "k", t0);
            Assert.NotNull(await state.ClaimAsync("p", "k", "f1", later, t0.AddHours(-1)));
            Assert.Null(await state.ClaimAsync("p", "k", "f2", later, t0.AddSeconds(1)));
            // Releasing someone else's (newer) claim does nothing; releasing your own frees the key.
            await state.ReleaseAsync("p", "k", t0);
            Assert.NotNull(await state.ClaimAsync("p", "k", "f2", later, t0));
            await state.ReleaseAsync("p", "k", later);
            Assert.Null(await state.ClaimAsync("p", "k", "f3", later, t0));

            // Plans: add, finish once, a cancelled (deleted) plan cannot finish.
            var table = new TableIdentifier(Namespace.Of("a", "b"), "t");
            await state.AddPlanAsync(new PlanRecord("p1", table, "submitted", null, null, t0, null));
            Assert.True(await state.FinishPlanAsync("p1", "completed", "{\"x\":1}", null, later));
            Assert.False(await state.FinishPlanAsync("p1", "failed", null, "late", later));
            var p1 = await state.LoadPlanAsync("p1");
            Assert.Equal(new PlanRecord("p1", table, "completed", "{\"x\":1}", null, t0, later), p1);
            await state.AddPlanAsync(new PlanRecord("p2", table, "submitted", null, null, t0, null));
            Assert.True(await state.DeletePlanAsync("p2"));
            Assert.False(await state.FinishPlanAsync("p2", "completed", "{}", null, later));
            Assert.Null(await state.LoadPlanAsync("p2"));
            Assert.False(await state.DeletePlanAsync("p2"));

            // Expiry: old claims, finished plans past their lifetime, plans stuck in submitted.
            await state.AddPlanAsync(new PlanRecord("p3", table, "submitted", null, null, t0, null));
            Assert.Equal((2, 2), await state.CountAsync());
            await state.ExpireAsync(later.AddSeconds(1), later.AddSeconds(1), t0.AddSeconds(1));
            Assert.Equal((0, 0), await state.CountAsync());
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>A claim whose request never finished (its server died) is taken over after the pending timeout.</summary>
    [Fact]
    public async Task An_abandoned_claim_is_taken_over_after_the_pending_timeout()
    {
        await using var host = ServerHost.Start("sqlite", new Dictionary<string, string> { ["Idempotency:PendingTimeout"] = "00:00:00.200" });
        var state = host.Service<IServerStateStore>();
        var key = Guid.NewGuid().ToString();
        // The claim a crashed server left behind: same principal, same request fingerprint, never completed.
        using var http = host.Http();
        var body = CreateNs("orphan");
        var fingerprint = Fingerprint("POST", "/v1/namespaces", body.ToJsonString());
        Assert.Null(await state.ClaimAsync("anonymous", key, fingerprint, DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
            DateTimeOffset.UtcNow.AddHours(-1)));
        var result = await SendAsync(http, HttpMethod.Post, "/v1/namespaces", key, body);
        Assert.Equal(HttpStatusCode.OK, result.Status);
        Assert.False(result.Replayed);
        Assert.True((await SendAsync(http, HttpMethod.Post, "/v1/namespaces", key, body)).Replayed);
    }

    private static string Fingerprint(string method, string path, string body)
    {
        using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256);
        hash.AppendData(System.Text.Encoding.UTF8.GetBytes($"{method} {path}\n"));
        hash.AppendData(System.Text.Encoding.UTF8.GetBytes(body));
        return Convert.ToHexString(hash.GetHashAndReset());
    }
}
