using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// <c>Idempotency-Key</c>: a mutation's final response is replayed for a retry with the same key instead of running it
/// again. Transport-level mutation retries require the advertised key lifetime; producer-level read-only recovery
/// can also resolve a lost response from positive snapshot evidence without replaying the write.
/// </summary>
[Spec("rest: getConfig")]
public sealed class IdempotencyTests
{
    private static async Task<(HttpStatusCode Status, JsonNode? Body, bool Replayed)> SendAsync(HttpClient http, HttpMethod method, string path, string key, JsonNode? body = null)
    {
        using var req = new HttpRequestMessage(method, path);
        req.Headers.Add("Idempotency-Key", key);
        if (body is not null) req.Content = ProtocolTests.JsonBody(body);
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text), resp.Headers.Contains("Idempotent-Replayed"));
    }

    private static JsonObject CreateNs(string name, string? note = null) =>
        new() { ["namespace"] = new JsonArray(name), ["properties"] = new JsonObject { ["note"] = note ?? "x" } };

    [Fact]
    public async Task Config_advertises_the_key_lifetime_and_the_client_picks_it_up()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["Idempotency:Lifetime"] = "00:45:00" });
        using var http = host.Http();
        var (_, config) = await ProtocolTests.SendAsync(http, HttpMethod.Get, "/v1/config");
        Assert.Equal("PT45M", config!["idempotency-key-lifetime"]!.GetValue<string>());
        using var catalog = await host.ConnectAsync();
        Assert.Equal(TimeSpan.FromMinutes(45), catalog.IdempotencyKeyLifetime);

        await using var off = ServerHost.Start(settings: new Dictionary<string, string> { ["Idempotency:Enabled"] = "false" });
        using var offHttp = off.Http();
        Assert.Null((await ProtocolTests.SendAsync(offHttp, HttpMethod.Get, "/v1/config")).Body!["idempotency-key-lifetime"]);
    }

    [Fact]
    public async Task A_replayed_key_returns_the_first_response_without_running_again()
    {
        await using var host = ServerHost.Start();
        using var http = host.Http();
        var key = Guid.NewGuid().ToString();

        var first = await SendAsync(http, HttpMethod.Post, "/v1/namespaces", key, CreateNs("a"));
        Assert.Equal(HttpStatusCode.OK, first.Status);
        Assert.False(first.Replayed);
        // Dropped in between: a replay must not create it again.
        Assert.Equal(HttpStatusCode.NoContent, (await ProtocolTests.SendAsync(http, HttpMethod.Delete, "/v1/namespaces/a")).Status);
        var replay = await SendAsync(http, HttpMethod.Post, "/v1/namespaces", key, CreateNs("a"));
        Assert.Equal(HttpStatusCode.OK, replay.Status);
        Assert.True(replay.Replayed);
        Assert.True(JsonNode.DeepEquals(first.Body, replay.Body));
        Assert.Equal(HttpStatusCode.NotFound, (await ProtocolTests.SendAsync(http, HttpMethod.Get, "/v1/namespaces/a")).Status);

        // The same key for another request is refused.
        var other = await SendAsync(http, HttpMethod.Post, "/v1/namespaces", key, CreateNs("a", "different"));
        Assert.Equal(HttpStatusCode.UnprocessableEntity, other.Status);
        Assert.Equal("UnprocessableEntityException", other.Body!["error"]!["type"]!.GetValue<string>());

        // A terminal 4xx is final too: the replay is the same 409 even once the conflict is gone.
        await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces", CreateNs("b"));
        var conflictKey = Guid.NewGuid().ToString();
        Assert.Equal(HttpStatusCode.Conflict, (await SendAsync(http, HttpMethod.Post, "/v1/namespaces", conflictKey, CreateNs("b"))).Status);
        await ProtocolTests.SendAsync(http, HttpMethod.Delete, "/v1/namespaces/b");
        var conflictReplay = await SendAsync(http, HttpMethod.Post, "/v1/namespaces", conflictKey, CreateNs("b"));
        Assert.Equal(HttpStatusCode.Conflict, conflictReplay.Status);
        Assert.True(conflictReplay.Replayed);

        // Requests without a key, and reads, are not kept.
        var kept = await host.Service<IdempotencyStore>().CountAsync();
        await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces", CreateNs("c"));
        await SendAsync(http, HttpMethod.Get, "/v1/namespaces", Guid.NewGuid().ToString());
        Assert.Equal(kept, await host.Service<IdempotencyStore>().CountAsync());
    }

    [Fact]
    public async Task Concurrent_duplicates_run_once()
    {
        await using var host = ServerHost.Start(store: "sqlite");
        using var http = host.Http();
        var key = Guid.NewGuid().ToString();
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => SendAsync(http, HttpMethod.Post, "/v1/namespaces", key, CreateNs("once"))));
        Assert.All(results, r => Assert.Equal(HttpStatusCode.OK, r.Status));
        Assert.Equal(7, results.Count(r => r.Replayed));
    }

    /// <summary>Loses the first commit response, optionally before the request reaches the server.</summary>
    private sealed class LoseFirstCommitResponse(HttpMessageHandler inner, bool applyBeforeLoss = true) : DelegatingHandler(inner)
    {
        public int Lost { get; private set; }
        public int StatusReadsAfterLoss { get; private set; }
        public List<string?> Keys { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var isCommit = request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath.EndsWith("/tables/events", StringComparison.Ordinal);
            if (isCommit) Keys.Add(request.Headers.TryGetValues("Idempotency-Key", out var k) ? k.Single() : null);
            if (Lost > 0 && request.Method == HttpMethod.Get && request.RequestUri!.AbsolutePath.EndsWith("/tables/events", StringComparison.Ordinal))
                StatusReadsAfterLoss++;
            if (isCommit && Lost == 0 && !applyBeforeLoss)
            {
                Lost++;
                throw new HttpRequestException("connection reset before dispatch (simulated)");
            }
            var response = await base.SendAsync(request, ct);
            if (isCommit && Lost == 0)
            {
                Lost++;
                response.Dispose();
                throw new HttpRequestException("connection reset (simulated)");
            }
            return response;
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    [Spec("Commit Conflict Resolution and Retry")]
    public async Task A_lost_applied_commit_response_is_resolved_without_duplicate_writes(bool enabled)
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["Idempotency:Enabled"] = enabled.ToString() });
        LoseFirstCommitResponse? lossy = null;
        using var catalog = await host.ConnectAsync(wrap: inner => lossy = new LoseFirstCommitResponse(inner));
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "events"), ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        var snapshot = await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, ScanPlanningTests.Rows(0, 5))).CommitAsync();
        if (enabled)
        {
            Assert.Equal(2, lossy!.Keys.Count);
            Assert.False(string.IsNullOrEmpty(lossy.Keys[0]));
            Assert.Equal(lossy.Keys[0], lossy.Keys[1]);
        }
        else
        {
            // Positive snapshot evidence resolves success through reads; no write replay is permitted.
            Assert.Single(lossy!.Keys);
            Assert.True(lossy.StatusReadsAfterLoss > 0);
        }
        Assert.Equal(1, lossy.Lost);
        var reloaded = await catalog.LoadTableAsync(t.Identifier);
        Assert.Equal(snapshot.SnapshotId, Assert.Single(reloaded.Metadata.Snapshots).SnapshotId);
        Assert.Equal(5, (await TableScan.ReadAllAsync(reloaded)).Count);
        await TableValidator.AssertValidAsync(reloaded, new ValidationOptions { Deep = true, AllSnapshots = true });
    }

    [Fact]
    [Spec("Commit Conflict Resolution and Retry")]
    public async Task A_lost_unapplied_commit_without_idempotency_remains_unknown_and_is_not_replayed()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["Idempotency:Enabled"] = "false" });
        LoseFirstCommitResponse? lossy = null;
        using var catalog = await host.ConnectAsync(wrap: inner => lossy = new LoseFirstCommitResponse(inner, applyBeforeLoss: false));
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "events"), ScanPlanningTests.Schema, ScanPlanningTests.Spec));
        var files = await TableWriter.WriteAsync(table, ScanPlanningTests.Rows(0, 5));

        await Assert.ThrowsAsync<CommitStateUnknownException>(() => SnapshotProducer.Append(table).AddFiles(files).CommitAsync());

        Assert.Single(lossy!.Keys);
        Assert.Equal(1, lossy.Lost);
        Assert.True(lossy.StatusReadsAfterLoss > 0);
        var fresh = await catalog.LoadTableAsync(table.Identifier);
        Assert.Empty(fresh.Metadata.Snapshots);
        Assert.Empty(await TableScan.ReadAllAsync(fresh));
        foreach (var file in files) Assert.True(await table.IO.ExistsAsync(file.FilePath));
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
    }
}
