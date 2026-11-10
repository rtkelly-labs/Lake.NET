using System.Diagnostics;
using System.Net;
using System.Text;
using Iceberg.Net.Catalog;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Spec.Tests;

/// <summary>REST mutations carry a UUIDv7 <c>Idempotency-Key</c>, and are retried with the same key only when the server deduplicates.</summary>
public sealed class IdempotencyTests
{
    /// <summary>A scripted server: config, then one canned response per call to the mutation path.</summary>
    private sealed class FakeServer(string? lifetime, params Func<HttpResponseMessage>[] mutationResponses) : HttpMessageHandler
    {
        private int _calls;
        public List<string> Keys { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.RequestUri!.AbsolutePath.EndsWith("/v1/config", StringComparison.Ordinal))
                return Task.FromResult(Json(lifetime is null ? """{"defaults":{},"overrides":{}}""" : $$"""{"defaults":{},"overrides":{},"idempotency-key-lifetime":"{{lifetime}}"}"""));
            Keys.Add(request.Headers.GetValues("Idempotency-Key").Single());
            var respond = mutationResponses[Math.Min(_calls++, mutationResponses.Length - 1)];
            return Task.FromResult(respond());
        }
    }

    private static HttpResponseMessage Json(string body, HttpStatusCode status = HttpStatusCode.OK) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static HttpResponseMessage Unavailable() => Json("""{"error":{"message":"try again","type":"ServiceUnavailableException","code":503}}""", HttpStatusCode.ServiceUnavailable);
    private static HttpResponseMessage Ok() => Json("""{"namespace":["n"],"properties":{}}""");

    private static Task<RestCatalog> Connect(FakeServer server) => RestCatalog.ConnectAsync(new RestCatalogOptions
    {
        Uri = new Uri("http://catalog.test"), Handler = server, AccessDelegation = null, IdempotentRetryDelay = TimeSpan.FromMilliseconds(1),
    });

    [Fact]
    public async Task Retries_reuse_one_uuidv7_key_when_the_server_deduplicates()
    {
        var server = new FakeServer("PT30M", Unavailable, () => throw new HttpRequestException("connection reset"), Ok);
        using var catalog = await Connect(server);
        Assert.Equal(TimeSpan.FromMinutes(30), catalog.IdempotencyKeyLifetime);

        await catalog.CreateNamespaceAsync(Namespace.Of("n"));
        Assert.Equal(3, server.Keys.Count);
        Assert.Single(server.Keys.Distinct());
        var key = Guid.Parse(server.Keys[0]).ToString();
        Assert.Equal('7', key[14]); // version nibble
        Assert.Contains(key[19], "89ab"); // RFC 9562 variant
    }

    [Fact]
    public async Task Each_mutation_gets_its_own_time_ordered_key()
    {
        var server = new FakeServer("PT1M", Ok);
        using var catalog = await Connect(server);
        await catalog.CreateNamespaceAsync(Namespace.Of("a"));
        await Task.Delay(2);
        await catalog.CreateNamespaceAsync(Namespace.Of("b"));
        Assert.Equal(2, server.Keys.Distinct().Count());
        Assert.True(string.CompareOrdinal(server.Keys[0], server.Keys[1]) < 0);
    }

    [Fact]
    public async Task Without_advertised_support_a_failed_commit_is_not_replayed()
    {
        var server = new FakeServer(null, Unavailable, Ok);
        using var catalog = await Connect(server);
        var commit = new TableCommit(TableIdentifier.Of("n", "t"), [], []);
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => catalog.CommitTableAsync(commit));
        Assert.Single(server.Keys); // one attempt: a blind retry could apply the commit twice
    }

    [Fact]
    public async Task Retries_stop_after_the_configured_count()
    {
        var server = new FakeServer("PT30M", Unavailable);
        using var catalog = await Connect(server);
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => catalog.CommitTableAsync(new TableCommit(TableIdentifier.Of("n", "t"), [], [])));
        Assert.Equal(4, server.Keys.Count); // first try + 3 retries
    }

    [Fact]
    public async Task Each_resend_is_an_event_on_the_catalog_span()
    {
        var table = TableIdentifier.Of("n", "resend_" + Guid.NewGuid().ToString("N"));
        var stopped = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == IcebergTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a => { if (Equals(a.GetTagItem(IcebergTelemetry.Tags.Table), table.ToString())) lock (stopped) stopped.Add(a); },
        };
        ActivitySource.AddActivityListener(listener);
        var server = new FakeServer("PT30M", Unavailable, () => throw new HttpRequestException("connection reset"), Unavailable);
        using var catalog = await Connect(server);

        await Assert.ThrowsAsync<CommitStateUnknownException>(() => catalog.CommitTableAsync(new TableCommit(table, [], [])));

        // One span for the call; each resend is an event on it, numbered, with what prompted it.
        var span = Assert.Single(stopped);
        Assert.Equal("iceberg.catalog.commit_table", span.OperationName);
        Assert.Equal(IcebergTelemetry.Outcomes.Unknown, span.GetTagItem(IcebergTelemetry.Tags.Outcome));
        var resends = span.Events.Where(e => e.Name == "iceberg.catalog.resend").ToList();
        Assert.Equal([1, 2, 3], resends.Select(e => e.Tags.Single(kv => kv.Key == "iceberg.catalog.resend").Value));
        Assert.Contains(resends[0].Tags, kv => kv.Key == "http.response.status_code" && Equals(kv.Value, 503));
        Assert.Contains(resends[1].Tags, kv => kv.Key == IcebergTelemetry.Tags.ErrorType && Equals(kv.Value, typeof(HttpRequestException).FullName));
    }
}
