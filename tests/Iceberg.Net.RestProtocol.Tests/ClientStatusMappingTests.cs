using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>
/// How the client turns each documented status into an exception, against a scripted server: including the ones this
/// repository's server never sends (419, 5xx on a commit), which decide whether a caller may retry.
/// </summary>
public sealed class ClientStatusMappingTests
{
    /// <summary>Answers /v1/config, then every other request with the next scripted response (or a transport failure).</summary>
    private sealed class ScriptedServer(string? idempotencyLifetime = null) : HttpMessageHandler
    {
        public Queue<Func<HttpResponseMessage>> Responses { get; } = new();
        public List<HttpRequestMessage> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.RequestUri!.AbsolutePath.EndsWith("/v1/config", StringComparison.Ordinal))
            {
                var config = new JsonObject { ["defaults"] = new JsonObject(), ["overrides"] = new JsonObject() };
                if (idempotencyLifetime is not null) config["idempotency-key-lifetime"] = idempotencyLifetime;
                return Task.FromResult(Json(HttpStatusCode.OK, config));
            }
            Requests.Add(request);
            return Task.FromResult(Responses.Dequeue()());
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode status, JsonNode body) =>
        new(status) { Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json") };

    private static Func<HttpResponseMessage> Error(int status, string type) => () => Json((HttpStatusCode)status, new JsonObject
    {
        ["error"] = new JsonObject { ["message"] = $"scripted {status}", ["type"] = type, ["code"] = status },
    });

    private static Task<Catalog.RestCatalog> Connect(ScriptedServer server) => Catalog.RestCatalog.ConnectAsync(new RestCatalogOptions
    {
        Uri = new Uri("https://catalog.example/"), Handler = server, AccessDelegation = null, IdempotentRetryDelay = TimeSpan.FromMilliseconds(1),
    });

    private static readonly TableIdentifier Orders = TableIdentifier.Of("sales", "orders");
    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long)]);
    private static TableCommit Commit => new(Orders, [new UpdateRequirement.AssertTableUuid(Guid.NewGuid())],
        [new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["k"] = "v" })]);

    [Spec("rest: loadTable")]
    [Spec("rest: loadNamespaceMetadata")]
    [Fact]
    public async Task A_404_maps_to_the_missing_kind_of_object()
    {
        var server = new ScriptedServer();
        using var catalog = await Connect(server);
        server.Responses.Enqueue(Error(404, "NoSuchTableException"));
        await Assert.ThrowsAsync<NoSuchTableException>(() => catalog.LoadTableAsync(Orders));
        server.Responses.Enqueue(Error(404, "NoSuchNamespaceException"));
        await Assert.ThrowsAsync<NoSuchNamespaceException>(() => catalog.ListTablesAsync(Namespace.Of("sales")));
    }

    [Spec("rest: createTable")]
    [Spec("rest: updateTable")]
    [Fact]
    public async Task A_409_is_already_exists_on_create_and_commit_failed_on_commit()
    {
        var server = new ScriptedServer();
        using var catalog = await Connect(server);
        server.Responses.Enqueue(Error(409, "AlreadyExistsException"));
        await Assert.ThrowsAsync<AlreadyExistsException>(() => catalog.CreateTableAsync(new CreateTableRequest(Orders, Schema)));
        server.Responses.Enqueue(Error(409, "CommitFailedException"));
        await Assert.ThrowsAsync<CommitFailedException>(() => catalog.CommitTableAsync(Commit));
    }

    [Spec("rest: updateTable")]
    [Spec("rest: commitTransaction")]
    [Theory]
    [InlineData(500)]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public async Task A_5xx_on_a_commit_means_its_outcome_is_unknown(int status)
    {
        var server = new ScriptedServer();
        using var catalog = await Connect(server);
        server.Responses.Enqueue(Error(status, "ServiceFailureException"));
        var e = await Assert.ThrowsAsync<CommitStateUnknownException>(() => catalog.CommitTableAsync(Commit));
        Assert.True(e.ServerResponded);
        server.Responses.Enqueue(Error(status, "ServiceFailureException"));
        await Assert.ThrowsAsync<CommitStateUnknownException>(() => catalog.CommitTransactionAsync([Commit]));
    }

    [Spec("rest: updateTable")]
    [Fact]
    public async Task A_lost_commit_response_is_unknown_not_failed()
    {
        var server = new ScriptedServer();
        using var catalog = await Connect(server);
        server.Responses.Enqueue(() => throw new HttpRequestException("connection reset"));
        var e = await Assert.ThrowsAsync<CommitStateUnknownException>(() => catalog.CommitTableAsync(Commit));
        Assert.False(e.ServerResponded);
    }

    [Spec("rest: updateTable")]
    [Fact]
    public async Task With_an_idempotency_key_lifetime_a_503_commit_is_retried_with_the_same_key()
    {
        var server = new ScriptedServer(idempotencyLifetime: "PT30M");
        using var catalog = await Connect(server);
        server.Responses.Enqueue(Error(503, "ServiceUnavailableException"));
        server.Responses.Enqueue(() => Json(HttpStatusCode.Conflict, new JsonObject
        {
            ["error"] = new JsonObject { ["message"] = "replayed", ["type"] = "CommitFailedException", ["code"] = 409 },
        }));
        await Assert.ThrowsAsync<CommitFailedException>(() => catalog.CommitTableAsync(Commit));
        Assert.Equal(2, server.Requests.Count);
        var keys = server.Requests.Select(r => r.Headers.GetValues("Idempotency-Key").Single()).ToList();
        Assert.Equal(keys[0], keys[1]);
    }

    [Spec("rest: loadTable")]
    [Theory]
    [InlineData(401)]
    [InlineData(403)]
    public async Task A_401_or_403_is_unauthorized_access(int status)
    {
        var server = new ScriptedServer();
        using var catalog = await Connect(server);
        server.Responses.Enqueue(Error(status, status == 401 ? "NotAuthorizedException" : "ForbiddenException"));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => catalog.LoadTableAsync(Orders));
    }

    [Spec("rest: loadTable")]
    [Theory]
    [InlineData(400, "BadRequestException")]
    [InlineData(419, "AuthenticationTimeoutException")]
    [InlineData(503, "ServiceUnavailableException")]
    public async Task Other_errors_outside_a_commit_surface_with_their_status(int status, string type)
    {
        var server = new ScriptedServer();
        using var catalog = await Connect(server);
        server.Responses.Enqueue(Error(status, type));
        var e = await Assert.ThrowsAsync<HttpRequestException>(() => catalog.LoadTableAsync(Orders));
        Assert.Equal((HttpStatusCode)status, e.StatusCode);
        Assert.Contains($"scripted {status}", e.Message);
    }
}
