using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>Racing commits: exactly one wins, the loser gets 409 CommitFailedException, and nothing it wrote is left referenced.</summary>
public sealed class ConcurrencyTests
{
    private static readonly Schema Schema = CatalogContractTests.Schema;

    /// <summary>A schema change asserting the current schema: of two racing ones, only the first can apply.</summary>
    private static JsonObject SchemaChange(int currentSchemaId, string column) => new()
    {
        ["requirements"] = new JsonArray(new JsonObject { ["type"] = "assert-current-schema-id", ["current-schema-id"] = currentSchemaId }),
        ["updates"] = new JsonArray(
            UpdateJson.ToJson(new MetadataUpdate.AddSchema(new Schema(currentSchemaId + 1, [.. Schema.Columns, NestedField.Optional(7, column, PrimitiveType.String)]))),
            UpdateJson.ToJson(new MetadataUpdate.SetCurrentSchema(-1))),
    };

    private static async Task<(HttpStatusCode, string?)> PostAsync(HttpClient http, string path, JsonNode body)
    {
        using var resp = await http.PostAsync(path, ProtocolTests.JsonBody(body));
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, resp.IsSuccessStatusCode ? null : JsonNode.Parse(text)!["error"]!["type"]!.GetValue<string>());
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    public async Task Two_racing_commits_one_wins_one_gets_409(string store)
    {
        await using var host = ServerHost.Start(store);
        using var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        using var http = host.Http();
        for (var round = 0; round < 10; round++)
        {
            var name = $"t{round}";
            await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", name), Schema));
            using var go = new SemaphoreSlim(0);
            var racers = new[] { "x", "y" }.Select(async col =>
            {
                await go.WaitAsync();
                return await PostAsync(http, $"/v1/namespaces/db/tables/{name}", SchemaChange(0, col));
            }).ToList();
            go.Release(2);
            var results = await Task.WhenAll(racers);
            Assert.Single(results, r => r.Item1 == HttpStatusCode.OK);
            Assert.Single(results, r => r.Item1 == HttpStatusCode.Conflict && r.Item2 == "CommitFailedException");
            Assert.Equal(1, (await catalog.LoadTableAsync(TableIdentifier.Of("db", name))).Metadata.CurrentSchemaId);
        }
    }

    /// <summary>
    /// Both commits pass their requirements against the same base and both write a metadata file; the store's
    /// compare-and-swap then decides. The loser's file is deleted (it was never referenced).
    /// </summary>
    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    public async Task A_lost_compare_and_swap_is_a_409_and_its_file_is_removed(string store)
    {
        var root = Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests", Guid.NewGuid().ToString("N"));
        ICatalogStore inner = store == "memory" ? new InMemoryCatalogStore() : SqlCatalogStore.Sqlite(Path.Combine(root, "catalog.db"));
        var gate = new GatedStore(inner);
        await using var host = ServerHost.Start(store, sharedStore: gate, root: root);
        try
        {
            using var catalog = await host.ConnectAsync();
            await catalog.CreateNamespaceAsync(Namespace.Of("db"));
            var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), Schema));
            using var http = host.Http();

            gate.Arm(2); // hold swaps until both commits have reached the store
            var results = await Task.WhenAll(
                PostAsync(http, "/v1/namespaces/db/tables/t", SchemaChange(0, "x")),
                PostAsync(http, "/v1/namespaces/db/tables/t", SchemaChange(0, "y")));
            Assert.Single(results, r => r.Item1 == HttpStatusCode.OK);
            Assert.Single(results, r => r.Item1 == HttpStatusCode.Conflict && r.Item2 == "CommitFailedException");
            Assert.Equal(2, gate.Reached); // both passed their requirements: the swap, not a requirement, decided

            var loaded = await catalog.LoadTableAsync(table.Identifier);
            var files = Directory.GetFiles(Path.Combine(table.Location, "metadata"), "*.metadata.json");
            Assert.Equal(2, files.Length); // the create and the winner; the loser's file is gone
            Assert.Contains(loaded.MetadataLocation, files);
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Two server processes over one SQLite catalog (how the server scales out): the database arbitrates.</summary>
    [Fact]
    public async Task Two_servers_over_one_database_serialise_commits()
    {
        var root = Path.Combine(Path.GetTempPath(), "iceberg-restserver-tests", Guid.NewGuid().ToString("N"));
        try
        {
            await using var one = ServerHost.Start("sqlite", root: root);
            await using var two = ServerHost.Start("sqlite", root: root);
            using var a = await one.ConnectAsync();
            using var b = await two.ConnectAsync();
            await a.CreateNamespaceAsync(Namespace.Of("db"));
            var id = TableIdentifier.Of("db", "t");
            await a.CreateTableAsync(new CreateTableRequest(id, Schema));
            Assert.True(await b.TableExistsAsync(id));

            using var httpOne = one.Http();
            using var httpTwo = two.Http();
            var results = await Task.WhenAll(
                PostAsync(httpOne, "/v1/namespaces/db/tables/t", SchemaChange(0, "x")),
                PostAsync(httpTwo, "/v1/namespaces/db/tables/t", SchemaChange(0, "y")));
            Assert.Single(results, r => r.Item1 == HttpStatusCode.OK);
            Assert.Single(results, r => r.Item1 == HttpStatusCode.Conflict);
            Assert.Equal((await a.LoadTableAsync(id)).MetadataLocation, (await b.LoadTableAsync(id)).MetadataLocation);
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            if (Directory.Exists(root)) Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Delegates to a store, but can hold <see cref="SwapAsync"/> until N callers are waiting, to force a CAS race.</summary>
    private sealed class GatedStore(ICatalogStore inner) : ICatalogStore
    {
        private TaskCompletionSource? _open;
        private int _waiting;
        private int _needed;
        public int Reached => _waiting;

        public void Arm(int callers)
        {
            _needed = callers;
            _waiting = 0;
            _open = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public async Task SwapAsync(IReadOnlyList<PointerSwap> swaps, CatalogEntryKind kind, CancellationToken ct = default)
        {
            if (_open is { } open)
            {
                if (Interlocked.Increment(ref _waiting) >= _needed) { _open = null; open.SetResult(); }
                await open.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);
            }
            await inner.SwapAsync(swaps, kind, ct);
        }

        public Task InitializeAsync(CancellationToken ct = default) => inner.InitializeAsync(ct);
        public Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string> properties, CancellationToken ct = default) => inner.CreateNamespaceAsync(ns, properties, ct);
        public Task<IReadOnlyDictionary<string, string>?> LoadNamespaceAsync(Namespace ns, CancellationToken ct = default) => inner.LoadNamespaceAsync(ns, ct);
        public Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent, CancellationToken ct = default) => inner.ListNamespacesAsync(parent, ct);
        public Task UpdateNamespacePropertiesAsync(Namespace ns, IReadOnlyDictionary<string, string> updates, IReadOnlyCollection<string> removals, CancellationToken ct = default) =>
            inner.UpdateNamespacePropertiesAsync(ns, updates, removals, ct);
        public Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default) => inner.DropNamespaceAsync(ns, ct);
        public Task<IReadOnlyList<string>> ListAsync(Namespace ns, CatalogEntryKind kind, CancellationToken ct = default) => inner.ListAsync(ns, kind, ct);
        public Task<string?> GetAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default) => inner.GetAsync(id, kind, ct);
        public Task<CatalogEntryKind?> KindAsync(TableIdentifier id, CancellationToken ct = default) => inner.KindAsync(id, ct);
        public Task<bool> DeleteAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default) => inner.DeleteAsync(id, kind, ct);
        public Task RenameAsync(TableIdentifier from, TableIdentifier to, CatalogEntryKind kind, CancellationToken ct = default) => inner.RenameAsync(from, to, kind, ct);
    }
}
