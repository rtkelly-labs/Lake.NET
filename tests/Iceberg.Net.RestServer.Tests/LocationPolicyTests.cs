using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using static Iceberg.Net.RestServer.Tests.ProtocolTests;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Locations a client chooses (docs/rest-server.md, Storage locations): the server acts on them with its own access, so
/// they must stay under an allowed root, inside their namespace, and clear of every other table, view and namespace.
/// Every refusal is a 400 and leaves nothing behind.
/// </summary>
public sealed class LocationPolicyTests : IAsyncLifetime
{
    private ServerHost _host = null!;
    private HttpClient _http = null!;
    private Catalog.RestCatalog _catalog = null!;

    public async Task InitializeAsync()
    {
        _host = ServerHost.Start();
        _http = _host.Http();
        _catalog = await _host.ConnectAsync();
        await _catalog.CreateNamespaceAsync(Namespace.Of("sales"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("hr"));
    }

    public async Task DisposeAsync()
    {
        _catalog.Dispose();
        _http.Dispose();
        await _host.DisposeAsync();
    }

    private string Wh(params string[] parts) => _host.Warehouse + "/" + string.Join('/', parts);

    private static JsonObject CreateBody(string name, string? location = null, Dictionary<string, string>? properties = null)
    {
        var body = new JsonObject { ["name"] = name, ["schema"] = TypeJson.SchemaToJson(ScanPlanningTests.Schema) };
        if (location is not null) body["location"] = location;
        if (properties is not null) body["properties"] = new JsonObject(properties.Select(kv => KeyValuePair.Create(kv.Key, (JsonNode?)kv.Value)));
        return body;
    }

    private async Task AssertRefusedAsync(string path, JsonNode body)
    {
        var r = await SendAsync(_http, HttpMethod.Post, path, body);
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", r);
    }

    [Theory]
    [InlineData("s3://another-bucket/wh/sales/t")]
    [InlineData("s3://*")]
    [InlineData("/etc")]
    [InlineData("file:///")]
    public async Task A_table_cannot_be_created_outside_the_allowed_storage(string location)
    {
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", location));
        Assert.False(await _catalog.TableExistsAsync(TableIdentifier.Of("sales", "t")));
    }

    [Fact]
    public async Task A_table_cannot_be_created_in_another_namespace_or_another_tables_directory()
    {
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("hr", "salaries"), ScanPlanningTests.Schema));
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("hr", "salaries")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("hr", "elsewhere")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "orders")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "orders", "inner")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "x", "..", "orders")));
        await AssertRefusedAsync("/v1/namespaces/sales/tables",
            CreateBody("t", properties: new() { ["write.data.path"] = Wh("hr", "salaries", "data") }));
        // Inside its own namespace and clear of others, a chosen location is fine.
        Assert.Equal(200, (int)(await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/tables", CreateBody("t", Wh("sales", "custom-t")))).Status);
    }

    [Fact]
    public async Task A_commit_cannot_move_a_table_or_its_data_onto_another_table()
    {
        var victim = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        var mine = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "mine"), ScanPlanningTests.Schema));
        UpdateRequirement[] uuid = [new UpdateRequirement.AssertTableUuid(mine.Metadata.TableUuid)];
        foreach (MetadataUpdate update in new MetadataUpdate[]
                 {
                     new MetadataUpdate.SetLocation(victim.Location),
                     new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["write.data.path"] = $"{victim.Location}/data" }),
                     new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["write.metadata.path"] = "s3://another-bucket/meta" }),
                 })
        {
            var e = await Assert.ThrowsAsync<HttpRequestException>(() => _catalog.CommitTableAsync(new TableCommit(mine.Identifier, uuid, [update])));
            Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        }
        var reloaded = await _catalog.LoadTableAsync(mine.Identifier);
        Assert.Equal(mine.Location, reloaded.Location);
        Assert.False(reloaded.Metadata.Properties.ContainsKey("write.data.path"));
    }

    [Fact]
    public async Task A_new_table_cannot_take_over_a_renamed_tables_directory()
    {
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        await _catalog.RenameTableAsync(TableIdentifier.Of("sales", "orders"), TableIdentifier.Of("sales", "orders_2024"));
        // The renamed table keeps its directory, sales/orders, which is where a new "orders" would go by default.
        var e = await Assert.ThrowsAsync<HttpRequestException>(() =>
            _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
    }

    [Fact]
    public async Task A_table_renamed_into_another_namespace_keeps_its_directory()
    {
        await _catalog.CreateNamespaceAsync(Namespace.Of("archive"));
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        await _catalog.RenameTableAsync(TableIdentifier.Of("sales", "orders"), TableIdentifier.Of("archive", "orders"));
        // archive.orders still writes to sales/orders: neither a new sales.orders nor a namespace there may claim it.
        var e = await Assert.ThrowsAsync<HttpRequestException>(() =>
            _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema)));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        await AssertRefusedAsync("/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("sales", "orders") });
    }

    [Fact]
    public async Task A_second_spelling_of_a_directory_is_refused()
    {
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        // A local file system reads sales//orders as sales/orders.
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", _host.Warehouse + "/sales//orders"));
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody("t", _host.Warehouse + "/sales//t"));
        Assert.False(await _catalog.TableExistsAsync(TableIdentifier.Of("sales", "t")));
    }

    [Fact]
    public async Task A_transaction_cannot_move_two_tables_into_one_directory()
    {
        var a = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "a"), ScanPlanningTests.Schema));
        var b = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "b"), ScanPlanningTests.Schema));
        var shared = Wh("sales", "shared");
        var e = await Assert.ThrowsAsync<HttpRequestException>(() => _catalog.CommitTransactionAsync(
        [
            new TableCommit(a.Identifier, [new UpdateRequirement.AssertTableUuid(a.Metadata.TableUuid)], [new MetadataUpdate.SetLocation(shared)]),
            new TableCommit(b.Identifier, [new UpdateRequirement.AssertTableUuid(b.Metadata.TableUuid)], [new MetadataUpdate.SetLocation(shared + "/inner")]),
        ]));
        Assert.Equal(HttpStatusCode.BadRequest, e.StatusCode);
        Assert.Equal(a.Location, (await _catalog.LoadTableAsync(a.Identifier)).Location);
        Assert.Equal(b.Location, (await _catalog.LoadTableAsync(b.Identifier)).Location);
    }

    [Fact]
    public async Task An_implied_child_namespace_does_not_block_creates_in_its_parent()
    {
        // The memory store lists a.b as a child of a once a.b.c exists, though a.b was never created.
        await _catalog.CreateNamespaceAsync(Namespace.Of("a"));
        await _catalog.CreateNamespaceAsync(Namespace.Of("a", "b", "c"));
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("a", "t"), ScanPlanningTests.Schema));
        Assert.True(await _catalog.TableExistsAsync(TableIdentifier.Of("a", "t")));
    }

    [Fact]
    public async Task Register_checks_the_metadata_location_before_reading_it()
    {
        var existing = Path.Combine(_host.Root, "catalog.db"); // exists, outside the namespace
        var missing = Path.Combine(_host.Root, "absent.metadata.json");
        var first = await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/register", new JsonObject { ["name"] = "x", ["metadata-location"] = existing });
        var second = await SendAsync(_http, HttpMethod.Post, "/v1/namespaces/sales/register", new JsonObject { ["name"] = "x", ["metadata-location"] = missing });
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", first);
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", second);
        // Same answer whether or not the file exists: the location is refused before anything is read.
        Assert.Contains("outside", first.Body!["error"]!["message"]!.GetValue<string>());
        Assert.Contains("outside", second.Body!["error"]!["message"]!.GetValue<string>());
    }

    [Fact]
    public async Task Namespace_locations_stay_in_allowed_storage_and_clear_of_other_namespaces()
    {
        await AssertRefusedAsync("/v1/namespaces", new JsonObject
            { ["namespace"] = new JsonArray("ops"), ["properties"] = new JsonObject { ["location"] = "s3://another-bucket/ops" } });
        await AssertRefusedAsync("/v1/namespaces", new JsonObject
            { ["namespace"] = new JsonArray("ops"), ["properties"] = new JsonObject { ["location"] = Wh("hr", "ops") } });
        Assert.False(await _catalog.NamespaceExistsAsync(Namespace.Of("ops")));

        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "orders"), ScanPlanningTests.Schema));
        // A namespace that has tables cannot move, and a child cannot be rooted in a parent's table.
        await AssertRefusedAsync("/v1/namespaces/sales/properties", new JsonObject { ["updates"] = new JsonObject { ["location"] = Wh("sales2") } });
        await AssertRefusedAsync("/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("sales", "orders") });
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("a/b")]
    [InlineData("..")]
    public async Task Names_that_would_reshape_a_path_are_refused(string name) =>
        await AssertRefusedAsync("/v1/namespaces/sales/tables", CreateBody(name));
}
