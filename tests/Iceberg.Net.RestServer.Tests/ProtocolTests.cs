using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>The wire protocol as the OpenAPI spec states it: status codes, the ErrorModel, headers, paging, staged creates.</summary>
public sealed class ProtocolTests
{
    private static readonly Schema Schema = CatalogContractTests.Schema;

    internal static StringContent JsonBody(JsonNode body) => new(body.ToJsonString(), Encoding.UTF8, "application/json");

    internal static async Task<(HttpStatusCode Status, JsonNode? Body)> SendAsync(HttpClient http, HttpMethod method, string path, JsonNode? body = null)
    {
        using var req = new HttpRequestMessage(method, path);
        if (body is not null) req.Content = JsonBody(body);
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        return (resp.StatusCode, text.Length == 0 ? null : JsonNode.Parse(text));
    }

    private static void AssertError(HttpStatusCode expected, string type, (HttpStatusCode Status, JsonNode? Body) actual)
    {
        Assert.Equal(expected, actual.Status);
        var error = actual.Body!["error"]!;
        Assert.Equal(type, error["type"]!.GetValue<string>());
        Assert.Equal((int)expected, error["code"]!.GetValue<int>());
        Assert.False(string.IsNullOrEmpty(error["message"]!.GetValue<string>()));
    }

    private static JsonObject CreateBody(string name, bool stage = false) => new()
    {
        ["name"] = name,
        ["schema"] = TypeJson.SchemaToJson(Schema),
        ["stage-create"] = stage,
        ["properties"] = new JsonObject { ["owner"] = "tests" },
    };

    [Fact]
    public async Task Config_advertises_every_endpoint_and_the_prefix()
    {
        await using var host = ServerHost.Start(settings: new Dictionary<string, string> { ["Prefix"] = "main", ["Defaults:clients"] = "4" });
        using var http = host.Http();
        var (status, config) = await SendAsync(http, HttpMethod.Get, "/v1/config?warehouse=anything");
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal("main", config!["overrides"]!["prefix"]!.GetValue<string>());
        Assert.Equal("4", config["defaults"]!["clients"]!.GetValue<string>());
        var endpoints = config["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()).ToList();
        Assert.Contains("POST /v1/{prefix}/transactions/commit", endpoints);
        Assert.Contains("GET /v1/{prefix}/namespaces/{namespace}/views/{view}", endpoints);

        // Routes live under the prefix, and the library's client follows it.
        Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(http, HttpMethod.Get, "/v1/namespaces")).Status);
        using var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        Assert.Equal(HttpStatusCode.OK, (await SendAsync(http, HttpMethod.Get, "/v1/main/namespaces")).Status);
        Assert.Equal(["db"], (await SendAsync(http, HttpMethod.Get, "/v1/main/namespaces")).Body!["namespaces"]![0]!.AsArray().Select(l => l!.GetValue<string>()));
    }

    [Fact]
    public async Task Errors_use_the_error_model_and_the_spec_status_codes()
    {
        await using var host = ServerHost.Start("sqlite");
        using var http = host.Http();
        AssertError(HttpStatusCode.NotFound, "NoSuchNamespaceException", await SendAsync(http, HttpMethod.Get, "/v1/namespaces/nope"));
        AssertError(HttpStatusCode.NotFound, "NoSuchNamespaceException", await SendAsync(http, HttpMethod.Get, "/v1/namespaces/nope/tables"));
        AssertError(HttpStatusCode.NotFound, "NoSuchNamespaceException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/nope/tables", CreateBody("t")));

        Assert.Equal(HttpStatusCode.OK, (await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") })).Status);
        AssertError(HttpStatusCode.Conflict, "AlreadyExistsException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") }));
        AssertError(HttpStatusCode.NotFound, "NoSuchTableException", await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/missing"));
        AssertError(HttpStatusCode.NotFound, "NoSuchViewException", await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/views/missing"));
        AssertError(HttpStatusCode.NotFound, "NoSuchTableException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/missing",
            new JsonObject { ["requirements"] = new JsonArray(), ["updates"] = new JsonArray() }));
        // A requirement against a missing table is still a 404, not a failed requirement.
        AssertError(HttpStatusCode.NotFound, "NoSuchTableException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/missing", new JsonObject
        {
            ["requirements"] = new JsonArray(new JsonObject { ["type"] = "assert-table-uuid", ["uuid"] = Guid.NewGuid().ToString() }),
            ["updates"] = new JsonArray(),
        }));
        AssertError(HttpStatusCode.NotFound, "NoSuchTableException", await SendAsync(http, HttpMethod.Delete, "/v1/namespaces/db/tables/missing"));

        Assert.Equal(HttpStatusCode.OK, (await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", CreateBody("t"))).Status);
        AssertError(HttpStatusCode.Conflict, "AlreadyExistsException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", CreateBody("t")));
        AssertError(HttpStatusCode.Conflict, "NamespaceNotEmptyException", await SendAsync(http, HttpMethod.Delete, "/v1/namespaces/db"));

        // Malformed and invalid requests are 400s, never 500s.
        using (var bad = new HttpRequestMessage(HttpMethod.Post, "/v1/namespaces") { Content = new StringContent("{not json", Encoding.UTF8, "application/json") })
        using (var resp = await http.SendAsync(bad))
            Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", new JsonObject { ["name"] = "x" }));
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/t", new JsonObject
        {
            ["requirements"] = new JsonArray(),
            ["updates"] = new JsonArray(new JsonObject { ["action"] = "set-current-schema", ["schema-id"] = 42 }),
        }));
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/t", new JsonObject
        {
            ["updates"] = new JsonArray(new JsonObject { ["action"] = "no-such-action" }),
        }));
        AssertError(HttpStatusCode.UnprocessableEntity, "UnprocessableEntityException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/properties", new JsonObject
        {
            ["updates"] = new JsonObject { ["a"] = "1" },
            ["removals"] = new JsonArray("a"),
        }));

        // A failed requirement is a 409 CommitFailedException.
        AssertError(HttpStatusCode.Conflict, "CommitFailedException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/t", new JsonObject
        {
            ["requirements"] = new JsonArray(new JsonObject { ["type"] = "assert-table-uuid", ["uuid"] = Guid.NewGuid().ToString() }),
            ["updates"] = new JsonArray(new JsonObject { ["action"] = "set-properties", ["updates"] = new JsonObject { ["k"] = "v" } }),
        }));

        // HEAD answers existence with 204 / 404 and no body.
        Assert.Equal(HttpStatusCode.NoContent, (await SendAsync(http, HttpMethod.Head, "/v1/namespaces/db")).Status);
        Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(http, HttpMethod.Head, "/v1/namespaces/nope")).Status);
        Assert.Equal(HttpStatusCode.NoContent, (await SendAsync(http, HttpMethod.Head, "/v1/namespaces/db/tables/t")).Status);
        Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(http, HttpMethod.Head, "/v1/namespaces/db/tables/missing")).Status);
    }

    [Fact]
    public async Task Nested_namespaces_use_the_unit_separator()
    {
        await using var host = ServerHost.Start();
        using var http = host.Http();
        foreach (var levels in new[] { new[] { "a" }, ["a", "b"], ["a", "b", "c"], ["a", "x"] })
            Assert.Equal(HttpStatusCode.OK, (await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray(levels.Select(l => (JsonNode)l).ToArray()) })).Status);
        var (_, children) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces?parent=a");
        Assert.Equal([["a", "b"], ["a", "x"]], children!["namespaces"]!.AsArray().Select(n => n!.AsArray().Select(l => l!.GetValue<string>()).ToArray()));
        var (_, grandchildren) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces?parent=a%1Fb");
        Assert.Equal(["a", "b", "c"], grandchildren!["namespaces"]![0]!.AsArray().Select(l => l!.GetValue<string>()));
        var (status, loaded) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/a%1Fb%1Fc");
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(3, loaded!["namespace"]!.AsArray().Count);
        AssertError(HttpStatusCode.NotFound, "NoSuchNamespaceException", await SendAsync(http, HttpMethod.Get, "/v1/namespaces?parent=zzz"));
    }

    [Fact]
    public async Task Namespace_properties_update_reports_updated_removed_and_missing()
    {
        await using var host = ServerHost.Start("sqlite");
        using var http = host.Http();
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db"), ["properties"] = new JsonObject { ["a"] = "1", ["b"] = "2" } });
        var (status, result) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/properties", new JsonObject
        {
            ["updates"] = new JsonObject { ["c"] = "3", ["a"] = "10" },
            ["removals"] = new JsonArray("b", "zz"),
        });
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(["a", "c"], result!["updated"]!.AsArray().Select(k => k!.GetValue<string>()).Order());
        Assert.Equal(["b"], result["removed"]!.AsArray().Select(k => k!.GetValue<string>()));
        Assert.Equal(["zz"], result["missing"]!.AsArray().Select(k => k!.GetValue<string>()));
        var (_, loaded) = await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db");
        Assert.Equal(new Dictionary<string, string> { ["a"] = "10", ["c"] = "3" }, MetadataJson.StringMapFrom(loaded!["properties"]));
    }

    [Fact]
    public async Task Tables_get_the_namespace_location_when_it_has_one()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        var custom = Path.Combine(host.Root, "elsewhere");
        await catalog.CreateNamespaceAsync(Namespace.Of("db"), new Dictionary<string, string> { ["location"] = custom });
        var t = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), Schema));
        Assert.Equal($"{custom}/t", t.Location);
        var explicitLocation = Path.Combine(host.Root, "explicit");
        Assert.Equal(explicitLocation, (await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "u"), Schema, Location: explicitLocation))).Location);
    }

    [Fact]
    public async Task Listing_pages_with_tokens()
    {
        await using var host = ServerHost.Start("sqlite");
        using var http = host.Http();
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") });
        foreach (var n in new[] { "e", "c", "a", "d", "b" }) await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", CreateBody(n));

        var seen = new List<string>();
        string? token = null;
        var pages = 0;
        do
        {
            var (_, page) = await SendAsync(http, HttpMethod.Get, $"/v1/namespaces/db/tables?pageSize=2{(token is null ? "" : $"&pageToken={Uri.EscapeDataString(token)}")}");
            seen.AddRange(page!["identifiers"]!.AsArray().Select(i => i!["name"]!.GetValue<string>()));
            token = page["next-page-token"]?.GetValue<string>();
            pages++;
        } while (token is not null);
        Assert.Equal(["a", "b", "c", "d", "e"], seen);
        Assert.Equal(3, pages);

        // The library's client follows the tokens too (it does not send pageSize; the server then returns one page).
        using var catalog = await host.ConnectAsync();
        Assert.Equal(5, (await catalog.ListTablesAsync(Namespace.Of("db"))).Count);
    }

    [Fact]
    public async Task Load_returns_an_etag_and_honours_if_none_match()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), Schema));
        using var http = host.Http();

        using var first = await http.GetAsync("/v1/namespaces/db/tables/t");
        var etag = first.Headers.ETag;
        Assert.NotNull(etag);
        using (var again = new HttpRequestMessage(HttpMethod.Get, "/v1/namespaces/db/tables/t"))
        {
            again.Headers.IfNoneMatch.Add(etag!);
            using var resp = await http.SendAsync(again);
            Assert.Equal(HttpStatusCode.NotModified, resp.StatusCode);
        }

        await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, CatalogContractTests.Rows(0, 5))).CommitAsync();
        using (var after = new HttpRequestMessage(HttpMethod.Get, "/v1/namespaces/db/tables/t"))
        {
            after.Headers.IfNoneMatch.Add(etag!);
            using var resp = await http.SendAsync(after);
            Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
            Assert.NotEqual(etag, resp.Headers.ETag);
        }
    }

    [Fact]
    public async Task Snapshots_refs_returns_only_referenced_snapshots()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), Schema));
        for (var i = 0; i < 3; i++)
            await SnapshotProducer.Append(table).AddFiles(await TableWriter.WriteAsync(table, CatalogContractTests.Rows(i * 5, 5))).CommitAsync();
        using var http = host.Http();
        Assert.Equal(3, (await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t")).Body!["metadata"]!["snapshots"]!.AsArray().Count);
        var refs = (await SendAsync(http, HttpMethod.Get, "/v1/namespaces/db/tables/t?snapshots=refs")).Body!["metadata"]!;
        Assert.Single(refs["snapshots"]!.AsArray());
        Assert.Equal(refs["current-snapshot-id"]!.GetValue<long>(), refs["snapshots"]![0]!["snapshot-id"]!.GetValue<long>());
    }

    [Fact]
    public async Task Staged_create_commits_with_assert_create()
    {
        await using var host = ServerHost.Start("sqlite");
        using var http = host.Http();
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") });
        var body = CreateBody("staged", stage: true);
        body["properties"]!["format-version"] = "3";
        var (status, staged) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", body);
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Null(staged!["metadata-location"]);
        Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(http, HttpMethod.Head, "/v1/namespaces/db/tables/staged")).Status);

        // The client turns the staged metadata into a create commit, as Java's and PyIceberg's create transactions do.
        var m = MetadataJson.FromJson(staged["metadata"]!);
        Assert.Equal(3, m.FormatVersion);
        JsonArray Updates() => new(
            UpdateJson.ToJson(new MetadataUpdate.AssignUuid(m.TableUuid)),
            UpdateJson.ToJson(new MetadataUpdate.UpgradeFormatVersion(m.FormatVersion)),
            UpdateJson.ToJson(new MetadataUpdate.AddSchema(m.CurrentSchema)),
            UpdateJson.ToJson(new MetadataUpdate.SetCurrentSchema(-1)),
            UpdateJson.ToJson(new MetadataUpdate.AddPartitionSpec(m.DefaultSpec)),
            UpdateJson.ToJson(new MetadataUpdate.SetDefaultPartitionSpec(-1)),
            UpdateJson.ToJson(new MetadataUpdate.AddSortOrder(m.DefaultSortOrder)),
            UpdateJson.ToJson(new MetadataUpdate.SetDefaultSortOrder(-1)),
            UpdateJson.ToJson(new MetadataUpdate.SetLocation(m.Location)),
            UpdateJson.ToJson(new MetadataUpdate.SetProperties(new Dictionary<string, string>(m.Properties) { ["staged"] = "yes" })));
        var commit = new JsonObject
        {
            ["identifier"] = new JsonObject { ["namespace"] = new JsonArray("db"), ["name"] = "staged" },
            ["requirements"] = new JsonArray(new JsonObject { ["type"] = "assert-create" }),
            ["updates"] = Updates(),
        };
        var (committedStatus, committed) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/staged", commit);
        Assert.Equal(HttpStatusCode.OK, committedStatus);
        Assert.NotNull(committed!["metadata-location"]);

        using var catalog = await host.ConnectAsync();
        var loaded = await catalog.LoadTableAsync(TableIdentifier.Of("db", "staged"));
        Assert.Equal(m.TableUuid, loaded.Metadata.TableUuid);
        Assert.Equal(3, loaded.Metadata.FormatVersion);
        Assert.Equal("yes", loaded.Metadata.Properties["staged"]);
        Assert.Equal(m.CurrentSchema.Columns.Select(c => (c.Id, c.Name)), loaded.Schema.Columns.Select(c => (c.Id, c.Name)));
        await SnapshotProducer.Append(loaded).AddFiles(await TableWriter.WriteAsync(loaded, CatalogContractTests.Rows(0, 3))).CommitAsync();
        Assert.Equal(3, (await TableScan.ReadAllAsync(await catalog.LoadTableAsync(loaded.Identifier))).Count);

        // A second create commit loses: the table exists now.
        commit["updates"] = Updates();
        AssertError(HttpStatusCode.Conflict, "CommitFailedException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/staged", commit));
    }

    [Fact]
    public async Task Staged_creates_can_commit_in_a_transaction()
    {
        await using var host = ServerHost.Start("sqlite");
        using var http = host.Http();
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") });
        await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", CreateBody("existing"));
        var (_, staged) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", CreateBody("fresh", stage: true));
        var m = MetadataJson.FromJson(staged!["metadata"]!);
        var body = new JsonObject
        {
            ["table-changes"] = new JsonArray(
                new JsonObject
                {
                    ["identifier"] = new JsonObject { ["namespace"] = new JsonArray("db"), ["name"] = "fresh" },
                    ["requirements"] = new JsonArray(new JsonObject { ["type"] = "assert-create" }),
                    ["updates"] = new JsonArray(UpdateJson.ToJson(new MetadataUpdate.AssignUuid(m.TableUuid)), UpdateJson.ToJson(new MetadataUpdate.AddSchema(m.CurrentSchema)),
                        UpdateJson.ToJson(new MetadataUpdate.SetCurrentSchema(-1)), UpdateJson.ToJson(new MetadataUpdate.SetLocation(m.Location))),
                },
                new JsonObject
                {
                    ["identifier"] = new JsonObject { ["namespace"] = new JsonArray("db"), ["name"] = "existing" },
                    ["requirements"] = new JsonArray(),
                    ["updates"] = new JsonArray(UpdateJson.ToJson(new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["k"] = "v" }))),
                }),
        };
        Assert.Equal(HttpStatusCode.NoContent, (await SendAsync(http, HttpMethod.Post, "/v1/transactions/commit", body)).Status);
        using var catalog = await host.ConnectAsync();
        Assert.Equal(m.TableUuid, (await catalog.LoadTableAsync(TableIdentifier.Of("db", "fresh"))).Metadata.TableUuid);
        Assert.Equal("v", (await catalog.LoadTableAsync(TableIdentifier.Of("db", "existing"))).Metadata.Properties["k"]);
        // Replaying it fails as a whole: "fresh" exists now, so "existing" does not change again either.
        body["table-changes"]![1]!["updates"] = new JsonArray(UpdateJson.ToJson(new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["k"] = "w" })));
        AssertError(HttpStatusCode.Conflict, "CommitFailedException", await SendAsync(http, HttpMethod.Post, "/v1/transactions/commit", body));
        Assert.Equal("v", (await catalog.LoadTableAsync(TableIdentifier.Of("db", "existing"))).Metadata.Properties["k"]);
    }

    [Fact]
    public async Task Register_with_overwrite_and_unregister()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        var a = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "a"), Schema));
        var b = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "b"), Schema));
        using var http = host.Http();
        AssertError(HttpStatusCode.Conflict, "AlreadyExistsException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/register",
            new JsonObject { ["name"] = "b", ["metadata-location"] = a.MetadataLocation }));
        var (status, _) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/register",
            new JsonObject { ["name"] = "b", ["metadata-location"] = a.MetadataLocation, ["overwrite"] = true });
        Assert.Equal(HttpStatusCode.OK, status);
        Assert.Equal(a.Metadata.TableUuid, (await catalog.LoadTableAsync(b.Identifier)).Metadata.TableUuid);
        AssertError(HttpStatusCode.BadRequest, "BadRequestException", await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/register",
            new JsonObject { ["name"] = "c", ["metadata-location"] = Path.Combine(host.Root, "missing.metadata.json") }));

        var (unregStatus, unregistered) = await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/a/unregister");
        Assert.Equal(HttpStatusCode.OK, unregStatus);
        Assert.Equal(a.MetadataLocation, unregistered!["metadata-location"]!.GetValue<string>());
        Assert.False(await catalog.TableExistsAsync(a.Identifier));
        Assert.True(File.Exists(a.MetadataLocation!));
    }

    [Fact]
    public async Task Metrics_reports_are_accepted()
    {
        await using var host = ServerHost.Start();
        using var catalog = await host.ConnectAsync();
        await catalog.CreateNamespaceAsync(Namespace.Of("db"));
        await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "t"), Schema));
        using var http = host.Http();
        Assert.Equal(HttpStatusCode.NoContent, (await SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables/t/metrics",
            new JsonObject { ["report-type"] = "scan-report", ["table-name"] = "db.t" })).Status);
    }
}
