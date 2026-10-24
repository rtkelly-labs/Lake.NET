using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Reqnroll;
using Xunit;

namespace Iceberg.Net.RestCatalogBehaviour.Tests.Steps;

/// <summary>Raw requests per the REST OpenAPI document, for what only the wire shows.</summary>
[Binding]
public sealed class ProtocolSteps(CatalogWorld world)
{
    private readonly List<string> _etags = [];

    [When(@"^I fetch the catalog configuration$")]
    public async Task Config() => Assert.Equal(HttpStatusCode.OK, await world.SendAsync(HttpMethod.Get, "/v1/config"));

    [Then(@"^the configuration advertises the endpoints:$")]
    public void Advertises(DataTable endpoints)
    {
        var advertised = world.Json!["endpoints"]!.AsArray().Select(e => e!.GetValue<string>()).ToHashSet(StringComparer.Ordinal);
        foreach (var row in endpoints.Rows) Assert.Contains(row["endpoint"], advertised);
    }

    [Then(@"^the configuration has ""defaults"" and ""overrides"" objects$")]
    public void DefaultsAndOverrides()
    {
        Assert.IsType<JsonObject>(world.Json!["defaults"]);
        Assert.IsType<JsonObject>(world.Json!["overrides"]);
    }

    [When(@"^I load ""([^""]*)"" over HTTP$")]
    public async Task Load(string name)
    {
        Assert.Equal(HttpStatusCode.OK, await world.SendAsync(HttpMethod.Get, CatalogWorld.TablePath(name)));
        if (world.Headers?.ETag is { } etag) _etags.Add(etag.ToString());
    }

    [When(@"^I load ""([^""]*)"" over HTTP again, sending that ETag as If-None-Match$")]
    public async Task Revalidate(string name)
    {
        var etag = _etags[^1];
        await world.SendAsync(HttpMethod.Get, CatalogWorld.TablePath(name), configure: r => r.Headers.IfNoneMatch.Add(EntityTagHeaderValue.Parse(etag)));
        if (world.Status == HttpStatusCode.OK && world.Headers?.ETag is { } fresh) _etags.Add(fresh.ToString());
    }

    [Then(@"^the response has an ETag$")]
    public void HasETag() => Assert.Single(_etags);

    [Then(@"^the response is 304 with no body$")]
    public void NotModified()
    {
        Assert.Equal(HttpStatusCode.NotModified, world.Status);
        Assert.Equal("", world.Body);
    }

    [Then(@"^the response is 200 with the table's metadata$")]
    public void Modified()
    {
        Assert.Equal(HttpStatusCode.OK, world.Status);
        Assert.NotNull(world.Json!["metadata"]?["current-snapshot-id"]);
    }

    [Then(@"^the response has an ETag different from the first$")]
    public void DifferentETag()
    {
        Assert.Equal(2, _etags.Count);
        Assert.NotEqual(_etags[0], _etags[1]);
    }

    // ---------- requests that the server must refuse ----------

    [When(@"^I send a request that (.*)$")]
    public Task Mistake(string mistake) => mistake switch
    {
        "loads the missing table \"sales.invoices\"" => world.SendAsync(HttpMethod.Get, CatalogWorld.TablePath("sales.invoices")),
        "loads a table in the missing namespace \"marketing\"" => world.SendAsync(HttpMethod.Get, CatalogWorld.TablePath("marketing.leads")),
        "loads the missing view \"sales.stale\"" => world.SendAsync(HttpMethod.Get, CatalogWorld.ViewPath("sales.stale")),
        "creates the existing namespace \"sales\"" => world.SendAsync(HttpMethod.Post, "/v1/namespaces",
            new JsonObject { ["namespace"] = new JsonArray("sales"), ["properties"] = new JsonObject() }),
        "creates the existing table \"sales.orders\"" => world.SendAsync(HttpMethod.Post, $"{CatalogWorld.NsPath("sales")}/tables",
            new JsonObject { ["name"] = "orders", ["schema"] = TypeJson.SchemaToJson(CatalogWorld.Schema) }),
        "drops the non-empty namespace \"sales\"" => world.SendAsync(HttpMethod.Delete, CatalogWorld.NsPath("sales")),
        "commits to \"sales.orders\" requiring a snapshot it never had" => world.SendAsync(HttpMethod.Post, CatalogWorld.TablePath("sales.orders"),
            new JsonObject
            {
                ["requirements"] = new JsonArray(new JsonObject { ["type"] = "assert-ref-snapshot-id", ["ref"] = "main", ["snapshot-id"] = 123456789L }),
                ["updates"] = new JsonArray(new JsonObject { ["action"] = "set-properties", ["updates"] = new JsonObject { ["k"] = "v" } }),
            }),
        "creates a namespace with malformed JSON" => world.SendAsync(HttpMethod.Post, "/v1/namespaces",
            configure: r => r.Content = new StringContent("{\"namespace\": [\"x\"", Encoding.UTF8, "application/json")),
        "creates a table without a schema" => world.SendAsync(HttpMethod.Post, $"{CatalogWorld.NsPath("sales")}/tables", new JsonObject { ["name"] = "noschema" }),
        "sets and removes the property \"owner\" of \"sales\"" => world.SendAsync(HttpMethod.Post, $"{CatalogWorld.NsPath("sales")}/properties",
            new JsonObject { ["updates"] = new JsonObject { ["owner"] = "x" }, ["removals"] = new JsonArray("owner") }),
        _ => throw new ArgumentException($"Unknown mistake '{mistake}'"),
    };
}
