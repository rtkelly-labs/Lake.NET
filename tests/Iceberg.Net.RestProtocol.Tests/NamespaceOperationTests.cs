using System.Text.Json.Nodes;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>The namespace operations of the REST OpenAPI: each one's success response and documented errors.</summary>
public sealed class NamespaceOperationTests : ServerTest
{
    [Spec("rest: createNamespace")]
    [Fact]
    public async Task Create_returns_the_namespace_and_its_properties()
    {
        var r = await Api.PostAsync("v1/namespaces", Rest.CreateNamespace("sales"));
        Assert.Equal(200, r.Code);
        Assert.Equal("sales", r.Body!["namespace"]![0]!.GetValue<string>());
        Assert.Equal("tests", r.Body["properties"]!["owner"]!.GetValue<string>());
    }

    [Spec("rest: createNamespace")]
    [Fact]
    public async Task Create_refuses_an_existing_namespace_with_409()
    {
        await CreateNamespaceAsync("sales");
        (await Api.PostAsync("v1/namespaces", Rest.CreateNamespace("sales"))).AssertError(409, "AlreadyExistsException");
    }

    [Spec("rest: createNamespace")]
    [Fact]
    public async Task Create_refuses_a_malformed_body_with_400()
    {
        (await Api.PostRawAsync("v1/namespaces", "{\"namespace\":")).AssertError(400);
        (await Api.PostAsync("v1/namespaces", new JsonObject { ["properties"] = new JsonObject() })).AssertError(400);
    }

    [Spec("rest: createNamespace")]
    [Spec("rest: listNamespaces")]
    [Fact]
    public async Task Nested_namespaces_are_listed_under_their_parent()
    {
        await CreateNamespaceAsync("sales");
        await CreateNamespaceAsync("sales", "eu");
        await CreateNamespaceAsync("sales", "us");
        await CreateNamespaceAsync("hr");

        var top = await Api.GetAsync("v1/namespaces");
        Assert.Equal(200, top.Code);
        Assert.Equal(["hr", "sales"], top.Body!["namespaces"]!.AsArray().Select(n => string.Join('.', n!.AsArray().Select(l => l!.GetValue<string>()))).Order());

        var children = await Api.GetAsync($"v1/namespaces?parent={Ns("sales")}");
        Assert.Equal(["sales.eu", "sales.us"], children.Body!["namespaces"]!.AsArray().Select(n => string.Join('.', n!.AsArray().Select(l => l!.GetValue<string>()))).Order());
    }

    [Spec("rest: listNamespaces")]
    [Fact]
    public async Task List_pages_with_a_page_token()
    {
        foreach (var n in new[] { "a", "b", "c", "d", "e" }) await CreateNamespaceAsync(n);
        var seen = new List<string>();
        string? token = null;
        var pages = 0;
        do
        {
            var r = await Api.GetAsync("v1/namespaces?pageSize=2" + (token is null ? "" : $"&pageToken={Uri.EscapeDataString(token)}"));
            Assert.Equal(200, r.Code);
            var page = r.Body!["namespaces"]!.AsArray();
            Assert.True(page.Count <= 2);
            seen.AddRange(page.Select(n => n![0]!.GetValue<string>()));
            token = r.Body["next-page-token"]?.GetValue<string>();
            pages++;
        } while (token is not null && pages < 10);
        Assert.Equal(["a", "b", "c", "d", "e"], seen.Order());
        Assert.True(pages >= 3);
    }

    [Spec("rest: listNamespaces")]
    [Fact]
    public async Task List_under_a_missing_parent_is_404() =>
        (await Api.GetAsync($"v1/namespaces?parent={Ns("missing")}")).AssertError(404, "NoSuchNamespaceException");

    [Spec("rest: loadNamespaceMetadata")]
    [Fact]
    public async Task Load_returns_properties_and_404s_a_missing_namespace()
    {
        await CreateNamespaceAsync("sales");
        var r = await Api.GetAsync($"v1/namespaces/{Ns("sales")}");
        Assert.Equal(200, r.Code);
        Assert.Equal("tests", r.Body!["properties"]!["owner"]!.GetValue<string>());
        (await Api.GetAsync($"v1/namespaces/{Ns("missing")}")).AssertError(404, "NoSuchNamespaceException");
    }

    [Spec("rest: namespaceExists")]
    [Fact]
    public async Task Exists_is_204_or_404_without_a_body()
    {
        await CreateNamespaceAsync("sales");
        Assert.Equal(204, (await Api.HeadAsync($"v1/namespaces/{Ns("sales")}")).Code);
        Assert.Equal(404, (await Api.HeadAsync($"v1/namespaces/{Ns("missing")}")).Code);
    }

    [Spec("rest: updateProperties")]
    [Fact]
    public async Task Update_reports_updated_removed_and_missing_keys()
    {
        await CreateNamespaceAsync("sales");
        var r = await Api.PostAsync($"v1/namespaces/{Ns("sales")}/properties", new JsonObject
        {
            ["updates"] = new JsonObject { ["team"] = "finance" },
            ["removals"] = new JsonArray("owner", "absent"),
        });
        Assert.Equal(200, r.Code);
        Assert.Equal(["team"], r.Body!["updated"]!.AsArray().Select(x => x!.GetValue<string>()));
        Assert.Equal(["owner"], r.Body["removed"]!.AsArray().Select(x => x!.GetValue<string>()));
        Assert.Equal(["absent"], r.Body["missing"]!.AsArray().Select(x => x!.GetValue<string>()));
        var props = (await Api.GetAsync($"v1/namespaces/{Ns("sales")}")).Body!["properties"]!.AsObject();
        Assert.Equal("finance", props["team"]!.GetValue<string>());
        Assert.False(props.ContainsKey("owner"));
    }

    [Spec("rest: updateProperties")]
    [Fact]
    public async Task Update_refuses_a_key_both_set_and_removed_with_422()
    {
        await CreateNamespaceAsync("sales");
        (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/properties", new JsonObject
        {
            ["updates"] = new JsonObject { ["owner"] = "x" },
            ["removals"] = new JsonArray("owner"),
        })).AssertError(422);
    }

    [Spec("rest: updateProperties")]
    [Fact]
    public async Task Update_on_a_missing_namespace_is_404() =>
        (await Api.PostAsync($"v1/namespaces/{Ns("missing")}/properties", new JsonObject { ["updates"] = new JsonObject() }))
            .AssertError(404, "NoSuchNamespaceException");

    [Spec("rest: dropNamespace")]
    [Fact]
    public async Task Drop_is_204_then_404()
    {
        await CreateNamespaceAsync("sales");
        Assert.Equal(204, (await Api.DeleteAsync($"v1/namespaces/{Ns("sales")}")).Code);
        (await Api.DeleteAsync($"v1/namespaces/{Ns("sales")}")).AssertError(404, "NoSuchNamespaceException");
    }

    [Spec("rest: dropNamespace")]
    [Fact]
    public async Task Drop_refuses_a_namespace_that_holds_a_table_with_409()
    {
        await CreateNamespaceAsync("sales");
        await CreateTableAsync("sales", "orders");
        (await Api.DeleteAsync($"v1/namespaces/{Ns("sales")}")).AssertError(409, "NamespaceNotEmptyException");
        Assert.Equal(204, (await Api.HeadAsync($"v1/namespaces/{Ns("sales")}")).Code);
    }
}
