using System.Text.Json.Nodes;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>The view operations of the REST OpenAPI: each one's success response and documented errors.</summary>
public sealed class ViewOperationTests : ServerTest
{
    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await CreateNamespaceAsync("sales");
    }

    private static string V(string name, string ns = "sales") => $"v1/namespaces/{Ns(ns)}/views/{name}";

    private async Task<JsonNode> CreateViewAsync(string name)
    {
        var r = await Api.PostAsync($"v1/namespaces/{Ns("sales")}/views", Rest.CreateView(name));
        Assert.Equal(200, r.Code);
        return r.Body!;
    }

    [Spec("rest: createView")]
    [Spec("rest: loadView")]
    [Fact]
    public async Task Create_and_load_return_the_view_metadata()
    {
        var created = await CreateViewAsync("daily");
        Assert.EndsWith(".metadata.json", created["metadata-location"]!.GetValue<string>());
        var loaded = await Api.GetAsync(V("daily"));
        Assert.Equal(200, loaded.Code);
        Assert.Equal(created["metadata"]!["view-uuid"]!.GetValue<string>(), loaded.Body!["metadata"]!["view-uuid"]!.GetValue<string>());
        Assert.Equal("SELECT 1 AS id, 'a' AS name", loaded.Body["metadata"]!["versions"]![0]!["representations"]![0]!["sql"]!.GetValue<string>());
    }

    [Spec("rest: createView")]
    [Fact]
    public async Task Create_refuses_an_existing_view_a_table_of_the_same_name_and_a_missing_namespace()
    {
        await CreateViewAsync("daily");
        (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/views", Rest.CreateView("daily"))).AssertError(409, "AlreadyExistsException");
        await CreateTableAsync("sales", "orders");
        (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/views", Rest.CreateView("orders"))).AssertError(409, "AlreadyExistsException");
        (await Api.PostAsync($"v1/namespaces/{Ns("missing")}/views", Rest.CreateView("x"))).AssertError(404, "NoSuchNamespaceException");
    }

    [Spec("rest: loadView")]
    [Spec("rest: viewExists")]
    [Fact]
    public async Task A_missing_view_is_404_on_load_and_exists()
    {
        (await Api.GetAsync(V("missing"))).AssertError(404, "NoSuchViewException");
        Assert.Equal(404, (await Api.HeadAsync(V("missing"))).Code);
        await CreateViewAsync("daily");
        Assert.Equal(204, (await Api.HeadAsync(V("daily"))).Code);
    }

    [Spec("rest: listViews")]
    [Fact]
    public async Task List_returns_views_only_and_404s_a_missing_namespace()
    {
        await CreateViewAsync("daily");
        await CreateViewAsync("weekly");
        await CreateTableAsync("sales", "orders");
        var r = await Api.GetAsync($"v1/namespaces/{Ns("sales")}/views");
        Assert.Equal(200, r.Code);
        Assert.Equal(["daily", "weekly"], r.Body!["identifiers"]!.AsArray().Select(i => i!["name"]!.GetValue<string>()).Order());
        (await Api.GetAsync($"v1/namespaces/{Ns("missing")}/views")).AssertError(404, "NoSuchNamespaceException");
    }

    [Spec("rest: replaceView")]
    [Fact]
    public async Task Replace_applies_when_requirements_hold_and_409s_when_stale()
    {
        var uuid = (await CreateViewAsync("daily"))["metadata"]!["view-uuid"]!.GetValue<string>();
        JsonObject Replace(string requiredUuid) => new()
        {
            ["requirements"] = new JsonArray(new JsonObject { ["type"] = "assert-view-uuid", ["uuid"] = requiredUuid }),
            ["updates"] = new JsonArray(new JsonObject { ["action"] = "set-properties", ["updates"] = new JsonObject { ["comment"] = "daily totals" } }),
        };
        var ok = await Api.PostAsync(V("daily"), Replace(uuid));
        Assert.Equal(200, ok.Code);
        Assert.Equal("daily totals", ok.Body!["metadata"]!["properties"]!["comment"]!.GetValue<string>());
        (await Api.PostAsync(V("daily"), Replace(Guid.NewGuid().ToString()))).AssertError(409, "CommitFailedException");
        (await Api.PostAsync(V("missing"), Replace(uuid))).AssertError(404, "NoSuchViewException");
    }

    [Spec("rest: renameView")]
    [Fact]
    public async Task Rename_moves_a_view_and_errors_like_table_rename()
    {
        await CreateViewAsync("daily");
        await CreateViewAsync("taken");
        Assert.Equal(204, (await Api.PostAsync("v1/views/rename", new JsonObject { ["source"] = Identifier("sales", "daily"), ["destination"] = Identifier("sales", "daily_v2") })).Code);
        Assert.Equal(204, (await Api.HeadAsync(V("daily_v2"))).Code);
        (await Api.PostAsync("v1/views/rename", new JsonObject { ["source"] = Identifier("sales", "daily"), ["destination"] = Identifier("sales", "x") }))
            .AssertError(404, "NoSuchViewException");
        (await Api.PostAsync("v1/views/rename", new JsonObject { ["source"] = Identifier("sales", "daily_v2"), ["destination"] = Identifier("sales", "taken") }))
            .AssertError(409, "AlreadyExistsException");
    }

    [Spec("rest: dropView")]
    [Fact]
    public async Task Drop_is_204_then_404()
    {
        await CreateViewAsync("daily");
        Assert.Equal(204, (await Api.DeleteAsync(V("daily"))).Code);
        (await Api.DeleteAsync(V("daily"))).AssertError(404, "NoSuchViewException");
    }
}
