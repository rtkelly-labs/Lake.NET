using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>The table operations of the REST OpenAPI: each one's success response and documented errors.</summary>
public sealed class TableOperationTests : ServerTest
{
    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await CreateNamespaceAsync("sales");
    }

    private static string T(string name, string ns = "sales") => $"v1/namespaces/{Ns(ns)}/tables/{Uri.EscapeDataString(name)}";

    [Spec("rest: createTable")]
    [Fact]
    public async Task Create_returns_the_metadata_and_its_location()
    {
        var body = await CreateTableAsync("sales", "orders");
        Assert.EndsWith(".metadata.json", body["metadata-location"]!.GetValue<string>());
        var metadata = body["metadata"]!;
        Assert.Equal(2, metadata["format-version"]!.GetValue<int>());
        Assert.Equal(["id", "name"], metadata["schemas"]![0]!["fields"]!.AsArray().Select(f => f!["name"]!.GetValue<string>()));
        Assert.False(string.IsNullOrEmpty(metadata["table-uuid"]!.GetValue<string>()));
    }

    [Spec("rest: createTable")]
    [Fact]
    public async Task Create_refuses_an_existing_table_a_missing_namespace_and_a_malformed_body()
    {
        await CreateTableAsync("sales", "orders");
        (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/tables", Rest.CreateTable("orders"))).AssertError(409, "AlreadyExistsException");
        (await Api.PostAsync($"v1/namespaces/{Ns("missing")}/tables", Rest.CreateTable("orders"))).AssertError(404, "NoSuchNamespaceException");
        (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/tables", new JsonObject { ["name"] = "no_schema" })).AssertError(400);
    }

    [Spec("rest: createTable")]
    [Fact]
    public async Task A_staged_create_does_not_make_the_table_visible()
    {
        var staged = Rest.CreateTable("pending");
        staged["stage-create"] = true;
        Assert.Equal(200, (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/tables", staged)).Code);
        Assert.Equal(404, (await Api.HeadAsync(T("pending"))).Code);
    }

    [Spec("rest: loadTable")]
    [Fact]
    public async Task Load_returns_an_etag_and_answers_a_matching_if_none_match_with_304()
    {
        await CreateTableAsync("sales", "orders");
        var first = await Api.GetAsync(T("orders"));
        Assert.Equal(200, first.Code);
        var etag = first.Message.Headers.ETag ?? throw new Xunit.Sdk.XunitException("no ETag");
        var again = await Api.SendAsync(HttpMethod.Get, T("orders"), configure: r => r.Headers.IfNoneMatch.Add(etag));
        Assert.Equal(304, again.Code);
        Assert.Null(again.Body);

        await TableMutations.InsertAsync(await Client.LoadTableAsync(TableIdentifier.Of("sales", "orders")), [[1L, "a"]]);
        var changed = await Api.SendAsync(HttpMethod.Get, T("orders"), configure: r => r.Headers.IfNoneMatch.Add(etag));
        Assert.Equal(200, changed.Code); // a commit changes the ETag
    }

    [Spec("rest: loadTable")]
    [Fact]
    public async Task Load_with_snapshots_refs_returns_only_the_referenced_snapshots()
    {
        await CreateTableAsync("sales", "orders");
        var t = await Client.LoadTableAsync(TableIdentifier.Of("sales", "orders"));
        await TableMutations.InsertAsync(t, [[1L, "a"]]);
        await TableMutations.InsertAsync(t, [[2L, "b"]]);
        var all = await Api.GetAsync(T("orders") + "?snapshots=all");
        var refs = await Api.GetAsync(T("orders") + "?snapshots=refs");
        Assert.Equal(2, all.Body!["metadata"]!["snapshots"]!.AsArray().Count);
        Assert.Single(refs.Body!["metadata"]!["snapshots"]!.AsArray());
    }

    [Spec("rest: loadTable")]
    [Fact]
    public async Task Load_of_a_missing_table_is_404() =>
        (await Api.GetAsync(T("missing"))).AssertError(404, "NoSuchTableException");

    [Spec("rest: tableExists")]
    [Fact]
    public async Task Exists_is_204_or_404()
    {
        await CreateTableAsync("sales", "orders");
        Assert.Equal(204, (await Api.HeadAsync(T("orders"))).Code);
        Assert.Equal(404, (await Api.HeadAsync(T("missing"))).Code);
    }

    [Spec("rest: listTables")]
    [Fact]
    public async Task List_returns_identifiers_pages_and_404s_a_missing_namespace()
    {
        foreach (var n in new[] { "a", "b", "c" }) await CreateTableAsync("sales", n);
        var page = await Api.GetAsync($"v1/namespaces/{Ns("sales")}/tables?pageSize=2");
        Assert.Equal(200, page.Code);
        Assert.Equal(2, page.Body!["identifiers"]!.AsArray().Count);
        var token = page.Body["next-page-token"]!.GetValue<string>();
        var rest = await Api.GetAsync($"v1/namespaces/{Ns("sales")}/tables?pageSize=2&pageToken={Uri.EscapeDataString(token)}");
        Assert.Equal(["a", "b", "c"], page.Body["identifiers"]!.AsArray().Concat(rest.Body!["identifiers"]!.AsArray())
            .Select(i => i!["name"]!.GetValue<string>()).Order());
        (await Api.GetAsync($"v1/namespaces/{Ns("missing")}/tables")).AssertError(404, "NoSuchNamespaceException");
    }

    [Spec("rest: renameTable")]
    [Fact]
    public async Task Rename_moves_a_table_across_namespaces()
    {
        await CreateNamespaceAsync("archive");
        await CreateTableAsync("sales", "orders");
        var r = await Api.PostAsync("v1/tables/rename", new JsonObject { ["source"] = Identifier("sales", "orders"), ["destination"] = Identifier("archive", "orders_2024") });
        Assert.Equal(204, r.Code);
        Assert.Equal(404, (await Api.HeadAsync(T("orders"))).Code);
        Assert.Equal(204, (await Api.HeadAsync(T("orders_2024", "archive"))).Code);
    }

    [Spec("rest: renameTable")]
    [Fact]
    public async Task Rename_errors_for_a_missing_source_an_existing_destination_and_a_missing_namespace()
    {
        await CreateTableAsync("sales", "orders");
        await CreateTableAsync("sales", "taken");
        (await Api.PostAsync("v1/tables/rename", new JsonObject { ["source"] = Identifier("sales", "missing"), ["destination"] = Identifier("sales", "x") }))
            .AssertError(404, "NoSuchTableException");
        (await Api.PostAsync("v1/tables/rename", new JsonObject { ["source"] = Identifier("sales", "orders"), ["destination"] = Identifier("sales", "taken") }))
            .AssertError(409, "AlreadyExistsException");
        (await Api.PostAsync("v1/tables/rename", new JsonObject { ["source"] = Identifier("sales", "orders"), ["destination"] = Identifier("nowhere", "x") }))
            .AssertError(404, "NoSuchNamespaceException");
    }

    [Spec("rest: registerTable")]
    [Fact]
    public async Task Register_adopts_an_existing_metadata_file_and_refuses_a_taken_name()
    {
        var created = await CreateTableAsync("sales", "orders");
        var location = created["metadata-location"]!.GetValue<string>();
        var r = await Api.PostAsync($"v1/namespaces/{Ns("sales")}/register", new JsonObject { ["name"] = "orders_copy", ["metadata-location"] = location });
        Assert.Equal(200, r.Code);
        Assert.Equal(location, r.Body!["metadata-location"]!.GetValue<string>());
        (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/register", new JsonObject { ["name"] = "orders", ["metadata-location"] = location }))
            .AssertError(409, "AlreadyExistsException");
        (await Api.PostAsync($"v1/namespaces/{Ns("missing")}/register", new JsonObject { ["name"] = "x", ["metadata-location"] = location }))
            .AssertError(404, "NoSuchNamespaceException");
    }

    [Spec("rest: unregisterTable")]
    [Fact]
    public async Task Unregister_forgets_the_table_but_keeps_its_files()
    {
        var created = await CreateTableAsync("sales", "orders");
        var location = created["metadata-location"]!.GetValue<string>();
        var r = await Api.PostAsync(T("orders") + "/unregister", null);
        Assert.Equal(200, r.Code); // UnregisterTableResult: the last metadata location and its metadata
        Assert.Equal(location, r.Body!["metadata-location"]!.GetValue<string>());
        Assert.NotNull(r.Body["metadata"]?["table-uuid"]);
        Assert.Equal(404, (await Api.HeadAsync(T("orders"))).Code);
        Assert.True(File.Exists(new Uri(location.StartsWith('/') ? "file://" + location : location).LocalPath));
        (await Api.PostAsync(T("orders") + "/unregister", null)).AssertError(404, "NoSuchTableException");
    }

    [Spec("rest: dropTable")]
    [Fact]
    public async Task Drop_is_204_then_404_and_purge_removes_the_files()
    {
        await CreateTableAsync("sales", "orders");
        var t = await Client.LoadTableAsync(TableIdentifier.Of("sales", "orders"));
        await TableMutations.InsertAsync(t, [[1L, "a"]]);
        var dir = new Uri(t.Location.StartsWith('/') ? "file://" + t.Location : t.Location).LocalPath;
        Assert.NotEmpty(Directory.GetFiles(dir, "*", SearchOption.AllDirectories));

        Assert.Equal(204, (await Api.DeleteAsync(T("orders") + "?purgeRequested=true")).Code);
        Assert.Empty(Directory.Exists(dir) ? Directory.GetFiles(dir, "*", SearchOption.AllDirectories) : []);
        (await Api.DeleteAsync(T("orders"))).AssertError(404, "NoSuchTableException");
    }
}
