using System.Text.Json.Nodes;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>updateTable and commitTransaction: requirements are checked before any update applies, all or nothing.</summary>
public sealed class CommitOperationTests : ServerTest
{
    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await CreateNamespaceAsync("sales");
    }

    private static string T(string name) => $"v1/namespaces/{Ns("sales")}/tables/{name}";

    private static JsonObject SetProperty(string key, string value) => new() { ["action"] = "set-properties", ["updates"] = new JsonObject { [key] = value } };
    private static JsonObject AssertUuid(string uuid) => new() { ["type"] = "assert-table-uuid", ["uuid"] = uuid };

    private async Task<string> UuidAsync(string name) => (await Api.GetAsync(T(name))).Body!["metadata"]!["table-uuid"]!.GetValue<string>();
    private async Task<string?> PropertyAsync(string name, string key) => (await Api.GetAsync(T(name))).Body!["metadata"]!["properties"]?[key]?.GetValue<string>();

    private static JsonObject Change(string name, JsonObject requirement, JsonObject update) => new()
    {
        ["identifier"] = Identifier("sales", name),
        ["requirements"] = new JsonArray(requirement),
        ["updates"] = new JsonArray(update),
    };

    [Spec("rest: updateTable")]
    [Fact]
    public async Task A_commit_whose_requirements_hold_applies_and_returns_the_new_metadata()
    {
        await CreateTableAsync("sales", "orders");
        var before = (await Api.GetAsync(T("orders"))).Body!["metadata-location"]!.GetValue<string>();
        var r = await Api.PostAsync(T("orders"), Change("orders", AssertUuid(await UuidAsync("orders")), SetProperty("team", "finance")));
        Assert.Equal(200, r.Code);
        Assert.NotEqual(before, r.Body!["metadata-location"]!.GetValue<string>());
        Assert.Equal("finance", r.Body["metadata"]!["properties"]!["team"]!.GetValue<string>());
    }

    [Spec("rest: updateTable")]
    [Fact]
    public async Task A_stale_requirement_is_409_and_nothing_changes()
    {
        await CreateTableAsync("sales", "orders");
        (await Api.PostAsync(T("orders"), Change("orders", AssertUuid(Guid.NewGuid().ToString()), SetProperty("team", "finance"))))
            .AssertError(409, "CommitFailedException");
        Assert.Null(await PropertyAsync("orders", "team"));

        var stale = new JsonObject { ["type"] = "assert-ref-snapshot-id", ["ref"] = "main", ["snapshot-id"] = 123 };
        (await Api.PostAsync(T("orders"), Change("orders", stale, SetProperty("team", "finance")))).AssertError(409, "CommitFailedException");
    }

    [Spec("rest: updateTable")]
    [Fact]
    public async Task A_commit_to_a_missing_table_is_404_and_a_mismatched_identifier_is_400()
    {
        (await Api.PostAsync(T("missing"), Change("missing", AssertUuid(Guid.NewGuid().ToString()), SetProperty("a", "b"))))
            .AssertError(404, "NoSuchTableException");
        await CreateTableAsync("sales", "orders");
        (await Api.PostAsync(T("orders"), Change("other", AssertUuid(await UuidAsync("orders")), SetProperty("a", "b")))).AssertError(400);
    }

    [Spec("rest: commitTransaction")]
    [Fact]
    public async Task A_transaction_applies_every_table_change()
    {
        await CreateTableAsync("sales", "a");
        await CreateTableAsync("sales", "b");
        var r = await Api.PostAsync("v1/transactions/commit", new JsonObject
        {
            ["table-changes"] = new JsonArray(
                Change("a", AssertUuid(await UuidAsync("a")), SetProperty("batch", "1")),
                Change("b", AssertUuid(await UuidAsync("b")), SetProperty("batch", "1"))),
        });
        Assert.Equal(204, r.Code);
        Assert.Equal("1", await PropertyAsync("a", "batch"));
        Assert.Equal("1", await PropertyAsync("b", "batch"));
    }

    [Spec("rest: commitTransaction")]
    [Fact]
    public async Task One_stale_table_in_a_transaction_is_409_and_no_table_changes()
    {
        await CreateTableAsync("sales", "a");
        await CreateTableAsync("sales", "b");
        (await Api.PostAsync("v1/transactions/commit", new JsonObject
        {
            ["table-changes"] = new JsonArray(
                Change("a", AssertUuid(await UuidAsync("a")), SetProperty("batch", "1")),
                Change("b", AssertUuid(Guid.NewGuid().ToString()), SetProperty("batch", "1"))),
        })).AssertError(409, "CommitFailedException");
        Assert.Null(await PropertyAsync("a", "batch"));
        Assert.Null(await PropertyAsync("b", "batch"));
    }

    [Spec("rest: commitTransaction")]
    [Fact]
    public async Task A_transaction_naming_a_missing_table_is_404_and_no_table_changes()
    {
        await CreateTableAsync("sales", "a");
        (await Api.PostAsync("v1/transactions/commit", new JsonObject
        {
            ["table-changes"] = new JsonArray(
                Change("a", AssertUuid(await UuidAsync("a")), SetProperty("batch", "1")),
                Change("missing", AssertUuid(Guid.NewGuid().ToString()), SetProperty("batch", "1"))),
        })).AssertError(404, "NoSuchTableException");
        Assert.Null(await PropertyAsync("a", "batch"));
    }
}
