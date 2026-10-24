using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>
/// 401 without a token and 403 without the grant an operation needs, for each level (docs/rest-server.md, Authorization):
/// root holds admin:*, writer write:sales, reader read:sales, nobody no grant at all.
/// </summary>
public sealed class AuthorizationTests : ServerTest
{
    protected override IReadOnlyDictionary<string, string> Settings => Rest.AuthSettings;
    protected override string ClientCredential => "root:root-secret";

    private Rest _root = null!;

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        _root = new Rest(Host.Http(), await Rest.TokenAsync(Host, "root", "root-secret"));
        Assert.Equal(200, (await _root.PostAsync("v1/namespaces", Rest.CreateNamespace("sales"))).Code);
        Assert.Equal(200, (await _root.PostAsync($"v1/namespaces/{Ns("sales")}/tables", Rest.CreateTable("orders"))).Code);
    }

    private async Task<Rest> As(string principal) => principal == "anonymous"
        ? new Rest(Host.Http())
        : new Rest(Host.Http(), await Rest.TokenAsync(Host, principal, $"{principal}-secret"));

    public static TheoryData<string, string, int> Matrix => new()
    {
        // operation, principal, expected status
        { "list-tables", "reader", 200 }, { "list-tables", "writer", 200 }, { "list-tables", "nobody", 403 }, { "list-tables", "anonymous", 401 },
        { "load-table", "reader", 200 }, { "load-table", "writer", 200 }, { "load-table", "nobody", 403 }, { "load-table", "anonymous", 401 },
        { "create-table", "reader", 403 }, { "create-table", "writer", 200 }, { "create-table", "root", 200 },
        { "commit", "reader", 403 }, { "commit", "writer", 200 },
        { "drop-table", "reader", 403 }, { "drop-table", "writer", 403 }, { "drop-table", "root", 204 },
        { "create-namespace", "writer", 403 }, { "create-namespace", "root", 200 },
        { "update-properties", "writer", 403 }, { "update-properties", "root", 200 },
        { "report-metrics", "reader", 204 }, { "report-metrics", "nobody", 403 },
    };

    [Spec("rest: listTables")]
    [Spec("rest: loadTable")]
    [Spec("rest: createTable")]
    [Spec("rest: updateTable")]
    [Spec("rest: dropTable")]
    [Spec("rest: createNamespace")]
    [Spec("rest: updateProperties")]
    [Spec("rest: reportMetrics")]
    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task Each_operation_needs_its_grant(string operation, string principal, int expected)
    {
        var api = await As(principal);
        var uuid = (await _root.GetAsync($"v1/namespaces/{Ns("sales")}/tables/orders")).Body!["metadata"]!["table-uuid"]!.GetValue<string>();
        var r = operation switch
        {
            "list-tables" => await api.GetAsync($"v1/namespaces/{Ns("sales")}/tables"),
            "load-table" => await api.GetAsync($"v1/namespaces/{Ns("sales")}/tables/orders"),
            "create-table" => await api.PostAsync($"v1/namespaces/{Ns("sales")}/tables", Rest.CreateTable($"t_{principal}")),
            "commit" => await api.PostAsync($"v1/namespaces/{Ns("sales")}/tables/orders", new System.Text.Json.Nodes.JsonObject
            {
                ["requirements"] = new System.Text.Json.Nodes.JsonArray(new System.Text.Json.Nodes.JsonObject { ["type"] = "assert-table-uuid", ["uuid"] = uuid }),
                ["updates"] = new System.Text.Json.Nodes.JsonArray(new System.Text.Json.Nodes.JsonObject
                    { ["action"] = "set-properties", ["updates"] = new System.Text.Json.Nodes.JsonObject { ["by"] = principal } }),
            }),
            "drop-table" => await api.DeleteAsync($"v1/namespaces/{Ns("sales")}/tables/orders"),
            "create-namespace" => await api.PostAsync("v1/namespaces", Rest.CreateNamespace($"ns_{principal}")),
            "update-properties" => await api.PostAsync($"v1/namespaces/{Ns("sales")}/properties", new System.Text.Json.Nodes.JsonObject
                { ["updates"] = new System.Text.Json.Nodes.JsonObject { ["k"] = "v" } }),
            "report-metrics" => await api.PostAsync($"v1/namespaces/{Ns("sales")}/tables/orders/metrics", new System.Text.Json.Nodes.JsonObject
            {
                ["report-type"] = "commit-report", ["table-name"] = "sales.orders", ["snapshot-id"] = 1, ["sequence-number"] = 1,
                ["operation"] = "append", ["metrics"] = new System.Text.Json.Nodes.JsonObject(),
            }),
            _ => throw new ArgumentOutOfRangeException(nameof(operation)),
        };
        if (expected >= 400) r.AssertError(expected);
        else Assert.True(r.Code == expected, $"{operation} as {principal}: expected {expected}, got {r.Code}: {r.Body?.ToJsonString()}");
    }

    [Fact]
    public async Task The_client_surfaces_401_and_403_as_unauthorized_access()
    {
        using var reader = await Host.ConnectAsync(credential: "reader:reader-secret");
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            reader.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "nope"), new Iceberg.Net.Types.Schema(0, [Iceberg.Net.Types.NestedField.Require(1, "id", Iceberg.Net.Types.PrimitiveType.Long)]))));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => Host.ConnectAsync(credential: "reader:wrong"));
    }
}
