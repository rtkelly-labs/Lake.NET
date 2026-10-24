using System.Text.Json.Nodes;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>reportMetrics: reports for existing tables are accepted (and, as the spec allows, not stored).</summary>
public sealed class MetricsOperationTests : ServerTest
{
    private static JsonObject CommitReport() => new()
    {
        ["report-type"] = "commit-report", ["table-name"] = "sales.orders", ["snapshot-id"] = 1, ["sequence-number"] = 1,
        ["operation"] = "append", ["metrics"] = new JsonObject(),
    };

    [Spec("rest: reportMetrics")]
    [Fact]
    public async Task A_report_for_an_existing_table_is_204_and_for_a_missing_one_404()
    {
        await CreateNamespaceAsync("sales");
        await CreateTableAsync("sales", "orders");
        Assert.Equal(204, (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/tables/orders/metrics", CommitReport())).Code);
        (await Api.PostAsync($"v1/namespaces/{Ns("sales")}/tables/missing/metrics", CommitReport())).AssertError(404, "NoSuchTableException");
    }
}
