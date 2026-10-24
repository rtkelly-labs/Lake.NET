using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.RestProtocol.Tests;

/// <summary>
/// planTableScan, fetchPlanningResult, cancelPlanning and fetchScanTasks: the response shapes and the 404s. What a plan
/// contains is checked against the library's own planner and Java's in the server's scan planning tests.
/// </summary>
public sealed class PlanningOperationTests : ServerTest
{
    protected override IReadOnlyDictionary<string, string> Settings => new Dictionary<string, string> { ["ScanPlanning:FilesPerPlanTask"] = "1" };

    private static string T(string name) => $"v1/namespaces/{Ns("sales")}/tables/{name}";

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();
        await CreateNamespaceAsync("sales");
        await CreateTableAsync("sales", "orders");
        var t = await Client.LoadTableAsync(TableIdentifier.Of("sales", "orders"));
        await TableMutations.InsertAsync(t, [[1L, "a"]]);
        await TableMutations.InsertAsync(t, [[2L, "b"]]);
    }

    [Spec("rest: planTableScan")]
    [Spec("rest: fetchScanTasks")]
    [Fact]
    public async Task A_plan_completes_with_plan_tasks_that_fetch_to_file_scan_tasks()
    {
        var plan = await Api.PostAsync(T("orders") + "/plan", new JsonObject());
        Assert.Equal(200, plan.Code);
        Assert.Equal("completed", plan.Body!["status"]!.GetValue<string>());
        var tasks = plan.Body["plan-tasks"]?.AsArray() ?? [];
        var files = plan.Body["file-scan-tasks"]?.AsArray().Count ?? 0;
        foreach (var task in tasks)
        {
            var fetched = await Api.PostAsync(T("orders") + "/tasks", new JsonObject { ["plan-task"] = task!.GetValue<string>() });
            Assert.Equal(200, fetched.Code);
            files += fetched.Body!["file-scan-tasks"]?.AsArray().Count ?? 0;
        }
        Assert.Equal(2, files); // one data file per insert
    }

    [Spec("rest: planTableScan")]
    [Fact]
    public async Task Planning_a_missing_table_is_404() =>
        (await Api.PostAsync(T("missing") + "/plan", new JsonObject())).AssertError(404, "NoSuchTableException");

    [Spec("rest: fetchPlanningResult")]
    [Spec("rest: cancelPlanning")]
    [Fact]
    public async Task An_unknown_plan_id_is_404_to_fetch_and_to_cancel()
    {
        (await Api.GetAsync(T("orders") + "/plan/unknown-plan")).AssertError(404);
        (await Api.DeleteAsync(T("orders") + "/plan/unknown-plan")).AssertError(404);
    }

    [Spec("rest: fetchScanTasks")]
    [Fact]
    public async Task An_unknown_plan_task_is_404() =>
        (await Api.PostAsync(T("orders") + "/tasks", new JsonObject { ["plan-task"] = "unknown-task" })).AssertError(404);
}
