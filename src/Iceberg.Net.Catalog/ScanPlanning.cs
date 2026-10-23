using System.Text.Json.Nodes;
using Iceberg.Net.Expressions;
using Iceberg.Net.Json;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Catalog;

/// <summary>What to plan (REST <c>PlanTableScanRequest</c>).</summary>
/// <param name="Filter">Rows wanted; the server prunes with it and returns a residual per task.</param>
/// <param name="SnapshotId">Snapshot to scan; default the current one.</param>
/// <param name="Select">Column names to project; default all.</param>
public sealed record ScanPlanRequest(Expression? Filter = null, long? SnapshotId = null, IReadOnlyList<string>? Select = null, bool CaseSensitive = true)
{
    public JsonObject ToJson()
    {
        var o = new JsonObject { ["case-sensitive"] = CaseSensitive };
        if (SnapshotId is { } s) o["snapshot-id"] = s;
        if (Select is { } cols) o["select"] = new JsonArray(cols.Select(c => (JsonNode)c).ToArray());
        if (Filter is { } f and not AlwaysTrue) o["filter"] = ExpressionJson.ToJson(f);
        return o;
    }
}

/// <summary>One data file to read, the delete files the server found applicable to it, and the filter rows must still pass.</summary>
public sealed record ServerScanTask(DataFile DataFile, IReadOnlyList<DataFile> Deletes, Expression? Residual);

/// <summary>A server-planned scan: every task, after following asynchronous plans and plan tasks to the end.</summary>
public sealed record ServerScanPlan(IReadOnlyList<ServerScanTask> Tasks, string? PlanId, int Polls, int PlanTasksFetched)
{
    /// <summary>The tasks of one completed planning response (no plan tasks to fetch), e.g. received over another transport.</summary>
    public static ServerScanPlan FromCompletedResponse(JsonNode response, TableMetadata metadata)
    {
        var tasks = new List<ServerScanTask>();
        var pending = new Queue<string>();
        ScanPlanningProtocol.Collect(response, metadata, tasks, pending);
        if (pending.Count > 0) throw new InvalidOperationException("The response has plan tasks to fetch: use RestCatalog.PlanTableScanAsync");
        return new ServerScanPlan(tasks, response["plan-id"]?.GetValue<string>(), 0, 0);
    }
}

/// <summary>Raised when the server reports the plan failed or was cancelled.</summary>
public sealed class ScanPlanningException(string message) : Exception(message);

/// <summary>
/// REST server-side scan planning (spec 1.10+: <c>planTableScan</c>, <c>fetchPlanningResult</c>,
/// <c>fetchScanTasks</c>, <c>cancelPlanning</c>). A <c>submitted</c> plan is polled until it completes; plan tasks are
/// fetched (recursively) until only file scan tasks remain; delete-file references are resolved against the
/// <c>delete-files</c> of the response that carried them. A plan abandoned part-way is cancelled, best effort.
/// </summary>
internal static class ScanPlanningProtocol
{
    public const string PlanEndpoint = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan";

    public static async Task<ServerScanPlan> PlanAsync(
        Func<HttpMethod, string, JsonNode?, CancellationToken, Task<JsonNode?>> send, string tablePath, TableMetadata metadata,
        ScanPlanRequest request, TimeSpan timeout, CancellationToken ct)
    {
        var tasks = new List<ServerScanTask>();
        string? planId = null;
        int polls = 0, fetched = 0;
        var completed = false;
        try
        {
            var response = await send(HttpMethod.Post, $"{tablePath}/plan", request.ToJson(), ct) ?? throw new InvalidDataException("Empty planTableScan response");
            planId = response["plan-id"]?.GetValue<string>();
            var started = DateTime.UtcNow;
            var delay = TimeSpan.FromMilliseconds(100);
            while (Status(response) == "submitted")
            {
                if (planId is null) throw new InvalidDataException("A submitted plan without 'plan-id'");
                if (DateTime.UtcNow - started > timeout) throw new TimeoutException($"Scan planning did not finish within {timeout}");
                await Task.Delay(delay, ct);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromSeconds(2).Ticks));
                response = await send(HttpMethod.Get, $"{tablePath}/plan/{Uri.EscapeDataString(planId)}", null, ct) ?? throw new InvalidDataException("Empty fetchPlanningResult response");
                polls++;
            }
            switch (Status(response))
            {
                case "completed": break;
                case "failed": throw new ScanPlanningException($"Scan planning failed: {response["error"]?["message"]?.GetValue<string>() ?? "no reason given"}");
                case "cancelled": throw new ScanPlanningException("Scan planning was cancelled by the server");
                case var s: throw new InvalidDataException($"Unknown plan status '{s}'");
            }

            var pending = new Queue<string>();
            Collect(response, metadata, tasks, pending);
            while (pending.Count > 0)
            {
                var next = await send(HttpMethod.Post, $"{tablePath}/tasks", new JsonObject { ["plan-task"] = pending.Dequeue() }, ct)
                    ?? throw new InvalidDataException("Empty fetchScanTasks response");
                fetched++;
                Collect(next, metadata, tasks, pending);
            }
            completed = true;
            return new ServerScanPlan(tasks, planId, polls, fetched);
        }
        finally
        {
            if (!completed && planId is not null)
            {
                try { await send(HttpMethod.Delete, $"{tablePath}/plan/{Uri.EscapeDataString(planId)}", null, CancellationToken.None); }
                catch (Exception) { /* best effort: the server expires abandoned plans */ }
            }
        }
    }

    private static string Status(JsonNode response) =>
        (response["status"] ?? response["plan-status"])?.GetValue<string>().ToLowerInvariant() ?? throw new InvalidDataException("Plan response without a status");

    internal static void Collect(JsonNode response, TableMetadata metadata, List<ServerScanTask> tasks, Queue<string> pending)
    {
        var schema = metadata.CurrentSchema;
        var deletes = response["delete-files"]?.AsArray().Select(d => ContentFileJson.FromJson(d!, metadata.SpecById, schema)).ToList() ?? [];
        foreach (var t in response["file-scan-tasks"]?.AsArray() ?? [])
        {
            var data = ContentFileJson.FromJson(t!["data-file"]!, metadata.SpecById, schema);
            var refs = t["delete-file-references"]?.AsArray().Select(r => r!.GetValue<int>()).ToList() ?? [];
            if (refs.Any(r => r < 0 || r >= deletes.Count)) throw new InvalidDataException($"Delete file reference out of range in the task for {data.FilePath}");
            var residual = t["residual-filter"] is { } rf ? ExpressionJson.FromJson(rf) : null;
            tasks.Add(new ServerScanTask(data, refs.Select(r => deletes[r]).ToList(), residual));
        }
        foreach (var p in response["plan-tasks"]?.AsArray() ?? []) pending.Enqueue(p!.GetValue<string>());
    }
}
