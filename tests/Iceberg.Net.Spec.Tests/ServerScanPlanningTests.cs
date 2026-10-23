using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Json;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// REST server-side scan planning against plans Iceberg Java made itself (fixtures/java-plans, from
/// interop/java/make-fixtures.sh over the Spark fixture tables): replayed by a scripted server — inline, asynchronously
/// (submitted → polled) and split into plan tasks — and read with the deletes Java matched to each file. Every
/// result must equal our own planner's.
/// </summary>
[Spec("rest: planTableScan")]
[Spec("rest: fetchPlanningResult")]
[Spec("rest: fetchScanTasks")]
[Spec("rest: cancelPlanning")]
public sealed class ServerScanPlanningTests : IDisposable
{
    private const string FixtureRoot = "file:///tmp/sentric-spark-fixture-root/warehouse";
    private static readonly string Fixtures = Path.Combine(AppContext.BaseDirectory, "fixtures");
    private static readonly JsonArray Cases = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures, "java-plans", "cases.json")))!.AsArray();
    private static readonly JsonArray Responses = JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures, "java-plans", "responses.json")))!.AsArray();

    private readonly string _copy = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-scanplan", Guid.NewGuid().ToString("N"));

    public ServerScanPlanningTests() => PrefixMappingFileIO.CopyTree(Path.Combine(Fixtures, "spark"), _copy);

    public void Dispose()
    {
        if (Directory.Exists(_copy)) Directory.Delete(_copy, true);
    }

    public enum Mode { Inline, Submitted, PlanTasks }

    /// <summary>A REST server that serves one table and replays one Java plan in the chosen shape.</summary>
    private sealed class PlanningServer(string metadataLocation, string metadataJson, JsonNode plan, Mode mode, bool advertise = true) : HttpMessageHandler
    {
        public List<string> Calls { get; } = [];
        public JsonNode? LastPlanRequest { get; private set; }
        public Func<string, HttpResponseMessage?>? Override { get; set; }
        private int _polls;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var path = request.RequestUri!.AbsolutePath;
            Calls.Add($"{request.Method} {path}");
            if (Override?.Invoke(path) is { } overridden) return overridden;
            if (path.EndsWith("/v1/config", StringComparison.Ordinal))
                return Json(new JsonObject { ["defaults"] = new JsonObject(), ["overrides"] = new JsonObject(),
                    ["endpoints"] = new JsonArray(advertise ? ["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}", "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"] : ["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"]) });
            if (request.Method == HttpMethod.Get && path.EndsWith("/tables/t", StringComparison.Ordinal))
                return Json(new JsonObject { ["metadata-location"] = metadataLocation, ["metadata"] = JsonNode.Parse(metadataJson) });
            if (request.Method == HttpMethod.Post && path.EndsWith("/tables/t/plan", StringComparison.Ordinal))
            {
                LastPlanRequest = JsonNode.Parse(await request.Content!.ReadAsStringAsync(ct));
                return mode switch
                {
                    Mode.Submitted => Json(new JsonObject { ["status"] = "submitted", ["plan-id"] = "p-1" }),
                    Mode.PlanTasks => Json(new JsonObject { ["status"] = "completed", ["plan-id"] = "p-2", ["plan-tasks"] = new JsonArray("first", "second") }),
                    _ => Json(plan.DeepClone()),
                };
            }
            if (request.Method == HttpMethod.Get && path.EndsWith("/plan/p-1", StringComparison.Ordinal))
                return ++_polls < 3 ? Json(new JsonObject { ["status"] = "submitted" }) : Json(plan.DeepClone());
            if (request.Method == HttpMethod.Post && path.EndsWith("/tables/t/tasks", StringComparison.Ordinal))
            {
                var which = JsonNode.Parse(await request.Content!.ReadAsStringAsync(ct))!["plan-task"]!.GetValue<string>();
                var tasks = plan["file-scan-tasks"]!.AsArray();
                var half = tasks.Count / 2;
                var slice = which switch { "first" => tasks.Take(half), "second" => tasks.Skip(half), _ => [] };
                var body = new JsonObject
                {
                    ["delete-files"] = plan["delete-files"]?.DeepClone() ?? new JsonArray(),
                    ["file-scan-tasks"] = new JsonArray(slice.Select(t => t!.DeepClone()).ToArray()),
                };
                if (which == "second") body["plan-tasks"] = new JsonArray("empty"); // plan tasks can yield further plan tasks
                return Json(body);
            }
            if (request.Method == HttpMethod.Delete) return new HttpResponseMessage(HttpStatusCode.NoContent);
            return new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent("""{"error":{"message":"no route","type":"NotFoundException","code":404}}""") };
        }

        private static HttpResponseMessage Json(JsonNode body) => new(HttpStatusCode.OK) { Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json") };
    }

    public static TheoryData<int, Mode> Matrix()
    {
        var data = new TheoryData<int, Mode>();
        for (var i = 0; i < Cases.Count; i++) data.Add(i, Mode.Inline);
        data.Add(0, Mode.Submitted);
        data.Add(3, Mode.Submitted);
        data.Add(0, Mode.PlanTasks);
        data.Add(4, Mode.PlanTasks);
        return data;
    }

    private (PlanningServer Server, string Table, Expression? Filter) Setup(int index, Mode mode, bool advertise = true)
    {
        var c = Cases[index]!;
        var metadataLocation = c["metadata"]!.GetValue<string>();
        var local = new PrefixMappingFileIO(FixtureRoot, _copy);
        var json = Encoding.UTF8.GetString(local.ReadAllAsync(metadataLocation).GetAwaiter().GetResult());
        var filter = c["filter"] is { } f ? ExpressionJson.FromJson(f) : null;
        return (new PlanningServer(metadataLocation, json, Responses[index]!, mode, advertise), c["table"]!.GetValue<string>(), filter);
    }

    private async Task<RestCatalog> Connect(PlanningServer server) =>
        await RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = new Uri("http://catalog.test"), Handler = server, AccessDelegation = null },
            _ => new PrefixMappingFileIO(FixtureRoot, _copy));

    private static string Canonical(Schema schema, IEnumerable<object?[]> rows) =>
        string.Join("\n", rows.Select(r => string.Join("|", schema.Columns.Select((c, i) => SparkJson.Ours(c.Type, r[i])))).Order(StringComparer.Ordinal));

    [Theory]
    [MemberData(nameof(Matrix))]
    public async Task Rows_read_from_a_java_plan_match_our_own_planner(int index, Mode mode)
    {
        var (server, _, filter) = Setup(index, mode);
        using var catalog = await Connect(server);
        Assert.True(catalog.SupportsScanPlanning);
        var table = await catalog.LoadTableAsync(TableIdentifier.Of("ref", "t"));

        var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest(filter));
        var theirs = await ServerSideScan.ReadAsync(table, plan, filter);
        var ours = filter is null ? await TableScan.ReadAllAsync(table) : await TableScan.ReadAsync(table, filter);
        Assert.Equal(Canonical(table.Schema, ours), Canonical(table.Schema, theirs));
        Assert.NotEmpty(theirs);

        // The request carried the filter in the form Java parses, and the plan really followed the chosen shape.
        Assert.Equal(filter is null ? null : Cases[index]!["filter"]!.ToJsonString(), server.LastPlanRequest!["filter"]?.ToJsonString());
        Assert.Equal(Responses[index]!["file-scan-tasks"]!.AsArray().Count, plan.Tasks.Count);
        if (mode == Mode.Submitted) Assert.Equal(3, plan.Polls);
        if (mode == Mode.PlanTasks) Assert.Equal(3, plan.PlanTasksFetched);
        Assert.DoesNotContain(server.Calls, c => c.StartsWith("DELETE", StringComparison.Ordinal)); // finished plans are not cancelled
    }

    [Fact]
    public async Task Merge_on_read_plans_carry_the_deletes_and_they_are_applied()
    {
        foreach (var i in new[] { 0, 3 }) // mor_v2 (position delete files), mor_v3 (deletion vectors)
        {
            var (server, _, filter) = Setup(i, Mode.Inline);
            using var catalog = await Connect(server);
            var table = await catalog.LoadTableAsync(TableIdentifier.Of("ref", "t"));
            var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest(filter));
            Assert.Contains(plan.Tasks, t => t.Deletes.Count > 0);
            var withoutDeletes = new ServerScanPlan(plan.Tasks.Select(t => t with { Deletes = [] }).ToList(), null, 0, 0);
            Assert.True((await ServerSideScan.ReadAsync(table, withoutDeletes, filter)).Count > (await ServerSideScan.ReadAsync(table, plan, filter)).Count);
        }
    }

    [Fact]
    public async Task A_failed_plan_raises_and_an_abandoned_one_is_cancelled()
    {
        var (server, _, _) = Setup(0, Mode.Submitted);
        using var catalog = await Connect(server);
        var table = await catalog.LoadTableAsync(TableIdentifier.Of("ref", "t"));

        server.Override = p => p.EndsWith("/plan/p-1", StringComparison.Ordinal)
            ? new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("""{"status":"failed","error":{"message":"out of memory","type":"ServiceFailureException","code":500}}""") }
            : null;
        var failed = await Assert.ThrowsAsync<ScanPlanningException>(() => catalog.PlanTableScanAsync(table, new ScanPlanRequest()));
        Assert.Contains("out of memory", failed.Message);

        using var cts = new CancellationTokenSource();
        server.Override = p => { if (p.EndsWith("/plan/p-1", StringComparison.Ordinal)) cts.Cancel(); return null; };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => catalog.PlanTableScanAsync(table, new ScanPlanRequest(), ct: cts.Token));
        Assert.Contains("DELETE /v1/namespaces/ref/tables/t/plan/p-1", server.Calls);
    }

    [Fact]
    public async Task Servers_that_do_not_advertise_planning_are_detected()
    {
        var (server, _, _) = Setup(0, Mode.Inline, advertise: false);
        using var catalog = await Connect(server);
        Assert.False(catalog.SupportsScanPlanning);
    }

    /// <summary>Content files as Java 1.10 wrote them (enum-name content, partition keyed by field ID) parse like the spec form Java 1.12 writes.</summary>
    [Fact]
    public async Task Java_1_10_form_content_files_parse_like_the_spec_form()
    {
        var plan = Responses[0]!;
        var spec = plan["file-scan-tasks"]![0]!["data-file"]!;
        var java110 = spec.DeepClone().AsObject();
        java110["content"] = "DATA";
        java110["file-format"] = "PARQUET";
        java110["partition"] = new JsonObject { ["1000"] = spec["partition"]![0]!.DeepClone() };
        var metadata = MetadataJson.Read(Encoding.UTF8.GetString(await new PrefixMappingFileIO(FixtureRoot, _copy).ReadAllAsync(Cases[0]!["metadata"]!.GetValue<string>())));
        var a = ContentFileJson.FromJson(spec, metadata.SpecById, metadata.CurrentSchema);
        var b = ContentFileJson.FromJson(java110, metadata.SpecById, metadata.CurrentSchema);
        Assert.Equal(a.Partition, b.Partition);
        Assert.Equal(a.FilePath, b.FilePath);
        Assert.Equal(a.FileFormat, b.FileFormat);
        Assert.Equal(Manifests.FileContent.Data, b.Content);
        Assert.Throws<InvalidDataException>(() => ContentFileJson.FromJson(new JsonObject { ["spec-id"] = 0, ["content"] = "data" }, metadata.SpecById, metadata.CurrentSchema));
    }
}

/// <summary>Expression JSON in Java's ExpressionParser form (fixtures/java-plans/expressions-java.json: Java's own re-serialization).</summary>
[Spec("rest: planTableScan")]
public sealed class ExpressionJsonTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "fixtures", "java-plans");

    public static TheoryData<int> Corpus() => new(Enumerable.Range(0, JsonNode.Parse(File.ReadAllText(Path.Combine(Dir, "expressions.json")))!.AsArray().Count));

    [Theory]
    [MemberData(nameof(Corpus))]
    public void We_read_javas_form_and_write_it_back_identically(int i)
    {
        var input = JsonNode.Parse(File.ReadAllText(Path.Combine(Dir, "expressions.json")))!.AsArray()[i];
        var java = JsonNode.Parse(File.ReadAllText(Path.Combine(Dir, "expressions-java.json")))!.AsArray()[i]!["json"]!;
        var ours = ExpressionJson.ToJson(ExpressionJson.FromJson(input));
        Assert.True(JsonNode.DeepEquals(java, ours), $"{java.ToJsonString()} vs {ours.ToJsonString()}");
    }

    [Fact]
    public void Typed_literals_travel_as_the_text_forms_binding_accepts()
    {
        var schema = new Schema(0, [NestedField.Optional(1, "day", PrimitiveType.Date), NestedField.Optional(2, "amount", new DecimalType(9, 3)), NestedField.Optional(3, "u", PrimitiveType.Uuid)]);
        var e = Expr.Equal("day", new DateOnly(2024, 1, 2)).And(Expr.Equal("amount", IcebergDecimal.Parse("12.340"))).And(Expr.Equal("u", Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7")));
        var json = ExpressionJson.ToJson(e);
        Assert.Contains("\"2024-01-02\"", json.ToJsonString());
        Assert.Contains("\"12.340\"", json.ToJsonString());
        // The text forms bind to the same typed predicate as the originals.
        Assert.Equal(Binder.Bind(schema, e).ToString(), Binder.Bind(schema, ExpressionJson.FromJson(json)).ToString());
    }

    [Fact]
    public void Malformed_expressions_are_format_errors()
    {
        foreach (var bad in new[] { "{}", "{\"type\":\"eq\",\"term\":\"x\"}", "{\"type\":\"bogus\",\"term\":\"x\"}", "[1]", "{\"type\":\"in\",\"term\":\"x\",\"values\":3}" })
            Assert.True(Corruption.IsFormatError(Record.Exception(() => ExpressionJson.FromJson(JsonNode.Parse(bad)))!), bad);
    }
}
