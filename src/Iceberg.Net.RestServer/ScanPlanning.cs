using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Json;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer;

/// <summary>A plan ID that does not exist (never did, expired, or belongs to another table): HTTP 404.</summary>
public sealed class NoSuchPlanIdException(string message) : Exception(message);

/// <summary>A plan task that does not exist (malformed, for another table, or its snapshot or manifests are gone): HTTP 404.</summary>
public sealed class NoSuchPlanTaskException(string message) : Exception(message);

/// <summary>A <c>PlanTableScanRequest</c>, parsed. Names are resolved against the scan schema when planning.</summary>
public sealed record PlanRequest(
    long? SnapshotId,
    IReadOnlyList<string>? Select,
    Expression Filter,
    bool CaseSensitive,
    bool UseSnapshotSchema,
    long? StartSnapshotId,
    long? EndSnapshotId,
    IReadOnlyList<string>? StatsFields)
{
    public static PlanRequest Parse(JsonNode body) => new(
        body["snapshot-id"]?.GetValue<long>(),
        body["select"]?.AsArray().Select(s => s!.GetValue<string>()).ToList(),
        body["filter"] is { } f ? ExpressionJson.FromJson(f) : Expression.True,
        body["case-sensitive"]?.GetValue<bool>() ?? true,
        body["use-snapshot-schema"]?.GetValue<bool>() ?? false,
        body["start-snapshot-id"]?.GetValue<long>(),
        body["end-snapshot-id"]?.GetValue<long>(),
        body["stats-fields"]?.AsArray().Select(s => s!.GetValue<string>()).ToList());
}

/// <summary>
/// Server-side scan planning over the library's own planner (<see cref="TableScan.PlanManifestsAsync"/>, the same
/// manifest, partition and metrics pruning every local scan uses) and delete index (<see cref="DeleteIndex.DeleteFilesFor"/>,
/// the spec's applicability rules). Data manifests that survive pruning are grouped by their file counts; one group is
/// answered inline, several become plan tasks. A plan task is self-contained (table UUID, snapshot, manifest paths,
/// filter), so any server instance can answer it and nothing is kept for it. Each task's residual is the request's
/// filter (rows must still pass it), or <c>true</c> without one; data files carry only the requested
/// <c>stats-fields</c> metrics, delete files all of theirs. Plans are kept in memory for <c>plan/{plan-id}</c> until
/// <see cref="ScanPlanningOptions.PlanLifetime"/> after they finish.
/// </summary>
public sealed class ScanPlanner(CatalogService catalog, ScanPlanningOptions options, ILogger? logger = null)
{
    private sealed class PlanState(TableIdentifier table, CancellationTokenSource cancel)
    {
        public TableIdentifier Table { get; } = table;
        public CancellationTokenSource Cancel { get; } = cancel;
        public volatile string Status = "submitted";
        public JsonObject? Result;
        public string? Error;
        public DateTimeOffset? Finished;
    }

    private readonly ConcurrentDictionary<string, PlanState> _plans = new(StringComparer.Ordinal);

    public Func<DateTimeOffset> Clock { get; init; } = () => DateTimeOffset.UtcNow;

    /// <summary>Plans kept in memory (for tests).</summary>
    public int PlansKept => _plans.Count;

    private long _plansStarted;

    /// <summary><c>planTableScan</c> requests accepted since start.</summary>
    public long PlansStarted => Interlocked.Read(ref _plansStarted);

    /// <summary><c>planTableScan</c>: a completed plan with its plan ID, or (with <see cref="ScanPlanningOptions.Async"/>) a submitted one.</summary>
    public async Task<JsonObject> PlanAsync(TableIdentifier id, PlanRequest request, CancellationToken ct)
    {
        Expire();
        var loaded = await catalog.LoadTableAsync(id, ct);
        Interlocked.Increment(ref _plansStarted);
        var planId = Guid.NewGuid().ToString();
        if (options.Async)
        {
            var state = new PlanState(id, new CancellationTokenSource());
            _plans[planId] = state;
            _ = Task.Run(async () =>
            {
                try
                {
                    var result = await PlanCoreAsync(loaded, request, state.Cancel.Token);
                    state.Result = result;
                    state.Status = "completed";
                }
                catch (OperationCanceledException) when (state.Cancel.IsCancellationRequested)
                {
                    state.Status = "cancelled";
                }
                catch (Exception e)
                {
                    logger?.LogWarning(e, "Scan planning {PlanId} for {Table} failed", planId, id);
                    state.Error = e.Message;
                    state.Status = "failed";
                }
                finally
                {
                    state.Finished = Clock();
                }
            }, CancellationToken.None);
            return new JsonObject { ["status"] = "submitted", ["plan-id"] = planId };
        }

        var completed = await PlanCoreAsync(loaded, request, ct);
        _plans[planId] = new PlanState(id, new CancellationTokenSource()) { Status = "completed", Result = completed, Finished = Clock() };
        var response = new JsonObject { ["status"] = "completed", ["plan-id"] = planId };
        foreach (var (k, v) in completed) response[k] = v?.DeepClone();
        return response;
    }

    /// <summary><c>fetchPlanningResult</c>.</summary>
    public JsonObject Fetch(TableIdentifier id, string planId)
    {
        Expire();
        var state = Find(id, planId);
        switch (state.Status)
        {
            case "completed":
                var response = new JsonObject { ["status"] = "completed" };
                foreach (var (k, v) in state.Result!) response[k] = v?.DeepClone();
                return response;
            case "failed":
                return new JsonObject
                {
                    ["status"] = "failed",
                    ["error"] = new JsonObject { ["message"] = state.Error ?? "Scan planning failed", ["type"] = "ScanPlanningException", ["code"] = 500 },
                };
            case var s:
                return new JsonObject { ["status"] = s };
        }
    }

    /// <summary><c>cancelPlanning</c>: stops a running plan and forgets the plan either way.</summary>
    public void Cancel(TableIdentifier id, string planId)
    {
        var state = Find(id, planId);
        state.Cancel.Cancel();
        _plans.TryRemove(planId, out _);
    }

    private PlanState Find(TableIdentifier id, string planId) =>
        _plans.TryGetValue(planId, out var state) && state.Table.Equals(id) ? state : throw new NoSuchPlanIdException($"No plan {planId} for {id}");

    private void Expire()
    {
        var now = Clock();
        foreach (var (planId, state) in _plans)
            if (state.Finished is { } done && now - done > options.PlanLifetime)
                _plans.TryRemove(planId, out _);
    }

    /// <summary><c>fetchScanTasks</c>: the file scan tasks of one plan task.</summary>
    public async Task<JsonObject> FetchTasksAsync(TableIdentifier id, string planTask, CancellationToken ct)
    {
        var token = PlanTaskToken.Decode(planTask);
        var loaded = await catalog.LoadTableAsync(id, ct);
        var m = loaded.Metadata;
        if (m.TableUuid != token.TableUuid) throw new NoSuchPlanTaskException($"The plan task is not for {id}");
        var snapshot = m.SnapshotById(token.SnapshotId) ?? throw new NoSuchPlanTaskException($"Snapshot {token.SnapshotId} of {id} no longer exists");
        var table = TableOf(loaded);
        var manifests = await TableScan.ManifestsAsync(table, snapshot, ct);
        var byPath = manifests.ToDictionary(mf => mf.ManifestPath, StringComparer.Ordinal);
        var group = token.Manifests.Select(p => byPath.GetValueOrDefault(p) ?? throw new NoSuchPlanTaskException($"Manifest {p} is not in snapshot {snapshot.SnapshotId}")).ToList();
        var schema = token.UseSnapshotSchema ? TimeTravel.SchemaFor(m, snapshot) : m.CurrentSchema;
        return await TasksAsync(table, snapshot, manifests, group, schema, token.Filter, token.StatsFields?.ToHashSet(), ct);
    }

    private async Task<JsonObject> PlanCoreAsync(LoadedTable loaded, PlanRequest request, CancellationToken ct)
    {
        if (request.StartSnapshotId is not null || request.EndSnapshotId is not null)
            throw new NotSupportedException("Incremental scans (start-snapshot-id / end-snapshot-id) are not supported by this server");
        var m = loaded.Metadata;
        var snapshot = request.SnapshotId is { } sid
            ? m.SnapshotById(sid) ?? throw new BadRequestException($"Unknown snapshot {sid}")
            : m.CurrentSnapshot;
        if (snapshot is null) return new JsonObject { ["delete-files"] = new JsonArray(), ["file-scan-tasks"] = new JsonArray() };

        var schema = request.UseSnapshotSchema ? TimeTravel.SchemaFor(m, snapshot) : m.CurrentSchema;
        var names = new NameResolver(schema, request.CaseSensitive);
        var filter = names.Resolve(request.Filter);
        foreach (var column in request.Select ?? []) names.Field(column);
        var statsFields = request.StatsFields?.Select(n => names.Field(n).Id).ToList();
        var bound = Binder.Bind(schema, filter);

        var table = TableOf(loaded);
        var manifests = await TableScan.ManifestsAsync(table, snapshot, ct);
        var evaluators = new Dictionary<int, ManifestEvaluator>();
        var data = manifests.Where(mf => mf.Content == ManifestContent.Data).Where(mf =>
        {
            if (!evaluators.TryGetValue(mf.PartitionSpecId, out var eval))
                evaluators[mf.PartitionSpecId] = eval = new ManifestEvaluator(Projections.Inclusive(m.SpecById(mf.PartitionSpecId), schema, bound));
            return eval.MightMatch(mf.Partitions);
        }).ToList();

        var groups = Group(data, Math.Max(1, options.FilesPerPlanTask));
        if (groups.Count <= 1)
            return await TasksAsync(table, snapshot, manifests, groups.FirstOrDefault() ?? [], schema, filter, statsFields?.ToHashSet(), ct);

        var tokens = groups.Select(g => (JsonNode)new PlanTaskToken(m.TableUuid, snapshot.SnapshotId, g.Select(mf => mf.ManifestPath).ToList(), filter,
            request.UseSnapshotSchema, statsFields).Encode()).ToArray();
        return new JsonObject { ["plan-tasks"] = new JsonArray(tokens) };
    }

    /// <summary>Consecutive manifests whose data file counts add up to about <paramref name="filesPerGroup"/>.</summary>
    private static List<List<ManifestFile>> Group(IReadOnlyList<ManifestFile> manifests, int filesPerGroup)
    {
        var groups = new List<List<ManifestFile>>();
        var current = new List<ManifestFile>();
        long files = 0;
        foreach (var mf in manifests)
        {
            // Counts are optional in v1 manifest lists (read as 0); such a manifest counts as one file.
            var count = Math.Max(1L, (long)mf.AddedFilesCount + mf.ExistingFilesCount);
            if (current.Count > 0 && files + count > filesPerGroup)
            {
                groups.Add(current);
                current = [];
                files = 0;
            }
            current.Add(mf);
            files += count;
        }
        if (current.Count > 0) groups.Add(current);
        return groups;
    }

    private async Task<JsonObject> TasksAsync(Table table, Snapshot snapshot, IReadOnlyList<ManifestFile> all, IReadOnlyList<ManifestFile> group, Schema schema,
        Expression filter, ISet<int>? statsFields, CancellationToken ct)
    {
        var m = table.Metadata;
        var plan = await TableScan.PlanManifestsAsync(table, snapshot, group, schema, filter, ct);
        var deletes = await DeleteIndex.LoadAsync(table, all, ct);
        var deleteFiles = new JsonArray();
        var indexOf = new Dictionary<(string Path, long? Offset), int>();
        var residual = filter is AlwaysTrue ? JsonValue.Create(true) : ExpressionJson.ToJson(filter);
        var tasks = new JsonArray();
        foreach (var task in plan.Tasks)
        {
            var refs = new JsonArray();
            foreach (var d in deletes.DeleteFilesFor(task.File, task.DataSequenceNumber))
            {
                if (!indexOf.TryGetValue((d.FilePath, d.ContentOffset), out var index))
                {
                    // Delete manifests are read with the current schema (DeleteIndex.LoadAsync), so their partitions are typed by it.
                    deleteFiles.Add(ContentFileJson.ToJson(d, m.SpecById(d.SpecId), m.CurrentSchema));
                    indexOf[(d.FilePath, d.ContentOffset)] = index = deleteFiles.Count - 1;
                }
                refs.Add(index);
            }
            var o = new JsonObject { ["data-file"] = ContentFileJson.ToJson(task.File, m.SpecById(task.File.SpecId), schema, statsFields ?? new HashSet<int>()) };
            if (refs.Count > 0) o["delete-file-references"] = refs;
            o["residual-filter"] = residual.DeepClone();
            tasks.Add(o);
        }
        return new JsonObject { ["delete-files"] = deleteFiles, ["file-scan-tasks"] = tasks };
    }

    // Planning only reads: the table never commits, so it needs no catalog.
    private Table TableOf(LoadedTable loaded) =>
        new(null!, loaded.Id, loaded.Metadata with { MetadataFileLocation = loaded.MetadataLocation }, catalog.FileIO);

    /// <summary>Field names of a request, resolved exactly or (with <c>case-sensitive: false</c>) ignoring case.</summary>
    private sealed class NameResolver(Schema schema, bool caseSensitive)
    {
        private readonly Dictionary<string, int> _names = TypeUtil.IndexByName(schema.Struct);

        public NestedField Field(string name)
        {
            if (schema.FindField(name) is { } exact) return exact;
            if (!caseSensitive)
            {
                var matches = _names.Where(kv => string.Equals(kv.Key, name, StringComparison.OrdinalIgnoreCase)).ToList();
                if (matches.Count == 1) return schema.FindField(matches[0].Value)!;
                if (matches.Count > 1) throw new BadRequestException($"Field name '{name}' is ambiguous ignoring case");
            }
            throw new BadRequestException($"Cannot find field '{name}' in the scan schema");
        }

        public Expression Resolve(Expression e) => e switch
        {
            And a => new And(Resolve(a.Left), Resolve(a.Right)),
            Or o => new Or(Resolve(o.Left), Resolve(o.Right)),
            Not n => new Not(Resolve(n.Child)),
            UnboundPredicate p => p with { Column = schema.FindColumnName(Field(p.Column).Id)! },
            _ => e,
        };
    }
}

/// <summary>
/// An opaque plan task: everything needed to plan one group of manifests again, base64url-encoded JSON. The manifests
/// must still belong to the snapshot when it is fetched, so a token cannot reach files outside its table.
/// </summary>
internal sealed record PlanTaskToken(Guid TableUuid, long SnapshotId, IReadOnlyList<string> Manifests, Expression Filter, bool UseSnapshotSchema,
    IReadOnlyList<int>? StatsFields)
{
    public string Encode()
    {
        var o = new JsonObject
        {
            ["t"] = TableUuid.ToString(),
            ["s"] = SnapshotId,
            ["m"] = new JsonArray(Manifests.Select(p => (JsonNode)p).ToArray()),
            ["f"] = ExpressionJson.ToJson(Filter),
            ["u"] = UseSnapshotSchema,
        };
        if (StatsFields is not null) o["sf"] = new JsonArray(StatsFields.Select(i => (JsonNode)i).ToArray());
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(o.ToJsonString())).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    public static PlanTaskToken Decode(string token)
    {
        try
        {
            var b64 = token.Replace('-', '+').Replace('_', '/');
            b64 += new string('=', (4 - b64.Length % 4) % 4);
            var o = JsonNode.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(b64)))!.AsObject();
            return new PlanTaskToken(
                Guid.Parse(o["t"]!.GetValue<string>()),
                o["s"]!.GetValue<long>(),
                o["m"]!.AsArray().Select(p => p!.GetValue<string>()).ToList(),
                ExpressionJson.FromJson(o["f"]),
                o["u"]?.GetValue<bool>() ?? false,
                o["sf"]?.AsArray().Select(i => i!.GetValue<int>()).ToList());
        }
        catch (Exception e) when (e is FormatException or System.Text.Json.JsonException or NullReferenceException or InvalidOperationException
                                       or InvalidDataException or ArgumentException)
        {
            throw new NoSuchPlanTaskException("Unknown or malformed plan task");
        }
    }
}
