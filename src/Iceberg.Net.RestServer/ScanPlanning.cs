using System.Collections.Concurrent;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
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
/// <c>stats-fields</c> metrics, delete files all of theirs. Plans are kept in the <see cref="IServerStateStore"/> for
/// <c>plan/{plan-id}</c> until <see cref="ScanPlanningOptions.PlanLifetime"/> after they finish, so every instance over
/// one database answers every plan ID. Incremental requests (<c>start-snapshot-id</c> / <c>end-snapshot-id</c>) are
/// planned as Java's <c>IncrementalAppendScan</c>.
/// </summary>
public sealed class ScanPlanner(CatalogService catalog, ScanPlanningOptions options, IServerStateStore state, ILogger? logger = null)
{
    /// <summary>Plans running in the background here, so a cancel can stop them (a cancel elsewhere just forgets the plan).</summary>
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _running = new(StringComparer.Ordinal);

    public Func<DateTimeOffset> Clock { get; init; } = () => DateTimeOffset.UtcNow;

    private long _plansStarted;
    private long _lastSweepTicks;

    /// <summary><c>planTableScan</c> requests accepted since start.</summary>
    public long PlansStarted => Interlocked.Read(ref _plansStarted);

    // Millisecond precision, as the SQL store keeps it.
    private DateTimeOffset Now() => DateTimeOffset.FromUnixTimeMilliseconds(Clock().ToUnixTimeMilliseconds());

    /// <summary>
    /// <c>planTableScan</c>: a completed plan with its plan ID, or (with <see cref="ScanPlanningOptions.Async"/>) a
    /// submitted one. Either is recorded in the <see cref="IServerStateStore"/>, so any instance answers its plan ID.
    /// </summary>
    public async Task<JsonObject> PlanAsync(TableIdentifier id, PlanRequest request, CancellationToken ct)
    {
        await ExpireAsync();
        var loaded = await catalog.LoadTableAsync(id, ct);
        Interlocked.Increment(ref _plansStarted);
        var planId = Guid.NewGuid().ToString();
        if (options.Async)
        {
            var cancel = new CancellationTokenSource();
            _running[planId] = cancel;
            await state.AddPlanAsync(new PlanRecord(planId, id, "submitted", null, null, Now(), null), ct);
            _ = Task.Run(async () =>
            {
                try
                {
                    var result = await PlanCoreAsync(loaded, request, cancel.Token);
                    await state.FinishPlanAsync(planId, "completed", result.ToJsonString(), null, Now());
                }
                catch (OperationCanceledException) when (cancel.IsCancellationRequested)
                {
                    // Cancelled: the plan was forgotten already.
                }
                catch (Exception e)
                {
                    logger?.LogWarning(e, "Scan planning {PlanId} for {Table} failed", planId, id);
                    await state.FinishPlanAsync(planId, "failed", null, e.Message, Now());
                }
                finally
                {
                    _running.TryRemove(planId, out _);
                    cancel.Dispose();
                }
            }, CancellationToken.None);
            return new JsonObject { ["status"] = "submitted", ["plan-id"] = planId };
        }

        var completed = await PlanCoreAsync(loaded, request, ct);
        var now = Now();
        await state.AddPlanAsync(new PlanRecord(planId, id, "completed", completed.ToJsonString(), null, now, now), ct);
        var response = new JsonObject { ["status"] = "completed", ["plan-id"] = planId };
        foreach (var (k, v) in completed) response[k] = v?.DeepClone();
        return response;
    }

    /// <summary><c>fetchPlanningResult</c>.</summary>
    public async Task<JsonObject> FetchAsync(TableIdentifier id, string planId, CancellationToken ct = default)
    {
        await ExpireAsync();
        var plan = await FindAsync(id, planId, ct);
        switch (plan.Status)
        {
            case "completed":
                var response = new JsonObject { ["status"] = "completed" };
                foreach (var (k, v) in JsonNode.Parse(plan.Result ?? "{}")!.AsObject().ToList())
                    response[k] = v?.DeepClone();
                return response;
            case "submitted" when Now() - plan.Created > options.PlanLifetime:
                return Failed($"Scan planning {planId} did not finish within {options.PlanLifetime}; the server planning it may have stopped");
            case "failed":
                return Failed(plan.Error ?? "Scan planning failed");
            case var s:
                return new JsonObject { ["status"] = s };
        }

        static JsonObject Failed(string message) => new()
        {
            ["status"] = "failed",
            ["error"] = new JsonObject { ["message"] = message, ["type"] = "ScanPlanningException", ["code"] = 500 },
        };
    }

    /// <summary><c>cancelPlanning</c>: stops a plan running here and forgets the plan either way.</summary>
    public async Task CancelAsync(TableIdentifier id, string planId, CancellationToken ct = default)
    {
        await FindAsync(id, planId, ct);
        await state.DeletePlanAsync(planId, ct);
        if (_running.TryGetValue(planId, out var cancel))
        {
            try { cancel.Cancel(); }
            catch (ObjectDisposedException) { /* it just finished */ }
        }
    }

    private async Task<PlanRecord> FindAsync(TableIdentifier id, string planId, CancellationToken ct) =>
        await state.LoadPlanAsync(planId, ct) is { } plan && plan.Table.Equals(id)
        && (plan.Finished is not { } done || Now() - done <= options.PlanLifetime)
            ? plan
            : throw new NoSuchPlanIdException($"No plan {planId} for {id}");

    /// <summary>At most once a minute per instance: forget plans past their lifetime.</summary>
    private async Task ExpireAsync()
    {
        var now = Clock();
        var last = Interlocked.Read(ref _lastSweepTicks);
        if (now.UtcTicks - last < TimeSpan.TicksPerMinute || Interlocked.CompareExchange(ref _lastSweepTicks, now.UtcTicks, last) != last) return;
        await state.ExpireAsync(DateTimeOffset.MinValue, now - options.PlanLifetime, now - 2 * options.PlanLifetime);
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
        IReadOnlyList<Snapshot>? appends = null;
        IReadOnlyList<ManifestFile> manifests;
        if (token.Incremental)
        {
            try { appends = TableScan.AppendSnapshots(m, token.StartSnapshotId, snapshot.SnapshotId); }
            catch (ArgumentException e) { throw new NoSuchPlanTaskException($"The plan task's snapshots of {id} no longer exist: {e.Message}"); }
            manifests = await TableScan.AppendManifestsAsync(table, appends, ct);
        }
        else manifests = await TableScan.ManifestsAsync(table, snapshot, ct);
        var byPath = manifests.ToDictionary(mf => mf.ManifestPath, StringComparer.Ordinal);
        var group = token.Manifests.Select(p => byPath.GetValueOrDefault(p) ?? throw new NoSuchPlanTaskException($"Manifest {p} is not in snapshot {snapshot.SnapshotId}")).ToList();
        var schema = token.UseSnapshotSchema ? TimeTravel.SchemaFor(m, snapshot) : m.CurrentSchema;
        return appends is null
            ? await TasksAsync(table, snapshot, manifests, group, schema, token.Filter, token.StatsFields?.ToHashSet(), ct)
            : await AppendTasksAsync(table, appends, group, schema, token.Filter, token.StatsFields?.ToHashSet(), ct);
    }

    private async Task<JsonObject> PlanCoreAsync(LoadedTable loaded, PlanRequest request, CancellationToken ct)
    {
        if (request.StartSnapshotId is not null || request.EndSnapshotId is not null) return await PlanIncrementalAsync(loaded, request, ct);
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

    /// <summary>
    /// An incremental scan (<c>start-snapshot-id</c> exclusive, <c>end-snapshot-id</c> inclusive), with Java's
    /// <c>IncrementalAppendScan</c> semantics: the data files that the <c>append</c> snapshots between them added, read
    /// from the manifests those snapshots wrote, pruned by the filter; overwrites and deletes in the range contribute
    /// nothing, and no delete files are attached. <c>end-snapshot-id</c> without a start covers every ancestor;
    /// a start without an end, a start that is not an ancestor of the end, or <c>snapshot-id</c> as well is a 400.
    /// </summary>
    private async Task<JsonObject> PlanIncrementalAsync(LoadedTable loaded, PlanRequest request, CancellationToken ct)
    {
        if (request.EndSnapshotId is not { } endId) throw new BadRequestException("end-snapshot-id is required when start-snapshot-id is given");
        if (request.SnapshotId is not null) throw new BadRequestException("snapshot-id cannot be combined with start-snapshot-id / end-snapshot-id");
        var m = loaded.Metadata;
        var end = m.SnapshotById(endId) ?? throw new BadRequestException($"Unknown end snapshot {endId}");
        IReadOnlyList<Snapshot> appends;
        try { appends = TableScan.AppendSnapshots(m, request.StartSnapshotId, endId); }
        catch (ArgumentException e) { throw new BadRequestException(e.Message); }

        var schema = request.UseSnapshotSchema ? TimeTravel.SchemaFor(m, end) : m.CurrentSchema;
        var names = new NameResolver(schema, request.CaseSensitive);
        var filter = names.Resolve(request.Filter);
        foreach (var column in request.Select ?? []) names.Field(column);
        var statsFields = request.StatsFields?.Select(n => names.Field(n).Id).ToList();
        var bound = Binder.Bind(schema, filter);

        var table = TableOf(loaded);
        var manifests = appends.Count == 0 ? [] : await TableScan.AppendManifestsAsync(table, appends, ct);
        var evaluators = new Dictionary<int, ManifestEvaluator>();
        var data = manifests.Where(mf =>
        {
            if (!evaluators.TryGetValue(mf.PartitionSpecId, out var eval))
                evaluators[mf.PartitionSpecId] = eval = new ManifestEvaluator(Projections.Inclusive(m.SpecById(mf.PartitionSpecId), schema, bound));
            return eval.MightMatch(mf.Partitions);
        }).ToList();

        var groups = Group(data, Math.Max(1, options.FilesPerPlanTask));
        if (groups.Count <= 1)
            return await AppendTasksAsync(table, appends, groups.FirstOrDefault() ?? [], schema, filter, statsFields?.ToHashSet(), ct);
        var tokens = groups.Select(g => (JsonNode)new PlanTaskToken(m.TableUuid, endId, g.Select(mf => mf.ManifestPath).ToList(), filter,
            request.UseSnapshotSchema, statsFields) { Incremental = true, StartSnapshotId = request.StartSnapshotId }.Encode()).ToArray();
        return new JsonObject { ["plan-tasks"] = new JsonArray(tokens) };
    }

    private async Task<JsonObject> AppendTasksAsync(Table table, IReadOnlyList<Snapshot> appends, IReadOnlyList<ManifestFile> group, Schema schema,
        Expression filter, ISet<int>? statsFields, CancellationToken ct)
    {
        var m = table.Metadata;
        var plan = group.Count == 0 ? null : await TableScan.PlanAppendManifestsAsync(table, appends, group, schema, filter, ct);
        var residual = filter is AlwaysTrue ? JsonValue.Create(true) : ExpressionJson.ToJson(filter);
        var tasks = new JsonArray();
        foreach (var task in plan?.Tasks ?? [])
            tasks.Add(new JsonObject
            {
                ["data-file"] = ContentFileJson.ToJson(task.File, m.SpecById(task.File.SpecId), schema, statsFields ?? new HashSet<int>()),
                ["residual-filter"] = residual.DeepClone(),
            });
        return new JsonObject { ["delete-files"] = new JsonArray(), ["file-scan-tasks"] = tasks };
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
    /// <summary>
    /// The table as the planner sees it: its FileIO reads only files inside the table's own directories and writes
    /// nothing. Commits already refuse manifest lists outside the table, but a manifest list inside it can still name
    /// another table's manifests; the planner reads those with the server's access, so it must refuse them itself.
    /// </summary>
    private Table TableOf(LoadedTable loaded) =>
        new(null!, loaded.Id, loaded.Metadata with { MetadataFileLocation = loaded.MetadataLocation },
            new TableConfinedFileIO(catalog.FileIO, loaded.Id, TableLocations.Prefixes(loaded.Metadata)));

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
    /// <summary>An incremental append scan from <see cref="StartSnapshotId"/> (exclusive) to <see cref="SnapshotId"/>.</summary>
    public bool Incremental { get; init; }

    public long? StartSnapshotId { get; init; }

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
        if (Incremental)
        {
            o["i"] = true;
            if (StartSnapshotId is { } start) o["st"] = start;
        }
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
                o["sf"]?.AsArray().Select(i => i!.GetValue<int>()).ToList())
            {
                Incremental = o["i"]?.GetValue<bool>() ?? false,
                StartSnapshotId = o["st"]?.GetValue<long>(),
            };
        }
        catch (Exception e) when (e is FormatException or System.Text.Json.JsonException or NullReferenceException or InvalidOperationException
                                       or InvalidDataException or ArgumentException)
        {
            throw new NoSuchPlanTaskException("Unknown or malformed plan task");
        }
    }
}

/// <summary>A read-only view of a FileIO that refuses any location outside one table's directories.</summary>
internal sealed class TableConfinedFileIO(IFileIO inner, TableIdentifier table, IReadOnlyList<string> prefixes) : IFileIO
{
    private string Check(string location) => CatalogService.IsOwnFile(location, prefixes)
        ? location
        // The location is not echoed: it names another table's file.
        : throw new BadRequestException($"Table {table} references a file outside the table's directories");

    public Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default) => inner.ReadAllAsync(Check(location), ct);
    public Task<Stream> OpenReadAsync(string location, CancellationToken ct = default) => inner.OpenReadAsync(Check(location), ct);
    public Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default) => inner.ReadRangeAsync(Check(location), offset, length, ct);
    public Task<long> LengthAsync(string location, CancellationToken ct = default) => inner.LengthAsync(Check(location), ct);
    public Task<bool> ExistsAsync(string location, CancellationToken ct = default) => inner.ExistsAsync(Check(location), ct);
    public Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default) =>
        throw new InvalidOperationException("Scan planning never writes");
    public Task DeleteAsync(string location, CancellationToken ct = default) =>
        throw new InvalidOperationException("Scan planning never deletes");
    public void Dispose() { } // the catalog owns the inner FileIO
}
