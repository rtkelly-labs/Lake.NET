using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;
using T = Iceberg.Net.Diagnostics.IcebergTelemetry.Tags;

namespace Iceberg.Net.Ecosystem.Tests;

/// <summary>
/// Spans and metrics from <see cref="IcebergTelemetry"/>, observed the way OpenTelemetry observes them: through an
/// <see cref="ActivityListener"/> and a <see cref="MeterListener"/>. Each test uses its own namespace, and listeners
/// keep only what carries that table, since other tests in the process emit too.
/// </summary>
public sealed class TelemetryTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
    private readonly string _ns = "telemetry_" + Guid.NewGuid().ToString("N")[..8];
    private readonly FaultyCatalog _catalog;
    private readonly ActivityListener _activities;
    private readonly MeterListener _meters;
    private readonly ConcurrentQueue<Activity> _stopped = new();
    private readonly ConcurrentQueue<(string Instrument, double Value, Dictionary<string, object?> Tags)> _measurements = new();

    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)], identifierFieldIds: [1]);

    /// <summary>Fails the next pointer swaps on demand, as a lost race or an ambiguous error.</summary>
    private sealed class FaultyCatalog(string warehouse, string name) : InMemoryCatalog(warehouse, name: name)
    {
        public Queue<Exception> SwapFailures { get; } = new();

        protected override Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct) =>
            SwapFailures.TryDequeue(out var failure) ? throw failure : base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
    }

    public TelemetryTests()
    {
        _catalog = new FaultyCatalog(_warehouse, _ns);
        _activities = new ActivityListener
        {
            ShouldListenTo = s => s.Name == IcebergTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a => { if (Mine(a.GetTagItem(T.Table)) || Equals(a.GetTagItem(T.CatalogName), _ns)) _stopped.Enqueue(a); },
        };
        ActivitySource.AddActivityListener(_activities);
        _meters = new MeterListener
        {
            InstrumentPublished = (instrument, listener) => { if (instrument.Meter.Name == IcebergTelemetry.Name) listener.EnableMeasurementEvents(instrument); },
        };
        _meters.SetMeasurementEventCallback<long>((i, v, tags, _) => Record(i, v, tags));
        _meters.SetMeasurementEventCallback<double>((i, v, tags, _) => Record(i, v, tags));
        _meters.Start();
        _catalog.CreateNamespaceAsync(Namespace.Of(_ns)).GetAwaiter().GetResult();
    }

    private bool Mine(object? table) => table is string s && s.StartsWith(_ns + ".", StringComparison.Ordinal);

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var map = new Dictionary<string, object?>();
        foreach (var t in tags) map[t.Key] = t.Value;
        if (Mine(map.GetValueOrDefault(T.Table)) || Equals(map.GetValueOrDefault(T.CatalogName), _ns)) _measurements.Enqueue((instrument.Name, value, map));
    }

    public void Dispose()
    {
        _activities.Dispose();
        _meters.Dispose();
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private Task<Table> CreateAsync(string name, IReadOnlyDictionary<string, string>? properties = null, int formatVersion = 2) =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of(_ns, name), Schema, Properties: properties, FormatVersion: formatVersion));

    private List<Activity> Spans(string name) => _stopped.Where(a => a.OperationName == name).ToList();

    private double Sum(string instrument, string? table = null, params (string Key, object Value)[] where) =>
        _measurements.Where(m => m.Instrument == instrument && (table is null || Equals(m.Tags.GetValueOrDefault(T.Table), table))
            && where.All(w => Equals(m.Tags.GetValueOrDefault(w.Key), w.Value))).Sum(m => m.Value);

    [Fact]
    public async Task Scan_spans_and_metrics_report_planning_pruning_bytes_and_rows()
    {
        var t = await CreateAsync("scan");
        for (var batch = 0; batch < 3; batch++)
            await TableMutations.InsertAsync(t, Enumerable.Range(batch * 10, 10).Select(i => new object?[] { (long)i, $"r{i}" }).ToList());
        await TableMutations.UpsertAsync(t, [[25L, "updated"]], strategy: DeleteStrategy.PositionDeletes);
        _stopped.Clear();
        _measurements.Clear();

        // ids 20..29 live in the third data file (and the upsert's file); metrics prune the other two.
        var rows = await TableScan.ReadAsync(t, Expr.GreaterThanOrEqual("id", 20L));
        Assert.Equal(10, rows.Count);

        var table = t.Identifier.ToString();
        var scan = Assert.Single(Spans("iceberg.scan"));
        var plan = Assert.Single(Spans("iceberg.scan.plan"));
        Assert.Equal(scan.SpanId, plan.ParentSpanId);
        Assert.Equal(table, plan.GetTagItem(T.Table));
        Assert.Equal(t.Metadata.CurrentSnapshotId, plan.GetTagItem(T.SnapshotId));
        Assert.Equal(2, plan.GetTagItem(T.FilesPlanned));
        Assert.Equal(2, plan.GetTagItem(T.FilesPrunedByMetrics));
        Assert.Equal(0, plan.GetTagItem(T.FilesPrunedByPartition));

        var planned = await TableScan.PlanFilesAsync(t, Expr.GreaterThanOrEqual("id", 20L));
        var bytes = planned.Sum(task => task.File.FileSizeInBytes);
        Assert.Equal(2L, scan.GetTagItem(T.FilesRead));
        Assert.Equal(bytes, scan.GetTagItem(T.BytesRead));
        Assert.Equal(10L, scan.GetTagItem(T.RowsReturned));
        Assert.Equal(1L, scan.GetTagItem(T.RowsDeleted)); // the old row 25, removed by the position delete

        Assert.Equal(2 * 2, Sum("iceberg.scan.files.planned", table)); // the scan, then PlanFilesAsync above
        Assert.Equal(2 * 2, Sum("iceberg.scan.files.pruned", table, (T.PruneReason, "metrics")));
        Assert.Equal(bytes, Sum("iceberg.scan.bytes", table));
        Assert.Equal(10, Sum("iceberg.scan.rows", table, (T.RowKind, "returned")));
        Assert.Equal(1, Sum("iceberg.scan.rows", table, (T.RowKind, "deleted")));
        Assert.Equal(2, _measurements.Count(m => m.Instrument == "iceberg.scan.planning.duration" && Equals(m.Tags[T.Table], table)));
    }

    [Fact]
    public async Task Commit_span_counts_attempts_and_retries_after_a_conflict()
    {
        var t = await CreateAsync("retry");
        _stopped.Clear();
        _measurements.Clear();
        _catalog.SwapFailures.Enqueue(new CommitFailedException("lost the race"));

        var result = await TableMutations.InsertAsync(t, [[1L, "a"]]);

        var commit = Assert.Single(Spans("iceberg.commit"));
        Assert.Equal(ActivityStatusCode.Ok, commit.Status);
        Assert.Equal(IcebergTelemetry.Outcomes.Success, commit.GetTagItem(T.Outcome));
        Assert.Equal(2, commit.GetTagItem(T.CommitAttempts));
        Assert.Equal(1, commit.GetTagItem(T.CommitRetries));
        Assert.Equal(Operations.Append, commit.GetTagItem(T.Operation));
        Assert.Equal(t.Metadata.CurrentSnapshotId, commit.GetTagItem(T.SnapshotId));
        var retry = Assert.Single(commit.Events, e => e.Name == "iceberg.commit.retry");
        Assert.Contains(retry.Tags, kv => kv.Key == "iceberg.commit.attempt" && Equals(kv.Value, 1));

        // Each attempt's catalog commit is a child client span: the first lost, the second won.
        var catalogCommits = Spans("iceberg.catalog.commit_table").Where(a => a.ParentSpanId == commit.SpanId).ToList();
        Assert.Equal(2, catalogCommits.Count);
        Assert.All(catalogCommits, a => Assert.Equal(ActivityKind.Client, a.Kind));
        Assert.Equal([ActivityStatusCode.Error, ActivityStatusCode.Ok], catalogCommits.OrderBy(a => a.StartTimeUtc).Select(a => a.Status));
        Assert.Contains(Spans("iceberg.catalog.load_table"), a => a.ParentSpanId == commit.SpanId); // the refresh between attempts

        var table = t.Identifier.ToString();
        Assert.Equal(1, Sum("iceberg.commit.retries", table));
        Assert.Equal(2, Sum("iceberg.commit.attempts", table, (T.Outcome, IcebergTelemetry.Outcomes.Success)));
        Assert.Single(_measurements, m => m.Instrument == "iceberg.commit.duration" && Equals(m.Tags[T.Table], table) && Equals(m.Tags[T.Outcome], "success"));
        Assert.Equal(1, _measurements.Count(m => m.Instrument == "iceberg.catalog.duration" && Equals(m.Tags[T.CatalogOperation], "commit_table") && Equals(m.Tags[T.Outcome], "error")));
        Assert.NotNull(result);
    }

    [Fact]
    public async Task A_snapshot_producer_on_its_own_owns_the_commit_span()
    {
        var t = await CreateAsync("producer", new Dictionary<string, string> { ["commit.retry.min-wait-ms"] = "1" });
        var files = await TableWriter.WriteAsync(t, [[1L, "a"]]);
        _stopped.Clear();
        _catalog.SwapFailures.Enqueue(new CommitFailedException("lost the race"));

        var snapshot = await SnapshotProducer.Append(t).AddFiles(files).CommitAsync();

        var commit = Assert.Single(Spans("iceberg.commit"));
        Assert.Equal(snapshot.SnapshotId, commit.GetTagItem(T.SnapshotId));
        Assert.Equal(Operations.Append, commit.GetTagItem(T.Operation));
        Assert.Equal(2, commit.GetTagItem(T.CommitAttempts));
        Assert.Contains(commit.Events, e => e.Name == "iceberg.commit.retry" && e.Tags.Any(kv => kv.Key == "iceberg.commit.backoff_ms"));
    }

    [Fact]
    public async Task Exhausted_retries_record_a_conflict_outcome()
    {
        var t = await CreateAsync("conflict", new Dictionary<string, string> { ["commit.retry.num-retries"] = "1", ["commit.retry.min-wait-ms"] = "1" });
        _stopped.Clear();
        _measurements.Clear();
        _catalog.SwapFailures.Enqueue(new CommitFailedException("lost"));
        _catalog.SwapFailures.Enqueue(new CommitFailedException("lost again"));

        await Assert.ThrowsAsync<CommitFailedException>(() => TableMutations.InsertAsync(t, [[1L, "a"]]));

        var commit = Assert.Single(Spans("iceberg.commit"));
        Assert.Equal(ActivityStatusCode.Error, commit.Status);
        Assert.Equal(IcebergTelemetry.Outcomes.Conflict, commit.GetTagItem(T.Outcome));
        Assert.Equal(typeof(CommitFailedException).FullName, commit.GetTagItem(T.ErrorType));
        Assert.Equal(2, commit.GetTagItem(T.CommitAttempts));
        Assert.Single(_measurements, m => m.Instrument == "iceberg.commit.duration" && Equals(m.Tags[T.Outcome], "conflict"));
    }

    [Fact]
    public async Task Unknown_commit_state_is_its_own_outcome()
    {
        var t = await CreateAsync("unknown");
        _stopped.Clear();
        _catalog.SwapFailures.Enqueue(new TimeoutException("socket timeout"));

        await Assert.ThrowsAsync<CommitStateUnknownException>(() => TableMutations.InsertAsync(t, [[1L, "a"]]));

        var commit = Assert.Single(Spans("iceberg.commit"));
        Assert.Equal(IcebergTelemetry.Outcomes.Unknown, commit.GetTagItem(T.Outcome));
        Assert.Equal(1, commit.GetTagItem(T.CommitAttempts));
    }

    [Fact]
    public async Task Catalog_calls_are_client_spans_with_catalog_tags()
    {
        var t = await CreateAsync("catalog");
        await _catalog.LoadTableAsync(t.Identifier);
        await Assert.ThrowsAsync<NoSuchTableException>(() => _catalog.LoadTableAsync(TableIdentifier.Of(_ns, "missing")));

        var create = Assert.Single(Spans("iceberg.catalog.create_table"));
        Assert.Equal(ActivityKind.Client, create.Kind);
        Assert.Equal(_ns, create.GetTagItem(T.CatalogName));
        Assert.Equal(nameof(FaultyCatalog), create.GetTagItem(T.CatalogType));
        var loads = Spans("iceberg.catalog.load_table");
        Assert.Contains(loads, a => a.Status == ActivityStatusCode.Ok && Equals(a.GetTagItem(T.Table), t.Identifier.ToString()));
        Assert.Contains(loads, a => a.Status == ActivityStatusCode.Error && Equals(a.GetTagItem(T.Table), $"{_ns}.missing"));
    }

    [Fact]
    public async Task Without_a_listener_nothing_is_recorded_and_scans_still_work()
    {
        _activities.Dispose();
        _meters.Dispose();
        var t = await CreateAsync("quiet");
        await TableMutations.InsertAsync(t, [[1L, "a"]]);
        Assert.Single(await TableScan.ReadAllAsync(t));
        Assert.Empty(_stopped);
        Assert.Empty(_measurements);
    }
}
