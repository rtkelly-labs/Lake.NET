using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Diagnostics;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;
using T = Iceberg.Net.Diagnostics.IcebergTelemetry.Tags;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Deterministic chaos: correlate injection, propagation, telemetry and durable state in one experiment.</summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("Metastore Tables")]
public sealed class ChaosPropagationTests
{
    private sealed record Fault(string Boundary, string Type, string TraceId, string SpanId, DateTimeOffset Timestamp);
    private sealed record Measurement(string Instrument, double Value, Dictionary<string, object?> Tags);

    private sealed class ChaosCatalog(string warehouse, IFileIO io, Action<string, Exception> observe) : InMemoryCatalog(warehouse, io)
    {
        public string? Experiment { get; set; }
        private int _recoveryFailures;

        protected override Task<string?> CurrentMetadataLocationAsync(TableIdentifier id, CancellationToken ct)
        {
            if (_recoveryFailures > 0)
            {
                _recoveryFailures--;
                var error = new IOException("chaos: recovery pointer read failed");
                observe("catalog.recovery-read", error);
                throw error;
            }
            return base.CurrentMetadataLocationAsync(id, ct);
        }

        protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
        {
            if (Experiment is not { } experiment)
            {
                await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
                return;
            }
            Experiment = null; // one scheduled fault; subsequent retries use normal semantics
            if (experiment is "metadata-before" or "metadata-after")
                throw new InvalidOperationException("Metadata write failure should prevent the pointer swap");
            if (experiment is "timeout-after" or "service-error-after" or "recovery-read")
                await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
            Exception error = experiment switch
            {
                "conflict" => new CommitFailedException("chaos: definite CAS conflict"),
                "service-error-after" or "recovery-read" => new CommitStateUnknownException("chaos: error response after applying swap") { ServerResponded = true },
                "cancelled" => new OperationCanceledException("chaos: cancellation at swap boundary"),
                _ => new IOException("chaos: lost connection at swap boundary"),
            };
            observe("catalog.swap", error);
            _recoveryFailures = experiment == "recovery-read" ? 4 : 0;
            throw error;
        }
    }

    public static TheoryData<string, int> Experiments => new()
    {
        { "metadata-before", 2 }, { "metadata-before", 3 },
        { "metadata-after", 2 }, { "metadata-after", 3 },
        { "conflict", 2 }, { "conflict", 3 },
        { "timeout-before", 2 }, { "timeout-before", 3 },
        { "timeout-after", 2 }, { "timeout-after", 3 },
        { "service-error-after", 2 }, { "service-error-after", 3 },
        { "recovery-read", 2 }, { "recovery-read", 3 },
        { "cancelled", 2 }, { "cancelled", 3 },
    };

    [Theory]
    [MemberData(nameof(Experiments))]
    public async Task Fault_propagation_preserves_classification_causality_and_table_state(string experiment, int formatVersion)
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (!File.Exists(Path.Combine(root.FullName, "Iceberg.Net.slnx")))
            root = root.Parent ?? throw new InvalidOperationException("Repository root not found");
        var run = Guid.NewGuid().ToString("N");
        var output = Path.Combine(root.FullName, "temp", "chaos", $"{experiment}-v{formatVersion}-{run}");
        var warehouse = Path.Combine(output, "warehouse");
        Directory.CreateDirectory(output);
        var faults = new List<Fault>();
        var spans = new ConcurrentQueue<Activity>();
        var metrics = new ConcurrentQueue<Measurement>();
        var injected = new List<Exception>();
        var io = new FaultInjectingFileIO(new LocalFileIO());
        void Observe(string boundary, Exception error)
        {
            injected.Add(error);
            var current = Activity.Current;
            faults.Add(new Fault(boundary, error.GetType().FullName!, current?.TraceId.ToString() ?? "", current?.SpanId.ToString() ?? "", DateTimeOffset.UtcNow));
            current?.AddEvent(new ActivityEvent("chaos.inject", tags: new ActivityTagsCollection
            {
                ["chaos.experiment"] = experiment,
                ["chaos.run"] = run,
                ["chaos.boundary"] = boundary,
                [T.ErrorType] = error.GetType().FullName,
            }));
        }
        using var catalog = new ChaosCatalog(warehouse, io, Observe);
        var ns = Namespace.Of("chaos_" + run);
        await catalog.CreateNamespaceAsync(ns);
        var table = await catalog.CreateTableAsync(new CreateTableRequest(new TableIdentifier(ns, "events"),
            new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long)]),
            Properties: new Dictionary<string, string> { ["commit.retry.min-wait-ms"] = "0", ["commit.status-check.min-wait-ms"] = "0" }, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(table, [[0L]]);
        io.Reset();
        var tableName = table.Identifier.ToString();
        using var pipeline = new Activity("chaos.pipeline").SetIdFormat(ActivityIdFormat.W3C).Start();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == IcebergTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => { if (activity.TraceId == pipeline.TraceId) spans.Enqueue(activity); },
        };
        ActivitySource.AddActivityListener(listener);
        using var meter = new MeterListener
        {
            InstrumentPublished = (instrument, target) => { if (instrument.Meter.Name == IcebergTelemetry.Name) target.EnableMeasurementEvents(instrument); },
            MeasurementsCompleted = (_, _) => { },
        };
        void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var map = new Dictionary<string, object?>();
            foreach (var tag in tags) map[tag.Key] = tag.Value;
            if (Equals(map.GetValueOrDefault(T.Table), tableName)) metrics.Enqueue(new Measurement(instrument.Name, value, map));
        }
        meter.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        meter.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
        meter.Start();
        catalog.Experiment = experiment;
        if (experiment is "metadata-before" or "metadata-after")
        {
            io.PathFilter = path => path.EndsWith(".metadata.json", StringComparison.Ordinal);
            io.Reset(1, experiment == "metadata-before" ? FaultKind.FailBeforeWrite : FaultKind.FailAfterWrite);
            io.OnFault = (_, kind, error) => Observe("storage." + kind, error);
        }

        Exception? callerError = null;
        long[]? rows = null;
        string? verificationError = null;
        var passed = false;
        try
        {
            callerError = await Xunit.Record.ExceptionAsync(() => TableMutations.InsertAsync(table, [[1L]]));
            // Capture the operation's spans before the independent verification reads begin.
            var commit = Assert.Single(spans, span => span.OperationName == "iceberg.commit");
            var expectedOutcome = experiment switch
            {
                "conflict" or "service-error-after" or "timeout-after" => IcebergTelemetry.Outcomes.Success,
                "timeout-before" or "recovery-read" or "cancelled" => IcebergTelemetry.Outcomes.Unknown,
                _ => IcebergTelemetry.Outcomes.Error,
            };
            Assert.Equal(expectedOutcome, commit.GetTagItem(T.Outcome));
            Assert.Equal(expectedOutcome == "success" ? ActivityStatusCode.Ok : ActivityStatusCode.Error, commit.Status);
            Assert.Equal(experiment == "conflict" ? 2 : 1, commit.GetTagItem(T.CommitAttempts));
            Assert.Equal(experiment == "conflict" ? 1 : 0, commit.GetTagItem(T.CommitRetries));
            Assert.Equal(experiment == "conflict" ? 1 : 0, commit.Events.Count(item => item.Name == "iceberg.commit.retry"));
            var checks = commit.Events.Where(item => item.Name == "iceberg.commit.status-check").ToList();
            var hasStatusCheck = experiment is "timeout-before" or "timeout-after" or "service-error-after" or "recovery-read" or "cancelled";
            Assert.Equal(hasStatusCheck ? experiment == "recovery-read" ? 4 : 1 : 0, checks.Count);
            if (hasStatusCheck)
                Assert.All(checks, item => Assert.Contains(item.Tags, tag => tag.Key == "iceberg.commit.status-check.result"
                    && Equals(tag.Value, expectedOutcome == "success" ? "success" : "unknown")));
            Assert.Equal(pipeline.SpanId, commit.ParentSpanId);
            Assert.Equal(experiment == "conflict" ? 2 : 1, spans.Count(span => span.OperationName == "iceberg.catalog.commit_table"));
            Assert.NotEmpty(faults);
            foreach (var fault in faults)
            {
                Assert.Equal(pipeline.TraceId.ToString(), fault.TraceId);
                var boundary = Assert.Single(spans, span => span.SpanId.ToString() == fault.SpanId);
                Assert.Equal(commit.SpanId, boundary.ParentSpanId);
                Assert.Equal(ActivityStatusCode.Error, boundary.Status);
                Assert.Contains(boundary.Events, item => item.Name == "chaos.inject");
            }
            var attempts = Assert.Single(metrics, item => item.Instrument == "iceberg.commit.attempts");
            Assert.Equal(experiment == "conflict" ? 2d : 1d, attempts.Value);
            Assert.Equal(expectedOutcome, attempts.Tags[T.Outcome]);
            Assert.Single(metrics, item => item.Instrument == "iceberg.commit.duration");
            Assert.Equal(experiment == "conflict" ? 1d : 0d, metrics.Where(item => item.Instrument == "iceberg.commit.retries").Sum(item => item.Value));
            if (expectedOutcome == "success") Assert.Null(callerError);
            else
            {
                Assert.NotNull(callerError);
                Assert.Equal(callerError.GetType().FullName, commit.GetTagItem(T.ErrorType));
                if (expectedOutcome == "unknown") Assert.IsType<CommitStateUnknownException>(callerError);
                if (experiment == "recovery-read") Assert.IsType<IOException>(((CommitStateUnknownException)callerError).ResolutionFailure);
                // Wrapping must preserve the original injection, even when recovery itself also fails.
                Assert.Contains(ExceptionChain(callerError), error => ReferenceEquals(error, injected[0]));
            }

            catalog.Experiment = null;
            io.FailAtWrite = null;
            var fresh = await catalog.LoadTableAsync(table.Identifier);
            rows = (await TableScan.ReadAllAsync(fresh)).Select(row => (long)row[0]!).Order().ToArray();
            var applied = experiment is "conflict" or "timeout-after" or "service-error-after" or "recovery-read";
            Assert.Equal(applied ? new long[] { 0, 1 } : [0L], rows);
            Assert.Equal(applied ? 2 : 1, fresh.Metadata.Snapshots.Count);
            await TableValidator.AssertValidAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
            if (expectedOutcome is "unknown" or "cancelled")
                Assert.All(io.Written, path => Assert.True(File.Exists(path), $"Potentially committed artifact deleted: {path}"));
            passed = true;
        }
        catch (Exception error)
        {
            verificationError = error.ToString();
            throw;
        }
        finally
        {
            var report = new
            {
                experiment, run, formatVersion, traceId = pipeline.TraceId.ToString(), pipelineSpanId = pipeline.SpanId.ToString(), passed,
                faults, callerExceptions = callerError is null ? [] : ExceptionChain(callerError).Select(error => error.GetType().FullName).ToArray(),
                resolutionFailure = (callerError as CommitStateUnknownException)?.ResolutionFailure?.GetType().FullName,
                rows, verificationError,
                spans = spans.Select(span => new
                {
                    name = span.OperationName, traceId = span.TraceId.ToString(), spanId = span.SpanId.ToString(), parentSpanId = span.ParentSpanId.ToString(),
                    status = span.Status.ToString(), startedUtc = span.StartTimeUtc, durationMs = span.Duration.TotalMilliseconds, tags = span.TagObjects.ToDictionary(),
                    events = span.Events.Select(item => new { name = item.Name, timestamp = item.Timestamp, tags = item.Tags.ToDictionary() }),
                }),
                measurements = metrics.ToArray(),
            };
            await File.WriteAllTextAsync(Path.Combine(output, "report.json"), JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true }));
            if (Directory.Exists(warehouse)) Directory.Delete(warehouse, recursive: true);
        }
    }

    private static IEnumerable<Exception> ExceptionChain(Exception error)
    {
        for (Exception? current = error; current is not null; current = current.InnerException) yield return current;
    }
}
