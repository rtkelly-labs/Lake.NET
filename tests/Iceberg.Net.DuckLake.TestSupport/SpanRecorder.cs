using System.Collections.Concurrent;
using System.Diagnostics;

namespace Iceberg.Net.DuckLake.TestSupport;

/// <summary>
/// The DuckLake spans of one lake (told apart by <see cref="DuckLakeTelemetry.Tags.DataPath"/>, since tests run in
/// parallel and listeners are process-wide), for assertions on catalog round trips.
/// </summary>
public sealed class SpanRecorder : IDisposable
{
    private readonly ActivityListener _listener;
    private readonly ConcurrentQueue<Activity> _spans = new();

    public SpanRecorder(TestLake lake)
    {
        var dataPath = lake.DataPath;
        _listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == DuckLakeTelemetry.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                if (a.GetTagItem(DuckLakeTelemetry.Tags.DataPath) as string == dataPath) _spans.Enqueue(a);
            },
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public IReadOnlyList<Activity> Spans => [.. _spans];

    public void Clear() => _spans.Clear();

    /// <summary>The catalog commands of the last finished span named <paramref name="operation"/>.</summary>
    public long Commands(string operation) =>
        Spans.LastOrDefault(s => s.OperationName == operation) is { } span
            ? (long)span.GetTagItem(DuckLakeTelemetry.Tags.CatalogCommands)!
            : throw new InvalidOperationException($"No {operation} span; recorded: {string.Join(", ", Spans.Select(s => s.OperationName))}");

    public void Dispose() => _listener.Dispose();
}
