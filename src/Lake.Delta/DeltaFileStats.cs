using System.Text.Json.Nodes;

namespace Lake.Delta;

/// <summary>
/// An <c>add</c>'s <c>stats</c> (PROTOCOL.md "Per-file Statistics"). <see cref="MinValues"/>, <see cref="MaxValues"/>
/// and <see cref="NullCount"/> are nested like the schema and keyed by physical column names; strings may be truncated
/// prefixes and timestamps truncated to milliseconds, so they are bounds, never values.
/// </summary>
public sealed class DeltaFileStats
{
    private DeltaFileStats(long? numRecords, bool tightBounds, JsonObject? min, JsonObject? max, JsonObject? nullCount)
    {
        NumRecords = numRecords;
        TightBounds = tightBounds;
        MinValues = min;
        MaxValues = max;
        NullCount = nullCount;
    }

    /// <summary>Physical rows in the file (deletion vectors not applied). Exact when the file has a DV.</summary>
    public long? NumRecords { get; }

    /// <summary>
    /// True (the default when absent): the bounds are exact over the file's valid rows. False: they are only bounds,
    /// and a <see cref="NullCount"/> carries information only when it is 0 or equals <see cref="NumRecords"/>.
    /// </summary>
    public bool TightBounds { get; }

    public JsonObject? MinValues { get; }
    public JsonObject? MaxValues { get; }
    public JsonObject? NullCount { get; }

    public static DeltaFileStats Parse(string json) => Corruption.Guard("add.stats", () =>
    {
        var o = JsonNode.Parse(json, documentOptions: Json.DocumentOptions) as JsonObject
            ?? throw new InvalidDataException("add.stats is not a JSON object");
        var numRecords = Json.Long(o, "numRecords", "stats");
        if (numRecords < 0) throw new InvalidDataException("stats.numRecords is negative");
        return new DeltaFileStats(numRecords, Json.Bool(o, "tightBounds", "stats") ?? true,
            Json.Obj(o, "minValues", "stats"), Json.Obj(o, "maxValues", "stats"), Json.Obj(o, "nullCount", "stats"));
    });

    /// <summary>The minimum recorded for the column at <paramref name="physicalPath"/>, or null when none is recorded.</summary>
    public JsonNode? Min(params string[] physicalPath) => Lookup(MinValues, physicalPath);

    public JsonNode? Max(params string[] physicalPath) => Lookup(MaxValues, physicalPath);

    public long? Nulls(params string[] physicalPath) =>
        Lookup(NullCount, physicalPath) is JsonValue v && Json.TryGetLong(v, out var n) ? n : null;

    private static JsonNode? Lookup(JsonObject? root, string[] path)
    {
        JsonNode? node = root;
        foreach (var part in path)
        {
            if (node is not JsonObject o || !o.TryGetPropertyValue(part, out node)) return null;
        }
        return node;
    }
}
