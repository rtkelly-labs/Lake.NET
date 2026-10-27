using System.Globalization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Table statistics as Puffin files: one <c>apache-datasketches-theta-v1</c> blob per column, carrying the
/// NDV estimate in its <c>ndv</c> property — the same shape Spark's <c>compute_table_stats</c> procedure
/// writes and Trino reads for cost-based optimisation.
/// </summary>
public static class PuffinStatistics
{
    public const string NdvProperty = "ndv";

    /// <summary>
    /// Compute NDV sketches over the live rows of the current snapshot (deletes applied) for the given
    /// top-level primitive columns (default: all), write them to a Puffin file and register it with
    /// <c>set-statistics</c>. Returns the registered statistics file.
    /// </summary>
    public static async Task<StatisticsFile> ComputeNdvAsync(Table table, IEnumerable<string>? columns = null, CancellationToken ct = default)
    {
        var m = table.Metadata;
        var snapshot = m.CurrentSnapshot ?? throw new InvalidOperationException("Cannot compute statistics for a table with no snapshot");
        var schema = m.CurrentSchema;
        var selected = (columns?.Select(c => schema.FindField(c) ?? throw new ArgumentException($"Unknown column {c}")) ?? schema.Columns)
            .Where(f => f.Type.IsPrimitive && schema.Columns.Contains(f))
            .ToList();
        var positions = selected.Select(f => schema.Columns.ToList().IndexOf(f)).ToArray();
        var sketches = selected.Select(_ => new ThetaSketch()).ToArray();

        foreach (var row in await TableScan.ReadAllAsync(table, snapshot.SnapshotId, ct))
        {
            for (var i = 0; i < selected.Count; i++)
            {
                // Iceberg hashes each value's single-value binary serialization (as Spark's ThetaSketchAgg does).
                if (row[positions[i]] is { } value) sketches[i].Update(Conversions.ToBytes(selected[i].Type, value));
            }
        }

        var written = IcebergPuffinFile.Write(selected.Select((f, i) => new PuffinBlob(
            PuffinBlobTypes.ThetaSketchV1,
            [f.Id],
            sketches[i].Serialize(),
            snapshot.SnapshotId,
            snapshot.SequenceNumber,
            Properties: new Dictionary<string, string> { [NdvProperty] = ((long)Math.Round(sketches[i].Estimate)).ToString(CultureInfo.InvariantCulture) })));

        var metaDir = m.Property("write.metadata.path", $"{m.Location}/metadata").TrimEnd('/');
        var location = $"{metaDir}/{snapshot.SnapshotId}-{Guid.NewGuid()}.stats";
        await table.IO.WriteAsync(location, written.Bytes, overwrite: false, ct);

        var stats = new StatisticsFile(
            snapshot.SnapshotId,
            location,
            written.Bytes.Length,
            written.FooterSize,
            written.Blobs.Select(b => new BlobMetadata(b.Type, b.SnapshotId, b.SequenceNumber, b.Fields, b.Properties)).ToList());
        await table.CommitAsync([new UpdateRequirement.AssertTableUuid(m.TableUuid)], [new MetadataUpdate.SetStatistics(stats)], ct);
        return stats;
    }

    /// <summary>Read the NDV estimates of a registered statistics file, keyed by field ID.</summary>
    public static async Task<IReadOnlyDictionary<int, double>> ReadNdvAsync(Table table, StatisticsFile stats, CancellationToken ct = default)
    {
        var puffin = PuffinFile.Read(await table.IO.ReadAllAsync(stats.StatisticsPath, ct));
        return puffin.Blobs.Where(b => b.Type == PuffinBlobTypes.ThetaSketchV1 && b.Fields.Count == 1)
            .ToDictionary(b => b.Fields[0], b => ThetaSketch.Deserialize(puffin.Payload(b)).Estimate);
    }
}
