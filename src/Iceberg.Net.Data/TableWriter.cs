using System.Globalization;
using Iceberg.Net.Avro;
using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Puffin;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;
/// <summary>Writes rows into Parquet files, one per partition, under <c>{location}/data/</c>.</summary>
public static class TableWriter
{
    public static Task<IReadOnlyList<DataFile>> WriteAsync(Table table, IReadOnlyList<object?[]> rows, CancellationToken ct = default) =>
        WriteAsync(table, rows, null, ct);

    /// <summary>
    /// Write rows; with <paramref name="lineage"/> (v3), each row's <c>_row_id</c> and
    /// <c>_last_updated_sequence_number</c> are stored explicitly so rows carried through a rewrite keep their identity.
    /// </summary>
    public static async Task<IReadOnlyList<DataFile>> WriteAsync(Table table, IReadOnlyList<object?[]> rows, IReadOnlyList<(long? RowId, long? Seq)>? lineage, CancellationToken ct = default)
    {
        if (lineage is not null)
        {
            if (lineage.Count != rows.Count) throw new ArgumentException("One lineage entry per row is required");
            if (table.Metadata.FormatVersion < 3) lineage = null; // no row lineage before v3
        }
        var schema = table.Schema;
        var spec = table.Spec;
        if (spec.Fields.Any(f => f.Transform is Transforms.UnknownTransform))
            throw new NotSupportedException("Cannot write with a partition spec containing unknown transforms");

        // Partition sources may be fields inside structs.
        var accessors = spec.Fields.SelectMany(f => f.AllSourceIds).Distinct().ToDictionary(id => id, schema.Accessor);
        var groups = rows.GroupBy(r => spec.Partition(schema, id => accessors[id](r)), PartitionComparer.Instance);

        var dataDir = table.Metadata.Property("write.data.path", $"{table.Location}/data").TrimEnd('/');
        // write.format.default (spec "Table Properties"): parquet unless the table asks for avro; orc is not supported.
        var format = table.Metadata.Property("write.format.default", "parquet").ToLowerInvariant();
        var avro = format switch { "parquet" => false, "avro" => true, _ => throw new NotSupportedException($"write.format.default={format} is not supported (parquet, avro)") };
        var index = new Dictionary<object?[], int>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < rows.Count; i++) index[rows[i]] = i;
        var files = new List<DataFile>();
        foreach (var g in groups)
        {
            var partitionPath = spec.IsUnpartitioned ? "" : spec.PartitionPath(schema, g.Key) + "/";
            var location = $"{dataDir}/{partitionPath}{Guid.NewGuid():N}.{(avro ? "avro" : "parquet")}";
            var groupRows = g.ToList();
            // Files are written in the table's default sort order (spec "Sorting"), and say so in sort_order_id.
            var order = table.Metadata.DefaultSortOrder;
            if (!order.IsUnsorted) groupRows = groupRows.OrderBy(r => r, new RowSorter(schema, order)).ToList(); // stable: ties keep input order
            DataFile written;
            var fileSchema = lineage is null ? schema : RowLineage.WithLineage(schema);
            var fileRows = lineage is null ? groupRows : groupRows.Select(r => (object?[])[.. r, lineage[index[r]].RowId, lineage[index[r]].Seq]).ToList();
            written = avro
                ? await AvroDataFiles.WriteAsync(table.IO, location, fileSchema, spec.SpecId, g.Key, fileRows, ct)
                : await ParquetFiles.WriteAsync(table.IO, location, fileSchema, spec.SpecId, g.Key, fileRows, ct);
            files.Add(order.IsUnsorted ? written : written with { SortOrderId = order.OrderId });
        }
        return files;
    }

    private sealed class PartitionComparer : IEqualityComparer<object?[]>
    {
        public static readonly PartitionComparer Instance = new();
        public bool Equals(object?[]? a, object?[]? b) => a!.Length == b!.Length && a.Zip(b).All(p => Equals(p.First, p.Second));
        public int GetHashCode(object?[] o) => o.Aggregate(17, (h, v) => HashCode.Combine(h, v));
    }
}
