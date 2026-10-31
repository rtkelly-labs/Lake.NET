using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// A <c>map_by_name</c> column mapping (spec <c>ducklake_column_mapping</c>, <c>ducklake_name_mapping</c>): the file's
/// field name for each column ID, and which columns come from the file's Hive partition path instead.
/// </summary>
internal sealed class DuckLakeNameMap
{
    private readonly Dictionary<long, (string Source, bool IsPartition)> _byColumn;

    public DuckLakeNameMap(long mappingId, IEnumerable<NameMappingEntity> rows)
    {
        MappingId = mappingId;
        _byColumn = [];
        foreach (var r in rows)
            _byColumn[r.TargetFieldId] = (r.SourceName ?? throw new InvalidDataException($"Name mapping {mappingId}: column {r.ColumnId} has no source_name"), r.IsPartition == true);
    }

    public long MappingId { get; }

    internal static async Task<Dictionary<long, DuckLakeNameMap>> LoadAsync(DuckLakeDbContext db, IReadOnlyCollection<long> ids, CancellationToken ct)
    {
        if (ids.Count == 0) return [];
        var kinds = await Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToListAsync(db.ColumnMappings.Where(m => ids.Contains(m.MappingId)), ct);
        foreach (var k in kinds.Where(k => k.Type is not "map_by_name"))
            throw new NotSupportedException($"Column mapping {k.MappingId} has type '{k.Type}' (only map_by_name is defined)");
        var missing = ids.Except(kinds.Select(k => k.MappingId)).ToList();
        if (missing.Count > 0) throw new InvalidDataException($"Data files refer to column mapping {missing[0]}, which ducklake_column_mapping does not have");
        var rows = await Microsoft.EntityFrameworkCore.EntityFrameworkQueryableExtensions.ToListAsync(db.NameMappings.Where(n => ids.Contains(n.MappingId)), ct);
        return ids.ToDictionary(id => id, id => new DuckLakeNameMap(id, rows.Where(r => r.MappingId == id)));
    }

    /// <summary>
    /// The projection under the file's names, and the reader options that match by name. Columns the mapping lacks, and
    /// partition columns, get a name no file field has (they read as their default; partition values are filled in).
    /// </summary>
    public (Schema Projection, DataFileReadOptions Options) Projection(Schema projection, DataFileReadOptions options) =>
        (new Schema(projection.SchemaId, projection.Columns.Select(Rename)), options with { MatchByName = true });

    private NestedField Rename(NestedField f) => f with
    {
        Name = _byColumn.TryGetValue(f.Id, out var m) && !m.IsPartition ? m.Source : $"\u0001unmapped-{f.Id}",
        Type = RenameIn(f.Type),
    };

    private IcebergType RenameIn(IcebergType type) => type switch
    {
        StructType s => new StructType(s.Fields.Select(Rename)),
        ListType l => l with { ElementType = RenameIn(l.ElementType) },
        MapType m => m with { KeyType = RenameIn(m.KeyType), ValueType = RenameIn(m.ValueType) },
        _ => type,
    };

    /// <summary>Values of the Hive partition columns, from <c>name=value</c> segments of the file's path (by column position).</summary>
    public IReadOnlyList<(int Index, object? Value)> PartitionValues(IReadOnlyList<DuckLakeColumn> columns, string location)
    {
        var result = new List<(int, object?)>();
        if (!_byColumn.Values.Any(v => v.IsPartition)) return result;
        var segments = location.Split('/', '\\').Select(s => s.Split('=', 2)).Where(p => p.Length == 2)
            .GroupBy(p => p[0], StringComparer.Ordinal).ToDictionary(g => g.Key, g => Uri.UnescapeDataString(g.Last()[1]), StringComparer.Ordinal);
        for (var i = 0; i < columns.Count; i++)
        {
            if (!_byColumn.TryGetValue(columns[i].Id, out var m) || !m.IsPartition) continue;
            if (!segments.TryGetValue(m.Source, out var text))
                throw new InvalidDataException($"{location}: no '{m.Source}=' path segment for partition column {columns[i].Name}");
            object? value = null;
            if (text != DuckLakePartitioning.HiveDefaultPartition && columns[i].Type is DuckLakePrimitive p)
            {
                if (p.Name is "varchar" or "json") value = text;
                else if (!DuckLakeValues.TryParse(p, text, out value))
                    throw new InvalidDataException($"{location}: partition value '{text}' is not a {p} for column {columns[i].Name}");
            }
            result.Add((i, value));
        }
        return result;
    }
}
