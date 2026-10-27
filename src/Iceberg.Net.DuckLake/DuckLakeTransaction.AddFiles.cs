using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

/// <summary>How <see cref="DuckLakeTransaction.AddDataFilesAsync"/> treats files that do not match the table exactly (DuckDB's options of the same names).</summary>
public sealed record DuckLakeAddFileOptions
{
    /// <summary>Accept files without some of the table's columns (they read as their default, or null).</summary>
    public bool AllowMissing { get; init; }
    /// <summary>Accept files with columns the table does not have (they are not read).</summary>
    public bool IgnoreExtraColumns { get; init; }
    /// <summary>Take the values of table columns the file lacks from <c>name=value</c> segments of its path.</summary>
    public bool HivePartitioning { get; init; }
}

public sealed partial class DuckLakeTransaction
{
    /// <summary>
    /// Register existing Parquet files as data files of a table without rewriting them (DuckDB's
    /// <c>ducklake_add_data_files</c>): columns are matched by name through a <c>map_by_name</c> column mapping
    /// (spec <c>ducklake_column_mapping</c>, <c>ducklake_name_mapping</c>), whatever field IDs the files have; their
    /// statistics come from the footers. The files stay where they are and are recorded by absolute path. Returns the
    /// rows added.
    /// </summary>
    public async Task<long> AddDataFilesAsync(string schema, string table, IEnumerable<string> locations, DuckLakeAddFileOptions? options = null,
        CancellationToken ct = default)
    {
        options ??= new DuckLakeAddFileOptions();
        PendingTable t;
        await using (var db = await BeginAsync(ct)) t = await TableAsync(db, schema, table, ct);
        if (_catalog.Encrypted) throw new NotSupportedException("The lake is encrypted: files written elsewhere have no key in it to be added with");
        if (t.PartitionKeys.Count > 0) throw new NotSupportedException($"{schema}.{table} is partitioned: existing files can only be added to unpartitioned tables");
        var columns = t.Columns;
        var projection = DuckLakeTypes.ToIceberg(columns.Where(c => c.Type is not DuckLakePrimitive { Name: "null" }).ToList());
        long added = 0;
        var files = new List<WrittenFile>();
        foreach (var location in locations)
        {
            var facts = await ParquetInspection.InspectAsync(_catalog.IO, location, projection, ct);
            if (facts.Incompatible.Count > 0)
            {
                var (id, found) = facts.Incompatible.First();
                var column = columns.SelectMany(c => c.SelfAndDescendants()).First(c => c.Id == id);
                throw new ArgumentException($"Failed to map column \"{column.Name}\" from file \"{location}\" to the column in table \"{table}\": {found} cannot be read as {column.Type}");
            }
            var partition = options.HivePartitioning ? HiveValues(location) : new Dictionary<string, string>(StringComparer.Ordinal);
            var fromPath = columns.Where(c => facts.Missing.Contains((int)c.Id) && partition.ContainsKey(c.Name)).ToList();
            var missing = columns.SelectMany(c => c.SelfAndDescendants())
                .Where(c => facts.Missing.Contains((int)c.Id) && !fromPath.Contains(c) && c.Type is not DuckLakePrimitive { Name: "null" }).ToList();
            if (missing.Count > 0 && !options.AllowMissing)
                throw new ArgumentException($"Column \"{missing[0].Name}\" exists in table \"{table}\" but was not found in file \"{location}\"; set AllowMissing to add the file anyway");
            var extra = ExtraFields(facts.Fields, columns);
            if (extra.Count > 0 && !options.IgnoreExtraColumns)
                throw new ArgumentException($"Column \"{extra[0]}\" exists in file \"{location}\" but was not found in table \"{table}\"; set IgnoreExtraColumns to add the file anyway");
            foreach (var c in missing.Where(c => !c.Nullable && columns.Contains(c) && c.InitialDefault is null))
                throw new ArgumentException($"Column \"{c.Name}\" is NOT NULL but file \"{location}\" does not have it");

            var stats = new List<DuckLakeColumnStats>();
            foreach (var c in columns.SelectMany(c => c.SelfAndDescendants()).Where(c => c.Type is DuckLakePrimitive))
            {
                if (!facts.Statistics.TryGetValue((int)c.Id, out var s)) continue;
                var type = (DuckLakePrimitive)c.Type;
                string? Bound(object? v) => v is null ? null : DuckLakeValues.Format(type, v);
                var (min, max) = (Bound(s.LowerBound), Bound(s.UpperBound));
                var nulls = s.NullValueCount ?? 0;
                stats.Add(new DuckLakeColumnStats(c.Id, (s.ValueCount ?? 0) - nulls, nulls, min is null || max is null ? null : min, min is null || max is null ? null : max, null)
                    { ColumnSizeBytes = s.ColumnSize });
            }
            foreach (var c in fromPath)
            {
                var text = partition[c.Name];
                var isNull = text == DuckLakePartitioning.HiveDefaultPartition;
                object? value = null;
                if (!isNull && c.Type is DuckLakePrimitive p && p.Name is not ("varchar" or "json") && !DuckLakeValues.TryParse(p, text, out value))
                    throw new ArgumentException($"Partition value '{text}' in \"{location}\" is not a {p} for column \"{c.Name}\"");
                var formatted = isNull || c.Type is not DuckLakePrimitive prim ? null : prim.Name is "varchar" or "json" ? text : DuckLakeValues.Format(prim, value!);
                stats.Add(new DuckLakeColumnStats(c.Id, isNull ? 0 : facts.RecordCount, isNull ? facts.RecordCount : 0, formatted, formatted, null) { ColumnSizeBytes = 0 });
            }
            files.Add(new WrittenFile(location, facts.RecordCount, facts.FileSize, facts.FooterSize, facts.RowGroups, stats, [])
            {
                Absolute = true, Mapping = Mapping(facts.Fields, columns, fromPath),
            });
            added += facts.RecordCount;
        }
        foreach (var f in files) _ops.Add(new InsertOp(t, f));
        if (files.Count > 0) t.Inserted = true;
        return added;
    }

    private static Dictionary<string, string> HiveValues(string location) =>
        location.Split('/', '\\').Select(s => s.Split('=', 2)).Where(p => p.Length == 2)
            .GroupBy(p => p[0], StringComparer.Ordinal).ToDictionary(g => g.Key, g => Uri.UnescapeDataString(g.Last()[1]), StringComparer.Ordinal);

    /// <summary>File fields (dotted paths) no table column matches, top-level and inside matched structs.</summary>
    private static List<string> ExtraFields(IReadOnlyList<ParquetField> fields, IReadOnlyList<DuckLakeColumn> columns, string prefix = "")
    {
        var result = new List<string>();
        foreach (var f in fields)
        {
            var column = columns.FirstOrDefault(c => c.Name == f.Name);
            if (column is null) result.Add(prefix + f.Name);
            else if (f.Kind == ParquetFieldKind.Struct && column.Type is DuckLakeStruct) result.AddRange(ExtraFields(f.Children, column.Children, $"{prefix}{f.Name}."));
        }
        return result;
    }

    /// <summary>
    /// The name mapping DuckDB writes for a file: its fields in file order (pre-order), numbered from 0, each with the
    /// column it maps to and its parent's number; list elements under the name of their repeated group, map keys and
    /// values under the map; then the columns taken from the Hive path.
    /// </summary>
    private static List<NameMappingEntity> Mapping(IReadOnlyList<ParquetField> fields, IReadOnlyList<DuckLakeColumn> columns, IReadOnlyList<DuckLakeColumn> fromPath)
    {
        var rows = new List<NameMappingEntity>();
        void Add(ParquetField f, DuckLakeColumn c, long? parent)
        {
            var id = (long)rows.Count;
            rows.Add(new NameMappingEntity { ColumnId = id, SourceName = f.Name, TargetFieldId = c.Id, ParentColumn = parent, IsPartition = false });
            switch (c.Type)
            {
                case DuckLakeStruct when f.Kind == ParquetFieldKind.Struct:
                    foreach (var child in f.Children)
                        if (c.Children.FirstOrDefault(x => x.Name == child.Name) is { } target) Add(child, target, id);
                    break;
                case DuckLakeList when f.Kind == ParquetFieldKind.List:
                    Add(f.Children[0], c.Children[0], id);
                    break;
                case DuckLakeMap when f.Kind == ParquetFieldKind.Map:
                    Add(f.Children[0], c.Children[0], id);
                    Add(f.Children[1], c.Children[1], id);
                    break;
            }
        }
        foreach (var f in fields)
            if (columns.FirstOrDefault(c => c.Name == f.Name) is { } column) Add(f, column, null);
        foreach (var c in fromPath)
            rows.Add(new NameMappingEntity { ColumnId = rows.Count, SourceName = c.Name, TargetFieldId = c.Id, ParentColumn = null, IsPartition = true });
        return rows;
    }
}
