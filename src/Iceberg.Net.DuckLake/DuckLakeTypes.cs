using Iceberg.Net.Types;
using Iceberg.Net.Data;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// A DuckLake column type (spec "Data Types"). Primitives carry the spec's type name (<c>int64</c>,
/// <c>decimal(10,2)</c>, ...); <c>list</c>, <c>struct</c> and <c>map</c> are stored in <c>ducklake_column</c> as a parent
/// row plus child rows, so they are modelled structurally here and flattened by <see cref="DuckLakeColumn"/>.
/// </summary>
public abstract record DuckLakeType
{
    public static readonly DuckLakePrimitive Boolean = new("boolean");
    public static readonly DuckLakePrimitive Int8 = new("int8");
    public static readonly DuckLakePrimitive Int16 = new("int16");
    public static readonly DuckLakePrimitive Int32 = new("int32");
    public static readonly DuckLakePrimitive Int64 = new("int64");
    public static readonly DuckLakePrimitive Float32 = new("float32");
    public static readonly DuckLakePrimitive Float64 = new("float64");
    public static readonly DuckLakePrimitive Date = new("date");
    public static readonly DuckLakePrimitive Time = new("time");
    public static readonly DuckLakePrimitive Timestamp = new("timestamp");
    public static readonly DuckLakePrimitive TimestampTz = new("timestamptz");
    public static readonly DuckLakePrimitive TimestampNs = new("timestamp_ns");
    public static readonly DuckLakePrimitive UInt8 = new("uint8");
    public static readonly DuckLakePrimitive UInt16 = new("uint16");
    public static readonly DuckLakePrimitive UInt32 = new("uint32");
    public static readonly DuckLakePrimitive UInt64 = new("uint64");
    public static readonly DuckLakePrimitive TimestampS = new("timestamp_s");
    public static readonly DuckLakePrimitive TimestampMs = new("timestamp_ms");
    public static readonly DuckLakePrimitive TimeTz = new("timetz");
    public static readonly DuckLakePrimitive Interval = new("interval");
    public static readonly DuckLakePrimitive Varchar = new("varchar");
    public static readonly DuckLakePrimitive Json = new("json");
    public static readonly DuckLakePrimitive Blob = new("blob");
    public static readonly DuckLakePrimitive Uuid = new("uuid");
    /// <summary>1.1: a column whose values are all NULL until it is given a type (<c>"NULL"</c> in DuckDB SQL).</summary>
    public static readonly DuckLakePrimitive Null = new("null");

    public static DuckLakePrimitive Decimal(int precision, int scale) => new($"decimal({precision},{scale})");
    public static DuckLakeList List(DuckLakeType element) => new(element);
    public static DuckLakeMap Map(DuckLakeType key, DuckLakeType value) => new(key, value);
    public static DuckLakeStruct Struct(params (string Name, DuckLakeType Type)[] fields) => new(fields);

    /// <summary>The <c>column_type</c> value of this type's own <c>ducklake_column</c> row.</summary>
    public abstract string ColumnType { get; }
}

public sealed record DuckLakePrimitive : DuckLakeType
{
    public DuckLakePrimitive(string name)
    {
        Name = name.Trim().ToLowerInvariant().Replace(" ", "");
        // Validate eagerly: an unsupported type fails at definition, not at the first write.
        DuckLakeTypes.ToIceberg(this);
    }

    public string Name { get; }
    public override string ColumnType => Name;
    public override string ToString() => Name;
}

public sealed record DuckLakeList(DuckLakeType Element) : DuckLakeType
{
    public override string ColumnType => "list";
    public override string ToString() => $"{Element}[]";
}

public sealed record DuckLakeMap(DuckLakeType Key, DuckLakeType Value) : DuckLakeType
{
    public override string ColumnType => "map";
    public override string ToString() => $"map({Key}, {Value})";
}

public sealed record DuckLakeStruct(IReadOnlyList<(string Name, DuckLakeType Type)> Fields) : DuckLakeType
{
    public override string ColumnType => "struct";
    public bool Equals(DuckLakeStruct? other) => other is not null && Fields.SequenceEqual(other.Fields);
    public override int GetHashCode() => Fields.Aggregate(17, (h, f) => HashCode.Combine(h, f));
    public override string ToString() => $"struct({string.Join(", ", Fields.Select(f => $"{f.Name} {f.Type}"))})";
}

/// <summary>A column version of a table, with its nested children (list <c>element</c>, map <c>key</c>/<c>value</c>, struct fields).</summary>
public sealed record DuckLakeColumn(long Id, string Name, DuckLakeType Type, bool Nullable, IReadOnlyList<DuckLakeColumn> Children)
{
    /// <summary>The column's <c>initial_default</c> as stored (a string literal), or null.</summary>
    public string? InitialDefault { get; init; }

    public IEnumerable<DuckLakeColumn> SelfAndDescendants() => Children.SelectMany(c => c.SelfAndDescendants()).Prepend(this);

    /// <summary>
    /// Assign column IDs to a definition in pre-order (parent, then children), starting at <paramref name="nextId"/>,
    /// as DuckDB does: <c>tags INT[]</c> becomes <c>tags</c> = n, <c>element</c> = n + 1.
    /// </summary>
    public static DuckLakeColumn Assign(string name, DuckLakeType type, bool nullable, ref long nextId)
    {
        var id = nextId++;
        var children = new List<DuckLakeColumn>();
        switch (type)
        {
            case DuckLakeList l:
                children.Add(Assign("element", l.Element, true, ref nextId));
                break;
            case DuckLakeMap m:
                children.Add(Assign("key", m.Key, false, ref nextId));
                children.Add(Assign("value", m.Value, true, ref nextId));
                break;
            case DuckLakeStruct s:
                foreach (var (fieldName, fieldType) in s.Fields) children.Add(Assign(fieldName, fieldType, true, ref nextId));
                break;
        }
        return new DuckLakeColumn(id, name, type, nullable, children);
    }
}

/// <summary>
/// DuckLake ↔ Iceberg type mapping. DuckLake <c>column_id</c> is the Parquet <c>field_id</c> (spec
/// <c>ducklake_column</c>), the same contract Iceberg has, so a DuckLake table's columns map onto an Iceberg
/// <see cref="Schema"/> and reuse Iceberg.Net.Data's Parquet reader and writer unchanged.
/// </summary>
public static class DuckLakeTypes
{
    /// <summary>
    /// Types this implementation reads and writes, as the Iceberg type of their Parquet column (the physical value
    /// type rows hold). The rest of the spec's list (<c>int128</c>/<c>uint128</c>, which DuckDB itself stores as
    /// DOUBLE and cannot read back, <c>time_ns</c>, which DuckDB cannot write, <c>variant</c> and <c>geometry</c>) is
    /// rejected with <see cref="NotSupportedException"/>, so a table using one fails loudly instead of reading wrong values.
    /// </summary>
    public static PrimitiveType ToIceberg(DuckLakePrimitive type) => type.Name switch
    {
        "boolean" => PrimitiveType.Boolean,
        // Narrow integers are Iceberg int, stored as INT32 annotated with their width and sign (as DuckDB writes them).
        "int8" or "int16" or "int32" or "uint8" or "uint16" => PrimitiveType.Int,
        "int64" or "uint32" => PrimitiveType.Long,
        // 0 to 2^64 - 1 fits decimal(20, 0); stored as INT64 annotated unsigned.
        "uint64" => UInt64Decimal,
        "float32" => PrimitiveType.Float,
        "float64" => PrimitiveType.Double,
        "date" => PrimitiveType.Date,
        // timetz values are DuckLakeTimeTz; their Parquet column is a UTC time (micros), as DuckDB writes it.
        "time" or "timetz" => PrimitiveType.Time,
        // Second and millisecond timestamps are microseconds truncated to their unit.
        "timestamp" or "timestamp_s" or "timestamp_ms" => PrimitiveType.Timestamp,
        // interval values are DuckLakeInterval; their Parquet column is the 12-byte INTERVAL.
        "interval" => IntervalFixed,
        "timestamptz" => PrimitiveType.TimestampTz,
        "timestamp_ns" => PrimitiveType.TimestampNs,
        "varchar" or "json" => PrimitiveType.String,
        "blob" => PrimitiveType.Binary,
        "uuid" => PrimitiveType.Uuid,
        // Iceberg's v3 "unknown" is the same idea: always null, never stored in data files.
        "null" => PrimitiveType.Unknown,
        var d when d.StartsWith("decimal(", StringComparison.Ordinal) => ParseDecimal(d),
        var other => throw new NotSupportedException($"DuckLake type '{other}' is not supported by Iceberg.Net.DuckLake"),
    };

    private static readonly DecimalType UInt64Decimal = new(20, 0);
    private static readonly FixedType IntervalFixed = new(12);

    /// <summary>The Parquet annotation a column of this type is written with, as DuckDB writes it; null for the Iceberg default.</summary>
    public static ParquetAnnotation? Annotation(DuckLakePrimitive type) => type.Name switch
    {
        "int8" => ParquetAnnotation.Int8,
        "int16" => ParquetAnnotation.Int16,
        "uint8" => ParquetAnnotation.UInt8,
        "uint16" => ParquetAnnotation.UInt16,
        "uint32" => ParquetAnnotation.UInt32,
        "uint64" => ParquetAnnotation.UInt64,
        "timestamp_ms" => ParquetAnnotation.TimestampMillis,
        "timetz" => ParquetAnnotation.TimeUtc,
        "interval" => ParquetAnnotation.Interval,
        _ => null,
    };

    /// <summary>The annotations of every column (nested ones too) that has one, by column ID: what a data file is written with.</summary>
    public static IReadOnlyDictionary<int, ParquetAnnotation>? Annotations(IEnumerable<DuckLakeColumn> columns)
    {
        Dictionary<int, ParquetAnnotation>? result = null;
        foreach (var c in columns.SelectMany(c => c.SelfAndDescendants()))
            if (c.Type is DuckLakePrimitive p && Annotation(p) is { } a)
                (result ??= [])[FieldId(c.Id)] = a;
        return result;
    }

    /// <summary>Whether a value of this type is held in another form in Parquet (<c>timetz</c>, <c>interval</c>).</summary>
    internal static bool HasFileForm(DuckLakeType type) => type switch
    {
        DuckLakePrimitive p => p.Name is "timetz" or "interval",
        DuckLakeList l => HasFileForm(l.Element),
        DuckLakeMap m => HasFileForm(m.Key) || HasFileForm(m.Value),
        DuckLakeStruct s => s.Fields.Any(f => HasFileForm(f.Type)),
        _ => false,
    };

    private static DecimalType ParseDecimal(string text)
    {
        var inner = text["decimal(".Length..^1].Split(',');
        if (inner.Length != 2 || !int.TryParse(inner[0], out var p) || !int.TryParse(inner[1], out var s) || p is < 1 or > 38 || s < 0 || s > p)
            throw new FormatException($"Malformed DuckLake decimal type '{text}'");
        return new DecimalType(p, s);
    }

    /// <summary>The Iceberg field for a column; IDs above <see cref="int.MaxValue"/> cannot be Parquet field IDs.</summary>
    public static NestedField ToIceberg(DuckLakeColumn column)
    {
        var id = FieldId(column.Id);
        IcebergType type = column.Type switch
        {
            DuckLakePrimitive p => ToIceberg(p),
            DuckLakeList => new ListType(FieldId(Child(column, 0).Id), ToIceberg(Child(column, 0)).Type, !Child(column, 0).Nullable),
            DuckLakeMap => new MapType(FieldId(Child(column, 0).Id), ToIceberg(Child(column, 0)).Type,
                FieldId(Child(column, 1).Id), ToIceberg(Child(column, 1)).Type, !Child(column, 1).Nullable),
            DuckLakeStruct => new StructType(column.Children.Select(ToIceberg)),
            _ => throw new NotSupportedException($"Column {column.Name}: {column.Type}"),
        };
        object? initial = null;
        if (column.InitialDefault is { } text && type is PrimitiveType pt && column.Type is DuckLakePrimitive dp
            && DuckLakeValues.TryParse(dp, text, out var value) && value is not null)
            initial = Json.SingleValueJson.ToJson(pt, DuckLakeValues.ToFile(dp, value)!);
        return new NestedField(id, column.Name, type, !column.Nullable, InitialDefault: initial);
    }

    public static Schema ToIceberg(IReadOnlyList<DuckLakeColumn> columns) => new(0, columns.Select(ToIceberg));

    private static DuckLakeColumn Child(DuckLakeColumn parent, int index) =>
        index < parent.Children.Count ? parent.Children[index]
            : throw new InvalidDataException($"Column {parent.Id} ({parent.Name}) of type {parent.Type.ColumnType} is missing child {index}");

    private static int FieldId(long columnId) =>
        columnId is > 0 and <= int.MaxValue ? (int)columnId
            : throw new InvalidDataException($"column_id {columnId} cannot be a Parquet field_id");

    /// <summary>
    /// Rebuild the column tree from <c>ducklake_column</c> rows of one table at one snapshot
    /// (<c>parent_column</c> links children to parents; siblings are ordered by <c>column_order</c>).
    /// </summary>
    internal static IReadOnlyList<DuckLakeColumn> Tree(IReadOnlyList<ColumnRow> rows) => Corruption.Guard("ducklake_column rows", () =>
    {
        var byParent = rows.GroupBy(r => r.ParentColumn ?? -1).ToDictionary(g => g.Key, g => g.OrderBy(r => r.ColumnOrder).ToList());
        var seen = new HashSet<long>();
        DuckLakeColumn Build(ColumnRow row, int depth)
        {
            if (depth > 64) throw new InvalidDataException("ducklake_column nesting is deeper than 64 levels");
            if (!seen.Add(row.ColumnId)) throw new InvalidDataException($"column_id {row.ColumnId} appears twice in one snapshot");
            var children = byParent.TryGetValue(row.ColumnId, out var c) ? c.Select(x => Build(x, depth + 1)).ToList() : [];
            DuckLakeType type = row.ColumnType switch
            {
                "list" when children.Count == 1 => new DuckLakeList(children[0].Type),
                "map" when children.Count == 2 => new DuckLakeMap(children[0].Type, children[1].Type),
                "struct" when children.Count > 0 => new DuckLakeStruct(children.Select(x => (x.Name, x.Type)).ToList()),
                "list" or "map" or "struct" => throw new InvalidDataException($"Column {row.ColumnId} ({row.ColumnName}): {row.ColumnType} with {children.Count} children"),
                var p => children.Count == 0 ? new DuckLakePrimitive(p)
                    : throw new InvalidDataException($"Primitive column {row.ColumnId} has children"),
            };
            return new DuckLakeColumn(row.ColumnId, row.ColumnName, type, row.NullsAllowed, children) { InitialDefault = row.InitialDefault };
        }
        var top = byParent.TryGetValue(-1, out var roots) ? roots.Select(r => Build(r, 0)).ToList() : [];
        if (seen.Count != rows.Count) throw new InvalidDataException("ducklake_column has rows whose parent_column is not a live column");
        return (IReadOnlyList<DuckLakeColumn>)top;
    });
}

/// <summary>One <c>ducklake_column</c> row.</summary>
internal sealed record ColumnRow(long ColumnId, long ColumnOrder, string ColumnName, string ColumnType, bool NullsAllowed, long? ParentColumn, string? InitialDefault);
