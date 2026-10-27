using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Partitioned writes (spec <c>ducklake_partition_column</c>, <c>ducklake_file_partition_value</c>): rows are split by
/// their partition key, each part is written to its own file, and each file records one value per key. Values are
/// written as statistics text; <c>year</c>/<c>month</c>/<c>day</c>/<c>hour</c> are the calendar fields (2023, 1–12,
/// 1–31, 0–23), not Iceberg's offsets from the epoch, and are taken in UTC.
/// </summary>
public static class DuckLakePartitioning
{
    public const string HiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

    public static void Validate(DuckLakePrimitive type, string transform)
    {
        var temporal = type.Name is "date" or "timestamp" or "timestamptz" or "timestamp_ns";
        var ok = transform switch
        {
            "identity" => DuckLakeValues.Format(type, Sample(type)) is not null,
            "year" or "month" or "day" => temporal,
            "hour" => temporal && type.Name != "date",
            _ => throw new NotSupportedException($"Partition transform '{transform}' is not supported (identity, year, month, day, hour)"),
        };
        if (!ok) throw new ArgumentException($"Transform {transform} does not apply to {type}");
    }

    private static object Sample(DuckLakePrimitive type) => DuckLakeTypes.ToIceberg(type).TypeId switch
    {
        TypeId.Boolean => false, TypeId.Int or TypeId.Date => 0, TypeId.Float => 0f, TypeId.Double => 0d, TypeId.String => "",
        TypeId.Binary => Array.Empty<byte>(), TypeId.Uuid => Guid.Empty, TypeId.Decimal => new IcebergDecimal(0, ((DecimalType)DuckLakeTypes.ToIceberg(type)).Scale),
        _ => 0L,
    };

    /// <summary>The partition value text of <paramref name="value"/>, or null for a null value.</summary>
    public static string? Apply(DuckLakePrimitive type, string transform, object? value)
    {
        if (value is null) return null;
        if (transform == "identity") return DuckLakeValues.Format(type, value) ?? throw new NotSupportedException($"Cannot partition by identity on {type}");
        var utc = type.Name switch
        {
            "date" => IcebergTime.Epoch.AddDays((int)value),
            "timestamp_ns" => IcebergTime.Epoch.AddTicks((long)value / 100),
            _ => IcebergTime.Epoch.AddTicks((long)value * 10),
        };
        long field = transform switch
        {
            "year" => utc.Year,
            "month" => utc.Month,
            "day" => utc.Day,
            "hour" => utc.Hour,
            _ => throw new NotSupportedException(transform),
        };
        return field.ToString(System.Globalization.CultureInfo.InvariantCulture);
    }

    internal static List<(IReadOnlyList<string?> Values, List<object?[]> Rows)> Split(IReadOnlyList<DuckLakeColumn> columns,
        IReadOnlyList<DuckLakePartitionKey> keys, List<object?[]> rows)
    {
        var resolved = keys.Select(k =>
        {
            var index = columns.ToList().FindIndex(c => c.Id == k.ColumnId);
            if (index < 0 || columns[index].Type is not DuckLakePrimitive p)
                throw new InvalidDataException($"Partition key column {k.ColumnId} is not a top-level primitive column");
            return (Index: index, Type: p, k.Transform);
        }).ToList();
        var groups = new Dictionary<string, (IReadOnlyList<string?> Values, List<object?[]> Rows)>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            var values = resolved.Select(k => Apply(k.Type, k.Transform, row[k.Index])).ToList();
            var key = string.Join("\u0001", values.Select(v => v is null ? "\u0000" : v));
            if (!groups.TryGetValue(key, out var g)) groups[key] = g = (values, []);
            g.Rows.Add(row);
        }
        return [.. groups.Values];
    }

    /// <summary>Hive-style directories, as DuckDB writes by default (<c>region=eu/year=2024/</c>).</summary>
    internal static string HivePath(IReadOnlyList<DuckLakeColumn> columns, IReadOnlyList<DuckLakePartitionKey> keys, IReadOnlyList<string?> values) =>
        string.Concat(keys.Select((k, i) =>
        {
            var name = k.Transform == "identity" ? columns.First(c => c.Id == k.ColumnId).Name : k.Transform;
            return $"{Uri.EscapeDataString(name)}={(values[i] is { } v ? Uri.EscapeDataString(v) : HiveDefaultPartition)}/";
        }));
}
