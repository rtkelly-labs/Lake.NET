using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Partitioned writes (spec <c>ducklake_partition_column</c>, <c>ducklake_file_partition_value</c>): rows are split by
/// their partition key, each part is written to its own file, and each file records one value per key. Values are
/// written as statistics text; <c>year</c>/<c>month</c>/<c>day</c>/<c>hour</c> are the calendar fields (2023, 1–12,
/// 1–31, 0–23), taken in UTC, and the 1.1 <c>epoch_*</c> transforms are Iceberg's offsets from the epoch.
/// </summary>
public static class DuckLakePartitioning
{
    public const string HiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

    public static void Validate(DuckLakePrimitive type, string transform)
    {
        var temporal = type.Name is "date" or "timestamp" or "timestamptz" or "timestamp_ns" or "timestamp_s" or "timestamp_ms";
        if (BucketCount(transform) is { } n)
        {
            if (n <= 0) throw new ArgumentException($"{transform}: the bucket count must be positive");
            // Unsigned, second/millisecond timestamp, timetz and interval buckets: no hash agreed with other engines.
            if (DuckLakeTypes.ToIceberg(type).TypeId is TypeId.Boolean or TypeId.Float or TypeId.Double or TypeId.Unknown
                || type.Name is "uint8" or "uint16" or "uint32" or "uint64" or "timestamp_s" or "timestamp_ms" or "timetz" or "interval")
                throw new ArgumentException($"Transform {transform} does not apply to {type}");
            return;
        }
        var ok = transform switch
        {
            "identity" => DuckLakeValues.Format(type, Sample(type)) is not null,
            "year" or "month" or "day" or "epoch_year" or "epoch_month" or "epoch_day" => temporal,
            "hour" or "epoch_hour" => temporal && type.Name != "date",
            _ => throw new NotSupportedException(
                $"Partition transform '{transform}' is not supported (identity, bucket(N), year, month, day, hour, epoch_year, epoch_month, epoch_day, epoch_hour)"),
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
        // bucket(N): Iceberg's Murmur3 bucket, (murmur3_32(v) & INT_MAX) % N (spec ducklake_partition_column).
        if (BucketCount(transform) is { } buckets)
            return Convert.ToInt64(Iceberg.Net.Transforms.Transform.Bucket(buckets).Apply(DuckLakeTypes.ToIceberg(type), value),
                System.Globalization.CultureInfo.InvariantCulture).ToString(System.Globalization.CultureInfo.InvariantCulture);
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
            // 1.1: Iceberg's convention, units since 1970-01-01 (the values Iceberg writes for the same data).
            "epoch_year" or "epoch_month" or "epoch_day" or "epoch_hour" =>
                Convert.ToInt64(EpochTransform(transform).Apply(DuckLakeTypes.ToIceberg(type), value), System.Globalization.CultureInfo.InvariantCulture),
            _ => throw new NotSupportedException(transform),
        };
        return field.ToString(System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>N of a <c>bucket(N)</c> transform (as DuckDB stores it in <c>ducklake_partition_column.transform</c>), else null.</summary>
    public static int? BucketCount(string transform) =>
        transform.StartsWith("bucket(", StringComparison.Ordinal) && transform.EndsWith(')')
            ? int.TryParse(transform["bucket(".Length..^1], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var n)
                ? n : throw new FormatException($"Malformed partition transform '{transform}'")
            : null;

    private static Iceberg.Net.Transforms.Transform EpochTransform(string name) => name switch
    {
        "epoch_year" => Iceberg.Net.Transforms.Transform.Year,
        "epoch_month" => Iceberg.Net.Transforms.Transform.Month,
        "epoch_day" => Iceberg.Net.Transforms.Transform.Day,
        _ => Iceberg.Net.Transforms.Transform.Hour,
    };

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
            var name = k.Transform == "identity" ? columns.First(c => c.Id == k.ColumnId).Name : BucketCount(k.Transform) is not null ? "bucket" : k.Transform;
            return $"{Uri.EscapeDataString(name)}={(values[i] is { } v ? Uri.EscapeDataString(v) : HiveDefaultPartition)}/";
        }));
}
