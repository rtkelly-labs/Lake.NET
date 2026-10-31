using System.Globalization;
using System.Text;

namespace Lake.Delta;

/// <summary>
/// <c>partitionValues</c> strings to typed values (PROTOCOL.md "Partition Value Serialization"). Null and <c>""</c> are
/// null. A <c>timestamp</c> without a zone (<c>1970-01-01 00:00:00[.ffffff]</c>) is in "the time zone of the system which
/// wrote to the table", which the log does not record: it is read as UTC, as delta-rs and Delta Kernel do.
/// </summary>
public static class PartitionValues
{
    private static readonly string[] LocalTimestampFormats =
    [
        "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.FFFFFFF",
        "yyyy-M-d H:m:s", "yyyy-M-d H:m:s.FFFFFFF",
    ];

    public static object? Parse(string? value, DeltaType type, string column)
    {
        if (string.IsNullOrEmpty(value)) return null;
        try
        {
            return type switch
            {
                DeltaPrimitiveType p when p == DeltaPrimitiveType.String => value,
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Binary => Encoding.UTF8.GetBytes(value),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Boolean => value switch
                {
                    _ when value.Equals("true", StringComparison.OrdinalIgnoreCase) => true,
                    _ when value.Equals("false", StringComparison.OrdinalIgnoreCase) => false,
                    _ => throw new FormatException("not true or false"),
                },
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Byte => sbyte.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Short => short.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Integer => int.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Long => long.Parse(value, NumberStyles.Integer, CultureInfo.InvariantCulture),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Float => float.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Double => double.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture),
                DeltaDecimalType => decimal.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Date => DateOnly.ParseExact(value, ["yyyy-MM-dd", "yyyy-M-d"], CultureInfo.InvariantCulture),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.Timestamp => Timestamp(value),
                DeltaPrimitiveType p when p == DeltaPrimitiveType.TimestampNtz =>
                    DateTime.SpecifyKind(DateTime.ParseExact(value, LocalTimestampFormats, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Unspecified),
                _ => throw new NotSupportedException($"partition columns of type {type} are not supported"),
            };
        }
        catch (Exception e) when (e is FormatException or OverflowException)
        {
            throw new InvalidDataException($"Partition value '{value}' of column '{column}' is not a valid {type}: {e.Message}", e);
        }
    }

    private static DateTimeOffset Timestamp(string value)
    {
        if (value.Contains('T'))
        {
            // The recommended form: ISO 8601 adjusted to UTC (1970-01-01T00:00:00.123456Z).
            return DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        }
        var local = DateTime.ParseExact(value, LocalTimestampFormats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        return new DateTimeOffset(DateTime.SpecifyKind(local, DateTimeKind.Utc));
    }
}
