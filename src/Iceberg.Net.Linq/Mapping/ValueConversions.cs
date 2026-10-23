using System.Globalization;
using System.Numerics;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq;

/// <summary>
/// CLR values ↔ the physical values rows carry (see <c>ParquetFiles</c>): int days for <c>date</c>, long micros for
/// <c>time</c>/<c>timestamp[tz]</c>, long nanos for <c>timestamp[tz]_ns</c>, <see cref="IcebergDecimal"/>, <see cref="Guid"/>,
/// <c>byte[]</c>. Conversions never lose information silently on write except where .NET has more precision than the
/// column (sub-microsecond ticks into a micros timestamp are truncated, as Spark does).
/// </summary>
internal static class ValueConversions
{
    internal sealed record Scalar(Func<object, object> To, Func<object, object> From);

    /// <summary>The underlying type of <c>Nullable&lt;T&gt;</c>, else the type itself.</summary>
    public static Type Underlying(Type t) => Nullable.GetUnderlyingType(t) ?? t;

    public static bool IsScalar(Type t)
    {
        var u = Underlying(t);
        return u.IsEnum || u.IsPrimitive || u == typeof(decimal) || u == typeof(string) || u == typeof(Guid) || u == typeof(byte[])
            || u == typeof(DateTime) || u == typeof(DateTimeOffset) || u == typeof(DateOnly) || u == typeof(TimeOnly) || u == typeof(TimeSpan);
    }

    /// <summary>The column type <see cref="IcebergSchema.FromType{T}"/> derives for a scalar member type.</summary>
    public static PrimitiveType? DefaultType(Type t)
    {
        var u = Underlying(t);
        if (u.IsEnum) return Type.GetTypeCode(u) is TypeCode.Int64 or TypeCode.UInt32 or TypeCode.UInt64 ? PrimitiveType.Long : PrimitiveType.Int;
        if (u == typeof(bool)) return PrimitiveType.Boolean;
        if (u == typeof(int) || u == typeof(short) || u == typeof(byte) || u == typeof(sbyte) || u == typeof(ushort)) return PrimitiveType.Int;
        if (u == typeof(long) || u == typeof(uint)) return PrimitiveType.Long;
        if (u == typeof(float)) return PrimitiveType.Float;
        if (u == typeof(double)) return PrimitiveType.Double;
        if (u == typeof(decimal)) return new DecimalType(38, 18);
        if (u == typeof(string)) return PrimitiveType.String;
        if (u == typeof(Guid)) return PrimitiveType.Uuid;
        if (u == typeof(byte[])) return PrimitiveType.Binary;
        if (u == typeof(DateOnly)) return PrimitiveType.Date;
        if (u == typeof(TimeOnly) || u == typeof(TimeSpan)) return PrimitiveType.Time;
        if (u == typeof(DateTime)) return PrimitiveType.Timestamp;
        if (u == typeof(DateTimeOffset)) return PrimitiveType.TimestampTz;
        return null;
    }

    /// <summary>Conversions between non-null values of <paramref name="clr"/> and <paramref name="type"/>, or null when the pair is not supported.</summary>
    public static Scalar? For(Type clr, PrimitiveType type)
    {
        var u = Underlying(clr);
        var id = type.TypeId;
        if (u.IsEnum) return Enum(u, id);
        if (u == typeof(bool)) return id == TypeId.Boolean ? Identity : null;
        if (u == typeof(int))
            return id switch
            {
                TypeId.Int or TypeId.Date => Identity,
                TypeId.Long => new(v => (long)(int)v, v => checked((int)(long)v)),
                _ => null,
            };
        if (u == typeof(short) || u == typeof(byte) || u == typeof(sbyte) || u == typeof(ushort))
            return id == TypeId.Int ? new(v => System.Convert.ToInt32(v, CultureInfo.InvariantCulture), v => System.Convert.ChangeType((int)v, u, CultureInfo.InvariantCulture)) : null;
        if (u == typeof(long))
            return id switch
            {
                TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs => Identity,
                TypeId.Int => new(v => checked((int)(long)v), v => (long)(int)v),
                _ => null,
            };
        if (u == typeof(uint)) return id == TypeId.Long ? new(v => (long)(uint)v, v => checked((uint)(long)v)) : null;
        if (u == typeof(float)) return id == TypeId.Float ? Identity : null;
        if (u == typeof(double))
            return id switch
            {
                TypeId.Double => Identity,
                TypeId.Float => new(v => ExactFloat((double)v), v => (double)(float)v),
                _ => null,
            };
        if (u == typeof(decimal))
            return type is DecimalType d ? new(v => ToDecimal((decimal)v, d), v => FromDecimal((IcebergDecimal)v)) : null;
        if (u == typeof(string)) return id == TypeId.String ? Identity : null;
        if (u == typeof(Guid)) return id == TypeId.Uuid ? Identity : null;
        if (u == typeof(byte[])) return id is TypeId.Binary or TypeId.Fixed ? Identity : null;
        if (u == typeof(DateOnly)) return id == TypeId.Date ? new(v => IcebergTime.DaysFromDate((DateOnly)v), v => IcebergTime.DateFromDays((int)v)) : null;
        if (u == typeof(TimeOnly)) return id == TypeId.Time ? new(v => IcebergTime.MicrosFromTime((TimeOnly)v), v => IcebergTime.TimeFromMicros((long)v)) : null;
        if (u == typeof(TimeSpan))
            return id == TypeId.Time ? new(v => ((TimeSpan)v).Ticks / 10, v => TimeSpan.FromTicks((long)v * 10)) : null;
        if (u == typeof(DateTime))
            return id switch
            {
                TypeId.Date => new(v => DaysOf((DateTime)v), v => IcebergTime.Epoch.AddDays((int)v).ToUnspecified()),
                TypeId.Timestamp => new(v => IcebergTime.MicrosFromTimestamp((DateTime)v), v => IcebergTime.TimestampFromMicros((long)v).ToUnspecified()),
                TypeId.TimestampTz => new(v => IcebergTime.MicrosFromTimestamp((DateTime)v), v => IcebergTime.TimestampFromMicros((long)v)),
                TypeId.TimestampNs => new(v => IcebergTime.NanosFromTimestamp((DateTime)v), v => FromNanos((long)v).ToUnspecified()),
                TypeId.TimestampTzNs => new(v => IcebergTime.NanosFromTimestamp((DateTime)v), v => FromNanos((long)v)),
                _ => null,
            };
        if (u == typeof(DateTimeOffset))
            return id switch
            {
                TypeId.Timestamp or TypeId.TimestampTz => new(v => IcebergTime.MicrosFromTimestamp((DateTimeOffset)v), v => new DateTimeOffset(IcebergTime.TimestampFromMicros((long)v))),
                TypeId.TimestampNs or TypeId.TimestampTzNs => new(v => (((DateTimeOffset)v).UtcTicks - IcebergTime.Epoch.Ticks) * 100, v => new DateTimeOffset(FromNanos((long)v))),
                _ => null,
            };
        return null;
    }

    private static readonly Scalar Identity = new(v => v, v => v);

    private static Scalar? Enum(Type e, TypeId id) => id switch
    {
        TypeId.Int => new(v => checked((int)System.Convert.ToInt64(v, CultureInfo.InvariantCulture)), v => System.Enum.ToObject(e, (int)v)),
        TypeId.Long => new(v => System.Convert.ToInt64(v, CultureInfo.InvariantCulture), v => System.Enum.ToObject(e, (long)v)),
        TypeId.String => new(v => v.ToString()!, v => System.Enum.Parse(e, (string)v)),
        _ => null,
    };

    private static DateTime ToUnspecified(this DateTime d) => DateTime.SpecifyKind(d, DateTimeKind.Unspecified);

    private static DateTime FromNanos(long ns) => IcebergTime.Epoch.AddTicks(IcebergTime.FloorDiv(ns, 100));

    private static object DaysOf(DateTime d) => d.TimeOfDay == TimeSpan.Zero
        ? IcebergTime.DaysFromDate(d)
        : throw new ArgumentException($"{d:O} has a time of day; a date column holds dates only");

    private static object ExactFloat(double d)
    {
        var f = (float)d;
        return f == d || double.IsNaN(d) ? f : throw new ArgumentException($"{d} is not exactly representable in a float column");
    }

    private static object ToDecimal(decimal value, DecimalType type)
    {
        var d = (IcebergDecimal)Literals.Convert(type, value);
        return BigInteger.Abs(d.Unscaled) < BigInteger.Pow(10, type.Precision)
            ? d
            : throw new ArgumentException($"{value} does not fit {type}");
    }

    private static object FromDecimal(IcebergDecimal d) => decimal.Parse(d.ToString(), NumberStyles.Number, CultureInfo.InvariantCulture);
}
