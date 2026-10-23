using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Transforms;

/// <summary>
/// Partition transforms from the spec ("Partition Transforms"). Values use Iceberg's physical CLR
/// representation: int, long (micros / nanos), string, Guid, byte[], <see cref="IcebergDecimal"/>.
/// </summary>
public abstract record Transform
{
    public abstract bool CanTransform(IcebergType source);
    public abstract IcebergType ResultType(IcebergType source);
    public abstract object? Apply(IcebergType source, object? value);

    /// <summary>Human-readable partition value, used in Hive-style data paths (<c>name=value</c>).</summary>
    public virtual string ToHumanString(IcebergType resultType, object? value) =>
        value is null ? "null" : HumanValue(resultType, value);

    public virtual bool PreservesOrder => false;

    public static readonly Transform Identity = new IdentityTransform();
    public static readonly Transform Year = new TimeTransform(TimeUnit.Year);
    public static readonly Transform Month = new TimeTransform(TimeUnit.Month);
    public static readonly Transform Day = new TimeTransform(TimeUnit.Day);
    public static readonly Transform Hour = new TimeTransform(TimeUnit.Hour);
    public static readonly Transform Void = new VoidTransform();

    public static Transform Bucket(int n) => new BucketTransform(n);
    public static Transform Truncate(int w) => new TruncateTransform(w);

    private static readonly Regex BucketRe = new(@"^bucket\[(\d+)\]$", RegexOptions.Compiled);
    private static readonly Regex TruncateRe = new(@"^truncate\[(\d+)\]$", RegexOptions.Compiled);

    /// <summary>Parse a transform's spec string. Unknown transforms are preserved rather than rejected (spec requirement for forward compatibility).</summary>
    public static Transform Parse(string text)
    {
        var t = text.Trim();
        var m = BucketRe.Match(t);
        if (m.Success) return Bucket(int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture));
        m = TruncateRe.Match(t);
        if (m.Success) return Truncate(int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture));
        return t switch
        {
            "identity" => Identity,
            "year" => Year,
            "month" => Month,
            "day" => Day,
            "hour" => Hour,
            "void" => Void,
            _ => new UnknownTransform(t),
        };
    }

    internal static string HumanValue(IcebergType type, object value) => (type.TypeId, value) switch
    {
        (TypeId.Date, int d) => IcebergTime.Epoch.AddDays(d).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        (TypeId.Time, long us) => IcebergTime.Epoch.AddTicks(us * 10).ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        (TypeId.Timestamp, long us) => IcebergTime.TimestampFromMicros(us).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        (TypeId.TimestampTz, long us) => IcebergTime.TimestampFromMicros(us).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "+00:00",
        (TypeId.Boolean, bool b) => b ? "true" : "false",
        (_, byte[] bytes) => Convert.ToBase64String(bytes),
        (_, IFormattable f) => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "null",
    };
}

public sealed record IdentityTransform : Transform
{
    public override bool CanTransform(IcebergType source) => source.IsPrimitive && source.TypeId is not (TypeId.Variant or TypeId.Geometry or TypeId.Geography);
    public override IcebergType ResultType(IcebergType source) => source;
    public override object? Apply(IcebergType source, object? value) => value;
    public override bool PreservesOrder => true;
    public override string ToString() => "identity";
}

public sealed record VoidTransform : Transform
{
    public override bool CanTransform(IcebergType source) => true;
    public override IcebergType ResultType(IcebergType source) => source;
    public override object? Apply(IcebergType source, object? value) => null;
    public override string ToString() => "void";
}

/// <summary>A transform this library does not understand. Metadata containing it must round-trip unchanged; writers must refuse to use the spec.</summary>
public sealed record UnknownTransform(string Name) : Transform
{
    public override bool CanTransform(IcebergType source) => true;
    public override IcebergType ResultType(IcebergType source) => PrimitiveType.String;
    public override object? Apply(IcebergType source, object? value) => throw new NotSupportedException($"Unknown transform '{Name}'");
    public override string ToString() => Name;
}

public sealed record BucketTransform : Transform
{
    public BucketTransform(int numBuckets)
    {
        if (numBuckets <= 0) throw new ArgumentOutOfRangeException(nameof(numBuckets));
        NumBuckets = numBuckets;
    }

    public int NumBuckets { get; }

    public override bool CanTransform(IcebergType source) => source.TypeId is
        TypeId.Int or TypeId.Long or TypeId.Decimal or TypeId.Date or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz
        or TypeId.TimestampNs or TypeId.TimestampTzNs or TypeId.String or TypeId.Uuid or TypeId.Fixed or TypeId.Binary;

    public override IcebergType ResultType(IcebergType source) => PrimitiveType.Int;

    public override object? Apply(IcebergType source, object? value) =>
        value is null ? null : (Hash(source, value) & int.MaxValue) % NumBuckets;

    public static int Hash(IcebergType source, object value) => (source.TypeId, value) switch
    {
        (TypeId.Int or TypeId.Date, int i) => Murmur3.HashInt(i),
        (TypeId.Long or TypeId.Time or TypeId.Timestamp or TypeId.TimestampTz, long l) => Murmur3.HashLong(l),
        // Nanosecond timestamps hash as micros so a micros -> nanos migration keeps bucket assignment.
        (TypeId.TimestampNs or TypeId.TimestampTzNs, long ns) => Murmur3.HashLong(IcebergTime.FloorDiv(ns, 1000)),
        (TypeId.Decimal, IcebergDecimal d) => Murmur3.HashDecimal(d.Unscaled),
        (TypeId.String, string s) => Murmur3.HashString(s),
        (TypeId.Uuid, Guid g) => Murmur3.HashUuid(g),
        (TypeId.Fixed or TypeId.Binary, byte[] b) => Murmur3.HashBytes(b),
        (TypeId.Float or TypeId.Double, double d) => Murmur3.HashDouble(d),
        (TypeId.Float, float f) => Murmur3.HashDouble(f),
        _ => throw new ArgumentException($"Cannot bucket {value.GetType().Name} as {source}"),
    };

    public override string ToString() => $"bucket[{NumBuckets}]";
}

public sealed record TruncateTransform : Transform
{
    public TruncateTransform(int width)
    {
        if (width <= 0) throw new ArgumentOutOfRangeException(nameof(width));
        Width = width;
    }

    public int Width { get; }

    public override bool CanTransform(IcebergType source) => source.TypeId is TypeId.Int or TypeId.Long or TypeId.Decimal or TypeId.String or TypeId.Binary;
    public override IcebergType ResultType(IcebergType source) => source;
    public override bool PreservesOrder => true;

    public override object? Apply(IcebergType source, object? value) => value switch
    {
        null => null,
        int i => i - (((i % Width) + Width) % Width),
        long l => l - (((l % Width) + Width) % Width),
        IcebergDecimal d => new IcebergDecimal(d.Unscaled - (((d.Unscaled % Width) + Width) % Width), d.Scale),
        string s => TruncateCodePoints(s, Width),
        byte[] b => b.Length <= Width ? b : b.AsSpan(0, Width).ToArray(),
        _ => throw new ArgumentException($"Cannot truncate {value.GetType().Name}"),
    };

    /// <summary>Truncation counts Unicode code points, not UTF-16 chars, so surrogate pairs are never split.</summary>
    private static string TruncateCodePoints(string s, int width)
    {
        var sb = new StringBuilder();
        var count = 0;
        foreach (var rune in s.EnumerateRunes())
        {
            if (count++ == width) break;
            sb.Append(rune.ToString());
        }
        return sb.ToString();
    }

    public override string ToString() => $"truncate[{Width}]";
}

public enum TimeUnit { Year, Month, Day, Hour }

public sealed record TimeTransform(TimeUnit Unit) : Transform
{
    public override bool CanTransform(IcebergType source) => source.TypeId switch
    {
        TypeId.Date => Unit != TimeUnit.Hour,
        TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs => true,
        _ => false,
    };

    public override IcebergType ResultType(IcebergType source) => Unit == TimeUnit.Day ? PrimitiveType.Date : PrimitiveType.Int;
    public override bool PreservesOrder => true;

    public override object? Apply(IcebergType source, object? value)
    {
        if (value is null) return null;
        long micros = (source.TypeId, value) switch
        {
            (TypeId.Date, int days) => days * 86_400_000_000L,
            (TypeId.Timestamp or TypeId.TimestampTz, long us) => us,
            (TypeId.TimestampNs or TypeId.TimestampTzNs, long ns) => IcebergTime.FloorDiv(ns, 1000),
            _ => throw new ArgumentException($"Cannot apply {this} to {value.GetType().Name} as {source}"),
        };
        var ts = IcebergTime.Epoch.AddTicks(micros * 10);
        return Unit switch
        {
            TimeUnit.Year => ts.Year - 1970,
            TimeUnit.Month => (ts.Year - 1970) * 12 + ts.Month - 1,
            TimeUnit.Day => (int)IcebergTime.FloorDiv(micros, 86_400_000_000L),
            TimeUnit.Hour => (int)IcebergTime.FloorDiv(micros, 3_600_000_000L),
            _ => throw new InvalidOperationException(),
        };
    }

    public override string ToHumanString(IcebergType resultType, object? value)
    {
        if (value is not int v) return "null";
        return Unit switch
        {
            TimeUnit.Year => (1970 + v).ToString("D4", CultureInfo.InvariantCulture),
            TimeUnit.Month => IcebergTime.Epoch.AddMonths(v).ToString("yyyy-MM", CultureInfo.InvariantCulture),
            TimeUnit.Day => IcebergTime.Epoch.AddDays(v).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            TimeUnit.Hour => IcebergTime.Epoch.AddHours(v).ToString("yyyy-MM-dd-HH", CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException(),
        };
    }

    public override string ToString() => Unit.ToString().ToLowerInvariant();
}
