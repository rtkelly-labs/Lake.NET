using Iceberg.Net.Types;

namespace Iceberg.Net.Manifests;

/// <summary>
/// A lower or upper bound of a <c>geometry</c> or <c>geography</c> column: one corner of the bounding box
/// (spec "Bounds for Geometry and Geography"). X and Y are required; Z and M are optional.
/// </summary>
public readonly record struct GeoPoint(double X, double Y, double? Z = null, double? M = null);

/// <summary>
/// A lower or upper bound of a <c>variant</c> column, kept serialized: Variant metadata followed by a Variant object
/// whose keys are normalized JSON paths (spec "Bounds for Variant"). Opaque here because variant values are not
/// implemented; it round-trips byte for byte.
/// </summary>
public sealed class VariantBound(byte[] bytes) : IEquatable<VariantBound>
{
    public byte[] Bytes { get; } = bytes ?? throw new ArgumentNullException(nameof(bytes));

    public bool Equals(VariantBound? other) => other is not null && Bytes.AsSpan().SequenceEqual(other.Bytes);
    public override bool Equals(object? obj) => Equals(obj as VariantBound);
    public override int GetHashCode() => Bytes.Length;
    public override string ToString() => $"variant-bound[{Bytes.Length}]";
}

/// <summary>
/// Typed statistics of one field in one content file: the information of the v1–v3 metrics maps
/// (<see cref="DataFile.ValueCounts"/> and friends, keyed by field ID) and of a v4 <c>content_stats</c> field struct,
/// with bounds as values of the field's type instead of serialized bytes. Every member is nullable: null means
/// "unknown", never zero or "no values" (spec: a missing map entry is equivalent to a null v4 field).
/// </summary>
/// <remarks>
/// Bound values use the same physical representation as rows and literals (<c>int</c>, <c>long</c>, <c>string</c>,
/// <see cref="IcebergDecimal"/>, <see cref="Guid"/>, <c>byte[]</c>, …), plus <see cref="GeoPoint"/> for geo types and
/// <see cref="VariantBound"/> for variant. <see cref="RawLowerBound"/>/<see cref="RawUpperBound"/> keep the serialized
/// bytes whenever the typed value cannot reproduce them exactly, so converting from the v1–v3 maps and back is
/// lossless.
/// </remarks>
public sealed record ColumnStats
{
    public required int FieldId { get; init; }

    /// <summary>The type the bounds are values of; null when the field is not in the schema used to decode.</summary>
    public IcebergType? Type { get; init; }

    public object? LowerBound { get; init; }
    public object? UpperBound { get; init; }

    /// <summary>
    /// v4 <c>tight_bounds</c>: the bounds are exactly the min and max values, not just a range around them. v1–v3 maps
    /// cannot say, so it is null when read from them, and dropped when written to them.
    /// </summary>
    public bool? TightBounds { get; init; }

    /// <summary>Values in the column, including nulls and NaNs.</summary>
    public long? ValueCount { get; init; }
    public long? NullValueCount { get; init; }
    public long? NanValueCount { get; init; }

    /// <summary>v1–v3 <c>column_sizes</c>: bytes on disk. v4 has no such field.</summary>
    public long? ColumnSize { get; init; }

    /// <summary>v4 <c>total_bytes</c>: estimated in-memory bytes of the non-null values. v1–v3 has no such field.</summary>
    public long? TotalBytes { get; init; }

    /// <summary>
    /// The serialized lower bound as read, kept only when <see cref="LowerBound"/> does not re-serialize to the same
    /// bytes: the type is unknown, the bytes do not decode, or they use another valid encoding (a 4-byte bound on a
    /// promoted <c>long</c> column, a non-minimal decimal, a <c>true</c> other than <c>0x01</c>). Written back verbatim.
    /// </summary>
    public byte[]? RawLowerBound { get; init; }

    /// <inheritdoc cref="RawLowerBound"/>
    public byte[]? RawUpperBound { get; init; }

    /// <summary>Bounds that can be compared with literals; geo and variant bounds are not ordered values.</summary>
    internal object? ComparableLower => LowerBound is GeoPoint or VariantBound ? null : LowerBound;
    internal object? ComparableUpper => UpperBound is GeoPoint or VariantBound ? null : UpperBound;

    public bool IsEmpty =>
        LowerBound is null && UpperBound is null && TightBounds is null && ValueCount is null && NullValueCount is null &&
        NanValueCount is null && ColumnSize is null && TotalBytes is null && RawLowerBound is null && RawUpperBound is null;

    public bool Equals(ColumnStats? other) =>
        other is not null && FieldId == other.FieldId && Equals(Type, other.Type) && BoundEquals(LowerBound, other.LowerBound) &&
        BoundEquals(UpperBound, other.UpperBound) && TightBounds == other.TightBounds && ValueCount == other.ValueCount &&
        NullValueCount == other.NullValueCount && NanValueCount == other.NanValueCount && ColumnSize == other.ColumnSize &&
        TotalBytes == other.TotalBytes && BoundEquals(RawLowerBound, other.RawLowerBound) && BoundEquals(RawUpperBound, other.RawUpperBound);

    public override int GetHashCode() => HashCode.Combine(FieldId, ValueCount, NullValueCount, NanValueCount);

    private static bool BoundEquals(object? a, object? b) => (a, b) switch
    {
        (null, null) => true,
        (byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y),
        (float x, float y) => x.Equals(y), // NaN equals NaN, -0.0 differs from +0.0 bitwise: both by Equals
        (double x, double y) => x.Equals(y),
        _ => Equals(a, b),
    };

    /// <summary>
    /// Field <paramref name="fieldId"/>'s statistics from a file's v1–v3 metrics maps, with bounds decoded as
    /// <paramref name="type"/> (null: kept serialized only). A bound that does not decode becomes unknown and is never
    /// used to prune, as before the typed model.
    /// </summary>
    public static ColumnStats FromMetricsMaps(DataFile file, int fieldId, IcebergType? type) => FromMetricsMaps(file, fieldId, type, keepRaw: true);

    /// <param name="keepRaw">False for pruning, which never re-serializes: skips the check that needs a re-encode.</param>
    internal static ColumnStats FromMetricsMaps(DataFile file, int fieldId, IcebergType? type, bool keepRaw)
    {
        static long? Count(IReadOnlyDictionary<int, long>? counts, int id) => counts is not null && counts.TryGetValue(id, out var n) ? n : null;
        var lowerBytes = file.LowerBounds?.GetValueOrDefault(fieldId);
        var upperBytes = file.UpperBounds?.GetValueOrDefault(fieldId);
        var lower = BoundSerialization.TryDecode(type, lowerBytes);
        var upper = BoundSerialization.TryDecode(type, upperBytes);
        return new ColumnStats
        {
            FieldId = fieldId,
            Type = type,
            ValueCount = Count(file.ValueCounts, fieldId),
            NullValueCount = Count(file.NullValueCounts, fieldId),
            NanValueCount = Count(file.NanValueCounts, fieldId),
            ColumnSize = Count(file.ColumnSizes, fieldId),
            LowerBound = lower,
            UpperBound = upper,
            RawLowerBound = !keepRaw || BoundSerialization.Reproduces(type, lower, lowerBytes) ? null : lowerBytes,
            RawUpperBound = !keepRaw || BoundSerialization.Reproduces(type, upper, upperBytes) ? null : upperBytes,
        };
    }

    /// <summary>The bound as v1–v3 bytes: the kept raw bytes, else the typed value serialized (spec Appendix D).</summary>
    public byte[]? LowerBoundBytes() => RawLowerBound ?? (LowerBound is null ? null : BoundSerialization.ToBytes(Type ?? throw NoType(), LowerBound));

    /// <inheritdoc cref="LowerBoundBytes"/>
    public byte[]? UpperBoundBytes() => RawUpperBound ?? (UpperBound is null ? null : BoundSerialization.ToBytes(Type ?? throw NoType(), UpperBound));

    private InvalidOperationException NoType() => new($"Field {FieldId} has a typed bound but no type to serialize it with");
}

/// <summary>
/// The typed statistics of one content file, by field ID: the v1–v3 metrics maps and the v4 <c>content_stats</c>
/// struct are two serializations of it (<see cref="FromDataFile(DataFile, Func{int, IcebergType?})"/> /
/// <see cref="ApplyTo"/>, and <see cref="ContentStatsLayout"/>).
/// </summary>
public sealed class ContentStats
{
    public static readonly ContentStats Empty = new([]);

    public ContentStats(IEnumerable<ColumnStats> fields)
    {
        var byId = new SortedDictionary<int, ColumnStats>();
        foreach (var f in fields)
        {
            if (byId.ContainsKey(f.FieldId)) throw new ArgumentException($"Duplicate stats for field {f.FieldId}");
            byId[f.FieldId] = f;
        }
        Fields = byId;
    }

    /// <summary>Statistics by field ID, ascending.</summary>
    public IReadOnlyDictionary<int, ColumnStats> Fields { get; }

    public ColumnStats? this[int fieldId] => Fields.GetValueOrDefault(fieldId);

    /// <summary>
    /// Every field with any metric in the file's v1–v3 maps, bounds decoded by the type <paramref name="typeOf"/>
    /// gives for the ID (null for a field the schema does not have: its bounds stay serialized).
    /// </summary>
    public static ContentStats FromDataFile(DataFile file, Func<int, IcebergType?> typeOf)
    {
        var ids = new SortedSet<int>();
        void Add<T>(IReadOnlyDictionary<int, T>? map)
        {
            if (map is not null) ids.UnionWith(map.Keys);
        }
        Add(file.ColumnSizes);
        Add(file.ValueCounts);
        Add(file.NullValueCounts);
        Add(file.NanValueCounts);
        Add(file.LowerBounds);
        Add(file.UpperBounds);
        return new ContentStats(ids.Select(id => ColumnStats.FromMetricsMaps(file, id, typeOf(id))));
    }

    public static ContentStats FromDataFile(DataFile file, Schema schema) => FromDataFile(file, schema.FindType);

    /// <summary>
    /// <paramref name="file"/> with its six metrics maps replaced by these statistics. A map with no entries is
    /// written as null (the spec treats a missing map and a missing entry alike). <see cref="ColumnStats.TightBounds"/>
    /// and <see cref="ColumnStats.TotalBytes"/> have no v1–v3 form and are dropped.
    /// </summary>
    public DataFile ApplyTo(DataFile file)
    {
        Dictionary<int, TV>? Map<TV>(Func<ColumnStats, TV?> get) where TV : class
        {
            var m = new Dictionary<int, TV>();
            foreach (var (id, s) in Fields)
                if (get(s) is { } v) m[id] = v;
            return m.Count == 0 ? null : m;
        }
        Dictionary<int, long>? Counts(Func<ColumnStats, long?> get)
        {
            var m = new Dictionary<int, long>();
            foreach (var (id, s) in Fields)
                if (get(s) is { } v) m[id] = v;
            return m.Count == 0 ? null : m;
        }
        return file with
        {
            ColumnSizes = Counts(s => s.ColumnSize),
            ValueCounts = Counts(s => s.ValueCount),
            NullValueCounts = Counts(s => s.NullValueCount),
            NanValueCounts = Counts(s => s.NanValueCount),
            LowerBounds = Map(s => s.LowerBoundBytes()),
            UpperBounds = Map(s => s.UpperBoundBytes()),
        };
    }
}

/// <summary>
/// Serialized bounds in v1–v3 metrics maps and manifest-list partition summaries (spec Appendix D, "Bound
/// serialization"): the binary single-value serialization (<see cref="Conversions"/>), except that geometry and
/// geography bounds are points (<c>x:y[:z[:m]]</c> little-endian doubles, <c>x:y:NaN:m</c> when only z is unset) and
/// variant bounds are opaque <see cref="VariantBound"/>s.
/// </summary>
public static class BoundSerialization
{
    public static byte[] ToBytes(IcebergType type, object value) => (type.TypeId, value) switch
    {
        (TypeId.Geometry or TypeId.Geography, GeoPoint p) => PointBytes(p),
        (TypeId.Variant, VariantBound v) => v.Bytes,
        _ => Conversions.ToBytes(type, value),
    };

    public static object FromBytes(IcebergType type, ReadOnlySpan<byte> bytes) => type.TypeId switch
    {
        TypeId.Geometry or TypeId.Geography => ReadPoint(bytes),
        TypeId.Variant => new VariantBound(bytes.ToArray()),
        _ => Conversions.FromBytes(type, bytes),
    };

    /// <summary>The decoded bound, or null when there is none, no type, or the bytes do not decode as the type.</summary>
    public static object? TryDecode(IcebergType? type, byte[]? bytes)
    {
        if (bytes is null || type is null) return null;
        try
        {
            return FromBytes(type, bytes);
        }
        catch (Exception e) when (e is ArgumentException or FormatException or InvalidDataException or NotSupportedException)
        {
            return null; // undecodable bound: no information, never prune on it
        }
    }

    /// <summary>Whether serializing <paramref name="value"/> gives back exactly <paramref name="bytes"/>.</summary>
    internal static bool Reproduces(IcebergType? type, object? value, byte[]? bytes)
    {
        if (bytes is null) return true;
        if (value is null || type is null) return false;
        try
        {
            return ToBytes(type, value).AsSpan().SequenceEqual(bytes);
        }
        catch (Exception e) when (e is ArgumentException or InvalidCastException or OverflowException)
        {
            return false;
        }
    }

    private static byte[] PointBytes(GeoPoint p)
    {
        var n = p.M is not null ? 4 : p.Z is not null ? 3 : 2;
        var b = new byte[n * 8];
        Compat.WriteDouble(b, p.X);
        Compat.WriteDouble(b.AsSpan(8), p.Y);
        // An unset z before a set m is NaN; written as Java's canonical quiet NaN (0x7ff8...), not .NET's (0xfff8...).
        if (n > 2) Compat.WriteDouble(b.AsSpan(16), p.Z ?? BitConverter.Int64BitsToDouble(0x7FF8000000000000));
        if (n > 3) Compat.WriteDouble(b.AsSpan(24), p.M!.Value);
        return b;
    }

    private static GeoPoint ReadPoint(ReadOnlySpan<byte> b)
    {
        if (b.Length is not (16 or 24 or 32)) throw new FormatException($"A geo bound is 16, 24 or 32 bytes, not {b.Length}");
        var x = Compat.ReadDouble(b);
        var y = Compat.ReadDouble(b.Slice(8));
        double? z = b.Length > 16 ? Compat.ReadDouble(b.Slice(16)) : null;
        double? m = b.Length > 24 ? Compat.ReadDouble(b.Slice(24)) : null;
        if (m is not null && z is { } zv && double.IsNaN(zv)) z = null; // x:y:NaN:m means z is unset
        return new GeoPoint(x, y, z, m);
    }
}
