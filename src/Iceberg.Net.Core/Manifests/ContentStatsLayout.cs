using Iceberg.Net.Types;

namespace Iceberg.Net.Manifests;

/// <summary>
/// The v4 <c>content_stats</c> struct (manifest field 146, spec "Content Stats"): one optional struct per table field
/// at field ID <c>10_000 + 200 × field-id</c> (its <c>base-id</c>), whose members sit at fixed offsets from the base.
/// This maps <see cref="ColumnStats"/> to and from that layout, with struct values as <c>object?[]</c> in the
/// struct type's field order and resolved by field ID, never by name or position. Model only: no v4 manifest is
/// written or read yet.
/// </summary>
public static class ContentStatsLayout
{
    /// <summary><c>content_stats</c> in the v4 <c>tracked_file</c> struct.</summary>
    public const int ContentStatsFieldId = 146;

    /// <summary>Stats IDs for table fields: <c>[10_000, 200_000_000)</c>.</summary>
    public const int FirstStatsId = 10_000;
    public const int StatsIdEnd = 200_000_000;
    public const int RangeSize = 200;

    // Offsets from base-id (spec "Field Statistics").
    public const int LowerBound = 1;
    public const int UpperBound = 2;
    public const int TightBounds = 3;
    public const int ValueCount = 4;
    public const int NullValueCount = 5;
    public const int NanValueCount = 6;
    public const int TotalBytes = 7;
    public const int GeoLowerX = 10, GeoLowerY = 11, GeoLowerZ = 12, GeoLowerM = 13;
    public const int GeoUpperX = 14, GeoUpperY = 15, GeoUpperZ = 16, GeoUpperM = 17;

    /// <summary>Reserved metadata columns with stats ranges of their own; other metadata columns are not tracked.</summary>
    private static readonly Dictionary<int, int> ReservedBaseIds = new()
    {
        [2147483539] = 9000, // _last_updated_sequence_number
        [2147483540] = 9200, // _row_id
    };

    /// <summary>The base ID of a field's stats struct, or null when its stats cannot be stored in <c>content_stats</c>.</summary>
    public static int? BaseId(int fieldId)
    {
        if (ReservedBaseIds.TryGetValue(fieldId, out var reserved)) return reserved;
        if (fieldId < 0) return null;
        var id = FirstStatsId + (long)RangeSize * fieldId;
        return id < StatsIdEnd ? (int)id : null;
    }

    /// <summary>The table field a stats struct ID belongs to, or null when the ID is not a base ID.</summary>
    public static int? FieldIdOf(int baseId)
    {
        foreach (var (fieldId, b) in ReservedBaseIds)
            if (b == baseId) return fieldId;
        if (baseId < FirstStatsId || baseId >= StatsIdEnd || (baseId - FirstStatsId) % RangeSize != 0) return null;
        return (baseId - FirstStatsId) / RangeSize;
    }

    /// <summary>
    /// The stats struct for a table field (the members the spec includes for its type and requirement), or null when
    /// the field has none: nested types (their leaves have their own), and fields outside the stats ID range.
    /// </summary>
    public static StructType? StatsStruct(NestedField field)
    {
        if (!field.Type.IsPrimitive || BaseId(field.Id) is not { } b) return null;
        var t = field.Type;
        var geo = t.TypeId is TypeId.Geometry or TypeId.Geography;
        var members = new List<NestedField>();
        // Bounds for every primitive and variant; unknown has no values to bound (and no single-value form).
        if (t.TypeId != TypeId.Unknown)
        {
            members.Add(NestedField.Optional(b + LowerBound, "lower_bound", geo ? GeoStruct(b, upper: false) : t));
            members.Add(NestedField.Optional(b + UpperBound, "upper_bound", geo ? GeoStruct(b, upper: true) : t));
            if (!geo) members.Add(NestedField.Optional(b + TightBounds, "tight_bounds", PrimitiveType.Boolean));
        }
        members.Add(NestedField.Optional(b + ValueCount, "value_count", PrimitiveType.Long));
        if (field.IsOptional) members.Add(NestedField.Optional(b + NullValueCount, "null_value_count", PrimitiveType.Long));
        if (t.TypeId is TypeId.Float or TypeId.Double) members.Add(NestedField.Optional(b + NanValueCount, "nan_value_count", PrimitiveType.Long));
        if (t.TypeId is TypeId.String or TypeId.Binary or TypeId.Variant or TypeId.Geometry or TypeId.Geography)
            members.Add(NestedField.Optional(b + TotalBytes, "total_bytes", PrimitiveType.Long));
        return new StructType(members);
    }

    private static StructType GeoStruct(int b, bool upper)
    {
        var o = upper ? GeoUpperX : GeoLowerX;
        return new StructType(
            NestedField.Require(b + o, "x", PrimitiveType.Double),
            NestedField.Require(b + o + 1, "y", PrimitiveType.Double),
            NestedField.Optional(b + o + 2, "z", PrimitiveType.Double),
            NestedField.Optional(b + o + 3, "m", PrimitiveType.Double));
    }

    /// <summary>
    /// A <c>content_stats</c> struct type for a schema: one optional stats struct per primitive field (at any depth),
    /// named by the field's full name, in field-ID order. Writers may track fewer fields; readers resolve by ID.
    /// </summary>
    public static StructType ContentStatsType(Schema schema) =>
        new(TypeUtil.IndexById(schema.Struct).Values
            .OrderBy(f => f.Id)
            .Select(f => (Field: f, Stats: StatsStruct(f)))
            .Where(x => x.Stats is not null)
            .Select(x => NestedField.Optional(BaseId(x.Field.Id)!.Value, schema.FindColumnName(x.Field.Id) ?? x.Field.Name, x.Stats!)));

    /// <summary>The <c>146: optional struct content_stats</c> field for a schema.</summary>
    public static NestedField ContentStatsField(Schema schema) => NestedField.Optional(ContentStatsFieldId, "content_stats", ContentStatsType(schema));

    /// <summary>
    /// One field's stats as a value of <paramref name="statsType"/> (from <see cref="StatsStruct"/> or a manifest),
    /// or null when nothing is known. Members the struct lacks are dropped; so are bounds known only as bytes
    /// (<see cref="ColumnStats.RawLowerBound"/> with no typed value), which v4 cannot carry, and the v1–v3-only
    /// <see cref="ColumnStats.ColumnSize"/>.
    /// </summary>
    public static object?[]? ToStruct(StructType statsType, ColumnStats stats)
    {
        var b = BaseId(stats.FieldId) ?? throw new ArgumentException($"Field {stats.FieldId} has no content_stats range");
        var values = new object?[statsType.Fields.Count];
        var any = false;
        for (var i = 0; i < values.Length; i++)
        {
            var member = statsType.Fields[i];
            values[i] = (member.Id - b) switch
            {
                LowerBound => BoundToStruct(member.Type, stats.LowerBound, b),
                UpperBound => BoundToStruct(member.Type, stats.UpperBound, b),
                TightBounds => stats.TightBounds,
                ValueCount => stats.ValueCount,
                NullValueCount => stats.NullValueCount,
                NanValueCount => stats.NanValueCount,
                TotalBytes => stats.TotalBytes,
                _ => null,
            };
            any |= values[i] is not null;
        }
        return any ? values : null;
    }

    /// <summary>
    /// One field's stats from a value of <paramref name="statsType"/>, members resolved by ID offset. Bounds are
    /// promoted to <paramref name="fieldType"/> (int → long, float → double, date → timestamp in the read schema), as the
    /// spec asks readers of older stats to do.
    /// </summary>
    public static ColumnStats FromStruct(int fieldId, StructType statsType, IReadOnlyList<object?>? values, IcebergType? fieldType)
    {
        var b = BaseId(fieldId) ?? throw new ArgumentException($"Field {fieldId} has no content_stats range");
        var s = new ColumnStats { FieldId = fieldId, Type = fieldType };
        if (values is null) return s;
        if (values.Count != statsType.Fields.Count) throw new ArgumentException($"Stats struct for field {fieldId} has {values.Count} values for {statsType.Fields.Count} fields");
        for (var i = 0; i < values.Count; i++)
        {
            var v = values[i];
            if (v is null) continue;
            var member = statsType.Fields[i];
            s = (member.Id - b) switch
            {
                LowerBound => s with { LowerBound = BoundFromStruct(member.Type, v, b, fieldType) },
                UpperBound => s with { UpperBound = BoundFromStruct(member.Type, v, b, fieldType) },
                TightBounds => s with { TightBounds = (bool)v },
                ValueCount => s with { ValueCount = (long)v },
                NullValueCount => s with { NullValueCount = (long)v },
                NanValueCount => s with { NanValueCount = (long)v },
                TotalBytes => s with { TotalBytes = (long)v },
                _ => s, // a member this reader does not know
            };
        }
        return s;
    }

    /// <summary>A whole file's stats as a value of <paramref name="contentStatsType"/>; fields it lacks are dropped.</summary>
    public static object?[] ToContentStats(StructType contentStatsType, ContentStats stats)
    {
        var values = new object?[contentStatsType.Fields.Count];
        for (var i = 0; i < values.Length; i++)
        {
            var f = contentStatsType.Fields[i];
            if (FieldIdOf(f.Id) is { } fieldId && stats[fieldId] is { } s && f.Type is StructType st) values[i] = ToStruct(st, s);
        }
        return values;
    }

    /// <summary>
    /// A whole file's stats from a <c>content_stats</c> value, each field resolved by its base ID and its bounds typed
    /// by <paramref name="typeOf"/> (the read schema). Structs that are null, or for fields the schema does not have,
    /// are skipped: the spec says stats for removed fields are discarded.
    /// </summary>
    public static ContentStats FromContentStats(StructType contentStatsType, IReadOnlyList<object?>? values, Func<int, IcebergType?> typeOf)
    {
        if (values is null) return ContentStats.Empty;
        if (values.Count != contentStatsType.Fields.Count) throw new ArgumentException($"content_stats has {values.Count} values for {contentStatsType.Fields.Count} fields");
        var result = new List<ColumnStats>();
        for (var i = 0; i < values.Count; i++)
        {
            var f = contentStatsType.Fields[i];
            if (values[i] is not IReadOnlyList<object?> v || f.Type is not StructType st || FieldIdOf(f.Id) is not { } fieldId) continue;
            if (typeOf(fieldId) is not { } type) continue;
            var s = FromStruct(fieldId, st, v, type);
            if (!s.IsEmpty) result.Add(s);
        }
        return new ContentStats(result);
    }

    private static object? BoundToStruct(IcebergType memberType, object? bound, int b)
    {
        if (bound is null) return null;
        if (memberType is not StructType geo) return bound;
        if (bound is not GeoPoint p) throw new ArgumentException($"A geo bound must be a {nameof(GeoPoint)}, not {bound.GetType().Name}");
        var values = new object?[geo.Fields.Count];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = ((geo.Fields[i].Id - b - GeoLowerX) % 4) switch
            {
                0 => p.X,
                1 => p.Y,
                2 => p.Z,
                _ => p.M,
            };
        }
        return values;
    }

    private static object BoundFromStruct(IcebergType memberType, object value, int b, IcebergType? fieldType)
    {
        if (memberType is StructType geo)
        {
            double? x = null, y = null, z = null, m = null;
            var v = (IReadOnlyList<object?>)value;
            for (var i = 0; i < geo.Fields.Count; i++)
            {
                var d = (double?)v[i];
                switch ((geo.Fields[i].Id - b - GeoLowerX) % 4)
                {
                    case 0: x = d; break;
                    case 1: y = d; break;
                    case 2: z = d; break;
                    default: m = d; break;
                }
            }
            return new GeoPoint(x ?? throw new FormatException("Geo bound without x"), y ?? throw new FormatException("Geo bound without y"), z, m);
        }
        return Promote(value, fieldType);
    }

    /// <summary>Spec type promotions applied to a bound read as an older type.</summary>
    private static object Promote(object value, IcebergType? to) => (value, to?.TypeId) switch
    {
        (int i, TypeId.Long) => (long)i,
        (float f, TypeId.Double) => (double)f,
        (int days, TypeId.Timestamp or TypeId.TimestampTz) => days * 86_400_000_000L,
        (int days, TypeId.TimestampNs or TypeId.TimestampTzNs) => days * 86_400_000_000_000L,
        _ => value,
    };
}
