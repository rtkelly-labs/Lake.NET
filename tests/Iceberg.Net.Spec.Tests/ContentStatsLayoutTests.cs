using Iceberg.Net.Manifests;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The v4 <c>content_stats</c> layout (spec "Content Stats" on apache/iceberg main): field IDs, struct shapes from the
/// spec's own examples, and the mapping to and from the typed model.
/// </summary>
[Spec("Content Stats")]
public sealed class ContentStatsLayoutTests
{
    [Spec("Field Statistics")]
    [Fact]
    public void Stats_ids_follow_ten_thousand_plus_two_hundred_times_the_field_id()
    {
        Assert.Equal(10_000, ContentStatsLayout.BaseId(0));
        Assert.Equal(10_400, ContentStatsLayout.BaseId(2));
        Assert.Equal(10_401, ContentStatsLayout.BaseId(2) + ContentStatsLayout.LowerBound);
        Assert.Equal(199_999_800, ContentStatsLayout.BaseId(999_949)); // last field whose range fits below 200_000_000
        Assert.Null(ContentStatsLayout.BaseId(999_950));
        Assert.Null(ContentStatsLayout.BaseId(int.MaxValue - 200)); // no overflow
        Assert.Null(ContentStatsLayout.BaseId(-1));
        // Reserved metadata columns with their own ranges; other metadata columns are not tracked.
        Assert.Equal(9000, ContentStatsLayout.BaseId(2147483539)); // _last_updated_sequence_number
        Assert.Equal(9200, ContentStatsLayout.BaseId(2147483540)); // _row_id
        Assert.Null(ContentStatsLayout.BaseId(2147483546)); // file_path

        Assert.Equal(2, ContentStatsLayout.FieldIdOf(10_400));
        Assert.Equal(2147483540, ContentStatsLayout.FieldIdOf(9200));
        Assert.Null(ContentStatsLayout.FieldIdOf(10_401));
        Assert.Null(ContentStatsLayout.FieldIdOf(9_999));
        Assert.Null(ContentStatsLayout.FieldIdOf(200_000_000));
    }

    private static string Describe(StructType t) => string.Join("; ", t.Fields.Select(f => f.Type is StructType s
        ? $"{f.Id} {(f.Required ? "required" : "optional")} {f.Name} {{{Describe(s)}}}"
        : $"{f.Id} {(f.Required ? "required" : "optional")} {f.Type} {f.Name}"));

    [Spec("Content Stats in Manifests")]
    [Fact]
    public void The_spec_examples_have_the_same_shape()
    {
        // "stats for a table with a required int, id, and an optional string, data"
        var schema = new Schema(NestedField.Require(2, "id", PrimitiveType.Int), NestedField.Optional(3, "data", PrimitiveType.String));
        var field = ContentStatsLayout.ContentStatsField(schema);
        Assert.Equal(146, field.Id);
        Assert.False(field.Required);
        Assert.Equal(
            "10400 optional id {10401 optional int lower_bound; 10402 optional int upper_bound; 10403 optional boolean tight_bounds; 10404 optional long value_count}; " +
            "10600 optional data {10601 optional string lower_bound; 10602 optional string upper_bound; 10603 optional boolean tight_bounds; 10604 optional long value_count; " +
            "10605 optional long null_value_count; 10607 optional long total_bytes}",
            Describe((StructType)field.Type));
    }

    [Spec("Field Statistics")]
    [Fact]
    public void Geo_stats_use_geo_lower_and_upper_structs_without_tight_bounds_or_nan_counts()
    {
        // The spec's "optional geometry field named location with field-id 4".
        var stats = ContentStatsLayout.StatsStruct(NestedField.Optional(4, "location", new GeometryType()))!;
        Assert.Equal(
            "10801 optional lower_bound {10810 required double x; 10811 required double y; 10812 optional double z; 10813 optional double m}; " +
            "10802 optional upper_bound {10814 required double x; 10815 required double y; 10816 optional double z; 10817 optional double m}; " +
            "10804 optional long value_count; 10805 optional long null_value_count; 10807 optional long total_bytes",
            Describe(stats));
    }

    [Spec("Field Statistics")]
    [Fact]
    public void Members_depend_on_type_and_requirement()
    {
        string Names(NestedField f) => string.Join(",", ContentStatsLayout.StatsStruct(f)!.Fields.Select(m => m.Name));
        Assert.Equal("lower_bound,upper_bound,tight_bounds,value_count,null_value_count,nan_value_count", Names(NestedField.Optional(1, "x", PrimitiveType.Double)));
        Assert.Equal("lower_bound,upper_bound,tight_bounds,value_count,nan_value_count", Names(NestedField.Require(1, "x", PrimitiveType.Float)));
        Assert.Equal("lower_bound,upper_bound,tight_bounds,value_count,null_value_count,total_bytes", Names(NestedField.Optional(1, "v", PrimitiveType.Variant)));
        Assert.Equal("lower_bound,upper_bound,tight_bounds,value_count,total_bytes", Names(NestedField.Require(1, "b", PrimitiveType.Binary)));
        Assert.Equal("value_count,null_value_count", Names(NestedField.Optional(1, "u", PrimitiveType.Unknown)));
        Assert.Null(ContentStatsLayout.StatsStruct(NestedField.Optional(1, "s", new StructType(NestedField.Optional(2, "a", PrimitiveType.Int)))));
        Assert.Null(ContentStatsLayout.StatsStruct(NestedField.Optional(999_950, "far", PrimitiveType.Int)));
    }

    [Fact]
    public void Nested_leaves_get_stats_named_by_full_name_in_id_order()
    {
        var schema = new Schema(
            NestedField.Optional(5, "tags", new ListType(6, PrimitiveType.String, true)),
            NestedField.Require(1, "loc", new StructType(NestedField.Optional(2, "lat", PrimitiveType.Double))),
            NestedField.Optional(3, "props", new MapType(7, PrimitiveType.String, 8, PrimitiveType.Long, false)));
        var t = ContentStatsLayout.ContentStatsType(schema);
        Assert.Equal(["loc.lat", "tags.element", "props.key", "props.value"], t.Fields.Select(f => f.Name));
        Assert.Equal([10_400, 11_200, 11_400, 11_600], t.Fields.Select(f => f.Id));
    }

    public static TheoryData<IcebergType, object, object> TypedBounds() => new()
    {
        { PrimitiveType.Boolean, false, true },
        { PrimitiveType.Int, -1, 7 },
        { PrimitiveType.Long, 1L, 2L },
        { PrimitiveType.Float, -0.0f, 1.5f },
        { PrimitiveType.Double, double.NegativeInfinity, 2.5 },
        { new DecimalType(9, 2), new IcebergDecimal(-1, 2), new IcebergDecimal(1234, 2) },
        { PrimitiveType.Date, 0, 17486 },
        { PrimitiveType.Time, 0L, 1L },
        { PrimitiveType.Timestamp, 1L, 2L },
        { PrimitiveType.TimestampTz, 1L, 2L },
        { PrimitiveType.TimestampNs, 1L, 2L },
        { PrimitiveType.TimestampTzNs, 1L, 2L },
        { PrimitiveType.String, "a", "\U0001F600" },
        { PrimitiveType.Uuid, Guid.Empty, Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7") },
        { new FixedType(2), new byte[] { 0, 1 }, new byte[] { 2, 3 } },
        { PrimitiveType.Binary, new byte[] { }, new byte[] { 9 } },
        { new GeometryType(), new GeoPoint(1, 2), new GeoPoint(3, 4, 5, 6) },
        { new GeographyType(), new GeoPoint(170, -10, null, 1), new GeoPoint(-170, 10, 2) },
        { PrimitiveType.Variant, new VariantBound([1, 0, 0]), new VariantBound([1, 0, 1]) },
    };

    /// <summary>v1–v3 maps → typed → v4 struct → typed → v1–v3 maps, byte for byte; tight_bounds survives v4.</summary>
    [Spec("Field-level Metrics and Statistics")]
    [Theory]
    [MemberData(nameof(TypedBounds))]
    public void Every_type_round_trips_through_the_v4_struct(IcebergType type, object lower, object upper)
    {
        var field = NestedField.Optional(3, "c", type);
        var statsType = ContentStatsLayout.StatsStruct(field)!;
        var stats = new ColumnStats
        {
            FieldId = 3, Type = type, LowerBound = lower, UpperBound = upper, ValueCount = 5, NullValueCount = 1,
            NanValueCount = type.TypeId is TypeId.Float or TypeId.Double ? 0 : null,
            TightBounds = type.TypeId is TypeId.Geometry or TypeId.Geography ? null : true,
            TotalBytes = statsType.Fields.Any(f => f.Name == "total_bytes") ? 64 : null,
        };
        var v4 = ContentStatsLayout.ToStruct(statsType, stats)!;
        Assert.Equal(statsType.Fields.Count, v4.Length);
        Assert.Equal(stats, ContentStatsLayout.FromStruct(3, statsType, v4, type));

        // Through the v1-v3 maps too: what both forms carry is unchanged.
        var file = ContentStats.Empty.ApplyTo(new DataFile { FilePath = "f", RecordCount = 5, FileSizeInBytes = 1 });
        file = new ContentStats([stats]).ApplyTo(file);
        var fromMaps = ContentStats.FromDataFile(file, _ => type)[3]!;
        Assert.Equal(stats with { TightBounds = null, TotalBytes = null }, fromMaps);
        var again = ContentStatsLayout.FromStruct(3, statsType, ContentStatsLayout.ToStruct(statsType, fromMaps), type);
        Assert.Equal(fromMaps, again);
    }

    [Spec("Content Stats in Manifests")]
    [Fact]
    public void A_whole_file_maps_by_id_and_skips_dropped_fields()
    {
        var schema = new Schema(NestedField.Require(2, "id", PrimitiveType.Int), NestedField.Optional(3, "data", PrimitiveType.String));
        var type = ContentStatsLayout.ContentStatsType(schema);
        var stats = new ContentStats(
        [
            new ColumnStats { FieldId = 2, Type = PrimitiveType.Int, LowerBound = 1, UpperBound = 9, ValueCount = 3, TightBounds = true },
            new ColumnStats { FieldId = 3, Type = PrimitiveType.String, NullValueCount = 3, ValueCount = 3 },
            new ColumnStats { FieldId = 4, Type = PrimitiveType.Long, ValueCount = 3 }, // not in this content_stats type
        ]);
        var value = ContentStatsLayout.ToContentStats(type, stats);
        Assert.Equal(new object?[] { 1, 9, true, 3L }, (object?[])value[0]!);
        Assert.Equal(new object?[] { null, null, null, 3L, 3L, null }, (object?[])value[1]!);

        var back = ContentStatsLayout.FromContentStats(type, value, schema.FindType);
        Assert.Equal([2, 3], back.Fields.Keys);
        Assert.Equal(stats[2], back[2]);
        Assert.Equal(stats[3], back[3]);

        // After "data" is dropped its stats are discarded (spec: removed fields).
        var dropped = new Schema(NestedField.Require(2, "id", PrimitiveType.Int));
        Assert.Equal([2], ContentStatsLayout.FromContentStats(type, value, dropped.FindType).Fields.Keys);
        // Nothing known about a field: its struct is null.
        Assert.Null(ContentStatsLayout.ToContentStats(type, new ContentStats([new ColumnStats { FieldId = 2 }]))[0]);
    }

    [Spec("Content Stats in Manifests")]
    [Fact]
    public void Bounds_written_with_an_older_type_are_promoted_by_the_read_schema()
    {
        var oldType = ContentStatsLayout.StatsStruct(NestedField.Optional(1, "n", PrimitiveType.Int))!;
        var v = ContentStatsLayout.ToStruct(oldType, new ColumnStats { FieldId = 1, LowerBound = 3, UpperBound = 4 })!;
        var asLong = ContentStatsLayout.FromStruct(1, oldType, v, PrimitiveType.Long);
        Assert.Equal((3L, 4L), ((long)asLong.LowerBound!, (long)asLong.UpperBound!));

        var oldFloat = ContentStatsLayout.StatsStruct(NestedField.Optional(1, "x", PrimitiveType.Float))!;
        var f = ContentStatsLayout.FromStruct(1, oldFloat, ContentStatsLayout.ToStruct(oldFloat, new ColumnStats { FieldId = 1, LowerBound = 1.5f })!, PrimitiveType.Double);
        Assert.Equal(1.5, f.LowerBound);

        var oldDate = ContentStatsLayout.StatsStruct(NestedField.Optional(1, "d", PrimitiveType.Date))!;
        var ts = ContentStatsLayout.FromStruct(1, oldDate, ContentStatsLayout.ToStruct(oldDate, new ColumnStats { FieldId = 1, LowerBound = 1 })!, PrimitiveType.Timestamp);
        Assert.Equal(86_400_000_000L, ts.LowerBound);
    }

    [Fact]
    public void Bounds_known_only_as_bytes_have_no_v4_form()
    {
        var statsType = ContentStatsLayout.StatsStruct(NestedField.Optional(1, "n", PrimitiveType.Int))!;
        var undecodable = new ColumnStats { FieldId = 1, Type = PrimitiveType.Int, RawLowerBound = [1, 2], ValueCount = 2 };
        Assert.Equal(new object?[] { null, null, null, 2L, null }, ContentStatsLayout.ToStruct(statsType, undecodable));
    }
}
