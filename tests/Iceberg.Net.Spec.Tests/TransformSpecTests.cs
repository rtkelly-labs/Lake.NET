using System.Text;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Test vectors from the spec, Appendix B ("32-bit Hash Requirements") and "Truncate Transform Details".</summary>
public class TransformSpecTests
{
    [Fact]
    public void Murmur3_matches_spec_vectors()
    {
        Assert.Equal(2017239379, Murmur3.HashInt(34));
        Assert.Equal(2017239379, Murmur3.HashLong(34L));
        Assert.Equal(-142385009, Murmur3.HashDouble(1.0f));
        Assert.Equal(-142385009, Murmur3.HashDouble(1.0d));
        Assert.Equal(-500754589, Murmur3.HashDecimal(IcebergDecimal.Parse("14.20").Unscaled));
        Assert.Equal(-653330422, Murmur3.HashInt(IcebergTime.DaysFromDate(new DateOnly(2017, 11, 16))));
        Assert.Equal(-662762989, Murmur3.HashLong(IcebergTime.MicrosFromTime(new TimeOnly(22, 31, 8))));
        Assert.Equal(-2047944441, Murmur3.HashLong(IcebergTime.MicrosFromTimestamp(new DateTime(2017, 11, 16, 22, 31, 8, DateTimeKind.Utc))));
        Assert.Equal(-2047944441, Murmur3.HashLong(IcebergTime.MicrosFromTimestamp(new DateTimeOffset(2017, 11, 16, 14, 31, 8, TimeSpan.FromHours(-8)))));
        Assert.Equal(1210000089, Murmur3.HashString("iceberg"));
        Assert.Equal(1488055340, Murmur3.HashUuid(Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7")));
        Assert.Equal(-188683207, Murmur3.HashBytes(new byte[] { 0, 1, 2, 3 }));
    }

    [Fact]
    public void Nanosecond_timestamps_bucket_like_their_microsecond_value()
    {
        // Spec: timestamp_ns hashes as hashLong(nanosToMicros(v)) so a micros->nanos migration keeps buckets.
        var micros = IcebergTime.MicrosFromTimestamp(new DateTime(2017, 11, 16, 22, 31, 8, DateTimeKind.Utc)) + 1;
        var nanos = micros * 1000 + 1;
        Assert.Equal(
            BucketTransform.Hash(PrimitiveType.Timestamp, micros),
            BucketTransform.Hash(PrimitiveType.TimestampNs, nanos));
        Assert.Equal(-1207196810, BucketTransform.Hash(PrimitiveType.TimestampNs, nanos));
    }

    [Fact]
    public void Guid_default_byte_order_would_break_uuid_buckets()
    {
        // Regression guard: Guid.ToByteArray() is mixed-endian; only the big-endian form matches the spec.
        var g = Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7");
        Assert.NotEqual(1488055340, Murmur3.HashBytes(g.ToByteArray()));
    }

    [Theory]
    [InlineData(34, 16)]
    public void Bucket_masks_sign_bit_before_mod(int value, int n)
    {
        var expected = (Murmur3.HashInt(value) & int.MaxValue) % n;
        Assert.Equal(expected, Transform.Bucket(n).Apply(PrimitiveType.Int, value));
    }

    [Fact]
    public void Truncate_matches_spec_examples()
    {
        var t10 = Transform.Truncate(10);
        Assert.Equal(0, t10.Apply(PrimitiveType.Int, 1));
        Assert.Equal(-10, t10.Apply(PrimitiveType.Int, -1));
        Assert.Equal(0L, t10.Apply(PrimitiveType.Long, 1L));
        Assert.Equal(-10L, t10.Apply(PrimitiveType.Long, -1L));
        Assert.Equal("10.50", ((IcebergDecimal)Transform.Truncate(50).Apply(new DecimalType(9, 2), IcebergDecimal.Parse("10.65"))!).ToString());
        Assert.Equal("ice", Transform.Truncate(3).Apply(PrimitiveType.String, "iceberg"));
        Assert.Equal(new byte[] { 1, 2, 3 }, Transform.Truncate(3).Apply(PrimitiveType.Binary, new byte[] { 1, 2, 3, 4 }));
    }

    [Fact]
    public void Truncate_counts_code_points_not_utf16_units()
    {
        var s = "a\U0001F600b"; // surrogate pair in the middle
        var truncated = (string)Transform.Truncate(2).Apply(PrimitiveType.String, s)!;
        Assert.Equal("a\U0001F600", truncated);
        Assert.Equal(5, Encoding.UTF8.GetByteCount(truncated));
    }

    [Fact]
    public void Temporal_transforms_floor_towards_negative_infinity()
    {
        var ts = PrimitiveType.Timestamp;
        var beforeEpoch = IcebergTime.MicrosFromTimestamp(new DateTime(1969, 12, 31, 23, 59, 59, DateTimeKind.Utc));
        Assert.Equal(-1, Transform.Year.Apply(ts, beforeEpoch));
        Assert.Equal(-1, Transform.Month.Apply(ts, beforeEpoch));
        Assert.Equal(-1, Transform.Day.Apply(ts, beforeEpoch));
        Assert.Equal(-1, Transform.Hour.Apply(ts, beforeEpoch));

        var t = IcebergTime.MicrosFromTimestamp(new DateTime(2017, 11, 16, 22, 31, 8, DateTimeKind.Utc));
        Assert.Equal(47, Transform.Year.Apply(ts, t));
        Assert.Equal(574, Transform.Month.Apply(ts, t));
        Assert.Equal(17486, Transform.Day.Apply(ts, t));
        Assert.Equal(17486 * 24 + 22, Transform.Hour.Apply(ts, t));
        Assert.Equal(17486, Transform.Day.Apply(PrimitiveType.Date, 17486));
        Assert.Equal(17486, Transform.Day.Apply(PrimitiveType.TimestampNs, t * 1000));
    }

    [Fact]
    public void Human_strings_match_hive_partition_paths()
    {
        Assert.Equal("2017", Transform.Year.ToHumanString(PrimitiveType.Int, 47));
        Assert.Equal("2017-11", Transform.Month.ToHumanString(PrimitiveType.Int, 574));
        Assert.Equal("2017-11-16", Transform.Day.ToHumanString(PrimitiveType.Date, 17486));
        Assert.Equal("2017-11-16-22", Transform.Hour.ToHumanString(PrimitiveType.Int, 17486 * 24 + 22));
        Assert.Equal("null", Transform.Identity.ToHumanString(PrimitiveType.String, null));
    }

    [Theory]
    [InlineData("identity")]
    [InlineData("bucket[16]")]
    [InlineData("truncate[4]")]
    [InlineData("year")]
    [InlineData("month")]
    [InlineData("day")]
    [InlineData("hour")]
    [InlineData("void")]
    [InlineData("zorder")] // unknown transforms must round-trip, not fail
    public void Transform_strings_round_trip(string text) => Assert.Equal(text, Transform.Parse(text).ToString());

    [Fact]
    public void Transform_type_rules()
    {
        Assert.False(Transform.Bucket(4).CanTransform(PrimitiveType.Double));
        Assert.False(Transform.Hour.CanTransform(PrimitiveType.Date));
        Assert.True(Transform.Day.CanTransform(PrimitiveType.TimestampTzNs));
        Assert.Equal(PrimitiveType.Date, Transform.Day.ResultType(PrimitiveType.Timestamp));
        Assert.Equal(PrimitiveType.Int, Transform.Month.ResultType(PrimitiveType.Timestamp));
        Assert.Equal(PrimitiveType.Int, Transform.Bucket(4).ResultType(PrimitiveType.String));
    }

    [Fact]
    public void Partition_spec_builds_type_and_path()
    {
        var schema = new Schema(
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "event_ts", PrimitiveType.TimestampTz),
            NestedField.Optional(3, "region", PrimitiveType.String));
        var spec = PartitionSpec.BuilderFor(schema).Day("event_ts").Bucket("id", 8).Identity("region").Build();

        Assert.Equal([1000, 1001, 1002], spec.Fields.Select(f => f.FieldId));
        Assert.Equal(["event_ts_day", "id_bucket", "region"], spec.Fields.Select(f => f.Name));
        var type = spec.PartitionType(schema);
        Assert.Equal(PrimitiveType.Date, type.Fields[0].Type);

        var ts = IcebergTime.MicrosFromTimestamp(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc));
        var tuple = spec.Partition(schema, id => id switch { 1 => 34L, 2 => ts, 3 => "eu west", _ => null });
        Assert.Equal($"event_ts_day=2024-01-02/id_bucket={(Murmur3.HashLong(34) & int.MaxValue) % 8}/region=eu%20west", spec.PartitionPath(schema, tuple));
    }
}
