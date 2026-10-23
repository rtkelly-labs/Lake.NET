using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Spec Appendix D, JSON single-value serialization: the spec's own examples, both directions.</summary>
[Spec("JSON single-value serialization")]
public sealed class SingleValueJsonTests
{
    public static TheoryData<IcebergType, object, string> Examples() => new()
    {
        { PrimitiveType.Boolean, true, "true" },
        { PrimitiveType.Int, 34, "34" },
        { PrimitiveType.Long, 34L, "34" },
        { PrimitiveType.Double, 1.5, "1.5" },
        { new DecimalType(9, 2), new IcebergDecimal(1420, 2), "\"14.20\"" },
        { PrimitiveType.Date, IcebergTime.DaysFromDate(new DateOnly(2017, 11, 16)), "\"2017-11-16\"" },
        { PrimitiveType.Time, (22 * 3600 + 31 * 60 + 8) * 1_000_000L + 123456, "\"22:31:08.123456\"" },
        { PrimitiveType.Timestamp, 1510871468123456L, "\"2017-11-16T22:31:08.123456\"" },
        { PrimitiveType.TimestampTz, 1510871468123456L, "\"2017-11-16T22:31:08.123456+00:00\"" },
        { PrimitiveType.TimestampNs, 1510871468123456789L, "\"2017-11-16T22:31:08.123456789\"" },
        { PrimitiveType.TimestampTzNs, 1510871468123456789L, "\"2017-11-16T22:31:08.123456789+00:00\"" },
        { PrimitiveType.String, "iceberg", "\"iceberg\"" },
        { PrimitiveType.Uuid, Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7"), "\"f79c3e09-677c-4bbd-a479-3f349cb785e7\"" },
        { PrimitiveType.Binary, new byte[] { 0, 1, 2, 0xff }, "\"000102ff\"" },
        { PrimitiveType.Timestamp, -1L, "\"1969-12-31T23:59:59.999999\"" }, // floor, not truncation, before the epoch
    };

    [Theory]
    [MemberData(nameof(Examples))]
    public void Spec_examples_round_trip(IcebergType type, object value, string json)
    {
        // Compare JSON values, not text (System.Text.Json escapes '+' as \u002B; the string is the same).
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(json), SingleValueJson.ToJson(type, value)), $"{type}: {SingleValueJson.ToJson(type, value)!.ToJsonString()}");
        Assert.Equal(value, SingleValueJson.FromJson(type, JsonNode.Parse(json)));
    }

    [Fact]
    public void Nested_values_use_field_ids_and_key_value_arrays()
    {
        var s = new StructType([NestedField.Optional(1, "a", PrimitiveType.Int), NestedField.Optional(2, "b", PrimitiveType.String)]);
        Assert.Equal("{\"1\":1,\"2\":\"bar\"}", SingleValueJson.ToJson(s, new object?[] { 1, "bar" })!.ToJsonString());
        Assert.Equal(new object?[] { 1, "bar" }, (object?[])SingleValueJson.FromJson(s, JsonNode.Parse("{\"1\": 1, \"2\": \"bar\"}"))!);
        var l = new ListType(3, PrimitiveType.Int, true);
        Assert.Equal("[1,2,3]", SingleValueJson.ToJson(l, new List<object?> { 1, 2, 3 })!.ToJsonString());
        var m = new MapType(4, PrimitiveType.String, 5, PrimitiveType.Int, true);
        Assert.Equal("{\"keys\":[\"a\",\"b\"],\"values\":[1,2]}",
            SingleValueJson.ToJson(m, new List<KeyValuePair<object, object?>> { new("a", 1), new("b", 2) })!.ToJsonString());
    }

    [Theory]
    [InlineData("timestamptz", "\"2017-11-16T22:31:08.123456\"")]   // must carry +00:00
    [InlineData("timestamptz", "\"2017-11-16T22:31:08.123456+01:00\"")]
    [InlineData("timestamp", "\"2017-11-16T22:31:08.123456+00:00\"")] // must not carry an offset
    [InlineData("timestamp", "\"2017-11-16T22:31:08.1234567\"")]      // more than microsecond precision
    public void Malformed_timestamps_are_rejected(string type, string json) =>
        Assert.Throws<FormatException>(() => SingleValueJson.FromJson(type == "timestamp" ? PrimitiveType.Timestamp : PrimitiveType.TimestampTz, JsonNode.Parse(json)));
}
