namespace Iceberg.Net.Spec.Tests;

public sealed class TargetTests
{
    /// <summary>Guards the point of this project: the library under test is the netstandard2.0 build.</summary>
    [Fact]
    public void Core_and_avro_are_the_netstandard2_0_builds()
    {
        foreach (var asm in new[] { typeof(Types.Schema).Assembly, typeof(Avro.ManifestCodec).Assembly })
        {
            var tfm = asm.GetCustomAttributes(typeof(System.Runtime.Versioning.TargetFrameworkAttribute), false)
                .Cast<System.Runtime.Versioning.TargetFrameworkAttribute>().Single().FrameworkName;
            Assert.Equal(".NETStandard,Version=v2.0", tfm);
        }
    }
}

/// <summary>Each polyfilled path against values fixed by the spec or by .NET 8's own behaviour.</summary>
public sealed class PolyfillTests
{
    private static readonly Guid Uuid = Guid.Parse("f79c3e09-677c-4bbd-a479-3f349cb785e7");

    [Fact]
    public void Uuid_bytes_are_big_endian_both_ways()
    {
        var bytes = Types.Conversions.ToBytes(Types.PrimitiveType.Uuid, Uuid);
        Assert.Equal("f79c3e09677c4bbda4793f349cb785e7", string.Concat(bytes.Select(b => b.ToString("x2"))));
        Assert.Equal(Uuid, Types.Conversions.FromBytes(Types.PrimitiveType.Uuid, bytes));
    }

    [Theory]
    [InlineData("14.20", "058c")]
    [InlineData("-1.00", "9c")]
    [InlineData("0.00", "00")]
    [InlineData("-128.00", "ce00")]
    public void Decimal_bytes_are_minimal_big_endian_twos_complement(string value, string hex)
    {
        var type = new Types.DecimalType(9, 2);
        var bytes = Types.Conversions.ToBytes(type, Types.IcebergDecimal.Parse(value));
        Assert.Equal(hex, string.Concat(bytes.Select(b => b.ToString("x2"))));
        Assert.Equal(Types.IcebergDecimal.Parse(value), Types.Conversions.FromBytes(type, bytes));
    }

    [Theory]
    [InlineData(1.5f, "0000c03f")]
    [InlineData(-0.0f, "00000080")]
    public void Float_bytes_are_little_endian(float f, string hex)
    {
        var bytes = Types.Conversions.ToBytes(Types.PrimitiveType.Float, f);
        Assert.Equal(hex, string.Concat(bytes.Select(b => b.ToString("x2"))));
        Assert.Equal(BitConverter.SingleToInt32Bits(f), BitConverter.SingleToInt32Bits((float)Types.Conversions.FromBytes(Types.PrimitiveType.Float, bytes)));
        Assert.Equal((double)f, Types.Conversions.FromBytes(Types.PrimitiveType.Double, bytes)); // a promoted column's 4-byte bound
    }

    [Fact]
    public void Double_bytes_are_little_endian()
    {
        var bytes = Types.Conversions.ToBytes(Types.PrimitiveType.Double, Math.PI);
        Assert.Equal("182d4454fb210940", string.Concat(bytes.Select(b => b.ToString("x2"))));
        Assert.Equal(Math.PI, Types.Conversions.FromBytes(Types.PrimitiveType.Double, bytes));
    }

    [Fact]
    public void Binary_single_values_are_lower_hex_and_parse_either_case()
    {
        var json = Json.SingleValueJson.ToJson(Types.PrimitiveType.Binary, new byte[] { 0x00, 0xAB, 0xFF });
        Assert.Equal("\"00abff\"", json!.ToJsonString());
        Assert.Equal(new byte[] { 0x00, 0xAB, 0xFF }, Json.SingleValueJson.FromJson(Types.PrimitiveType.Binary, System.Text.Json.Nodes.JsonValue.Create("00ABff")));
        Assert.Throws<FormatException>(() => Json.SingleValueJson.FromJson(Types.PrimitiveType.Binary, System.Text.Json.Nodes.JsonValue.Create("0g")));
    }

    [Fact]
    public void Truncation_never_splits_a_surrogate_pair()
    {
        var t = Transforms.Transform.Truncate(2);
        Assert.Equal("a\U0001F600", t.Apply(Types.PrimitiveType.String, "a\U0001F600b"));
        Assert.Equal("ab", t.Apply(Types.PrimitiveType.String, "abc"));
    }

    [Fact]
    public void Bucket_hashes_match_the_spec_vectors()
    {
        Assert.Equal(1488055340, Transforms.Murmur3.HashUuid(Uuid));
        Assert.Equal(-500754589, Transforms.Murmur3.HashDecimal(Types.IcebergDecimal.Parse("14.20").Unscaled));
        Assert.Equal(1210000089, Transforms.Murmur3.HashString("iceberg"));
        Assert.Equal(-188683207, Transforms.Murmur3.HashBytes(new byte[] { 0, 1, 2, 3 }));
    }
}
