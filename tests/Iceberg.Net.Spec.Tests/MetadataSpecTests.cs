using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

public class MetadataSpecTests
{
    private static string Fixture(string name) => File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "fixtures", "metadata", name));

    [Fact]
    public void V1_legacy_fields_are_upgraded_on_read()
    {
        var m = MetadataJson.Read(Fixture("v1-legacy.metadata.json"));
        Assert.Equal(1, m.FormatVersion);
        Assert.Single(m.Schemas);
        Assert.Equal(0, m.CurrentSchemaId);
        // v1 partition fields without field-id get sequential IDs from 1000.
        Assert.Equal(1000, m.DefaultSpec.Fields.Single().FieldId);
        Assert.Equal(1000, m.LastPartitionId);
        Assert.Null(m.CurrentSnapshotId); // -1 means "no snapshot"
        Assert.True(m.DefaultSortOrder.IsUnsorted);
    }

    [Fact]
    public void V1_write_keeps_legacy_fields_for_old_readers()
    {
        var json = JsonNode.Parse(MetadataJson.Write(MetadataJson.Read(Fixture("v1-legacy.metadata.json"))))!;
        Assert.NotNull(json["schema"]);
        Assert.NotNull(json["partition-spec"]);
        Assert.Null(json["last-sequence-number"]);
    }

    [Fact]
    public void V2_round_trips_semantically()
    {
        var m = MetadataJson.Read(Fixture("v2-valid.metadata.json"));
        Assert.Equal(34, m.LastSequenceNumber);
        Assert.Equal([1, 2], m.CurrentSchema.IdentifierFieldIds);
        Assert.Equal("comment", m.CurrentSchema.FindField("y")!.Doc);
        Assert.Equal(3, m.DefaultSortOrder.OrderId);
        Assert.Equal("bucket[4]", m.DefaultSortOrder.Fields[1].Transform.ToString());
        // refs absent => main implied by current-snapshot-id.
        Assert.Equal(3055729675574597004, m.Refs[SnapshotRef.Main].SnapshotId);
        Assert.Equal(1, m.CurrentSnapshot!.SchemaId);
        Assert.Equal([3055729675574597004, 3051729675574597004], m.Ancestors(m.CurrentSnapshotId).Select(s => s.SnapshotId));

        var again = MetadataJson.Read(MetadataJson.Write(m));
        Assert.Equal(MetadataJson.Write(m), MetadataJson.Write(again));
    }

    [Fact]
    public void V3_fields_round_trip()
    {
        var m = MetadataJson.Read(Fixture("v3-valid.metadata.json"));
        Assert.Equal(30, m.NextRowId);
        Assert.Equal(0, m.CurrentSnapshot!.FirstRowId);
        Assert.Equal(30, m.CurrentSnapshot.AddedRows);
        Assert.Equal(PrimitiveType.TimestampTzNs, m.CurrentSchema.FindType(2));
        Assert.Equal(PrimitiveType.Variant, m.CurrentSchema.FindType(3));
        Assert.Equal(new GeographyType("OGC:CRS84", "karney"), m.CurrentSchema.FindType(4));
        Assert.Equal(PrimitiveType.Unknown, m.CurrentSchema.FindType(6));
        Assert.Equal(7, ((JsonNode)m.CurrentSchema.FindField(5)!.InitialDefault!).GetValue<int>());
        Assert.Equal(SnapshotRefType.Tag, m.Refs["audit"].Type);
        Assert.Equal("kms-1", m.EncryptionKeys.Single().EncryptedById);
        Assert.Equal("30", m.Statistics.Single().BlobMetadata.Single().Properties!["ndv"]);
        Assert.Equal(3, m.CurrentSchema.MinFormatVersion);

        var written = JsonNode.Parse(MetadataJson.Write(m))!;
        Assert.Equal(30, written["next-row-id"]!.GetValue<long>());
        Assert.Equal(MetadataJson.Write(m), MetadataJson.Write(MetadataJson.Read(written.ToJsonString())));
    }

    [Theory]
    [InlineData(2, "-1")]
    [InlineData(3, "null")]
    public void Empty_table_current_snapshot_id_matches_java(int formatVersion, string expected)
    {
        var m = TableMetadata.NewTable(new Schema(NestedField.Require(1, "id", PrimitiveType.Long)), null, null, "s3://b/t", formatVersion: formatVersion);
        var json = JsonNode.Parse(MetadataJson.Write(m))!.AsObject();
        Assert.True(json.ContainsKey("current-snapshot-id"));
        Assert.Equal(expected, json["current-snapshot-id"]?.ToJsonString() ?? "null");
        Assert.Null(MetadataJson.Read(json.ToJsonString()).CurrentSnapshotId);
    }

    [Fact]
    public void Unknown_format_version_is_rejected() =>
        Assert.Throws<NotSupportedException>(() => MetadataJson.Read("""{"format-version": 9, "location": "x", "last-updated-ms": 0, "last-column-id": 0}"""));

    [Theory]
    [InlineData("decimal(9,2)")]
    [InlineData("decimal(38, 10)")]
    [InlineData("fixed[16]")]
    [InlineData("geometry(srid:3857)")]
    [InlineData("timestamp_ns")]
    public void Parameterised_types_parse(string text)
    {
        var t = TypeJson.TypeFromJson(JsonValue.Create(text)!);
        Assert.Equal(t, TypeJson.TypeFromJson(TypeJson.ToJson(t)));
    }

    [Fact]
    public void Nested_types_round_trip_with_ids()
    {
        var schema = new Schema(1,
        [
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "tags", new ListType(4, PrimitiveType.String, false)),
            NestedField.Optional(3, "attrs", new MapType(5, PrimitiveType.String, 6, new StructType(
                NestedField.Optional(7, "a", PrimitiveType.Int),
                NestedField.Optional(8, "b", new DecimalType(10, 2))), false)),
        ], [1]);
        var back = TypeJson.SchemaFromJson(TypeJson.SchemaToJson(schema));
        Assert.True(schema.SameSchema(back));
        Assert.Equal(8, back.HighestFieldId);
        Assert.Equal(7, back.FindField("attrs.value.a")!.Id);
        Assert.Equal(4, back.FindField("tags.element")!.Id);
        Assert.Equal("attrs.value.b", back.FindColumnName(8));
    }

    [Fact]
    public void Identifier_fields_must_be_required_non_float_primitives()
    {
        Assert.Throws<ArgumentException>(() => new Schema(0, [NestedField.Optional(1, "x", PrimitiveType.Long)], [1]));
        Assert.Throws<ArgumentException>(() => new Schema(0, [NestedField.Require(1, "x", PrimitiveType.Double)], [1]));
    }

    [Theory]
    [InlineData("int", "long", 2, true)]
    [InlineData("float", "double", 2, true)]
    [InlineData("decimal(9,2)", "decimal(18,2)", 2, true)]
    [InlineData("decimal(9,2)", "decimal(18,3)", 2, false)]
    [InlineData("long", "int", 2, false)]
    [InlineData("date", "timestamp", 2, false)]
    [InlineData("date", "timestamp", 3, true)]
    [InlineData("unknown", "string", 3, true)]
    public void Type_promotion_rules(string from, string to, int formatVersion, bool allowed) =>
        Assert.Equal(allowed, TypeUtil.IsPromotionAllowed(
            TypeJson.TypeFromJson(JsonValue.Create(from)!), TypeJson.TypeFromJson(JsonValue.Create(to)!), formatVersion));

    [Fact]
    public void New_table_assigns_fresh_ids_and_remaps_spec_sources()
    {
        // Placeholder IDs in, fresh depth-first IDs out (siblings before children).
        var schema = new Schema(0,
        [
            NestedField.Require(10, "id", PrimitiveType.Long),
            NestedField.Optional(20, "location", new StructType(NestedField.Optional(30, "lat", PrimitiveType.Double))),
            NestedField.Optional(40, "day", PrimitiveType.Date),
        ]);
        var spec = new PartitionSpec(0, [new PartitionField(40, 5000, "day", Transforms.Transform.Identity)]);
        var m = TableMetadata.NewTable(schema, spec, null, "s3://b/t/", formatVersion: 2);
        Assert.Equal([1, 2, 3], m.CurrentSchema.Columns.Select(c => c.Id));
        Assert.Equal(4, m.CurrentSchema.FindField("location.lat")!.Id);
        Assert.Equal(4, m.LastColumnId);
        Assert.Equal(3, m.DefaultSpec.Fields.Single().SourceId);
        Assert.Equal(1000, m.DefaultSpec.Fields.Single().FieldId);
        Assert.Equal("s3://b/t", m.Location);
    }

    [Fact]
    public void New_table_rejects_v3_types_on_v2()
    {
        var schema = new Schema(NestedField.Optional(1, "v", PrimitiveType.Variant));
        Assert.Throws<ArgumentException>(() => TableMetadata.NewTable(schema, null, null, "s3://b/t", formatVersion: 2));
        Assert.Equal(3, TableMetadata.NewTable(schema, null, null, "s3://b/t", formatVersion: 3).FormatVersion);
    }
}
