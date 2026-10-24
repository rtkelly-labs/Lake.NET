using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The v4 <c>file</c> type (spec "File Type" on apache/iceberg main; not in the pinned 1.12.0 sections): JSON form,
/// the seven reserved field IDs, its restrictions, and refusal below format v4.
/// </summary>
[Spec("Schemas and Data Types")]
public sealed class FileTypeTests
{
    private static Schema WithFile() => new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "doc", FileType.Instance),
        NestedField.Optional(9, "after", PrimitiveType.String),
    ]);

    [Fact]
    public void Serializes_as_the_string_file_and_round_trips()
    {
        var json = TypeJson.SchemaToJson(WithFile());
        Assert.Equal("file", json["fields"]![1]!["type"]!.GetValue<string>());
        Assert.Equal(3, json["fields"]!.AsArray().Count); // sub-fields are implicit, never in the JSON
        var back = TypeJson.SchemaFromJson(JsonNode.Parse(json.ToJsonString())!);
        Assert.True(back.SameSchema(WithFile()));
        Assert.Same(FileType.Instance, back.FindType(2));
        Assert.Equal("file", FileType.Instance.ToString());
    }

    [Spec("Reserved Field IDs")]
    [Fact]
    public void A_file_field_reserves_its_id_and_the_next_six()
    {
        var schema = WithFile();
        Assert.Equal(9, schema.HighestFieldId);
        Assert.Equal(["uri", "offset", "size", "content_type", "checksum", "inline"], Enumerable.Range(3, 6).Select(id => schema.FindField(id)!.Name));
        Assert.Equal(PrimitiveType.Binary, schema.FindType(8));
        Assert.Equal("doc.content_type", schema.FindColumnName(6));
        Assert.Equal(4, schema.FindField("doc.offset")!.Id);

        // Another field may not take a reserved ID.
        Assert.Throws<ArgumentException>(() => new Schema(NestedField.Optional(2, "doc", FileType.Instance), NestedField.Optional(5, "clash", PrimitiveType.Int)));
        Assert.Equal(8, new Schema(NestedField.Optional(2, "doc", FileType.Instance)).HighestFieldId); // last-column-id must cover all 7
        Assert.Throws<ArgumentOutOfRangeException>(() => FileType.SubFields(int.MaxValue - 3));
    }

    [Fact]
    public void Fresh_ids_skip_the_reserved_sub_field_ids()
    {
        var schema = new Schema(NestedField.Optional(50, "doc", FileType.Instance), NestedField.Optional(60, "n", PrimitiveType.Int),
            NestedField.Optional(61, "docs", new ListType(62, FileType.Instance, false)));
        var next = 0;
        var fresh = TypeUtil.AssignFreshIds(schema, () => ++next);
        Assert.Equal(1, fresh.FindField("doc")!.Id);
        Assert.Equal(8, fresh.FindField("n")!.Id);
        Assert.Equal(9, fresh.FindField("docs")!.Id);
        Assert.Equal(10, fresh.FindField("docs.element")!.Id);
        Assert.Equal(16, fresh.HighestFieldId);
        Assert.Equal(16, next);
    }

    [Fact]
    public void Is_neither_a_primitive_nor_a_struct()
    {
        Assert.False(FileType.Instance.IsPrimitive);
        var asStruct = new StructType(FileType.SubFields(2));
        Assert.NotEqual<IcebergType>(asStruct, FileType.Instance);
        Assert.False(TypeUtil.IsPromotionAllowed(asStruct, FileType.Instance, 3));
        Assert.False(TypeUtil.IsPromotionAllowed(FileType.Instance, asStruct, 3));
        // Not an identifier field, not a partition or sort source.
        Assert.Throws<ArgumentException>(() => new Schema(0, [NestedField.Require(1, "doc", FileType.Instance)], [1]));
        Assert.False(Transform.Identity.CanTransform(FileType.Instance));
        Assert.False(Transform.Bucket(4).CanTransform(FileType.Instance));
        Assert.False(Transform.Truncate(4).CanTransform(FileType.Instance));
    }

    [Spec("Content Stats")]
    [Fact]
    public void Stats_are_tracked_per_sub_field()
    {
        var t = ContentStatsLayout.ContentStatsType(WithFile());
        Assert.Equal(["id", "doc.uri", "doc.offset", "doc.size", "doc.content_type", "doc.checksum", "doc.inline", "after"], t.Fields.Select(f => f.Name));
        Assert.Null(ContentStatsLayout.StatsStruct(NestedField.Optional(2, "doc", FileType.Instance)));
    }

    [Spec("Format Versioning")]
    [Fact]
    public void Refused_below_format_v4()
    {
        Assert.Equal(4, FileType.Instance.MinFormatVersion);
        Assert.Equal(4, WithFile().MinFormatVersion);
        foreach (var v in new[] { 1, 2, 3 })
            Assert.Throws<ArgumentException>(() => TableMetadata.NewTable(WithFile(), PartitionSpec.Unpartitioned, null, "s3://b/t", formatVersion: v));

        var v3 = TableMetadata.NewTable(new Schema(NestedField.Require(1, "id", PrimitiveType.Long)), PartitionSpec.Unpartitioned, null, "s3://b/t", formatVersion: 3);
        Assert.Throws<InvalidOperationException>(() => new SchemaUpdate(v3).AddColumn("doc", FileType.Instance).Apply());

        // Metadata that claims v3 but has a file column is rejected on read.
        var json = JsonNode.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "fixtures", "metadata", "v3-valid.metadata.json")))!;
        MetadataJson.FromJson(json);
        json["schemas"]![0]!["fields"]!.AsArray().Add(new JsonObject { ["id"] = 900, ["name"] = "doc", ["required"] = false, ["type"] = "file" });
        Assert.Throws<InvalidDataException>(() => MetadataJson.FromJson(json));
    }
}
