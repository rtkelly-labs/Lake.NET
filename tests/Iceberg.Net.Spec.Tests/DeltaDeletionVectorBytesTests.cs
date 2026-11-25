using System.Buffers.Binary;
using System.Text.Json.Nodes;
using Iceberg.Net.Puffin;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Delta and Iceberg v3 share one deletion-vector byte layout: a 4-byte big-endian length, the magic <c>D1 D3 39 64</c>,
/// a Roaring64 bitmap in the portable format, and a big-endian CRC-32. The shared Roaring64 layer (decision: it moves to
/// Lake.Core when Delta lands; docs/lake/reference/formats/puffin-and-deletion-vectors.md) rests on that claim, so it is
/// pinned against a deletion vector Spark/Delta wrote (fixtures/delta-rs/table-with-dv-small, copied from delta-rs),
/// not against the specs' prose.
/// </summary>
public sealed class DeltaDeletionVectorBytesTests
{
    private static readonly string Table = Path.Combine(AppContext.BaseDirectory, "fixtures", "delta-rs", "table-with-dv-small");

    private static (byte[] File, int Offset, int SizeInBytes, long Cardinality) DeltaVector()
    {
        var add = File.ReadLines(Path.Combine(Table, "_delta_log", "00000000000000000001.json"))
            .Select(l => JsonNode.Parse(l)!).Single(a => a["add"] is not null)["add"]!;
        var dv = add["deletionVector"]!;
        Assert.Equal("u", dv["storageType"]!.GetValue<string>());
        var file = Directory.GetFiles(Table, "deletion_vector_*.bin").Single();
        return (File.ReadAllBytes(file), dv["offset"]!.GetValue<int>(), dv["sizeInBytes"]!.GetValue<int>(), dv["cardinality"]!.GetValue<long>());
    }

    [Spec("Deletion Vectors")]
    [Fact]
    public void A_Delta_deletion_vector_file_record_is_an_Iceberg_deletion_vector_blob()
    {
        var (file, offset, size, cardinality) = DeltaVector();
        Assert.Equal(1, file[0]); // Delta's DV file format version, before the first record
        // The record at `offset`: the size Delta records (magic + bitmap) as a big-endian int, then those bytes, then a CRC.
        Assert.Equal(size, BinaryPrimitives.ReadInt32BigEndian(file.AsSpan(offset, 4)));
        var record = file.AsSpan(offset, 4 + size + 4);

        var positions = DeletionVector.Decode(record);
        Assert.Equal(cardinality, positions.Cardinality);
        Assert.Equal(new long[] { 0, 9 }, positions.Positions().Order().ToArray()); // DELETE … WHERE value IN (0, 9)

        // And the other way: Iceberg's encoding of the same positions is byte-for-byte Delta's record, CRC included.
        var ours = new RoaringPositionBitmap();
        ours.AddRange([0, 9]);
        Assert.Equal(record.ToArray(), DeletionVector.Encode(ours));
    }

    [Spec("Deletion Vectors")]
    [Fact]
    public void The_sizes_differ_by_the_framing_Iceberg_counts_and_Delta_does_not()
    {
        // Iceberg's content-size-in-bytes covers length + magic + bitmap + CRC; Delta's sizeInBytes covers magic + bitmap.
        var (_, _, size, _) = DeltaVector();
        var ours = new RoaringPositionBitmap();
        ours.AddRange([0, 9]);
        Assert.Equal(size + 8, DeletionVector.Encode(ours).Length);
    }
}
