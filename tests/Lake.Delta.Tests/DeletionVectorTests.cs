using System.Buffers.Binary;
using System.Text.Json.Nodes;
using Iceberg.Net.Puffin;

namespace Lake.Delta.Tests;

/// <summary>Z85 and the three deletion vector storage types (PROTOCOL.md "Deletion Vectors").</summary>
public sealed class DeletionVectorTests
{
    [Fact]
    public async Task The_delta_rs_table_with_a_stored_DV_reads_values_1_to_8()
    {
        var table = DeltaTable.OpenLocal(Fixtures.DeltaRsDvSmall);
        var snapshot = await table.GetSnapshotAsync();
        var file = Assert.Single(snapshot.Files);
        Assert.Equal("u", file.DeletionVector!.StorageType);
        var values = (await Fixtures.ReadAllAsync(snapshot)).Select(r => (int)r[0]!).Order().ToArray();
        Assert.Equal(Enumerable.Range(1, 8).ToArray(), values); // DELETE … WHERE value IN (0, 9)
        Assert.Equal(Enumerable.Range(0, 10).ToArray(), (await Fixtures.ReadAllAsync(table, 0)).Select(r => (int)r[0]!).Order().ToArray());
    }

    [Fact]
    public void Z85_decodes_the_protocols_UUID_example_with_its_random_prefix()
    {
        // PROTOCOL.md JSON Example 1: "ab^-aqEH.-t@S}K{vb[*k^" relative to s3://mytable/.
        var dv = new DeletionVectorDescriptor("u", "ab^-aqEH.-t@S}K{vb[*k^", 4, 40, 6);
        Assert.Equal("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", DeletionVectors.AbsolutePath("s3://mytable", dv));
        Assert.Equal("uab^-aqEH.-t@S}K{vb[*k^@4", dv.UniqueId);
    }

    [Fact]
    public void Z85_resolves_the_DAT_deletion_vector_file()
    {
        var dv = new DeletionVectorDescriptor("u", "ITfmIXOK!>U^L:if1E+a", 1, 40, 4);
        Assert.Equal("/t/deletion_vector_8aebe228-b96c-4831-b0b6-22882eb9a3d3.bin", DeletionVectors.AbsolutePath("/t", dv));
    }

    [Fact]
    public void Z85_round_trips_and_pads_to_four_bytes()
    {
        var data = Enumerable.Range(0, 37).Select(i => (byte)(i * 7)).ToArray();
        var text = Z85.Encode(data);
        Assert.Equal(50, text.Length);
        Assert.Equal(data, Z85.Decode(text, data.Length));
        Assert.Equal("HelloWorld", Z85.Encode([0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B])); // RFC 32's test vector
    }

    [Theory]
    [InlineData("abc")]          // not a multiple of 5
    [InlineData("abcd~")]        // ~ is not in the alphabet
    [InlineData("#####")]        // 84·(85⁴+…+1) exceeds 32 bits
    public void Z85_refuses_malformed_text(string text) => Assert.Throws<InvalidDataException>(() => Z85.Decode(text));

    [Fact]
    public void The_protocols_inline_example_is_the_native_layout_and_decodes()
    {
        // PROTOCOL.md JSON Example 3: rows 3, 4, 7, 11, 18, 29. Its bytes start 64 39 D3 D0 (magic 1681511376, big-endian).
        var bytes = Z85.Decode("wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L", 40);
        Assert.Equal(new byte[] { 0x64, 0x39, 0xD3, 0xD0 }, bytes[..4]);
        var bitmap = DeletionVectors.DecodeBitmapData(bytes, 6);
        Assert.Equal(new long[] { 3, 4, 7, 11, 18, 29 }, bitmap.Positions().Order().ToArray());
    }

    [Fact]
    public async Task An_inline_portable_DV_reads_like_the_stored_one()
    {
        // Same table, the DAT DV moved inline: Z85 of the file record's bitmapData (portable magic + Roaring64).
        var table = Fixtures.CopyToTemp(Fixtures.Dat("deletion_vectors"));
        var expected = Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        var file = Directory.GetFiles(table, "deletion_vector_*.bin").Single();
        var record = File.ReadAllBytes(file);
        var size = BinaryPrimitives.ReadInt32BigEndian(record.AsSpan(1));
        var inline = Z85.Encode(record.AsSpan(5, size));
        Fixtures.EditCommit(table, 2, line =>
        {
            if (line["add"]?["deletionVector"] is JsonObject dv)
            {
                dv["storageType"] = "i";
                dv["pathOrInlineDv"] = inline;
                dv.Remove("offset");
            }
            return line;
        });
        File.Delete(file);
        var snapshot = await DeltaTable.OpenLocal(table).GetSnapshotAsync();
        Assert.Equal("i", snapshot.Files.Single().DeletionVector!.StorageType);
        Assert.Equal(expected, Canon.Rows(await Fixtures.ReadAllAsync(snapshot)));
        Assert.Single(expected); // 5 rows, 4 deleted
    }

    [Fact]
    public async Task An_absolute_path_DV_reads_like_the_relative_one()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("deletion_vectors"));
        var expected = Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        var file = Directory.GetFiles(table, "deletion_vector_*.bin").Single();
        var moved = Path.Combine(Fixtures.EmptyTable(), "elsewhere.bin");
        File.Move(file, moved);
        Fixtures.EditCommit(table, 2, line =>
        {
            if (line["add"]?["deletionVector"] is JsonObject dv)
            {
                dv["storageType"] = "p";
                dv["pathOrInlineDv"] = new Uri(moved).AbsoluteUri;
            }
            return line;
        });
        Assert.Equal(expected, Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table))));
    }

    [Fact]
    public async Task A_DV_whose_cardinality_disagrees_with_its_descriptor_is_corrupt()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("deletion_vectors"));
        Fixtures.EditCommit(table, 2, line =>
        {
            if (line["add"]?["deletionVector"] is JsonObject dv) dv["cardinality"] = 3;
            return line;
        });
        await Assert.ThrowsAsync<InvalidDataException>(() => Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
    }

    [Fact]
    public async Task A_DV_file_with_a_flipped_bit_fails_its_CRC()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("deletion_vectors"));
        var file = Directory.GetFiles(table, "deletion_vector_*.bin").Single();
        var bytes = File.ReadAllBytes(file);
        bytes[^6] ^= 0x01; // inside the bitmap, before the CRC
        File.WriteAllBytes(file, bytes);
        var e = await Assert.ThrowsAsync<InvalidDataException>(() => Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        Assert.Contains("deletion vector", e.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void The_stored_DAT_DV_is_an_Iceberg_deletion_vector_blob_too()
    {
        // The second Spark-written DV (after delta-rs's): offset 1 is a big-endian dataSize, the record decodes with Iceberg's codec.
        var record = File.ReadAllBytes(Directory.GetFiles(Fixtures.Dat("deletion_vectors"), "deletion_vector_*.bin").Single());
        Assert.Equal(1, record[0]);
        Assert.Equal(4, DeletionVector.Decode(record.AsSpan(1)).Cardinality);
    }
}
