using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Iceberg.Net.Avro;
using Iceberg.Net.Json;
using Iceberg.Net.Puffin;
using Xunit.Abstractions;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Decoders fed corrupted input (bit flips, overwritten bytes, truncation, insertions, hostile length fields, structural
/// JSON damage) must fail loudly with a format error — never crash with a programming-error exception, hang, try to
/// allocate gigabytes, or (where the format can tell) silently return a wrong value.
/// ICEBERG_FUZZ_RUNS (default 500) mutations per sample; ICEBERG_FUZZ_SEED reproduces a run.
/// </summary>
public sealed class CorruptionTests(ITestOutputHelper output)
{
    private static readonly string Fixtures = Path.Combine(AppContext.BaseDirectory, "fixtures");
    private static int Runs => int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_FUZZ_RUNS"), out var n) ? n : 500;
    private static int Seed => int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_FUZZ_SEED"), out var s) ? s : Environment.TickCount;

    /// <summary>The documented ways a decoder reports bad input. Anything else is a decoder bug.</summary>
    private static bool IsFormatError(Exception e) => e is FormatException or InvalidDataException or EndOfStreamException or NotSupportedException or JsonException;

    private static byte[] Mutate(byte[] input, Random rnd)
    {
        var b = (byte[])input.Clone();
        switch (rnd.Next(6))
        {
            case 0: // flip 1-4 bits
                for (var i = rnd.Next(1, 5); i > 0; i--) b[rnd.Next(b.Length)] ^= (byte)(1 << rnd.Next(8));
                return b;
            case 1: // overwrite a run of bytes
            {
                var at = rnd.Next(b.Length);
                for (var i = at; i < Math.Min(b.Length, at + rnd.Next(1, 16)); i++) b[i] = (byte)rnd.Next(256);
                return b;
            }
            case 2: // truncate
                return b[..rnd.Next(b.Length)];
            case 3: // insert random bytes
            {
                var at = rnd.Next(b.Length);
                var extra = new byte[rnd.Next(1, 16)];
                rnd.NextBytes(extra);
                return [.. b[..at], .. extra, .. b[at..]];
            }
            case 4: // hostile 32-bit field (length prefixes, counts, offsets) in either byte order
            {
                if (b.Length < 4) return b;
                var at = rnd.Next(b.Length - 3);
                var v = new[] { int.MaxValue, -1, int.MinValue, 0, 0x10000000 }[rnd.Next(5)];
                if (rnd.Next(2) == 0) System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(b.AsSpan(at), v);
                else System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(b.AsSpan(at), v);
                return b;
            }
            default: // hostile Avro varint (zig-zag 0x7f.. huge positive / negative)
            {
                var at = rnd.Next(b.Length);
                byte[] varint = rnd.Next(2) == 0 ? [0xFE, 0xFF, 0xFF, 0xFF, 0x0F] : [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
                return [.. b[..at], .. varint, .. b[Math.Min(b.Length, at + varint.Length)..]];
            }
        }
    }

    private void Fuzz(string name, byte[] sample, Action<byte[]> decode)
    {
        decode(sample); // the sample itself must decode
        var seed = Seed;
        var rnd = new Random(seed);
        var outcomes = new Dictionary<string, int>();
        for (var run = 0; run < Runs; run++)
        {
            var input = Mutate(sample, rnd);
            var task = Task.Run(() => decode(input));
            string outcome;
            try
            {
                if (!task.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
                outcome = "accepted";
            }
            catch (AggregateException ae) when (ae.InnerException is { } e && IsFormatError(e))
            {
                outcome = e.GetType().Name;
            }
            catch (Exception ex)
            {
                var e = ex is AggregateException { InnerException: { } inner } ? inner : ex;
                throw new Xunit.Sdk.XunitException(
                    $"{name}: run {run} (ICEBERG_FUZZ_SEED={seed}) threw {e.GetType().Name} instead of a format error: {e.Message}\n" +
                    $"input ({input.Length} bytes): {Convert.ToHexString(input.AsSpan(0, Math.Min(256, input.Length)))}\n{e.StackTrace}");
            }
            outcomes[outcome] = outcomes.GetValueOrDefault(outcome) + 1;
        }
        output.WriteLine($"{name} (seed {seed}): {string.Join(", ", outcomes.OrderByDescending(kv => kv.Value).Select(kv => $"{kv.Key}={kv.Value}"))}");
    }

    private static byte[] Fixture(params string[] path) => File.ReadAllBytes(Path.Combine([Fixtures, .. path]));

    private static string SparkMeta => Path.Combine(Fixtures, "spark", "ref", "mor_v3", "metadata");

    [Fact]
    public void Table_metadata_json() =>
        Fuzz("metadata", Fixture("metadata", "v2-valid.metadata.json"), b => MetadataJson.Read(Encoding.UTF8.GetString(b)));

    [Fact]
    public void Manifest_list_avro()
    {
        var list = Directory.GetFiles(SparkMeta, "snap-*.avro").OrderBy(f => new FileInfo(f).Length).Last();
        Fuzz("manifest-list", File.ReadAllBytes(list), b => ManifestCodec.ReadManifestList(b));
    }

    [Fact]
    public void Manifest_avro()
    {
        var metadata = MetadataJson.Read(File.ReadAllText(Path.Combine(SparkMeta, "v5.metadata.json")));
        foreach (var path in Directory.GetFiles(SparkMeta, "*-m*.avro").Order().Take(2))
            Fuzz($"manifest {Path.GetFileName(path)}", File.ReadAllBytes(path), b => ManifestCodec.ReadManifest(b, metadata.DefaultSpec, metadata.CurrentSchema));
    }

    [Fact]
    public void Puffin_file() =>
        Fuzz("puffin", Fixture("puffin", "dv.puffin"), b =>
        {
            var p = PuffinFile.Read(b);
            foreach (var blob in p.Blobs) p.Payload(blob);
        });

    [Fact]
    public void Deletion_vector()
    {
        var puffin = PuffinFile.Read(Fixture("puffin", "dv.puffin"));
        Fuzz("deletion-vector", puffin.Payload(puffin.Blobs[0]), b => DeletionVector.Decode(b));
    }

    [Fact]
    public void Roaring_bitmap()
    {
        var bm = new RoaringPositionBitmap();
        foreach (var p in new long[] { 0, 1, 2, 70_000, 1L << 33, (1L << 33) + 5 }) bm.Add(p);
        for (long p = 100; p < 5_000; p += 3) bm.Add(p);
        Fuzz("roaring", bm.Serialize(), b => RoaringPositionBitmap.Deserialize(b));
    }

    [Theory]
    [InlineData("theta-exact3.bin")]
    [InlineData("theta-est10k.bin")]
    public void Theta_sketch(string file) => Fuzz(file, Fixture("puffin", file), b => ThetaSketch.Deserialize(b));

    /// <summary>The CRC catches every single-bit error in a deletion vector: none may decode to a different bitmap.</summary>
    [Fact]
    public void Every_single_bit_flip_in_a_deletion_vector_is_detected()
    {
        var puffin = PuffinFile.Read(Fixture("puffin", "dv.puffin"));
        var blob = puffin.Payload(puffin.Blobs[0]);
        for (var bit = 0; bit < blob.Length * 8; bit++)
        {
            var b = (byte[])blob.Clone();
            b[bit / 8] ^= (byte)(1 << (bit % 8));
            Assert.ThrowsAny<FormatException>(() => DeletionVector.Decode(b));
        }
    }

    /// <summary>Removing any field the spec marks required must be rejected, not defaulted.</summary>
    [Theory]
    [InlineData("v2-valid.metadata.json", "format-version", "table-uuid", "location", "last-sequence-number", "last-updated-ms", "last-column-id",
        "schemas", "current-schema-id", "partition-specs", "default-spec-id", "last-partition-id", "sort-orders", "default-sort-order-id")]
    [InlineData("v3-valid.metadata.json", "format-version", "table-uuid", "location", "last-sequence-number", "last-updated-ms", "last-column-id",
        "schemas", "current-schema-id", "partition-specs", "default-spec-id", "last-partition-id", "sort-orders", "default-sort-order-id", "next-row-id")]
    public void Missing_required_metadata_fields_are_rejected(string file, params string[] required)
    {
        var json = File.ReadAllText(Path.Combine(Fixtures, "metadata", file));
        MetadataJson.Read(json);
        foreach (var field in required)
        {
            var node = JsonNode.Parse(json)!.AsObject();
            Assert.True(node.Remove(field), $"{file} has no {field}");
            var e = Record.Exception(() => MetadataJson.Read(node.ToJsonString()));
            Assert.True(e is not null, $"metadata without '{field}' was accepted");
            Assert.True(IsFormatError(e), $"missing '{field}': {e.GetType().Name}: {e.Message}");
        }
    }

    /// <summary>Wrong JSON types anywhere in the metadata tree must produce a format error.</summary>
    [Fact]
    public void Structurally_damaged_metadata_fails_with_a_format_error()
    {
        var json = File.ReadAllText(Path.Combine(Fixtures, "metadata", "v2-valid.metadata.json"));
        var seed = Seed;
        var rnd = new Random(seed);
        JsonNode?[] replacements = [null, "x", -1, long.MaxValue, 1.5, true, new JsonArray(), new JsonObject()];
        for (var run = 0; run < Runs; run++)
        {
            var root = JsonNode.Parse(json)!;
            var all = new List<JsonNode>();
            void Walk(JsonNode n) { all.Add(n); foreach (var c in n is JsonObject o ? o.Select(kv => kv.Value) : n is JsonArray a ? a : []) if (c is not null) Walk(c); }
            Walk(root);
            var victim = all[rnd.Next(1, all.Count)];
            var replacement = replacements[rnd.Next(replacements.Length)]?.DeepClone();
            var path = victim.GetPath();
            switch (victim.Parent)
            {
                case JsonObject o: o[victim.GetPropertyName()] = replacement; break;
                case JsonArray a: a[victim.GetElementIndex()] = replacement; break;
            }
            var e = Record.Exception(() => MetadataJson.Read(root.ToJsonString()));
            if (e is not null && !IsFormatError(e))
                Assert.Fail($"run {run} (ICEBERG_FUZZ_SEED={seed}): {path} := {replacement?.ToJsonString() ?? "null"} threw {e.GetType().Name}: {e.Message}");
        }
    }
}
