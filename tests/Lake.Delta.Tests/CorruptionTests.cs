using System.Diagnostics;
using System.Text;

namespace Lake.Delta.Tests;

/// <summary>
/// The <c>Corruption</c> contract (AGENTS.md): a malformed log line, JSON document, checkpoint or deletion vector fails
/// with <see cref="InvalidDataException"/> and never crashes or hangs.
/// </summary>
public sealed class CorruptionTests
{
    private static async Task<Exception?> TryReadAsync(string table)
    {
        try
        {
            await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table));
            return null;
        }
        catch (Exception e)
        {
            return e;
        }
    }

    private static async Task AssertCorruptAsync(string table, string? messagePart = null)
    {
        var e = await TryReadAsync(table);
        var format = Assert.IsType<InvalidDataException>(e);
        if (messagePart is not null) Assert.Contains(messagePart, format.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("{\"add\":{\"path\":\"x.parquet\"", "not valid JSON")]                         // truncated line
    [InlineData("[1,2,3]", "not a JSON object")]                                              // not an object
    [InlineData("{\"add\":{\"partitionValues\":{},\"size\":1}}", "add.path is required")]   // missing required field
    [InlineData("{\"add\":{\"path\":7,\"partitionValues\":{},\"size\":1}}", "not a string")] // wrong kind
    [InlineData("{\"add\":{\"path\":\"x\",\"partitionValues\":{},\"size\":-1}}", "negative")]
    [InlineData("{\"add\":{\"path\":\"x\",\"partitionValues\":{\"a\":1},\"size\":1}}", "not a string")]
    [InlineData("{\"add\":{\"path\":\"x\",\"partitionValues\":{},\"size\":1,\"deletionVector\":{\"storageType\":\"z\",\"pathOrInlineDv\":\"\",\"sizeInBytes\":1,\"cardinality\":1}}}", "storageType")]
    [InlineData("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}", "more than one protocol")]
    [InlineData("{\"txn\":{\"appId\":\"a\"}}", "txn.version is required")]
    [InlineData("{\"add\":{\"path\":\"x\",\"partitionValues\":{},\"size\":1},\"remove\":{\"path\":\"y\"}}", "more than one action")]
    public async Task A_malformed_commit_line_is_a_format_error(string line, string messagePart)
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        File.AppendAllText(Fixtures.CommitPath(table, 0), line + "\n");
        await AssertCorruptAsync(table, messagePart);
    }

    [Fact]
    public async Task Invalid_UTF8_in_a_commit_is_a_format_error()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        File.AppendAllBytes(Fixtures.CommitPath(table, 1), [0xC3, 0x28, (byte)'\n']);
        await AssertCorruptAsync(table);
    }

    [Fact]
    public async Task Deeply_nested_JSON_is_refused_without_exhausting_the_stack()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        File.AppendAllText(Fixtures.CommitPath(table, 1), "{\"commitInfo\":" + new string('[', 100_000) + new string(']', 100_000) + "}\n");
        await AssertCorruptAsync(table);
    }

    [Fact]
    public async Task A_malformed_schema_string_is_a_format_error()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        Fixtures.EditCommit(table, 0, line =>
        {
            if (line["metaData"] is System.Text.Json.Nodes.JsonObject md) md["schemaString"] = "{\"type\":\"struct\",\"fields\":[";
            return line;
        });
        await AssertCorruptAsync(table, "Delta schema");
    }

    [Fact]
    public async Task Malformed_stats_are_a_format_error_when_used()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        Fixtures.EditCommit(table, 0, line =>
        {
            if (line["add"] is System.Text.Json.Nodes.JsonObject add) add["stats"] = "{\"numRecords\":";
            return line;
        });
        var snapshot = await DeltaTable.OpenLocal(table).GetSnapshotAsync();
        Assert.Throws<InvalidDataException>(() => snapshot.Files.Single(f => f.StatsJson!.Length < 20).Stats);
    }

    [Theory]
    [InlineData("garbage")]   // not Parquet at all
    [InlineData("truncate")]  // a real checkpoint cut in half
    [InlineData("footer")]    // an impossible footer length
    public async Task A_corrupt_checkpoint_is_a_format_error(string damage)
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("with_checkpoint"));
        var checkpoint = Path.Combine(table, "_delta_log", "00000000000000000002.checkpoint.parquet");
        var bytes = File.ReadAllBytes(checkpoint);
        File.WriteAllBytes(checkpoint, damage switch
        {
            "garbage" => Encoding.ASCII.GetBytes("PAR1 this is not a parquet file PAR1"),
            "truncate" => bytes[..(bytes.Length / 2)],
            _ => Flip(bytes, bytes.Length - 5), // the footer length's most significant byte
        });
        await AssertCorruptAsync(table, "checkpoint");
    }

    [Fact]
    public async Task A_corrupt_data_file_is_a_format_error()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        var data = Directory.GetFiles(table, "*.parquet").First();
        File.WriteAllBytes(data, File.ReadAllBytes(data)[..100]);
        await AssertCorruptAsync(table, "data file");
    }

    [Fact]
    public async Task A_truncated_DV_file_is_a_format_error()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("deletion_vectors"));
        var dv = Directory.GetFiles(table, "deletion_vector_*.bin").Single();
        File.WriteAllBytes(dv, File.ReadAllBytes(dv)[..20]);
        await AssertCorruptAsync(table, "does not fit");
    }

    private static byte[] Flip(byte[] bytes, int at)
    {
        var copy = (byte[])bytes.Clone();
        copy[at] ^= 0xFF;
        return copy;
    }

    /// <summary>
    /// Seeded byte mutations of a commit, a checkpoint and a DV: every outcome is a successful read or one of the
    /// declared exceptions, and each read finishes quickly.
    /// </summary>
    [Fact]
    public async Task Random_damage_never_escapes_the_declared_exceptions()
    {
        var targets = new (string Case, Func<string, string> File)[]
        {
            ("basic_partitioned", t => Fixtures.CommitPath(t, 1)),
            ("with_checkpoint", t => Path.Combine(t, "_delta_log", "00000000000000000002.checkpoint.parquet")),
            ("deletion_vectors", t => Directory.GetFiles(t, "deletion_vector_*.bin").Single()),
            ("deletion_vectors", t => Fixtures.CommitPath(t, 2)),
        };
        var random = new Random(20261031);
        foreach (var (name, file) in targets)
        {
            var pristine = Fixtures.CopyToTemp(Fixtures.Dat(name));
            var original = File.ReadAllBytes(file(pristine));
            for (var i = 0; i < 40; i++)
            {
                var bytes = (byte[])original.Clone();
                var flips = random.Next(1, 4);
                for (var f = 0; f < flips; f++) bytes[random.Next(bytes.Length)] = (byte)random.Next(256);
                if (random.Next(5) == 0) bytes = bytes[..random.Next(bytes.Length)];
                File.WriteAllBytes(file(pristine), bytes);

                var clock = Stopwatch.StartNew();
                var e = await TryReadAsync(pristine);
                Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"{name} mutation {i} took {clock.Elapsed}");
                Assert.True(e is null or InvalidDataException or DeltaUnsupportedFeatureException or DeltaVersionNotFoundException or FileNotFoundException,
                    $"{name} mutation {i}: {e}");
            }
        }
    }
}
