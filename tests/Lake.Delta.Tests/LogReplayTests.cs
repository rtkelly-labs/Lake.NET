using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Iceberg.Net.IO;

namespace Lake.Delta.Tests;

/// <summary>Action reconciliation, checkpoint kinds, <c>_last_checkpoint</c>, and time travel by version and timestamp.</summary>
public sealed class LogReplayTests
{
    /// <summary>DAT with_checkpoint's checkpoint split into other shapes by tools/delta_checkpoint_fixtures.py.</summary>
    private static readonly string Shapes = Path.Combine(Fixtures.RepositoryRoot, "tests", "Lake.Delta.Tests", "fixtures", "checkpoint-shapes");

    private const string Schema = "{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"x\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}";
    private const string Protocol = """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""";
    private const string Metadata = """{"metaData":{"id":"t","format":{"provider":"parquet","options":{}},"schemaString":"SCHEMA","partitionColumns":[],"configuration":{},"createdTime":0}}""";

    private static string Add(string path, string? dv = null) =>
        $$$"""{"add":{"path":"{{{path}}}","partitionValues":{},"size":1,"modificationTime":0,"dataChange":true{{{(dv is null ? "" : ",\"deletionVector\":" + dv)}}}}}""";

    private static string Remove(string path, string? dv = null) =>
        $$$"""{"remove":{"path":"{{{path}}}","deletionTimestamp":1,"dataChange":true{{{(dv is null ? "" : ",\"deletionVector\":" + dv)}}}}}""";

    [Fact]
    public async Task Actions_reconcile_as_the_protocol_says()
    {
        const string dv = """{"storageType":"i","pathOrInlineDv":"wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L","sizeInBytes":40,"cardinality":6}""";
        var table = Fixtures.EmptyTable();
        Fixtures.WriteCommit(table, 0, Protocol, Metadata.Replace("SCHEMA", Schema),
            """{"txn":{"appId":"a","version":1}}""",
            """{"domainMetadata":{"domain":"d1","configuration":"{}","removed":false}}""",
            """{"domainMetadata":{"domain":"d2","configuration":"{\"k\":1}","removed":false}}""",
            Add("f1.parquet"));
        Fixtures.WriteCommit(table, 1, """{"commitInfo":{"timestamp":1,"operation":"X"}}""",
            """{"txn":{"appId":"a","version":5}}""",
            """{"domainMetadata":{"domain":"d1","configuration":"{}","removed":true}}""",
            Add("f2.parquet"), Remove("f1.parquet"));
        Fixtures.WriteCommit(table, 2, """{"txn":{"appId":"a","version":3}}""", """{"txn":{"appId":"b","version":1}}""",
            Remove("f2.parquet"), Add("f2.parquet", dv), """{"someFutureAction":{"x":1}}""");

        var delta = DeltaTable.OpenLocal(table);
        var latest = await delta.GetSnapshotAsync();
        Assert.Equal(2, latest.Version);
        Assert.Equal(3, latest.Transactions["a"].Version); // the latest seen wins, not the highest
        Assert.Equal(1, latest.Transactions["b"].Version);
        Assert.Equal(["d2"], latest.DomainMetadata.Keys);
        var live = Assert.Single(latest.Files);
        Assert.Equal("f2.parquet", live.Path);
        Assert.Equal("i", live.DeletionVector!.StorageType); // (f2, dv) is a different logical file from (f2, none)
        Assert.Equal(["f1.parquet", "f2.parquet"], latest.Tombstones.Select(t => t.Path).Order());
        Assert.Null(latest.CommitInfo);

        var v1 = await delta.GetSnapshotAsync(1);
        Assert.Equal("X", v1.CommitInfo!.Raw["operation"]!.GetValue<string>());
        Assert.Equal(5, v1.Transactions["a"].Version);
        Assert.Equal(["d2"], v1.DomainMetadata.Keys);
        Assert.Equal(["f2.parquet"], v1.Files.Select(f => f.Path));

        var v0 = await delta.GetSnapshotAsync(0);
        Assert.Equal(["d1", "d2"], v0.DomainMetadata.Keys.Order());
        await Assert.ThrowsAsync<DeltaVersionNotFoundException>(() => delta.GetSnapshotAsync(3));
    }

    [Fact]
    public async Task A_classic_checkpoint_replays_with_the_commits_after_it()
    {
        // with_checkpoint: 0.json, 1.json, 2.checkpoint.parquet, 2.json, 3.json. Without the early JSON only the checkpoint can serve.
        var table = Fixtures.CopyToTemp(Fixtures.Dat("with_checkpoint"));
        var expected = Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        File.Delete(Fixtures.CommitPath(table, 0));
        File.Delete(Fixtures.CommitPath(table, 1));
        Assert.Equal(expected, Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table))));
        await Assert.ThrowsAsync<DeltaVersionNotFoundException>(() => DeltaTable.OpenLocal(table).GetSnapshotAsync(1));
    }

    [Fact]
    public async Task A_multi_part_checkpoint_is_read_whole_and_ignored_when_a_part_is_missing()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("with_checkpoint"));
        var expected = Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        var log = Path.Combine(table, "_delta_log");
        var classic = Path.Combine(log, "00000000000000000002.checkpoint.parquet");
        var part1 = Path.Combine(log, "00000000000000000002.checkpoint.0000000001.0000000002.parquet");
        var part2 = Path.Combine(log, "00000000000000000002.checkpoint.0000000002.0000000002.parquet");
        File.Copy(Path.Combine(Shapes, "multi-part", "part1.parquet"), part1); // remove + add
        File.Copy(Path.Combine(Shapes, "multi-part", "part2.parquet"), part2); // protocol + metaData
        File.Delete(classic);
        File.Delete(Fixtures.CommitPath(table, 0));
        File.Delete(Fixtures.CommitPath(table, 1));

        Assert.Equal(expected, Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table))));

        File.Delete(part2); // an incomplete multi-part checkpoint is ignored, never half-read
        await Assert.ThrowsAsync<DeltaVersionNotFoundException>(() => DeltaTable.OpenLocal(table).GetSnapshotAsync());
    }

    [Fact]
    public async Task A_V2_checkpoint_with_a_sidecar_is_read()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("with_checkpoint"));
        var expected = Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        var log = Path.Combine(table, "_delta_log");
        var classic = Path.Combine(log, "00000000000000000002.checkpoint.parquet");
        Directory.CreateDirectory(Path.Combine(log, "_sidecars"));
        var sidecarName = "3a0d65cd-4056-49b8-937b-95f9e3ee90e5.parquet";
        File.Copy(Path.Combine(Shapes, "sidecar.parquet"), Path.Combine(log, "_sidecars", sidecarName)); // add + remove columns only
        var metaData = File.ReadLines(Fixtures.CommitPath(table, 0)).Single(l => l.StartsWith("{\"metaData\"", StringComparison.Ordinal));
        File.WriteAllLines(Path.Combine(log, "00000000000000000002.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json"),
        [
            """{"checkpointMetadata":{"version":2,"tags":{}}}""",
            """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"],"writerFeatures":["v2Checkpoint"]}}""",
            metaData,
            """{"sidecar":{"path":"SIDECAR","sizeInBytes":1,"modificationTime":0,"tags":{}}}""".Replace("SIDECAR", sidecarName, StringComparison.Ordinal),
        ]);
        File.Delete(classic);
        File.Delete(Fixtures.CommitPath(table, 0));
        File.Delete(Fixtures.CommitPath(table, 1));

        var snapshot = await DeltaTable.OpenLocal(table).GetSnapshotAsync();
        Assert.Contains("v2Checkpoint", snapshot.Protocol.ReaderFeatures!);
        Assert.Equal(expected, Canon.Rows(await Fixtures.ReadAllAsync(snapshot)));

        File.Delete(Path.Combine(log, "_sidecars", sidecarName));
        await Assert.ThrowsAsync<FileNotFoundException>(() => DeltaTable.OpenLocal(table).GetSnapshotAsync());
    }

    [Fact]
    public async Task A_malformed_last_checkpoint_is_only_a_hint_and_is_ignored()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("with_checkpoint"));
        File.WriteAllText(Path.Combine(table, "_delta_log", "_last_checkpoint"), "{\"version\":2,\"si");
        Assert.Equal(3, (await DeltaTable.OpenLocal(table).GetSnapshotAsync()).Version);
    }

    [Fact]
    public async Task Last_checkpoint_finds_a_checkpoint_the_listing_does_not_show_yet()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("with_checkpoint"));
        var expected = Canon.Rows(await Fixtures.ReadAllAsync(DeltaTable.OpenLocal(table)));
        File.Delete(Fixtures.CommitPath(table, 0));
        File.Delete(Fixtures.CommitPath(table, 1));
        var io = new LaggingListingFileIO(name => name.Contains(".checkpoint.", StringComparison.Ordinal));
        Assert.Equal(expected, Canon.Rows(await Fixtures.ReadAllAsync(new DeltaTable(table, io))));

        File.Delete(Path.Combine(table, "_delta_log", "_last_checkpoint"));
        await Assert.ThrowsAsync<DeltaVersionNotFoundException>(() => new DeltaTable(table, io).GetSnapshotAsync());
    }

    [Fact]
    public async Task Time_travel_by_timestamp_uses_commit_file_times_made_monotonic()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        var t0 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        File.SetLastWriteTimeUtc(Fixtures.CommitPath(table, 0), t0);
        File.SetLastWriteTimeUtc(Fixtures.CommitPath(table, 1), t0.AddDays(1));
        var delta = DeltaTable.OpenLocal(table);

        Assert.Equal(0, (await delta.GetSnapshotAsOfAsync(new DateTimeOffset(t0.AddHours(12)))).Version);
        Assert.Equal(1, (await delta.GetSnapshotAsOfAsync(new DateTimeOffset(t0.AddDays(1)))).Version);
        Assert.Equal(1, (await delta.GetSnapshotAsOfAsync(new DateTimeOffset(t0.AddDays(30)))).Version);
        await Assert.ThrowsAsync<DeltaVersionNotFoundException>(() => delta.GetSnapshotAsOfAsync(new DateTimeOffset(t0.AddSeconds(-1))));

        File.SetLastWriteTimeUtc(Fixtures.CommitPath(table, 1), t0.AddDays(-1)); // a clock that went backwards
        var times = await delta.GetCommitTimestampsAsync();
        Assert.Equal(new DateTimeOffset(t0).AddMilliseconds(1), times[1].Timestamp);
        Assert.All(times, t => Assert.False(t.IsInCommitTimestamp));
    }

    [Fact]
    public async Task Time_travel_by_timestamp_uses_in_commit_timestamps_when_enabled()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        var ict0 = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var ict1 = ict0.AddDays(1);
        Fixtures.EditCommit(table, 0, line =>
        {
            if (line["commitInfo"] is JsonObject ci) ci["inCommitTimestamp"] = ict0.ToUnixTimeMilliseconds();
            if (line["metaData"] is JsonObject md) md["configuration"]!["delta.enableInCommitTimestamps"] = "true";
            return line;
        });
        Fixtures.EditCommit(table, 1, line =>
        {
            if (line["commitInfo"] is JsonObject ci) ci["inCommitTimestamp"] = ict1.ToUnixTimeMilliseconds();
            return line;
        });
        // File times that contradict the ICTs: they must not be used.
        File.SetLastWriteTimeUtc(Fixtures.CommitPath(table, 0), new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        File.SetLastWriteTimeUtc(Fixtures.CommitPath(table, 1), new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var delta = DeltaTable.OpenLocal(table);

        var times = await delta.GetCommitTimestampsAsync();
        Assert.Equal([ict0, ict1], times.Select(t => t.Timestamp));
        Assert.All(times, t => Assert.True(t.IsInCommitTimestamp));
        Assert.Equal(0, (await delta.GetSnapshotAsOfAsync(ict0.AddHours(1))).Version);
        Assert.Equal(1, (await delta.GetSnapshotAsOfAsync(ict1)).Version);
        await Assert.ThrowsAsync<DeltaVersionNotFoundException>(() => delta.GetSnapshotAsOfAsync(ict0.AddMilliseconds(-1)));
    }

    [Fact]
    public async Task In_commit_timestamps_enabled_later_split_time_travel_at_the_enablement_version()
    {
        var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
        var fileTime0 = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ict1 = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var metaData = File.ReadLines(Fixtures.CommitPath(table, 0)).Select(l => JsonNode.Parse(l)!.AsObject()).Single(o => o["metaData"] is not null);
        var config = metaData["metaData"]!["configuration"]!.AsObject();
        config["delta.enableInCommitTimestamps"] = "true";
        config["delta.inCommitTimestampEnablementVersion"] = "1";
        config["delta.inCommitTimestampEnablementTimestamp"] = ict1.ToUnixTimeMilliseconds().ToString(System.Globalization.CultureInfo.InvariantCulture);
        Fixtures.EditCommit(table, 1, line =>
        {
            if (line["commitInfo"] is JsonObject ci) ci["inCommitTimestamp"] = ict1.ToUnixTimeMilliseconds();
            return line;
        });
        File.AppendAllText(Fixtures.CommitPath(table, 1), metaData.ToJsonString() + "\n");
        File.SetLastWriteTimeUtc(Fixtures.CommitPath(table, 0), fileTime0);
        File.SetLastWriteTimeUtc(Fixtures.CommitPath(table, 1), new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var delta = DeltaTable.OpenLocal(table);

        var times = await delta.GetCommitTimestampsAsync();
        Assert.False(times[0].IsInCommitTimestamp);
        Assert.Equal(new DateTimeOffset(fileTime0), times[0].Timestamp);
        Assert.True(times[1].IsInCommitTimestamp);
        Assert.Equal(0, (await delta.GetSnapshotAsOfAsync(ict1.AddDays(-1))).Version); // before enablement: versions < 1
        Assert.Equal(1, (await delta.GetSnapshotAsOfAsync(ict1)).Version);
    }

    /// <summary>A local FileIO whose listing does not (yet) show some <c>_delta_log</c> files, like an eventually consistent store.</summary>
    private sealed class LaggingListingFileIO(Func<string, bool> hidden) : IFileIO, ISupportsListing
    {
        private readonly LocalFileIO _local = new();

        public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, [EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var f in _local.ListAsync(prefix, ct))
                if (!hidden(Path.GetFileName(f.Location))) yield return f;
        }

        public Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default) => _local.ReadAllAsync(l, ct);
        public Task<Stream> OpenReadAsync(string l, CancellationToken ct = default) => _local.OpenReadAsync(l, ct);
        public Task<byte[]> ReadRangeAsync(string l, long o, int n, CancellationToken ct = default) => _local.ReadRangeAsync(l, o, n, ct);
        public Task<long> LengthAsync(string l, CancellationToken ct = default) => _local.LengthAsync(l, ct);
        public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => _local.ExistsAsync(l, ct);
        public Task WriteAsync(string l, ReadOnlyMemory<byte> c, bool overwrite = false, CancellationToken ct = default) => _local.WriteAsync(l, c, overwrite, ct);
        public Task DeleteAsync(string l, CancellationToken ct = default) => _local.DeleteAsync(l, ct);
        public void Dispose() => _local.Dispose();
    }
}
