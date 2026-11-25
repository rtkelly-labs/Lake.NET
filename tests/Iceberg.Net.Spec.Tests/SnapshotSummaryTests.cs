using System.Globalization;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Every snapshot's summary agrees with the files it actually tracks: the totals with the live files, and the
/// added/removed counts with the file diff from its parent. The stryker-commit mutation run (see docs/roadmap.md,
/// "Mutation testing") showed nothing checked these fields once delete files were involved.
/// </summary>
[Spec("Optional Snapshot Summary Fields")]
public sealed class SnapshotSummaryTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-summary", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    private static readonly Schema IdValue = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "v", PrimitiveType.String)]);

    public SnapshotSummaryTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("s")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Summaries_match_the_files_through_inserts_deletes_and_compaction(int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("s", $"v{formatVersion}"), IdValue, FormatVersion: formatVersion));
        async Task Step(Func<Task> op)
        {
            await op();
            await AssertSummaryMatchesFilesAsync(t);
        }

        await Step(() => TableMutations.InsertAsync(t, [[1L, "a"], [2L, "b"], [3L, "c"]]));
        await Step(() => TableMutations.InsertAsync(t, [[4L, "d"], [5L, "e"]]));
        await Step(() => TableMutations.InsertAsync(t, [[6L, "f"]]));
        await Step(() => TableMutations.DeleteByKeysAsync(t, ["id"], [[1L]], DeleteStrategy.PositionDeletes));
        await Step(() => TableMutations.DeleteByKeysAsync(t, ["id"], [[4L]], DeleteStrategy.EqualityDeletes));
        // A second position delete on the same data file: on v3 the deletion vector is replaced (removed-dvs).
        await Step(() => TableMutations.DeleteByKeysAsync(t, ["id"], [[2L]], DeleteStrategy.PositionDeletes));
        await Step(() => TableMutations.DeleteByKeysAsync(t, ["id"], [[5L]], DeleteStrategy.CopyOnWrite));
        await Step(() => Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 1, RewriteAll: true)));

        Assert.Equal(new[] { 3L, 6L }, (await TableScan.ReadAllAsync(t)).Select(r => (long)r[0]!).Order());
        // The steps above produced at least one snapshot that removed delete files and one that added each kind.
        var summaries = t.Metadata.Snapshots.Select(s => s.Summary).ToList();
        Assert.Contains(summaries, s => s.ContainsKey("added-position-deletes"));
        Assert.Contains(summaries, s => s.ContainsKey("added-equality-deletes"));
        Assert.Contains(summaries, s => s.ContainsKey("deleted-data-files"));
        if (formatVersion >= 3)
        {
            Assert.Contains(summaries, s => s.ContainsKey("added-dvs"));
            Assert.Contains(summaries, s => s.ContainsKey("removed-dvs"));
        }
    }

    private static async Task AssertSummaryMatchesFilesAsync(Table t)
    {
        var snapshot = t.Metadata.CurrentSnapshot ?? throw new InvalidOperationException("no snapshot");
        var summary = snapshot.Summary;
        var live = await MetadataTables.FilesAsync(t, SnapshotSelector.Id(snapshot.SnapshotId));
        var before = snapshot.ParentSnapshotId is { } parent ? await MetadataTables.FilesAsync(t, SnapshotSelector.Id(parent)) : Array.Empty<FilesRow>();
        var beforePaths = before.Select(f => f.FilePath).ToHashSet(StringComparer.Ordinal);
        var livePaths = live.Select(f => f.FilePath).ToHashSet(StringComparer.Ordinal);
        var added = live.Where(f => !beforePaths.Contains(f.FilePath)).ToList();
        var removed = before.Where(f => !livePaths.Contains(f.FilePath)).ToList();
        var context = $"snapshot {snapshot.SnapshotId} ({summary.GetValueOrDefault("operation")})";

        static bool IsData(FilesRow f) => f.Content == (int)FileContent.Data;
        static bool IsPosition(FilesRow f) => f.Content == (int)FileContent.PositionDeletes;
        static bool IsEquality(FilesRow f) => f.Content == (int)FileContent.EqualityDeletes;
        static bool IsDv(FilesRow f) => f.FileFormat == "PUFFIN";

        // Totals: always present, and equal to what the snapshot tracks.
        Total("total-data-files", live.Count(IsData));
        Total("total-records", live.Where(IsData).Sum(f => f.RecordCount));
        Total("total-files-size", live.Where(IsData).Sum(f => f.FileSizeInBytes));
        Total("total-delete-files", live.Count(f => !IsData(f)));
        Total("total-position-deletes", live.Where(IsPosition).Sum(f => f.RecordCount));
        Total("total-equality-deletes", live.Where(IsEquality).Sum(f => f.RecordCount));

        // Changes: equal to the diff. The spec makes these fields optional; this writer omits a zero, except the added data counts.
        Change("added-data-files", added.Count(IsData));
        Change("added-records", added.Where(IsData).Sum(f => f.RecordCount));
        Change("added-files-size", added.Where(IsData).Sum(f => f.FileSizeInBytes));
        Change("deleted-data-files", removed.Count(IsData));
        Change("deleted-records", removed.Where(IsData).Sum(f => f.RecordCount));
        Change("removed-files-size", removed.Where(IsData).Sum(f => f.FileSizeInBytes));
        Change("added-delete-files", added.Count(f => !IsData(f)));
        Change("added-position-deletes", added.Where(IsPosition).Sum(f => f.RecordCount));
        Change("added-equality-deletes", added.Where(IsEquality).Sum(f => f.RecordCount));
        Change("added-dvs", added.Count(IsDv));
        Change("removed-delete-files", removed.Count(f => !IsData(f)));
        Change("removed-position-deletes", removed.Where(IsPosition).Sum(f => f.RecordCount));
        Change("removed-equality-deletes", removed.Where(IsEquality).Sum(f => f.RecordCount));
        Change("removed-dvs", removed.Count(IsDv));

        void Total(string key, long expected)
        {
            Assert.True(summary.TryGetValue(key, out var v), $"{context}: missing {key}");
            Assert.True(long.Parse(v!, CultureInfo.InvariantCulture) == expected, $"{context}: {key} = {v}, files say {expected}");
        }

        void Change(string key, long expected)
        {
            // The added data counts are always written; every other change field only when non-zero.
            if (expected == 0 && key is not ("added-data-files" or "added-records" or "added-files-size"))
                Assert.False(summary.ContainsKey(key), $"{context}: {key} = {summary.GetValueOrDefault(key)}, files say 0");
            else
                Total(key, expected);
        }
    }
}
