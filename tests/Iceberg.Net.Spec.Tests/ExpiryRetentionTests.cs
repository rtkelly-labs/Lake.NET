using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Snapshot expiry honours the table's <c>history.expire.*</c> defaults and each ref's own retention, as Java's
/// RemoveSnapshots does: per-branch min-snapshots-to-keep / max-snapshot-age-ms, max-ref-age-ms for refs (never main).
/// </summary>
[Spec("Snapshot Retention Policy")]
[Spec("Snapshot References")]
public sealed class ExpiryRetentionTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-retention", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public ExpiryRetentionTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("x")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    /// <summary>A table with <paramref name="n"/> appends on main, at least 10 ms apart; returns it and the snapshots oldest first.</summary>
    private async Task<(Table Table, List<Snapshot> Snapshots)> Chain(int n, int formatVersion = 2, Dictionary<string, string>? properties = null)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("x", $"t{Guid.NewGuid():N}"), SnapshotManagementTests.Schema,
            FormatVersion: formatVersion, Properties: properties));
        for (var i = 0; i < n; i++)
        {
            await TableMutations.InsertAsync(t, SnapshotManagementTests.Rows(i * 10, 2));
            await Task.Delay(10);
        }
        return (t, t.Metadata.Ancestors(t.Metadata.CurrentSnapshotId).Reverse().ToList());
    }

    private static HashSet<long> Ids(Table t) => t.Metadata.Snapshots.Select(s => s.SnapshotId).ToHashSet();

    private async Task Valid(Table t)
    {
        var fresh = await _catalog.LoadTableAsync(t.Identifier);
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { AllSnapshots = true, Deep = true });
        foreach (var name in fresh.Metadata.Refs.Keys)
            await foreach (var _ in TableScan.ReadRowsAsync(fresh, new ScanRequest { Snapshot = SnapshotSelector.Ref(name) })) { } // every ref still reads
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Table_defaults_set_the_age_and_count_when_no_argument_is_given(int formatVersion)
    {
        var (t, s) = await Chain(5, formatVersion);
        var now = s[^1].TimestampMs + 1;
        // Max age reaches back to s[2]: s[0] and s[1] are older; min-snapshots-to-keep (1) is already met by the head.
        await t.SetPropertiesAsync(new Dictionary<string, string> { [Maintenance.MaxSnapshotAgeMs] = (now - s[2].TimestampMs).ToString() });
        var result = await Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = now });
        Assert.Equal(new[] { s[0].SnapshotId, s[1].SnapshotId }.Order(), result.Expired.Order());
        await Valid(t);

        // min-snapshots-to-keep outweighs age: with everything old, 2 ancestors stay.
        await t.SetPropertiesAsync(new Dictionary<string, string> { [Maintenance.MinSnapshotsToKeep] = "2" });
        await Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = now + 1_000_000 });
        Assert.Equal(new[] { s[3].SnapshotId, s[4].SnapshotId }.ToHashSet(), Ids(t));
        await Valid(t);
    }

    [Fact]
    public async Task Default_max_age_is_five_days()
    {
        var (t, s) = await Chain(3);
        Assert.Empty((await Maintenance.ExpireAsync(t, (ExpireOptions?)null)).Expired); // all younger than 5 days
        var result = await Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = s[^1].TimestampMs + Maintenance.MaxSnapshotAgeMsDefault + 1 });
        Assert.Equal(2, result.Expired.Count);
        await Valid(t);
    }

    [Fact]
    public async Task Explicit_arguments_override_table_properties()
    {
        var (t, s) = await Chain(4, properties: new() { [Maintenance.MinSnapshotsToKeep] = "4", [Maintenance.MaxSnapshotAgeMs] = "999999999" });
        var expired = await Maintenance.ExpireSnapshotsAsync(t, olderThanMs: long.MaxValue, retainLast: 1);
        Assert.Equal(3, expired.Count);
        Assert.Equal([s[^1].SnapshotId], Ids(t));
        await Valid(t);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task A_branch_keeps_its_own_minimum_and_age_whatever_the_table_default(int formatVersion)
    {
        var (t, s) = await Chain(6, formatVersion);
        await t.ManageSnapshots()
            .CreateBranch("keep3", s[3].SnapshotId, new RefRetention(MinSnapshotsToKeep: 3))                     // s3, s2, s1
            .CreateBranch("young", s[2].SnapshotId, new RefRetention(MaxSnapshotAgeMs: long.MaxValue / 2))        // s2, s1, s0
            .CommitAsync();
        var result = await Maintenance.ExpireAsync(t, olderThanMs: long.MaxValue, retainLast: 1); // main keeps only s5
        Assert.Equal(new[] { s[4].SnapshotId }, result.Expired);
        Assert.Equal(new[] { s[0], s[1], s[2], s[3], s[5] }.Select(x => x.SnapshotId).ToHashSet(), Ids(t));
        await Valid(t);

        // A branch's min-snapshots-to-keep beats an explicit retainLast for that branch only.
        await t.ManageSnapshots().SetMinSnapshotsToKeep(SnapshotRef.Main, 2).RemoveBranch("young").CommitAsync();
        result = await Maintenance.ExpireAsync(t, olderThanMs: long.MaxValue, retainLast: 1);
        Assert.Equal(new[] { s[0].SnapshotId }, result.Expired); // keep3 still holds s1-s3; main's chain stops at s5 (s4 is gone)
        await Valid(t);
    }

    [Fact]
    public async Task Refs_older_than_their_max_ref_age_are_removed_but_never_main()
    {
        var (t, s) = await Chain(4, properties: new() { [Maintenance.MaxRefAgeMs] = "3600000" }); // table default: 1 hour
        await t.ManageSnapshots()
            .CreateTag("old-tag", s[0].SnapshotId, new RefRetention(MaxRefAgeMs: 1))                // ages out
            .CreateTag("kept-tag", s[1].SnapshotId, new RefRetention(MaxRefAgeMs: long.MaxValue))     // never ages out
            .CreateBranch("default-age", s[2].SnapshotId)                                             // 1 hour, from the table
            .SetMaxRefAgeMs(SnapshotRef.Main, 1)                                                      // ignored for main
            .CommitAsync();

        var soon = s[^1].TimestampMs + 60_000; // a minute later: only old-tag is past its age
        var result = await Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = soon, OlderThanMs = long.MaxValue });
        Assert.Equal(["old-tag"], result.RemovedRefs);
        Assert.Contains(s[0].SnapshotId, result.Expired);
        Assert.Equal(new[] { "default-age", "kept-tag", SnapshotRef.Main }, t.Metadata.Refs.Keys.Order(StringComparer.Ordinal));
        await Valid(t);

        var later = s[^1].TimestampMs + 7_200_000; // two hours: the table default removes default-age too
        result = await Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = later, OlderThanMs = long.MaxValue });
        Assert.Equal(["default-age"], result.RemovedRefs);
        Assert.Equal(new[] { "kept-tag", SnapshotRef.Main }, t.Metadata.Refs.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(new[] { s[1].SnapshotId, s[3].SnapshotId }.ToHashSet(), Ids(t));
        await Valid(t);
    }

    [Fact]
    public async Task Unreferenced_snapshots_stay_while_young_and_explicit_ids_expire_unless_referenced()
    {
        var (t, s) = await Chain(3);
        await t.ManageSnapshots().RollbackTo(s[1].SnapshotId).CreateTag("t1", s[0].SnapshotId).CommitAsync(); // s2 is now unreferenced
        var now = s[^1].TimestampMs + 1;
        var young = await Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = now, OlderThanMs = s[2].TimestampMs });
        Assert.Empty(young.Expired); // s2 is not older than the cutoff; s0 is tagged; s1 is main
        await Assert.ThrowsAsync<ArgumentException>(() => Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = now, SnapshotIds = [s[0].SnapshotId] }));

        var picked = await Maintenance.ExpireAsync(t, new ExpireOptions { NowMs = now, SnapshotIds = [s[2].SnapshotId] });
        Assert.Equal([s[2].SnapshotId], picked.Expired);
        Assert.NotEmpty(picked.DeletedFiles); // its manifest list and data file
        await Valid(t);
    }
}
