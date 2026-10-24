using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Branches, tags, rollback, cherry-pick and fast-forward (Java's ManageSnapshots), each followed by deep validation.</summary>
[Spec("Snapshot References")]
public sealed class SnapshotManagementTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-refs", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public SnapshotManagementTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("r")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    internal static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "v", PrimitiveType.String),
        NestedField.Require(3, "p", PrimitiveType.Int),
    ], identifierFieldIds: [1]);

    internal static List<object?[]> Rows(int from, int n, string v = "a") => Enumerable.Range(from, n).Select(i => new object?[] { (long)i, v, i % 2 }).ToList();

    private Task<Table> NewTable(int formatVersion = 2, bool partitioned = false) =>
        _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("r", $"t{Guid.NewGuid():N}"), Schema,
            partitioned ? PartitionSpec.BuilderFor(Schema).Identity("p").Build() : null, FormatVersion: formatVersion));

    /// <summary>Append rows and return the new snapshot ID (a little apart in time, so time-based lookups are unambiguous).</summary>
    private static async Task<long> Append(Table t, int from, int n, string v = "a")
    {
        await Task.Delay(3);
        await TableMutations.InsertAsync(t, Rows(from, n, v));
        return t.Metadata.CurrentSnapshotId!.Value;
    }

    /// <summary>Reload from the catalog (so refs went through metadata JSON) and validate every ref's history deeply.</summary>
    private async Task<Table> Valid(Table t)
    {
        var fresh = await _catalog.LoadTableAsync(t.Identifier);
        await TableValidator.AssertValidAsync(fresh, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.Equal(fresh.Metadata.Refs.OrderBy(kv => kv.Key), t.Metadata.Refs.OrderBy(kv => kv.Key));
        return fresh;
    }

    private static async Task<List<long>> Ids(Table t, SnapshotSelector at)
    {
        var ids = new List<long>();
        await foreach (var row in TableScan.ReadRowsAsync(t, new ScanRequest { Snapshot = at })) ids.Add((long)row[0]!);
        return ids.Order().ToList();
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Branches_and_tags_are_created_replaced_renamed_and_removed(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        var s1 = await Append(t, 0, 3);
        var s2 = await Append(t, 3, 3);

        await t.ManageSnapshots()
            .CreateTag("v1", s1, new RefRetention(MaxRefAgeMs: 86_400_000))
            .CreateBranch("dev", retention: new RefRetention(MinSnapshotsToKeep: 2, MaxSnapshotAgeMs: 3_600_000))
            .CommitAsync();
        Assert.Equal(new SnapshotRef(s1, SnapshotRefType.Tag, MaxRefAgeMs: 86_400_000), t.Metadata.Refs["v1"]);
        Assert.Equal(new SnapshotRef(s2, SnapshotRefType.Branch, 2, 3_600_000), t.Metadata.Refs["dev"]);
        t = await Valid(t);
        Assert.Equal([0L, 1, 2], await Ids(t, SnapshotSelector.Ref("v1")));
        Assert.Equal([0L, 1, 2, 3, 4, 5], await Ids(t, SnapshotSelector.Ref("dev")));

        await t.ManageSnapshots().ReplaceTag("v1", s2).RenameBranch("dev", "feature").SetMaxRefAgeMs("feature", 1000).CommitAsync();
        Assert.Equal(s2, t.Metadata.Refs["v1"].SnapshotId);
        Assert.False(t.Metadata.Refs.ContainsKey("dev"));
        Assert.Equal(new SnapshotRef(s2, SnapshotRefType.Branch, 2, 3_600_000, 1000), t.Metadata.Refs["feature"]); // retention moved with it
        t = await Valid(t);

        await t.ManageSnapshots().ReplaceBranch("feature", s1).SetMinSnapshotsToKeep("feature", 5).SetMaxSnapshotAgeMs("feature", 10).CommitAsync();
        Assert.Equal(new SnapshotRef(s1, SnapshotRefType.Branch, 5, 10, 1000), t.Metadata.Refs["feature"]);
        Assert.Equal(s2, t.Metadata.CurrentSnapshotId); // main untouched
        t = await Valid(t);

        await t.ManageSnapshots().RemoveTag("v1").RemoveBranch("feature").CommitAsync();
        Assert.Equal([SnapshotRef.Main], t.Metadata.Refs.Keys);
        await Valid(t);
    }

    [Fact]
    public async Task Invalid_ref_changes_are_rejected_before_anything_is_committed()
    {
        var t = await NewTable();
        var s1 = await Append(t, 0, 2);
        await t.ManageSnapshots().CreateTag("v1", s1).CreateBranch("b").CommitAsync();
        var before = t.Metadata.MetadataFileLocation;

        Assert.Throws<ArgumentException>(() => t.ManageSnapshots().RemoveBranch(SnapshotRef.Main));
        Assert.Throws<ArgumentException>(() => t.ManageSnapshots().RenameBranch(SnapshotRef.Main, "x"));
        Assert.Throws<ArgumentException>(() => t.ManageSnapshots().CreateTag("t", s1, new RefRetention(MinSnapshotsToKeep: 1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => t.ManageSnapshots().SetMinSnapshotsToKeep("b", 0));
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().CreateTag("v1", s1).CommitAsync());       // exists
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().CreateBranch("v1").CommitAsync());        // exists (as a tag)
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().ReplaceTag("b", s1).CommitAsync());       // a branch
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().RemoveBranch("v1").CommitAsync());        // a tag
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().SetMinSnapshotsToKeep("v1", 3).CommitAsync());
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().CreateTag("t", 12345).CommitAsync());      // no such snapshot
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().RenameBranch("b", "v1").CommitAsync());   // target exists
        // One bad change fails the whole set: the valid tag before it is not created either.
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().CreateTag("ok", s1).RemoveTag("missing").CommitAsync());
        Assert.Equal(before, t.Metadata.MetadataFileLocation);
        Assert.False((await _catalog.LoadTableAsync(t.Identifier)).Metadata.Refs.ContainsKey("ok"));
    }

    [Spec("Point in Time Reads (Time Travel)")]
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Rollback_moves_main_to_an_ancestor_only_and_set_current_to_any_snapshot(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        var s1 = await Append(t, 0, 2);
        var s2 = await Append(t, 2, 2);
        var s3 = await Append(t, 4, 2);

        await t.ManageSnapshots().RollbackTo(s1).CommitAsync();
        Assert.Equal(s1, t.Metadata.CurrentSnapshotId);
        Assert.Equal(s1, t.Metadata.SnapshotLog[^1].SnapshotId); // made current again, now
        Assert.Equal([0L, 1], await Ids(t, SnapshotSelector.Current));
        Assert.NotNull(t.Metadata.SnapshotById(s3));               // rolled-back snapshots stay until expired
        t = await Valid(t);

        // s3 is no longer an ancestor of main: Java's rollbackTo refuses; setCurrentSnapshot does not.
        await Assert.ThrowsAsync<ValidationException>(() => t.ManageSnapshots().RollbackTo(s3).CommitAsync());
        await t.ManageSnapshots().SetCurrentSnapshot(s3).CommitAsync();
        Assert.Equal([0L, 1, 2, 3, 4, 5], await Ids(t, SnapshotSelector.Current));
        t = await Valid(t);

        // The latest ancestor strictly older than the instant: just after s2 was committed → s2; at s1's own instant → none.
        var s2At = t.Metadata.SnapshotById(s2)!.TimestampMs;
        await t.ManageSnapshots().RollbackToTime(DateTimeOffset.FromUnixTimeMilliseconds(s2At + 1)).CommitAsync();
        Assert.Equal(s2, t.Metadata.CurrentSnapshotId);
        await Assert.ThrowsAsync<ValidationException>(() =>
            t.ManageSnapshots().RollbackToTime(DateTimeOffset.FromUnixTimeMilliseconds(t.Metadata.SnapshotById(s1)!.TimestampMs)).CommitAsync());
        await Valid(t);

        // The snapshot log tells time travel which snapshot was current when: after the rollback it is s2, not s3.
        Assert.Equal(s2, TimeTravel.Resolve(t.Metadata, SnapshotSelector.AsOf(DateTimeOffset.UtcNow.AddSeconds(1)))!.SnapshotId);
    }

    [Fact]
    public async Task Main_keeps_its_retention_settings_when_it_moves()
    {
        var t = await NewTable();
        var s1 = await Append(t, 0, 2);
        await t.ManageSnapshots().SetMinSnapshotsToKeep(SnapshotRef.Main, 3).CommitAsync();
        await Append(t, 2, 2);
        await t.ManageSnapshots().RollbackTo(s1).CommitAsync();
        Assert.Equal(new SnapshotRef(s1, SnapshotRefType.Branch, 3), t.Metadata.Refs[SnapshotRef.Main]);
        await Valid(t);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Fast_forward_requires_the_branch_to_be_an_ancestor(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        var s1 = await Append(t, 0, 2);
        var s2 = await Append(t, 2, 2);
        await t.ManageSnapshots().CreateBranch("ahead", s2).RollbackTo(s1).CommitAsync();
        Assert.Equal(s1, t.Metadata.CurrentSnapshotId);

        await t.ManageSnapshots().FastForwardBranch(SnapshotRef.Main, "ahead").CommitAsync();
        Assert.Equal(s2, t.Metadata.CurrentSnapshotId);
        t = await Valid(t);

        // Main moves on to s3: main cannot be fast-forwarded to "ahead" (s2) any more, but "ahead" can be to main.
        var s3 = await Append(t, 4, 2);
        await Assert.ThrowsAsync<ArgumentException>(() => t.ManageSnapshots().FastForwardBranch(SnapshotRef.Main, "ahead").CommitAsync());
        await t.ManageSnapshots().FastForwardBranch("ahead", SnapshotRef.Main).CommitAsync();
        Assert.Equal(s3, t.Metadata.Refs["ahead"].SnapshotId);
        // replaceBranch(from, to) moves regardless of ancestry.
        await t.ManageSnapshots().ReplaceBranch("ahead", s1).CommitAsync();
        await t.ManageSnapshots().ReplaceBranch(SnapshotRef.Main, "ahead").CommitAsync();
        Assert.Equal(s1, t.Metadata.CurrentSnapshotId);
        await Valid(t);
    }

    /// <summary>Stage a snapshot the classic WAP way: commit it to main, then roll main back past it.</summary>
    private static async Task<long> Staged(Table t, Func<Task> write)
    {
        var parent = t.Metadata.CurrentSnapshotId!.Value;
        await write();
        var staged = t.Metadata.CurrentSnapshotId!.Value;
        await t.ManageSnapshots().RollbackTo(parent).CommitAsync();
        return staged;
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Cherry_pick_fast_forwards_or_replays_an_append_once(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        await Append(t, 0, 2);
        var staged = await Staged(t, () => TableMutations.InsertAsync(t, Rows(10, 2)));

        // Parent is main's head: published as is.
        await t.ManageSnapshots().CherryPick(staged).CommitAsync();
        Assert.Equal(staged, t.Metadata.CurrentSnapshotId);
        t = await Valid(t);

        var staged2 = await Staged(t, () => TableMutations.InsertAsync(t, Rows(20, 2)));
        var concurrent = await Append(t, 30, 1);
        var count = t.Metadata.Snapshots.Count;
        await t.ManageSnapshots().CherryPick(staged2).CommitAsync();
        var picked = t.Metadata.CurrentSnapshot!;
        Assert.Equal(count + 1, t.Metadata.Snapshots.Count);
        Assert.Equal(concurrent, picked.ParentSnapshotId);
        Assert.Equal(Operations.Append, picked.Operation);
        Assert.Equal(staged2.ToString(System.Globalization.CultureInfo.InvariantCulture), picked.Summary["source-snapshot-id"]);
        Assert.Equal([0L, 1, 10, 11, 20, 21, 30], await Ids(t, SnapshotSelector.Current));
        t = await Valid(t);

        // Picked already, or already an ancestor: refused.
        await Assert.ThrowsAsync<ValidationException>(() => t.ManageSnapshots().CherryPick(staged2).CommitAsync());
        await Assert.ThrowsAsync<ValidationException>(() => t.ManageSnapshots().CherryPick(concurrent).CommitAsync());
        await Assert.ThrowsAsync<ValidationException>(() => t.ManageSnapshots().CherryPick(424242).CommitAsync());
        Assert.Throws<InvalidOperationException>(() => t.ManageSnapshots().CreateTag("x", concurrent).CherryPick(staged2));
        await Valid(t);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Cherry_pick_of_an_overwrite_only_fast_forwards(int formatVersion)
    {
        var t = await NewTable(formatVersion);
        await Append(t, 0, 4);
        var staged = await Staged(t, () => TableMutations.DeleteWhereAsync(t, Expressions.Expr.Equal("id", 1L), DeleteStrategy.CopyOnWrite));
        await Append(t, 10, 1);
        await Assert.ThrowsAsync<ValidationException>(() => t.ManageSnapshots().CherryPick(staged).CommitAsync());

        var staged2 = await Staged(t, () => TableMutations.UpsertAsync(t, Rows(2, 1, "new"), strategy: DeleteStrategy.PositionDeletes));
        await t.ManageSnapshots().CherryPick(staged2).CommitAsync();
        Assert.Equal(staged2, t.Metadata.CurrentSnapshotId);
        await Valid(t);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Cherry_pick_replays_a_dynamic_partition_overwrite_unless_its_partitions_changed(int formatVersion)
    {
        var t = await NewTable(formatVersion, partitioned: true);
        await Append(t, 0, 6); // p=0: 0,2,4  p=1: 1,3,5
        async Task<long> StageOverwriteOfP0(string v) => await Staged(t, async () =>
        {
            var p0 = (await TableScan.PlanFilesAsync(t)).Where(x => (int)x.File.Partition[0]! == 0).Select(x => x.File.FilePath).ToHashSet();
            await SnapshotProducer.Overwrite(t, f => p0.Contains(f.FilePath)).Set("replace-partitions", "true")
                .AddFiles(await TableWriter.WriteAsync(t, [[100L, v, 0]])).CommitAsync();
        });

        var staged = await StageOverwriteOfP0("x");
        await Append(t, 7, 1); // p=1 only: does not touch the replaced partition
        await t.ManageSnapshots().CherryPick(staged).CommitAsync();
        Assert.Equal([1L, 3, 5, 7, 100], await Ids(t, SnapshotSelector.Current));
        Assert.Equal("true", t.Metadata.CurrentSnapshot!.Summary["replace-partitions"]);
        t = await Valid(t);

        var staged2 = await StageOverwriteOfP0("y");
        await Append(t, 8, 1); // p=0: the partition staged2 replaces changed since its parent
        await Assert.ThrowsAsync<ValidationException>(() => t.ManageSnapshots().CherryPick(staged2).CommitAsync());
        await Valid(t);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Cherry_pick_replays_files_written_under_an_older_partition_spec(int formatVersion)
    {
        var t = await NewTable(formatVersion, partitioned: true); // spec 0: identity(p)
        await Append(t, 0, 4);
        var staged = await Staged(t, () => TableMutations.InsertAsync(t, Rows(10, 4))); // written under spec 0
        var p0 = (await TableScan.PlanFilesAsync(t)).Where(x => (int)x.File.Partition[0]! == 0).Select(x => x.File.FilePath).ToHashSet();
        var stagedOverwrite = await Staged(t, async () =>
            await SnapshotProducer.Overwrite(t, f => p0.Contains(f.FilePath)).Set("replace-partitions", "true")
                .AddFiles(await TableWriter.WriteAsync(t, [[100L, "x", 0]])).CommitAsync());

        // The spec evolves (spec 1: bucket(id) instead of p) and main moves on under it before the picks.
        await t.UpdateSpecAsync(u => u.RemoveField("p").AddField("id", Transforms.Transform.Bucket(4)));
        t = await _catalog.LoadTableAsync(t.Identifier);
        Assert.NotEqual(0, t.Metadata.DefaultSpecId);
        await Append(t, 20, 2);

        // A dynamic overwrite staged under spec 0: main's spec-1 append since its parent is in other partitions (partitions
        // are compared with their spec), so it replays, and the p=0 rows it replaced (0, 2) stay replaced.
        await t.ManageSnapshots().CherryPick(stagedOverwrite).CommitAsync();
        Assert.Equal("true", t.Metadata.CurrentSnapshot!.Summary["replace-partitions"]);
        Assert.Equal([1L, 3, 20, 21, 100], await Ids(t, SnapshotSelector.Current));
        t = await Valid(t);

        await t.ManageSnapshots().CherryPick(staged).CommitAsync();
        var picked = t.Metadata.CurrentSnapshot!;
        Assert.Equal(staged.ToString(System.Globalization.CultureInfo.InvariantCulture), picked.Summary["source-snapshot-id"]);
        Assert.Equal([1L, 3, 10, 11, 12, 13, 20, 21, 100], await Ids(t, SnapshotSelector.Current));
        // The replayed files keep spec 0 (and their spec-0 partition tuples), in a manifest of spec 0.
        var added = (await TableScan.ManifestsAsync(t, picked)).Where(mf => mf.AddedSnapshotId == picked.SnapshotId).ToList();
        Assert.Equal([0], added.Select(mf => mf.PartitionSpecId));
        Assert.All((await TableScan.PlanFilesAsync(t)).Where(x => x.File.SpecId == 0), x => Assert.Single(x.File.Partition));
        await Valid(t);
    }

    [Fact]
    public async Task One_commit_may_add_files_of_several_partition_specs()
    {
        var t = await NewTable(2, partitioned: true);
        var old = await TableWriter.WriteAsync(t, Rows(0, 2));
        await t.UpdateSpecAsync(u => u.RemoveField("p"));
        t = await _catalog.LoadTableAsync(t.Identifier);
        var current = await TableWriter.WriteAsync(t, Rows(10, 2));
        await SnapshotProducer.Append(t).AddFiles([.. old, .. current]).CommitAsync();
        var manifests = await TableScan.ManifestsAsync(t, t.Metadata.CurrentSnapshot!);
        Assert.Equal([t.Metadata.DefaultSpecId, 0], manifests.Select(mf => mf.PartitionSpecId));
        Assert.Equal([0L, 1, 10, 11], await Ids(t, SnapshotSelector.Current));
        await Valid(t);

        var unknown = current[0] with { SpecId = 42 };
        await Assert.ThrowsAsync<ValidationException>(() => SnapshotProducer.Append(t).AddFiles([unknown]).CommitAsync());
    }
}
