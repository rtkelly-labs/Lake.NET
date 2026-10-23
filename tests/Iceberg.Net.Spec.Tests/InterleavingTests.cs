using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Deterministic interleavings: another writer's change lands after this writer planned its change and before it
/// swaps the metadata pointer, the window where Glue compaction, Athena DML or a second producer races us. Every
/// pair asserts the serializable outcome and a valid table (docs: "Snapshot conflicts with Athena and Glue, by
/// timing").
/// </summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("Optimistic Concurrency")]
public sealed class InterleavingTests : IDisposable
{
    /// <summary>Runs <see cref="BeforeNextSwap"/> once, just before the next pointer swap, so its commit lands first.</summary>
    private sealed class InterleavingCatalog(string warehouse) : InMemoryCatalog(warehouse)
    {
        public Func<Task>? BeforeNextSwap { get; set; }

        protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
        {
            if (BeforeNextSwap is { } interfere)
            {
                BeforeNextSwap = null; // the interfering commit swaps through here too
                await interfere();
            }
            await base.SwapMetadataLocationAsync(id, expected, newLocation, metadata, ct);
        }
    }

    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-interleave", Guid.NewGuid().ToString("N"));
    private readonly InterleavingCatalog _catalog;

    public InterleavingTests()
    {
        _catalog = new InterleavingCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("i")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "v", PrimitiveType.String)], identifierFieldIds: [1]);

    /// <summary>A table holding ids 1..6 across three small files, so compaction has work; plus a second handle on it.</summary>
    private async Task<(Table Subject, Table Other)> SetupAsync(int formatVersion)
    {
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("i", $"t{Guid.NewGuid():N}"), Schema, FormatVersion: formatVersion));
        for (var i = 1L; i <= 6; i += 2) await TableMutations.InsertAsync(t, [[i, "base"], [i + 1, "base"]]);
        await t.RefreshAsync();
        return (t, await _catalog.LoadTableAsync(t.Identifier));
    }

    /// <summary>The live table's rows as id → v, after asserting it passes deep validation over every snapshot.</summary>
    private async Task<Dictionary<long, string?>> StateAsync(Table t)
    {
        var fresh = await _catalog.LoadTableAsync(t.Identifier);
        var report = await TableValidator.ValidateAsync(fresh, new ValidationOptions { Deep = true, AllSnapshots = true });
        Assert.True(report.IsValid, report.ToString());
        var rows = await TableScan.ReadAllAsync(fresh);
        Assert.Equal(rows.Count, rows.Select(r => (long)r[0]!).Distinct().Count()); // no key appears twice
        return rows.ToDictionary(r => (long)r[0]!, r => (string?)r[1]);
    }

    private static readonly Expression Id2 = Expr.Equal("id", 2L);

    // The interfering writers: each is a change Glue, Athena or another producer could commit meanwhile.
    public static TheoryData<string> Interferers => new() { "append", "upsert-2", "delete-2", "compaction", "expire", "orphan-cleanup" };

    private static Func<Task> Interferer(string kind, Table other) => kind switch
    {
        "append" => () => TableMutations.InsertAsync(other, [[10L, "other"]]),
        "upsert-2" => () => TableMutations.UpsertAsync(other, [[2L, "other"]], strategy: DeleteStrategy.PositionDeletes),
        "delete-2" => () => TableMutations.DeleteWhereAsync(other, Id2, DeleteStrategy.PositionDeletes),
        "compaction" => () => Compaction.RewriteDataFilesAsync(other, new RewriteOptions(MinInputFiles: 2, RewriteAll: true)),
        "expire" => async () =>
        {
            await other.RefreshAsync();
            await Maintenance.ExpireAsync(other, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 1, retainLast: 1);
        },
        // At the default age, files this writer has just written are not orphans yet and must survive.
        "orphan-cleanup" => async () =>
        {
            await other.RefreshAsync();
            await Maintenance.DeleteOrphanFilesAsync(other);
        },
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>What the interferer leaves for id 2 and id 10 when it commits alone.</summary>
    private static void AssertInterfererKept(string kind, Dictionary<long, string?> state)
    {
        if (kind == "append") Assert.Equal("other", state[10]);
        else Assert.False(state.ContainsKey(10));
    }

    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Append_always_lands(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        {
            var (subject, other) = await SetupAsync(v);
            _catalog.BeforeNextSwap = Interferer(kind, other);
            await TableMutations.InsertAsync(subject, [[20L, "mine"]]);

            var state = await StateAsync(subject);
            Assert.Equal("mine", state[20]);
            AssertInterfererKept(kind, state);
            Assert.Equal(kind == "delete-2" ? 6 : kind == "append" ? 8 : 7, state.Count);
        }
    }

    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Upsert_of_a_key_is_the_last_word_whatever_lands_first(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        foreach (var strategy in new[] { DeleteStrategy.PositionDeletes, DeleteStrategy.CopyOnWrite, DeleteStrategy.EqualityDeletes })
        {
            var (subject, other) = await SetupAsync(v);
            _catalog.BeforeNextSwap = Interferer(kind, other);
            await TableMutations.UpsertAsync(subject, [[2L, "mine"]], strategy: strategy);

            var state = await StateAsync(subject);
            Assert.Equal("mine", state[2]); // re-planned on the winner's state: one row, ours
            AssertInterfererKept(kind, state);
            foreach (var id in new long[] { 1, 3, 4, 5, 6 }) Assert.Equal("base", state[id]);
        }
    }

    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Delete_removes_the_key_even_if_it_was_rewritten_meanwhile(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        foreach (var strategy in new[] { DeleteStrategy.PositionDeletes, DeleteStrategy.CopyOnWrite })
        {
            var (subject, other) = await SetupAsync(v);
            _catalog.BeforeNextSwap = Interferer(kind, other);
            await TableMutations.DeleteWhereAsync(subject, Id2, strategy);

            var state = await StateAsync(subject);
            Assert.False(state.ContainsKey(2));
            AssertInterfererKept(kind, state);
            foreach (var id in new long[] { 1, 3, 4, 5, 6 }) Assert.Equal("base", state[id]);
        }
    }

    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Compaction_never_undoes_a_concurrent_change(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        {
            var (subject, other) = await SetupAsync(v);
            _catalog.BeforeNextSwap = Interferer(kind, other);
            await Compaction.RewriteDataFilesAsync(subject, new RewriteOptions(MinInputFiles: 2, RewriteAll: true));

            var state = await StateAsync(subject);
            AssertInterfererKept(kind, state);
            switch (kind)
            {
                case "upsert-2": Assert.Equal("other", state[2]); break; // not resurrected as "base"
                case "delete-2": Assert.False(state.ContainsKey(2)); break; // not resurrected
                default: Assert.Equal("base", state[2]); break;
            }
        }
    }

    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Overwrite_is_serializable_against_every_interferer(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        {
            var (subject, other) = await SetupAsync(v);
            // Replace every file holding id 2 with one row; a concurrent change to those files must not be lost.
            var plan = await TableScan.PlanFilesAsync(subject, Id2);
            var targets = plan.Select(t => t.File.FilePath).ToHashSet();
            var overwrite = SnapshotProducer.Overwrite(subject, f => targets.Contains(f.FilePath))
                .AddFiles(await TableWriter.WriteAsync(subject, [[1L, "base"], [2L, "mine"]]));
            _catalog.BeforeNextSwap = Interferer(kind, other);

            if (kind == "expire")
            {
                // Expiry kept only the current snapshot, which is still the one this overwrite started from.
                await overwrite.CommitAsync();
                Assert.Equal("mine", (await StateAsync(subject))[2]);
                continue;
            }
            if (kind is "append" or "orphan-cleanup")
            {
                await overwrite.CommitAsync(); // neither touched the files being replaced
                var after = await StateAsync(subject);
                Assert.Equal("mine", after[2]);
                AssertInterfererKept(kind, after);
                continue;
            }
            // upsert-2 / delete-2 add a delete file against a replaced file; compaction removes the replaced files.
            // Either way committing would drop or duplicate the concurrent change, so it must fail (Java's
            // validateNoConflictingDeletes and failMissingDeletePaths) and leave the interferer's result alone.
            await Assert.ThrowsAsync<ValidationException>(() => overwrite.CommitAsync());
            var state = await StateAsync(subject);
            Assert.NotEqual("mine", state.GetValueOrDefault(2));
            if (kind == "upsert-2") Assert.Equal("other", state[2]);
            if (kind == "delete-2") Assert.False(state.ContainsKey(2));
        }
    }

    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Update_re_plans_against_the_winners_rows(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        foreach (var strategy in new[] { DeleteStrategy.PositionDeletes, DeleteStrategy.CopyOnWrite })
        {
            var (subject, other) = await SetupAsync(v);
            _catalog.BeforeNextSwap = Interferer(kind, other);
            var result = await TableMutations.UpdateAsync(subject, Id2, u => u.Set("v", row => $"{row[1]}+mine"), strategy);

            Assert.Equal(kind == "orphan-cleanup" ? 1 : 2, result.Attempts); // the race really happened (orphan cleanup commits nothing)
            var state = await StateAsync(subject);
            AssertInterfererKept(kind, state);
            foreach (var id in new long[] { 1, 3, 4, 5, 6 }) Assert.Equal("base", state[id]);
            switch (kind)
            {
                case "delete-2": // the row is gone in the winner's state: nothing to update, nothing committed
                    Assert.False(state.ContainsKey(2));
                    Assert.Null(result.Snapshot);
                    break;
                case "upsert-2": Assert.Equal("other+mine", state[2]); break; // computed from the winner's value
                default: Assert.Equal("base+mine", state[2]); break;
            }
        }
    }

    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Merge_re_plans_against_the_winners_rows(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        foreach (var strategy in new[] { DeleteStrategy.PositionDeletes, DeleteStrategy.CopyOnWrite, DeleteStrategy.EqualityDeletes })
        {
            var (subject, other) = await SetupAsync(v);
            _catalog.BeforeNextSwap = Interferer(kind, other);
            var bySource = strategy != DeleteStrategy.EqualityDeletes; // equality deletes cannot express "not matched by source"
            var result = await TableMutations.MergeAsync(subject, [[2L, "mine"], [20L, "new"]], ["id"], m =>
            {
                m.WhenMatchedUpdate(u => u.Set("v", row => $"{row[1]}+mine")).WhenNotMatchedInsert();
                if (bySource) m.WhenNotMatchedBySourceDelete(Expr.GreaterThanOrEqual("id", 10L));
            }, strategy);

            Assert.Equal(kind == "orphan-cleanup" ? 1 : 2, result.Attempts);
            var state = await StateAsync(subject);
            Assert.Equal("new", state[20]);
            foreach (var id in new long[] { 1, 3, 4, 5, 6 }) Assert.Equal("base", state[id]);
            if (bySource) Assert.False(state.ContainsKey(10)); // the appended row is seen by the re-plan and deleted
            else AssertInterfererKept(kind, state);
            Assert.Equal(kind switch
            {
                "delete-2" => "mine", // not matched any more: the source row is inserted
                "upsert-2" => "other+mine",
                _ => "base+mine",
            }, state[2]);
        }
    }

    // Ref changes (ManageSnapshots): an operation that moves a ref to a fixed place fails cleanly when that ref moved,
    // one that composes (cherry-pick replay, fast-forward, ref creation) is re-validated and re-applied.

    [Spec("Snapshot References")]
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Rollback_and_set_current_fail_cleanly_when_main_moved(int formatVersion)
    {
        foreach (var setCurrent in new[] { false, true })
        {
            var (subject, other) = await SetupAsync(formatVersion);
            var target = subject.Metadata.CurrentSnapshot!.ParentSnapshotId!.Value;
            _catalog.BeforeNextSwap = Interferer("append", other);
            var manager = subject.ManageSnapshots();
            if (setCurrent) manager.SetCurrentSnapshot(target);
            else manager.RollbackTo(target);
            await Assert.ThrowsAsync<CommitFailedException>(() => manager.CommitAsync());

            var state = await StateAsync(subject);
            Assert.Equal("other", state[10]); // the concurrent append survives: main was not moved over it
            Assert.Equal(7, state.Count);
        }
    }

    [Spec("Snapshot References")]
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Fast_forward_is_revalidated_after_a_concurrent_commit(int formatVersion)
    {
        // The source branch advanced meanwhile: still a fast-forward, so main moves to the new head.
        var (subject, other) = await SetupAsync(formatVersion);
        var parent = subject.Metadata.CurrentSnapshotId!.Value;
        await TableMutations.InsertAsync(subject, [[30L, "staged"]]);
        var first = subject.Metadata.CurrentSnapshotId!.Value;
        await TableMutations.InsertAsync(subject, [[31L, "staged"]]);
        var second = subject.Metadata.CurrentSnapshotId!.Value;
        await subject.ManageSnapshots().CreateBranch("staging", first).RollbackTo(parent).CommitAsync();
        await other.RefreshAsync();
        _catalog.BeforeNextSwap = () => other.ManageSnapshots().ReplaceBranch("staging", second).CommitAsync();
        await subject.ManageSnapshots().FastForwardBranch(SnapshotRef.Main, "staging").CommitAsync();
        Assert.Equal(second, subject.Metadata.CurrentSnapshotId);
        var state = await StateAsync(subject);
        Assert.Equal("staged", state[31]);
        Assert.Equal("staged", state[30]);

        // Main moved meanwhile: no longer a fast-forward, so it fails rather than dropping main's new snapshot.
        (subject, other) = await SetupAsync(formatVersion);
        var head = subject.Metadata.CurrentSnapshotId!.Value;
        parent = subject.Metadata.CurrentSnapshot!.ParentSnapshotId!.Value;
        await subject.ManageSnapshots().CreateBranch("staging", head).RollbackTo(parent).CommitAsync();
        _catalog.BeforeNextSwap = Interferer("append", other);
        await Assert.ThrowsAsync<ArgumentException>(() => subject.ManageSnapshots().FastForwardBranch(SnapshotRef.Main, "staging").CommitAsync());
        state = await StateAsync(subject);
        Assert.Equal("other", state[10]);
        Assert.False(state.ContainsKey(5));
    }

    [Spec("Snapshot References")]
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Cherry_pick_composes_with_a_concurrent_append_and_conflicts_with_a_concurrent_pick(int formatVersion)
    {
        var (subject, other) = await SetupAsync(formatVersion);
        var parent = subject.Metadata.CurrentSnapshotId!.Value;
        await TableMutations.InsertAsync(subject, [[40L, "picked"]]);
        var staged = subject.Metadata.CurrentSnapshotId!.Value;
        await subject.ManageSnapshots().RollbackTo(parent).CommitAsync();
        _catalog.BeforeNextSwap = Interferer("append", other);
        await subject.ManageSnapshots().CherryPick(staged).CommitAsync(); // was a fast-forward; replayed after the race
        var state = await StateAsync(subject);
        Assert.Equal("picked", state[40]);
        Assert.Equal("other", state[10]);

        // Two writers publish the same staged snapshot: exactly one wins.
        (subject, other) = await SetupAsync(formatVersion);
        parent = subject.Metadata.CurrentSnapshotId!.Value;
        await TableMutations.InsertAsync(subject, [[40L, "picked"]]);
        staged = subject.Metadata.CurrentSnapshotId!.Value;
        await subject.ManageSnapshots().RollbackTo(parent).CommitAsync();
        await TableMutations.InsertAsync(subject, [[41L, "main"]]); // so the picks are replays, not fast-forwards
        await other.RefreshAsync();
        _catalog.BeforeNextSwap = () => other.ManageSnapshots().CherryPick(staged).CommitAsync();
        await Assert.ThrowsAsync<ValidationException>(() => subject.ManageSnapshots().CherryPick(staged).CommitAsync());
        state = await StateAsync(subject);
        Assert.Equal("picked", state[40]);
        Assert.Equal(8, state.Count);
    }

    [Spec("Snapshot References")]
    [Fact]
    public async Task Concurrent_tag_changes_conflict_on_the_tag()
    {
        var (subject, other) = await SetupAsync(2);
        var head = subject.Metadata.CurrentSnapshotId!.Value;
        var parent = subject.Metadata.CurrentSnapshot!.ParentSnapshotId!.Value;
        _catalog.BeforeNextSwap = () => other.ManageSnapshots().CreateTag("release", parent).CommitAsync();
        await Assert.ThrowsAsync<ArgumentException>(() => subject.ManageSnapshots().CreateTag("release", head).CommitAsync());
        Assert.Equal(parent, (await _catalog.LoadTableAsync(subject.Identifier)).Metadata.Refs["release"].SnapshotId);

        await subject.RefreshAsync();
        await other.RefreshAsync();
        _catalog.BeforeNextSwap = () => other.ManageSnapshots().ReplaceTag("release", head).CommitAsync();
        await Assert.ThrowsAsync<CommitFailedException>(() => subject.ManageSnapshots().RemoveTag("release").CommitAsync());
        Assert.Equal(head, (await _catalog.LoadTableAsync(subject.Identifier)).Metadata.Refs["release"].SnapshotId);
        await StateAsync(subject);
    }

    private static async Task<Dictionary<long, string?>> BranchRowsAsync(Table t, string branch)
    {
        var rows = new Dictionary<long, string?>();
        await foreach (var r in TableScan.ReadRowsAsync(t, new ScanRequest { Snapshot = SnapshotSelector.Ref(branch) })) rows.Add((long)r[0]!, (string?)r[1]);
        return rows;
    }

    [Spec("Snapshot References")]
    [Theory]
    [MemberData(nameof(Interferers))]
    public async Task Branch_overwrites_are_isolated_from_every_writer_on_main(string kind)
    {
        foreach (var v in new[] { 2, 3 })
        {
            var (subject, other) = await SetupAsync(v);
            await subject.ManageSnapshots().CreateBranch("audit").CommitAsync();
            await other.RefreshAsync();
            var targets = (await TableScan.PlanFilesAsync(subject, Id2, subject.Metadata.Refs["audit"].SnapshotId)).Select(t => t.File.FilePath).ToHashSet();
            var overwrite = SnapshotProducer.Overwrite(subject, f => targets.Contains(f.FilePath))
                .AddFiles(await TableWriter.WriteAsync(subject, [[1L, "base"], [2L, "mine"]])).ToBranch("audit");
            _catalog.BeforeNextSwap = Interferer(kind, other);
            await overwrite.CommitAsync();

            var main = await StateAsync(subject); // validates every ref's history
            AssertInterfererKept(kind, main);
            Assert.NotEqual("mine", main.GetValueOrDefault(2));
            var audit = await BranchRowsAsync(await _catalog.LoadTableAsync(subject.Identifier), "audit");
            Assert.Equal(Enumerable.Range(1, 6).Select(i => (long)i), audit.Keys.Order());
            Assert.Equal("mine", audit[2]);
        }
    }

    [Spec("Snapshot References")]
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Branch_commits_conflict_with_writers_on_the_same_branch(int formatVersion)
    {
        foreach (var concurrent in new[] { "append", "delete-2" })
        {
            var (subject, other) = await SetupAsync(formatVersion);
            await subject.ManageSnapshots().CreateBranch("audit").CommitAsync();
            await other.RefreshAsync();
            var head = subject.Metadata.Refs["audit"].SnapshotId;
            var targets = (await TableScan.PlanFilesAsync(subject, Id2, head)).Select(t => t.File.FilePath).ToHashSet();
            var overwrite = SnapshotProducer.Overwrite(subject, f => targets.Contains(f.FilePath))
                .AddFiles(await TableWriter.WriteAsync(subject, [[1L, "base"], [2L, "mine"]])).ToBranch("audit");
            _catalog.BeforeNextSwap = async () =>
            {
                if (concurrent == "append")
                {
                    await SnapshotProducer.Append(other).AddFiles(await TableWriter.WriteAsync(other, [[10L, "other"]])).ToBranch("audit").CommitAsync();
                    return;
                }
                var file = targets.Single();
                var pos = (await DataFiles.ReadAsync(other.IO, file, "PARQUET", Schema)).FindIndex(r => (long)r[0]! == 2);
                await SnapshotProducer.DeleteRows(other, new Dictionary<string, IEnumerable<long>> { [file] = [pos] }).ToBranch("audit").CommitAsync();
            };

            if (concurrent == "append")
            {
                await overwrite.CommitAsync(); // rebased onto the other branch commit
                var audit = await BranchRowsAsync(await _catalog.LoadTableAsync(subject.Identifier), "audit");
                Assert.Equal("mine", audit[2]);
                Assert.Equal("other", audit[10]);
            }
            else
            {
                await Assert.ThrowsAsync<ValidationException>(() => overwrite.CommitAsync()); // would resurrect id 2 on the branch
                Assert.False((await BranchRowsAsync(await _catalog.LoadTableAsync(subject.Identifier), "audit")).ContainsKey(2));
            }
            var main = await StateAsync(subject);
            Assert.Equal("base", main[2]); // main never saw either branch commit
            Assert.Equal(6, main.Count);
        }
    }
}
