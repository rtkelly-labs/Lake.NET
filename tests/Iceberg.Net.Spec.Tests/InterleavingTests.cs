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
}
