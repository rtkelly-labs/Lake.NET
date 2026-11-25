using System.Collections.Concurrent;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;
using Microsoft.Extensions.Time.Testing;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Crash consistency. Every write, read and delete an operation performs is made to fail (writes before they land,
/// after they land, or torn half-way; reads failing or stalling until the caller cancels; deletes refused), one fault
/// point per run. After each fault the four invariants of the chaos model must hold:
/// <list type="number">
/// <item><b>Before or after:</b> the table reads exactly the state before or the state after the operation, and an
/// error other than an unknown outcome or a cancellation means nothing was applied. Multi-commit operations (partial
/// compaction, orphan cleanup) may stop part-way, but never change what the table reads.</item>
/// <item><b>Valid:</b> a deep fsck over every snapshot passes.</item>
/// <item><b>Nothing possibly committed is deleted:</b> no file the resulting table references was deleted.</item>
/// <item><b>Retry converges:</b> retrying what did not apply (or, for idempotent operations, retrying anyway)
/// reaches exactly the fault-free result: no lost or duplicated rows, snapshots, statistics or schema changes.</item>
/// </list>
/// Read points are sampled (seeded by the operation's name) when an operation has more than <c>ICEBERG_FAULT_POINTS</c>
/// (default 8) of them, stall and torn-write points when it has more than half that; set it to 0 for every point.
/// </summary>
[Spec("Commit Conflict Resolution and Retry")]
[Spec("File System Operations")]
[Spec("Snapshot Retention Policy")]
public sealed class FaultInjectionTests
{
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "event_date", PrimitiveType.Date),
    ], identifierFieldIds: [1, 3]);

    private static readonly TableIdentifier T = TableIdentifier.Of("f", "t");
    private static readonly TableIdentifier U = TableIdentifier.Of("f", "u");
    private const string Branch = "b";

    private static object?[] Row(long id, string name, int day) => [id, name, 19_700 + day];

    /// <param name="Atomic">One commit: the table is before or after, and a definite error means before.</param>
    /// <param name="Idempotent">Running it again after it took effect changes nothing, so a retry needs no check first.</param>
    /// <param name="MergeManifests">Create the table with <c>commit.manifest.min-count-to-merge=2</c> (merge-append on every commit).</param>
    /// <param name="ObserveFiles">The operation's effect is on storage only (orphan cleanup): observe the file count too.</param>
    public sealed record Operation(string Name, int FormatVersion, Func<InMemoryCatalog, Table, CancellationToken, Task> Run,
        bool Atomic = true, bool Idempotent = false, bool MergeManifests = false, bool ObserveFiles = false)
    {
        public Operation(string name, int formatVersion, Func<Table, Task> run) : this(name, formatVersion, (_, t, _) => run(t)) { }
        public override string ToString() => $"{Name} v{FormatVersion}";
    }

    /// <summary>Orphan cleanup sees every unreferenced file as old: its clock runs well past the default age.</summary>
    private static readonly TimeProvider Later = new FakeTimeProvider(DateTimeOffset.UtcNow.AddDays(30));

    private static IEnumerable<Operation> AllOperations()
    {
        foreach (var v in new[] { 2, 3 })
        {
            yield return new Operation("insert", v, t => TableMutations.InsertAsync(t, [Row(100, "n", 0)]));
            foreach (var s in new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes, DeleteStrategy.EqualityDeletes })
            {
                yield return new Operation($"upsert/{s}", v, t => TableMutations.UpsertAsync(t, [Row(1, "u", 1), Row(50, "u", 2)], strategy: s));
                yield return new Operation($"replace-groups/{s}", v, t => TableMutations.ReplaceGroupsAsync(t, [Row(60, "g", 0)], ["event_date"], s));
            }
            yield return new Operation("delete-where/position", v, t => TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 4 == 0, DeleteStrategy.PositionDeletes));
            yield return new Operation("drop-partition", v, t => SnapshotProducer.DeleteFiles(t, f => (int?)f.Partition[0] == 19_702).CommitAsync());
            yield return new Operation("schema-change", v, t => t.UpdateSchemaAsync(new Schema(0, [.. Schema.Columns, NestedField.Optional(4, "extra", PrimitiveType.Int)], [1, 3])));
            yield return new Operation("ndv-statistics", v, t => PuffinStatistics.ComputeNdvAsync(t));

            // Maintenance and the other write paths (gap review D1).
            yield return new Operation("expire", v, (_, t, ct) => Maintenance.ExpireAsync(t, new ExpireOptions { OlderThanMs = long.MaxValue, RetainLast = 2 }, ct), Idempotent: true);
            yield return new Operation("orphan-cleanup", v, (_, t, ct) => Maintenance.RemoveOrphanFilesAsync(t, new OrphanFilesOptions { TimeProvider = Later }, ct),
                Atomic: false, Idempotent: true, ObserveFiles: true);
            yield return new Operation("compact", v, (_, t, ct) => Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 2), ct: ct), Idempotent: true);
            yield return new Operation("compact-partial", v, (_, t, ct) => Compaction.RewriteDataFilesAsync(t,
                new RewriteOptions(MinInputFiles: 2, PartialProgressEnabled: true, PartialProgressMaxCommits: 3), ct: ct), Atomic: false, Idempotent: true);
            // v3's deletes are already one deletion vector per data file: nothing to rewrite.
            if (v == 2) yield return new Operation("rewrite-position-deletes", v, (_, t, ct) => Compaction.RewritePositionDeletesAsync(t, ct), Idempotent: true);
            yield return new Operation("rewrite-manifests", v, (_, t, ct) => Maintenance.RewriteManifestsAsync(t, ct: ct));
            yield return new Operation("merge-append", v, (_, t, ct) => TableMutations.InsertAsync(t, [Row(101, "m", 1)], ct: ct), MergeManifests: true);
            yield return new Operation("branch-append", v, async (_, t, ct) =>
                await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, [Row(102, "b", 2)], ct)).ToBranch(Branch).CommitAsync(ct));
            foreach (var s in new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes })
            {
                yield return new Operation($"update/{s}", v, (_, t, ct) => TableMutations.UpdateAsync(t, Expr.Equal("id", 4L), u => u.Set("name", "upd"), s, ct));
                yield return new Operation($"merge/{s}", v, (_, t, ct) => TableMutations.MergeAsync(t, [Row(1, "m", 1), Row(70, "m", 0)], ["id", "event_date"],
                    m => m.WhenMatchedUpdate((_, source) => source).WhenNotMatchedInsert(), s, ct));
            }
            yield return new Operation("staged-transaction", v, async (catalog, t, ct) =>
            {
                // Two tables, one atomic catalog transaction (StagedCommit.CommitAllAsync).
                var staged = new List<StagedCommit>();
                try
                {
                    staged.Add(await TableMutations.StageMergeAsync(t, [Row(1, "x", 1), Row(80, "x", 2)], ["id", "event_date"],
                        m => m.WhenMatchedDelete().WhenNotMatchedInsert(), DeleteStrategy.PositionDeletes, ct));
                    staged.Add(await TableMutations.StageUpdateAsync(await catalog.LoadTableAsync(U, ct), Expr.Equal("id", 1L), u => u.Set("name", "tx"), DeleteStrategy.CopyOnWrite, ct));
                    await StagedCommit.CommitAllAsync(staged, ct);
                }
                catch (Exception e) when (e is not CommitStateUnknownException)
                {
                    foreach (var s in staged) await s.DiscardAsync(CancellationToken.None); // a stage that failed before CommitAllAsync
                    throw;
                }
            });
        }
        yield return new Operation("convert-to-dvs", 2, async t =>
        {
            if (t.Metadata.FormatVersion == 2) await t.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(3)]);
            await SnapshotProducer.ConvertPositionDeletesToDeletionVectors(t).CommitAsync();
        });
    }

    public static TheoryData<Operation> Operations()
    {
        var data = new TheoryData<Operation>();
        foreach (var op in AllOperations()) data.Add(op);
        return data;
    }

    public static TheoryData<Operation, FaultKind> OperationsAnd(params FaultKind[] kinds)
    {
        var data = new TheoryData<Operation, FaultKind>();
        foreach (var op in AllOperations())
            foreach (var kind in kinds)
                data.Add(op, kind);
        return data;
    }

    private static string Canon(IEnumerable<object?[]> rows) =>
        string.Join(" ", rows.Select(r => string.Join(",", r.Take(3).Select(v => v?.ToString() ?? "null"))).Order(StringComparer.Ordinal));

    private static string Local(string location) => location.StartsWith('/') ? location : new Uri(location).LocalPath;

    /// <summary>
    /// A fresh table <c>t</c>: 12 rows over 3 partitions in two appends (two files per partition, for compaction), two
    /// position deletes against one data file, a copy-on-write upsert (so expiry frees files), a branch at the head and an
    /// orphan file. And a second table <c>u</c> for two-table transactions.
    /// </summary>
    private static async Task<(InMemoryCatalog Catalog, FaultInjectingFileIO IO, string Dir)> Fixture(Operation op, string? dir = null)
    {
        dir ??= Path.Combine(Path.GetTempPath(), "iceberg-dotnet-faults", Guid.NewGuid().ToString("N"));
        var io = new FaultInjectingFileIO(new LocalFileIO());
        var catalog = new InMemoryCatalog(dir, io);
        await catalog.CreateNamespaceAsync(Namespace.Of("f"));
        var properties = op.MergeManifests ? new Dictionary<string, string> { [SnapshotProducer.ManifestMinCountToMerge] = "2" } : null;
        var t = await catalog.CreateTableAsync(new CreateTableRequest(T, Schema,
            PartitionSpec.BuilderFor(Schema).Identity("event_date").Build(), FormatVersion: op.FormatVersion, Properties: properties));
        await TableMutations.InsertAsync(t, Enumerable.Range(0, 6).Select(i => Row(i, $"v{i}", i % 3)).ToList());
        await TableMutations.InsertAsync(t, Enumerable.Range(6, 6).Select(i => Row(i, $"v{i}", i % 3)).ToList());
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], [[2L, 19_702]], DeleteStrategy.PositionDeletes);
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], [[5L, 19_702]], DeleteStrategy.PositionDeletes);
        await TableMutations.UpsertAsync(t, [Row(11, "w", 2)], strategy: DeleteStrategy.CopyOnWrite);
        await t.ManageSnapshots().CreateBranch(Branch).CommitAsync();
        await File.WriteAllBytesAsync(Path.Combine(Local(t.Location), "data", "orphan.parquet"), [1, 2, 3]);
        var u = await catalog.CreateTableAsync(new CreateTableRequest(U, Schema, PartitionSpec.Unpartitioned, FormatVersion: op.FormatVersion));
        await TableMutations.InsertAsync(u, [Row(1, "u1", 0), Row(2, "u2", 0)]);
        return (catalog, io, dir);
    }

    private sealed record Template(string Dir, string Copy, string TLocation, string ULocation);

    private static readonly ConcurrentDictionary<string, Task<Template>> Templates = new();

    /// <summary>
    /// The fixture of <paramref name="op"/>, built once per lane and restored by copying its files back (metadata names
    /// absolute paths, so it is restored to the same directory) and registering both tables in a fresh catalog. A lane
    /// (one operation and fault kind) runs its fault points one at a time and drops its template when done.
    /// </summary>
    private static async Task<(InMemoryCatalog Catalog, FaultInjectingFileIO IO, string Dir)> Restore(Operation op, string lane)
    {
        var template = await Templates.GetOrAdd($"{op}/{lane}", _ => Task.Run(async () =>
        {
            var dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-faults", Guid.NewGuid().ToString("N"));
            var (catalog, _, _) = await Fixture(op, dir);
            var (t, u) = ((await catalog.LoadTableAsync(T)).MetadataLocation!, (await catalog.LoadTableAsync(U)).MetadataLocation!);
            catalog.Dispose();
            var copy = dir + "-template";
            CopyDirectory(dir, copy);
            return new Template(dir, copy, t, u);
        }));
        if (Directory.Exists(template.Dir)) Directory.Delete(template.Dir, true);
        CopyDirectory(template.Copy, template.Dir);
        var io = new FaultInjectingFileIO(new LocalFileIO());
        var restored = new InMemoryCatalog(template.Dir, io);
        await restored.CreateNamespaceAsync(Namespace.Of("f"));
        await restored.RegisterTableAsync(T, template.TLocation);
        await restored.RegisterTableAsync(U, template.ULocation);
        return (restored, io, template.Dir);
    }

    private static void CopyDirectory(string from, string to)
    {
        foreach (var d in Directory.GetDirectories(from, "*", SearchOption.AllDirectories)) Directory.CreateDirectory(to + d[from.Length..]);
        Directory.CreateDirectory(to);
        foreach (var f in Directory.GetFiles(from, "*", SearchOption.AllDirectories)) File.Copy(f, to + f[from.Length..]);
    }

    /// <summary>What the operation may change: rows, snapshots, schema and statistics of t, the branch, and u.</summary>
    private static async Task<string> Observe(InMemoryCatalog catalog, Operation op, bool rowsOnly = false)
    {
        var t = await catalog.LoadTableAsync(T);
        var rows = Canon(await TableScan.ReadAllAsync(t));
        if (rowsOnly) return rows;
        var m = t.Metadata;
        var state = $"{rows} | snapshots={m.Snapshots.Count} schema={m.CurrentSchemaId} stats={m.Statistics.Count}";
        if (m.Refs.TryGetValue(Branch, out var b)) state += $" | {Branch}: {Canon(await TableScan.ReadAllAsync(t, b.SnapshotId))}";
        state += $" | u: {Canon(await TableScan.ReadAllAsync(await catalog.LoadTableAsync(U)))}";
        if (op.ObserveFiles) state += $" | files={Directory.GetFiles(Local(t.Location), "*", SearchOption.AllDirectories).Length}";
        return state;
    }

    private sealed record CleanRun(string Before, string After, string RowsBefore, int Writes, int Reads, int Deletes);

    private static readonly ConcurrentDictionary<string, Task<CleanRun>> CleanRuns = new();

    /// <summary>The fault-free run: the states before and after, and how many writes, reads and deletes it performs.</summary>
    private static Task<CleanRun> Clean(Operation op) => CleanRuns.GetOrAdd(op.ToString(), _ => Task.Run(async () =>
    {
        var (catalog, io, dir) = await Fixture(op);
        try
        {
            var before = await Observe(catalog, op);
            var rowsBefore = await Observe(catalog, op, rowsOnly: true);
            var t = await catalog.LoadTableAsync(T);
            io.Reset();
            await op.Run(catalog, t, CancellationToken.None);
            var (writes, reads, deletes) = (io.Writes, io.Reads, io.Deletes);
            io.Disarm();
            return new CleanRun(before, await Observe(catalog, op), rowsBefore, writes, reads, deletes);
        }
        finally
        {
            catalog.Dispose();
            Directory.Delete(dir, true);
        }
    }));

    private static int MaxPoints => int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_FAULT_POINTS"), out var n) ? n : 8;

    /// <summary>Every point, or a seeded sample (always the first and last) when there are more than <paramref name="max"/>.</summary>
    private static IEnumerable<int> Points(Operation op, FaultKind kind, int count, int max)
    {
        if (max <= 0 || count <= max) return Enumerable.Range(1, count);
        var seed = $"{op}/{kind}".Aggregate(17, (h, c) => unchecked(h * 31 + c)); // stable across runs, unlike string.GetHashCode
        var rnd = new Random(seed);
        var picked = new SortedSet<int> { 1, count };
        while (picked.Count < max) picked.Add(rnd.Next(1, count + 1));
        return picked;
    }

    private static async Task<List<string>> FaultEveryPoint(Operation op, FaultKind kind)
    {
        var lane = kind.ToString();
        var clean = await Clean(op);
        var count = kind switch
        {
            FaultKind.FailBeforeWrite or FaultKind.FailAfterWrite or FaultKind.TornWrite => clean.Writes,
            FaultKind.FailRead or FaultKind.SlowRead => clean.Reads,
            _ => clean.Deletes,
        };
        // Write faults before and after landing, and delete faults, are exhaustive; the rest are sampled.
        var points = kind switch
        {
            FaultKind.FailRead => Points(op, kind, count, MaxPoints),
            FaultKind.SlowRead or FaultKind.TornWrite => Points(op, kind, count, MaxPoints == 0 ? 0 : Math.Max(2, MaxPoints / 2)),
            _ => Enumerable.Range(1, count),
        };
        var outcomes = new List<string>();
        try
        {
            foreach (var k in points) outcomes.Add(await FaultAt(op, kind, k, count, clean, lane));
        }
        finally
        {
            if (Templates.TryRemove($"{op}/{lane}", out var template))
            {
                var t = await template;
                foreach (var d in new[] { t.Dir, t.Copy }) if (Directory.Exists(d)) Directory.Delete(d, true);
            }
        }
        return outcomes;
    }

    /// <summary>One fault at point <paramref name="k"/>, then the four invariants. Returns "rolled-back", "committed" or "partial".</summary>
    private static async Task<string> FaultAt(Operation op, FaultKind kind, int k, int count, CleanRun clean, string lane)
    {
        var (catalog, io, _) = await Restore(op, lane);
        var where = $"{op} with {kind} at {k}/{count}";
        try
        {
            var t = await catalog.LoadTableAsync(T);
            using var cts = new CancellationTokenSource();
            io.Reset(k, kind);
            io.SlowReadDelay = TimeSpan.FromMilliseconds(300); // bounded, in case a layer drops the token
            io.OnFault = (_, _) => { if (kind == FaultKind.SlowRead) cts.Cancel(); };
            Exception? error = null;
            try { await op.Run(catalog, t, cts.Token); }
            catch (Exception e) when (e is IOException or CommitStateUnknownException or OperationCanceledException) { error = e; }
            io.Disarm();
            io.OnFault = null;
            var deleted = io.Deleted.ToList();
            if (Environment.GetEnvironmentVariable("ICEBERG_FAULT_TRACE") is { Length: > 0 } trace)
                foreach (var u in io.UncancellableReads) File.AppendAllText(trace, $"== {where}\n{u}\n");

            // 1. Before or after; a definite error means before.
            var state = await Observe(catalog, op);
            string outcome;
            if (op.Atomic)
            {
                Assert.True(state == clean.Before || state == clean.After,
                    $"{where} left a third state:\n  before: {clean.Before}\n  after:  {clean.After}\n  actual: {state}\n  error: {error}");
                var definite = error is not null and not CommitStateUnknownException and not OperationCanceledException;
                Assert.False(definite && state != clean.Before && clean.Before != clean.After,
                    $"{where} took effect but reported a definite failure (a caller would retry it):\n  {error}");
                outcome = state == clean.Before && clean.Before != clean.After ? "rolled-back" : "committed";
            }
            else
            {
                var rows = await Observe(catalog, op, rowsOnly: true);
                Assert.True(rows == clean.RowsBefore, $"{where} changed what the table reads:\n  before: {clean.RowsBefore}\n  actual: {rows}\n  error: {error}");
                outcome = state == clean.After ? "committed" : state == clean.Before ? "rolled-back" : "partial";
            }

            // 2. Deep fsck of both tables, every snapshot.
            foreach (var id in new[] { T, U })
            {
                var report = await TableValidator.ValidateAsync(await catalog.LoadTableAsync(id), new ValidationOptions { Deep = true, AllSnapshots = true });
                Assert.True(report.IsValid, $"{where}: {id}: {report}\n  error: {error}");
            }

            // 3. Nothing the tables reference (any snapshot, any metadata file) was deleted.
            foreach (var id in new[] { T, U })
            {
                var m = (await catalog.LoadTableAsync(id)).Metadata;
                var referenced = await TableFiles.ReachableAsync(m, io, m.Snapshots);
                referenced.UnionWith(m.MetadataLog.Select(l => l.MetadataFile));
                referenced.UnionWith(m.Statistics.Select(s => s.StatisticsPath));
                if (m.MetadataFileLocation is { } current) referenced.Add(current);
                var lost = deleted.Where(referenced.Contains).ToList();
                Assert.True(lost.Count == 0, $"{where} deleted files {id} references: {string.Join(", ", lost)}\n  error: {error}");
            }

            // 4. Retry converges on the fault-free result.
            if (op.Idempotent || state == clean.Before)
            {
                t = await catalog.LoadTableAsync(T);
                await op.Run(catalog, t, CancellationToken.None);
            }
            var final = await Observe(catalog, op);
            Assert.True(final == clean.After, $"{where}, then a retry, did not converge:\n  expected: {clean.After}\n  actual:   {final}\n  first error: {error}");
            await TableValidator.AssertValidAsync(await catalog.LoadTableAsync(T), new ValidationOptions { Deep = true, AllSnapshots = true });
            return outcome;
        }
        finally
        {
            catalog.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Every_write_fault_leaves_the_table_before_or_after(Operation op)
    {
        var clean = await Clean(op);
        Assert.True(clean.Before != clean.After, $"{op} changes nothing observable:\n  {clean.Before}");
        if (clean.Writes == 0 && op.ObserveFiles) return; // orphan cleanup: deletes only
        Assert.True(clean.Writes > 0, $"{op} writes nothing");
        var outcomes = new List<string>();
        foreach (var kind in new[] { FaultKind.FailBeforeWrite, FaultKind.FailAfterWrite })
            outcomes.AddRange(await FaultEveryPoint(op, kind));
        // Faults before the final pointer swap must roll back; at least one fault point must exist that does.
        if (op.Atomic) Assert.Contains("rolled-back", outcomes);
    }

    // One class per fault class, so xUnit runs them in parallel.

    public sealed class TornWrites
    {
        public static TheoryData<Operation, FaultKind> Cases() => OperationsAnd(FaultKind.TornWrite);

        [Theory]
        [MemberData(nameof(Cases))]
        public async Task A_torn_write_keeps_the_invariants(Operation op, FaultKind kind) => await FaultEveryPoint(op, kind);
    }

    public sealed class ReadFaults
    {
        public static TheoryData<Operation, FaultKind> Cases() => OperationsAnd(FaultKind.FailRead);

        [Theory]
        [MemberData(nameof(Cases))]
        public async Task A_failed_read_keeps_the_invariants(Operation op, FaultKind kind) => await FaultEveryPoint(op, kind);
    }

    public sealed class SlowReads
    {
        public static TheoryData<Operation, FaultKind> Cases() => OperationsAnd(FaultKind.SlowRead);

        /// <summary>The read stalls and the caller cancels the operation right there.</summary>
        [Theory]
        [MemberData(nameof(Cases))]
        public async Task Cancelling_at_a_stalled_read_keeps_the_invariants(Operation op, FaultKind kind) => await FaultEveryPoint(op, kind);
    }

    public sealed class DeleteFaults
    {
        public static TheoryData<Operation, FaultKind> Cases() => OperationsAnd(FaultKind.FailDelete);

        [Theory]
        [MemberData(nameof(Cases))]
        public async Task A_failed_delete_keeps_the_invariants(Operation op, FaultKind kind) => await FaultEveryPoint(op, kind);
    }

    [Fact]
    public async Task Ambiguous_catalog_swap_never_deletes_the_new_metadata()
    {
        // The catalog applied the swap but the caller saw an error: the table must be the committed state.
        var op = AllOperations().First();
        var (catalog, io, dir) = await Fixture(op);
        try
        {
            var t = await catalog.LoadTableAsync(T);
            io.Reset();
            io.PathFilter = p => p.EndsWith(".metadata.json", StringComparison.Ordinal);
            io.Reset(1, FaultKind.FailAfterWrite);
            await Assert.ThrowsAnyAsync<Exception>(() => TableMutations.InsertAsync(t, [Row(500, "x", 0)]));
            var reloaded = await catalog.LoadTableAsync(t.Identifier);
            await TableValidator.AssertValidAsync(reloaded, new ValidationOptions { Deep = true });
        }
        finally
        {
            catalog.Dispose();
            Directory.Delete(dir, true);
        }
    }
}
