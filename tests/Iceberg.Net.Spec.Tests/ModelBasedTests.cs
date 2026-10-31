using System.Globalization;
using FsCheck;
using FsCheck.Fluent;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Model-based property test: random sequences of writes (every mode × every delete strategy × v2/v3) are applied
/// to a real table and to a trivial in-memory model (a list of rows). After every step the table must read
/// exactly the model's rows; at the end every surviving snapshot must time-travel to the model's history and the
/// table must pass a deep fsck. Failures shrink to a minimal operation sequence and print a replay seed:
///   ICEBERG_PBT_SEED="seed,gamma" ICEBERG_PBT_RUNS=n dotnet test --filter ModelBased
/// </summary>
[Spec("Row-level Deletes")]
[Spec("Sequence Numbers")]
[Spec("Row Lineage")]
[Spec("Snapshot Retention Policy")]
public sealed class ModelBasedTests
{
    // Small key space (id 0..7 × 3 days) so operations collide often.
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "event_date", PrimitiveType.Date),
        NestedField.Optional(4, "score", PrimitiveType.Double),
    ], identifierFieldIds: [1, 3]);

    public sealed record Row(long Id, string? Name, int Day, double? Score)
    {
        public object?[] ToArray() => [Id, Name, 19_700 + Day, Score];
        public static Row From(object?[] r) => new((long)r[0]!, (string?)r[1], (int)r[2]! - 19_700, (double?)r[3]);
        public override string ToString() => $"({Id},{Name ?? "null"},d{Day},{Score?.ToString(CultureInfo.InvariantCulture) ?? "null"})";
    }

    public abstract record Op;
    public sealed record Insert(Row[] Rows) : Op { public override string ToString() => $"Insert[{string.Join(" ", (IEnumerable<Row>)Rows)}]"; }
    public sealed record Upsert(Row[] Rows, DeleteStrategy S) : Op { public override string ToString() => $"Upsert/{S}[{string.Join(" ", (IEnumerable<Row>)Rows)}]"; }
    public sealed record ReplaceDays(Row[] Rows, DeleteStrategy S) : Op { public override string ToString() => $"ReplaceDays/{S}[{string.Join(" ", (IEnumerable<Row>)Rows)}]"; }
    public sealed record DeleteKeys((long Id, int Day)[] Keys, DeleteStrategy S) : Op { public override string ToString() => $"DeleteKeys/{S}[{string.Join(" ", Keys)}]"; }
    public sealed record DeleteWhereIdMod(int Mod, int Rem, DeleteStrategy S) : Op;
    public sealed record DropDay(int Day) : Op;
    public sealed record Expire(int RetainLast) : Op;
    public sealed record ComputeStats : Op;
    public sealed record UpgradeAndConvert : Op;
    /// <summary>Compaction (bin-pack; <c>All</c>: rewrite every file): the rows must not change.</summary>
    public sealed record Compact(bool All) : Op;
    public sealed record RewriteManifests : Op;
    /// <summary>Partition evolution: add a bucket on id, drop the day field, or bring it back (a no-op when not applicable).</summary>
    public sealed record EvolveSpec(SpecChange Change) : Op;
    /// <summary>Add an optional column; existing rows read it as null, and later writes leave it null.</summary>
    public sealed record AddColumn : Op;

    public enum SpecChange { AddIdBucket, DropDay, RestoreDay }

    public sealed record Case(int FormatVersion, Op[] Ops)
    {
        public override string ToString() => $"v{FormatVersion}: " + string.Join(" ; ", (IEnumerable<Op>)Ops);
    }

    private static readonly Gen<DeleteStrategy> AnyStrategy = Gen.Elements(DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes, DeleteStrategy.EqualityDeletes);
    private static readonly Gen<DeleteStrategy> ScanStrategy = Gen.Elements(DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes);

    private static readonly Gen<Row> RowGen =
        from id in Gen.Choose(0, 7)
        from day in Gen.Choose(0, 2)
        from name in Gen.Elements<string?>("a", "b", "c", null)
        from score in Gen.Elements<double?>(1.5, -2.0, 0.0, double.NaN, null)
        select new Row(id, name, day, score);

    private static Gen<Row[]> Rows(int min, int max) => Gen.Choose(min, max).SelectMany(n => Gen.ArrayOf(RowGen, n));

    private static Gen<Row[]> UniqueKeyRows => Rows(1, 5).Select(rs => rs.DistinctBy(r => (r.Id, r.Day)).ToArray());

    private static readonly Gen<Op> OpGen = Gen.Frequency(
        (3, Rows(1, 6).Select(r => (Op)new Insert(r))),
        (4, UniqueKeyRows.SelectMany(r => AnyStrategy.Select(s => (Op)new Upsert(r, s)))),
        (2, Rows(1, 4).SelectMany(r => AnyStrategy.Select(s => (Op)new ReplaceDays(r, s)))),
        (2, Gen.ArrayOf(Gen.Choose(0, 7).SelectMany(id => Gen.Choose(0, 2).Select(d => ((long)id, d))), 2).SelectMany(k => AnyStrategy.Select(s => (Op)new DeleteKeys(k, s)))),
        (2, Gen.Choose(2, 4).SelectMany(m => Gen.Choose(0, m - 1).SelectMany(r => ScanStrategy.Select(s => (Op)new DeleteWhereIdMod(m, r, s))))),
        (1, Gen.Choose(0, 2).Select(d => (Op)new DropDay(d))),
        (1, Gen.Choose(1, 3).Select(n => (Op)new Expire(n))),
        (1, Gen.Constant((Op)new ComputeStats())),
        (1, Gen.Constant((Op)new UpgradeAndConvert())),
        (1, Gen.Elements(false, true).Select(all => (Op)new Compact(all))),
        (1, Gen.Constant((Op)new RewriteManifests())),
        (1, Gen.Elements(SpecChange.AddIdBucket, SpecChange.DropDay, SpecChange.RestoreDay).Select(c => (Op)new EvolveSpec(c))),
        (1, Gen.Constant((Op)new AddColumn())));

    private static readonly Gen<Case> CaseGen =
        from v in Gen.Elements(2, 3)
        from n in Gen.Choose(1, 8)
        from ops in Gen.ArrayOf(OpGen, n)
        select new Case(v, ops);

    /// <summary>Shrink by dropping operations, then by trimming rows inside an operation.</summary>
    private static IEnumerable<Case> Shrink(Case c)
    {
        for (var i = 0; i < c.Ops.Length; i++) yield return c with { Ops = c.Ops.Where((_, j) => j != i).ToArray() };
        for (var i = 0; i < c.Ops.Length; i++)
        {
            Op? smaller = c.Ops[i] switch
            {
                Insert { Rows.Length: > 1 } x => x with { Rows = x.Rows[1..] },
                Upsert { Rows.Length: > 1 } x => x with { Rows = x.Rows[1..] },
                ReplaceDays { Rows.Length: > 1 } x => x with { Rows = x.Rows[1..] },
                _ => null,
            };
            if (smaller is not null) yield return c with { Ops = c.Ops.Select((o, j) => j == i ? smaller : o).ToArray() };
        }
        if (c.FormatVersion == 3) yield return c with { FormatVersion = 2 };
    }

    [Fact]
    public void Table_always_matches_the_model()
    {
        var runs = int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_PBT_RUNS"), out var n) ? n : 40;
        var config = Config.QuickThrowOnFailure.WithMaxTest(runs);
        if (Environment.GetEnvironmentVariable("ICEBERG_PBT_SEED") is { Length: > 0 } seed)
        {
            var parts = seed.Split(',').Select(ulong.Parse).ToArray();
            config = config.WithReplay(parts[0], parts[1]);
        }
        Check.One(config, Prop.ForAll(Arb.From(CaseGen, Shrink), (Case c) => Run(c).GetAwaiter().GetResult()));
    }

    private static string Canon(IEnumerable<Row> rows) => string.Join(" ", rows.Select(r => r.ToString()).Order(StringComparer.Ordinal));

    private static async Task<bool> Run(Case c)
    {
        var warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-pbt", Guid.NewGuid().ToString("N"));
        using var catalog = new InMemoryCatalog(warehouse);
        try
        {
            await catalog.CreateNamespaceAsync(Namespace.Of("pbt"));
            var table = await catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("pbt", "t"), Schema,
                PartitionSpec.BuilderFor(Schema).Identity("event_date").Build(), FormatVersion: c.FormatVersion,
                // Merge manifests aggressively so random sequences exercise merge-append too.
                Properties: new Dictionary<string, string> { [SnapshotProducer.ManifestMinCountToMerge] = "3" }));
            var model = new List<Row>();
            var history = new Dictionary<long, string>();
            var step = 0;

            foreach (var op in c.Ops)
            {
                step++;
                await Apply(table, op, model);
                var actual = (await TableScan.ReadAllAsync(table)).Select(Row.From);
                if (Canon(actual) != Canon(model))
                    throw new InvalidOperationException($"step {step} ({op}): table [{Canon(actual)}] != model [{Canon(model)}]");
                var report = await TableValidator.ValidateAsync(table, new ValidationOptions { Deep = true });
                if (!report.IsValid) throw new InvalidOperationException($"step {step} ({op}): fsck failed: {report}");
                if (table.Metadata.CurrentSnapshotId is { } sid) history[sid] = Canon(model);
                if (table.Metadata.FormatVersion >= 3) await CheckLineage(table, createdV3: c.FormatVersion >= 3, step, op);
            }

            foreach (var (sid, expected) in history.Where(h => table.Metadata.SnapshotById(h.Key) is not null))
            {
                var past = Canon((await TableScan.ReadAllAsync(table, sid)).Select(Row.From));
                if (past != expected) throw new InvalidOperationException($"time travel to {sid}: [{past}] != [{expected}]");
            }
            await TableValidator.AssertValidAsync(table, new ValidationOptions { Deep = true, AllSnapshots = true });
            return true;
        }
        finally
        {
            if (Directory.Exists(warehouse)) Directory.Delete(warehouse, recursive: true);
        }
    }

    /// <summary>
    /// v3 row lineage invariants: live row IDs are unique and below <c>next-row-id</c>, and last-updated sequence numbers
    /// are committed ones. Rows written before an upgrade from v2 legitimately have no row ID until rewritten.
    /// </summary>
    private static async Task CheckLineage(Table table, bool createdV3, int step, Op op)
    {
        var rows = await TableScan.ReadWithLineageAsync(table);
        var ids = rows.Where(r => r.RowId is not null).Select(r => r.RowId!.Value).ToList();
        if (createdV3 && ids.Count != rows.Count) throw new InvalidOperationException($"step {step} ({op}): a row in a v3 table has no _row_id");
        if (ids.Distinct().Count() != ids.Count) throw new InvalidOperationException($"step {step} ({op}): duplicate _row_id among live rows");
        if (ids.Any(id => id < 0 || id >= table.Metadata.NextRowId)) throw new InvalidOperationException($"step {step} ({op}): _row_id outside [0, next-row-id)");
        if (rows.Any(r => r.LastUpdatedSequenceNumber is { } s && (s < 1 || s > table.Metadata.LastSequenceNumber)))
            throw new InvalidOperationException($"step {step} ({op}): _last_updated_sequence_number outside committed range");
    }

    /// <summary>A model row as a table row: columns added since (AddColumn) are null.</summary>
    private static object?[] Full(Table t, Row r)
    {
        var row = r.ToArray();
        return row.Length == t.Schema.Columns.Count ? row : [.. row, .. new object?[t.Schema.Columns.Count - row.Length]];
    }

    private static async Task Apply(Table t, Op op, List<Row> model)
    {
        switch (op)
        {
            case Insert x:
                await TableMutations.InsertAsync(t, x.Rows.Select(r => Full(t, r)).ToList());
                model.AddRange(x.Rows);
                break;
            case Upsert x:
                await TableMutations.UpsertAsync(t, x.Rows.Select(r => Full(t, r)).ToList(), strategy: x.S);
                model.RemoveAll(m => x.Rows.Any(r => (r.Id, r.Day) == (m.Id, m.Day)));
                model.AddRange(x.Rows);
                break;
            case ReplaceDays x:
                await TableMutations.ReplaceGroupsAsync(t, x.Rows.Select(r => Full(t, r)).ToList(), ["event_date"], x.S);
                model.RemoveAll(m => x.Rows.Any(r => r.Day == m.Day));
                model.AddRange(x.Rows);
                break;
            case DeleteKeys x:
                await TableMutations.DeleteByKeysAsync(t, ["id", "event_date"], x.Keys.Select(k => new object?[] { k.Id, 19_700 + k.Day }), x.S);
                model.RemoveAll(m => x.Keys.Contains((m.Id, m.Day)));
                break;
            case DeleteWhereIdMod x:
                await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % x.Mod == x.Rem, x.S);
                model.RemoveAll(m => m.Id % x.Mod == x.Rem);
                break;
            case DropDay x:
                if (t.Metadata.CurrentSnapshot is null) break;
                if (t.Metadata.PartitionSpecs.Count == 1)
                    await SnapshotProducer.DeleteFiles(t, f => (int?)f.Partition[0] == 19_700 + x.Day).CommitAsync();
                else // after partition evolution a file may hold several days: delete the rows instead
                    await TableMutations.DeleteWhereAsync(t, r => (int)r[2]! == 19_700 + x.Day, DeleteStrategy.CopyOnWrite);
                model.RemoveAll(m => m.Day == x.Day);
                break;
            case Expire x:
                await Maintenance.ExpireSnapshotsAsync(t, long.MaxValue, x.RetainLast);
                break;
            case ComputeStats:
                if (t.Metadata.CurrentSnapshot is not null) await PuffinStatistics.ComputeNdvAsync(t);
                break;
            case UpgradeAndConvert:
                if (t.Metadata.FormatVersion == 3) break;
                await t.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(3)]);
                if (t.Metadata.CurrentSnapshot is not null) await SnapshotProducer.ConvertPositionDeletesToDeletionVectors(t).CommitAsync();
                break;
            case Compact x:
                await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 2, RewriteAll: x.All));
                break;
            case RewriteManifests:
                if (t.Metadata.CurrentSnapshot is not null) await Maintenance.RewriteManifestsAsync(t);
                break;
            case EvolveSpec x:
            {
                var fields = t.Metadata.DefaultSpec.Fields.Select(f => f.Name).ToHashSet();
                switch (x.Change)
                {
                    case SpecChange.AddIdBucket when !fields.Contains("id_bucket"):
                        await t.UpdateSpecAsync(p => p.AddField("id", Transform.Bucket(2), "id_bucket"));
                        break;
                    case SpecChange.DropDay when fields.Contains("event_date"):
                        await t.UpdateSpecAsync(p => p.RemoveField("event_date"));
                        break;
                    case SpecChange.RestoreDay when !fields.Contains("event_date"):
                        await t.UpdateSpecAsync(p => p.AddField("event_date"));
                        break;
                }
                break;
            }
            case AddColumn:
                if (t.Schema.Columns.Count < 7) await t.UpdateSchemaAsync(s => s.AddColumn($"extra{t.Schema.Columns.Count}", PrimitiveType.Int));
                break;
        }
    }
}
