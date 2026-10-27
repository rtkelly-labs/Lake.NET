using System.Globalization;
using FsCheck;
using FsCheck.Fluent;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Model-based property test: random sequences of inserts, deletes, updates and partitioning changes are committed to
/// a real lake and applied to a trivial in-memory model (a list of rows). After every commit the lake must read exactly
/// the model's rows with unique row IDs, and a row a commit did not touch keeps its row ID (spec "Row Lineage"); at the
/// end every snapshot must time-travel to the model's history and the lake must pass a deep validation. Failures shrink
/// to a minimal operation sequence and print a replay seed:
///   ICEBERG_PBT_SEED="seed,gamma" ICEBERG_PBT_RUNS=n dotnet test --filter DuckLakeModelBased
/// </summary>
[Spec("ducklake: INSERT")]
[Spec("ducklake: DELETE")]
[Spec("ducklake: UPDATE")]
[Spec("ducklake: ducklake_snapshot")]
public abstract class DuckLakeModelBasedTests(CatalogBackend backend, int defaultRuns, bool duckDb = false)
{
    public sealed record Row(long Id, string? Name, int Day)
    {
        public object?[] ToArray() => [Id, Name, new DateOnly(2024, 1, 1).AddDays(Day)];
        public static Row From(object?[] r) => new((long)r[0]!, (string?)r[1], IcebergTime.DateFromDays((int)r[2]!).DayNumber - new DateOnly(2024, 1, 1).DayNumber);
        public override string ToString() => $"({Id},{Name ?? "null"},d{Day})";
    }

    public abstract record Op;
    public sealed record Insert(Row[] Rows) : Op { public override string ToString() => $"Insert[{string.Join(" ", (IEnumerable<Row>)Rows)}]"; }
    public sealed record DeleteIds(long[] Ids) : Op { public override string ToString() => $"Delete[{string.Join(",", Ids)}]"; }
    public sealed record DeleteDay(int Day) : Op;
    public sealed record Rename(long[] Ids, string Name) : Op { public override string ToString() => $"Update[{string.Join(",", Ids)}]={Name}"; }
    public sealed record PartitionByDay(bool On) : Op;

    public sealed record Case(string Version, Op[] Ops)
    {
        public override string ToString() => $"{Version}: " + string.Join(" ; ", (IEnumerable<Op>)Ops);
    }

    private static readonly Gen<Row> RowGen =
        from id in Gen.Choose(0, 9)
        from day in Gen.Choose(0, 2)
        from name in Gen.Elements<string?>("a", "b", null)
        select new Row(id, name, day);

    private static Gen<long[]> Ids => Gen.ArrayOf(Gen.Choose(0, 9).Select(i => (long)i), 2).Select(a => a.Distinct().ToArray());

    private static readonly Gen<Op> OpGen = Gen.Frequency(
        (4, Gen.Choose(1, 6).SelectMany(n => Gen.ArrayOf(RowGen, n)).Select(r => (Op)new Insert(r))),
        (2, Ids.Select(i => (Op)new DeleteIds(i))),
        (1, Gen.Choose(0, 2).Select(d => (Op)new DeleteDay(d))),
        (2, Ids.SelectMany(i => Gen.Elements("x", "y").Select(n => (Op)new Rename(i, n)))),
        (1, Gen.Elements(true, false).Select(on => (Op)new PartitionByDay(on))));

    private static readonly Gen<Case> CaseGen =
        from v in Gen.Elements(DuckLakeCatalog.FormatVersion10, DuckLakeCatalog.FormatVersion11)
        from n in Gen.Choose(1, 8)
        from ops in Gen.ArrayOf(OpGen, n)
        select new Case(v, ops);

    /// <summary>For the DuckDB variant: the version the available oracle writes.</summary>
    private Gen<Case> Cases => duckDb ? CaseGen.Select(c => c with { Version = DuckLakeCatalog.FormatVersion10 }) : CaseGen;

    private static IEnumerable<Case> Shrink(Case c)
    {
        for (var i = 0; i < c.Ops.Length; i++) yield return c with { Ops = c.Ops.Where((_, j) => j != i).ToArray() };
        for (var i = 0; i < c.Ops.Length; i++)
            if (c.Ops[i] is Insert { Rows.Length: > 1 } x)
                yield return c with { Ops = c.Ops.Select((o, j) => j == i ? x with { Rows = x.Rows[1..] } : o).ToArray() };
    }

    [SkippableFact]
    public void Lake_always_matches_the_model()
    {
        if (backend == CatalogBackend.Postgres) TestLake.RequirePostgres();
        if (duckDb) DuckLakeOracle.V10.Require();
        var runs = int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_PBT_RUNS"), out var n) ? n : defaultRuns;
        var config = Config.QuickThrowOnFailure.WithMaxTest(runs);
        if (Environment.GetEnvironmentVariable("ICEBERG_PBT_SEED") is { Length: > 0 } seed)
        {
            var parts = seed.Split(',').Select(ulong.Parse).ToArray();
            config = config.WithReplay(parts[0], parts[1]);
        }
        Check.One(config, Prop.ForAll(Arb.From(Cases, Shrink), (Case c) => Run(c).GetAwaiter().GetResult()));
    }

    private static string Canon(IEnumerable<Row> rows) => string.Join(" ", rows.Select(r => r.ToString()).Order(StringComparer.Ordinal));

    private async Task<bool> Run(Case c)
    {
        using var lake = new TestLake(backend, c.Version);
        var catalog = await lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("name", DuckLakeType.Varchar), new("day", DuckLakeType.Date)]);
            await tx.CommitAsync();
        }
        var model = new List<Row>();
        var history = new Dictionary<long, string>();
        var rowIds = new Dictionary<long, Row>(); // row ID -> row, as of the previous commit
        var step = 0;
        foreach (var op in c.Ops)
        {
            step++;
            var touched = await Apply(catalog, op, model);
            var table = await catalog.LoadTableAsync("main", "t");
            var rows = new List<DuckLakeRow>();
            await foreach (var r in catalog.ReadRowsAsync(table)) rows.Add(r);
            var actual = rows.Select(r => Row.From(r.Values)).ToList();
            if (Canon(actual) != Canon(model))
                throw new InvalidOperationException($"step {step} ({op}): lake [{Canon(actual)}] != model [{Canon(model)}]");
            if (rows.Select(r => r.RowId).Distinct().Count() != rows.Count) throw new InvalidOperationException($"step {step} ({op}): duplicate row IDs");
            // Lineage: a surviving row the operation did not touch keeps its row ID.
            foreach (var r in rows)
            {
                var row = Row.From(r.Values);
                if (rowIds.TryGetValue(r.RowId, out var before) && before != row && !touched(before))
                    throw new InvalidOperationException($"step {step} ({op}): row ID {r.RowId} moved from {before} to {row}");
            }
            rowIds = rows.ToDictionary(r => r.RowId, r => Row.From(r.Values));
            history[table.SnapshotId] = Canon(model);
        }
        foreach (var (snapshot, expected) in history)
        {
            var past = Canon((await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t", snapshot))).Select(Row.From));
            if (past != expected) throw new InvalidOperationException($"time travel to {snapshot}: [{past}] != [{expected}]");
        }
        await DuckLakeValidator.AssertValidAsync(catalog, new DuckLakeValidationOptions { Deep = true });
        if (duckDb)
        {
            // DuckDB reads every snapshot the same (one oracle job for all of them).
            var snapshots = history.Keys.Order().ToList();
            var results = await DuckLakeOracle.V10.RunAsync(lake, snapshots.Select(id => $"SELECT * FROM lake.t AT (VERSION => {id})").ToArray());
            for (var i = 0; i < snapshots.Count; i++)
            {
                var duck = Canon(results[i]["rows"]!.AsArray().Select(r => new Row(r![0]!.GetValue<long>(), r[1]?.GetValue<string>(),
                    DateOnly.Parse(r[2]!.GetValue<string>(), CultureInfo.InvariantCulture).DayNumber - new DateOnly(2024, 1, 1).DayNumber)));
                if (duck != history[snapshots[i]]) throw new InvalidOperationException($"DuckDB at snapshot {snapshots[i]}: [{duck}] != [{history[snapshots[i]]}]");
            }
        }
        return true;
    }

    /// <summary>Apply to the lake and the model; returns which model rows the operation may have changed.</summary>
    private static async Task<Func<Row, bool>> Apply(DuckLakeCatalog catalog, Op op, List<Row> model)
    {
        await using var tx = catalog.BeginTransaction();
        Func<Row, bool> touched = _ => false;
        var changed = true;
        switch (op)
        {
            case Insert x:
                await tx.InsertAsync("main", "t", x.Rows.Select(r => r.ToArray()));
                model.AddRange(x.Rows);
                break;
            case DeleteIds x:
                changed = await tx.DeleteAsync("main", "t", Expr.In("id", x.Ids.Cast<object>())) > 0;
                model.RemoveAll(r => x.Ids.Contains(r.Id));
                break;
            case DeleteDay x:
                changed = await tx.DeleteAsync("main", "t", Expr.Equal("day", IcebergTime.DaysFromDate(new DateOnly(2024, 1, 1).AddDays(x.Day)))) > 0;
                model.RemoveAll(r => r.Day == x.Day);
                break;
            case Rename x:
                changed = await tx.UpdateAsync("main", "t", Expr.In("id", x.Ids.Cast<object>()), r => [r[0], x.Name, r[2]]) > 0;
                for (var i = 0; i < model.Count; i++)
                    if (x.Ids.Contains(model[i].Id)) model[i] = model[i] with { Name = x.Name };
                touched = r => x.Ids.Contains(r.Id);
                break;
            case PartitionByDay x:
                await tx.SetPartitioningAsync("main", "t", x.On ? [("day", "identity")] : []);
                break;
        }
        if (changed) await tx.CommitAsync();
        return touched;
    }
}

public sealed class SqliteDuckLakeModelBasedTests() : DuckLakeModelBasedTests(CatalogBackend.Sqlite, 40);
public sealed class PostgresDuckLakeModelBasedTests() : DuckLakeModelBasedTests(CatalogBackend.Postgres, 15);
public sealed class DuckDbDuckLakeModelBasedTests() : DuckLakeModelBasedTests(CatalogBackend.Sqlite, 10, duckDb: true);
