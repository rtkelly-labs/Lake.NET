using System.Data.Common;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// Faults in the catalog database (gap D4), injected through EF Core interceptors (<see cref="CatalogFaults"/>), over
/// operations × fault points. The invariants: a commit lands once or not at all; a known failure removes the files it
/// wrote, an unknown outcome keeps them; transient failures (SQLite <c>BUSY</c>, PostgreSQL <c>40001</c>) are retried;
/// a lost acknowledgement is recognised by the snapshot's idempotency token instead of being applied twice; and the
/// lake validates afterwards.
/// </summary>
public abstract class CatalogFaultTests : IAsyncLifetime
{
    private readonly TestLake _lake;
    private readonly CatalogFaults _faults = new();

    protected CatalogFaultTests(CatalogBackend backend, string version)
    {
        _lake = new TestLake(backend, version);
        _lake.ConfigureStore = o => o.AddInterceptors(_faults);
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        _faults.Disarm();
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    public enum Outcome
    {
        Success,
        Error,
        Unknown,
    }

    private static readonly DuckLakeOptions Fast = new() { RetryWait = TimeSpan.FromMilliseconds(1), MaxRetryWait = TimeSpan.FromMilliseconds(5) };

    /// <summary>A table with five rows in a file and two inlined rows, over several snapshots.</summary>
    private async Task<DuckLakeCatalog> LakeAsync()
    {
        var catalog = await _lake.CreateAsync(Fast);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("v", DuckLakeType.Varchar)]);
            await tx.CommitAsync();
        }
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", Enumerable.Range(0, 5).Select(i => new object?[] { (long)i, "v" }));
            await tx.CommitAsync();
        }
        var inlining = await _lake.OpenAsync(Fast with { DataInliningRowLimit = 10 });
        await using (var tx = inlining.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[10L, "i"], [11L, "i"]]);
            await tx.CommitAsync();
        }
        return catalog;
    }

    private static readonly string[] Before = ["0:v", "10:i", "11:i", "1:v", "2:v", "3:v", "4:v"];

    /// <summary>The operation (staged, then committed with the faults armed by <c>arm</c>) and the rows it leaves.</summary>
    private static (Func<DuckLakeCatalog, Action, Task> Run, string[] After) Operation(string name) => name switch
    {
        "insert" => (async (c, arm) => await Commit(c, arm, tx => tx.InsertAsync("main", "t", [[20L, "n"], [21L, "n"], [22L, "n"]])),
            ["0:v", "10:i", "11:i", "1:v", "20:n", "21:n", "22:n", "2:v", "3:v", "4:v"]),
        "delete" => (async (c, arm) => await Commit(c, arm, tx => tx.DeleteAsync("main", "t", Expr.Equal("id", 1L))),
            ["0:v", "10:i", "11:i", "2:v", "3:v", "4:v"]),
        "update" => (async (c, arm) => await Commit(c, arm, tx => tx.UpdateAsync("main", "t", Expr.Equal("id", 2L), r => [r[0], "u"])),
            ["0:v", "10:i", "11:i", "1:v", "2:u", "3:v", "4:v"]),
        "flush" => (async (c, arm) => await Commit(c, arm, tx => tx.FlushInlinedDataAsync("main", "t", default)), Before),
        "expire" => (async (c, arm) =>
        {
            arm();
            await c.ExpireSnapshotsAsync(DateTimeOffset.UtcNow.AddMinutes(1));
        }, Before),
        _ => throw new ArgumentException(name),
    };

    private static async Task Commit(DuckLakeCatalog catalog, Action arm, Func<DuckLakeTransaction, Task> stage)
    {
        await using var tx = catalog.BeginTransaction(author: "fault test");
        await stage(tx);
        arm();
        await tx.CommitAsync();
    }

    public static TheoryData<string, string> Cases()
    {
        var data = new TheoryData<string, string>();
        foreach (var op in new[] { "insert", "delete", "update", "flush", "expire" })
            foreach (var fault in Faults.Keys)
                data.Add(op, fault);
        return data;
    }

    /// <summary>Each fault: where it is thrown, what is thrown, and the outcome when it fires.</summary>
    private static readonly Dictionary<string, (FaultPoint Point, int Command, Func<Exception> Error, Outcome Outcome)> Faults = new()
    {
        ["error-before-command-1"] = (FaultPoint.BeforeCommand, 1, CatalogFaults.Generic, Outcome.Error),
        ["error-before-command-3"] = (FaultPoint.BeforeCommand, 3, CatalogFaults.Generic, Outcome.Error),
        ["error-after-command-2"] = (FaultPoint.AfterCommand, 2, CatalogFaults.Generic, Outcome.Error),
        ["error-before-commit"] = (FaultPoint.BeforeCommit, 0, CatalogFaults.Generic, Outcome.Unknown),
        ["lost-ack"] = (FaultPoint.AfterCommit, 0, CatalogFaults.Generic, Outcome.Success),
        ["busy-before-command-1"] = (FaultPoint.BeforeCommand, 1, CatalogFaults.SqliteBusy, Outcome.Success),
        ["busy-after-command-3"] = (FaultPoint.AfterCommand, 3, CatalogFaults.SqliteBusy, Outcome.Success),
        ["busy-before-commit"] = (FaultPoint.BeforeCommit, 0, CatalogFaults.SqliteBusy, Outcome.Success),
        ["40001-before-command-2"] = (FaultPoint.BeforeCommand, 2, CatalogFaults.SerializationFailure, Outcome.Success),
        ["40001-before-commit"] = (FaultPoint.BeforeCommit, 0, CatalogFaults.SerializationFailure, Outcome.Success),
    };

    [SkippableTheory]
    [MemberData(nameof(Cases))]
    [Spec("ducklake: Snapshot Creation")]
    [Spec("ducklake: Conflict Resolution")]
    public async Task A_catalog_fault_lands_the_commit_once_or_not_at_all(string operation, string fault)
    {
        var catalog = await LakeAsync();
        var (run, after) = Operation(operation);
        var (point, command, error, outcome) = Faults[fault];
        var snapshots = (await catalog.ListSnapshotsAsync()).Count;
        var files = _lake.Files().ToHashSet();

        Exception? thrown = null;
        try
        {
            await run(catalog, () => _faults.Arm(point, command, error));
        }
        catch (Exception e)
        {
            thrown = e;
        }
        var fired = _faults.Fired;
        _faults.Disarm();
        Assert.True(fired > 0 || command > 1, "the fault never fired");
        if (fired == 0) outcome = Outcome.Success; // the operation ran fewer commands than the fault waits for

        var rows = (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Select(r => $"{r[0]}:{r[1]}").Order(StringComparer.Ordinal).ToArray();
        var nowSnapshots = (await catalog.ListSnapshotsAsync()).Count;
        var landed = operation == "expire" ? nowSnapshots == 1 : nowSnapshots == snapshots + 1;
        Assert.True(landed || nowSnapshots == snapshots, $"{nowSnapshots} snapshots after starting with {snapshots}");
        var newFiles = _lake.Files().Except(files).ToList();
        switch (outcome)
        {
            case Outcome.Success:
                Assert.Null(thrown);
                Assert.True(landed, "the commit did not land");
                Assert.Equal(after, rows); // once: a retry after a lost ack did not apply it again
                break;
            case Outcome.Error:
                Assert.NotNull(thrown);
                Assert.IsNotType<DuckLakeCommitStateUnknownException>(thrown);
                Assert.True(thrown is DbException || thrown.InnerException is DbException, thrown.ToString());
                Assert.False(landed);
                Assert.Equal(Before, rows);
                Assert.Empty(newFiles); // known not committed: the files it wrote are gone
                break;
            case Outcome.Unknown:
                Assert.IsType<DuckLakeCommitStateUnknownException>(thrown);
                Assert.False(landed); // the fault was before COMMIT; the caller cannot know that
                Assert.Equal(Before, rows);
                if (operation != "expire") Assert.NotEmpty(newFiles); // unknown: kept, in case it landed
                Assert.All(files, f => Assert.True(File.Exists(f)));
                break;
        }
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    public async Task A_commit_gives_up_after_MaxCommitAttempts_and_removes_its_files()
    {
        await LakeAsync();
        var catalog = await _lake.OpenAsync(Fast with { MaxCommitAttempts = 3 });
        var snapshots = (await catalog.ListSnapshotsAsync()).Count;
        var files = _lake.Files().ToHashSet();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[20L, "n"]]);
            Assert.NotEmpty(_lake.Files().Except(files));
            _faults.Arm(FaultPoint.BeforeCommand, 1, CatalogFaults.SqliteBusy, times: int.MaxValue); // busy for good
            var e = await Assert.ThrowsAsync<DuckLakeConflictException>(() => tx.CommitAsync());
            Assert.Contains("3 attempts", e.Message, StringComparison.Ordinal);
            Assert.IsType<Microsoft.Data.Sqlite.SqliteException>(e.InnerException);
            Assert.Equal(3, _faults.Fired); // one per attempt, then no more
            _faults.Disarm();
        }
        Assert.Equal(snapshots, (await catalog.ListSnapshotsAsync()).Count);
        Assert.Equal(files, _lake.Files().ToHashSet()); // nothing landed: its files are gone
        Assert.Equal(Before, (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).Select(r => $"{r[0]}:{r[1]}").Order(StringComparer.Ordinal));
    }

    [SkippableFact]
    [Spec("ducklake: Conflict Resolution")]
    public async Task A_transient_failure_on_every_attempt_but_the_last_still_commits_once()
    {
        await LakeAsync();
        var catalog = await _lake.OpenAsync(Fast with { MaxCommitAttempts = 4 });
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.InsertAsync("main", "t", [[20L, "n"]]);
            _faults.Arm(FaultPoint.BeforeCommit, error: CatalogFaults.SqliteBusy, times: 3);
            await tx.CommitAsync();
            Assert.Equal(3, _faults.Fired);
            Assert.Equal(4, _faults.Commits);
            _faults.Disarm();
        }
        Assert.Single(await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t")), r => (long)r[0]! == 20);
    }
}

public sealed class SqliteCatalogFaultTests10() : CatalogFaultTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteCatalogFaultTests11() : CatalogFaultTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresCatalogFaultTests11() : CatalogFaultTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
