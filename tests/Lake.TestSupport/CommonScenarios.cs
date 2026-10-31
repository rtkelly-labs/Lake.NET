using System.Globalization;
using Xunit;

namespace Lake.TestSupport;

/// <summary>
/// The behaviour every format must share (docs/lake/verification/bdd.md, "The common feature set"). A format's test
/// project derives one class per driver; a scenario whose capability the format lacks is skipped with the reason, never
/// silently. The format's validator runs after every scenario.
/// </summary>
public abstract class CommonScenarios : IAsyncLifetime
{
    protected abstract IFormatDriver CreateDriver();

    private IFormatDriver _driver = null!;

    public Task InitializeAsync()
    {
        _driver = CreateDriver();
        return Task.CompletedTask;
    }

    public Task DisposeAsync() => _driver.DisposeAsync().AsTask();

    public static List<object?[]> Rows(int start, int count, string suffix = "") => Enumerable.Range(start, count)
        .Select(i => new object?[] { (long)i, i % 5 == 0 ? null : $"n{i}{suffix}", 19_700 + i % 3, i * 0.5 }).ToList();

    private static List<string> Canonical(IEnumerable<object?[]> rows) => rows
        .Select(r => string.Join("|", r.Select(v => v switch
        {
            null => "∅",
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            _ => Convert.ToString(v, CultureInfo.InvariantCulture),
        })))
        .Order(StringComparer.Ordinal).ToList();

    private async Task<ITestTable> TableAsync(string name, Capabilities needs)
    {
        var missing = needs & ~_driver.Capabilities;
        Skip.If(missing != Capabilities.None, $"{_driver.Name} does not support {missing}");
        return await _driver.CreateTableAsync(name, TestSchema.Standard);
    }

    [SkippableFact]
    public async Task Inserted_rows_read_back()
    {
        var t = await TableAsync("insert", Capabilities.Write);
        await t.InsertAsync(Rows(0, 50));
        await t.InsertAsync(Rows(50, 25));
        Assert.Equal(Canonical(Rows(0, 75)), Canonical(await t.ReadAsync()));
        await t.ValidateAsync();
    }

    [SkippableFact]
    public async Task Upsert_replaces_by_key_and_inserts_the_rest()
    {
        var t = await TableAsync("upsert", Capabilities.Write | Capabilities.Upsert);
        await t.InsertAsync(Rows(0, 20));
        await t.UpsertAsync(Rows(10, 20, "-u"));
        Assert.Equal(Canonical(Rows(0, 10).Concat(Rows(10, 20, "-u"))), Canonical(await t.ReadAsync()));
        await t.ValidateAsync();
    }

    [SkippableFact]
    public async Task Delete_by_key_removes_exactly_those_rows()
    {
        var t = await TableAsync("delete", Capabilities.Write | Capabilities.DeleteByKey);
        await t.InsertAsync(Rows(0, 30));
        await t.DeleteByKeyAsync([3, 7, 29, 1000]);
        Assert.Equal(Canonical(Rows(0, 30).Where(r => (long)r[0]! is not (3 or 7 or 29))), Canonical(await t.ReadAsync()));
        await t.ValidateAsync();
    }

    [SkippableFact]
    public async Task Time_travel_reads_each_version_as_it_was()
    {
        var t = await TableAsync("travel", Capabilities.Write | Capabilities.TimeTravel | Capabilities.DeleteByKey);
        await t.InsertAsync(Rows(0, 10));
        var v1 = (await t.CurrentVersionAsync())!.Value;
        await t.InsertAsync(Rows(10, 10));
        var v2 = (await t.CurrentVersionAsync())!.Value;
        await t.DeleteByKeyAsync([0, 1]);
        Assert.Equal(Canonical(Rows(0, 10)), Canonical(await t.ReadAsync(v1)));
        Assert.Equal(Canonical(Rows(0, 20)), Canonical(await t.ReadAsync(v2)));
        Assert.Equal(18, (await t.ReadAsync()).Count);
        await t.ValidateAsync();
    }

    [SkippableFact]
    public async Task An_added_column_reads_null_for_old_rows_and_values_for_new_ones()
    {
        var t = await TableAsync("evolve", Capabilities.Write | Capabilities.AddColumn);
        await t.InsertAsync(Rows(0, 5));
        await t.AddColumnAsync("region");
        await t.InsertAsync(Rows(5, 5).Select(r => (object?[])[.. r, "eu"]).ToList());
        var rows = await t.ReadAsync();
        Assert.Equal(10, rows.Count);
        Assert.All(rows, r => Assert.Equal(5, r.Length));
        Assert.Equal(5, rows.Count(r => r[4] is null));
        Assert.Equal(5, rows.Count(r => (string?)r[4] == "eu"));
        await t.ValidateAsync();
    }

    [SkippableFact]
    public async Task Two_writers_racing_lose_no_rows()
    {
        var t = await TableAsync("race", Capabilities.Write | Capabilities.ConcurrentWriters);
        await t.InsertAsync(Rows(0, 1));
        var other = await t.OpenAgainAsync();
        await Task.WhenAll(
            Task.Run(async () => { for (var i = 0; i < 5; i++) await t.InsertAsync(Rows(1000 + i * 10, 10)); }),
            Task.Run(async () => { for (var i = 0; i < 5; i++) await other.InsertAsync(Rows(2000 + i * 10, 10)); }));
        Assert.Equal(101, (await t.ReadAsync()).Count);
        await t.ValidateAsync();
    }
}
