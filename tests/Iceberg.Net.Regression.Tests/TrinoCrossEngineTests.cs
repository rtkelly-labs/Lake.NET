using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Trino's Iceberg connector as a third engine, both directions, through the shared REST fixture + moto S3:
/// Trino reads what we write (every delete strategy), we read what Trino writes (its merge-on-read DELETE/UPDATE/MERGE),
/// and mixed histories (Trino's position deletes, then our equality deletes) read the same in both.
/// </summary>
[Spec("Row-level Deletes")]
[Spec("Scan Planning")]
[Trait("Category", "Regression")]
public sealed class TrinoCrossEngineTests : IAsyncLifetime
{
    private static readonly Schema Keyed = new(0, CatalogConformanceTests.Schema.Columns, identifierFieldIds: [1]);
    private ICatalog? _catalog;
    private readonly Namespace _ns = Namespace.Of(TestEnvironment.Current.NewNamespace("trino"));

    public async Task InitializeAsync()
    {
        if (TrinoOracle.SkipReason is not null) return;
        _catalog = await TestEnvironment.Current.CatalogAsync("rest");
        await _catalog.CreateNamespaceAsync(_ns);
    }

    public async Task DisposeAsync()
    {
        if (_catalog is null) return;
        foreach (var id in await _catalog.ListTablesAsync(_ns)) await _catalog.DropTableAsync(id, purge: true);
        await _catalog.DropNamespaceAsync(_ns);
        _catalog.Dispose();
    }

    private string Sql(string table) => TrinoOracle.Table(_ns.ToString(), table);

    private static async Task<Dictionary<long, (string? Name, int? Qty, string? Region)>> Ours(Table t) =>
        (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => ((string?)r[1], (int?)r[5], (string?)r[4]));

    private async Task<Dictionary<long, (string? Name, int? Qty, string? Region)>> Trinos(string table) =>
        (await TrinoOracle.QueryAsync($"SELECT id, name, qty, region FROM {Sql(table)}")).Rows
            .ToDictionary(r => r[0]!.GetValue<long>(), r => (r[1]?.GetValue<string>(), r[2]?.GetValue<int>(), r[3]?.GetValue<string>()));

    public static TheoryData<int, DeleteStrategy> Matrix() => new()
    {
        { 2, DeleteStrategy.CopyOnWrite }, { 2, DeleteStrategy.PositionDeletes }, { 2, DeleteStrategy.EqualityDeletes },
        { 3, DeleteStrategy.CopyOnWrite }, { 3, DeleteStrategy.PositionDeletes }, { 3, DeleteStrategy.EqualityDeletes },
    };

    [SkippableTheory]
    [MemberData(nameof(Matrix))]
    public async Task Trino_reads_what_we_write(int formatVersion, DeleteStrategy strategy)
    {
        Require.Trino();
        var name = $"ours_v{formatVersion}_{strategy}".ToLowerInvariant();
        var t = await _catalog!.CreateTableAsync(new CreateTableRequest(new TableIdentifier(_ns, name), Keyed, CatalogConformanceTests.Spec, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(0, 30));
        await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(25, 10).Select(r => { r[1] = $"upserted-{r[0]}"; return r; }).ToList(), ["id", "event_ts", "region"], strategy);
        await TableMutations.DeleteByKeysAsync(t, ["id", "event_ts", "region"], CatalogConformanceTests.Rows(3, 2).Select(r => new object?[] { r[0], r[3], r[4] }), strategy);

        var ours = await Ours(await _catalog.LoadTableAsync(t.Identifier));
        if (formatVersion >= 3 && strategy == DeleteStrategy.PositionDeletes)
        {
            // Characterization: Trino 478 reads v3 tables but not deletion vectors (Spark does; SparkCrossEngineTests).
            // It must refuse rather than return deleted rows. When Trino adds DV support this fails: drop the branch.
            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => Trinos(name));
            Assert.Contains("PUFFIN", e.Message);
            return;
        }
        Assert.Equal(ours.OrderBy(kv => kv.Key), (await Trinos(name)).OrderBy(kv => kv.Key));
    }

    [SkippableFact]
    public async Task We_read_what_trino_writes_with_merge_on_read()
    {
        Require.Trino();
        var table = Sql("trino_mor");
        await TrinoOracle.QueryAsync($"CREATE TABLE {table} (id bigint, name varchar, qty integer, region varchar) WITH (format_version = 2, partitioning = ARRAY['region'])");
        await TrinoOracle.QueryAsync($"INSERT INTO {table} SELECT x, 'n' || CAST(x AS varchar), CAST(x % 10 AS integer), IF(x % 2 = 0, 'eu', 'us') FROM UNNEST(sequence(0, 39)) AS u(x)");
        await TrinoOracle.QueryAsync($"DELETE FROM {table} WHERE id % 5 = 0");
        await TrinoOracle.QueryAsync($"UPDATE {table} SET name = 'updated' WHERE id % 7 = 1");
        await TrinoOracle.QueryAsync($"MERGE INTO {table} t USING (VALUES (2, 'merged'), (100, 'inserted')) AS s(id, name) ON t.id = s.id " +
                                     "WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name, qty, region) VALUES (s.id, s.name, 0, 'eu')");

        var t = await _catalog!.LoadTableAsync(new TableIdentifier(_ns, "trino_mor"));
        var rows = (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => ((string?)r[1], (int?)r[2], (string?)r[3]));
        var trino = (await TrinoOracle.QueryAsync($"SELECT id, name, qty, region FROM {table}")).Rows
            .ToDictionary(r => r[0]!.GetValue<long>(), r => (r[1]?.GetValue<string>(), r[2]?.GetValue<int>(), r[3]?.GetValue<string>()));
        Assert.Equal(trino.OrderBy(kv => kv.Key), rows.OrderBy(kv => kv.Key));
        Assert.Equal("merged", rows[2].Item1);
        Assert.Contains(100L, rows.Keys);
        Assert.DoesNotContain(5L, rows.Keys);

        // Trino really used merge-on-read: there are position delete files for us to apply.
        Assert.False((await TableScan.DeletesAsync(t)).IsEmpty);
        var report = await TableValidator.ValidateAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());
    }

    [SkippableTheory]
    [InlineData(DeleteStrategy.EqualityDeletes)]
    [InlineData(DeleteStrategy.PositionDeletes)]
    [InlineData(DeleteStrategy.CopyOnWrite)]
    public async Task Mixed_history_reads_the_same_in_both(DeleteStrategy strategy)
    {
        Require.Trino();
        var name = $"mixed_{strategy}".ToLowerInvariant();
        var table = Sql(name);
        await TrinoOracle.QueryAsync($"CREATE TABLE {table} (id bigint, name varchar, qty integer, region varchar) WITH (format_version = 2)");
        await TrinoOracle.QueryAsync($"INSERT INTO {table} SELECT x, 'n' || CAST(x AS varchar), 1, 'eu' FROM UNNEST(sequence(0, 19)) AS u(x)");
        await TrinoOracle.QueryAsync($"DELETE FROM {table} WHERE id IN (1, 2)");

        var t = await _catalog!.LoadTableAsync(new TableIdentifier(_ns, name));
        await TableMutations.UpsertAsync(t, [[3L, "ours", 2, "eu"], [50L, "ours-new", 2, "eu"]], ["id"], strategy);
        await TableMutations.DeleteByKeysAsync(t, ["id"], [[4L]], strategy);

        await TrinoOracle.QueryAsync($"UPDATE {table} SET qty = 9 WHERE id = 5");

        t = await _catalog.LoadTableAsync(t.Identifier);
        var ours = (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!, r => ((string?)r[1], (int?)r[2]));
        var trino = (await TrinoOracle.QueryAsync($"SELECT id, name, qty FROM {table}")).Rows
            .ToDictionary(r => r[0]!.GetValue<long>(), r => (r[1]?.GetValue<string>(), r[2]?.GetValue<int>()));
        Assert.Equal(trino.OrderBy(kv => kv.Key), ours.OrderBy(kv => kv.Key));
        Assert.Equal(("ours", 2), ours[3]);
        Assert.Equal(9, ours[5].Item2);
        Assert.DoesNotContain(4L, ours.Keys);
        var report = await TableValidator.ValidateAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());
    }
}
