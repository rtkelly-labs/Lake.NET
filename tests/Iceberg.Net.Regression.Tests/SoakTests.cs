using System.Collections.Concurrent;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Concurrency soak: several writers mutate one table at once for ICEBERG_SOAK_SECONDS (default 15).
/// Each writer owns a disjoint key range, so its final rows follow from its own acknowledged operations; all
/// writers also upsert a shared set of hot keys, which must end with exactly one row each. Lost updates,
/// duplicated keys, resurrected deletes or a broken table fail the run.
/// A writer that exhausts its retry budget (starvation: a slow copy-on-write planner losing to fast appenders)
/// gets a <see cref="Iceberg.Net.Metadata.CommitFailedException"/>; that is allowed, but the failed operation
/// must leave no trace, which the per-writer model checks because it only records acknowledged operations.
/// </summary>
[Spec("Optimistic Concurrency")]
[Spec("Commit Conflict Resolution and Retry")]
[Trait("Category", "Soak")]
public sealed class SoakTests
{
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "bucket", PrimitiveType.Int),
    ], identifierFieldIds: [1, 3]);

    private const int Writers = 4;
    private const long HotKeys = 5;

    public static TheoryData<string, int> Targets() => new() { { "memory", 2 }, { "memory", 3 }, { "glue", 2 }, { "rest", 3 }, { "polaris", 3 }, { "gravitino", 2 }, { "nessie", 2 }, { "lakekeeper", 3 }, { "jdbc", 2 }, { "dotnet-rest", 3 } };

    private static int Seconds => int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_SOAK_SECONDS"), out var s) ? s : 15;

    [SkippableTheory]
    [MemberData(nameof(Targets))]
    public async Task Concurrent_writers_never_lose_or_duplicate_rows(string kind, int formatVersion)
    {
        var dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-soak", Guid.NewGuid().ToString("N"));
        ICatalog catalog;
        if (kind == "memory") catalog = new InMemoryCatalog(dir);
        else
        {
            Require.Catalog(kind);
            catalog = await TestEnvironment.Current.CatalogAsync(kind);
        }
        var ns = Namespace.Of(TestEnvironment.Current.NewNamespace($"soak_{kind}_{formatVersion}"));
        var id = new TableIdentifier(ns, "t");
        try
        {
            await catalog.CreateNamespaceAsync(ns);
            var created = await catalog.CreateTableAsync(new CreateTableRequest(id, Schema,
                PartitionSpec.BuilderFor(Schema).Identity("bucket").Build(), FormatVersion: formatVersion,
                // Generous retry budget: the soak is about correctness under contention, not giving up.
                Properties: new Dictionary<string, string> { ["commit.retry.num-retries"] = "50", ["commit.retry.min-wait-ms"] = "5", ["commit.retry.max-wait-ms"] = "200" }));

            var deadline = DateTime.UtcNow.AddSeconds(Seconds);
            var models = new ConcurrentDictionary<int, Dictionary<long, string>>();
            var operations = new ConcurrentBag<string>();
            var gaveUp = new ConcurrentBag<string>();
            var tasks = Enumerable.Range(0, Writers).Select(w => Task.Run(async () =>
            {
                var rnd = new Random(w * 7919 + formatVersion);
                var mine = new Dictionary<long, string>();
                models[w] = mine;
                var table = await catalog.LoadTableAsync(id);
                var next = 0;
                while (DateTime.UtcNow < deadline)
                {
                    var strategy = (DeleteStrategy)rnd.Next(1, 4);
                    try
                    {
                    long Key(int n) => 1_000 + w * 100_000L + n;
                    object?[] Row(long k, string v) => [k, v, (int)(k % 3)];
                    switch (rnd.Next(5))
                    {
                        case 0: // insert new own keys
                        {
                            var keys = Enumerable.Range(0, rnd.Next(1, 4)).Select(_ => Key(next++)).ToList();
                            await TableMutations.InsertAsync(table, keys.Select(k => Row(k, "i")).ToList());
                            foreach (var k in keys) mine[k] = "i";
                            operations.Add("insert");
                            break;
                        }
                        case 1 when mine.Count > 0: // upsert own keys with a new version
                        {
                            var keys = mine.Keys.OrderBy(_ => rnd.Next()).Take(rnd.Next(1, 3)).ToList();
                            var v = $"w{w}-{rnd.Next()}";
                            await TableMutations.UpsertAsync(table, keys.Select(k => Row(k, v)).ToList(), strategy: strategy);
                            foreach (var k in keys) mine[k] = v;
                            operations.Add($"upsert/{strategy}");
                            break;
                        }
                        case 2 when mine.Count > 0: // delete own keys
                        {
                            var keys = mine.Keys.OrderBy(_ => rnd.Next()).Take(rnd.Next(1, 3)).ToList();
                            await TableMutations.DeleteByKeysAsync(table, ["id", "bucket"], keys.Select(k => new object?[] { k, (int)(k % 3) }), strategy);
                            foreach (var k in keys) mine.Remove(k);
                            operations.Add($"delete/{strategy}");
                            break;
                        }
                        default: // contend on shared hot keys
                        {
                            var k = (long)rnd.Next(0, (int)HotKeys);
                            await TableMutations.UpsertAsync(table, [Row(k, $"hot-w{w}")], strategy: strategy);
                            operations.Add($"hot/{strategy}");
                            break;
                        }
                    }
                    }
                    catch (Iceberg.Net.Metadata.CommitFailedException)
                    {
                        gaveUp.Add($"w{w}"); // starved: retries exhausted; the model was not updated
                        table = await catalog.LoadTableAsync(id);
                    }
                }
            })).ToList();
            await Task.WhenAll(tasks);

            var final = await catalog.LoadTableAsync(id);
            var rows = await TableScan.ReadAllAsync(final);
            var byKey = rows.GroupBy(r => (long)r[0]!).ToDictionary(g => g.Key, g => g.Select(r => (string?)r[1]).ToList());

            foreach (var dup in byKey.Where(kv => kv.Value.Count > 1))
                Assert.Fail($"key {dup.Key} has {dup.Value.Count} rows after {operations.Count} operations");
            foreach (var (w, model) in models)
            {
                var actual = byKey.Where(kv => kv.Key >= 1_000 + w * 100_000L && kv.Key < 1_000 + (w + 1) * 100_000L).ToDictionary(kv => kv.Key, kv => kv.Value[0] ?? "null");
                Assert.Equal(model.OrderBy(kv => kv.Key), actual.OrderBy(kv => kv.Key));
            }
            Assert.All(byKey.Keys.Where(k => k < HotKeys), k => Assert.StartsWith("hot-w", byKey[k][0]));

            var report = await TableValidator.ValidateAsync(final, new ValidationOptions { Deep = true });
            Assert.True(report.IsValid, report.ToString());
            Assert.True(operations.Count >= Writers, $"only {operations.Count} operations ran");
            // Liveness, loosely: starvation must stay the exception, not the rule.
            Assert.True(gaveUp.Count * 4 <= operations.Count, $"{gaveUp.Count} operations gave up vs {operations.Count} committed");
        }
        finally
        {
            if (kind != "memory")
            {
                try { await catalog.DropTableAsync(id, purge: true); await catalog.DropNamespaceAsync(ns); }
                catch (Exception) { /* best effort */ }
            }
            catalog.Dispose();
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }
}
