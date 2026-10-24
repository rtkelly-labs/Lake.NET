using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// The EF Core provider over this server: a <c>SaveChanges</c> spanning tables goes through
/// <c>POST /v1/transactions/commit</c>, so a failure in one table leaves every table unchanged, under both stores.
/// </summary>
public sealed class EfCoreTransactionTests
{
    public static TheoryData<string> Stores => new() { "memory", "sqlite" };

    public sealed class Item
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        [ConcurrencyCheck] public int Stock { get; set; }
    }

    public sealed class Movement
    {
        public long Id { get; set; }
        public long ItemId { get; set; }
        public int Delta { get; set; }
    }

    /// <summary>Tables sort as <c>a_items</c>, then <c>b_movements</c>.</summary>
    public sealed class StockContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Item> Items => Set<Item>();
        public DbSet<Movement> Movements => Set<Movement>();

        protected override void OnModelCreating(ModelBuilder b)
        {
            b.UseIcebergSnakeCaseNames();
            b.Entity<Item>().ToIcebergTable("a_items");
            b.Entity<Movement>().ToIcebergTable("b_movements");
        }
    }

    /// <summary>Counts transaction commits and runs <see cref="BeforeNextTransaction"/> once, before the next one is sent.</summary>
    private sealed class TransactionHook(HttpMessageHandler inner) : DelegatingHandler(inner)
    {
        public Func<Task>? BeforeNextTransaction { get; set; }
        public int Transactions { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath.EndsWith("/transactions/commit", StringComparison.Ordinal))
            {
                Transactions++;
                if (BeforeNextTransaction is { } hook)
                {
                    BeforeNextTransaction = null;
                    await hook();
                }
            }
            return await base.SendAsync(request, ct);
        }
    }

    private static async Task<(ServerHost Host, RestCatalog Catalog, TransactionHook Hook)> StartAsync(string store)
    {
        var host = ServerHost.Start(store);
        var hook = new TransactionHook(host.Factory.Server.CreateHandler());
        var catalog = await RestCatalog.ConnectAsync(new RestCatalogOptions
        {
            Uri = new Uri("http://localhost/"),
            Handler = hook,
            AccessDelegation = null,
        }, _ => new LocalFileIO(), "dotnet-rest");
        await using (var ctx = Context(catalog))
        {
            await ctx.Database.EnsureCreatedAsync();
            ctx.Items.AddRange(new Item { Id = 1, Name = "bolt", Stock = 10 }, new Item { Id = 2, Name = "nut", Stock = 20 });
            ctx.Movements.Add(new Movement { Id = 1, ItemId = 1, Delta = 10 });
            await ctx.SaveChangesAsync();
        }
        return (host, catalog, hook);
    }

    private static StockContext Context(ICatalog catalog) =>
        new(new DbContextOptionsBuilder<StockContext>().UseIceberg(catalog, "shop", o => o.RequireAtomicSaveChanges()).Options);

    private static async Task<long?> HeadAsync(ICatalog catalog, string table) =>
        (await catalog.LoadTableAsync(TableIdentifier.Of("shop", table))).Metadata.CurrentSnapshotId;

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task SaveChanges_across_tables_commits_in_one_server_transaction(string store)
    {
        var (host, catalog, hook) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        Assert.True(catalog.SupportsMultiTableTransactions);
        Assert.Equal(1, hook.Transactions); // the seed spanned both tables

        await using (var ctx = Context(catalog))
        {
            (await ctx.Items.SingleAsync(i => i.Id == 1)).Stock = 7;
            ctx.Movements.Add(new Movement { Id = 2, ItemId = 1, Delta = -3 });
            Assert.Equal(2, await ctx.SaveChangesAsync());
        }

        Assert.Equal(2, hook.Transactions);
        await using var check = Context(catalog);
        Assert.Equal(7, (await check.Items.SingleAsync(i => i.Id == 1)).Stock);
        Assert.Equal(2, await check.Movements.CountAsync());
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task A_failure_in_table_b_leaves_table_a_unchanged(string store)
    {
        var (host, catalog, hook) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var headA = await HeadAsync(catalog, "a_items");

        await using var ctx = Context(catalog);
        var bolt = await ctx.Items.SingleAsync(i => i.Id == 1);
        bolt.Stock = 7;
        ctx.Movements.Add(new Movement { Id = 1, ItemId = 1, Delta = -3 }); // the key exists: the insert fails

        await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());

        Assert.Equal(EntityState.Modified, ctx.Entry(bolt).State);
        Assert.Equal(headA, await HeadAsync(catalog, "a_items"));
        await using var check = Context(catalog);
        Assert.Equal(10, (await check.Items.SingleAsync(i => i.Id == 1)).Stock);
    }

    [Theory]
    [MemberData(nameof(Stores))]
    public async Task A_conflict_on_table_b_found_at_commit_rolls_back_table_a(string store)
    {
        var (host, catalog, hook) = await StartAsync(store);
        await using var _ = host;
        using var __ = catalog;
        var headA = await HeadAsync(catalog, "a_items");

        await using var ctx = Context(catalog);
        (await ctx.Items.SingleAsync(i => i.Id == 1)).Stock = 7;
        var movement = await ctx.Movements.SingleAsync();
        ctx.Movements.Remove(movement);
        // After both tables are planned, another writer deletes the movement: the server refuses the transaction (409),
        // and the re-planned MERGE on b finds its row gone.
        hook.BeforeNextTransaction = async () =>
        {
            await using var other = Context(catalog);
            other.Movements.Remove(await other.Movements.SingleAsync());
            await other.SaveChangesAsync();
        };

        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => ctx.SaveChangesAsync());

        Assert.Equal(2, hook.Transactions); // the seed, then the refused attempt; the re-plan failed before a retry
        Assert.Equal(headA, await HeadAsync(catalog, "a_items"));
        await using var check = Context(catalog);
        Assert.Equal(10, (await check.Items.SingleAsync(i => i.Id == 1)).Stock);
        Assert.Equal(0, await check.Movements.CountAsync());
    }
}
