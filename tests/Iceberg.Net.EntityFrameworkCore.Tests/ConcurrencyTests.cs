using Microsoft.EntityFrameworkCore;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.EntityFrameworkCore.Tests;

/// <summary>
/// Races: another writer commits between the moment SaveChanges planned its MERGE and the moment it commits. The core
/// re-plans on the conflict, and the provider's checks run again against the new state.
/// </summary>
public sealed class ConcurrencyTests : EfTest
{
    [Fact]
    public async Task A_token_changed_by_a_racing_commit_is_caught_on_the_replanned_attempt()
    {
        await using (var setup = await CreatedAsync())
        {
            setup.Customers.Add(new Customer { Name = "ann", Region = "eu" });
            await setup.SaveChangesAsync();
        }
        using var hooked = new HookCatalog(Catalog);
        await using var ctx = new ShopContext(new DbContextOptionsBuilder<ShopContext>().UseIceberg(hooked, Ns).Options);
        var mine = await ctx.Customers.SingleAsync();
        mine.Region = "us";
        hooked.BeforeCommit = async () =>
        {
            await using var other = NewContext();
            (await other.Customers.SingleAsync()).Region = "apac";
            await other.SaveChangesAsync();
        };
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => ctx.SaveChangesAsync());
        Assert.Equal(1, hooked.Conflicts);
        await using var check = NewContext();
        Assert.Equal("apac", (await check.Customers.SingleAsync()).Region);
    }

    [Fact]
    public async Task Without_tokens_a_racing_commit_to_other_rows_is_merged_not_lost()
    {
        await SeedOrdersAsync(4);
        using var hooked = new HookCatalog(Catalog);
        await using var ctx = new ShopContext(new DbContextOptionsBuilder<ShopContext>().UseIceberg(hooked, Ns).Options);
        (await ctx.Orders.SingleAsync(o => o.Id == 1)).Qty = 100;
        hooked.BeforeCommit = async () =>
        {
            await using var other = NewContext();
            (await other.Orders.SingleAsync(o => o.Id == 2)).Qty = 200;
            await other.SaveChangesAsync();
        };
        await ctx.SaveChangesAsync();
        Assert.Equal(1, hooked.Conflicts);
        await using var check = NewContext();
        var qty = await check.Orders.Where(o => o.Id <= 2).OrderBy(o => o.Id).Select(o => o.Qty).ToListAsync();
        Assert.Equal([100, 200], qty);
    }

    /// <summary>Forwards to a catalog, running <see cref="BeforeCommit"/> once before the next table commit.</summary>
    private sealed class HookCatalog(ICatalog inner) : ICatalog
    {
        public Func<Task>? BeforeCommit { get; set; }
        public int Conflicts { get; private set; }

        public string Name => inner.Name;
        public IFileIO FileIO => inner.FileIO;
        public Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default) => inner.CreateNamespaceAsync(ns, properties, ct);
        public Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default) => inner.ListNamespacesAsync(parent, ct);
        public Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default) => inner.NamespaceExistsAsync(ns, ct);
        public Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default) => inner.DropNamespaceAsync(ns, ct);
        public Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default) => inner.ListTablesAsync(ns, ct);
        public async Task<Table> CreateTableAsync(CreateTableRequest request, CancellationToken ct = default) => Wrap(await inner.CreateTableAsync(request, ct));
        public async Task<Table> LoadTableAsync(TableIdentifier id, CancellationToken ct = default) => Wrap(await inner.LoadTableAsync(id, ct));
        public Task<bool> TableExistsAsync(TableIdentifier id, CancellationToken ct = default) => inner.TableExistsAsync(id, ct);
        public async Task<Table> RegisterTableAsync(TableIdentifier id, string metadataLocation, CancellationToken ct = default) => Wrap(await inner.RegisterTableAsync(id, metadataLocation, ct));
        public Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default) => inner.RenameTableAsync(from, to, ct);
        public Task DropTableAsync(TableIdentifier id, bool purge = false, CancellationToken ct = default) => inner.DropTableAsync(id, purge, ct);
        public Task CommitTransactionAsync(IReadOnlyList<TableCommit> commits, CancellationToken ct = default) => inner.CommitTransactionAsync(commits, ct);

        public async Task<TableMetadata> CommitTableAsync(TableCommit commit, CancellationToken ct = default)
        {
            if (BeforeCommit is { } hook)
            {
                BeforeCommit = null;
                await hook();
            }
            try
            {
                return await inner.CommitTableAsync(commit, ct);
            }
            catch (CommitFailedException)
            {
                Conflicts++;
                throw;
            }
        }

        private Table Wrap(Table t) => new(this, t.Identifier, t.Metadata, t.IO);

        public void Dispose() { }
    }
}
