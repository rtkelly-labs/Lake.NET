using Microsoft.EntityFrameworkCore;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.EntityFrameworkCore.Tests;

/// <summary>An in-memory catalog that can run a hook just before its next multi-table pointer swap, or refuse transactions.</summary>
public sealed class TransactionHookCatalog(string warehouse, bool transactions = true) : InMemoryCatalog(warehouse)
{
    public Func<Task>? BeforeNextTransaction { get; set; }
    public int Transactions { get; private set; }

    public override bool SupportsMultiTableTransactions => transactions;

    protected override async Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct)
    {
        Transactions++;
        if (BeforeNextTransaction is { } hook)
        {
            BeforeNextTransaction = null;
            await hook();
        }
        await base.SwapMetadataLocationsAsync(swaps, ct);
    }
}

/// <summary><c>SaveChanges</c> across tables on a catalog with multi-table transactions: all tables or none.</summary>
public sealed class AtomicSaveChangesTests() : EfTest(w => new TransactionHookCatalog(w))
{
    private TransactionHookCatalog Hooked => (TransactionHookCatalog)Catalog;

    private async Task<long?> HeadAsync(string table) => (await TableAsync(table)).Metadata.CurrentSnapshotId;

    [Fact]
    public async Task Several_tables_commit_in_one_transaction_with_one_snapshot_each()
    {
        await SeedOrdersAsync(3);
        var orderSnapshots = (await TableAsync("orders")).Metadata.Snapshots.Count;
        await using var ctx = NewContext();
        var customer = new Customer { Name = "kim", Region = "eu" };
        ctx.Customers.Add(customer);
        (await ctx.Orders.SingleAsync(o => o.Id == 1)).Qty = 42;
        ctx.Orders.Remove(await ctx.Orders.SingleAsync(o => o.Id == 2));
        ctx.Accounts.Add(new Account { Balance = 10m, Stamp = "a" });

        Assert.Equal(4, await ctx.SaveChangesAsync());

        Assert.Equal(1, Hooked.Transactions);
        Assert.Equal(1, customer.Version);                               // the row version is written back
        Assert.Equal(EntityState.Unchanged, ctx.Entry(customer).State);
        Assert.Equal(orderSnapshots + 1, (await TableAsync("orders")).Metadata.Snapshots.Count);
        Assert.Single((await TableAsync("customer")).Metadata.Snapshots);
        await using var check = NewContext();
        Assert.Equal([1L, 3L], await check.Orders.OrderBy(o => o.Id).Select(o => o.Id).ToListAsync());
        Assert.Equal(42, (await check.Orders.SingleAsync(o => o.Id == 1)).Qty);
        Assert.Equal(1, await check.Customers.CountAsync());
        Assert.Equal(1, await check.Accounts.CountAsync());
    }

    [Fact]
    public async Task A_failure_in_a_later_table_leaves_earlier_tables_unchanged()
    {
        await SeedOrdersAsync(2);
        await using var ctx = NewContext();
        // customer sorts before orders; the order insert fails (duplicate key) after the customer was staged.
        var customer = new Customer { Name = "kim", Region = "eu" };
        ctx.Customers.Add(customer);
        var duplicate = new Order { Id = 1, Customer = "dup", CreatedAt = new DateTime(2026, 1, 1) };
        ctx.Orders.Add(duplicate);

        await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());

        Assert.Equal(EntityState.Added, ctx.Entry(customer).State);     // nothing was accepted
        Assert.Null(await HeadAsync("customer"));
        Assert.Equal(0, Hooked.Transactions);

        // Fix the cause and save again: the customer is saved now.
        ctx.Entry(duplicate).State = EntityState.Detached;
        Assert.Equal(1, await ctx.SaveChangesAsync());
        await using var check = NewContext();
        Assert.Equal("kim", (await check.Customers.SingleAsync()).Name);
        Assert.Equal(2, await check.Orders.CountAsync());
    }

    [Fact]
    public async Task A_token_changed_by_a_racing_commit_is_caught_on_the_re_plan_and_rolls_back_every_table()
    {
        await using (var setup = await CreatedAsync())
        {
            setup.Customers.Add(new Customer { Name = "ann", Region = "eu" });
            setup.Orders.Add(new Order { Id = 1, Customer = "ann", CreatedAt = new DateTime(2026, 1, 1) });
            await setup.SaveChangesAsync();
        }
        var ordersHead = await HeadAsync("orders");
        var before = Hooked.Transactions;                                // the setup saved two tables in one
        await using var ctx = NewContext();
        (await ctx.Customers.SingleAsync()).Region = "us";
        (await ctx.Orders.SingleAsync()).Qty = 5;
        ctx.Orders.Add(new Order { Id = 2, Customer = "ann", CreatedAt = new DateTime(2026, 1, 2) });
        // Between planning and the commit, another writer changes the customer (and its row version).
        Hooked.BeforeNextTransaction = async () =>
        {
            await using var other = NewContext();
            (await other.Customers.SingleAsync()).Region = "apac";
            await other.SaveChangesAsync();
        };

        var e = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => ctx.SaveChangesAsync());

        Assert.Contains("concurrency token", e.Message);
        Assert.Equal(before + 1, Hooked.Transactions);                   // the re-plan failed the check; no second commit
        Assert.Equal(ordersHead, await HeadAsync("orders"));            // the orders were not committed
        await using var check = NewContext();
        Assert.Equal("apac", (await check.Customers.SingleAsync()).Region);
        Assert.Equal(0, (await check.Orders.SingleAsync()).Qty);
    }

    [Fact]
    public async Task An_unrelated_racing_commit_is_merged_and_both_tables_commit()
    {
        await SeedOrdersAsync(3);
        await using var ctx = NewContext();
        (await ctx.Orders.SingleAsync(o => o.Id == 1)).Qty = 100;
        ctx.Customers.Add(new Customer { Name = "kim", Region = "eu" });
        Hooked.BeforeNextTransaction = async () =>
        {
            await using var other = NewContext();
            (await other.Orders.SingleAsync(o => o.Id == 2)).Qty = 200;
            await other.SaveChangesAsync();
        };

        await ctx.SaveChangesAsync();

        Assert.Equal(2, Hooked.Transactions);
        await using var check = NewContext();
        Assert.Equal([100, 200], await check.Orders.Where(o => o.Id <= 2).OrderBy(o => o.Id).Select(o => o.Qty).ToListAsync());
        Assert.Equal(1, await check.Customers.CountAsync());
    }
}

/// <summary>A catalog without multi-table transactions (Glue): table by table, or refused with RequireAtomicSaveChanges.</summary>
public sealed class PerTableSaveChangesTests() : EfTest(w => new TransactionHookCatalog(w, transactions: false))
{
    [Fact]
    public async Task A_failure_on_a_later_table_keeps_earlier_tables_committed_and_accepted()
    {
        await SeedOrdersAsync(2);
        await using var ctx = NewContext();
        // Tables commit in name order: customer, then orders. The order insert fails (duplicate key).
        var customer = new Customer { Name = "kim", Region = "eu" };
        ctx.Customers.Add(customer);
        ctx.Orders.Add(new Order { Id = 1, Customer = "dup", CreatedAt = new DateTime(2026, 1, 1) });
        await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        Assert.Equal(EntityState.Unchanged, ctx.Entry(customer).State);
        Assert.Equal(1, customer.Version);
        await using var check = NewContext();
        Assert.Equal(1, await check.Customers.CountAsync());
    }

    [Fact]
    public async Task RequireAtomicSaveChanges_refuses_several_tables_before_writing_anything()
    {
        await SeedOrdersAsync(2);
        await using var ctx = NewContext(o => o.RequireAtomicSaveChanges());
        var customer = new Customer { Name = "kim", Region = "eu" };
        ctx.Customers.Add(customer);
        ctx.Orders.Add(new Order { Id = 3, Customer = "kim", CreatedAt = new DateTime(2026, 1, 1) });

        var e = await Assert.ThrowsAsync<NotSupportedException>(() => ctx.SaveChangesAsync());

        Assert.Contains("RequireAtomicSaveChanges", e.Message);
        Assert.Equal(EntityState.Added, ctx.Entry(customer).State);
        Assert.Null((await TableAsync("customer")).Metadata.CurrentSnapshotId);
        Assert.Equal(2, await NewContext().Orders.CountAsync());

        // One table is atomic anyway.
        ctx.Entry(customer).State = EntityState.Detached;
        Assert.Equal(1, await ctx.SaveChangesAsync());
        Assert.Equal(3, await NewContext().Orders.CountAsync());
    }
}
