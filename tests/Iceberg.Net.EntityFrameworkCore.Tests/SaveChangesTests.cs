using Microsoft.EntityFrameworkCore;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.EntityFrameworkCore.Tests;

public sealed class SaveChangesTests : EfTest
{
    private async Task<int> SnapshotsAsync(string table) => (await TableAsync(table)).Metadata.Snapshots.Count;

    [Fact]
    public async Task Added_modified_and_deleted_entities_commit_one_snapshot_per_table()
    {
        await SeedOrdersAsync(10);
        Assert.Equal(1, await SnapshotsAsync("orders"));
        await using (var ctx = NewContext())
        {
            var five = await ctx.Orders.SingleAsync(o => o.Id == 5);
            five.Qty = 500;
            five.Note = null;
            ctx.Orders.Remove(await ctx.Orders.SingleAsync(o => o.Id == 6));
            ctx.Orders.Add(new Order { Id = 11, Customer = "dan", Qty = 1, CreatedAt = new DateTime(2026, 3, 1) });
            ctx.Customers.Add(new Customer { Name = "dan", Region = "eu" });
            Assert.Equal(4, await ctx.SaveChangesAsync());
            Assert.Equal(EntityState.Unchanged, ctx.Entry(five).State);
        }
        Assert.Equal(2, await SnapshotsAsync("orders"));
        Assert.Equal(1, await SnapshotsAsync("customer"));

        await using var check = NewContext();
        var all = await check.Orders.AsNoTracking().ToListAsync();
        Assert.Equal([1L, 2, 3, 4, 5, 7, 8, 9, 10, 11], all.Select(o => o.Id).Order());
        var updated = all.Single(o => o.Id == 5);
        Assert.Equal(500, updated.Qty);
        Assert.Null(updated.Note);
        Assert.Equal("eu", (await check.Customers.SingleAsync()).Region);
    }

    [Fact]
    public async Task Sync_SaveChanges_and_attached_updates_work_without_a_query()
    {
        await SeedOrdersAsync(3);
        using (var ctx = NewContext())
        {
            var detached = new Order { Id = 2, Customer = "zoe", Qty = 9, CreatedAt = new DateTime(2026, 1, 1) };
            ctx.Orders.Update(detached);
            Assert.Equal(1, ctx.SaveChanges());
        }
        using var check = NewContext();
        Assert.Equal("zoe", check.Orders.Single(o => o.Id == 2).Customer);
        Assert.Equal(3, check.Orders.Count());
    }

    [Fact]
    public async Task Inserting_an_existing_key_fails_and_commits_nothing()
    {
        await SeedOrdersAsync(3);
        await using var ctx = NewContext();
        ctx.Orders.Add(new Order { Id = 2, Customer = "dup", CreatedAt = new DateTime(2026, 1, 1) });
        ctx.Orders.Add(new Order { Id = 9, Customer = "new", CreatedAt = new DateTime(2026, 1, 1) });
        var e = await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        Assert.Contains("already has a row with that key", e.Message);
        Assert.Equal(1, await SnapshotsAsync("orders"));
        Assert.Equal(EntityState.Added, ctx.ChangeTracker.Entries<Order>().First().State);
    }

    [Fact]
    public async Task Deleting_and_re_adding_a_key_replaces_the_row()
    {
        await SeedOrdersAsync(3);
        await using (var ctx = NewContext())
        {
            ctx.Orders.Remove(await ctx.Orders.SingleAsync(o => o.Id == 1));
            ctx.Orders.Add(new Order { Id = 1, Customer = "again", CreatedAt = new DateTime(2026, 1, 1) });
            await ctx.SaveChangesAsync();
        }
        await using var check = NewContext();
        Assert.Equal("again", (await check.Orders.SingleAsync(o => o.Id == 1)).Customer);
        Assert.Equal(3, await check.Orders.CountAsync());
    }

    [Fact]
    public async Task Updating_a_row_another_writer_deleted_is_a_concurrency_conflict()
    {
        await SeedOrdersAsync(3);
        await using var ctx = NewContext();
        var order = await ctx.Orders.SingleAsync(o => o.Id == 3);
        await using (var other = NewContext()) await other.Orders.Where(o => o.Id == 3).ExecuteDeleteAsync();
        order.Qty = 42;
        var e = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => ctx.SaveChangesAsync());
        Assert.Same(order, Assert.Single(e.Entries).Entity);
    }

    [Fact]
    public async Task Row_versions_are_generated_and_checked()
    {
        await using (var setup = await CreatedAsync())
        {
            setup.Customers.Add(new Customer { Name = "ann", Region = "eu" });
            await setup.SaveChangesAsync();
            Assert.Equal(1, setup.Customers.Local.Single().Version);
        }
        await using var first = NewContext();
        await using var second = NewContext();
        var a = await first.Customers.SingleAsync();
        var b = await second.Customers.SingleAsync();
        a.Region = "us";
        await first.SaveChangesAsync();
        Assert.Equal(2, a.Version);

        b.Region = "apac";
        var e = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => second.SaveChangesAsync());
        Assert.Contains("concurrency token Version", e.Message);

        // The EF Core way to resolve it: reload the stored values and save again.
        await e.Entries.Single().ReloadAsync();
        Assert.Equal(2, b.Version);
        b.Region = "apac";
        await second.SaveChangesAsync();
        await using var check = NewContext();
        var stored = await check.Customers.SingleAsync();
        Assert.Equal(("apac", 3L), (stored.Region, stored.Version));
    }

    [Fact]
    public async Task User_managed_concurrency_tokens_are_checked_on_delete()
    {
        var id = Guid.NewGuid();
        await using (var setup = await CreatedAsync())
        {
            setup.Accounts.Add(new Account { Id = id, Balance = 10m, Stamp = "v1" });
            await setup.SaveChangesAsync();
        }
        await using var stale = NewContext();
        var mine = await stale.Accounts.SingleAsync();
        await using (var other = NewContext())
        {
            var theirs = await other.Accounts.SingleAsync();
            theirs.Balance = 20m;
            theirs.Stamp = "v2";
            await other.SaveChangesAsync();
        }
        stale.Accounts.Remove(mine);
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => stale.SaveChangesAsync());
        await using var check = NewContext();
        Assert.Equal(20m, (await check.Accounts.SingleAsync()).Balance);
    }

    [Fact]
    public async Task RequireUnchangedSnapshot_fails_when_the_table_moved_since_it_was_read()
    {
        await SeedOrdersAsync(3);
        await using var ctx = NewContext(o => o.RequireUnchangedSnapshot());
        var order = await ctx.Orders.SingleAsync(o => o.Id == 1);
        await using (var other = NewContext())
        {
            other.Orders.Add(new Order { Id = 50, Customer = "x", CreatedAt = new DateTime(2026, 1, 1) });
            await other.SaveChangesAsync();
        }
        order.Qty = 7;
        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => ctx.SaveChangesAsync());

        // Without the option only the row matters: the unrelated insert does not conflict.
        await using var relaxed = NewContext();
        var again = await relaxed.Orders.SingleAsync(o => o.Id == 1);
        await using (var other = NewContext())
        {
            other.Orders.Add(new Order { Id = 51, Customer = "y", CreatedAt = new DateTime(2026, 1, 1) });
            await other.SaveChangesAsync();
        }
        again.Qty = 8;
        await relaxed.SaveChangesAsync();
    }

    [Fact]
    public async Task Columns_the_model_does_not_map_keep_their_values_on_update()
    {
        await SeedOrdersAsync(2);
        var table = await TableAsync("orders");
        await table.UpdateSchemaAsync(u => u.AddColumn("source_system", PrimitiveType.String));
        await TableMutations.UpdateAsync(table, Iceberg.Net.Expressions.Expression.True, set => set.Set("source_system", "erp"));
        await using (var ctx = NewContext())
        {
            (await ctx.Orders.SingleAsync(o => o.Id == 1)).Qty = 99;
            await ctx.SaveChangesAsync();
        }
        var rows = new List<object?[]>();
        await foreach (var row in TableScan.ReadRowsAsync(await TableAsync("orders"), new ScanRequest { Columns = ["id", "qty", "source_system"] })) rows.Add(row);
        Assert.Equal([(1L, 99, "erp"), (2L, 2, "erp")], rows.Select(r => ((long)r[0]!, (int)r[1]!, (string)r[2]!)).OrderBy(r => r.Item1));
    }

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
}
