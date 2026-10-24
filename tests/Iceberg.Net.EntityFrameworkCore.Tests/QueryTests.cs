using Microsoft.EntityFrameworkCore;
using Iceberg.Net.Data;

namespace Iceberg.Net.EntityFrameworkCore.Tests;

public sealed class QueryTests : EfTest
{
    [Fact]
    public async Task Where_is_pushed_into_the_scan_and_matches_linq_to_objects()
    {
        var orders = await SeedOrdersAsync();
        await using var ctx = NewContext();
        var min = 3;
        var q = ctx.Orders.Where(o => o.Qty > min && o.Customer == "bob");
        Assert.Equal("(qty > 3 and customer == 'bob')", q.ToIcebergScanRequest().Filter.ToString());
        Assert.Equal(orders.Where(o => o.Qty > min && o.Customer == "bob").Select(o => o.Id).Order(), (await q.ToListAsync()).Select(o => o.Id).Order());
        Assert.Equal(orders.Count(o => o.Qty > min && o.Customer == "bob"), q.ToList().Count); // sync

        // An untranslatable conjunct stays a residual on the entity.
        var residual = ctx.Orders.Where(o => o.Qty > 1 && o.Customer.Length == 5);
        Assert.Contains("residual", residual.ToIcebergQueryString());
        Assert.Equal(orders.Count(o => o.Qty > 1 && o.Customer.Length == 5), await residual.CountAsync());
    }

    [Fact]
    public async Task Operators_count_first_take_order_projection_any()
    {
        var orders = await SeedOrdersAsync();
        await using var ctx = NewContext();
        Assert.Equal(30, await ctx.Orders.CountAsync());
        Assert.Equal(30L, await ctx.Orders.LongCountAsync());
        Assert.Equal(orders.Count(o => o.Status == OrderStatus.Open), await ctx.Orders.CountAsync(o => o.Status == OrderStatus.Open));
        Assert.True(await ctx.Orders.AnyAsync(o => o.Id == 7));
        Assert.False(await ctx.Orders.AnyAsync(o => o.Id == 700));
        Assert.Equal(7, (await ctx.Orders.FirstAsync(o => o.Id == 7)).Id);
        Assert.Null(await ctx.Orders.FirstOrDefaultAsync(o => o.Id == 700));
        Assert.Equal(7, (await ctx.Orders.SingleAsync(o => o.Id == 7)).Id);
        await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.Orders.SingleAsync(o => o.Customer == "bob"));

        var top = await ctx.Orders.OrderByDescending(o => o.Amount).ThenBy(o => o.Id).Take(3).Select(o => new { o.Id, o.Amount }).ToListAsync();
        Assert.Equal(orders.OrderByDescending(o => o.Amount).ThenBy(o => o.Id).Take(3).Select(o => new { o.Id, o.Amount }), top);
        Assert.Equal(["customer"], ctx.Orders.Select(o => o.Customer).ToIcebergScanRequest().Columns);
        Assert.Equal(orders.Max(o => o.Amount), await ctx.Orders.MaxAsync(o => o.Amount));
        Assert.Equal(orders.Sum(o => o.Qty), await ctx.Orders.SumAsync(o => o.Qty));
        Assert.Equal(orders.Where(o => o.Note == null).Select(o => o.Id).Order(), ctx.Orders.Where(o => o.Note == null).Select(o => o.Id).AsEnumerable().Order());
        Assert.Equal(
            orders.GroupBy(o => o.Customer).Select(g => (g.Key, g.Count())).OrderBy(x => x.Key),
            (await ctx.Orders.GroupBy(o => o.Customer).Select(g => new { g.Key, N = g.Count() }).ToListAsync()).Select(x => (x.Key, x.N)).OrderBy(x => x.Key));
        Assert.Equal(orders.Skip(5).Count(), await ctx.Orders.Skip(5).CountAsync());
        var streamed = 0;
        await foreach (var _ in ctx.Orders.Where(o => o.Id <= 4).AsAsyncEnumerable()) streamed++;
        Assert.Equal(4, streamed);
    }

    [Fact]
    public async Task Find_and_EF_Property_push_down()
    {
        await SeedOrdersAsync();
        await using var ctx = NewContext();
        var order = await ctx.Orders.FindAsync(12L);
        Assert.Equal(12, order!.Id);
        Assert.Same(order, ctx.Orders.Find(12L)); // the tracked instance, without a query
        var q = ctx.Orders.Where(o => EF.Property<string>(o, nameof(Order.Customer)) == "alice");
        Assert.Equal("customer == 'alice'", q.ToIcebergScanRequest().Filter.ToString());
        Assert.Equal(10, await q.CountAsync());
    }

    [Fact]
    public async Task Results_are_tracked_with_identity_resolution_unless_no_tracking()
    {
        await SeedOrdersAsync();
        await using var ctx = NewContext();
        var a = await ctx.Orders.SingleAsync(o => o.Id == 3);
        var b = (await ctx.Orders.Where(o => o.Qty == 3).ToListAsync()).Single(o => o.Id == 3);
        Assert.Same(a, b);
        Assert.Equal(EntityState.Unchanged, ctx.Entry(a).State);
        Assert.Equal(4, ctx.ChangeTracker.Entries<Order>().Count()); // ids 3, 10, 17, 24 have qty 3

        await using var other = NewContext();
        var untracked = await other.Orders.AsNoTracking().Where(o => o.Id < 5).ToListAsync();
        Assert.Equal(4, untracked.Count);
        Assert.Empty(other.ChangeTracker.Entries());
        other.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
        _ = other.Orders.ToList();
        Assert.Empty(other.ChangeTracker.Entries());
        _ = await other.Orders.AsTracking().Where(o => o.Id == 1).ToListAsync();
        Assert.Single(other.ChangeTracker.Entries());
    }

    [Fact]
    public async Task Time_travel_reads_an_older_snapshot_without_tracking()
    {
        await SeedOrdersAsync(10);
        var first = (await TableAsync("orders")).Metadata.CurrentSnapshotId!.Value;
        await using (var ctx = NewContext())
        {
            ctx.Orders.Add(new Order { Id = 100, Customer = "zed", CreatedAt = new DateTime(2026, 2, 1) });
            await ctx.SaveChangesAsync();
        }
        await using var reader = NewContext();
        Assert.Equal(11, await reader.Orders.CountAsync());
        Assert.Equal(10, await reader.Orders.AsOfSnapshot(first).CountAsync());
        Assert.False(await reader.Orders.AsOfSnapshot(first).AnyAsync(o => o.Customer == "zed"));
        var table = await TableAsync("orders");
        var at = DateTimeOffset.FromUnixTimeMilliseconds(table.Metadata.SnapshotById(first)!.TimestampMs);
        Assert.Equal(10, (await reader.Orders.AsOfSnapshot(at).ToListAsync()).Count);
        Assert.Equal(SnapshotSelector.Id(first), reader.Orders.Where(o => o.Id > 0).AsOfSnapshot(first).ToIcebergScanRequest().Snapshot);
        await table.CommitAsync([], [new Iceberg.Net.Metadata.MetadataUpdate.SetSnapshotRef("v1", Iceberg.Net.Metadata.SnapshotRef.Tag(first))]);
        Assert.Equal(10, await reader.Orders.AsOfRef("v1").CountAsync());
        Assert.Empty(reader.ChangeTracker.Entries());
    }

    [Fact]
    public async Task Strict_mode_refuses_residual_filters()
    {
        await SeedOrdersAsync(5);
        await using var ctx = NewContext(o => o.Strict());
        await Assert.ThrowsAsync<InvalidOperationException>(() => ctx.Orders.Where(o => o.Customer.Length == 3).ToListAsync());
        Assert.Equal(5, await ctx.Orders.Where(o => o.Id > 0).CountAsync());
    }

    [Fact]
    public async Task A_second_DbSet_in_a_query_is_read_by_its_own_scan_and_joined_in_memory()
    {
        await SeedOrdersAsync(6);
        await using (var setup = NewContext())
        {
            setup.Customers.AddRange(new Customer { Name = "alice", Region = "eu" }, new Customer { Name = "bob", Region = "us" });
            await setup.SaveChangesAsync();
        }
        await using var ctx = NewContext();
        var joined = await ctx.Orders.Join(ctx.Customers, o => o.Customer, c => c.Name, (o, c) => new { o.Id, c.Region }).ToListAsync();
        Assert.Equal([(1L, "us"), (3L, "eu"), (4L, "us"), (6L, "eu")], joined.Select(x => (x.Id, x.Region)).OrderBy(x => x.Id));
    }

    [Fact]
    public async Task Unsupported_operators_say_so()
    {
        await SeedOrdersAsync(3);
        await using var ctx = NewContext();
        Assert.ThrowsAny<NotSupportedException>(() => EF.CompileQuery((ShopContext c) => c.Orders.Where(o => o.Id > 1))(ctx).ToList());
    }
}
