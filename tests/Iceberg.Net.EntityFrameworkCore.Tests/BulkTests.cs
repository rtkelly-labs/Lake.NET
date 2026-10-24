using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.EntityFrameworkCore.Tests;

public sealed class BulkTests : EfTest
{
    [Fact]
    public async Task ExecuteDelete_deletes_matching_rows_in_one_snapshot()
    {
        var orders = await SeedOrdersAsync();
        await using var ctx = NewContext();
        Assert.Equal(orders.Count(o => o.Customer == "bob"), await ctx.Orders.Where(o => o.Customer == "bob").ExecuteDeleteAsync());
        // a residual (untranslatable) filter still deletes exactly the matching rows
        Assert.Equal(orders.Count(o => o.Customer != "bob" && o.Qty > 2 && o.Customer.Length == 5),
            ctx.Orders.Where(o => o.Qty > 2 && o.Customer.Length == 5).ExecuteDelete());
        var left = orders.Where(o => o.Customer != "bob" && !(o.Qty > 2 && o.Customer.Length == 5)).Select(o => o.Id).Order();
        Assert.Equal(left, (await ctx.Orders.AsNoTracking().Select(o => o.Id).ToListAsync()).Order());
        Assert.Equal(3, (await TableAsync("orders")).Metadata.Snapshots.Count);
    }

    [Fact]
    public async Task ExecuteUpdate_maps_setters_onto_the_linq_layer()
    {
        var orders = await SeedOrdersAsync();
        await using var ctx = NewContext();
        var suffix = "!";
        var n = await ctx.Orders.Where(o => o.Status == OrderStatus.Open)
            .ExecuteUpdateAsync(s => s.SetProperty(o => o.Qty, o => o.Qty + 100).SetProperty(o => o.Note, o => o.Customer + suffix));
        Assert.Equal(orders.Count(o => o.Status == OrderStatus.Open), n);
        Assert.Equal(1, ctx.Orders.Where(o => o.Id == 1).ExecuteUpdate(s => s.SetProperty(o => o.Customer, "renamed")));

        var all = await ctx.Orders.AsNoTracking().ToListAsync();
        foreach (var o in all)
        {
            var before = orders.Single(x => x.Id == o.Id);
            Assert.Equal(before.Status == OrderStatus.Open ? before.Qty + 100 : before.Qty, o.Qty);
            Assert.Equal(before.Status == OrderStatus.Open ? before.Customer + "!" : before.Note, o.Note);
        }
        Assert.Equal("renamed", all.Single(o => o.Id == 1).Customer);
        await Assert.ThrowsAsync<NotSupportedException>(() => ctx.Orders.Where(o => o.Customer.Length == 3).ExecuteUpdateAsync(s => s.SetProperty(o => o.Qty, 0)));
    }
}
