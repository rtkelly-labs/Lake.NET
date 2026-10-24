using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq.Tests;

public sealed class TypedWriteTests : CatalogTest
{

    private ReflectionRowMapper<Order> Mapper { get; } = new(Snake);

    private static List<long> Ids(IEnumerable<Order> orders) => orders.Select(o => o.Id).Order().ToList();

    [Fact]
    public async Task Insert_appends_mapped_rows()
    {
        var (table, orders) = await OrdersTableAsync();
        Assert.Equal(3, table.Metadata.Snapshots.Count);
        Assert.Equal(Ids(orders), Ids(await table.AsQueryable<Order>(SnakeQuery).ToListAsync()));
    }

    [Fact]
    public async Task Upsert_replaces_by_the_key_selector_or_the_identifier_fields()
    {
        var (table, orders) = await OrdersTableAsync();
        var changed = orders.Take(3).Select(o => { o.Customer = "zed"; return o; }).Append(new Order { Id = 100, Customer = "new", Day = new DateOnly(2024, 1, 1) }).ToList();
        var r = await table.UpsertAsync(changed, x => x.Id, mapper: Mapper);
        Assert.Equal(4, r.RowsInserted);
        var all = await table.AsQueryable<Order>(SnakeQuery).ToListAsync();
        Assert.Equal(41, all.Count);
        Assert.Equal([1L, 2L, 3L], Ids(all.Where(o => o.Customer == "zed")));

        // [IcebergKey] made Id the identifier field, so the key can be omitted; a composite selector also works.
        await table.UpsertAsync([new Order { Id = 100, Customer = "newer" }], mapper: Mapper);
        Assert.Equal("newer", (await table.AsQueryable<Order>(SnakeQuery).SingleAsync2(x => x.Id == 100)).Customer);
        await table.UpsertAsync([new Order { Id = 100, Customer = "newer", Qty = 9 }], x => new { x.Id, x.Customer }, mapper: Mapper);
        Assert.Equal(9, (await table.AsQueryable<Order>(SnakeQuery).SingleAsync2(x => x.Id == 100)).Qty);
    }

    [Fact]
    public async Task ExecuteDelete_pushes_a_translated_filter_down()
    {
        var (table, orders) = await OrdersTableAsync();
        var r = await table.AsQueryable<Order>(SnakeQuery).Where(x => x.Customer == "bob" || x.Qty > 5).ExecuteDeleteAsync();
        var expected = orders.Where(x => !(x.Customer == "bob" || x.Qty > 5)).ToList();
        Assert.Equal(orders.Count - expected.Count, r.RowsDeleted);
        Assert.Equal(Ids(expected), Ids(await table.AsQueryable<Order>(SnakeQuery).ToListAsync()));
    }

    [Fact]
    public async Task ExecuteDelete_with_a_residual_matches_rows_in_dotnet_instead_of_dropping_it()
    {
        var (table, orders) = await OrdersTableAsync();
        // Pushing down only "qty > 1" would delete too much; the residual must decide.
        var q = table.AsQueryable<Order>(SnakeQuery).Where(x => x.Qty > 1 && x.Customer.Length == 5);
        var r = await q.ExecuteDeleteAsync();
        var expected = orders.Where(x => !(x.Qty > 1 && x.Customer.Length == 5)).ToList();
        Assert.Equal(orders.Count - expected.Count, r.RowsDeleted);
        Assert.Equal(Ids(expected), Ids(await table.AsQueryable<Order>(SnakeQuery).ToListAsync()));

        var strict = table.AsQueryable<Order>(SnakeQuery with { Strict = true }).Where(x => x.Customer.Length == 3);
        await Assert.ThrowsAsync<InvalidOperationException>(() => strict.ExecuteDeleteAsync());
        await Assert.ThrowsAsync<NotSupportedException>(() => table.AsQueryable<Order>(SnakeQuery).Take(3).ExecuteDeleteAsync());
        await Assert.ThrowsAsync<NotSupportedException>(() => table.AsQueryable<Order>(SnakeQuery).AtSnapshot(table.Metadata.Snapshots[0].SnapshotId).ExecuteDeleteAsync());
    }

    [Fact]
    public async Task ExecuteDelete_with_a_residual_still_prunes_files_by_the_translated_part()
    {
        var warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-linq", Guid.NewGuid().ToString("N"));
        var io = new ReadCountingFileIO(new Iceberg.Net.IO.LocalFileIO());
        using var catalog = new InMemoryCatalog(warehouse, io);
        try
        {
            await catalog.CreateNamespaceAsync(Namespace.Of("l"));
            var table = await catalog.CreateTableAsync<Order>(NewId("orders"), Snake);
            var orders = SampleOrders();
            foreach (var chunk in orders.Chunk(14)) await table.InsertAsync(chunk, Mapper);
            io.DataFilesOpened.Clear();

            // id <= 10 lives in the first of three files; the residual (string length) is decided per row.
            var r = await table.AsQueryable<Order>(SnakeQuery).Where(x => x.Id <= 10 && x.Customer.Length == 5).ExecuteDeleteAsync();
            var expected = orders.Where(x => !(x.Id <= 10 && x.Customer.Length == 5)).ToList();
            Assert.Equal(orders.Count - expected.Count, r.RowsDeleted);
            Assert.Single(io.DataFilesOpened.Distinct());
            Assert.Equal(Ids(expected), Ids(await table.AsQueryable<Order>(SnakeQuery).ToListAsync()));
        }
        finally
        {
            if (Directory.Exists(warehouse)) Directory.Delete(warehouse, recursive: true);
        }
    }

    [Fact]
    public async Task Update_setters_translate_into_row_update_assignments()
    {
        var (table, orders) = await OrdersTableAsync();
        var bonus = 10;
        var set = IcebergWrites.CompileSetters<Order>(table,
            s => s.SetProperty(x => x.Customer, "closed").SetProperty(x => x.Qty, x => x.Qty + bonus).SetProperty(x => x.Status, Status.Pending)
                .SetProperty(x => x.Amount, x => x.Amount * 2).SetProperty(x => x.Note, x => null), Mapper);
        var update = new RowUpdate();
        set(update);
        Assert.Equal(["customer", "qty", "status", "amount", "note"], update.Assignments.Select(a => a.Column));

        var binding = Mapper.Bind(table.Schema);
        var row = binding.ToRow(orders[4]); // Id 5: qty 5, amount 50.25
        var values = update.Assignments.Select(a => a.Value(row)).ToList();
        Assert.Equal("closed", values[0]);
        Assert.Equal(15, values[1]);
        Assert.Equal(3, values[2]); // enums are stored as their value
        Assert.Equal(new IcebergDecimal(10050, 2), values[3]);
        Assert.Null(values[4]);

        Assert.Throws<NotSupportedException>(() => IcebergWrites.CompileSetters<Order>(table, s => s.SetProperty(x => x.Location!.Lat, 1.0), Mapper)(new RowUpdate()));
        Assert.Throws<ArgumentException>(() => IcebergWrites.CompileSetters<Order>(table, s => s.SetProperty(x => x.Qty, 1).SetProperty(x => x.Qty, 2), Mapper));
    }

    [Fact]
    public async Task ExecuteUpdate_refuses_residual_filters()
    {
        var (table, _) = await OrdersTableAsync();
        await Assert.ThrowsAsync<NotSupportedException>(() => table.AsQueryable<Order>(SnakeQuery).Where(x => x.Customer.Length == 3)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Qty, 0)));
    }

    [Fact]
    public async Task ExecuteUpdate_changes_matching_rows()
    {
        var (table, orders) = await OrdersTableAsync();
        var r = await table.AsQueryable<Order>(SnakeQuery).Where(x => x.Customer == "bob")
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Note, "closed").SetProperty(x => x.Qty, x => x.Qty + 1));
        var bob = orders.Where(o => o.Customer == "bob").ToList();
        Assert.Equal(bob.Count, r.RowsUpdated);
        var after = (await table.AsQueryable<Order>(SnakeQuery).Where(x => x.Customer == "bob").ToListAsync()).OrderBy(o => o.Id).ToList();
        Assert.All(after, o => Assert.Equal("closed", o.Note));
        Assert.Equal(bob.Select(o => o.Qty + 1), after.Select(o => o.Qty));
        Assert.Equal(orders.Count, await table.AsQueryable<Order>(SnakeQuery).CountAsync());
    }

    [Fact]
    public async Task Merge_updates_matches_inserts_the_rest_and_deletes_by_source()
    {
        var (table, orders) = await OrdersTableAsync();
        var source = new List<Order>
        {
            new() { Id = 1, Customer = "alice", Qty = 100 },
            new() { Id = 2, Customer = "x", Qty = 1 },
            new() { Id = 500, Customer = "new", Qty = 5 },
        };
        var r = await table.MergeAsync(source, on: x => x.Id, m => m
            .WhenMatchedDelete(when: (t, s) => s.Customer == "x")
            .WhenMatchedUpdate(s => s.SetProperty(x => x.Qty, (t, src) => t.Qty + src.Qty).SetProperty(x => x.Note, "merged"))
            .WhenNotMatchedInsert(), mapper: Mapper);
        Assert.Equal((1, 1, 1), (r.RowsUpdated, r.RowsDeleted, r.RowsInserted));
        var all = (await table.AsQueryable<Order>(SnakeQuery).ToListAsync()).ToDictionary(o => o.Id);
        Assert.Equal(orders[0].Qty + 100, all[1].Qty);
        Assert.Equal("merged", all[1].Note);
        Assert.False(all.ContainsKey(2));
        Assert.Equal("new", all[500].Customer);

        await table.MergeAsync(source.Take(1), on: x => x.Id, m => m.WhenMatchedUpdateAll().WhenNotMatchedBySourceDelete(x => x.Qty > 3), mapper: Mapper);
        all = (await table.AsQueryable<Order>(SnakeQuery).ToListAsync()).ToDictionary(o => o.Id);
        Assert.Equal(100, all[1].Qty);
        Assert.DoesNotContain(all.Values, o => o.Id != 1 && o.Qty > 3);
    }

    [Fact]
    public async Task Merge_by_source_filters_must_translate()
    {
        var (table, _) = await OrdersTableAsync();
        await Assert.ThrowsAsync<NotSupportedException>(() => table.MergeAsync(SampleOrders().Take(1), x => x.Id,
            m => m.WhenNotMatchedBySourceDelete(x => x.Customer.Length > 2), mapper: Mapper));
    }
}

internal static class QueryTestExtensions
{
    public static Task<T> SingleAsync2<T>(this IQueryable<T> q, System.Linq.Expressions.Expression<Func<T, bool>> predicate) => q.Where(predicate).SingleAsync();
}

/// <summary>Records which data files a scan opens.</summary>
internal sealed class ReadCountingFileIO(Iceberg.Net.IO.IFileIO inner) : Iceberg.Net.IO.IFileIO
{
    public List<string> DataFilesOpened { get; } = [];

    private void Note(string l)
    {
        if (l.EndsWith(".parquet", StringComparison.Ordinal)) lock (DataFilesOpened) DataFilesOpened.Add(l);
    }

    public Task<byte[]> ReadAllAsync(string l, CancellationToken ct = default) { Note(l); return inner.ReadAllAsync(l, ct); }
    public Task<Stream> OpenReadAsync(string l, CancellationToken ct = default) { Note(l); return inner.OpenReadAsync(l, ct); }
    public Task<byte[]> ReadRangeAsync(string l, long offset, int length, CancellationToken ct = default) { Note(l); return inner.ReadRangeAsync(l, offset, length, ct); }
    public Task<long> LengthAsync(string l, CancellationToken ct = default) => inner.LengthAsync(l, ct);
    public Task<bool> ExistsAsync(string l, CancellationToken ct = default) => inner.ExistsAsync(l, ct);
    public Task WriteAsync(string l, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default) => inner.WriteAsync(l, content, overwrite, ct);
    public Task DeleteAsync(string l, CancellationToken ct = default) => inner.DeleteAsync(l, ct);
    public void Dispose() => inner.Dispose();
}
