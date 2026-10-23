using System.Linq.Expressions;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq.Tests;

/// <summary>
/// The query matrix: for every predicate shape, the rows equal LINQ to Objects over the inserted rows (the oracle), and
/// the scan filter is exactly the expected pushdown — with a residual where the predicate only partly translates.
/// </summary>
[Spec("Scan Planning")]
public sealed class QueryTranslationTests : CatalogTest
{
    private static readonly long[] SomeIds = [3, 5, 8, 99];
    private static readonly List<int?> SomeDiscounts = [1, null];
    private static readonly HashSet<string> SomeCustomers = ["bob", "al"];

    /// <summary>(name, predicate, expected filter text, fully translated).</summary>
    public static TheoryData<string, Expression<Func<Order, bool>>, string, bool> Predicates()
    {
        var qty = 3;
        var since = new DateOnly(2024, 1, 20);
        var created = new DateTime(2024, 1, 5, 0, 0, 0);
        var shipped = new DateTimeOffset(2024, 2, 2, 0, 0, 0, TimeSpan.FromHours(2));
        var d = new TheoryData<string, Expression<Func<Order, bool>>, string, bool>
        {
            // comparisons, both sides, constants and captured variables
            { "eq", x => x.Id == 5, "id == 5", true },
            { "eq-flipped", x => 5 == x.Id, "id == 5", true },
            { "ne", x => x.Customer != "bob", "customer != 'bob'", true },
            { "lt-captured", x => x.Qty < qty, "qty < 3", true },
            { "le", x => x.Qty <= 3, "qty <= 3", true },
            { "gt-flipped", x => 3 > x.Qty, "qty < 3", true },
            { "ge", x => x.Price >= 10.0, "price >= 10", true },
            { "decimal", x => x.Amount > 100.5m, "amount > 100.50", true },
            { "date", x => x.Day >= since, "day >= 19742", true },
            { "timestamp", x => x.CreatedAt < created, "created_at < 1704412800000000", true },
            { "timestamptz-offset", x => x.ShippedAt > shipped, "shipped_at > 1706824800000000", true },
            { "guid", x => x.Ref == new Guid(7, 0, 0, new byte[8]), "ref == 00000007-0000-0000-0000-000000000000", true },
            // nullable members: lifted comparisons, null checks, HasValue
            { "nullable-eq", x => x.Discount == 2, "discount == 2", true },
            { "nullable-ne", x => x.Discount != 2, "discount != 2", true },
            { "nullable-lt", x => x.Discount < 2, "discount < 2", true },
            { "is-null", x => x.Note == null, "note is null", true },
            { "not-null", x => x.Note != null, "note is not null", true },
            { "has-value", x => x.Discount.HasValue, "discount is not null", true },
            { "not-has-value", x => !x.Discount.HasValue, "discount is null", true },
            { "value", x => x.Discount.HasValue && x.Discount.Value > 2, "(discount is not null and discount > 2)", true },
            // bool members
            { "bool", x => x.Active, "active == True", true },
            { "not-bool", x => !x.Active, "active != True", true },
            { "bool-eq-false", x => x.Active == false, "active == False", true },
            // enums: stored as int (ordered) and as names (equality only)
            { "enum-int", x => x.Status == Status.Closed, "status == 2", true },
            { "enum-int-ordered", x => x.Status >= Status.Closed, "status >= 2", true },
            { "enum-name", x => x.Stage == Status.Pending, "stage == 'Pending'", true },
            { "enum-name-ordered", x => x.Stage > Status.Open, "true", false },
            // In / StartsWith / IsNullOrEmpty
            { "contains-array", x => SomeIds.Contains(x.Id), "id in (3, 5, 8, 99)", true },
            { "contains-list-null", x => SomeDiscounts.Contains(x.Discount), "(discount in (1) or discount is null)", true },
            { "contains-set", x => SomeCustomers.Contains(x.Customer), "customer in ('bob', 'al')", true },
            { "contains-empty", x => Array.Empty<long>().Contains(x.Id), "false", true },
            { "starts-with", x => x.Customer.StartsWith("al"), "customer starts with 'al'", true },
            { "starts-with-ordinal", x => x.Customer.StartsWith("a", StringComparison.Ordinal), "customer starts with 'a'", true },
            { "null-or-empty", x => string.IsNullOrEmpty(x.Note), "(note is null or note == '')", true },
            { "equals-method", x => x.Customer.Equals("dave"), "customer == 'dave'", true },
            // nested struct field
            { "nested", x => x.Location != null && x.Location.Lat > 30, "location.lat > 30", false },
            // boolean structure
            { "and", x => x.Qty > 2 && x.Active, "(qty > 2 and active == True)", true },
            { "or", x => x.Qty == 1 || x.Customer == "bob", "(qty == 1 or customer == 'bob')", true },
            { "not-and", x => !(x.Qty > 2 && x.Customer == "al"), "(qty <= 2 or customer != 'al')", true },
            { "not-lt-nullable", x => !(x.Discount < 2), "(discount >= 2 or discount is null)", true },
            { "not-lt-double", x => !(x.Price < 10), "(price >= 10 or price is nan)", true },
            { "const-true", x => true, "true", true },
            // untranslatable → residual (and never an unsound pushdown)
            { "method", x => x.Customer.Length > 3, "true", false },
            { "member-vs-member", x => x.Qty > x.Discount, "true", false },
            { "computed", x => x.Label.EndsWith("#5"), "true", false },
            { "or-half-untranslatable", x => x.Qty == 1 || x.Customer.Length == 5, "true", false },
            { "and-half-untranslatable", x => x.Qty == 1 && x.Customer.Length == 5, "qty == 1", false },
            { "not-of-residual", x => !(x.Qty == 1 && x.Customer.Length == 5), "true", false },
            { "not-of-or-residual", x => !(x.Qty == 1 || x.Customer.Length == 5), "true", false },
            { "inexact-constant", x => x.Qty < 2.5, "true", false },
            { "sub-microsecond", x => x.CreatedAt < created.AddTicks(1), "true", false },
            { "decimal-too-precise", x => x.Amount == 10.051m, "true", false },
            { "or-inside-and", x => x.Active && (x.Qty == 1 || x.Customer.Contains('o')), "active == True", false },
        };
        return d;
    }

    [Theory]
    [MemberData(nameof(Predicates))]
    public async Task Where_matches_linq_to_objects_and_pushes_down_what_it_can(string name, Expression<Func<Order, bool>> predicate, string filter, bool exact)
    {
        var (table, orders) = await Shared();
        var query = table.AsQueryable<Order>(SnakeQuery).Where(predicate);
        var request = query.ToScanRequest();
        Assert.Equal(filter, request.Filter.ToString());
        Assert.Equal(!exact, query.ToQueryString().Contains("residual"));

        var expected = orders.AsQueryable().Where(predicate).Select(o => o.Id).Order().ToList();
        Assert.Equal(expected, query.AsEnumerable().Select(o => o.Id).Order().ToList());
        Assert.Equal(expected, (await query.ToListAsync()).Select(o => o.Id).Order().ToList());
        Assert.NotEmpty(name);
    }

    [Fact]
    public async Task Strict_mode_throws_for_filters_that_would_run_in_memory()
    {
        var (table, _) = await Shared();
        var strict = SnakeQuery with { Strict = true };
        var q = table.AsQueryable<Order>(strict).Where(x => x.Customer.Length > 3);
        Assert.Throws<InvalidOperationException>(() => q.ToList());
        await Assert.ThrowsAsync<InvalidOperationException>(() => q.ToListAsync());
        Assert.Throws<InvalidOperationException>(() => table.AsQueryable<Order>(strict).Take(3).Where(x => x.Id > 1).ToList());
        // Fully translated filters, sorting and paging are fine.
        Assert.Equal([2L, 3L], table.AsQueryable<Order>(strict).Where(x => x.Id > 1).OrderBy(x => x.Id).Take(2).Select(x => x.Id).ToList());
    }

    [Fact]
    [Spec("Column Projection")]
    public async Task Projection_reads_only_the_columns_used()
    {
        var (table, orders) = await Shared();
        var q = table.AsQueryable<Order>(SnakeQuery);

        var anon = q.Where(x => x.Qty > 4).Select(x => new { x.Id, Who = x.Customer.ToUpperInvariant() });
        Assert.Equal(["id", "customer"], anon.ToScanRequest().Columns);
        Assert.Equal(orders.Where(x => x.Qty > 4).Select(x => new { x.Id, Who = x.Customer.ToUpperInvariant() }).OrderBy(a => a.Id), anon.ToList().OrderBy(a => a.Id));

        // Residual members are read too; ordering keys before the projection as well.
        var dto = q.Where(x => x.Note != null && x.Note.EndsWith("7")).OrderBy(x => x.Price).Select(x => new IdAndDay(x.Id, x.Day));
        Assert.Equal(["id", "note", "price", "day"], dto.ToScanRequest().Columns);
        Assert.Equal(orders.Where(x => x.Note?.EndsWith("7") == true).OrderBy(x => x.Price).Select(x => new IdAndDay(x.Id, x.Day)), await dto.ToListAsync());

        // A nested member reads its top-level column.
        Assert.Equal(["location"], q.Select(x => x.Location!.Lat).ToScanRequest().Columns);
        // Counting needs no columns; the whole row escapes otherwise.
        Assert.Equal([], ColumnsOfCount(q.Where(x => x.Id > 3))!);
        Assert.Equal(orders.Count(x => x.Id > 3), q.Count(x => x.Id > 3));
        Assert.Null(q.Where(x => x.Id > 3).ToScanRequest().Columns);
        Assert.Null(q.Select(x => x).ToScanRequest().Columns);
        Assert.Null(q.Select(x => x.Label).ToScanRequest().Columns); // computed member: may read anything
    }

    private static IReadOnlyList<string>? ColumnsOfCount(IQueryable<Order> q) =>
        // Count() is a terminal; inspect the plan the same way Execute does.
        ((QuerySource<Order>)IcebergQueryableExtensions.Root(q)).Plan(Expression.Call(typeof(Queryable), "Count", [typeof(Order)], q.Expression)).Request.Columns;

    public sealed record IdAndDay(long Id, DateOnly Day);

    [Fact]
    public async Task Terminals_and_in_memory_operators_match_linq_to_objects()
    {
        var (table, orders) = await Shared();
        var q = table.AsQueryable<Order>(SnakeQuery);
        Assert.Equal(orders.Count, q.Count());
        Assert.Equal(orders.Count, await q.CountAsync());
        Assert.Equal(orders.LongCount(x => x.Active), q.LongCount(x => x.Active));
        Assert.Equal(orders.Count(x => x.Qty > 3), await q.CountAsync(x => x.Qty > 3));
        Assert.True(q.Any(x => x.Customer == "carol"));
        Assert.False(await q.AnyAsync(x => x.Id > 1000));
        Assert.True(await q.AnyAsync());
        Assert.Equal(7, q.First(x => x.Id == 7).Id);
        Assert.Equal(7, (await q.FirstAsync(x => x.Id == 7)).Id);
        Assert.Null(q.FirstOrDefault(x => x.Id == 700));
        Assert.Null(await q.FirstOrDefaultAsync(x => x.Id == 700));
        Assert.Equal(9, q.Single(x => x.Id == 9).Id);
        Assert.Equal(9, (await q.Where(x => x.Id == 9).SingleAsync()).Id);
        Assert.Throws<InvalidOperationException>(() => q.Single(x => x.Id > 1));
        await Assert.ThrowsAsync<InvalidOperationException>(() => q.Where(x => x.Id > 1).SingleOrDefaultAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => q.Where(x => x.Id > 100).FirstAsync());

        Assert.Equal(orders.OrderByDescending(x => x.Price).ThenBy(x => x.Id).Skip(3).Take(5).Select(x => x.Id),
            q.OrderByDescending(x => x.Price).ThenBy(x => x.Id).Skip(3).Take(5).Select(x => x.Id).ToList());
        Assert.Equal(orders.OrderBy(x => x.Customer).ThenByDescending(x => x.Id).Select(x => x.Id),
            await q.OrderBy(x => x.Customer).ThenByDescending(x => x.Id).Select(x => x.Id).ToArrayAsync());
        // Where after OrderBy still pushes down (it commutes); after Take it must not.
        var sorted = q.OrderBy(x => x.Id).Where(x => x.Qty == 2);
        Assert.Equal("qty == 2", sorted.ToScanRequest().Filter.ToString());
        Assert.Equal(orders.Where(x => x.Qty == 2).Select(x => x.Id), sorted.Select(x => x.Id).ToList());
        var paged = q.OrderBy(x => x.Id).Take(10).Where(x => x.Qty == 2);
        Assert.Equal("true", paged.ToScanRequest().Filter.ToString());
        Assert.Equal(orders.Take(10).Where(x => x.Qty == 2).Select(x => x.Id), (await paged.ToListAsync()).Select(x => x.Id));
        Assert.Equal(orders.GroupBy(x => x.Customer).Select(g => new { g.Key, N = g.Count() }).OrderBy(g => g.Key),
            q.GroupBy(x => x.Customer).Select(g => new { g.Key, N = g.Count() }).OrderBy(g => g.Key).ToList());
        Assert.Equal(orders.Max(x => x.Amount), q.Max(x => x.Amount));
        Assert.Equal(orders.Where(x => x.Active).Sum(x => x.Qty), q.Where(x => x.Active).Sum(x => x.Qty));

        var streamed = new List<long>();
        await foreach (var o in q.Where(x => x.Qty > 1).Select(x => x.Id).Skip(2).Take(3).AsAsyncEnumerable()) streamed.Add(o);
        // Without OrderBy the order is the scan's (file order), so the oracle runs over the provider's own full read.
        var scanned = q.ToList();
        Assert.Equal(scanned.Where(x => x.Qty > 1).Select(x => x.Id).Skip(2).Take(3), streamed);
    }

    [Fact]
    public async Task Take_and_First_stop_the_scan_early()
    {
        var (table, _) = await Shared();
        var counting = new CountingMapper<Order>(new ReflectionRowMapper<Order>(Snake));
        var q = table.AsQueryable(counting, SnakeQuery);
        Assert.Equal(2, q.Take(2).ToList().Count);
        Assert.InRange(counting.Rows, 2, 3);
        counting.Rows = 0;
        Assert.Equal(2, (await q.Take(2).ToListAsync()).Count);
        Assert.InRange(counting.Rows, 2, 3);
        counting.Rows = 0;
        Assert.NotNull(await q.FirstOrDefaultAsync());
        Assert.InRange(counting.Rows, 1, 2);
        counting.Rows = 0;
        Assert.True(q.Any());
        Assert.Equal(1, counting.Rows);
    }

    [Fact]
    public async Task Debug_string_shows_the_split()
    {
        var (table, _) = await Shared();
        var text = table.AsQueryable<Order>(SnakeQuery).Where(x => x.Qty > 2 && x.Customer.Length > 3).OrderBy(x => x.Id).Select(x => x.Id).ToQueryString();
        Assert.Contains("filter:    qty > 2", text);
        Assert.Contains("residual:  x => (x.Customer.Length > 3)", text);
        Assert.Contains("in memory: OrderBy(x => x.Id) -> Select(x => x.Id)", text);
        Assert.Contains("columns:   [id, customer]", text);
    }

    [Fact]
    public async Task Non_nullable_members_over_optional_columns_treat_null_as_the_default()
    {
        // qty is optional in the table but an int in the model: a null reads as 0, so "Qty == 0" must keep it.
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "qty", PrimitiveType.Int), NestedField.Optional(3, "price", PrimitiveType.Double)]);
        var table = await Catalog.CreateTableAsync(new CreateTableRequest(NewId(), schema));
        await TableMutations.InsertAsync(table, [[1L, null, null], [2L, 0, 0.0], [3L, 5, double.NaN], [4L, -1, -1.0]]);
        var rows = new List<Plain> { new(1, 0, 0), new(2, 0, 0), new(3, 5, double.NaN), new(4, -1, -1) };
        var q = table.AsQueryable<Plain>();
        var cases = new Expression<Func<Plain, bool>>[]
        {
            x => x.qty == 0, x => x.qty != 0, x => x.qty < 1, x => x.qty > -1, x => !(x.qty > -1), x => new[] { 0, 5 }.Contains(x.qty),
            x => x.price == 0, x => x.price != 0, x => !(x.price < 0), x => x.price >= 0,
        };
        foreach (var c in cases)
        {
            var expected = rows.AsQueryable().Where(c).Select(r => r.id).Order().ToList();
            Assert.True(expected.SequenceEqual(q.Where(c).Select(r => r.id).ToList().Order()), $"{c}: {q.Where(c).ToScanRequest().Filter}");
            Assert.DoesNotContain("residual", q.Where(c).ToQueryString());
        }
        Assert.Equal("(qty == 0 or qty is null)", q.Where(x => x.qty == 0).ToScanRequest().Filter.ToString());
        Assert.Equal("(qty != 0 and qty is not null)", q.Where(x => x.qty != 0).ToScanRequest().Filter.ToString());
    }

    public sealed record Plain(long id, int qty, double price);

    // One table shared by the read-only tests of a class instance (xunit creates an instance per test, so cache statically).
    private static readonly SemaphoreSlim Gate = new(1, 1);
    private static (Table, List<Order>)? _shared;
    private static readonly string SharedWarehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-linq-shared", Guid.NewGuid().ToString("N"));
    private static InMemoryCatalog? _sharedCatalog;

    private async Task<(Table, List<Order>)> Shared()
    {
        await Gate.WaitAsync();
        try
        {
            if (_shared is null)
            {
                _sharedCatalog = new InMemoryCatalog(SharedWarehouse);
                await _sharedCatalog.CreateNamespaceAsync(Namespace.Of("l"));
                var table = await _sharedCatalog.CreateTableAsync<Order>(NewId("orders"), Snake);
                var orders = SampleOrders();
                foreach (var chunk in orders.Chunk(14)) await table.InsertAsync(chunk, new ReflectionRowMapper<Order>(Snake));
                _shared = (table, orders);
            }
            return _shared.Value;
        }
        finally
        {
            Gate.Release();
        }
    }
}

/// <summary>Counts rows materialized, to prove early termination.</summary>
public sealed class CountingMapper<T>(IIcebergRowMapper<T> inner) : IIcebergRowMapper<T>
{
    public int Rows { get; set; }

    public IIcebergRowBinding<T> Bind(Schema schema, NameMapping? nameMapping = null) => new Binding(this, inner.Bind(schema, nameMapping));

    private sealed class Binding(CountingMapper<T> owner, IIcebergRowBinding<T> inner) : IIcebergRowBinding<T>
    {
        public Schema Schema => inner.Schema;
        public IReadOnlyList<IcebergMemberBinding> Members => inner.Members;

        public Func<object?[], T> CreateReader(Schema rowSchema)
        {
            var read = inner.CreateReader(rowSchema);
            return row =>
            {
                owner.Rows++;
                return read(row);
            };
        }

        public object?[] ToRow(T item) => inner.ToRow(item);
    }
}
