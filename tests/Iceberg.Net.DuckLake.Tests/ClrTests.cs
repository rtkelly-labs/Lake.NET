using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>Reading rows as natural CLR values and as records (<see cref="DuckLakeClr"/>).</summary>
public sealed class ClrTests : IAsyncLifetime
{
    private readonly TestLake _lake = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    public sealed record Order(long Id, string? CustomerName, DateOnly Day, decimal? Amount)
    {
        public DateTimeOffset? PlacedAt { get; init; }
        public List<object?>? Tags { get; set; }
        public int? Quantity { get; set; } // int64 column into int?
    }

    [Fact]
    public async Task Rows_bind_to_records_by_column_name()
    {
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "orders",
            [
                new("id", DuckLakeType.Int64, Nullable: false), new("customer_name", DuckLakeType.Varchar), new("day", DuckLakeType.Date),
                new("amount", DuckLakeType.Decimal(10, 2)), new("placed_at", DuckLakeType.TimestampTz), new("tags", DuckLakeType.List(DuckLakeType.Varchar)),
                new("quantity", DuckLakeType.Int64), new("unused", DuckLakeType.Varchar),
            ]);
            await tx.InsertAsync("main", "orders",
            [
                [1L, "Ann", new DateOnly(2024, 5, 1), 12.5m, new DateTimeOffset(2024, 5, 1, 10, 0, 0, TimeSpan.Zero), new List<object?> { "a", null }, 3L, "x"],
                [2L, null, new DateOnly(1969, 12, 31), null, null, null, null, null],
            ]);
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "orders");
        var orders = new List<Order>();
        await foreach (var o in catalog.ReadAsync<Order>(table)) orders.Add(o);
        orders.Sort((a, b) => a.Id.CompareTo(b.Id));
        Assert.Equal(new Order(1, "Ann", new DateOnly(2024, 5, 1), 12.5m) { PlacedAt = new DateTimeOffset(2024, 5, 1, 10, 0, 0, TimeSpan.Zero), Tags = orders[0].Tags, Quantity = 3 }, orders[0]);
        Assert.Equal(["a", null], orders[0].Tags!);
        Assert.Equal(new Order(2, null, new DateOnly(1969, 12, 31), null), orders[1]);

        var filtered = new List<Order>();
        await foreach (var o in catalog.ReadAsync<Order>(table, Expr.Equal("id", 2L))) filtered.Add(o);
        Assert.Equal(2, Assert.Single(filtered).Id);
    }

    public sealed record NoMatch(long Missing);

    public sealed record NotNullable(long Id, DateOnly Day);

    [Fact]
    public async Task Binding_fails_clearly()
    {
        var catalog = await _lake.CreateAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.CreateTableAsync("main", "t", [new("id", DuckLakeType.Int64), new("day", DuckLakeType.Date)]);
            await tx.InsertAsync("main", "t", [[1L, null]]);
            await tx.CommitAsync();
        }
        var table = await catalog.LoadTableAsync("main", "t");
        Assert.Throws<ArgumentException>(() => DuckLakeClr.Binder<NoMatch>(table.Columns));
        var e = await Assert.ThrowsAsync<InvalidCastException>(async () => { await foreach (var _ in catalog.ReadAsync<NotNullable>(table)) { } });
        Assert.Contains("day", e.Message);
    }

    public static TheoryData<string, object, object> Values => new()
    {
        { "int8", -5, (sbyte)-5 },
        { "int16", 300, (short)300 },
        { "uint8", 200, (byte)200 },
        { "uint16", 60000, (ushort)60000 },
        { "uint32", 4_000_000_000L, 4_000_000_000u },
        { "uint64", new IcebergDecimal(ulong.MaxValue, 0), ulong.MaxValue },
        { "decimal(38,0)", new IcebergDecimal(System.Numerics.BigInteger.Pow(10, 37), 0), new IcebergDecimal(System.Numerics.BigInteger.Pow(10, 37), 0) },
        { "decimal(10,2)", new IcebergDecimal(1234, 2), 12.34m },
        { "date", -1, new DateOnly(1969, 12, 31) },
        { "time", 86_400_000_000L, TimeSpan.FromHours(24) },
        { "timestamp", 1_500_000L, new DateTime(1970, 1, 1, 0, 0, 1, 500, DateTimeKind.Unspecified) },
        { "timestamp_ms", -1000L, new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Unspecified) },
        { "timestamp_ns", 1_234L, new DateTime(1970, 1, 1).AddTicks(12) },
        { "timestamptz", 0L, DateTimeOffset.UnixEpoch },
        { "uuid", Guid.Empty, Guid.Empty },
        { "timetz", new DuckLakeTimeTz(1, 60), new DuckLakeTimeTz(1, 60) },
    };

    [Theory]
    [MemberData(nameof(Values))]
    public void Physical_values_become_CLR_values(string type, object physical, object clr)
    {
        var value = DuckLakeClr.ToClr(new DuckLakePrimitive(type), physical);
        Assert.Equal(clr, value);
        Assert.Equal(clr.GetType(), value!.GetType());
    }

    [Fact]
    public void Nested_values_become_lists_dictionaries_and_named_structs()
    {
        var type = DuckLakeType.Struct(("d", DuckLakeType.Date), ("m", DuckLakeType.Map(DuckLakeType.Varchar, DuckLakeType.List(DuckLakeType.UInt8))));
        var value = (Dictionary<string, object?>)DuckLakeClr.ToClr(type, new object?[] { 0, new Dictionary<object, object?> { ["k"] = new List<object?> { 1, null } } })!;
        Assert.Equal(new DateOnly(1970, 1, 1), value["d"]);
        Assert.Equal(new List<object?> { (byte)1, null }, ((Dictionary<object, object?>)value["m"]!)["k"]);
    }
}
