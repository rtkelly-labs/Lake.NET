using Iceberg.Net.Catalog;
using Iceberg.Net.Data;

namespace Iceberg.Net.Linq.Tests;

public enum Status { Open = 1, Closed = 2, Pending = 3 }

public sealed record Location(double Lat, double Lon);

/// <summary>The query-matrix model: nullable and non-nullable scalars, enums both ways, a struct column.</summary>
public sealed class Order
{
    [IcebergKey] public long Id { get; set; }
    public string Customer { get; set; } = "";
    public string? Note { get; set; }
    public int Qty { get; set; }
    public int? Discount { get; set; }
    public double Price { get; set; }
    [IcebergField(Type = "decimal(12, 2)")] public decimal Amount { get; set; }
    public bool Active { get; set; }
    public Status Status { get; set; }
    [IcebergField(Type = "string")] public Status? Stage { get; set; }
    public DateOnly Day { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTimeOffset? ShippedAt { get; set; }
    public Guid Ref { get; set; }
    public Location? Location { get; set; }

    /// <summary>Computed: not a column.</summary>
    public string Label => $"{Customer}#{Id}";

    public override string ToString() => $"Order {Id}";
}

/// <summary>Shared temp-warehouse catalog.</summary>
public abstract class CatalogTest : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-linq", Guid.NewGuid().ToString("N"));

    protected CatalogTest()
    {
        Catalog = new InMemoryCatalog(_warehouse);
        Catalog.CreateNamespaceAsync(Namespace.Of("l")).GetAwaiter().GetResult();
    }

    protected InMemoryCatalog Catalog { get; }

    protected static TableIdentifier NewId(string prefix = "t") => TableIdentifier.Of("l", $"{prefix}{Guid.NewGuid():N}");

    public void Dispose()
    {
        Catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
        GC.SuppressFinalize(this);
    }

    protected static readonly IcebergMapperOptions Snake = new() { NameConverter = IcebergNaming.SnakeCase };
    protected static readonly IcebergQueryOptions SnakeQuery = new() { Mapping = Snake };

    /// <summary>40 orders over three commits (three data files), with nulls, NaN-free doubles and every enum value.</summary>
    protected static List<Order> SampleOrders()
    {
        var customers = new[] { "alice", "bob", "carol", "dave", "al" };
        var list = new List<Order>();
        for (var i = 1; i <= 40; i++)
        {
            list.Add(new Order
            {
                Id = i,
                Customer = customers[i % customers.Length],
                Note = i % 3 == 0 ? null : $"note {i}",
                Qty = i % 7,
                Discount = i % 4 == 0 ? null : i % 5,
                Price = i * 1.25,
                Amount = i * 10.05m,
                Active = i % 2 == 0,
                Status = (Status)(i % 3 + 1),
                Stage = i % 5 == 0 ? null : (Status)(i % 3 + 1),
                Day = new DateOnly(2024, 1, 1).AddDays(i),
                CreatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Unspecified).AddHours(i * 7),
                ShippedAt = i % 6 == 0 ? null : new DateTimeOffset(2024, 2, 1, 0, 0, 0, TimeSpan.Zero).AddMinutes(i * 90),
                Ref = new Guid(i, 0, 0, new byte[8]),
                Location = i % 8 == 0 ? null : new Location(i * 2.0, -i),
            });
        }
        return list;
    }

    protected async Task<(Table Table, List<Order> Orders)> OrdersTableAsync()
    {
        var table = await Catalog.CreateTableAsync<Order>(NewId("orders"), Snake);
        var orders = SampleOrders();
        foreach (var chunk in orders.Chunk(14)) await table.InsertAsync(chunk, new ReflectionRowMapper<Order>(Snake));
        return (table, orders);
    }
}
