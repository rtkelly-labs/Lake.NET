using System;
using System.Linq;
using System.Threading.Tasks;
using Iceberg.Net.Linq;
using Iceberg.Net.Types;
using Xunit;

namespace Iceberg.Net.Linq.Tests;

[IcebergMappable]
public partial record GeneratedCustomer(
    [property: IcebergField(1)] int Id,
    [property: IcebergField(2)] string Name,
    [property: IcebergField(3)] string? Email,
    [property: IcebergField(4)] decimal Balance
);

[IcebergMappable]
public partial class GeneratedOrder
{
    [IcebergField(10)]
    public long OrderId { get; set; }

    [IcebergField(20, Name = "customer_id")]
    public int CustomerId { get; set; }

    [IcebergField(30)]
    public string? Description { get; set; }
}

public class SourceGeneratorTests : CatalogTest
{
    [Fact]
    public async Task GeneratedModel_EndToEnd_CreateTable_Insert_And_Query()
    {
        var table = await Catalog.CreateTableAsync<GeneratedCustomer>(NewId());
        var customers = new[]
        {
            new GeneratedCustomer(1, "Alice", "alice@example.com", 100m),
            new GeneratedCustomer(2, "Bob", null, 250.5m),
        };

        await table.InsertAsync(customers);

        var queryable = table.AsQueryable<GeneratedCustomer>();
        var results = (await queryable.ToListAsync()).OrderBy(c => c.Id).ToList();

        Assert.Equal(2, results.Count);
        var alice = results[0];
        Assert.Equal("Alice", alice.Name);
        Assert.Equal("alice@example.com", alice.Email);
        Assert.Equal(100m, alice.Balance);

        var bob = results[1];
        Assert.Equal("Bob", bob.Name);
        Assert.Null(bob.Email);
        Assert.Equal(250.5m, bob.Balance);
    }

    [Fact]
    public void GeneratedRecord_ImplementsIIcebergMappable()
    {
        Assert.True(typeof(IIcebergMappable<GeneratedCustomer>).IsAssignableFrom(typeof(GeneratedCustomer)));
    }

    [Fact]
    public void GeneratedRecord_HasCorrectStaticIcebergSchema()
    {
        var schema = GeneratedCustomer.IcebergSchema;
        Assert.NotNull(schema);
        Assert.Equal(4, schema.Columns.Count);

        var idCol = schema.Columns.First(c => c.Id == 1);
        Assert.Equal("Id", idCol.Name);
        Assert.True(idCol.Required);
        Assert.Equal(PrimitiveType.Int, idCol.Type);

        var nameCol = schema.Columns.First(c => c.Id == 2);
        Assert.Equal("Name", nameCol.Name);
        Assert.True(nameCol.Required);
        Assert.Equal(PrimitiveType.String, nameCol.Type);

        var emailCol = schema.Columns.First(c => c.Id == 3);
        Assert.Equal("Email", emailCol.Name);
        Assert.True(emailCol.IsOptional);
        Assert.Equal(PrimitiveType.String, emailCol.Type);

        var balanceCol = schema.Columns.First(c => c.Id == 4);
        Assert.Equal("Balance", balanceCol.Name);
        Assert.True(balanceCol.Required);
        Assert.IsType<DecimalType>(balanceCol.Type);
    }

    [Fact]
    public void GeneratedClass_HasCustomNamesAndProperties()
    {
        var schema = GeneratedOrder.IcebergSchema;
        Assert.NotNull(schema);
        Assert.Equal(3, schema.Columns.Count);

        var orderIdCol = schema.Columns.First(c => c.Id == 10);
        Assert.Equal("OrderId", orderIdCol.Name);
        Assert.Equal(PrimitiveType.Long, orderIdCol.Type);

        var custIdCol = schema.Columns.First(c => c.Id == 20);
        Assert.Equal("customer_id", custIdCol.Name);
        Assert.Equal(PrimitiveType.Int, custIdCol.Type);

        var descCol = schema.Columns.First(c => c.Id == 30);
        Assert.Equal("Description", descCol.Name);
        Assert.True(descCol.IsOptional);
    }

    [Fact]
    public void IcebergRowMappers_SelectsGeneratedMapper()
    {
        var mapper = IcebergRowMappers.For<GeneratedCustomer>();
        Assert.NotNull(mapper);
        // Ensure it is not the fallback ReflectionRowMapper
        Assert.DoesNotContain("ReflectionRowMapper", mapper.GetType().FullName!);
    }

    [Fact]
    public void GeneratedRecord_ToRowAndCreateReader_RoundTrip()
    {
        var mapper = IcebergRowMappers.For<GeneratedCustomer>();
        var binding = mapper.Bind(GeneratedCustomer.IcebergSchema);

        var original = new GeneratedCustomer(42, "Alice", "alice@example.com", 123.45m);
        var row = binding.ToRow(original);

        Assert.Equal(4, row.Length);
        Assert.Equal(42, row[0]);
        Assert.Equal("Alice", row[1]);
        Assert.Equal("alice@example.com", row[2]);
        Assert.Equal(IcebergDecimal.FromDecimal(123.45m, 18), row[3]);

        var reader = binding.CreateReader(GeneratedCustomer.IcebergSchema);
        var reconstituted = reader(row);

        Assert.Equal(original, reconstituted);
    }

    [Fact]
    public void GeneratedClass_ToRowAndCreateReader_RoundTrip()
    {
        var mapper = IcebergRowMappers.For<GeneratedOrder>();
        var binding = mapper.Bind(GeneratedOrder.IcebergSchema);

        var original = new GeneratedOrder
        {
            OrderId = 9876543210L,
            CustomerId = 42,
            Description = "Rush order"
        };
        var row = binding.ToRow(original);

        Assert.Equal(3, row.Length);
        Assert.Equal(9876543210L, row[0]);
        Assert.Equal(42, row[1]);
        Assert.Equal("Rush order", row[2]);

        var reader = binding.CreateReader(GeneratedOrder.IcebergSchema);
        var reconstituted = reader(row);

        Assert.Equal(original.OrderId, reconstituted.OrderId);
        Assert.Equal(original.CustomerId, reconstituted.CustomerId);
        Assert.Equal(original.Description, reconstituted.Description);
    }
}
