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

[IcebergMappable]
public partial class GeneratedUnannotatedPair
{
    public string A { get; set; } = "";
    public string B { get; set; } = "";
}

[IcebergMappable]
public partial record GeneratedPositionalWithExtra(int Id)
{
    public string Extra { get; init; } = "";
}

[IcebergMappable]
public partial class GeneratedKeyedModel
{
    [IcebergKey]
    public int KeyId { get; set; }

    public string Value { get; set; } = "";
}

[IcebergMappable]
public partial class GeneratedOverridesModel
{
    [IcebergField(1, Type = "decimal(18, 2)")]
    public decimal Price { get; set; }

    [IcebergField(2, Type = "date")]
    public DateTime Birthday { get; set; }
}

[IcebergMappable]
public partial class GeneratedScalarVarieties
{
    public int Id { get; set; }
    public short Small { get; set; }
    public uint Unsigned { get; set; }
    public Status State { get; set; }
    public TimeSpan Elapsed { get; set; }
}

[IcebergMappable]
public partial class GeneratedNullableScalars
{
    public int Id { get; set; }
    public decimal? MaybeDecimal { get; set; }
    public DateOnly? MaybeDate { get; set; }
    public TimeOnly? MaybeTime { get; set; }
    public DateTime? MaybeDateTime { get; set; }
    public DateTimeOffset? MaybeOffset { get; set; }
}

[IcebergMappable]
public partial class GeneratedMixedIds
{
    public string A { get; set; } = "";
    [IcebergField(1)]
    public string B { get; set; } = "";
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

    [Fact]
    public void UnannotatedMembers_ResolveCorrectlyAgainstDifferentColumnOrder()
    {
        // Table schema has 1: B, 2: A (reverse of declaration order)
        var tableSchema = new Schema(0, [
            NestedField.Require(1, "B", PrimitiveType.String),
            NestedField.Require(2, "A", PrimitiveType.String)
        ]);

        var mapper = IcebergRowMappers.For<GeneratedUnannotatedPair>();
        var binding = mapper.Bind(tableSchema);

        var model = new GeneratedUnannotatedPair { A = "first", B = "second" };
        var row = binding.ToRow(model);

        // Position 0 is B, position 1 is A
        Assert.Equal("second", row[0]);
        Assert.Equal("first", row[1]);

        var reader = binding.CreateReader(tableSchema);
        var read = reader(row);

        Assert.Equal("first", read.A);
        Assert.Equal("second", read.B);
    }

    [Fact]
    public void PositionalRecord_InitializesRemainingProperties()
    {
        var mapper = IcebergRowMappers.For<GeneratedPositionalWithExtra>();
        var binding = mapper.Bind(GeneratedPositionalWithExtra.IcebergSchema);

        var original = new GeneratedPositionalWithExtra(7) { Extra = "bonus" };
        var row = binding.ToRow(original);

        var reader = binding.CreateReader(GeneratedPositionalWithExtra.IcebergSchema);
        var reconstituted = reader(row);

        Assert.Equal(7, reconstituted.Id);
        Assert.Equal("bonus", reconstituted.Extra);
    }

    [Fact]
    public async Task KeyField_IncludedInIdentifierFieldIds_AndSupportsUpsert()
    {
        var schema = GeneratedKeyedModel.IcebergSchema;
        Assert.Single(schema.IdentifierFieldIds);
        Assert.Equal(1, schema.IdentifierFieldIds[0]);

        var table = await Catalog.CreateTableAsync<GeneratedKeyedModel>(NewId());
        Assert.Single(table.Schema.IdentifierFieldIds);

        await table.UpsertAsync([
            new GeneratedKeyedModel { KeyId = 1, Value = "v1" }
        ]);

        var items = await table.AsQueryable<GeneratedKeyedModel>().ToListAsync();
        Assert.Single(items);
        Assert.Equal("v1", items[0].Value);

        // Update via upsert
        await table.UpsertAsync([
            new GeneratedKeyedModel { KeyId = 1, Value = "v2" }
        ]);

        items = await table.AsQueryable<GeneratedKeyedModel>().ToListAsync();
        Assert.Single(items);
        Assert.Equal("v2", items[0].Value);
    }

    [Fact]
    public void TypeOverrides_PreservedInSchemaAndMapping()
    {
        var schema = GeneratedOverridesModel.IcebergSchema;
        var priceCol = schema.Columns.First(c => c.Id == 1);
        Assert.IsType<DecimalType>(priceCol.Type);
        var dec = (DecimalType)priceCol.Type;
        Assert.Equal(18, dec.Precision);
        Assert.Equal(2, dec.Scale);

        var bdayCol = schema.Columns.First(c => c.Id == 2);
        Assert.Equal(PrimitiveType.Date, bdayCol.Type);

        var mapper = IcebergRowMappers.For<GeneratedOverridesModel>();
        var binding = mapper.Bind(schema);

        var original = new GeneratedOverridesModel
        {
            Price = 45.50m,
            Birthday = new DateTime(2000, 1, 15)
        };
        var row = binding.ToRow(original);
        Assert.IsType<IcebergDecimal>(row[0]);
        Assert.Equal(2, ((IcebergDecimal)row[0]!).Scale);
        Assert.IsType<int>(row[1]); // date as days

        var reader = binding.CreateReader(schema);
        var read = reader(row);
        Assert.Equal(original.Price, read.Price);
        Assert.Equal(original.Birthday.Date, read.Birthday.Date);
    }

    [Fact]
    public async Task VarietiesOfScalars_RoundTripSuccessfully()
    {
        var table = await Catalog.CreateTableAsync<GeneratedScalarVarieties>(NewId());
        var item = new GeneratedScalarVarieties
        {
            Id = 10,
            Small = 42,
            Unsigned = 9999,
            State = Status.Pending,
            Elapsed = TimeSpan.FromMinutes(15)
        };

        await table.InsertAsync([item]);

        var read = Assert.Single(await table.AsQueryable<GeneratedScalarVarieties>().ToListAsync());
        Assert.Equal(item.Id, read.Id);
        Assert.Equal(item.Small, read.Small);
        Assert.Equal(item.Unsigned, read.Unsigned);
        Assert.Equal(item.State, read.State);
        Assert.Equal(item.Elapsed, read.Elapsed);
    }

    [Fact]
    public async Task NullableScalars_ReadNullsCorrectly_WithoutDefaultingToZeroOrEpoch()
    {
        var table = await Catalog.CreateTableAsync<GeneratedNullableScalars>(NewId());
        var item = new GeneratedNullableScalars
        {
            Id = 1,
            MaybeDecimal = null,
            MaybeDate = null,
            MaybeTime = null,
            MaybeDateTime = null,
            MaybeOffset = null
        };

        await table.InsertAsync([item]);

        var read = Assert.Single(await table.AsQueryable<GeneratedNullableScalars>().ToListAsync());
        Assert.Equal(1, read.Id);
        Assert.Null(read.MaybeDecimal);
        Assert.Null(read.MaybeDate);
        Assert.Null(read.MaybeTime);
        Assert.Null(read.MaybeDateTime);
        Assert.Null(read.MaybeOffset);
    }

    [Fact]
    public void MixedExplicitAndAutoIds_DoNotCollide()
    {
        var schema = GeneratedMixedIds.IcebergSchema;
        Assert.Equal(2, schema.Columns.Count);
        var bCol = schema.Columns.First(c => c.Name == "B");
        var aCol = schema.Columns.First(c => c.Name == "A");

        Assert.Equal(1, bCol.Id);
        Assert.Equal(2, aCol.Id); // Auto ID skipped 1 because B claimed it!
    }
}
