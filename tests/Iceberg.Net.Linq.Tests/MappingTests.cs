using System.Text.Json;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq.Tests;

public sealed record Point(double X, double Y);

public enum Color : long { Red = 1, Green = 2, Blue = 3 }

/// <summary>Every member type the reflection mapper supports.</summary>
public sealed class AllTypes
{
    public int Id { get; set; }
    public bool Flag { get; set; }
    public bool? MaybeFlag { get; set; }
    public short Small { get; set; }
    public byte Tiny { get; set; }
    public long Big { get; set; }
    public uint Unsigned { get; set; }
    public float Ratio { get; set; }
    public double Score { get; set; }
    public double? MaybeScore { get; set; }
    [IcebergField(Type = "decimal(20, 4)")] public decimal Money { get; set; }
    public decimal? DefaultDecimal { get; set; }
    public string Name { get; set; } = "";
    public string? Nick { get; set; }
    public Guid Key { get; set; }
    public Guid? MaybeKey { get; set; }
    public byte[] Blob { get; set; } = [];
    [IcebergField(Type = "fixed[4]")] public byte[]? Fixed4 { get; set; }
    public DateOnly Day { get; set; }
    [IcebergField(Type = "date")] public DateTime DayAsDateTime { get; set; }
    public DateTime Local { get; set; }
    [IcebergField(Type = "timestamptz")] public DateTime Utc { get; set; }
    public DateTimeOffset Instant { get; set; }
    [IcebergField(Type = "timestamp")] public DateTimeOffset? WallClock { get; set; }
    public TimeOnly At { get; set; }
    public TimeSpan? Elapsed { get; set; }
    [IcebergField(Type = "timestamp_ns")] public long RawNanos { get; set; }
    [IcebergField(Type = "timestamptz_ns")] public DateTime Nanos { get; set; }
    public Status Status { get; set; }
    public Color Color { get; set; }
    [IcebergField(Type = "string")] public Status? StatusName { get; set; }
    public Point Origin { get; set; } = new(0, 0);
    public Point? Maybe { get; set; }
    public List<int> Numbers { get; set; } = [];
    public string?[] Tags { get; set; } = [];
    public IReadOnlyList<Point> Path { get; set; } = [];
    public HashSet<long> Set { get; set; } = [];
    public Dictionary<string, int> Counts { get; set; } = [];
    public IReadOnlyDictionary<string, Point?>? Places { get; set; }
}

/// <summary>Positional record: constructor parameters, no setters.</summary>
public sealed record Person(long Id, string Name, DateOnly? Born, IReadOnlyList<string> Emails);

/// <summary>Init-only properties and a required member, plus a constructor-less nested record.</summary>
public sealed record Account
{
    public required long Id { get; init; }
    public string Owner { get; init; } = "";
    public decimal? Balance { get; init; }
}

/// <summary>Explicit field IDs: bound by ID, not name.</summary>
public sealed class ById
{
    [IcebergField(1)] public long Key { get; set; }
    [IcebergField(2)] public string? Label { get; set; }
}

public sealed class MappingTests : CatalogTest
{
    private static readonly JsonSerializerOptions Json = new() { IncludeFields = false };

    private static AllTypes Sample(int i) => new()
    {
        Id = i,
        Flag = i % 2 == 0,
        MaybeFlag = i % 3 == 0 ? null : i % 2 == 1,
        Small = (short)(-i * 100),
        Tiny = (byte)i,
        Big = long.MaxValue - i,
        Unsigned = uint.MaxValue - (uint)i,
        Ratio = i / 4f,
        Score = Math.PI * i,
        MaybeScore = i % 2 == 0 ? null : -i,
        Money = 12345678901234.5678m + i,
        DefaultDecimal = i % 2 == 0 ? null : 1.000000000000000001m * i,
        Name = $"name-{i}-é漢😀",
        Nick = i % 2 == 0 ? null : "nick",
        Key = Guid.NewGuid(),
        MaybeKey = i % 2 == 0 ? null : Guid.NewGuid(),
        Blob = [1, 2, (byte)i],
        Fixed4 = i % 2 == 0 ? null : [9, 8, 7, (byte)i],
        Day = new DateOnly(1969, 12, 31).AddDays(i),
        DayAsDateTime = new DateTime(2000, 2, 29).AddDays(i),
        Local = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Unspecified).AddTicks(1230 * i),
        Utc = new DateTime(1960, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(10 * i),
        Instant = new DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero).AddMilliseconds(i),
        WallClock = i % 2 == 0 ? null : new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero),
        At = new TimeOnly(23, 59, 59).Add(TimeSpan.FromTicks(-10 * i)),
        Elapsed = i % 2 == 0 ? null : TimeSpan.FromMinutes(i),
        RawNanos = 1_700_000_000_123_456_789L + i,
        Nanos = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(i),
        Status = (Status)(i % 3 + 1),
        Color = (Color)(i % 3 + 1),
        StatusName = i % 2 == 0 ? null : Status.Pending,
        Origin = new Point(i, -i),
        Maybe = i % 2 == 0 ? null : new Point(0.5, 1.5),
        Numbers = Enumerable.Range(0, i).ToList(),
        Tags = ["a", null, $"t{i}"],
        Path = [new Point(1, 2), new Point(3, i)],
        Set = [i, i * 2L],
        Counts = new() { ["x"] = i, ["y"] = -i },
        Places = i % 2 == 0 ? null : new Dictionary<string, Point?> { ["home"] = new Point(1, 1), ["nowhere"] = null },
    };

    [Fact]
    public async Task Every_supported_type_round_trips_through_a_table()
    {
        var table = await Catalog.CreateTableAsync<AllTypes>(NewId(), formatVersion: 3);
        var items = Enumerable.Range(1, 6).Select(Sample).ToList();
        await table.InsertAsync(items);
        var read = (await table.AsQueryable<AllTypes>().ToListAsync()).OrderBy(x => x.Id).ToList();
        Assert.Equal(items.Count, read.Count);
        for (var i = 0; i < items.Count; i++)
            Assert.Equal(JsonSerializer.Serialize(items[i], Json), JsonSerializer.Serialize(read[i], Json));
        // Kinds: timestamp reads Unspecified, timestamptz reads Utc; DateTimeOffset reads at offset zero.
        Assert.Equal(DateTimeKind.Unspecified, read[0].Local.Kind);
        Assert.Equal(DateTimeKind.Utc, read[0].Utc.Kind);
        Assert.Equal(TimeSpan.Zero, read[0].Instant.Offset);
    }

    [Fact]
    public async Task Rows_hold_the_physical_values_the_core_writes()
    {
        var table = await Catalog.CreateTableAsync<AllTypes>(NewId(), formatVersion: 3);
        var binding = new ReflectionRowMapper<AllTypes>().Bind(table.Schema);
        var row = binding.ToRow(Sample(1));
        object? At(string name) => row[table.Schema.Columns.ToList().FindIndex(c => c.Name == name)];
        Assert.IsType<int>(At("Day"));
        Assert.Equal(-1 + 1, (int)At("Day")!); // 1969-12-31 + 1 day = epoch
        Assert.IsType<long>(At("Local"));
        Assert.Equal(new IcebergDecimal(123456789012355678, 4), At("Money"));
        Assert.Equal("Pending", At("StatusName"));
        Assert.Equal(2, At("Status"));
        Assert.Equal(2L, At("Color"));
        Assert.Equal(1_700_000_000_123_456_790L, At("RawNanos"));
        Assert.IsType<object?[]>(At("Origin"));
        Assert.IsType<List<object?>>(At("Numbers"));
        Assert.IsType<Dictionary<object, object?>>(At("Counts"));
    }

    [Fact]
    public async Task Positional_and_init_only_records_round_trip()
    {
        var people = await Catalog.CreateTableAsync<Person>(NewId());
        var person = new Person(1, "Ada", new DateOnly(1815, 12, 10), ["ada@example.com"]);
        await people.InsertAsync([person, person with { Id = 2, Born = null, Emails = [] }]);
        var read = (await people.AsQueryable<Person>().ToListAsync()).OrderBy(p => p.Id).ToList();
        Assert.Equal(person.Name, read[0].Name);
        Assert.Equal(person.Born, read[0].Born);
        Assert.Equal(person.Emails, read[0].Emails);
        Assert.Null(read[1].Born);

        var accounts = await Catalog.CreateTableAsync<Account>(NewId());
        await accounts.InsertAsync([new Account { Id = 7, Owner = "o", Balance = 1.5m }]);
        Assert.Equal(new Account { Id = 7, Owner = "o", Balance = 1.5m }, Assert.Single(await accounts.AsQueryable<Account>().ToListAsync()));
    }

    [Fact]
    [Spec("Schema Evolution")]
    public async Task Members_bind_by_field_id_and_survive_a_rename()
    {
        var schema = new Schema(0, [NestedField.Require(1, "key", PrimitiveType.Long), NestedField.Optional(2, "label", PrimitiveType.String)]);
        var table = await Catalog.CreateTableAsync(new CreateTableRequest(NewId(), schema));
        await table.InsertAsync([new ById { Key = 1, Label = "a" }]);
        await table.UpdateSchemaAsync(u => u.RenameColumn("label", "title"));
        var read = Assert.Single(await table.AsQueryable<ById>().ToListAsync());
        Assert.Equal("a", read.Label);
    }

    [Fact]
    public async Task Names_match_exactly_then_by_alias_then_case_insensitively_when_asked()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "full_name", PrimitiveType.String)]);
        var table = await Catalog.CreateTableAsync(new CreateTableRequest(NewId(), schema));
        Assert.Throws<InvalidOperationException>(() => new ReflectionRowMapper<Lower>().Bind(table.Schema)); // "Id" vs "id"
        var ci = new ReflectionRowMapper<Lower>(new IcebergMapperOptions { CaseInsensitive = true }).Bind(table.Schema);
        Assert.Equal([1, 2], ci.Members.Select(m => m.Field.Id));

        var mapping = NameMapping.FromSchema(table.Schema).WithAlias(2, "Full_Name");
        var aliased = new ReflectionRowMapper<Aliased>().Bind(table.Schema, mapping);
        Assert.Equal([1, 2], aliased.Members.Select(m => m.Field.Id));
        Assert.Equal("full_name", new IcebergMapperOptions { NameConverter = IcebergNaming.SnakeCase }.NameConverter!("FullName"));
        Assert.Equal("http_code", IcebergNaming.SnakeCase("HTTPCode"));
    }

    public sealed class Lower
    {
        public long Id { get; set; }
        [IcebergField(Name = "FULL_NAME")] public string? FullName { get; set; }
    }

    public sealed class Aliased
    {
        [IcebergField(Name = "id")] public long Id { get; set; }
        [IcebergField(Name = "Full_Name")] public string? FullName { get; set; }
    }

    [Fact]
    public async Task Promoted_columns_read_with_the_snapshot_type()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Int), NestedField.Optional(2, "v", PrimitiveType.Float)]);
        var table = await Catalog.CreateTableAsync(new CreateTableRequest(NewId(), schema));
        await TableMutations.InsertAsync(table, [[1, 1.5f]]);
        var old = table.Metadata.CurrentSnapshotId!.Value;
        await table.UpdateSchemaAsync(u => u.UpdateColumn("id", PrimitiveType.Long).UpdateColumn("v", PrimitiveType.Double));
        await TableMutations.InsertAsync(table, [[2L, 2.5]]);
        var all = (await table.AsQueryable<Promoted>().ToListAsync()).OrderBy(p => p.id).ToList();
        Assert.Equal([1L, 2L], all.Select(p => p.id));
        Assert.Equal([1.5, 2.5], all.Select(p => p.v));
        Assert.Equal(1L, Assert.Single(await table.AsQueryable<Promoted>().AtSnapshot(old).ToListAsync()).id);
    }

    public sealed record Promoted(long id, double? v);

    [Fact]
    public void Unsupported_pairs_and_lossy_writes_fail_loudly()
    {
        var schema = new Schema(0, [NestedField.Require(1, "Id", PrimitiveType.String)]);
        Assert.Throws<InvalidOperationException>(() => new ReflectionRowMapper<ById2>().Bind(schema));
        var dates = new Schema(0, [NestedField.Require(1, "DayAsDateTime", PrimitiveType.Date)]);
        var b = new ReflectionRowMapper<DateHolder>().Bind(dates);
        Assert.Throws<ArgumentException>(() => b.ToRow(new DateHolder { DayAsDateTime = new DateTime(2024, 1, 1, 12, 0, 0) }));
        var dec = new Schema(0, [NestedField.Require(1, "Amount", new DecimalType(5, 2))]);
        var d = new ReflectionRowMapper<DecimalHolder>().Bind(dec);
        Assert.Throws<ArgumentException>(() => d.ToRow(new DecimalHolder { Amount = 1.234m }));
        Assert.Throws<ArgumentException>(() => d.ToRow(new DecimalHolder { Amount = 1234.5m }));
        Assert.Equal(new IcebergDecimal(150, 2), d.ToRow(new DecimalHolder { Amount = 1.5m })[0]);
    }

    public sealed class ById2
    {
        public long Id { get; set; }
    }

    public sealed class DateHolder
    {
        public DateTime DayAsDateTime { get; set; }
    }

    public sealed class DecimalHolder
    {
        public decimal Amount { get; set; }
    }

    [Fact]
    public async Task Writes_need_every_required_column()
    {
        var schema = new Schema(0, [NestedField.Require(1, "Key", PrimitiveType.Long), NestedField.Require(2, "other", PrimitiveType.String)]);
        var table = await Catalog.CreateTableAsync(new CreateTableRequest(NewId(), schema));
        var e = await Assert.ThrowsAsync<InvalidOperationException>(() => table.InsertAsync([new KeyOnly { Key = 1 }]));
        Assert.Contains("other", e.Message);
    }

    public sealed class KeyOnly
    {
        public long Key { get; set; }
    }
}
