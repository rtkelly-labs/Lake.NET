using Parquet.SourceGenerator;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

// Models compiled by the published Parquet.SourceGenerator 0.0.3 (zero-reflection Parquet.Net writers).

/// <summary>Uses only mappings that already match Iceberg's Parquet representation.</summary>
[ParquetSerializable]
public partial record CompatibleEvent
{
    [ParquetColumn("id", Order = 1)] public long Id { get; init; }
    [ParquetColumn("name", Order = 2)] public string? Name { get; init; }
    [ParquetColumn("event_ts", Order = 3)][ParquetTimestamp(ParquetTimestampUnit.Microseconds)] public DateTime EventTs { get; init; }
    [ParquetColumn("day", Order = 4)] public DateOnly Day { get; init; }
    [ParquetColumn("uid", Order = 5)] public Guid Uid { get; init; }
    [ParquetColumn("amount", Order = 6)][ParquetDecimal(9, 2)] public decimal Amount { get; init; }
    [ParquetColumn("qty", Order = 7)] public int? Qty { get; init; }
}

[ParquetSerializable]
public partial record DurationModel
{
    [ParquetColumn("id", Order = 1)] public long Id { get; init; }
    [ParquetColumn("dur", Order = 2)] public TimeSpan Dur { get; init; }
}

[ParquetSerializable]
public partial record DefaultTimestampModel
{
    [ParquetColumn("id", Order = 1)] public long Id { get; init; }
    [ParquetColumn("ts", Order = 2)] public DateTime Ts { get; init; }
}

[ParquetSerializable]
public partial record UnsignedModel
{
    [ParquetColumn("id", Order = 1)] public long Id { get; init; }
    [ParquetColumn("n", Order = 2)] public uint N { get; init; }
}

/// <summary>
/// Parquet.SourceGenerator as a typed, reflection-free write path for Iceberg tables: generated files are
/// adopted with <see cref="ParquetImport"/> (name mapping, type validation, metrics) and must read back in
/// .NET and PyIceberg. The "gap" tests pin the generator behaviours that Iceberg rejects today; each one names
/// the fix (docs/parquet-sourcegenerator.md). When the generator closes a gap, its test fails and moves to
/// the compatible list.
/// </summary>
[Trait("Category", "Regression")]
public sealed class SourceGeneratorInteropTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-sg", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public SourceGeneratorInteropTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema EventSchema = new(
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Require(3, "event_ts", PrimitiveType.TimestampTz),
        NestedField.Require(4, "day", PrimitiveType.Date),
        NestedField.Require(5, "uid", PrimitiveType.Uuid),
        NestedField.Require(6, "amount", new DecimalType(9, 2)),
        NestedField.Optional(7, "qty", PrimitiveType.Int));

    private static List<CompatibleEvent> Events(int n) => Enumerable.Range(0, n).Select(i => new CompatibleEvent
    {
        Id = i,
        Name = i % 3 == 0 ? null : $"n{i}",
        EventTs = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMinutes(i * 37).AddTicks(10 * i),
        Day = new DateOnly(2024, 1, 1).AddDays(i % 2),
        Uid = new Guid(i, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1]),
        Amount = i * 1.25m - 3m,
        Qty = i % 4 == 0 ? null : i,
    }).ToList();

    private async Task<(Table Table, string Location)> GeneratedFileIn<T>(string tableName, Schema schema, IReadOnlyCollection<T> rows, Func<IReadOnlyCollection<T>, Stream, Task> write, bool mapping = true)
    {
        var table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", tableName), schema));
        if (mapping) await ParquetImport.ApplyNameMappingAsync(table);
        var location = Path.Combine(table.Location, "data", $"generated-{Guid.NewGuid():N}.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(location)!);
        await using (var f = File.Create(location)) await write(rows, f);
        return (table, location);
    }

    [Fact]
    public async Task Generated_files_are_adopted_and_read_back_exactly()
    {
        var events = Events(20);
        var (table, location) = await GeneratedFileIn("events", EventSchema, events, (r, s) => r.WriteParquetAsync(s));
        var file = await ParquetImport.ToDataFileAsync(table, location);
        Assert.Equal(20, file.RecordCount);
        Assert.Equal(19L, Conversions.FromBytes(PrimitiveType.Long, file.UpperBounds![1]));
        await SnapshotProducer.Append(table).AddFiles([file]).CommitAsync();

        var rows = (await TableScan.ReadAllAsync(table)).OrderBy(r => (long)r[0]!).ToList();
        for (var i = 0; i < events.Count; i++)
        {
            var e = events[i];
            Assert.Equal(e.Id, rows[i][0]);
            Assert.Equal(e.Name, rows[i][1]);
            Assert.Equal(IcebergTime.MicrosFromTimestamp(e.EventTs), rows[i][2]);
            Assert.Equal(IcebergTime.DaysFromDate(e.Day), rows[i][3]);
            Assert.Equal(e.Uid, rows[i][4]);
            Assert.Equal(e.Amount, ((IcebergDecimal)rows[i][5]!).ToDecimal());
            Assert.Equal(e.Qty, rows[i][6]);
        }
    }

    [SkippableFact]
    public async Task Pyiceberg_reads_generated_files_through_the_name_mapping()
    {
        Require.LocalOracle();
        var events = Events(12);
        var (table, location) = await GeneratedFileIn("events_py", EventSchema, events, (r, s) => r.WriteParquetAsync(s));
        await SnapshotProducer.Append(table).AddFiles([await ParquetImport.ToDataFileAsync(table, location)]).CommitAsync();

        var py = await TestEnvironment.Current.OracleAsync("read-metadata", table.MetadataLocation!);
        Assert.Equal(events.Select(e => e.Id), py["ids"]!.AsArray().Select(i => i!.GetValue<long>()));
        var first = py["rows"]!.AsArray()[1]!;
        Assert.Equal(events[1].Uid.ToString(), first["uid"]!.GetValue<string>());
        Assert.Equal(events[1].Amount, decimal.Parse(first["amount"]!.GetValue<string>(), System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(events[1].Day.ToString("yyyy-MM-dd"), first["day"]!.GetValue<string>());
        Assert.Equal(IcebergTime.MicrosFromTimestamp(events[1].EventTs), first["event_ts"]!.GetValue<long>());
    }

    [Fact]
    public async Task Gap_no_field_ids_requires_a_name_mapping()
    {
        var (table, location) = await GeneratedFileIn("no_mapping", EventSchema, Events(3), (r, s) => r.WriteParquetAsync(s), mapping: false);
        var e = await Assert.ThrowsAsync<ParquetImportException>(() => ParquetImport.ToDataFileAsync(table, location));
        Assert.Contains("no Parquet field IDs", e.Message);
    }

    [Fact]
    public async Task Gap_timestamps_are_always_utc_adjusted()
    {
        // Iceberg `timestamp` (no zone) needs isAdjustedToUTC=false; the generator has no way to ask for it.
        var schema = new Schema(0, EventSchema.Columns.Select(c => c.Id == 3 ? c with { Type = PrimitiveType.Timestamp } : c));
        var (table, location) = await GeneratedFileIn("local_ts", schema, Events(3), (r, s) => r.WriteParquetAsync(s));
        var e = await Assert.ThrowsAsync<ParquetImportException>(() => ParquetImport.ToDataFileAsync(table, location));
        Assert.Contains(e.Problems, p => p.Column == "event_ts" && p.Problem.Contains("UTC-adjusted"));
    }

    [Fact]
    public async Task Gap_timespan_is_written_as_time_millis()
    {
        var schema = new Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Require(2, "dur", PrimitiveType.Time));
        var (table, location) = await GeneratedFileIn("dur", schema, new[] { new DurationModel { Id = 1, Dur = TimeSpan.FromSeconds(1) } }, (r, s) => r.WriteParquetAsync(s));
        var e = await Assert.ThrowsAsync<ParquetImportException>(() => ParquetImport.ToDataFileAsync(table, location));
        Assert.Contains(e.Problems, p => p.Column == "dur" && p.Problem.Contains("Millis"));
    }

    [Fact]
    public async Task Gap_default_datetime_mapping_is_int96()
    {
        var schema = new Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Require(2, "ts", PrimitiveType.TimestampTz));
        var (table, location) = await GeneratedFileIn("int96", schema, new[] { new DefaultTimestampModel { Id = 1, Ts = DateTime.UtcNow } }, (r, s) => r.WriteParquetAsync(s));
        var e = await Assert.ThrowsAsync<ParquetImportException>(() => ParquetImport.ToDataFileAsync(table, location));
        Assert.Contains(e.Problems, p => p.Column == "ts" && p.Problem.Contains("INT96"));
    }

    [Fact]
    public async Task Gap_unsigned_integers_have_no_iceberg_type()
    {
        var schema = new Schema(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Require(2, "n", PrimitiveType.Long));
        var (table, location) = await GeneratedFileIn("unsigned", schema, new[] { new UnsignedModel { Id = 1, N = 7 } }, (r, s) => r.WriteParquetAsync(s));
        var e = await Assert.ThrowsAsync<ParquetImportException>(() => ParquetImport.ToDataFileAsync(table, location));
        Assert.Contains(e.Problems, p => p.Column == "n" && p.Problem.Contains("unsigned"));
    }
}
