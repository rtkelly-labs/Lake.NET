using System.Text.Json.Nodes;
using Iceberg.Net.Data;
using Iceberg.Net.DuckLake.TestSupport;
using Iceberg.Net.Expressions;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;
using ParquetSharp;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>
/// The unsigned integers, second and millisecond timestamps, <c>timetz</c> and <c>interval</c> (spec "Data Types"),
/// stored as DuckDB 1.5.5 stores them: Parquet annotations, statistics text, inlined column types and text.
/// </summary>
internal static class WideTypes
{
    public static readonly DuckLakeColumnDefinition[] Columns =
    [
        new("id", DuckLakeType.Int64, Nullable: false),
        new("u8", DuckLakeType.UInt8),
        new("u16", DuckLakeType.UInt16),
        new("u32", DuckLakeType.UInt32),
        new("u64", DuckLakeType.UInt64),
        new("i8", DuckLakeType.Int8),
        new("i16", DuckLakeType.Int16),
        new("ts_s", DuckLakeType.TimestampS),
        new("ts_ms", DuckLakeType.TimestampMs),
        new("ttz", DuckLakeType.TimeTz),
        new("iv", DuckLakeType.Interval),
        new("u32s", DuckLakeType.List(DuckLakeType.UInt32)),
        new("ivs", DuckLakeType.List(DuckLakeType.Interval)),
        new("pair", DuckLakeType.Struct(("t", DuckLakeType.TimeTz), ("n", DuckLakeType.UInt64))),
    ];

    /// <summary>Boundary values in the CLR forms callers pass (unsigned CLR integers, DateTime, strings for timetz/interval).</summary>
    public static object?[] Row(long i) => i switch
    {
        0 => [0L, (byte)0, (ushort)0, 0u, 0UL, -128, -32768,
            new DateTime(1960, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(1960, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc),
            "00:00:00+00", "00:00:00", new List<object?> { 0u }, new List<object?>(), new object?[] { "00:00:00-15:59:59", 0UL }],
        1 => [1L, (byte)255, (ushort)65535, uint.MaxValue, ulong.MaxValue, 127, 32767,
            // Sub-second digits are dropped by the unit (towards negative infinity, as DuckDB's casts).
            new DateTime(2024, 1, 2, 3, 4, 5, 999, DateTimeKind.Utc), new DateTime(2024, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc).AddTicks(9990),
            new DuckLakeTimeTz(3_723_456_789, 5 * 3600 + 30 * 60), new DuckLakeInterval(1, 2, 3_004_005),
            new List<object?> { uint.MaxValue, null, 1u }, new List<object?> { new DuckLakeInterval(14, 1, 0), null },
            new object?[] { "24:00:00+15:59:59", ulong.MaxValue }],
        2 => [2L, 7, 7, 7L, new IcebergDecimal(7, 0), 0, 0,
            new DateTime(1969, 12, 31, 23, 59, 59, 500, DateTimeKind.Utc), new DateTime(1969, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc).AddTicks(5),
            new DuckLakeTimeTz(1_000_000, -3 * 3600), "2 years 3 days 04:05:06.000001",
            null, new List<object?> { new DuckLakeInterval(0, 0, 1000) }, null],
        _ => [i, .. Enumerable.Repeat<object?>(null, Columns.Length - 1)],
    };

    public static IEnumerable<object?[]> Rows() => Enumerable.Range(0, 4).Select(i => Row(i));

    /// <summary>A row as read back: physical values, and what Parquet keeps of timetz (the UTC instant) and interval (milliseconds).</summary>
    public static object?[] Expected(object?[] written, bool fromFile)
    {
        var row = Columns.Select((c, i) => DuckLakeValues.Normalize(c.Type, written[i], c.Name)).ToArray();
        return fromFile ? row.Select((v, i) => Lossy(Columns[i].Type, v)).ToArray() : row;
    }

    private static object? Lossy(DuckLakeType type, object? v) => v is null ? null : type switch
    {
        DuckLakePrimitive { Name: "timetz" } => DuckLakeTimeTz.FromUtcMicros(((DuckLakeTimeTz)v).UtcMicros),
        DuckLakePrimitive { Name: "interval" } => (DuckLakeInterval)v with { Micros = ((DuckLakeInterval)v).Micros / 1000 * 1000 },
        DuckLakeList l => ((IEnumerable<object?>)v).Select(x => Lossy(l.Element, x)).ToList(),
        DuckLakeStruct s => s.Fields.Select((f, k) => Lossy(f.Type, ((object?[])v)[k])).ToArray(),
        _ => v,
    };

    /// <summary>Rows as comparable text: timetz, interval and uint64 as DuckDB prints them, timestamps as epoch micros.</summary>
    public static List<string> Json(IEnumerable<object?[]> rows) => rows.Select(r =>
    {
        static JsonNode? Text(object? v) => v is null ? null : JsonValue.Create(v is IcebergDecimal d ? d.Unscaled.ToString() : v.ToString());
        var a = new JsonArray(
            JsonValue.Create((long)r[0]!), Num(r[1]), Num(r[2]), Num(r[3]), Text(r[4]), Num(r[5]), Num(r[6]), Num(r[7]), Num(r[8]), Text(r[9]), Text(r[10]),
            r[11] is IEnumerable<object?> u ? new JsonArray(u.Select(Num).ToArray()) : null,
            r[12] is IEnumerable<object?> iv ? new JsonArray(iv.Select(Text).ToArray()) : null,
            r[13] is object?[] p ? new JsonObject { ["t"] = Text(p[0]), ["n"] = Text(p[1]) } : null);
        return LakeJson.Canonical(a)!.ToJsonString();
    }).Order(StringComparer.Ordinal).ToList();

    private static JsonNode? Num(object? v) => v is null ? null : JsonValue.Create(Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture));

    /// <summary>The same columns from DuckDB, cast so Python returns them losslessly.</summary>
    public const string Select = """
        SELECT id, u8, u16, u32, u64::VARCHAR, i8, i16, epoch_us(ts_s), epoch_us(ts_ms), ttz::VARCHAR, iv::VARCHAR, u32s,
               list_transform(ivs, x -> x::VARCHAR),
               CASE WHEN pair IS NULL THEN NULL ELSE {'t': pair.t::VARCHAR, 'n': pair.n::VARCHAR} END
        FROM lake.t
        """;

    public const string Create = """
        CREATE TABLE lake.t (id BIGINT NOT NULL, u8 UTINYINT, u16 USMALLINT, u32 UINTEGER, u64 UBIGINT, i8 TINYINT, i16 SMALLINT,
            ts_s TIMESTAMP_S, ts_ms TIMESTAMP_MS, ttz TIMETZ, iv INTERVAL, u32s UINTEGER[], ivs INTERVAL[], pair STRUCT(t TIMETZ, n UBIGINT))
        """;

    /// <summary>Rows like Row(0..3) as DuckDB literals, for DuckDB to write.</summary>
    public static readonly string[] Values =
    [
        "(0, 0, 0, 0, 0, -128, -32768, TIMESTAMP_S '1960-01-01 00:00:00', TIMESTAMP_MS '1960-01-01 00:00:00.001', TIMETZ '00:00:00+00', INTERVAL '00:00:00', [0], [], {'t': TIMETZ '00:00:00-15:59:59', 'n': 0})",
        "(1, 255, 65535, 4294967295, 18446744073709551615, 127, 32767, '2024-01-02 03:04:05.999'::TIMESTAMP::TIMESTAMP_S, '2024-01-02 03:04:05.678999'::TIMESTAMP::TIMESTAMP_MS, TIMETZ '01:02:03.456789+05:30', INTERVAL '1 month 2 days 00:00:03.004005', [4294967295, NULL, 1], [INTERVAL '1 year 2 months 1 day', NULL], {'t': TIMETZ '24:00:00+15:59:59', 'n': 18446744073709551615})",
        "(2, 7, 7, 7, 7, 0, 0, '1969-12-31 23:59:59.5'::TIMESTAMP::TIMESTAMP_S, '1969-12-31 23:59:59.9990005'::TIMESTAMP::TIMESTAMP_MS, TIMETZ '00:00:01-03', INTERVAL '2 years 3 days 04:05:06.000001', NULL, [INTERVAL '1 millisecond'], NULL)",
        "(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    ];
}

public abstract class TypeTests : IAsyncLifetime
{
    private readonly TestLake _lake;

    protected TypeTests(CatalogBackend backend, string version) => _lake = new TestLake(backend, version);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        try { await _lake.ValidateAsync(); }
        finally { _lake.Dispose(); }
    }

    private async Task<DuckLakeCatalog> LakeAsync(int inlining = 0)
    {
        var catalog = await _lake.CreateAsync(new DuckLakeOptions { DataInliningRowLimit = inlining });
        await using var tx = catalog.BeginTransaction();
        await tx.CreateTableAsync("main", "t", WideTypes.Columns);
        await tx.CommitAsync();
        return catalog;
    }

    private static async Task InsertAsync(DuckLakeCatalog catalog, IEnumerable<object?[]> rows)
    {
        await using var tx = catalog.BeginTransaction();
        await tx.InsertAsync("main", "t", rows);
        await tx.CommitAsync();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(10)]
    [Spec("ducklake: Data Types")]
    [Spec("ducklake: Data Inlining")]
    public async Task Unsigned_second_millisecond_timetz_and_interval_values_round_trip(int inlining)
    {
        var catalog = await LakeAsync(inlining);
        await InsertAsync(catalog, WideTypes.Rows());
        Assert.Equal(inlining == 0 ? 1 : 0, _lake.Files().Count());

        var table = await catalog.LoadTableAsync("main", "t");
        var read = (await catalog.ReadAsync(table)).OrderBy(r => (long)r[0]!).ToList();
        var expected = WideTypes.Rows().Select(r => WideTypes.Expected(r, fromFile: inlining == 0)).ToList();
        Assert.Equal(expected.Count, read.Count);
        for (var i = 0; i < read.Count; i++) Assert.Equal(LakeJson.Rows(table.Columns, [expected[i]]), LakeJson.Rows(table.Columns, [read[i]]));

        // The physical forms: unsigned 32-bit as long, 64-bit as decimal(20, 0), truncated microsecond timestamps.
        var max = read[1];
        Assert.Equal((int)byte.MaxValue, max[1]);
        Assert.Equal((long)uint.MaxValue, max[3]);
        Assert.Equal(new IcebergDecimal(ulong.MaxValue, 0), max[4]);
        Assert.Equal(IcebergTime.MicrosFromTimestamp(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)), max[7]);
        Assert.Equal(IcebergTime.MicrosFromTimestamp(new DateTime(2024, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc)), max[8]);
        Assert.Equal(IcebergTime.MicrosFromTimestamp(new DateTime(1969, 12, 31, 23, 59, 59, DateTimeKind.Utc)), read[2][7]);
        // A file keeps timetz's instant (at offset 0) and interval's milliseconds; inlined rows keep both exactly.
        Assert.Equal(inlining == 0 ? new DuckLakeTimeTz(70_323_456_789, 0) : new DuckLakeTimeTz(3_723_456_789, 19_800), max[9]);
        Assert.Equal(inlining == 0 ? new DuckLakeInterval(1, 2, 3_004_000) : new DuckLakeInterval(1, 2, 3_004_005), max[10]);
    }

    [Fact]
    [Spec("ducklake: Data Types")]
    public async Task Data_files_carry_the_Parquet_annotations_DuckDB_writes()
    {
        var catalog = await LakeAsync();
        await InsertAsync(catalog, WideTypes.Rows());
        using var reader = new ParquetFileReader(_lake.Files().Single());
        var schema = reader.FileMetaData.Schema;
        string Annotation(string path)
        {
            for (var i = 0; i < schema.NumColumns; i++)
            {
                var c = schema.Column(i);
                if (c.Path.ToDotString() != path) continue;
                using var logical = c.LogicalType;
                return $"{c.PhysicalType} {logical switch
                {
                    IntLogicalType n => $"INT({n.BitWidth},{(n.IsSigned ? "signed" : "unsigned")})",
                    TimestampLogicalType t => $"TIMESTAMP({t.TimeUnit},utc={t.IsAdjustedToUtc})",
                    TimeLogicalType t => $"TIME({t.TimeUnit},utc={t.IsAdjustedToUtc})",
                    IntervalLogicalType => "INTERVAL",
                    _ => logical.Type.ToString(),
                }}";
            }
            throw new KeyNotFoundException(path);
        }
        Assert.Equal("Int32 INT(8,unsigned)", Annotation("u8"));
        Assert.Equal("Int32 INT(16,unsigned)", Annotation("u16"));
        Assert.Equal("Int32 INT(32,unsigned)", Annotation("u32"));
        Assert.Equal("Int64 INT(64,unsigned)", Annotation("u64"));
        Assert.Equal("Int32 INT(8,signed)", Annotation("i8"));
        Assert.Equal("Int32 INT(16,signed)", Annotation("i16"));
        Assert.Equal("Int64 TIMESTAMP(Micros,utc=False)", Annotation("ts_s"));
        Assert.Equal("Int64 TIMESTAMP(Millis,utc=False)", Annotation("ts_ms"));
        Assert.Equal("Int64 TIME(Micros,utc=True)", Annotation("ttz"));
        Assert.Equal("FixedLenByteArray INTERVAL", Annotation("iv"));
        Assert.Equal("Int32 INT(32,unsigned)", Annotation("u32s.list.element"));
        Assert.Equal("FixedLenByteArray INTERVAL", Annotation("ivs.list.element"));
        Assert.Equal("Int64 INT(64,unsigned)", Annotation("pair.n"));
    }

    [Fact]
    [Spec("ducklake: Type Encoding for Statistics")]
    [Spec("ducklake: ducklake_file_column_stats")]
    public async Task Statistics_are_written_as_DuckDB_writes_them_and_none_for_timetz_and_interval()
    {
        var catalog = await LakeAsync();
        await InsertAsync(catalog, WideTypes.Rows());
        var stats = (await _lake.SqlAsync("SELECT c.column_name, s.min_value, s.max_value FROM ducklake_file_column_stats s JOIN ducklake_column c ON c.column_id = s.column_id WHERE c.parent_column IS NULL"))
            .ToDictionary(r => (string)r[0]!, r => (r[1] as string, r[2] as string));
        Assert.Equal(("0", "255"), stats["u8"]);
        Assert.Equal(("0", "4294967295"), stats["u32"]);
        Assert.Equal(("0", "18446744073709551615"), stats["u64"]);
        Assert.Equal(("1960-01-01 00:00:00", "2024-01-02 03:04:05"), stats["ts_s"]);
        Assert.Equal(("1960-01-01 00:00:00.001", "2024-01-02 03:04:05.678"), stats["ts_ms"]);
        Assert.Equal((null, null), stats["ttz"]);
        Assert.Equal((null, null), stats["iv"]);

        // Pruning on unsigned and millisecond columns uses those bounds (and footer statistics decode millis).
        var table = await catalog.LoadTableAsync("main", "t");
        Assert.Single(await catalog.ReadAsync(table, Expr.Equal("u32", (long)uint.MaxValue)));
        Assert.Single(await catalog.ReadAsync(table, Expr.GreaterThan("u64", new IcebergDecimal(long.MaxValue, 0))));
        Assert.Single(await catalog.ReadAsync(table, Expr.Equal("ts_ms", IcebergTime.MicrosFromTimestamp(new DateTime(2024, 1, 2, 3, 4, 5, 678, DateTimeKind.Utc)))));
        var plan = await catalog.PlanScanAsync(table, Expr.GreaterThan("ts_ms", IcebergTime.MicrosFromTimestamp(new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc))));
        Assert.Empty(plan.Files);
    }

    [Fact]
    public async Task Values_out_of_range_and_negative_intervals_in_files_are_rejected()
    {
        var catalog = await LakeAsync();
        object?[] With(int column, object? value)
        {
            var row = WideTypes.Row(3);
            row[column] = value;
            return row;
        }
        foreach (var (column, value) in new (int, object?)[] { (1, 256), (1, -1), (2, 65536), (3, -1L), (3, 1L << 32), (4, -1L), (4, new IcebergDecimal(System.Numerics.BigInteger.Pow(2, 64), 0)) })
        {
            await using var tx = catalog.BeginTransaction();
            await Assert.ThrowsAsync<ArgumentException>(() => tx.InsertAsync("main", "t", [With(column, value)]));
        }
        await using (var tx = catalog.BeginTransaction())
        {
            // Parquet's INTERVAL is unsigned, so a file cannot hold one (DuckDB refuses the same).
            await Assert.ThrowsAsync<ArgumentException>(() => tx.InsertAsync("main", "t", [With(10, new DuckLakeInterval(0, -1, 0))]));
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => new DuckLakeTimeTz(0, 16 * 3600));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DuckLakeTimeTz(DuckLakeTimeTz.MicrosPerDay + 1, 0));
    }

    [Fact]
    public async Task Negative_intervals_are_kept_inlined()
    {
        var catalog = await LakeAsync(inlining: 10);
        var row = WideTypes.Row(3);
        row[10] = new DuckLakeInterval(-1, -2, -3_500_000);
        await InsertAsync(catalog, [row]);
        var read = await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"));
        Assert.Equal(new DuckLakeInterval(-1, -2, -3_500_000), Assert.Single(read)[10]);
    }

    [Fact]
    public async Task A_flush_of_negative_intervals_is_refused_and_they_stay_inlined()
    {
        var catalog = await LakeAsync(inlining: 10);
        var row = WideTypes.Row(3);
        row[10] = new DuckLakeInterval(0, -1, 0);
        await InsertAsync(catalog, [row]);
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.DeleteAsync("main", "t", Expr.Equal("id", 3L));
            await tx.CommitAsync();
        }
        // Deleted rows are flushed too (earlier snapshots read them), so even a deleted negative interval stays, as in DuckDB.
        await Assert.ThrowsAsync<ArgumentException>(() => catalog.FlushInlinedDataAsync());
        Assert.Empty(_lake.Files());
        var before = await catalog.LoadTableAsync("main", "t", (await catalog.CurrentSnapshotAsync()).SnapshotId - 1);
        Assert.Equal(new DuckLakeInterval(0, -1, 0), Assert.Single(await catalog.ReadAsync(before))[10]);
    }

    [Fact]
    public async Task Timetz_and_interval_columns_cannot_be_filtered_or_bucketed_and_unsigned_columns_cannot_be_bucketed()
    {
        var catalog = await LakeAsync();
        var table = await catalog.LoadTableAsync("main", "t");
        await Assert.ThrowsAsync<NotSupportedException>(() => catalog.ReadAsync(table, Expr.IsNull("ttz")));
        await Assert.ThrowsAsync<NotSupportedException>(() => catalog.ReadAsync(table, Expr.NotNull("iv")));
        foreach (var type in new[] { DuckLakeType.UInt8, DuckLakeType.UInt64, DuckLakeType.TimestampMs, DuckLakeType.TimeTz, DuckLakeType.Interval })
            Assert.Throws<ArgumentException>(() => DuckLakePartitioning.Validate(type, "bucket(4)"));
        Assert.Throws<ArgumentException>(() => DuckLakePartitioning.Validate(DuckLakeType.Interval, "identity"));
        DuckLakePartitioning.Validate(DuckLakeType.TimestampMs, "year");
        DuckLakePartitioning.Validate(DuckLakeType.UInt64, "identity");
    }

    [Fact]
    public async Task Rows_partitioned_by_month_of_a_millisecond_timestamp_land_in_their_month()
    {
        var catalog = await LakeAsync();
        await using (var tx = catalog.BeginTransaction())
        {
            await tx.SetPartitioningAsync("main", "t", [("ts_ms", "month")]);
            await tx.CommitAsync();
        }
        await InsertAsync(catalog, WideTypes.Rows());
        var values = await _lake.SqlAsync("SELECT partition_value FROM ducklake_file_partition_value ORDER BY partition_value");
        Assert.Equal(["1", "12", null], values.Select(v => v[0] as string).OrderBy(v => v is null).ThenBy(v => v, StringComparer.Ordinal));
    }

    [Fact]
    public async Task Inlined_columns_have_the_types_DuckDB_creates()
    {
        var catalog = await LakeAsync(inlining: 10);
        await InsertAsync(catalog, WideTypes.Rows());
        var name = (string)(await _lake.SqlAsync("SELECT table_name FROM ducklake_inlined_data_tables"))[0][0]!;
        var types = _lake.Backend == CatalogBackend.Sqlite
            ? (await _lake.SqlAsync($"SELECT name, type FROM pragma_table_info('{name}')")).ToDictionary(r => (string)r[0]!, r => (string)r[1]!)
            : (await _lake.SqlAsync($"SELECT column_name::text, data_type::text FROM information_schema.columns WHERE table_name = '{name}'")).ToDictionary(r => (string)r[0]!, r => (string)r[1]!);
        string[] expected = _lake.Backend == CatalogBackend.Sqlite
            ? ["u8 BIGINT", "u16 BIGINT", "u32 BIGINT", "u64 VARCHAR", "ts_s VARCHAR", "ts_ms VARCHAR", "ttz VARCHAR", "iv VARCHAR"]
            : ["u8 integer", "u16 integer", "u32 bigint", "u64 character varying", "ts_s character varying", "ts_ms character varying", "ttz time with time zone", "iv interval"];
        Assert.Equal(expected, expected.Select(e => e.Split(' ', 2)[0]).Select(c => $"{c} {types[c]}"));
        var text = (await _lake.SqlAsync($"SELECT CAST(u64 AS TEXT), CAST(ttz AS TEXT), CAST(iv AS TEXT) FROM {name} WHERE {(_lake.Version == DuckLakeCatalog.FormatVersion10 ? "row_id" : "_ducklake_row_id")} = 1"))[0];
        Assert.Equal("18446744073709551615", text[0]);
        Assert.Equal("01:02:03.456789+05:30", text[1]);
        Assert.Equal(_lake.Backend == CatalogBackend.Sqlite ? "1 month 2 days 00:00:03.004005" : "1 mon 2 days 00:00:03.004005", text[2]);
    }

    [Fact]
    public async Task Flushing_inlined_rows_writes_what_a_file_holds()
    {
        var catalog = await LakeAsync(inlining: 10);
        await InsertAsync(catalog, WideTypes.Rows());
        await catalog.FlushInlinedDataAsync();
        var read = (await catalog.ReadAsync(await catalog.LoadTableAsync("main", "t"))).OrderBy(r => (long)r[0]!).ToList();
        var table = await catalog.LoadTableAsync("main", "t");
        Assert.Equal(LakeJson.Rows(table.Columns, WideTypes.Rows().Select(r => WideTypes.Expected(r, fromFile: true))), LakeJson.Rows(table.Columns, read));
    }

    [Theory]
    [InlineData("00:00:00")]
    [InlineData("25:00:00")]
    [InlineData("1 year 1 month")]
    [InlineData("1 year 2 months 1 day")]
    [InlineData("-1 month -2 days -00:00:03.5")]
    [InlineData("2 years 3 days 04:05:06.000001")]
    [InlineData("1 day 00:00:00.1")]
    public void Interval_text_round_trips_in_DuckDBs_spelling(string text) => Assert.Equal(text, DuckLakeInterval.Parse(text).ToString());

    [Theory]
    [InlineData("1 mon 2 days 00:00:03.004005", 1, 2, 3_004_005)] // PostgreSQL
    [InlineData("1 year 2 mons -3 days +04:05:06", 14, -3, 14_706_000_000)]
    [InlineData("-1 days", 0, -1, 0)]
    [InlineData("2 weeks 3 hours 1.5 seconds", 0, 14, 10_801_500_000)]
    [InlineData("1 millisecond 2 microseconds", 0, 0, 1002)]
    public void Interval_text_from_other_writers_parses(string text, int months, int days, long micros) =>
        Assert.Equal(new DuckLakeInterval(months, days, micros), DuckLakeInterval.Parse(text));

    [Theory]
    [InlineData("01:02:03+05:30")]
    [InlineData("01:02:03.1-00:01")]
    [InlineData("12:00:00+15:59:59")]
    [InlineData("24:00:00-15:59:59")]
    [InlineData("00:00:00.000001+00")]
    public void Timetz_text_round_trips_in_DuckDBs_spelling(string text) => Assert.Equal(text, DuckLakeTimeTz.Parse(text).ToString());

    [Theory]
    [InlineData("1 fortnight")]
    [InlineData("1.5 months")]
    [InlineData("0.0000001 seconds")]
    [InlineData("12:00:00")]
    [InlineData("25:00:00+00")]
    [InlineData("12:00:00+16")]
    public void Malformed_interval_and_timetz_text_is_rejected(string text) =>
        Assert.False(DuckLakeValues.TryParse(text.Contains(':') ? DuckLakeType.TimeTz : DuckLakeType.Interval, text, out _));
}

public sealed class SqliteTypeTests10() : TypeTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion10);
public sealed class SqliteTypeTests11() : TypeTests(CatalogBackend.Sqlite, DuckLakeCatalog.FormatVersion11);
public sealed class PostgresTypeTests10() : TypeTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion10);
public sealed class PostgresTypeTests11() : TypeTests(CatalogBackend.Postgres, DuckLakeCatalog.FormatVersion11);
