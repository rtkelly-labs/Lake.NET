using System.Globalization;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Nodes;
using DuckDB.NET.Data;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckDbNet.Tests;

/// <summary>
/// DuckDB's own Parquet reader, in-process, reading the data files this library writes. ParquetSharp (Arrow C++) writes
/// them, so reading them back with Arrow C++ cannot catch a file that only Arrow C++ accepts; DuckDB's reader is an
/// independent implementation. Each test checks one property every other engine relies on: the values, the field IDs
/// (Iceberg resolves columns by them), and the footer statistics (DuckDB prunes row groups on them).
/// </summary>
public sealed class DuckDbParquetReaderTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-duckdbnet", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;
    private readonly DuckDBConnection _duck = new("DataSource=:memory:");

    public DuckDbParquetReaderTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
        _duck.Open();
    }

    public void Dispose()
    {
        _duck.Dispose();
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    internal static readonly Schema AllTypes = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "b", PrimitiveType.Boolean),
        NestedField.Optional(3, "i", PrimitiveType.Int),
        NestedField.Optional(4, "l", PrimitiveType.Long),
        NestedField.Optional(5, "f", PrimitiveType.Float),
        NestedField.Optional(6, "d", PrimitiveType.Double),
        NestedField.Optional(7, "dt", PrimitiveType.Date),
        NestedField.Optional(8, "tm", PrimitiveType.Time),
        NestedField.Optional(9, "ts", PrimitiveType.Timestamp),
        NestedField.Optional(10, "tstz", PrimitiveType.TimestampTz),
        NestedField.Optional(11, "tsns", PrimitiveType.TimestampNs),
        NestedField.Optional(12, "s", PrimitiveType.String),
        NestedField.Optional(13, "bin", PrimitiveType.Binary),
        NestedField.Optional(14, "u", PrimitiveType.Uuid),
        NestedField.Optional(15, "dec9", new DecimalType(9, 2)),
        NestedField.Optional(16, "dec18", new DecimalType(18, 4)),
        NestedField.Optional(17, "dec38", new DecimalType(38, 6)),
        NestedField.Optional(18, "fx", new FixedType(4)),
        NestedField.Optional(19, "st", new StructType([NestedField.Require(30, "a", PrimitiveType.Int), NestedField.Optional(31, "z", PrimitiveType.String)])),
        NestedField.Optional(20, "lst", new ListType(32, PrimitiveType.Long, ElementRequired: false)),
        NestedField.Optional(21, "mp", new MapType(33, PrimitiveType.String, 34, PrimitiveType.Int, ValueRequired: false)),
    ]);

    /// <summary>Iceberg physical values, about one in six null, with the edge cases readers get wrong.</summary>
    internal static List<object?[]> AllTypeRows(int count, int seed)
    {
        var rnd = new Random(seed);
        T? Maybe<T>(Func<T> v) where T : class => rnd.Next(6) == 0 ? null : v();
        object? MaybeV<T>(Func<T> v) where T : struct => rnd.Next(6) == 0 ? null : v();
        return Enumerable.Range(0, count).Select(i => new object?[]
        {
            (long)i,
            MaybeV(() => rnd.Next(2) == 0),
            MaybeV(() => i == 1 ? int.MinValue : i == 2 ? int.MaxValue : rnd.Next(-1000, 1000)),
            MaybeV(() => i == 1 ? long.MinValue : i == 2 ? long.MaxValue : rnd.NextInt64(-1_000_000_000_000, 1_000_000_000_000)),
            MaybeV(() => i == 3 ? -0.0f : (float)(rnd.NextDouble() * 100 - 50)),
            MaybeV(() => i == 4 ? double.NaN : i == 5 ? double.NegativeInfinity : rnd.NextDouble() * 1e6),
            MaybeV(() => i == 1 ? -719_162 /* 0001-01-01 */ : 19_000 + rnd.Next(1000)),
            MaybeV(() => i == 1 ? 86_399_999_999L : rnd.NextInt64(0, 86_400_000_000)),
            MaybeV(() => i == 1 ? -62_135_596_800_000_000L /* 0001-01-01 */ : rnd.NextInt64(0, 2_000_000_000_000_000)),
            MaybeV(() => i == 2 ? -1L /* just before the epoch */ : rnd.NextInt64(0, 2_000_000_000_000_000)),
            MaybeV(() => rnd.NextInt64(0, 2_000_000_000_000_000_000)),
            Maybe(() => i switch
            {
                1 => "",
                2 => "\U0001F600 supplementary",
                3 => " private use", // UTF-16 order puts this above U+1F600; code-point order below it
                _ => rnd.Next(4) == 0 ? $"long-string-value-{i}-beyond-sixteen-chars" : $"s{rnd.Next(50)}",
            }),
            Maybe(() => { var b = new byte[rnd.Next(0, 5)]; rnd.NextBytes(b); return b; }),
            MaybeV(() => new Guid(Enumerable.Range(0, 16).Select(_ => (byte)rnd.Next(256)).ToArray())),
            MaybeV(() => new IcebergDecimal(rnd.Next(-9_999_999, 9_999_999), 2)),
            MaybeV(() => new IcebergDecimal(rnd.NextInt64(-99_999_999_999, 99_999_999_999), 4)),
            MaybeV(() => new IcebergDecimal(i == 1 ? -(BigInteger.Pow(10, 38) - 1) : BigInteger.Parse("123456789012345678901234567", CultureInfo.InvariantCulture) * rnd.Next(-5, 5), 6)),
            Maybe(() => { var b = new byte[4]; rnd.NextBytes(b); return b; }),
            Maybe(() => new object?[] { rnd.Next(100), rnd.Next(3) == 0 ? null : $"z{rnd.Next(9)}" }),
            Maybe(() => (object)Enumerable.Range(0, rnd.Next(4)).Select(k => rnd.Next(4) == 0 ? null : (object?)(long)k).ToList()),
            Maybe(() => (object)Enumerable.Range(0, rnd.Next(3)).ToDictionary(k => (object)$"k{k}", k => rnd.Next(3) == 0 ? null : (object?)k)),
        }).ToList();
    }

    private async Task<(Table Table, IReadOnlyList<DataFile> Files, List<object?[]> Rows)> WriteAllTypesAsync(int formatVersion)
    {
        var table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"all_v{formatVersion}"), AllTypes, FormatVersion: formatVersion, PreserveFieldIds: true));
        var rows = AllTypeRows(300, seed: 17);
        var files = await TableWriter.WriteAsync(table, rows);
        await SnapshotProducer.Append(table).AddFiles(files).CommitAsync();
        return (table, files, rows);
    }

    private static string LocalPath(string location) =>
        location.StartsWith("file:", StringComparison.Ordinal) ? new Uri(location).LocalPath : location;

    /// <summary>One line per row, every column rendered canonically, in id order.</summary>
    private const string CanonicalSelect = """
        SELECT id, b, i, l, f, d,
               date_diff('day', DATE '1970-01-01', dt),
               epoch_us(DATE '1970-01-01' + tm),
               epoch_us(ts), epoch_us(tstz), epoch_ns(tsns),
               s, hex(bin), CAST(u AS VARCHAR),
               CAST(dec9 AS VARCHAR), CAST(dec18 AS VARCHAR), CAST(dec38 AS VARCHAR),
               hex(fx), CAST(to_json(st) AS VARCHAR), CAST(to_json(lst) AS VARCHAR), CAST(to_json(mp) AS VARCHAR)
        FROM read_parquet($path) ORDER BY id
        """;

    private static string Canonical(object?[] row) => string.Join(" | ", row.Select((v, i) => Render(i, v)));

    private static string Render(int column, object? v) => (column, v) switch
    {
        (_, null) => "NULL",
        (_, bool b) => b ? "true" : "false",
        (_, float x) => x.ToString("R", CultureInfo.InvariantCulture),
        (_, double x) => x.ToString("R", CultureInfo.InvariantCulture),
        (_, IcebergDecimal d) => Decimal(d),
        (_, Guid g) => g.ToString("D"),
        (_, byte[] b) => Convert.ToHexString(b),
        (_, object?[] st) => new JsonObject { ["a"] = (int)st[0]!, ["z"] = st[1] is string z ? z : null }.ToJsonString(),
        (_, System.Collections.IDictionary m) => Json(m.Keys.Cast<object>().ToDictionary(k => (string)k, k => m[k] is int x ? (int?)x : null)),
        (_, System.Collections.IEnumerable e) when v is not string => Json(e.Cast<object?>().Select(x => (long?)x).ToList()),
        _ => Convert.ToString(v, CultureInfo.InvariantCulture)!,
    };

    private static string Json<T>(T value) => JsonSerializer.Serialize(value);

    private static string Decimal(IcebergDecimal d)
    {
        var digits = BigInteger.Abs(d.Unscaled).ToString(CultureInfo.InvariantCulture).PadLeft(d.Scale + 1, '0');
        var text = d.Scale == 0 ? digits : $"{digits[..^d.Scale]}.{digits[^d.Scale..]}";
        return d.Unscaled.Sign < 0 ? "-" + text : text;
    }

    private List<string> Query(string sql, string path)
    {
        using var command = _duck.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter("path", path));
        using var reader = command.ExecuteReader();
        var lines = new List<string>();
        while (reader.Read())
            lines.Add(string.Join(" | ", Enumerable.Range(0, reader.FieldCount).Select(i => Render(i, reader.IsDBNull(i) ? null : reader.GetValue(i)))));
        return lines;
    }

    [Spec("Parquet")]
    [Fact]
    public async Task DuckDB_reads_every_type_as_written()
    {
        // v3 because of timestamp_ns; the Parquet encoding of every other type is the same in v2.
        var (_, files, rows) = await WriteAllTypesAsync(3);
        var expected = rows.Select(Canonical).ToList();
        var actual = files.SelectMany(f => Query(CanonicalSelect, LocalPath(f.FilePath))).ToList();
        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
            Assert.True(expected[i] == actual[i], $"row {i}\n  ours:   {expected[i]}\n  duckdb: {actual[i]}");
    }

    [Spec("Parquet")]
    [Fact]
    public async Task Every_column_and_nested_field_carries_its_field_id()
    {
        var (_, files, _) = await WriteAllTypesAsync(3);
        var expected = new HashSet<int>(Enumerable.Range(1, 21).Concat([30, 31, 32, 33, 34]));
        foreach (var file in files)
        {
            using var command = _duck.CreateCommand();
            command.CommandText = "SELECT name, field_id FROM parquet_schema($path)";
            command.Parameters.Add(new DuckDBParameter("path", LocalPath(file.FilePath)));
            using var reader = command.ExecuteReader();
            var ids = new HashSet<int>();
            var unnumbered = new List<string>();
            var first = true;
            while (reader.Read())
            {
                var name = reader.GetString(0);
                if (first) { first = false; continue; } // the root
                if (reader.IsDBNull(1)) unnumbered.Add(name);
                else Assert.True(ids.Add(Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture)), $"field id on {name} repeated");
            }
            Assert.Equal(expected.Order(), ids.Order());
            // Only the spec's repeated wrapper groups go without an id: list's "list", map's "key_value".
            Assert.All(unnumbered, n => Assert.Contains(n, new[] { "list", "key_value" }));
        }
    }

    [Spec("Parquet")]
    [Fact]
    public async Task Footer_statistics_agree_with_the_manifest_metrics()
    {
        var (_, files, rows) = await WriteAllTypesAsync(3);
        var leafIds = new Dictionary<string, int> { ["i"] = 3, ["l"] = 4, ["dt"] = 7, ["s"] = 12 };
        foreach (var file in files)
        {
            using var command = _duck.CreateCommand();
            command.CommandText = """
                SELECT path_in_schema, CAST(sum(stats_null_count) AS BIGINT), min(stats_min_value), max(stats_max_value)
                FROM parquet_metadata($path) GROUP BY path_in_schema
                """;
            command.Parameters.Add(new DuckDBParameter("path", LocalPath(file.FilePath)));
            using var reader = command.ExecuteReader();
            var seen = new HashSet<string>();
            while (reader.Read())
            {
                var path = reader.GetString(0);
                if (!leafIds.TryGetValue(path, out var id)) continue;
                seen.Add(path);
                var nulls = Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
                Assert.Equal(file.NullValueCounts![id], nulls);
                Assert.Equal(rows.LongCount(r => r[id - 1] is null), nulls);
                if (path is "i" or "l")
                {
                    // The manifest bounds are the true extremes; the footer's own per-group bounds are checked against
                    // DuckDB's reads in Every_row_group_statistic_bounds_the_values_DuckDB_reads_from_that_group.
                    var lower = id == 3 ? BitConverter.ToInt32(file.LowerBounds![id]) : BitConverter.ToInt64(file.LowerBounds![id]);
                    var upper = id == 3 ? BitConverter.ToInt32(file.UpperBounds![id]) : BitConverter.ToInt64(file.UpperBounds![id]);
                    var values = rows.Where(r => r[id - 1] is not null).Select(r => Convert.ToInt64(r[id - 1], CultureInfo.InvariantCulture)).ToList();
                    Assert.Equal(values.Min(), lower);
                    Assert.Equal(values.Max(), upper);
                }
            }
            Assert.Equal(leafIds.Keys.Order(), seen.Order());
        }
    }

    [Spec("Parquet")]
    [Theory]
    [InlineData("l")]
    [InlineData("i")]
    [InlineData("s")] // code-point order: UTF-16 order would misplace U+E000 against U+1F600
    [InlineData("dt")]
    public async Task Every_row_group_statistic_bounds_the_values_DuckDB_reads_from_that_group(string column)
    {
        // DuckDB prunes row groups on these bounds: one that excludes a value the group holds makes DuckDB skip rows.
        var (_, files, _) = await WriteAllTypesAsync(3);
        foreach (var file in files)
        {
            var path = LocalPath(file.FilePath);
            var groups = new List<(long Rows, string? Lo, string? Hi)>();
            using (var meta = _duck.CreateCommand())
            {
                meta.CommandText = """
                    SELECT row_group_num_rows, stats_min_value, stats_max_value FROM parquet_metadata($path)
                    WHERE path_in_schema = $column ORDER BY row_group_id
                    """;
                meta.Parameters.Add(new DuckDBParameter("path", path));
                meta.Parameters.Add(new DuckDBParameter("column", column));
                using var reader = meta.ExecuteReader();
                while (reader.Read())
                    groups.Add((Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
                        reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }
            Assert.NotEmpty(groups);
            var start = 0L;
            foreach (var (count, lo, hi) in groups)
            {
                Assert.NotNull(lo);
                Assert.NotNull(hi);
                using var outside = _duck.CreateCommand();
                var typed = column == "dt" ? "CAST({0} AS DATE)" : column == "s" ? "{0}" : "CAST({0} AS BIGINT)";
                outside.CommandText = $"""
                    SELECT count(*) FROM read_parquet($path, file_row_number = true)
                    WHERE file_row_number >= $start AND file_row_number < $end
                      AND ({column} < {string.Format(CultureInfo.InvariantCulture, typed, "$lo")} OR {column} > {string.Format(CultureInfo.InvariantCulture, typed, "$hi")})
                    """;
                outside.Parameters.Add(new DuckDBParameter("path", path));
                outside.Parameters.Add(new DuckDBParameter("start", start));
                outside.Parameters.Add(new DuckDBParameter("end", start + count));
                outside.Parameters.Add(new DuckDBParameter("lo", lo));
                outside.Parameters.Add(new DuckDBParameter("hi", hi));
                Assert.True(Convert.ToInt64(outside.ExecuteScalar(), CultureInfo.InvariantCulture) == 0,
                    $"{column}: values outside [{lo}, {hi}] in the row group starting at row {start}");
                start += count;
            }
        }
    }

    [Spec("Parquet")]
    [Fact]
    public async Task Files_DuckDB_writes_with_field_ids_import_and_read_back_the_same()
    {
        var schema = new Schema(0,
        [
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "name", PrimitiveType.String),
            NestedField.Optional(3, "day", PrimitiveType.Date),
            NestedField.Optional(4, "seen_at", PrimitiveType.Timestamp),
            NestedField.Optional(5, "amount", new DecimalType(18, 4)),
        ]);
        var table = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "from_duckdb"), schema));
        var path = Path.Combine(_warehouse, "external", "duckdb.parquet");
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        using (var command = _duck.CreateCommand())
        {
            command.CommandText = $$"""
                COPY (
                  SELECT i::BIGINT AS id,
                         CASE WHEN i % 7 = 0 THEN NULL ELSE 'n' || i END AS name,
                         DATE '2024-01-01' + i::INTEGER AS day,
                         TIMESTAMP '2024-01-01 00:00:00' + to_microseconds(i * 1000) AS seen_at,
                         (i * 1.25)::DECIMAL(18,4) AS amount
                  FROM range(1000) t(i)
                ) TO '{{path.Replace("'", "''", StringComparison.Ordinal)}}'
                (FORMAT parquet, FIELD_IDS {id: 1, name: 2, day: 3, seen_at: 4, amount: 5}, ROW_GROUP_SIZE 300)
                """;
            command.ExecuteNonQuery();
        }

        var dataFile = await ParquetImport.ToDataFileAsync(table, path);
        Assert.Equal(1000, dataFile.RecordCount);
        await SnapshotProducer.Append(table).AddFiles([dataFile]).CommitAsync();

        var ours = (await TableScan.ReadAllAsync(table)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal(1000, ours.Count);
        using var check = _duck.CreateCommand();
        check.CommandText = """
            SELECT id, name, date_diff('day', DATE '1970-01-01', day), epoch_us(seen_at), CAST(amount AS VARCHAR)
            FROM read_parquet($path) ORDER BY id
            """;
        check.Parameters.Add(new DuckDBParameter("path", path));
        using var reader = check.ExecuteReader();
        var i = 0;
        while (reader.Read())
        {
            var duck = string.Join(" | ", Enumerable.Range(0, 5).Select(c => Render(c, reader.IsDBNull(c) ? null : reader.GetValue(c))));
            Assert.Equal(duck, Canonical(ours[i++]));
        }
        Assert.Equal(1000, i);
    }
}
