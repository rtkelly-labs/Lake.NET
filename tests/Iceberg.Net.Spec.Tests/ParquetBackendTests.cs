using System.Numerics;
using System.Text.Json;
using Iceberg.Net.Data;
using Iceberg.Net.Data.ParquetNet;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The two Parquet backends of the data layer, ParquetSharp (parquet-cpp) and Parquet.Net (external/parquet-dotnet):
/// each reads what the other writes, both declare the same schema (types, annotations, field IDs, repetition), and
/// their footer statistics prune row groups alike.
/// </summary>
public sealed class ParquetBackendTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-backends", Guid.NewGuid().ToString("N"));
    private readonly LocalFileIO _io = new();

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
    }

    private static readonly Schema Wide = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "b", PrimitiveType.Boolean),
        NestedField.Optional(3, "i", PrimitiveType.Int),
        NestedField.Optional(4, "f", PrimitiveType.Float),
        NestedField.Optional(5, "d", PrimitiveType.Double),
        NestedField.Optional(6, "date", PrimitiveType.Date),
        NestedField.Optional(7, "time", PrimitiveType.Time),
        NestedField.Optional(8, "ts", PrimitiveType.Timestamp),
        NestedField.Optional(9, "tstz", PrimitiveType.TimestampTz),
        NestedField.Optional(10, "ts_ns", PrimitiveType.TimestampNs),
        NestedField.Optional(11, "s", PrimitiveType.String),
        NestedField.Optional(12, "bin", PrimitiveType.Binary),
        NestedField.Optional(13, "u", PrimitiveType.Uuid),
        NestedField.Optional(14, "d9", new DecimalType(9, 2)),
        NestedField.Optional(15, "d18", new DecimalType(18, 4)),
        NestedField.Optional(16, "d38", new DecimalType(38, 10)),
        NestedField.Optional(17, "fx", new FixedType(3)),
        NestedField.Optional(18, "tags", new ListType(19, PrimitiveType.String, false)),
        NestedField.Optional(20, "attrs", new MapType(21, PrimitiveType.String, 22, PrimitiveType.Long, false)),
        NestedField.Optional(23, "st", new StructType(NestedField.Optional(24, "a", PrimitiveType.Int), NestedField.Require(25, "ns", new ListType(26, PrimitiveType.Int, true)))),
        NestedField.Require(27, "req_list", new ListType(28, PrimitiveType.Long, true)),
        NestedField.Require(29, "req_map", new MapType(30, PrimitiveType.Int, 31, PrimitiveType.String, true)),
    ]);

    private static List<object?[]> Rows(int n)
    {
        var rows = new List<object?[]>();
        for (var k = 0; k < n; k++)
        {
            var nulls = k % 5 == 4;
            rows.Add(
            [
                (long)k,
                nulls ? null : k % 2 == 0,
                nulls ? null : k - 3,
                nulls ? null : k == 1 ? float.NaN : k * 0.5f,
                nulls ? null : k == 2 ? double.NaN : -k * 1.25,
                nulls ? null : 19_000 + k,
                nulls ? null : 3_600_000_000L + k,
                nulls ? null : 1_700_000_000_000_000L + k,
                nulls ? null : -1_000L * k,
                nulls ? null : 1_700_000_000_000_000_123L + k,
                nulls ? null : k == 3 ? "\U0001F600 emoji" : "s" + k,
                nulls ? null : new byte[] { (byte)k, 0, 255 },
                nulls ? null : new Guid(k, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                nulls ? null : new IcebergDecimal(12345 - k * 1000, 2),
                nulls ? null : new IcebergDecimal(-123456789012345 + k, 4),
                nulls ? null : new IcebergDecimal(BigInteger.Pow(10, 37) * (k % 2 == 0 ? 1 : -1) + k, 10),
                nulls ? null : new byte[] { 1, (byte)k, 3 },
                nulls ? null : k % 3 == 0 ? new List<object?>() : new List<object?> { "t" + k, null, "u" },
                nulls ? null : k % 3 == 1 ? new Dictionary<object, object?>() : new Dictionary<object, object?> { ["k" + k] = (long)k, ["z"] = null },
                nulls ? null : new object?[] { k % 2 == 0 ? null : k, new List<object?> { k, k + 1 } },
                new List<object?> { (long)k },
                k % 2 == 0 ? new Dictionary<object, object?>() : new Dictionary<object, object?> { [k] = "v" + k },
            ]);
        }
        return rows;
    }

    private async Task<string> WriteAsync(string backend, Schema schema, List<object?[]> rows, ParquetWriteOptions? options = null)
    {
        using var scope = backend == "parquet-net" ? ParquetNet.Scope() : ParquetNet.ParquetSharpScope();
        var path = Path.Combine(_dir, $"{backend}-{Guid.NewGuid():N}.parquet");
        await ParquetFiles.WriteAsync(_io, path, schema, 0, [], rows, options ?? new ParquetWriteOptions { RowGroupRows = 7 });
        return path;
    }

    private async Task<List<object?[]>> ReadAsync(string backend, string path, Schema schema, DataFileReadOptions? options = null)
    {
        using var scope = backend == "parquet-net" ? ParquetNet.Scope() : ParquetNet.ParquetSharpScope();
        var rows = new List<object?[]>();
        await foreach (var batch in ParquetFiles.ReadBatchesAsync(_io, path, schema, null, options)) rows.AddRange(batch.Rows);
        return rows;
    }

    private static string Canonical(IEnumerable<object?[]> rows) => JsonSerializer.Serialize(rows.Select(r => r.Select(Canon).ToArray()));

    private static object? Canon(object? v) => v switch
    {
        null => null,
        byte[] b => Convert.ToHexString(b),
        float f when float.IsNaN(f) => "NaN",
        double d when double.IsNaN(d) => "NaN",
        System.Collections.IDictionary map => map.Keys.Cast<object>().OrderBy(k => k.ToString(), StringComparer.Ordinal)
            .Select(k => new[] { Canon(k), Canon(map[k]) }).ToArray(),
        System.Collections.IList list when v is not byte[] => list.Cast<object?>().Select(Canon).ToArray(),
        _ => v.ToString(),
    };

    [Theory]
    [InlineData("parquetsharp", "parquet-net")]
    [InlineData("parquet-net", "parquetsharp")]
    [InlineData("parquet-net", "parquet-net")]
    public async Task Each_backend_reads_what_the_other_writes(string writer, string reader)
    {
        var rows = Rows(25);
        var path = await WriteAsync(writer, Wide, rows);
        Assert.Equal(Canonical(rows), Canonical(await ReadAsync(reader, path, Wide)));

        // A projection of a few columns, in another order.
        var projection = new Schema(0, [Wide.Columns[20], Wide.Columns[10], Wide.Columns[0]]);
        var projected = await ReadAsync(reader, path, projection);
        Assert.Equal(Canonical(rows.Select(r => new[] { r[20], r[10], r[0] })), Canonical(projected));
    }

    [Fact]
    public async Task Both_backends_declare_the_same_schema_and_metrics()
    {
        var rows = Rows(25);
        var sharp = await WriteAsync("parquetsharp", Wide, rows);
        var net = await WriteAsync("parquet-net", Wide, rows);
        var sharpFacts = await ParquetInspection.InspectAsync(_io, sharp, Wide);
        var netFacts = await ParquetInspection.InspectAsync(_io, net, Wide);
        Assert.Equal(JsonSerializer.Serialize(sharpFacts.Fields), JsonSerializer.Serialize(netFacts.Fields));
        Assert.Equal((sharpFacts.RecordCount, sharpFacts.RowGroups), (netFacts.RecordCount, netFacts.RowGroups));
        Assert.Empty(netFacts.Incompatible);
        // Footer statistics (counts and bounds) agree, column by column; sizes differ with the encoders.
        foreach (var (id, s) in sharpFacts.Statistics)
        {
            var n = netFacts.Statistics[id];
            Assert.Equal((s.ValueCount, s.NullValueCount), (n.ValueCount, n.NullValueCount));
            Assert.True(Equals(Canon(s.LowerBound), Canon(n.LowerBound)) && Equals(Canon(s.UpperBound), Canon(n.UpperBound)),
                $"field {id}: {Canon(s.LowerBound)}..{Canon(s.UpperBound)} vs {Canon(n.LowerBound)}..{Canon(n.UpperBound)}");
        }
    }

    [Theory]
    [InlineData("parquetsharp")]
    [InlineData("parquet-net")]
    public async Task Statistics_prune_row_groups_alike(string writer)
    {
        var rows = Rows(70); // ten row groups of seven
        var path = await WriteAsync(writer, Wide, rows);
        foreach (var reader in new[] { "parquetsharp", "parquet-net" })
        {
            async Task<(int Rows, int Skipped)> Count(Expression filter)
            {
                var metrics = new ScanMetrics();
                var options = new DataFileReadOptions { Filter = Binder.Bind(Wide, filter), Metrics = metrics };
                var read = await ReadAsync(reader, path, Wide, options);
                return (read.Count, (int)metrics.RowGroupsSkipped);
            }
            // ids 14..20 are the third row group; strings and timestamps bound it too
            Assert.Equal((7, 9), await Count(Expr.And(Expr.GreaterThanOrEqual("id", 14L), Expr.LessThanOrEqual("id", 20L))));
            Assert.Equal((7, 9), await Count(Expr.Equal("tstz", -1_000L * 15)));
            Assert.Equal((7, 9), await Count(Expr.Equal("ts_ns", 1_700_000_000_000_000_123L + 16)));
            // uuids: no bounds from either (orderings writers disagree on), so nothing is pruned
            Assert.Equal((70, 0), await Count(Expr.Equal("u", new Guid(17, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))));
        }
    }

    [Theory]
    [InlineData("parquetsharp", "parquet-net")]
    [InlineData("parquet-net", "parquetsharp")]
    public async Task Annotated_and_encrypted_files_cross_backends(string writer, string reader)
    {
        var schema = new Schema(0,
        [
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "u8", PrimitiveType.Int),
            NestedField.Optional(3, "u32", PrimitiveType.Long),
            NestedField.Optional(4, "u64", new DecimalType(20, 0)),
            NestedField.Optional(5, "ms", PrimitiveType.TimestampTz),
            NestedField.Optional(6, "tutc", PrimitiveType.Time),
            NestedField.Optional(7, "iv", new FixedType(12)),
            NestedField.Optional(8, "i16s", new ListType(9, PrimitiveType.Int, false)),
        ]);
        var annotations = new Dictionary<int, ParquetAnnotation>
        {
            [2] = ParquetAnnotation.UInt8, [3] = ParquetAnnotation.UInt32, [4] = ParquetAnnotation.UInt64, [5] = ParquetAnnotation.TimestampMillis,
            [6] = ParquetAnnotation.TimeUtc, [7] = ParquetAnnotation.Interval, [9] = ParquetAnnotation.Int16,
        };
        var interval = new byte[12];
        interval[0] = 1; interval[4] = 2; interval[8] = 3;
        var rows = new List<object?[]>
        {
            new object?[] { 1L, 255, (long)uint.MaxValue, new IcebergDecimal(ulong.MaxValue, 0), 1_700_000_000_123_000L, 1L, interval, new List<object?> { -32768, null, 32767 } },
            new object?[] { 2L, null, null, null, null, null, null, null },
            new object?[] { 3L, 0, 0L, new IcebergDecimal(0, 0), -1_000L, 86_399_999_999L, new byte[12], new List<object?>() },
        };
        var key = new byte[32];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)i;
        var path = await WriteAsync(writer, schema, rows, new ParquetWriteOptions { Annotations = annotations, EncryptionKey = key });
        Assert.Equal(Canonical(rows), Canonical(await ReadAsync(reader, path, schema, new DataFileReadOptions { DecryptionKey = key })));
        await Assert.ThrowsAnyAsync<Exception>(() => ReadAsync(reader, path, schema));
    }
}
