using Iceberg.Net.Arrow;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.IO;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The read path behind <see cref="TableScan.ReadRowsAsync"/>: column projection by field ID, Parquet row-group pruning on
/// footer statistics, delete positions across skipped groups, streaming one row group at a time, time travel binding and
/// metadata-only counts. Every optimisation is checked against an unoptimised oracle.
/// </summary>
public sealed class ReadPathTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-readpath", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public ReadPathTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("r")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    // ---------- helpers ----------

    private async Task<Table> Create(Schema schema, int formatVersion = 2) =>
        await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("r", $"t{Guid.NewGuid():N}"), schema, PartitionSpec.Unpartitioned, FormatVersion: formatVersion));

    private Task<Table> Reload(Table t) => _catalog.LoadTableAsync(t.Identifier);

    /// <summary>Write one data file with the given layout (Parquet row groups of <paramref name="rowGroupRows"/>, or Avro) and append it.</summary>
    private async Task<(Table Table, DataFile File)> AppendFile(Table t, IReadOnlyList<object?[]> rows, int rowGroupRows = 10, string format = "PARQUET", bool statistics = true)
    {
        var location = $"{t.Location}/data/{Guid.NewGuid():N}.{format.ToLowerInvariant()}";
        var file = format == "AVRO"
            ? await AvroDataFiles.WriteAsync(t.IO, location, t.Schema, 0, [], rows)
            : await ParquetFiles.WriteAsync(t.IO, location, t.Schema, 0, [], rows, new ParquetWriteOptions { RowGroupRows = rowGroupRows, Statistics = statistics });
        await SnapshotProducer.Append(t).AddFiles([file]).CommitAsync();
        return (await Reload(t), file);
    }

    private static async Task<List<object?[]>> Read(Table t, ScanRequest r)
    {
        var rows = new List<object?[]>();
        await foreach (var row in TableScan.ReadRowsAsync(t, r)) rows.Add(row);
        return rows;
    }

    private static string Canonical(IEnumerable<object?[]> rows) => NestedTypesTests.Canonical(rows);

    private static string Sorted(IEnumerable<object?[]> rows) =>
        string.Join("\n", rows.Select(r => NestedTypesTests.Canonical([r])).Order(StringComparer.Ordinal));

    // ---------- column projection ----------

    private static readonly Schema AllTypes = new(0,
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

    private static List<object?[]> AllTypeRows(int start, int count, int seed)
    {
        var rnd = new Random(seed);
        T? Maybe<T>(Func<T> v) where T : class => rnd.Next(6) == 0 ? null : v();
        object? MaybeV<T>(Func<T> v) where T : struct => rnd.Next(6) == 0 ? null : v();
        return Enumerable.Range(start, count).Select(i => new object?[]
        {
            (long)i,
            MaybeV(() => rnd.Next(2) == 0),
            MaybeV(() => rnd.Next(-1000, 1000)),
            MaybeV(() => rnd.NextInt64(-1_000_000_000_000, 1_000_000_000_000)),
            MaybeV(() => (float)(rnd.NextDouble() * 100 - 50)),
            MaybeV(() => rnd.Next(20) == 0 ? double.NaN : rnd.NextDouble() * 1e6),
            MaybeV(() => 19_000 + rnd.Next(1000)),
            MaybeV(() => rnd.NextInt64(0, 86_400_000_000)),
            MaybeV(() => rnd.NextInt64(0, 2_000_000_000_000_000)),
            MaybeV(() => rnd.NextInt64(0, 2_000_000_000_000_000)),
            MaybeV(() => rnd.NextInt64(0, 2_000_000_000_000_000_000)),
            Maybe(() => rnd.Next(4) == 0 ? $"long-string-value-{i}-beyond-sixteen-chars" : $"s{rnd.Next(50)}\U0001F600"),
            Maybe(() => { var b = new byte[rnd.Next(0, 5)]; rnd.NextBytes(b); return b; }),
            MaybeV(() => new Guid(Enumerable.Range(0, 16).Select(_ => (byte)rnd.Next(256)).ToArray())),
            MaybeV(() => new IcebergDecimal(rnd.Next(-9_999_999, 9_999_999), 2)),
            MaybeV(() => new IcebergDecimal(rnd.NextInt64(-99_999_999_999, 99_999_999_999), 4)),
            MaybeV(() => new IcebergDecimal(System.Numerics.BigInteger.Parse("123456789012345678901234567") * rnd.Next(-5, 5), 6)),
            Maybe(() => { var b = new byte[4]; rnd.NextBytes(b); return b; }),
            Maybe(() => new object?[] { rnd.Next(100), rnd.Next(3) == 0 ? null : $"z{rnd.Next(9)}" }),
            Maybe(() => (object)Enumerable.Range(0, rnd.Next(4)).Select(k => rnd.Next(4) == 0 ? null : (object?)(long)k).ToList()),
            Maybe(() => (object)Enumerable.Range(0, rnd.Next(3)).ToDictionary(k => (object)$"k{k}", k => rnd.Next(3) == 0 ? null : (object?)k)),
        }).ToList();
    }

    [Spec("Column Projection")]
    [Spec("Default values")]
    [Spec("Nested Types")]
    [Fact]
    public async Task Projection_returns_the_same_values_as_a_full_read_for_every_type_nested_column_and_default()
    {
        var t = await Create(AllTypes, formatVersion: 3);
        (t, _) = await AppendFile(t, AllTypeRows(0, 45, seed: 1), rowGroupRows: 7);
        (t, _) = await AppendFile(t, AllTypeRows(45, 20, seed: 2), format: "AVRO");
        await t.UpdateSchemaAsync(u => u.AddRequiredColumn("region", PrimitiveType.String, "eu"));
        t = await Reload(t);

        var schema = TableScan.SchemaOf(t, new ScanRequest());
        var full = await Read(t, new ScanRequest());
        Assert.Equal(65, full.Count);
        Assert.All(full, r => Assert.Equal("eu", r[^1])); // the initial default, for files written before the column
        for (var c = 0; c < schema.Columns.Count; c++)
        {
            var name = schema.Columns[c].Name;
            var projected = await Read(t, new ScanRequest { Columns = [name] });
            Assert.Equal(Canonical(full.Select(r => new[] { r[c] })), Canonical(projected));
        }

        // Several columns, out of schema order, with a filter on a column that is not returned.
        string[] some = ["mp", "s", "st", "region", "dec38"];
        var idx = some.Select(n => schema.Columns.ToList().FindIndex(f => f.Name == n)).ToArray();
        var filter = Expr.And(Expr.GreaterThan("i", 0), Expr.NotNull("u"));
        var matches = RowEvaluator.Compile(Binder.Bind(schema, filter));
        Assert.Equal(
            Canonical(full.Where(r => matches(r)).Select(r => idx.Select(i => r[i]).ToArray())),
            Canonical(await Read(t, new ScanRequest { Columns = some, Filter = filter })));

        // A filter on a field inside a struct that is not returned.
        var nested = Expr.LessThan("st.a", 50);
        var nestedMatches = RowEvaluator.Compile(Binder.Bind(schema, nested));
        Assert.Equal(Canonical(full.Where(r => nestedMatches(r)).Select(r => new[] { r[0] })), Canonical(await Read(t, new ScanRequest { Columns = ["id"], Filter = nested })));

        // The legacy list API is the same scan.
        Assert.Equal(Canonical(full), Canonical(await TableScan.ReadAllAsync(t)));
    }

    [Spec("Column Projection")]
    [Spec("Equality Delete Files")]
    [Fact]
    public async Task Projection_still_reads_the_columns_equality_deletes_match_on()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)], [1]);
        var t = await Create(schema);
        await TableMutations.InsertAsync(t, Enumerable.Range(0, 10).Select(i => new object?[] { (long)i, $"n{i}" }).ToList());
        await TableMutations.DeleteByKeysAsync(await Reload(t), ["id"], [[3L], [7L]], DeleteStrategy.EqualityDeletes);
        t = await Reload(t);
        Assert.NotEmpty((await TableScan.DeletesAsync(t)).EqualityDeletes);
        var names = (await Read(t, new ScanRequest { Columns = ["name"] })).Select(r => (string)r[0]!).Order().ToList();
        Assert.Equal(Enumerable.Range(0, 10).Where(i => i is not (3 or 7)).Select(i => $"n{i}").Order(), names);
        Assert.Equal(8, await TableScan.CountAsync(t, new ScanRequest()));
    }

    // ---------- row-group pruning ----------

    private static readonly Schema Prunable = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "cat", PrimitiveType.String),
        NestedField.Optional(3, "score", PrimitiveType.Double),
        NestedField.Optional(4, "day", PrimitiveType.Date),
        NestedField.Optional(5, "amt", new DecimalType(9, 2)),
        NestedField.Optional(6, "flag", PrimitiveType.Boolean),
        NestedField.Optional(7, "name", PrimitiveType.String),
        NestedField.Optional(8, "pt", new StructType([NestedField.Optional(20, "x", PrimitiveType.Int)])),
        NestedField.Optional(9, "small", PrimitiveType.Int),
    ]);

    private static readonly string[] Names = ["", "a", "ab", "abc", "b", "�", "\U0001F600", "\U0001F600x", "zz", "a-very-long-name-longer-than-sixteen"];

    /// <summary>Rows sorted by id and clustered by day, so row groups have narrow, mostly disjoint ranges.</summary>
    private static List<object?[]> PrunableRows(int start, int count, int seed)
    {
        var rnd = new Random(seed);
        return Enumerable.Range(start, count).Select(i => new object?[]
        {
            (long)i,
            rnd.Next(5) switch { 0 => null, 1 => "a", 2 => "b", _ => i < start + count / 2 ? "c" : "d" },
            rnd.Next(8) switch { 0 => null, 1 => double.NaN, _ => i * 1.5 },
            i % 10 == 0 ? null : 19_000 + i / 10,
            new IcebergDecimal(i * 100 - 5_000, 2),
            i % 3 == 0 ? null : i % 2 == 0,
            Names[(i / 7 + rnd.Next(2)) % Names.Length],
            rnd.Next(6) == 0 ? null : new object?[] { rnd.Next(6) == 0 ? null : i / 10 },
            i / 10 % 2 == 0 ? null : (object?)(i % 4), // whole row groups of nulls
        }).ToList();
    }

    private async Task<Table> PrunableTable()
    {
        var t = await Create(Prunable);
        (t, _) = await AppendFile(t, PrunableRows(0, 60, 1), rowGroupRows: 10);
        (t, _) = await AppendFile(t, PrunableRows(60, 30, 2), rowGroupRows: 10, statistics: false); // a writer that omits statistics
        (t, _) = await AppendFile(t, PrunableRows(90, 30, 3), rowGroupRows: 7);
        (t, _) = await AppendFile(t, PrunableRows(120, 15, 4), format: "AVRO");
        return t;
    }

    [Spec("Scan Planning")]
    [Spec("Parquet")]
    [Fact]
    public async Task Row_groups_are_skipped_on_footer_statistics_and_groups_without_statistics_are_read()
    {
        var t = await PrunableTable();
        var metrics = new ScanMetrics();
        var rows = await Read(t, new ScanRequest { Filter = Expr.Equal("id", 25L), Metrics = metrics });
        Assert.Equal(25L, Assert.Single(rows)[0]);
        Assert.Equal(1, metrics.FilesPlanned); // the other files are pruned on file metrics
        Assert.Equal(1, metrics.RowGroupsRead);
        Assert.Equal(5, metrics.RowGroupsSkipped);

        // The file written without statistics: file-level metrics still prune files, but every row group is read.
        metrics = new ScanMetrics();
        rows = await Read(t, new ScanRequest { Filter = Expr.Equal("id", 75L), Metrics = metrics });
        Assert.Equal(75L, Assert.Single(rows)[0]);
        Assert.Equal(3, metrics.RowGroupsRead);
        Assert.Equal(0, metrics.RowGroupsSkipped);

        // A whole group of nulls: comparisons skip it, IS NULL reads only those groups.
        metrics = new ScanMetrics();
        var ids = (await Read(t, new ScanRequest { Filter = Expr.IsNull("small"), Columns = ["id"], Metrics = metrics })).Select(r => (long)r[0]!).ToList();
        Assert.Equal(Enumerable.Range(0, 135).Where(i => i / 10 % 2 == 0).Select(i => (long)i), ids.Order());
        Assert.True(metrics.RowGroupsSkipped >= 3, metrics.ToString());

        // A field inside a struct, and a string range over code points beyond the BMP.
        metrics = new ScanMetrics();
        await Read(t, new ScanRequest { Filter = Expr.GreaterThanOrEqual("pt.x", 5), Metrics = metrics });
        Assert.True(metrics.RowGroupsSkipped > 0, metrics.ToString());
        metrics = new ScanMetrics();
        await Read(t, new ScanRequest { Filter = Expr.Equal("day", 19_002), Metrics = metrics });
        Assert.True(metrics.RowGroupsSkipped > 0, metrics.ToString());
    }

    private static Expression RandomPredicate(Random rnd)
    {
        object Pick(params object[] xs) => xs[rnd.Next(xs.Length)];
        var column = (string)Pick("id", "cat", "score", "day", "amt", "flag", "name", "pt.x", "small");
        object Literal() => column switch
        {
            "id" => (long)rnd.Next(-5, 140),
            "cat" => Pick("a", "b", "c", "d", "e", ""),
            "score" => rnd.Next(-2, 210) * 0.75,
            "day" => 19_000 + rnd.Next(-1, 15),
            "amt" => new IcebergDecimal(rnd.Next(-6_000, 10_000), 2),
            "flag" => rnd.Next(2) == 0,
            "name" => Pick(Names.Cast<object>().Concat(["a-very", "\U0001F600", "￼", "y"]).ToArray()),
            "pt.x" => rnd.Next(-1, 15),
            _ => rnd.Next(-1, 5),
        };
        var ops = column is "cat" or "name"
            ? new[] { Operation.Lt, Operation.LtEq, Operation.Gt, Operation.GtEq, Operation.Eq, Operation.NotEq, Operation.In, Operation.NotIn, Operation.IsNull, Operation.NotNull, Operation.StartsWith, Operation.NotStartsWith }
            : new[] { Operation.Lt, Operation.LtEq, Operation.Gt, Operation.GtEq, Operation.Eq, Operation.NotEq, Operation.In, Operation.NotIn, Operation.IsNull, Operation.NotNull, Operation.IsNaN, Operation.NotNaN };
        var op = ops[rnd.Next(ops.Length)];
        return op switch
        {
            Operation.IsNull => Expr.IsNull(column),
            Operation.NotNull => Expr.NotNull(column),
            Operation.IsNaN => Expr.IsNaN(column),
            Operation.NotNaN => Expr.NotNaN(column),
            Operation.In => Expr.In(column, Enumerable.Range(0, rnd.Next(1, 4)).Select(_ => Literal()).ToArray()),
            Operation.NotIn => Expr.NotIn(column, Enumerable.Range(0, rnd.Next(1, 4)).Select(_ => Literal()).ToArray()),
            Operation.StartsWith => Expr.StartsWith(column, (string)Literal()),
            Operation.NotStartsWith => Expr.NotStartsWith(column, (string)Literal()),
            _ => new UnboundPredicate(column, op, [Literal()]),
        };
    }

    private static Expression RandomFilter(Random rnd, int depth = 0) => (depth < 2 ? rnd.Next(5) : 0) switch
    {
        1 => Expr.And(RandomFilter(rnd, depth + 1), RandomFilter(rnd, depth + 1)),
        2 => Expr.Or(RandomFilter(rnd, depth + 1), RandomFilter(rnd, depth + 1)),
        3 => Expr.Not(RandomFilter(rnd, depth + 1)),
        _ => RandomPredicate(rnd),
    };

    [Spec("Scan Planning")]
    [Spec("Parquet")]
    [Fact]
    public async Task Pruning_never_changes_results_random_filters_against_a_full_scan_oracle()
    {
        var t = await PrunableTable();
        // Oracle: every row of every file, decoded with no pruning at all, then filtered.
        var all = new List<object?[]>();
        foreach (var task in await TableScan.PlanFilesAsync(t))
            all.AddRange(await DataFiles.ReadAsync(t.IO, task.File, t.Schema));
        Assert.Equal(135, all.Count);

        var rnd = new Random(20261023);
        var totals = new ScanMetrics();
        for (var n = 0; n < 250; n++)
        {
            var filter = RandomFilter(rnd);
            var matches = RowEvaluator.Compile(Binder.Bind(t.Schema, filter));
            var expected = Sorted(all.Where(r => matches(r)));
            var metrics = new ScanMetrics();
            Assert.True(expected == Sorted(await Read(t, new ScanRequest { Filter = filter, Metrics = metrics })), $"filter {filter}: {metrics}");
            Assert.True(expected == Sorted(await TableScan.ReadAsync(t, filter)), $"legacy ReadAsync, filter {filter}");
            Assert.Equal(all.Count(r => matches(r)), await TableScan.CountAsync(t, new ScanRequest { Filter = filter, Metrics = totals }));
        }
        Assert.True(totals.RowGroupsSkipped > 100, totals.ToString()); // the filters exercised row-group pruning, not just file pruning
    }

    // ---------- deletes across skipped row groups ----------

    private static List<object?[]> Simple(int count) => Enumerable.Range(0, count).Select(i => new object?[] { (long)i, $"n{i}" }).ToList();
    private static readonly Schema SimpleSchema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String)]);

    [Spec("Deletion Vectors")]
    [Spec("Position Delete Files")]
    [Spec("Scan Planning")]
    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Deletes_keep_absolute_positions_when_earlier_row_groups_are_skipped(int formatVersion)
    {
        var t = await Create(SimpleSchema, formatVersion);
        DataFile file;
        (t, file) = await AppendFile(t, Simple(40), rowGroupRows: 10);
        await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [file.FilePath] = [2, 35, 36] }).CommitAsync();
        t = await Reload(t);
        var deletes = await TableScan.DeletesAsync(t);
        if (formatVersion == 3) Assert.NotNull(deletes.DeletionVectorFor(file.FilePath));

        var metrics = new ScanMetrics();
        var late = await Read(t, new ScanRequest { Filter = Expr.GreaterThanOrEqual("id", 30L), Metrics = metrics });
        Assert.Equal(3, metrics.RowGroupsSkipped); // positions 0-29 never decoded
        Assert.Equal(new long[] { 30, 31, 32, 33, 34, 37, 38, 39 }, late.Select(r => (long)r[0]!));
        Assert.Equal(2, metrics.RowsDeleted);

        var early = await Read(t, new ScanRequest { Filter = Expr.LessThan("id", 10L), Columns = ["name"] });
        Assert.Equal(Enumerable.Range(0, 10).Where(i => i != 2).Select(i => $"n{i}"), early.Select(r => (string)r[0]!));
        Assert.Equal(37, await TableScan.CountAsync(t, new ScanRequest()));
        Assert.Equal(8, await TableScan.CountAsync(t, new ScanRequest { Filter = Expr.GreaterThanOrEqual("id", 30L) }));
    }

    // ---------- streaming ----------

    [Spec("Scan Planning")]
    [Fact]
    public async Task Reads_stream_one_row_group_at_a_time()
    {
        var t = await Create(SimpleSchema);
        (t, _) = await AppendFile(t, Simple(50), rowGroupRows: 10);
        var metrics = new ScanMetrics();
        await using (var rows = TableScan.ReadRowsAsync(t, new ScanRequest { Metrics = metrics }).GetAsyncEnumerator())
        {
            for (var i = 0; i < 50; i++)
            {
                Assert.True(await rows.MoveNextAsync());
                Assert.Equal((long)i, rows.Current[0]);
                // Row i is produced once its own group is decoded, before any later group.
                Assert.Equal(i / 10 + 1, metrics.RowGroupsRead);
                Assert.Equal((i / 10 + 1) * 10, metrics.RowsDecoded);
            }
            Assert.False(await rows.MoveNextAsync());
        }
        Assert.Equal(10, metrics.MaxBatchRows);

        var batches = new List<int>();
        await foreach (var batch in TableArrow.ReadBatchesAsync(t, new ScanRequest { Columns = ["name"] })) batches.Add(batch.Length);
        Assert.Equal([10, 10, 10, 10, 10], batches);
        batches.Clear();
        await foreach (var batch in TableArrow.ReadBatchesAsync(t, Expr.GreaterThan("id", 25L))) batches.Add(batch.Length);
        Assert.Equal([4, 10, 10], batches); // the first two groups are skipped on statistics
    }

    // ---------- time travel ----------

    [Spec("Point in Time Reads (Time Travel)")]
    [Spec("Schema Evolution")]
    [Fact]
    public async Task Time_travel_binds_filters_and_columns_to_the_snapshot_schema()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String), NestedField.Optional(3, "extra", PrimitiveType.Int)]);
        var t = await Create(schema);
        var s1 = (await TableMutations.InsertAsync(t, [[1L, "a", 10], [2L, "b", 20]])).Snapshot.SnapshotId;
        await (await Reload(t)).UpdateSchemaAsync(u => u.RenameColumn("name", "title").DeleteColumn("extra"));
        t = await Reload(t);
        await TableMutations.InsertAsync(t, [[3L, "c"]]);
        t = await Reload(t);

        var old = new ScanRequest { Snapshot = SnapshotSelector.Id(s1), Filter = Expr.Equal("name", "b"), Columns = ["extra", "name"] };
        Assert.Equal(["extra", "name"], TableScan.SchemaOf(t, old).Columns.Select(c => c.Name));
        Assert.Equal("(20, \"b\")", Canonical(await Read(t, old)));
        Assert.Single((await TableScan.PlanAsync(t, old)).Tasks);
        Assert.Equal(1, await TableScan.CountAsync(t, old));
        // As of the instant of s1, too.
        var at = DateTimeOffset.FromUnixTimeMilliseconds(t.Metadata.SnapshotLog.Single(e => e.SnapshotId == s1).TimestampMs);
        Assert.Equal("(20, \"b\")", Canonical(await Read(t, old with { Snapshot = SnapshotSelector.AsOf(at) })));
        // The current names do not exist at s1, nor the dropped one now.
        await Assert.ThrowsAsync<ArgumentException>(() => Read(t, old with { Filter = Expr.Equal("title", "b"), Columns = null }));
        await Assert.ThrowsAsync<ArgumentException>(() => Read(t, new ScanRequest { Filter = Expr.Equal("extra", 20) }));
        Assert.Equal(["c"], (await Read(t, new ScanRequest { Filter = Expr.GreaterThan("id", 2L), Columns = ["title"] })).Select(r => (string)r[0]!));

        // Decision: the legacy list API keeps the current schema for an old snapshot (positional callers rely on it).
        var legacy = await TableScan.ReadAllAsync(t, s1);
        Assert.All(legacy, r => Assert.Equal(2, r.Length));
        Assert.Equal("(1, \"a\")\n(2, \"b\")", Canonical(legacy.OrderBy(r => (long)r[0]!)));
        Assert.Single(await TableScan.ReadAsync(t, Expr.Equal("title", "b"), s1));
    }

    // ---------- counts ----------

    [Spec("Scan Planning")]
    [Spec("Manifests")]
    [Fact]
    public async Task Count_uses_manifest_record_counts_unless_a_filter_or_delete_needs_the_data()
    {
        var t = await Create(SimpleSchema, formatVersion: 3);
        DataFile first;
        (t, first) = await AppendFile(t, Simple(30), rowGroupRows: 10);
        var s1 = t.Metadata.CurrentSnapshotId!.Value;
        (t, _) = await AppendFile(t, Simple(12), rowGroupRows: 5, format: "AVRO");

        var metrics = new ScanMetrics();
        Assert.Equal(42, await TableScan.CountAsync(t, new ScanRequest { Metrics = metrics }));
        Assert.Equal(0, metrics.RowGroupsRead); // no data file opened
        Assert.Equal(2, metrics.FilesPlanned);

        metrics = new ScanMetrics();
        Assert.Equal(12, await TableScan.CountAsync(t, new ScanRequest { Filter = Expr.LessThan("id", 6L), Metrics = metrics }));
        Assert.True(metrics.RowGroupsRead > 0);
        Assert.Equal(2, metrics.RowGroupsSkipped); // ids 10-29 of the Parquet file, two row groups
        Assert.Equal(30, await TableScan.CountAsync(t, new ScanRequest { Snapshot = SnapshotSelector.Id(s1) }));

        await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [first.FilePath] = [0, 29] }).CommitAsync();
        t = await Reload(t);
        metrics = new ScanMetrics();
        Assert.Equal(40, await TableScan.CountAsync(t, new ScanRequest { Metrics = metrics }));
        Assert.True(metrics.RowGroupsRead > 0); // deletes force the scan path
        Assert.Equal(40, (await Read(t, new ScanRequest())).Count);
        Assert.Equal(30, await TableScan.CountAsync(t, new ScanRequest { Snapshot = SnapshotSelector.Id(s1) })); // before the delete
    }

    // ---------- ranged reads ----------

    /// <summary>Counts what is downloaded: whole-object reads and ranged reads.</summary>
    private sealed class CountingFileIO(IFileIO inner) : IFileIO
    {
        public long BytesRead, RangeRequests, WholeReads;
        public async Task<byte[]> ReadAllAsync(string location, CancellationToken ct = default)
        {
            var b = await inner.ReadAllAsync(location, ct);
            Interlocked.Add(ref BytesRead, b.Length);
            Interlocked.Increment(ref WholeReads);
            return b;
        }
        public Task<Stream> OpenReadAsync(string location, CancellationToken ct = default) => throw new InvalidOperationException("unmetered stream");
        public async Task<byte[]> ReadRangeAsync(string location, long offset, int length, CancellationToken ct = default)
        {
            var b = await inner.ReadRangeAsync(location, offset, length, ct);
            Interlocked.Add(ref BytesRead, b.Length);
            Interlocked.Increment(ref RangeRequests);
            return b;
        }
        public Task<long> LengthAsync(string location, CancellationToken ct = default) => inner.LengthAsync(location, ct);
        public Task<bool> ExistsAsync(string location, CancellationToken ct = default) => inner.ExistsAsync(location, ct);
        public Task WriteAsync(string location, ReadOnlyMemory<byte> content, bool overwrite = false, CancellationToken ct = default) => inner.WriteAsync(location, content, overwrite, ct);
        public Task DeleteAsync(string location, CancellationToken ct = default) => inner.DeleteAsync(location, ct);
        public void Dispose() { }
    }

    private static readonly Schema WideSchema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "a", PrimitiveType.Long),
        NestedField.Optional(3, "b", PrimitiveType.Long),
        NestedField.Optional(4, "s", new StructType([NestedField.Optional(5, "x", PrimitiveType.Long), NestedField.Optional(6, "y", PrimitiveType.Long)])),
    ]);

    /// <summary>A ~4 MB file: 100k rows of incompressible longs in 4 row groups, ids ascending.</summary>
    private async Task<DataFile> WideFile(IFileIO io)
    {
        var random = new Random(7);
        var rows = Enumerable.Range(0, 100_000).Select(i => new object?[] { (long)i, random.NextInt64(), random.NextInt64(), new object?[] { random.NextInt64(), random.NextInt64() } }).ToList();
        var location = Path.Combine(_warehouse, "wide", $"{Guid.NewGuid():N}.parquet");
        var file = await ParquetFiles.WriteAsync(io, location, WideSchema, 0, [], rows, new ParquetWriteOptions { RowGroupRows = 25_000 });
        Assert.True(file.FileSizeInBytes > 3_000_000, $"{file.FileSizeInBytes}");
        return file;
    }

    private static async Task<List<object?[]>> ReadFile(IFileIO io, DataFile file, Schema projection, Expression? filter = null)
    {
        var rows = new List<object?[]>();
        var options = new DataFileReadOptions { Filter = filter is null ? null : Binder.Bind(WideSchema, filter) };
        await foreach (var batch in DataFiles.ReadBatchesAsync(io, file, projection, null, options)) rows.AddRange(batch.Rows);
        return rows;
    }

    [Fact]
    public async Task A_projected_read_downloads_only_the_footer_and_the_projected_column_chunks()
    {
        var local = new LocalFileIO();
        var file = await WideFile(local);
        var all = await ParquetFiles.ReadAsync(local, file.FilePath, WideSchema); // oracle: the whole file

        // One of four equal-width leaf columns (s has two): about a fifth of the file, never the whole of it.
        var io = new CountingFileIO(local);
        var projection = new Schema(0, [WideSchema.Columns[2]]);
        var rows = await ReadFile(io, file, projection);
        Assert.Equal(all.Select(r => r[2]), rows.Select(r => r[0]));
        Assert.Equal(0, io.WholeReads);
        Assert.True(io.BytesRead < file.FileSizeInBytes * 0.3, $"read {io.BytesRead} of {file.FileSizeInBytes} bytes");
        Assert.True(io.RangeRequests <= 1 + 4, $"{io.RangeRequests} requests"); // footer + one chunk per row group

        // Row-group pruning skips the chunks of groups the filter rules out: one group of four.
        var pruned = new CountingFileIO(local);
        var some = await ReadFile(pruned, file, projection, Expr.And(Expr.GreaterThanOrEqual("id", 30_000L), Expr.LessThan("id", 40_000L)));
        Assert.Equal(25_000, some.Count);
        Assert.Equal(all.Skip(25_000).Take(25_000).Select(r => r[2]), some.Select(r => r[0]));
        Assert.True(pruned.BytesRead < io.BytesRead / 2, $"pruned read {pruned.BytesRead}, unpruned {io.BytesRead}");

        // A nested column reads its own leaves only.
        var nested = new CountingFileIO(local);
        var structRows = await ReadFile(nested, file, new Schema(0, [WideSchema.Columns[3]]));
        Assert.Equal(Canonical(all.Select(r => new[] { r[3] })), Canonical(structRows));
        Assert.True(nested.BytesRead < file.FileSizeInBytes * 0.6, $"read {nested.BytesRead} of {file.FileSizeInBytes} bytes");
    }

    [Fact]
    public async Task A_read_of_every_column_or_of_a_small_file_is_one_request_after_the_footer()
    {
        var local = new LocalFileIO();
        var file = await WideFile(local);
        var io = new CountingFileIO(local);
        Assert.Equal(Canonical(await ParquetFiles.ReadAsync(local, file.FilePath, WideSchema)), Canonical(await ReadFile(io, file, WideSchema)));
        Assert.Equal(2, io.RangeRequests); // the footer, then the rest in one request
        Assert.Equal(file.FileSizeInBytes, io.BytesRead);

        var small = await ParquetFiles.WriteAsync(local, Path.Combine(_warehouse, "wide", "small.parquet"), WideSchema, 0, [], [[1L, 2L, 3L, null]]);
        var once = new CountingFileIO(local);
        Assert.Single(await ReadFile(once, small, new Schema(0, [WideSchema.Columns[0]])));
        Assert.Equal((1L, small.FileSizeInBytes), (once.RangeRequests, once.BytesRead));
    }

    [Fact]
    public async Task A_ranged_stream_serves_reads_across_fetched_ranges()
    {
        var local = new LocalFileIO();
        var path = Path.Combine(_warehouse, "bytes.bin");
        var content = Enumerable.Range(0, 100_000).Select(i => (byte)(i * 31)).ToArray();
        await local.WriteAsync(path, content);
        var io = new CountingFileIO(local);
        await using var s = new RangeReadStream(io, path, content.Length) { MinimumFetch = 1000 };
        await s.PrefetchAsync(50_000, 10_000);
        await s.PrefetchAsync(55_000, 10_000); // only 60 000..65 000 is new
        Assert.Equal((2L, 15_000L), (io.RangeRequests, io.BytesRead));

        s.Position = 45_000; // 45 000..50 000 is fetched on demand, then 50 000..65 000 comes from the cache, then 65 000..70 000
        var buffer = new byte[25_000];
        s.ReadExactly(buffer);
        Assert.Equal(content[45_000..70_000], buffer);
        Assert.Equal((4L, 25_000L), (io.RangeRequests, io.BytesRead));

        s.Seek(-10, SeekOrigin.End);
        Assert.Equal(10, s.Read(new byte[100])); // short read at the end
        Assert.Equal(0, s.Read(new byte[100]));
        Assert.Equal((5L, 25_010L), (io.RangeRequests, io.BytesRead)); // the tail fetch: MinimumFetch clamped to the end
    }

    // ---------- equality deletes across schema evolution ----------

    /// <summary>
    /// id, k (int) and v; ids 1..6 with k = id; then an equality delete on k ∈ {2, 4}, a rename of k to key and its
    /// promotion to long, and a new row with key 2 (written after the delete, so live).
    /// </summary>
    private async Task<(Table Table, long Deleted, long Promoted)> EvolvedEqualityDeletes()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "k", PrimitiveType.Int), NestedField.Optional(3, "v", PrimitiveType.String)]);
        var t = await Create(schema);
        await TableMutations.InsertAsync(t, Enumerable.Range(1, 6).Select(i => new object?[] { (long)i, i, $"v{i}" }).ToList());
        await TableMutations.DeleteByKeysAsync(t, ["k"], [[2], [4]], DeleteStrategy.EqualityDeletes);
        t = await Reload(t);
        var deleted = t.Metadata.CurrentSnapshotId!.Value;
        await t.UpdateSchemaAsync(u => u.RenameColumn("k", "key").UpdateColumn("key", PrimitiveType.Long));
        t = await Reload(t);
        await TableMutations.InsertAsync(t, [[7L, 2L, "v7"]]);
        t = await Reload(t);
        return (t, deleted, t.Metadata.CurrentSnapshotId!.Value);
    }

    [Fact]
    public async Task Equality_deletes_apply_across_a_rename_and_a_type_promotion()
    {
        var (t, deleted, promoted) = await EvolvedEqualityDeletes();

        // Time travel to the delete: the old schema (k int), deletes decoded with it.
        var old = await Read(t, new ScanRequest { Snapshot = SnapshotSelector.Id(deleted) });
        Assert.Equal(["1|1|v1", "3|3|v3", "5|5|v5", "6|6|v6"], old.Select(r => string.Join('|', r)).Order());
        Assert.IsType<int>(old[0][1]);
        Assert.Equal(["id", "k", "v"], TableScan.SchemaOf(t, new ScanRequest { Snapshot = SnapshotSelector.Id(deleted) }).Columns.Select(c => c.Name));

        // The current snapshot: key long; the new key-2 row is not deleted (its sequence number is later).
        var now = await Read(t, new ScanRequest { Snapshot = SnapshotSelector.Id(promoted) });
        Assert.Equal(["1|1|v1", "3|3|v3", "5|5|v5", "6|6|v6", "7|2|v7"], now.Select(r => string.Join('|', r)).Order());
        Assert.IsType<long>(now[0][1]);

        // Filtering and projecting by the old name at the old snapshot, and the legacy current-schema API.
        Assert.Equal([3L], (await Read(t, new ScanRequest { Snapshot = SnapshotSelector.Id(deleted), Filter = Expr.Equal("k", 3), Columns = ["id"] })).Select(r => r[0]));
        Assert.Equal(4, (await TableScan.ReadAllAsync(t, deleted)).Count);
        Assert.Equal(4, await TableScan.CountAsync(t, new ScanRequest { Snapshot = SnapshotSelector.Id(deleted) }));
    }

    [Fact]
    public async Task Equality_deletes_still_apply_after_their_column_is_dropped()
    {
        var (t, deleted, promoted) = await EvolvedEqualityDeletes();
        await t.UpdateSchemaAsync(u => u.DeleteColumn("key"));
        t = await Reload(t);

        // The current state no longer has the column, but the deletes on it stand: the deleted rows stay deleted.
        Assert.Equal(["1|v1", "3|v3", "5|v5", "6|v6", "7|v7"], (await TableScan.ReadAllAsync(t)).Select(r => string.Join('|', r)).Order());
        Assert.Equal(["1|v1", "3|v3", "5|v5", "6|v6"], (await TableScan.ReadAllAsync(t, deleted)).Select(r => string.Join('|', r)).Order());
        Assert.Equal(5, await TableScan.CountAsync(t, new ScanRequest()));
        Assert.Equal([1L, 3L], (await Read(t, new ScanRequest { Filter = Expr.LessThan("id", 4L), Columns = ["id"] })).Select(r => r[0]).Order());
        // Time travel still reads the old snapshots in their own schemas.
        Assert.Equal(4, (await Read(t, new ScanRequest { Snapshot = SnapshotSelector.Id(deleted) })).Count);
        Assert.Equal(5, (await Read(t, new ScanRequest { Snapshot = SnapshotSelector.Id(promoted) })).Count);

        // Writers that read existing rows apply them too: an UPDATE, the change feed and a compaction.
        await TableMutations.UpdateAsync(t, Expr.Equal("id", 1L), u => u.Set("v", "w1"), DeleteStrategy.PositionDeletes);
        t = await Reload(t);
        var replayed = new List<string>();
        await foreach (var c in ChangeFeed.ChangesAsync(t, null))
            if (c.ChangeType == ChangeType.Insert) replayed.Add(string.Join('|', c.Row));
            else Assert.True(replayed.Remove(string.Join('|', c.Row)));
        Assert.Equal(["1|w1", "3|v3", "5|v5", "6|v6", "7|v7"], replayed.Order());
        await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 1, RewriteAll: true));
        t = await Reload(t);
        Assert.Equal(["1|w1", "3|v3", "5|v5", "6|v6", "7|v7"], (await TableScan.ReadAllAsync(t)).Select(r => string.Join('|', r)).Order());
    }
}
