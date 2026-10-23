using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Expressions;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.TestSupport;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Builds one table per write path (copy-on-write, position deletes, deletion vectors, equality deletes, v2→v3
/// upgrade, multi-transform partitioning) and has Spark + Iceberg Java read them all in a single JVM.
/// </summary>
public sealed class SparkTablesFixture : IAsyncLifetime
{
    public sealed record Built(Table Table, Dictionary<long, object?[]> Rows, List<LineageRow>? Lineage);

    public Dictionary<string, Built> Tables { get; } = new();
    public JsonObject? Spark { get; private set; }
    private readonly string _warehouse = Path.Combine(SparkOracle.SharedRoot, Guid.NewGuid().ToString("N"));
    private InMemoryCatalog? _catalog;

    private static readonly Schema Keyed = new(0, CatalogConformanceTests.Schema.Columns, identifierFieldIds: [1]);

    public async Task InitializeAsync()
    {
        if (SparkOracle.SkipReason is not null) return;
        _catalog = new InMemoryCatalog(_warehouse);
        await _catalog.CreateNamespaceAsync(Namespace.Of("db"));

        foreach (var v in new[] { 2, 3 })
        foreach (var s in new[] { DeleteStrategy.CopyOnWrite, DeleteStrategy.PositionDeletes, DeleteStrategy.EqualityDeletes })
            await Build($"{s}_v{v}", v, async t =>
            {
                await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(25, 10).Select(r => { r[1] = $"upserted-{r[0]}"; return r; }).ToList(), ["id"], s);
                await TableMutations.DeleteByKeysAsync(t, ["id"], [[3L], [4L], [26L]], s);
                if (s != DeleteStrategy.EqualityDeletes) // DeleteWhere needs row positions
                    await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 7 == 1, s);
                await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(5, 1).Select(r => { r[1] = "again"; return r; }).ToList(), ["id"], s);
            });

        await Build("upgraded_v2_to_v3", 2, async t =>
        {
            await TableMutations.DeleteByKeysAsync(t, ["id"], [[1L], [2L]], DeleteStrategy.PositionDeletes);
            await t.CommitAsync([], [new MetadataUpdate.UpgradeFormatVersion(3)]);
            await SnapshotProducer.ConvertPositionDeletesToDeletionVectors(t).CommitAsync();
            await TableMutations.DeleteByKeysAsync(t, ["id"], [[10L]], DeleteStrategy.PositionDeletes);
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(100, 3));
        });

        // Day + bucket + identity partitioning: Java must find every row in the partition our transforms chose.
        await Build("partitioned_v3", 3, async t =>
        {
            await TableMutations.ReplaceGroupsAsync(t, CatalogConformanceTests.Rows(200, 4).Select(r => { r[4] = "eu"; return r; }).ToList(), ["region"], DeleteStrategy.CopyOnWrite);
            await TableMutations.DeleteByKeysAsync(t, ["id", "event_ts", "region"], [[201L, CatalogConformanceTests.Rows(201, 1)[0][3], "eu"]], DeleteStrategy.PositionDeletes);
        }, CatalogConformanceTests.Spec);

        // Maintenance output must read the same in Java: compaction (lineage preserved), merged manifests, an
        // evolved schema, and snapshot expiry with file cleanup.
        await Build("compacted_v3", 3, async t =>
        {
            for (var i = 0; i < 4; i++) await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(100 + i * 5, 5));
            await TableMutations.DeleteByKeysAsync(t, ["id"], [[101L], [7L]], DeleteStrategy.PositionDeletes);
            await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 2));
        });
        await Build("merged_v3", 3, async t =>
        {
            await t.SetPropertiesAsync(new Dictionary<string, string> { [SnapshotProducer.ManifestMinCountToMerge] = "2" });
            for (var i = 0; i < 4; i++) await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(200 + i * 5, 5));
            await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(3, 2).Select(r => { r[1] = "merged"; return r; }).ToList(), ["id"], DeleteStrategy.EqualityDeletes);
        });
        await Build("evolved_v2", 2, async t =>
        {
            await t.UpdateSchemaAsync(s => s.UpdateColumn("qty", Types.PrimitiveType.Long).AddColumn("note", Types.PrimitiveType.String));
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(300, 3).Select(r => (object?[])[.. r[..5], (long)(int)r[5]!, "new"]).ToList());
        });
        // Partition evolution through UpdateSpecAsync: rows under the original and the evolved spec, then deletes across both.
        await Build("spec_evolved_v2", 2, async t =>
        {
            await t.UpdateSpecAsync(p => p.AddField("region").AddField("id", Transforms.Transform.Bucket(4)));
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(500, 10));
            await t.UpdateSpecAsync(p => p.RemoveField("id_bucket").AddField("event_ts", Transforms.Transform.Day));
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(600, 10));
            await TableMutations.DeleteByKeysAsync(t, ["id"], [[1L], [501L], [601L]], DeleteStrategy.PositionDeletes);
        });
        await Build("expired_v2", 2, async t =>
        {
            await TableMutations.UpsertAsync(t, CatalogConformanceTests.Rows(0, 10).Select(r => { r[1] = "expired"; return r; }).ToList(), ["id"], DeleteStrategy.CopyOnWrite);
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(400, 3));
            await Maintenance.ExpireAsync(t, long.MaxValue, retainLast: 1);
        });

        // Struct, list and map columns (with null lists of structs, which PyIceberg cannot check) after deletes and compaction.
        {
            var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "nested_v3"), ShadowCompareTests.Nested, FormatVersion: 3));
            await TableMutations.InsertAsync(t, ShadowCompareTests.NestedRows(40, nullStructLists: true));
            await TableMutations.InsertAsync(t, ShadowCompareTests.NestedRows(60, nullStructLists: true).Skip(40).ToList());
            await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 13 == 4, DeleteStrategy.PositionDeletes);
            await Compaction.RewriteDataFilesAsync(t, new RewriteOptions(MinInputFiles: 2));
            t = await _catalog.LoadTableAsync(t.Identifier);
            Tables["nested_v3"] = new Built(t, (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!), await TableScan.ReadWithLineageAsync(t));
        }

        // Partitioned by fields inside a struct (identity + bucket), with a row-level delete by a nested-field filter.
        {
            var spec = Partitioning.PartitionSpec.BuilderFor(ShadowCompareTests.Nested).Identity("place.label").Bucket("place.since", 4).Build();
            var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "nested_partitioned_v2"), ShadowCompareTests.Nested, spec, FormatVersion: 2));
            await TableMutations.InsertAsync(t, ShadowCompareTests.NestedRows(60, nullStructLists: true));
            await TableMutations.DeleteWhereAsync(t, Expr.LessThan("place.lat", 3.0), DeleteStrategy.PositionDeletes);
            t = await _catalog.LoadTableAsync(t.Identifier);
            Tables["nested_partitioned_v2"] = new Built(t, (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!), null);
        }

        // Avro data files (write.format.default=avro) with nested types, uuids, decimals and timestamps, plus deletes.
        foreach (var v in new[] { 2, 3 })
        {
            var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"avro_nested_v{v}"), ShadowCompareTests.Nested, FormatVersion: v,
                Properties: new Dictionary<string, string> { ["write.format.default"] = "avro" }));
            await TableMutations.InsertAsync(t, ShadowCompareTests.NestedRows(30, nullStructLists: true));
            await TableMutations.InsertAsync(t, ShadowCompareTests.NestedRows(50, nullStructLists: true).Skip(30).ToList());
            await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 9 == 2, DeleteStrategy.PositionDeletes);
            t = await _catalog.LoadTableAsync(t.Identifier);
            Tables[$"avro_nested_v{v}"] = new Built(t, (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!), v >= 3 ? await TableScan.ReadWithLineageAsync(t) : null);
        }

        await BuildQueriesAsync();
        Spark = await SparkOracle.ReadTablesAsync(Tables.Values.Select(b => b.Table.MetadataLocation!).Concat(Queries.Select(q => q.Key)));
    }

    /// <summary>A read Spark runs in the same JVM (key: <c>query:{json}</c>, see spark_oracle.py run_query) and its <see cref="ScanRequest"/> twin.</summary>
    public sealed record Query(string Key, Table Table, ScanRequest Ours);

    public List<Query> Queries { get; } = new();

    /// <summary>
    /// Time travel across a rename, a drop, a widening and an added column (Spark's versionAsOf/timestampAsOf, i.e.
    /// VERSION AS OF / TIMESTAMP AS OF), and filtered, projected reads of the delete-heavy tables built above.
    /// </summary>
    private async Task BuildQueriesAsync()
    {
        var schema = new Schema(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String),
            NestedField.Optional(3, "extra", PrimitiveType.Int), NestedField.Optional(4, "qty", PrimitiveType.Int)]);
        static object?[] Row(int i) => [(long)i, i % 4 == 0 ? null : $"n{i}", i * 2, i % 5];
        var t = await _catalog!.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "time_travel_v2"), schema, FormatVersion: 2));
        var s1 = (await TableMutations.InsertAsync(t, Enumerable.Range(0, 10).Select(Row).ToList())).Snapshot.SnapshotId;
        t = await _catalog.LoadTableAsync(t.Identifier);
        // Small row groups, so row-group pruning runs on what Spark also reads.
        var file = await ParquetFiles.WriteAsync(t.IO, $"{t.Location}/data/{Guid.NewGuid():N}.parquet", t.Schema, 0, [], Enumerable.Range(10, 20).Select(Row).ToList(),
            new ParquetWriteOptions { RowGroupRows = 4 });
        var s2 = (await SnapshotProducer.Append(t).AddFiles([file]).CommitAsync()).SnapshotId;
        await Task.Delay(20); // distinct snapshot-log instants
        t = await _catalog.LoadTableAsync(t.Identifier);
        await t.UpdateSchemaAsync(u => u.RenameColumn("name", "title").DeleteColumn("extra").UpdateColumn("qty", PrimitiveType.Long).AddColumn("note", PrimitiveType.String));
        t = await _catalog.LoadTableAsync(t.Identifier);
        await TableMutations.InsertAsync(t, Enumerable.Range(30, 10).Select(i => new object?[] { (long)i, $"n{i}", (long)(i % 5), i % 2 == 0 ? "x" : null }).ToList());
        await TableMutations.DeleteWhereAsync(await _catalog.LoadTableAsync(t.Identifier), Expr.In("id", 13L, 22L), DeleteStrategy.PositionDeletes);
        t = await _catalog.LoadTableAsync(t.Identifier);
        string At(long snapshotId) => DateTimeOffset.FromUnixTimeMilliseconds(t.Metadata.SnapshotLog.Single(e => e.SnapshotId == snapshotId).TimestampMs)
            .UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture);
        DateTimeOffset Instant(long snapshotId) => DateTimeOffset.FromUnixTimeMilliseconds(t.Metadata.SnapshotLog.Single(e => e.SnapshotId == snapshotId).TimestampMs);

        void Add(Table table, JsonObject spark, ScanRequest ours)
        {
            spark["location"] = table.MetadataLocation;
            Queries.Add(new Query("query:" + spark.ToJsonString(), table, ours));
        }
        Add(t, new JsonObject { ["version_as_of"] = s1 }, new ScanRequest { Snapshot = SnapshotSelector.Id(s1) });
        Add(t, new JsonObject { ["version_as_of"] = s2, ["where"] = "name = 'n13' OR extra > 50 OR name IS NULL" },
            new ScanRequest { Snapshot = SnapshotSelector.Id(s2), Filter = Expr.Or(Expr.Equal("name", "n13"), Expr.GreaterThan("extra", 50), Expr.IsNull("name")) });
        Add(t, new JsonObject { ["version_as_of"] = s2, ["where"] = "extra BETWEEN 30 AND 41", ["columns"] = new JsonArray("id", "extra", "name") },
            new ScanRequest { Snapshot = SnapshotSelector.Id(s2), Filter = Expr.And(Expr.GreaterThanOrEqual("extra", 30), Expr.LessThanOrEqual("extra", 41)), Columns = ["id", "extra", "name"] });
        Add(t, new JsonObject { ["timestamp_as_of"] = At(s2), ["where"] = "id >= 12" },
            new ScanRequest { Snapshot = SnapshotSelector.AsOf(Instant(s2)), Filter = Expr.GreaterThan("id", 11L) });
        Add(t, new JsonObject { ["timestamp_as_of"] = At(s1) }, new ScanRequest { Snapshot = SnapshotSelector.AsOf(Instant(s1)) });
        Add(t, new JsonObject { ["where"] = "title LIKE 'n2%' OR note IS NOT NULL" },
            new ScanRequest { Filter = Expr.Or(Expr.StartsWith("title", "n2"), Expr.NotNull("note")) });
        Add(t, new JsonObject { ["where"] = "qty < 2 AND id <> 31", ["columns"] = new JsonArray("id", "qty", "title") },
            new ScanRequest { Filter = Expr.And(Expr.LessThan("qty", 2L), Expr.NotEqual("id", 31L)), Columns = ["id", "qty", "title"] });
        foreach (var name in new[] { "PositionDeletes_v3", "EqualityDeletes_v2", "partitioned_v3", "evolved_v2", "upgraded_v2_to_v3" })
        {
            var table = Tables[name].Table;
            Add(table, new JsonObject { ["where"] = "amount > 10.0 AND region = 'eu'" },
                new ScanRequest { Filter = Expr.And(Expr.GreaterThan("amount", 10.0), Expr.Equal("region", "eu")) });
            Add(table, new JsonObject { ["where"] = "name IS NULL OR id < 7 OR id IN (26, 101, 201)", ["columns"] = new JsonArray("id", "name", "qty") },
                new ScanRequest { Filter = Expr.Or(Expr.IsNull("name"), Expr.LessThan("id", 7L), Expr.In("id", 26L, 101L, 201L)), Columns = ["id", "name", "qty"] });
        }
    }

    private async Task Build(string name, int formatVersion, Func<Table, Task> mutate, Partitioning.PartitionSpec? spec = null)
    {
        var t = await _catalog!.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", name), Keyed, spec, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(0, 30));
        await mutate(t);
        t = await _catalog.LoadTableAsync(t.Identifier);
        var rows = (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!);
        Tables[name] = new Built(t, rows, t.Metadata.FormatVersion >= 3 ? await TableScan.ReadWithLineageAsync(t) : null);
    }

    public Task DisposeAsync()
    {
        _catalog?.Dispose();
        // ICEBERG_SPARK_KEEP=1 keeps the tables for debugging with the oracle by hand.
        if (Directory.Exists(_warehouse) && Environment.GetEnvironmentVariable("ICEBERG_SPARK_KEEP") != "1") Directory.Delete(_warehouse, recursive: true);
        return Task.CompletedTask;
    }
}

/// <summary>Spark + Iceberg Java (the reference implementation) reads every table we write exactly as we do.</summary>
[Trait("Category", "Regression")]
public sealed class SparkCrossEngineTests(SparkTablesFixture fx) : IClassFixture<SparkTablesFixture>
{
    public static TheoryData<string> Names() => new()
    {
        "CopyOnWrite_v2", "PositionDeletes_v2", "EqualityDeletes_v2",
        "CopyOnWrite_v3", "PositionDeletes_v3", "EqualityDeletes_v3",
        "upgraded_v2_to_v3", "partitioned_v3", "compacted_v3", "merged_v3", "evolved_v2", "expired_v2", "spec_evolved_v2",
    };

    public static TheoryData<string> AllNames() { var all = Names(); all.Add("nested_v3"); return all; }

    private JsonNode SparkView(string name)
    {
        var view = fx.Spark![fx.Tables[name].Table.MetadataLocation!]!;
        Assert.True(view["error"] is null, $"Spark failed to read {name}: {view["error"]}");
        return view;
    }

    [Spec("Scan Planning")]
    [Spec("Equality Delete Files")]
    [Spec("Position Delete Files")]
    [Spec("Deletion Vectors")]
    [SkippableTheory]
    [MemberData(nameof(Names))]
    public void Spark_reads_the_same_rows(string name)
    {
        Require.Spark();
        var ours = fx.Tables[name].Rows;
        var spark = SparkView(name)["rows"]!.AsArray().ToDictionary(r => r!["id"]!.GetValue<long>(), r => r!.AsObject());

        Assert.Equal(ours.Keys.Order(), spark.Keys.Order());
        foreach (var (id, row) in ours)
        {
            var s = spark[id];
            Assert.Equal((string?)row[1], s["name"]?.GetValue<string>());
            Assert.Equal((double?)row[2], s["amount"]?.GetValue<double>());
            Assert.Equal((long?)row[3], s["event_ts"]?.GetValue<long>());
            Assert.Equal((string?)row[4], s["region"]?.GetValue<string>());
            Assert.Equal(row[5] is null ? null : Convert.ToInt64(row[5]), s["qty"]?.GetValue<long>());
        }
    }

    [Spec("Nested Types")]
    [SkippableTheory]
    [InlineData("nested_v3")]
    [InlineData("nested_partitioned_v2")]
    [InlineData("avro_nested_v2")]
    [InlineData("avro_nested_v3")]
    public void Spark_reads_the_same_nested_values(string name)
    {
        Require.Spark();
        var built = fx.Tables[name];
        var schema = built.Table.Schema;
        Assert.Equal(SparkJson.Rows(schema, built.Rows.Values), SparkJson.Rows(schema, SparkView(name)["rows"]!.AsArray()));
        Assert.Contains(built.Rows.Values, r => r[4] is null); // the null list<struct> case really is covered
    }

    /// <summary>Java plans scans of the struct-partitioned table with filters on nested fields; we read its plans like ours.</summary>
    [Spec("Partitioning")]
    [SkippableFact]
    public async Task Java_plans_nested_field_filters_like_we_do()
    {
        Require.Spark();
        var t = fx.Tables["nested_partitioned_v2"].Table;
        var filters = new[] { Expr.Equal("place.label", "p8"), Expr.GreaterThan("place.lat", 10.0), Expr.IsNull("place.label"), Expr.And(Expr.NotNull("place.since"), Expr.LessThan("id", 40L)) };
        var plans = await SparkOracle.PlanScansAsync(filters.Select(f => (t.MetadataLocation!, (System.Text.Json.Nodes.JsonNode?)ExpressionJson.ToJson(f))));
        for (var i = 0; i < filters.Length; i++)
        {
            var plan = ServerScanPlan.FromCompletedResponse(plans[i]!, t.Metadata);
            var ours = await TableScan.PlanAsync(t, filters[i]);
            Assert.Equal(ours.Tasks.Select(x => x.File.FilePath).Order(), plan.Tasks.Select(x => x.DataFile.FilePath.Replace("file:/", "file:///")).Order()); // same pruning
            Assert.Equal(SparkJson.Rows(t.Schema, await TableScan.ReadAsync(t, filters[i])), SparkJson.Rows(t.Schema, await ServerSideScan.ReadAsync(t, plan, filters[i])));
        }
    }

    /// <summary>Iceberg Java plans scans of every table we wrote (as a REST server would) and we read its plans exactly.</summary>
    [Spec("rest: planTableScan")]
    [Spec("Scan Planning")]
    [SkippableFact]
    public async Task Java_plans_our_tables_and_we_read_its_plans()
    {
        Require.Spark();
        var filters = new Expression?[] { null, Expr.LessThan("id", 20L), Expr.In("id", 3L, 5L, 7L, 26L, 101L, 201L) };
        var cases = (from name in fx.Tables.Keys from f in filters select (name, f)).ToList();
        var plans = await SparkOracle.PlanScansAsync(cases.Select(c => (fx.Tables[c.name].Table.MetadataLocation!, c.f is null ? null : ExpressionJson.ToJson(c.f))));
        for (var i = 0; i < cases.Count; i++)
        {
            var (name, filter) = cases[i];
            var t = fx.Tables[name].Table;
            var plan = ServerScanPlan.FromCompletedResponse(plans[i]!, t.Metadata);
            var theirs = await ServerSideScan.ReadAsync(t, plan, filter);
            var ours = filter is null ? await TableScan.ReadAllAsync(t) : await TableScan.ReadAsync(t, filter);
            Assert.True(SparkJson.Rows(t.Schema, theirs).OrderBy(kv => kv.Key).SequenceEqual(SparkJson.Rows(t.Schema, ours).OrderBy(kv => kv.Key)), $"{name} filter {filter}");
        }
    }

    [Spec("Snapshots")]
    [SkippableTheory]
    [MemberData(nameof(AllNames))]
    public void Spark_sees_the_same_snapshots_and_format(string name)
    {
        Require.Spark();
        var t = fx.Tables[name].Table;
        var view = SparkView(name);
        Assert.Equal(t.Metadata.FormatVersion, view["format_version"]!.GetValue<int>());
        Assert.Equal(t.Metadata.Snapshots.Count, view["snapshot_count"]!.GetValue<int>());
    }

    [Spec("Delete Formats")]
    [SkippableTheory]
    [InlineData("PositionDeletes_v3", "1:PUFFIN")]
    [InlineData("PositionDeletes_v2", "1:PARQUET")]
    [InlineData("EqualityDeletes_v2", "2:PARQUET")]
    [InlineData("EqualityDeletes_v3", "2:PARQUET")]
    [InlineData("upgraded_v2_to_v3", "1:PUFFIN")]
    public void Spark_sees_our_delete_files_by_kind(string name, string kind)
    {
        Require.Spark();
        var deletes = SparkView(name)["delete_files"]!.AsArray().Select(d => d!.GetValue<string>()).ToList();
        Assert.Contains(kind, deletes);
        if (name.EndsWith("_v3", StringComparison.Ordinal) && kind == "1:PUFFIN") Assert.DoesNotContain("1:PARQUET", deletes); // v3: DVs only
    }

    [Spec("Row lineage assignment")]
    [Spec("Row Lineage for Upgraded Tables")]
    [SkippableTheory]
    [InlineData("CopyOnWrite_v3")]
    [InlineData("PositionDeletes_v3")]
    [InlineData("EqualityDeletes_v3")]
    [InlineData("partitioned_v3")]
    [InlineData("upgraded_v2_to_v3")]
    [InlineData("compacted_v3")]
    [InlineData("merged_v3")]
    [InlineData("nested_v3")]
    [InlineData("avro_nested_v3")]
    public void Spark_derives_the_same_row_lineage(string name)
    {
        Require.Spark();
        var ours = fx.Tables[name].Lineage!.ToDictionary(r => (long)r.Row[0]!, r => (r.RowId, r.LastUpdatedSequenceNumber));
        var spark = SparkView(name)["row_lineage"]!.AsArray().ToDictionary(
            r => r!["id"]!.GetValue<long>(),
            r => (r!["row_id"]?.GetValue<long>(), r["last_updated_seq"]?.GetValue<long>()));
        Assert.Equal(ours.OrderBy(kv => kv.Key), spark.OrderBy(kv => kv.Key));
    }
    /// <summary>
    /// Spark reads old snapshots (VERSION AS OF / TIMESTAMP AS OF) in the schema they were written with, and filtered,
    /// projected reads, exactly as <see cref="TableScan.ReadRowsAsync"/> does: same columns, same rows.
    /// </summary>
    [Spec("Point in Time Reads (Time Travel)")]
    [Spec("Column Projection")]
    [Spec("Scan Planning")]
    [SkippableFact]
    public async Task Spark_agrees_on_time_travel_and_filtered_reads()
    {
        Require.Spark();
        Assert.NotEmpty(fx.Queries);
        foreach (var q in fx.Queries)
        {
            var view = fx.Spark![q.Key]!;
            Assert.True(view["error"] is null, $"Spark failed {q.Key}: {view["error"]}");
            var schema = TableScan.SchemaOf(q.Table, q.Ours);
            Assert.Equal(schema.Columns.Select(c => c.Name), view["columns"]!.AsArray().Select(c => c!.GetValue<string>()));
            var ours = new List<object?[]>();
            await foreach (var row in TableScan.ReadRowsAsync(q.Table, q.Ours)) ours.Add(row);
            var theirs = SparkJson.Rows(schema, view["rows"]!.AsArray());
            Assert.True(SparkJson.Rows(schema, ours).OrderBy(kv => kv.Key).SequenceEqual(theirs.OrderBy(kv => kv.Key)),
                $"{q.Key}\nours:   {string.Join(", ", ours.Select(r => r[0]).Order())}\nspark:  {string.Join(", ", theirs.Keys.Order())}");
            Assert.Equal(ours.Count, await TableScan.CountAsync(q.Table, q.Ours));
        }
    }
}
