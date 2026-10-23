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

        // Branches, tags, a branch write and row delta, rollback, cherry-pick replay, fast-forward and ref expiry: Spark must
        // read every ref as we do and see the same metadata tables.
        foreach (var v in new[] { 2, 3 })
            await Build($"refs_v{v}", v, async t =>
            {
                var s1 = t.Metadata.CurrentSnapshotId!.Value;
                await t.ManageSnapshots().CreateTag("v1", s1, new RefRetention(MaxRefAgeMs: 30L * 86_400_000))
                    .CreateBranch("audit", retention: new RefRetention(2, 3_600_000, 7L * 86_400_000)).CommitAsync();
                await SnapshotProducer.Append(t).AddFiles(await TableWriter.WriteAsync(t, CatalogConformanceTests.Rows(100, 3))).ToBranch("audit").CommitAsync();
                var staged = (await TableScan.PlanFilesAsync(t, t.Metadata.Refs["audit"].SnapshotId)).First(f => f.File.RecordCount == 3).File.FilePath;
                await SnapshotProducer.DeleteRows(t, new Dictionary<string, IEnumerable<long>> { [staged] = [0] }).ToBranch("audit").CommitAsync();
                await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(200, 2));
                var s2 = t.Metadata.CurrentSnapshotId!.Value;
                await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(300, 2));
                var s3 = t.Metadata.CurrentSnapshotId!.Value;
                await t.ManageSnapshots().RollbackTo(s2).CommitAsync();
                await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(400, 1));
                await t.ManageSnapshots().CherryPick(s3).CommitAsync();
                await t.ManageSnapshots().CreateBranch("ff", s1).CreateTag("short-lived", s1, new RefRetention(MaxRefAgeMs: 1)).CommitAsync();
                await t.ManageSnapshots().FastForwardBranch("ff", SnapshotRef.Main).CommitAsync();
                await Task.Delay(5);
                var expired = await Maintenance.ExpireAsync(t, new ExpireOptions { OlderThanMs = 0 }); // only the aged-out tag goes
                Assert.Equal(["short-lived"], expired.RemovedRefs);
            });

        var withMetadataTables = new HashSet<string>(MetadataTableNames());
        SparkMadeWarehouse = Path.Combine(_warehouse, "spark-made");
        Spark = await SparkOracle.ReadTablesAsync(Tables
            .Select(kv => (withMetadataTables.Contains(kv.Key) ? "meta:" : "") + kv.Value.Table.MetadataLocation!)
            .Append($"create-refs:{SparkMadeWarehouse}"));
    }

    /// <summary>Tables Spark also dumps the metadata tables and the changelog of.</summary>
    public static IEnumerable<string> MetadataTableNames() =>
        ["refs_v2", "refs_v3", "PositionDeletes_v2", "PositionDeletes_v3", "EqualityDeletes_v2", "CopyOnWrite_v3", "partitioned_v3", "spec_evolved_v2"];

    /// <summary>Where Spark built its own table with branches, tags, a rollback and a cherry-pick.</summary>
    public string? SparkMadeWarehouse { get; private set; }

    public InMemoryCatalog Catalog => _catalog!;

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

    // ---- Snapshot management, metadata tables and the change feed against Spark (one oracle run, see the fixture) ----

    private static string NormPath(string? p) => p is null ? "" : System.Text.RegularExpressions.Regex.Replace(p, "^file:/+", "/");

    private static string Text(JsonNode? n) => n switch
    {
        null => "null",
        JsonValue v when v.TryGetValue<string>(out var s) => s,
        _ => n.ToJsonString(),
    };

    /// <summary>Our partition value as Spark shows it: day-transform values (and dates) as ISO dates, the rest as is.</summary>
    private static string PartitionText(object? ours, JsonNode? theirs)
    {
        if (ours is null) return "null";
        if (ours is int days && Text(theirs) is var t && t.Length == 10 && t[4] == '-')
            return DateOnly.FromDayNumber(new DateOnly(1970, 1, 1).DayNumber + days).ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
        return ours switch
        {
            byte[] b => Convert.ToHexString(b).ToLowerInvariant(),
            bool x => x ? "true" : "false",
            IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture),
            _ => ours.ToString()!,
        };
    }

    private static string MapText<T>(IReadOnlyDictionary<int, T>? ours, Func<T, string> value) =>
        ours is null ? "" : string.Join(",", ours.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={value(kv.Value)}"));

    private static string MapText(JsonNode? theirs) =>
        theirs is not JsonObject o ? "" : string.Join(",", o.OrderBy(kv => int.Parse(kv.Key, System.Globalization.CultureInfo.InvariantCulture)).Select(kv => $"{kv.Key}={Text(kv.Value).ToLowerInvariant()}"));

    private static string ListText<T>(IEnumerable<T>? ours) => ours is null ? "null" : "[" + string.Join(",", ours) + "]";

    private static string ListText(JsonNode? theirs) => theirs is not JsonArray a ? "null" : "[" + string.Join(",", a.Select(Text)) + "]";

    private static string FileText(FilesRow f, JsonNode s) => string.Join("|",
        $"{f.Content}", f.FileFormat, $"{f.SpecId}", $"{f.RecordCount}", $"{f.FileSizeInBytes}",
        MapText(f.ColumnSizes, v => $"{v}"), MapText(f.ValueCounts, v => $"{v}"), MapText(f.NullValueCounts, v => $"{v}"), MapText(f.NanValueCounts, v => $"{v}"),
        MapText(f.LowerBounds, v => Convert.ToHexString(v).ToLowerInvariant()), MapText(f.UpperBounds, v => Convert.ToHexString(v).ToLowerInvariant()),
        ListText(f.SplitOffsets), ListText(f.EqualityIds), $"{f.SortOrderId?.ToString() ?? "null"}", $"{f.FirstRowId?.ToString() ?? "null"}",
        NormPath(f.ReferencedDataFile), $"{f.ContentOffset?.ToString() ?? "null"}", $"{f.ContentSizeInBytes?.ToString() ?? "null"}",
        string.Join(",", f.Partition.Select(kv => $"{kv.Key}={PartitionText(kv.Value, s["partition"]?[kv.Key])}")));

    private static string SparkFileText(JsonNode s) => string.Join("|",
        Text(s["content"]), Text(s["file_format"]), Text(s["spec_id"]), Text(s["record_count"]), Text(s["file_size_in_bytes"]),
        MapText(s["column_sizes"]), MapText(s["value_counts"]), MapText(s["null_value_counts"]), MapText(s["nan_value_counts"]),
        MapText(s["lower_bounds"]), MapText(s["upper_bounds"]),
        ListText(s["split_offsets"]), ListText(s["equality_ids"]), Text(s["sort_order_id"]), Text(s["first_row_id"]),
        NormPath(s["referenced_data_file"]?.GetValue<string>()), Text(s["content_offset"]), Text(s["content_size_in_bytes"]),
        s["partition"] is JsonObject p ? string.Join(",", p.Select(kv => $"{kv.Key}={Text(kv.Value)}")) : "");

    private static JsonArray Rows(JsonNode tables, string name)
    {
        var table = tables[name]!;
        Assert.True(table["error"] is null, $"Spark could not read the {name} metadata table: {table["error"]}");
        return table["rows"]!.AsArray();
    }

    /// <summary>Our metadata tables against Spark's, column by column (paths normalised, timestamps to the millisecond).</summary>
    private static async Task AssertSameMetadataTables(Table t, JsonNode tables, string label)
    {
        var snapshots = MetadataTables.Snapshots(t).ToDictionary(s => s.SnapshotId);
        var sparkSnapshots = Rows(tables, "snapshots");
        Assert.Equal(snapshots.Count, sparkSnapshots.Count);
        foreach (var r in sparkSnapshots)
        {
            var o = snapshots[r!["snapshot_id"]!.GetValue<long>()];
            Assert.Equal(o.ParentId, r["parent_id"]?.GetValue<long>());
            Assert.Equal(o.Operation, r["operation"]?.GetValue<string>());
            Assert.Equal(NormPath(o.ManifestList), NormPath(r["manifest_list"]?.GetValue<string>()));
            Assert.Equal(o.CommittedAt.ToUnixTimeMilliseconds(), (long)Math.Round(r["committed_at"]!.GetValue<long>() / 1000.0));
            Assert.Equal(o.Summary.Where(kv => kv.Key != "operation").OrderBy(kv => kv.Key), r["summary"]!.AsObject().Select(kv => KeyValuePair.Create(kv.Key, kv.Value!.GetValue<string>())).OrderBy(kv => kv.Key));
        }

        Assert.Equal(
            MetadataTables.History(t).Select(h => $"{h.MadeCurrentAt.ToUnixTimeMilliseconds()}|{h.SnapshotId}|{h.ParentId}|{h.IsCurrentAncestor}"),
            Rows(tables, "history").Select(r => $"{(long)Math.Round(r!["made_current_at"]!.GetValue<long>() / 1000.0)}|{r["snapshot_id"]}|{r["parent_id"]?.GetValue<long>()}|{r["is_current_ancestor"]!.GetValue<bool>()}"));

        Assert.Equal(
            MetadataTables.Refs(t).Select(r => $"{r.Name}|{r.Type}|{r.SnapshotId}|{r.MaxReferenceAgeInMs}|{r.MinSnapshotsToKeep}|{r.MaxSnapshotAgeInMs}").Order(),
            Rows(tables, "refs").Select(r => $"{r!["name"]}|{r["type"]}|{r["snapshot_id"]}|{r["max_reference_age_in_ms"]?.GetValue<long>()}|{r["min_snapshots_to_keep"]?.GetValue<int>()}|{r["max_snapshot_age_in_ms"]?.GetValue<long>()}").Order());

        var manifests = (await MetadataTables.ManifestsAsync(t)).ToDictionary(m => NormPath(m.Path));
        var sparkManifests = Rows(tables, "manifests");
        Assert.Equal(manifests.Count, sparkManifests.Count);
        foreach (var r in sparkManifests)
        {
            var o = manifests[NormPath(r!["path"]!.GetValue<string>())];
            Assert.Equal(
                $"{o.Content}|{o.Length}|{o.PartitionSpecId}|{o.AddedSnapshotId}|{o.AddedDataFilesCount}|{o.ExistingDataFilesCount}|{o.DeletedDataFilesCount}|{o.AddedDeleteFilesCount}|{o.ExistingDeleteFilesCount}|{o.DeletedDeleteFilesCount}|" +
                string.Join(";", o.PartitionSummaries.Select(p => $"{p.ContainsNull}|{p.ContainsNan?.ToString() ?? "null"}|{p.LowerBound ?? "null"}|{p.UpperBound ?? "null"}")),
                $"{r["content"]}|{r["length"]}|{r["partition_spec_id"]}|{r["added_snapshot_id"]}|{r["added_data_files_count"]}|{r["existing_data_files_count"]}|{r["deleted_data_files_count"]}|{r["added_delete_files_count"]}|{r["existing_delete_files_count"]}|{r["deleted_delete_files_count"]}|" +
                string.Join(";", r["partition_summaries"]!.AsArray().Select(p => $"{(p!["contains_null"]!.GetValue<bool>() ? "True" : "False")}|{(p["contains_nan"] is { } n ? (n.GetValue<bool>() ? "True" : "False") : "null")}|{Text(p["lower_bound"])}|{Text(p["upper_bound"])}")));
        }

        var files = (await MetadataTables.FilesAsync(t)).ToDictionary(f => $"{NormPath(f.FilePath)}@{f.ContentOffset}");
        var sparkFiles = Rows(tables, "files");
        Assert.Equal(files.Count, sparkFiles.Count);
        foreach (var r in sparkFiles)
        {
            var o = files[$"{NormPath(r!["file_path"]!.GetValue<string>())}@{r["content_offset"]?.GetValue<long>()}"];
            Assert.Equal(SparkFileText(r), FileText(o, r));
        }

        Assert.Equal(
            (await MetadataTables.EntriesAsync(t)).Select(e => $"{e.Status}|{e.SnapshotId}|{e.SequenceNumber}|{e.FileSequenceNumber}|{NormPath(e.DataFile.FilePath)}@{e.DataFile.ContentOffset}").Order(),
            Rows(tables, "entries").Select(r => $"{r!["status"]}|{r["snapshot_id"]}|{r["sequence_number"]?.GetValue<long>()}|{r["file_sequence_number"]?.GetValue<long>()}|{NormPath(r["data_file"]!["file_path"]!.GetValue<string>())}@{r["data_file"]!["content_offset"]?.GetValue<long>()}").Order());

        var partitions = await MetadataTables.PartitionsAsync(t);
        var sparkPartitions = Rows(tables, "partitions");
        Assert.Equal(partitions.Count, sparkPartitions.Count);
        string Counts(long records, int files, long bytes, long posRecords, int posFiles, long eqRecords, int eqFiles, long? last) =>
            $"{records}|{files}|{bytes}|{posRecords}|{posFiles}|{eqRecords}|{eqFiles}|{last}";
        var ours = partitions.Select(p => (Key: p.Partition, Row: p)).ToList();
        foreach (var r in sparkPartitions)
        {
            var key = r!["partition"] as JsonObject;
            var match = ours.Single(o => key is null
                ? o.Key.Count == 0
                : o.Key.Count == key.Count && o.Key.All(kv => PartitionText(kv.Value, key[kv.Key]) == Text(key[kv.Key])));
            var p = match.Row;
            Assert.Equal(
                Counts(p.RecordCount, p.FileCount, p.TotalDataFileSizeInBytes, p.PositionDeleteRecordCount, p.PositionDeleteFileCount, p.EqualityDeleteRecordCount, p.EqualityDeleteFileCount, p.LastUpdatedSnapshotId),
                Counts(r["record_count"]!.GetValue<long>(), r["file_count"]!.GetValue<int>(), r["total_data_file_size_in_bytes"]!.GetValue<long>(),
                    r["position_delete_record_count"]!.GetValue<long>(), r["position_delete_file_count"]!.GetValue<int>(),
                    r["equality_delete_record_count"]!.GetValue<long>(), r["equality_delete_file_count"]!.GetValue<int>(), r["last_updated_snapshot_id"]?.GetValue<long>()));
            if (key is not null) Assert.Equal(p.SpecId, r["spec_id"]!.GetValue<int>());
        }
        _ = label;
    }

    private static async Task<List<long>> RefIds(Table t, string name)
    {
        var ids = new List<long>();
        await foreach (var r in TableScan.ReadRowsAsync(t, new ScanRequest { Snapshot = SnapshotSelector.Ref(name) })) ids.Add((long)r[0]!);
        return ids.Order().ToList();
    }

    [Spec("Snapshot References")]
    [SkippableTheory]
    [InlineData("refs_v2")]
    [InlineData("refs_v3")]
    public async Task Spark_reads_our_branches_and_tags(string name)
    {
        Require.Spark();
        var t = fx.Tables[name].Table;
        var refs = SparkView(name)["refs"]!.AsObject();
        Assert.Equal(t.Metadata.Refs.Keys.Order(StringComparer.Ordinal), refs.Select(kv => kv.Key).Order(StringComparer.Ordinal));
        Assert.DoesNotContain("short-lived", t.Metadata.Refs.Keys); // expired by its max-ref-age
        foreach (var (refName, r) in refs)
        {
            Assert.Equal(t.Metadata.Refs[refName].SnapshotId, r!["snapshot_id"]!.GetValue<long>());
            Assert.Equal(t.Metadata.Refs[refName].Type == SnapshotRefType.Branch ? "BRANCH" : "TAG", r["type"]!.GetValue<string>());
            Assert.Equal(await RefIds(t, refName), r["ids"]!.AsArray().Select(x => x!.GetValue<long>()));
        }
        Assert.Contains(101L, await RefIds(t, "audit"));    // staged on the branch...
        Assert.DoesNotContain(100L, await RefIds(t, "audit")); // ...with a row delta on the branch
        Assert.DoesNotContain(101L, await RefIds(t, SnapshotRef.Main));
        Assert.Equal(await RefIds(t, SnapshotRef.Main), await RefIds(t, "ff")); // fast-forwarded
    }

    [Spec("Snapshots")]
    [Spec("Manifests")]
    [Spec("Snapshot References")]
    [SkippableTheory]
    [MemberData(nameof(MetadataTableCases))]
    public async Task Spark_sees_the_same_metadata_tables(string name)
    {
        Require.Spark();
        await AssertSameMetadataTables(fx.Tables[name].Table, SparkView(name)["metadata_tables"]!, name);
    }

    public static TheoryData<string> MetadataTableCases()
    {
        var data = new TheoryData<string>();
        foreach (var n in SparkTablesFixture.MetadataTableNames()) data.Add(n);
        return data;
    }

    private static List<string> SparkChanges(JsonNode changes) =>
        changes["rows"]!.AsArray().Select(r => $"{r!["_change_ordinal"]}|{r["_commit_snapshot_id"]}|{r["_change_type"]}|{r[r.AsObject().First().Key]}|{Text(r.AsObject().ElementAt(1).Value)}").Order().ToList();

    private static async Task<List<string>> OurChanges(Table t)
    {
        var list = new List<string>();
        await foreach (var c in ChangeFeed.ChangesAsync(t, null))
            list.Add($"{c.ChangeOrdinal}|{c.CommitSnapshotId}|{c.ChangeType.ToString().ToUpperInvariant()}|{c.Row[0]}|{c.Row[1] ?? "null"}");
        return list.Order().ToList();
    }

    /// <summary>Our change feed against Spark's <c>changes</c> table, for every table Spark can produce a changelog of.</summary>
    [Spec("Row-level Deletes")]
    [SkippableFact]
    public async Task Spark_reports_the_same_changes()
    {
        Require.Spark();
        var compared = new List<string>();
        foreach (var name in SparkTablesFixture.MetadataTableNames())
        {
            var changes = SparkView(name)["changes"]!;
            if (changes["error"] is { } error)
            {
                // Iceberg Java 1.12's changelog scan refuses tables with delete files; only the feed's own L0 tests cover those.
                Assert.Contains("Delete files are currently not supported in changelog scans", error.GetValue<string>());
                continue;
            }
            Assert.Equal(SparkChanges(changes), await OurChanges(fx.Tables[name].Table));
            compared.Add(name);
        }
        Assert.Contains("refs_v2", compared);
        Assert.Contains("refs_v3", compared);
        Assert.Contains("CopyOnWrite_v3", compared); // copy-on-write: whole rewritten files as DELETE + INSERT, carry-overs included
    }

    /// <summary>A table Spark gave branches, tags, a branch write, a rollback and a cherry-pick reads the same here.</summary>
    [Spec("Snapshot References")]
    [Spec("Snapshot Retention Policy")]
    [SkippableFact]
    public async Task We_read_Spark_branches_tags_rollback_and_cherry_pick()
    {
        Require.Spark();
        var made = fx.Spark!["create-refs"]!;
        Assert.True(made["error"] is null, $"Spark failed to build its table: {made["error"]}");
        var t = await fx.Catalog.RegisterTableAsync(TableIdentifier.Of("db", "spark_refs"), made["metadata_location"]!.GetValue<string>());
        await TableValidator.AssertValidAsync(t, new ValidationOptions { AllSnapshots = true, Deep = true });

        foreach (var (name, r) in made["refs"]!.AsObject())
        {
            Assert.Equal(r!["snapshot_id"]!.GetValue<long>(), t.Metadata.Refs[name].SnapshotId);
            Assert.Equal(r["ids"]!.AsArray().Select(x => x!.GetValue<long>()), await RefIds(t, name));
        }
        Assert.Equal(new SnapshotRef(t.Metadata.Refs["audit"].SnapshotId, SnapshotRefType.Branch, 2, null, 7L * 86_400_000), t.Metadata.Refs["audit"]);
        Assert.Equal(30L * 86_400_000, t.Metadata.Refs["t1"].MaxRefAgeMs);
        Assert.Equal(made["cherry_picked"]!.GetValue<long>().ToString(System.Globalization.CultureInfo.InvariantCulture), t.Metadata.CurrentSnapshot!.Summary["source-snapshot-id"]);
        var rolledBackTo = made["rolled_back_to"]!.GetValue<long>();
        Assert.Contains(MetadataTables.History(t), h => h.SnapshotId == rolledBackTo);
        Assert.Contains(MetadataTables.History(t), h => !h.IsCurrentAncestor);

        await AssertSameMetadataTables(t, made["metadata_tables"]!, "spark_refs");
        Assert.True(made["changes"]!["error"] is null, $"{made["changes"]!["error"]}");
        Assert.Equal(SparkChanges(made["changes"]!), await OurChanges(t));

        // Our expiry honours Spark's retention settings: audit keeps 2 snapshots, nothing tagged or on main expires.
        var expired = await Maintenance.ExpireAsync(t, new ExpireOptions { OlderThanMs = long.MaxValue, CleanExpiredFiles = false });
        Assert.All(t.Metadata.Refs.Values, r => Assert.NotNull(t.Metadata.SnapshotById(r.SnapshotId)));
        Assert.Equal(2, t.Metadata.Ancestors(t.Metadata.Refs["audit"].SnapshotId).Count());
        Assert.NotEmpty(expired.Expired);
    }
}
