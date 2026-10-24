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


        await BuildUpdateMergeAsync();
        await BuildNestedUpdatesAsync();
        await BuildCherryPicksAsync();
        await BuildQueriesAsync();

        // One Spark run: every table (some also with metadata tables and changelog), the MERGEs Spark replays, the read
        // queries, and a table with refs that Spark builds itself.
        var withMetadataTables = new HashSet<string>(MetadataTableNames());
        SparkMadeWarehouse = Path.Combine(_warehouse, "spark-made");
        Spark = await SparkOracle.ReadTablesAsync(Tables
            .Select(kv => (withMetadataTables.Contains(kv.Key) ? "meta:" : "") + kv.Value.Table.MetadataLocation!)
            .Concat(SparkMerges.Values).Concat(CherryPicks.Values).Concat(Queries.Select(q => q.Key))
            .Append($"create-refs:{SparkMadeWarehouse}"));
    }

    /// <summary>
    /// Keys of <see cref="Spark"/> for MERGEs Spark replays itself (interop/spark_oracle.py "merge:" entries): Spark
    /// copies our table's pre-merge snapshot, runs <see cref="MergeSql"/> with our source table, and describes the result.
    /// </summary>
    public Dictionary<string, string> SparkMerges { get; } = new();

    /// <summary>The MERGE that <see cref="MergeClauses"/> expresses, in Spark SQL.</summary>
    public const string MergeSql = """
        MERGE INTO {target} t USING {source} s ON t.id = s.id
        WHEN MATCHED AND s.qty = 0 THEN DELETE
        WHEN MATCHED THEN UPDATE SET name = s.name, amount = t.amount + 1
        WHEN NOT MATCHED AND s.qty > 2 THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND t.region = 'us' THEN DELETE
        """;

    public static void MergeClauses(MergeBuilder m) => m
        .WhenMatchedDelete((t, s) => (int?)s[5] == 0)
        .WhenMatchedUpdate((t, s) => [t[0], s[1], (double)t[2]! + 1, t[3], t[4], t[5]])
        .WhenNotMatchedInsert(s => (int?)s[5] > 2)
        .WhenNotMatchedBySourceDelete(Expr.Equal("region", "us"));

    /// <summary>UPDATE (with a partition move) and MERGE (every clause kind) on v2 copy-on-write and v3 deletion vectors.</summary>
    private async Task BuildUpdateMergeAsync()
    {
        var sourceTable = await _catalog!.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "merge_source"), Keyed, FormatVersion: 2));
        var source = CatalogConformanceTests.Rows(20, 20).Select(r => { r[1] = $"src-{r[0]}"; return r; }).ToList(); // 20..29 match, 30..39 do not
        await TableMutations.InsertAsync(sourceTable, source);
        sourceTable = await _catalog.LoadTableAsync(sourceTable.Identifier);

        foreach (var v in new[] { 2, 3 })
        {
            var strategy = v == 2 ? DeleteStrategy.CopyOnWrite : DeleteStrategy.PositionDeletes;
            await Build($"updated_v{v}", v, async t =>
            {
                // region is an identity partition source: these rows move partition.
                await TableMutations.UpdateAsync(t, Expr.LessThan("id", 6L), u => u.Set("region", "apac").Set("name", r => $"moved-{r[0]}"), strategy);
                await TableMutations.UpdateAsync(t, Expr.Equal("region", "eu"), u => u.Set("qty", r => r[5] is int q ? q + 100 : null), strategy);
            }, CatalogConformanceTests.Spec);

            long? before = null;
            var name = $"merged_into_v{v}";
            await Build(name, v, async t =>
            {
                await t.RefreshAsync();
                before = t.Metadata.CurrentSnapshotId;
                await TableMutations.MergeAsync(t, source, ["id"], MergeClauses, strategy);
            }, v == 3 ? CatalogConformanceTests.Spec : null);
            var spec = Path.Combine(_warehouse, $"{name}.merge.json");
            await File.WriteAllTextAsync(spec, new JsonObject
            {
                ["target"] = Tables[name].Table.MetadataLocation,
                ["snapshot_id"] = before,
                ["format_version"] = v,
                ["source"] = sourceTable.MetadataLocation,
                ["sql"] = MergeSql,
            }.ToJsonString());
            SparkMerges[name] = $"merge:{spec}";
        }
    }

    /// <summary>id, address struct&lt;city, zip, geo struct&lt;lat, lon&gt;&gt; and tag struct&lt;code (required), note&gt;.</summary>
    public static readonly Schema NestedUpdateSchema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "address", new StructType([
            NestedField.Optional(3, "city", PrimitiveType.String),
            NestedField.Optional(4, "zip", PrimitiveType.String),
            NestedField.Optional(5, "geo", new StructType([NestedField.Optional(6, "lat", PrimitiveType.Double), NestedField.Optional(7, "lon", PrimitiveType.Double)]))])),
        NestedField.Optional(8, "tag", new StructType([NestedField.Require(9, "code", PrimitiveType.String), NestedField.Optional(10, "note", PrimitiveType.String)])),
    ], identifierFieldIds: [1]);

    /// <summary>Null address every 5th row, null geo every 3rd, null tag every 4th.</summary>
    public static List<object?[]> NestedUpdateRows(int from, int count, string prefix = "") => Enumerable.Range(from, count).Select(i => new object?[]
    {
        (long)i,
        i % 5 == 0 ? null : new object?[] { $"{prefix}city{i}", i % 2 == 0 ? null : $"z{i}", i % 3 == 0 ? null : new object?[] { i * 1.0, -i * 1.0 } },
        i % 4 == 0 ? null : new object?[] { $"{prefix}c{i}", null },
    }).ToList();

    /// <summary>
    /// The nested UPDATE / MERGE statements <see cref="BuildNestedUpdatesAsync"/> runs, in Spark SQL: one and two levels
    /// deep, on rows whose enclosing struct (address, geo) is null, and a MERGE assigning two fields of one struct.
    /// </summary>
    public static readonly string[] NestedUpdateSql =
    [
        "UPDATE {target} SET address.city = concat('c-', CAST(id AS string)) WHERE id < 12",
        "UPDATE {target} SET address.geo.lat = 9.5D WHERE id >= 6",
        "UPDATE {target} SET tag.note = 'n' WHERE id IN (1, 2)",
        "MERGE INTO {target} t USING {source} s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.address.zip = 'M', t.address.geo.lon = 1.25D WHEN NOT MATCHED THEN INSERT *",
    ];

    /// <summary>The statement Spark and we both refuse: tag is null in row 4 and its required code is not assigned.</summary>
    public const string NestedUpdateRequiredSql = "UPDATE {target} SET tag.note = 'x' WHERE id = 4";

    /// <summary>Nested UPDATE and MERGE (v2 copy-on-write, v3 deletion vectors), replayed by Spark on a copy of the table.</summary>
    private async Task BuildNestedUpdatesAsync()
    {
        var sourceTable = await _catalog!.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "nested_merge_source"), NestedUpdateSchema, FormatVersion: 2));
        var source = NestedUpdateRows(15, 8, "s-"); // 15..19 match (15: null address), 20..22 are new
        await TableMutations.InsertAsync(sourceTable, source);
        sourceTable = await _catalog.LoadTableAsync(sourceTable.Identifier);

        foreach (var v in new[] { 2, 3 })
        {
            var strategy = v == 2 ? DeleteStrategy.CopyOnWrite : DeleteStrategy.PositionDeletes;
            var name = $"nested_updated_v{v}";
            var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", name), NestedUpdateSchema, FormatVersion: v));
            await TableMutations.InsertAsync(t, NestedUpdateRows(0, 20));
            t = await _catalog.LoadTableAsync(t.Identifier);
            var before = t.Metadata.CurrentSnapshotId;
            await TableMutations.UpdateAsync(t, Expr.LessThan("id", 12L), u => u.Set("address.city", r => $"c-{r[0]}"), strategy);
            await TableMutations.UpdateAsync(t, Expr.GreaterThanOrEqual("id", 6L), u => u.Set("address.geo.lat", 9.5), strategy);
            await TableMutations.UpdateAsync(t, Expr.In("id", 1L, 2L), u => u.Set("tag.note", "n"), strategy);
            await TableMutations.MergeAsync(t, source, ["id"], m => m.WhenMatchedUpdate(u => u.Set("address.zip", "M").Set("address.geo.lon", 1.25)).WhenNotMatchedInsert(), strategy);
            NestedUpdateRefused[name] = await Record.ExceptionAsync(() => TableMutations.UpdateAsync(t, Expr.Equal("id", 4L), u => u.Set("tag.note", "x"), strategy));
            t = await _catalog.LoadTableAsync(t.Identifier);
            Tables[name] = new Built(t, (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!), v >= 3 ? await TableScan.ReadWithLineageAsync(t) : null);

            foreach (var (key, sql) in new[] { (name, (JsonNode)new JsonArray(NestedUpdateSql.Select(x => (JsonNode?)x).ToArray())), ($"{name}_required", NestedUpdateRequiredSql) })
            {
                var spec = Path.Combine(_warehouse, $"{key}.merge.json");
                await File.WriteAllTextAsync(spec, new JsonObject
                {
                    ["target"] = t.MetadataLocation,
                    ["snapshot_id"] = before,
                    ["format_version"] = v,
                    ["source"] = sourceTable.MetadataLocation,
                    ["sql"] = sql,
                    // A DataFrame copy would make tag.code nullable, and Spark would then write the null it refuses here.
                    ["ddl"] = "CREATE TABLE {target} (id bigint NOT NULL, address struct<city: string, zip: string, geo: struct<lat: double, lon: double>>, " +
                        $"tag struct<code: string NOT NULL, note: string>) USING iceberg TBLPROPERTIES ('format-version'='{v}')",
                }.ToJsonString());
                SparkMerges[key] = $"merge:{spec}";
            }
        }
    }

    /// <summary>What our nested UPDATE that leaves a required field of a null struct unassigned threw (by table).</summary>
    public Dictionary<string, Exception?> NestedUpdateRefused { get; } = new();

    /// <summary>
    /// Keys of <see cref="Spark"/> for cherry-picks (interop/spark_oracle.py "cherrypick:" entries): Spark runs
    /// <c>CALL system.cherrypick_snapshot</c> on a copy of the table before our picks, and Iceberg Java describes what
    /// its picks and ours committed.
    /// </summary>
    public Dictionary<string, string> CherryPicks { get; } = new();

    /// <summary>
    /// An append and a dynamic partition overwrite staged (WAP-style: committed, then rolled back) under spec 0
    /// (identity region); the spec then becomes bucket(4, id) and main appends under it; then both are cherry-picked.
    /// </summary>
    private async Task BuildCherryPicksAsync()
    {
        foreach (var v in new[] { 2, 3 })
        {
            var name = $"cherry_picked_v{v}";
            var spec = Partitioning.PartitionSpec.BuilderFor(Keyed).Identity("region").Build();
            var t = await _catalog!.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", name), Keyed, spec, FormatVersion: v));
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(0, 10));
            var parent = t.Metadata.CurrentSnapshotId!.Value;

            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(100, 4));
            var staged = t.Metadata.CurrentSnapshotId!.Value;
            await t.ManageSnapshots().RollbackTo(parent).CommitAsync();

            var eu = (await TableScan.PlanFilesAsync(t)).Where(x => (string?)x.File.Partition[0] == "eu").Select(x => x.File.FilePath).ToHashSet();
            var replacement = CatalogConformanceTests.Rows(300, 2).Select(r => { r[4] = "eu"; return r; }).ToList();
            await SnapshotProducer.Overwrite(t, f => eu.Contains(f.FilePath)).Set("replace-partitions", "true")
                .AddFiles(await TableWriter.WriteAsync(t, replacement)).CommitAsync();
            var stagedOverwrite = t.Metadata.CurrentSnapshotId!.Value;
            await t.ManageSnapshots().RollbackTo(parent).CommitAsync();

            await t.UpdateSpecAsync(p => p.RemoveField("region").AddField("id", Transforms.Transform.Bucket(4)));
            t = await _catalog.LoadTableAsync(t.Identifier);
            await TableMutations.InsertAsync(t, CatalogConformanceTests.Rows(200, 4)); // under spec 1, eu rows included
            t = await _catalog.LoadTableAsync(t.Identifier);
            var head = t.Metadata.CurrentSnapshotId!.Value;
            var beforePicks = t.MetadataLocation;

            await t.ManageSnapshots().CherryPick(stagedOverwrite).CommitAsync();
            await t.ManageSnapshots().CherryPick(staged).CommitAsync();
            t = await _catalog.LoadTableAsync(t.Identifier);
            Tables[name] = new Built(t, (await TableScan.ReadAllAsync(t)).ToDictionary(r => (long)r[0]!), v >= 3 ? await TableScan.ReadWithLineageAsync(t) : null);

            var path = Path.Combine(_warehouse, $"{name}.cherrypick.json");
            await File.WriteAllTextAsync(path, new JsonObject
            {
                ["before"] = beforePicks,
                ["after"] = t.MetadataLocation,
                ["head"] = head,
                ["picks"] = new JsonArray(stagedOverwrite, staged),
            }.ToJsonString());
            CherryPicks[name] = $"cherrypick:{path}";
        }
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

    /// <summary>Tables Spark also dumps the metadata tables and the changelog of.</summary>
    public static IEnumerable<string> MetadataTableNames() =>
        ["refs_v2", "refs_v3", "PositionDeletes_v2", "PositionDeletes_v3", "EqualityDeletes_v2", "CopyOnWrite_v3", "partitioned_v3", "spec_evolved_v2",
         "cherry_picked_v2", "cherry_picked_v3"];

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
        "updated_v2", "updated_v3", "merged_into_v2", "merged_into_v3", "cherry_picked_v2", "cherry_picked_v3",
    };

    public static TheoryData<string> AllNames()
    {
        var all = Names();
        foreach (var n in new[] { "nested_v3", "nested_updated_v2", "nested_updated_v3" }) all.Add(n);
        return all;
    }

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
    [InlineData("updated_v3")]
    [InlineData("merged_into_v3")]
    [InlineData("nested_updated_v3")]
    [InlineData("cherry_picked_v3")]
    public void Spark_derives_the_same_row_lineage(string name)
    {
        Require.Spark();
        var ours = fx.Tables[name].Lineage!.ToDictionary(r => (long)r.Row[0]!, r => (r.RowId, r.LastUpdatedSequenceNumber));
        var spark = SparkView(name)["row_lineage"]!.AsArray().ToDictionary(
            r => r!["id"]!.GetValue<long>(),
            r => (r!["row_id"]?.GetValue<long>(), r["last_updated_seq"]?.GetValue<long>()));
        Assert.Equal(ours.OrderBy(kv => kv.Key), spark.OrderBy(kv => kv.Key));
    }

    /// <summary>Spark runs the same MERGE (SQL) on a copy of the pre-merge table and ends with the rows we committed.</summary>
    [Spec("Row-level Deletes")]
    [SkippableTheory]
    [InlineData("merged_into_v2")]
    [InlineData("merged_into_v3")]
    public void Spark_merge_on_a_copy_matches_ours(string name)
    {
        Require.Spark();
        var view = fx.Spark![fx.SparkMerges[name]]!;
        Assert.True(view["error"] is null, $"Spark failed to merge {name}: {view["error"]}");
        var spark = view["rows"]!.AsArray().ToDictionary(r => r!["id"]!.GetValue<long>(), r => r!.AsObject());
        var ours = fx.Tables[name].Rows;
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
        // The merge did exercise every clause: matched update and delete, conditional insert, not matched by source.
        Assert.Contains(ours.Values, r => (string?)r[1] == "src-21");
        Assert.DoesNotContain(20L, ours.Keys);
        Assert.Contains(33L, ours.Keys);
        Assert.DoesNotContain(31L, ours.Keys);
        Assert.DoesNotContain(1L, ours.Keys);
    }

    /// <summary>
    /// Nested UPDATE and MERGE (<c>Set("address.city", …)</c>, two levels deep, and through null structs): Spark reads
    /// our result as we do, and running the same SQL on a copy of the table ends with the same rows. Spark's nested
    /// assignment turns a null enclosing struct into a struct whose other fields are null, as ours does.
    /// </summary>
    [Spec("Nested Types")]
    [Spec("Row-level Deletes")]
    [SkippableTheory]
    [InlineData("nested_updated_v2")]
    [InlineData("nested_updated_v3")]
    public void Spark_runs_the_same_nested_updates(string name)
    {
        Require.Spark();
        var schema = SparkTablesFixture.NestedUpdateSchema;
        var rows = fx.Tables[name].Rows;
        var ours = SparkJson.Rows(schema, rows.Values).OrderBy(kv => kv.Key).ToList();
        Assert.Equal(ours, SparkJson.Rows(schema, SparkView(name)["rows"]!.AsArray()).OrderBy(kv => kv.Key));
        var replay = fx.Spark![fx.SparkMerges[name]]!;
        Assert.True(replay["error"] is null, $"Spark failed the nested updates on {name}: {replay["error"]}");
        Assert.Equal(ours, SparkJson.Rows(schema, replay["rows"]!.AsArray()).OrderBy(kv => kv.Key));

        // The cases that matter did occur: null address (0, 10, 15), null geo (6, 9), inserted rows.
        Assert.Equal(new object?[] { "c-0", null, null }, rows[0][1]);
        Assert.Equal(new object?[] { "c-10", null, new object?[] { 9.5, null } }, rows[10][1]);
        Assert.Equal(new object?[] { null, "M", new object?[] { 9.5, 1.25 } }, rows[15][1]);
        Assert.Equal(new object?[] { "c-6", null, new object?[] { 9.5, null } }, rows[6][1]);
        Assert.Equal(new object?[] { "c-9", "z9", new object?[] { 9.5, null } }, rows[9][1]);
        Assert.Equal(new object?[] { "c1", "n" }, rows[1][2]);
        Assert.Equal(23, rows.Count);
    }

    /// <summary>
    /// Assigning a field of a null struct whose other field is required: Spark fails the statement at run time
    /// (<c>NOT_NULL_ASSERT_VIOLATION</c>) and commits nothing; we refuse it with an <see cref="ArgumentException"/>
    /// before writing anything.
    /// </summary>
    [Spec("Nested Types")]
    [SkippableTheory]
    [InlineData("nested_updated_v2")]
    [InlineData("nested_updated_v3")]
    public void Spark_and_we_refuse_to_leave_a_required_field_of_a_null_struct_null(string name)
    {
        Require.Spark();
        Assert.IsType<ArgumentException>(fx.NestedUpdateRefused[name]);
        Assert.Contains("required field 'code'", fx.NestedUpdateRefused[name]!.Message);
        var replay = fx.Spark![fx.SparkMerges[$"{name}_required"]]!;
        var error = replay["error"]?.GetValue<string>();
        Assert.True(error is not null, $"Spark accepted it: {replay["rows"]?.ToJsonString()}");
        Assert.Contains("NOT_NULL_ASSERT_VIOLATION", error);
    }

    /// <summary>Summary keys that name the engine, or count manifests (Java's manifest layout is not ours to match).</summary>
    private static readonly HashSet<string> EngineSummaryKeys =
        ["engine-name", "engine-version", "iceberg-version", "app-id", "app-name", "spark.app.id", "manifests-created", "manifests-kept", "manifests-replaced"];

    /// <summary>One committed snapshot as Iceberg Java describes it (spark_oracle.py describe_picks), without engine identity.</summary>
    private static string Pick(JsonNode p)
    {
        static string Files(JsonNode? files) => string.Join(";", files!.AsArray().Select(f =>
            $"{NormPath(f!["path"]!.GetValue<string>())}|{f["spec_id"]}|{f["partition"]}|{f["record_count"]}|{Text(f["first_row_id"])}"));
        var summary = p["summary"]!.AsObject().Where(kv => !EngineSummaryKeys.Contains(kv.Key)).OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}={kv.Value}");
        return $"{p["operation"]}\n  summary: {string.Join(",", summary)}\n  first_row_id: {Text(p["first_row_id"])}\n  added: {Files(p["added"])}\n  removed: {Files(p["removed"])}";
    }

    /// <summary>
    /// Cherry-picking an append and a dynamic partition overwrite staged under an older partition spec: Spark's
    /// <c>CALL system.cherrypick_snapshot</c> on a copy of the table commits what we committed (operation, summary,
    /// added and removed files with their spec and partition, row IDs), and reads our result as we do.
    /// </summary>
    [Spec("Partition Evolution")]
    [Spec("Snapshot References")]
    [SkippableTheory]
    [InlineData("cherry_picked_v2")]
    [InlineData("cherry_picked_v3")]
    public void Spark_cherry_picks_snapshots_of_an_older_spec_as_we_do(string name)
    {
        Require.Spark();
        var view = fx.Spark![fx.CherryPicks[name]]!;
        Assert.True(view["error"] is null, $"Spark failed to cherry-pick on {name}: {view["error"]}");
        var spark = view["spark"]!["picks"]!.AsArray().Select(p => Pick(p!)).ToList();
        var ours = view["ours"]!["picks"]!.AsArray().Select(p => Pick(p!)).ToList();
        Assert.Equal(2, spark.Count);
        Assert.True(spark.SequenceEqual(ours), $"spark:\n{string.Join("\n", spark)}\nours:\n{string.Join("\n", ours)}");

        var theirs = view["spark"]!["description"]!;
        var mine = view["ours"]!["description"]!;
        Assert.Equal(SparkJson.Rows(fx.Tables[name].Table.Schema, mine["rows"]!.AsArray()).OrderBy(kv => kv.Key), SparkJson.Rows(fx.Tables[name].Table.Schema, theirs["rows"]!.AsArray()).OrderBy(kv => kv.Key));
        if (theirs["row_lineage"] is { } lineage) Assert.Equal(lineage.ToJsonString(), mine["row_lineage"]!.ToJsonString());

        // The replayed files kept spec 0 (identity region) and the overwrite replaced only spec 0's eu partition.
        var picks = view["spark"]!["picks"]!.AsArray();
        Assert.All(picks.SelectMany(p => p!["added"]!.AsArray()), f => Assert.Equal(0, f!["spec_id"]!.GetValue<int>()));
        Assert.All(picks[0]!["removed"]!.AsArray(), f => Assert.Equal("region=eu", f!["partition"]!.GetValue<string>()));
        Assert.NotEmpty(picks[0]!["removed"]!.AsArray());
        var ids = fx.Tables[name].Rows.Keys.Order().ToList();
        Assert.Contains(300L, ids);
        Assert.Contains(100L, ids);
        Assert.Contains(200L, ids); // main's eu row under spec 1 survives the overwrite of spec 0's eu partition
        Assert.DoesNotContain(0L, ids);
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
