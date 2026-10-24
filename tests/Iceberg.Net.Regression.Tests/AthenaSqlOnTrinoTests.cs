using Iceberg.Net.Athena;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Trino 478 standing in for Athena's engine (Athena engine v3 is Trino): the SQL <see cref="AthenaSql"/> generates
/// for queries — SELECT with time travel by version and by timestamp, quoted identifiers and literals, the
/// <c>$snapshots</c>/<c>$history</c>/<c>$files</c>/<c>$manifests</c>/<c>$partitions</c>/<c>$refs</c> metadata
/// tables — runs on Trino's Iceberg catalog (REST fixture + moto S3) against tables this library writes, and returns
/// what this library reads. Athena's own statements (Hive DDL, OPTIMIZE, VACUUM) do not parse on Trino; their
/// <see cref="TrinoSql"/> counterparts run instead, and this library checks what they did.
/// </summary>
[Spec("Scan Planning")]
[Spec("Snapshots")]
[Trait("Category", "Regression")]
public sealed class AthenaSqlOnTrinoTests : IAsyncLifetime
{
    private static readonly Schema Events = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "event_ts", PrimitiveType.TimestampTz),
    ]);

    private static readonly Dictionary<string, string> NoMinimumRetention = new()
    {
        ["iceberg.expire_snapshots_min_retention"] = "0s", ["iceberg.remove_orphan_files_min_retention"] = "0s",
    };

    private readonly Namespace _ns = Namespace.Of(TestEnvironment.Current.NewNamespace("athena_trino"));
    private ICatalog? _catalog;

    public async Task InitializeAsync()
    {
        if (TrinoOracle.SkipReason is not null) return;
        _catalog = await TestEnvironment.Current.CatalogAsync("rest");
        await _catalog.CreateNamespaceAsync(_ns);
    }

    public async Task DisposeAsync()
    {
        if (_catalog is null) return;
        foreach (var id in await _catalog.ListTablesAsync(_ns)) await _catalog.DropTableAsync(id, purge: true);
        await _catalog.DropNamespaceAsync(_ns);
        _catalog.Dispose();
    }

    private AthenaTable Athena(string name) => new(_ns.ToString(), name, TrinoOracle.Catalog);

    private static List<object?[]> Rows(int start, int count) => Enumerable.Range(start, count).Select(i => new object?[]
    {
        (long)i, i == 7 ? "it's" : $"name-{i}", IcebergTime.MicrosFromTimestamp(new DateTime(2026, 10, 1 + i % 3, 12, 0, 0, DateTimeKind.Utc)),
    }).ToList();

    private static async Task<List<long>> Ids(string sql) =>
        (await TrinoOracle.QueryAsync(sql)).Rows.Select(r => r[0]!.GetValue<long>()).Order().ToList();

    private static async Task<List<long>> OurIds(Table table, long? snapshotId = null) =>
        (await TableScan.ReadAllAsync(table, snapshotId)).Select(r => (long)r[0]!).Order().ToList();

    [SkippableFact]
    public async Task Athena_queries_time_travel_and_metadata_tables_run_on_trino()
    {
        Require.Trino();
        var table = await _catalog!.CreateTableAsync(new CreateTableRequest(new TableIdentifier(_ns, "events-1"), Events,
            PartitionSpec.BuilderFor(Events).Day("event_ts").Build(), FormatVersion: 2));
        await TableMutations.InsertAsync(table, Rows(0, 10));
        var first = table.Metadata.CurrentSnapshot!;
        await Task.Delay(20); // distinct commit milliseconds for FOR TIMESTAMP AS OF
        await TableMutations.InsertAsync(table, Rows(10, 10));
        var second = table.Metadata.CurrentSnapshot!;
        await Task.Delay(20);
        await TableMutations.DeleteByKeysAsync(table, ["id"], [[3L], [12L]], DeleteStrategy.CopyOnWrite);
        table = await _catalog.LoadTableAsync(table.Identifier);
        var t = Athena("events-1");

        Assert.Equal(await OurIds(table), await Ids(AthenaSql.Select(t, ["id"])));
        Assert.Equal(await OurIds(table, first.SnapshotId), await Ids(AthenaSql.Select(t, ["id"], AthenaSql.AsOfVersion(first.SnapshotId))));
        Assert.Equal(await OurIds(table, second.SnapshotId),
            await Ids(AthenaSql.Select(t, ["id"], AthenaSql.AsOf(DateTimeOffset.FromUnixTimeMilliseconds(second.TimestampMs)))));
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (long)i), // just before the second commit: the first snapshot
            await Ids(AthenaSql.Select(t, ["id"], AthenaSql.AsOf(DateTimeOffset.FromUnixTimeMilliseconds(second.TimestampMs - 1)))));
        Assert.Equal([7L], await Ids(AthenaSql.Select(t, ["id"], where: "\"name\" = " + AthenaSql.String("it's"))));
        Assert.Equal([0L, 3L], await Ids(AthenaSql.Select(t, ["id"], AthenaSql.AsOfVersion(first.SnapshotId), // ids < 5 on 2026-10-01
            "\"event_ts\" < " + AthenaSql.Timestamp(new DateTimeOffset(2026, 10, 2, 0, 0, 0, TimeSpan.Zero)) + " AND \"id\" < 5")));

        // Metadata tables: Trino reports the snapshots, files and partitions this library committed.
        var snapshots = await TrinoOracle.QueryAsync($"SELECT snapshot_id, parent_id, operation FROM {AthenaSql.MetadataTable(t, "snapshots")}");
        Assert.Equal(table.Metadata.Snapshots.Select(s => s.SnapshotId).Order(), snapshots.Column("snapshot_id").Select(v => v!.GetValue<long>()).Order());
        Assert.Contains("overwrite", snapshots.Column("operation").Select(v => v!.GetValue<string>()));
        var history = await TrinoOracle.QueryAsync($"SELECT snapshot_id FROM {AthenaSql.MetadataTable(t, "history")}");
        Assert.Equal(table.Metadata.SnapshotLog.Select(e => e.SnapshotId).Order(), history.Rows.Select(r => r[0]!.GetValue<long>()).Order());
        var files = await TrinoOracle.QueryAsync($"SELECT file_path, record_count FROM {AthenaSql.MetadataTable(t, "files")}");
        var ourFiles = await TableScan.PlanFilesAsync(table);
        Assert.Equal(ourFiles.Select(f => f.File.FilePath).Order(), files.Column("file_path").Select(v => v!.GetValue<string>()).Order());
        Assert.Equal(ourFiles.Sum(f => f.File.RecordCount), files.Column("record_count").Sum(v => v!.GetValue<long>()));
        var manifests = await TrinoOracle.QueryAsync($"SELECT path FROM {AthenaSql.MetadataTable(t, "manifests")}");
        Assert.Equal((await TableScan.ManifestsAsync(table, table.Metadata.CurrentSnapshot!)).Select(m => m.ManifestPath).Order(),
            manifests.Rows.Select(r => r[0]!.GetValue<string>()).Order());
        var partitions = await TrinoOracle.QueryAsync($"SELECT record_count FROM {AthenaSql.MetadataTable(t, "partitions")}");
        Assert.Equal(3, partitions.Rows.Count); // three event days
        Assert.Equal(18L, partitions.Rows.Sum(r => r[0]!.GetValue<long>()));
        var refs = await TrinoOracle.QueryAsync($"SELECT name, snapshot_id FROM {AthenaSql.MetadataTable(t, "refs")}");
        Assert.Equal("main", refs.Rows.Single()[0]!.GetValue<string>());
        Assert.Equal(table.Metadata.CurrentSnapshotId, refs.Rows.Single()[1]!.GetValue<long>());
    }

    [SkippableFact]
    public async Task Athena_only_statements_do_not_parse_on_trino_and_the_trino_dialect_does_their_work()
    {
        Require.Trino();
        var table = await _catalog!.CreateTableAsync(new CreateTableRequest(new TableIdentifier(_ns, "maintained"), Events, FormatVersion: 2));
        for (var batch = 0; batch < 3; batch++) await TableMutations.InsertAsync(table, Rows(batch * 5, 5));
        table = await _catalog.LoadTableAsync(table.Identifier);
        var expected = await OurIds(table);
        var t = Athena("maintained");

        // Athena's Hive DDL and its OPTIMIZE / VACUUM are Athena's own syntax.
        foreach (var athenaOnly in new[]
        {
            AthenaSql.Optimize(t), AthenaSql.Vacuum(t),
            AthenaSql.SetTableProperties(t, new AthenaIcebergProperties { VacuumMinSnapshotsToKeep = 1 }),
            AthenaSql.UnsetTableProperties(t, ["write_compression"]),
            AthenaSql.CreateTable(new AthenaTable(_ns.ToString(), "hive_ddl"),
                new Schema(0, [NestedField.Optional(1, "id", PrimitiveType.Long)]), "s3://iceberg-regression/rest-warehouse/hive_ddl"),
        })
        {
            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => TrinoOracle.QueryAsync(athenaOnly));
            Assert.Contains("SYNTAX_ERROR", e.Message);
        }

        // OPTIMIZE: one rewrite of the three small files.
        await TrinoOracle.QueryAsync(TrinoSql.Optimize(t, fileSizeThresholdBytes: 128L << 20));
        table = await _catalog.LoadTableAsync(table.Identifier);
        Assert.Equal("replace", table.Metadata.CurrentSnapshot!.Operation);
        Assert.Single(await TableScan.PlanFilesAsync(table));
        Assert.Equal(expected, await OurIds(table));

        // VACUUM: expire every snapshot but the current one, then remove what nothing references.
        var orphan = $"{table.Location}/data/orphan-{Guid.NewGuid():N}.parquet";
        await table.IO.WriteAsync(orphan, "not referenced"u8.ToArray());
        foreach (var sql in TrinoSql.Vacuum(t, TimeSpan.Zero)) await TrinoOracle.QueryAsync(sql, NoMinimumRetention);
        table = await _catalog.LoadTableAsync(table.Identifier);
        Assert.Single(table.Metadata.Snapshots);
        Assert.False(await table.IO.ExistsAsync(orphan));
        Assert.Equal(expected, await OurIds(table));
        var report = await TableValidator.ValidateAsync(table, new ValidationOptions { AllSnapshots = true, Deep = true });
        Assert.True(report.IsValid, report.ToString());

        // Trino keeps its 7-day floor unless the session lowers it.
        var floor = await Assert.ThrowsAsync<InvalidOperationException>(() => TrinoOracle.QueryAsync(TrinoSql.ExpireSnapshots(t, TimeSpan.FromDays(1))));
        Assert.Contains("minimum retention", floor.Message);
    }

    [SkippableFact]
    public async Task Trino_dialect_create_table_makes_the_iceberg_schema_and_spec()
    {
        Require.Trino();
        var schema = new Schema(0,
        [
            NestedField.Require(1, "id", PrimitiveType.Long),
            NestedField.Optional(2, "name", PrimitiveType.String, "display name, it's"),
            NestedField.Optional(3, "ts", PrimitiveType.TimestampTz),
            NestedField.Optional(4, "local ts", PrimitiveType.Timestamp),
            NestedField.Optional(5, "day", PrimitiveType.Date),
            NestedField.Optional(6, "at", PrimitiveType.Time),
            NestedField.Optional(7, "uid", PrimitiveType.Uuid),
            NestedField.Optional(8, "amount", new DecimalType(38, 2)),
            NestedField.Optional(9, "ratio", PrimitiveType.Float),
            NestedField.Optional(10, "score", PrimitiveType.Double),
            NestedField.Optional(11, "qty", PrimitiveType.Int),
            NestedField.Optional(12, "flag", PrimitiveType.Boolean),
            NestedField.Optional(13, "raw", PrimitiveType.Binary),
            NestedField.Optional(14, "tags", new ListType(20, PrimitiveType.String, false)),
            NestedField.Optional(15, "attrs", new MapType(21, PrimitiveType.String, 22, PrimitiveType.Int, false)),
            NestedField.Optional(16, "loc", new StructType(NestedField.Optional(23, "lat lon", PrimitiveType.Double), NestedField.Optional(24, "label", PrimitiveType.String))),
            NestedField.Optional(17, "region", PrimitiveType.String),
        ]);
        var spec = PartitionSpec.BuilderFor(schema).Bucket("id", 4).Day("ts").Truncate("name", 3).Identity("region").Year("day").Hour("local ts").Build();
        var t = Athena("created-by-trino");
        var location = $"{TestEnvironment.Current.Warehouse.TrimEnd('/')}/trino-created/{_ns}/{Guid.NewGuid():N}";

        await TrinoOracle.QueryAsync(TrinoSql.CreateTable(t, schema, spec, location, formatVersion: 2));
        await TrinoOracle.QueryAsync($"INSERT INTO {t.Dml} (\"id\", \"name\", \"ts\", \"region\", \"loc\") VALUES " +
            "(1, 'abc', TIMESTAMP '2026-10-01 10:00:00 UTC', 'eu', ROW(1.5, 'x')), (2, 'it''s', TIMESTAMP '2026-10-02 10:00:00 UTC', NULL, NULL)");

        var table = await _catalog!.LoadTableAsync(new TableIdentifier(_ns, "created-by-trino"));
        Assert.Equal(2, table.Metadata.FormatVersion);
        Assert.Equal(location, table.Location);
        // Trino assigns its own field IDs and makes every column optional; names, types and docs carry over.
        Assert.Equal(schema.Columns.Select(c => (c.Name, TrinoSql.Type(c.Type), c.Doc)), table.Schema.Columns.Select(c => (c.Name, TrinoSql.Type(c.Type), c.Doc)));
        Assert.All(table.Schema.Columns, c => Assert.False(c.Required));
        string Source(Schema s, PartitionField f) => s.Columns.Single(c => c.Id == f.SourceId).Name;
        Assert.Equal(spec.Fields.Select(f => (Source(schema, f), f.Transform.ToString())), table.Spec.Fields.Select(f => (Source(table.Schema, f), f.Transform.ToString())));

        var rows = (await TableScan.ReadAllAsync(table)).OrderBy(r => (long)r[0]!).ToList();
        Assert.Equal([(1L, "abc"), (2L, "it's")], rows.Select(r => ((long)r[0]!, (string?)r[1])));
        Assert.Equal(2, (await TableScan.PlanFilesAsync(table)).Count); // the two rows share no partition

        Assert.Throws<ArgumentException>(() => TrinoSql.CreateTable(t, new Schema(0, [NestedField.Optional(1, "Id", PrimitiveType.Long)])));
        Assert.Throws<NotSupportedException>(() => TrinoSql.Type(new FixedType(16)));
        Assert.Equal("'86400000ms'", TrinoSql.Duration(TimeSpan.FromDays(1)));
    }
}
