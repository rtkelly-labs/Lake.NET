using System.Text.Json;
using Iceberg.Net.Catalog;
using Iceberg.Net.IO;
using Iceberg.Net.Spark;

namespace Iceberg.Net.Ecosystem.Tests;

/// <summary>The Spark SQL the procedure helpers generate, character for character (Spark runs it in <see cref="SparkProcedureOracleTests"/>).</summary>
public sealed class SparkSqlTests
{
    private static readonly IcebergProcedures Procs = new("glue");
    private static readonly SparkTable Events = new("glue", "analytics", "events");

    [Theory]
    [InlineData("events", "`events`")]
    [InlineData("select", "`select`")]
    [InlineData("we`ird", "`we``ird`")]
    [InlineData("a.b", "`a.b`")]
    public void Identifiers_are_always_back_quoted(string name, string expected) => Assert.Equal(expected, SparkSql.Identifier(name));

    [Theory]
    [InlineData("plain", "'plain'")]
    [InlineData("it's", @"'it\'s'")]
    [InlineData(@"back\slash", @"'back\\slash'")]
    [InlineData("a;b", "'a;b'")]
    [InlineData("line\nbreak", @"'line\nbreak'")]
    public void String_literals_escape_quotes_and_backslashes(string value, string expected) => Assert.Equal(expected, SparkSql.String(value));

    [Fact]
    public void Literals_cover_the_argument_types_procedures_take()
    {
        Assert.Equal("NULL", SparkSql.Literal(null));
        Assert.Equal("true", SparkSql.Literal(true));
        Assert.Equal("5", SparkSql.Literal(5));
        Assert.Equal("123L", SparkSql.Literal(123L));
        Assert.Equal("1.5D", SparkSql.Literal(1.5));
        Assert.Equal("1.25BD", SparkSql.Literal(1.25m));
        Assert.Equal("TIMESTAMP '2026-10-01 12:30:00.000000Z'", SparkSql.Literal(new DateTimeOffset(2026, 10, 1, 13, 30, 0, TimeSpan.FromHours(1))));
        Assert.Equal("array(1L, 2L)", SparkSql.Literal(new[] { 1L, 2L }));
        Assert.Equal("array('a', 'b\\'c')", SparkSql.Literal(new List<string> { "a", "b'c" }));
        Assert.Equal("map('a', '1', 'b', '2')", SparkSql.Literal(new Dictionary<string, string> { ["b"] = "2", ["a"] = "1" }));
        Assert.Throws<ArgumentException>(() => SparkSql.Literal(double.NaN));
        Assert.Throws<ArgumentException>(() => SparkSql.Literal(new object()));
    }

    [Fact]
    public void Table_names_quote_every_part_and_the_procedure_argument_omits_the_catalog()
    {
        var t = new SparkTable("glue", ["raw", "eu-west"], "my table");
        Assert.Equal("`glue`.`raw`.`eu-west`.`my table`", t.QualifiedName);
        Assert.Equal("'`raw`.`eu-west`.`my table`'", t.ProcedureArgument);
        Assert.Equal("`c`.`a`.`b`.`t`", SparkTable.From("c", TableIdentifier.Of("a.b", "t")).QualifiedName);
    }

    [Fact]
    public void Rewrite_data_files_bin_pack_with_options_and_a_where_filter()
    {
        var sql = Procs.RewriteDataFiles(Events, new RewriteDataFilesOptions
        {
            Strategy = RewriteStrategy.BinPack,
            Where = "event_date >= '2026-01-01' AND name <> 'a;b'",
            TargetFileSizeBytes = 512L * 1024 * 1024,
            MinInputFiles = 2,
            PartialProgressEnabled = true,
            Options = { ["max-file-group-size-bytes"] = "1073741824" },
        }).ToSql();
        Assert.Equal(
            "CALL `glue`.system.`rewrite_data_files`(table => '`analytics`.`events`', strategy => 'binpack', " +
            "options => map('max-file-group-size-bytes', '1073741824', 'min-input-files', '2', 'partial-progress.enabled', 'true', 'target-file-size-bytes', '536870912'), " +
            @"where => 'event_date >= \'2026-01-01\' AND name <> \'a;b\'')", sql);
    }

    [Fact]
    public void Rewrite_data_files_with_a_sort_order_implies_the_sort_strategy()
    {
        Assert.Equal("CALL `glue`.system.`rewrite_data_files`(table => '`analytics`.`events`', strategy => 'sort', sort_order => 'id DESC NULLS LAST')",
            Procs.RewriteDataFiles(Events, new RewriteDataFilesOptions { SortOrder = "id DESC NULLS LAST" }).ToSql());
        Assert.Equal("CALL `glue`.system.`rewrite_data_files`(table => '`analytics`.`events`', strategy => 'sort', sort_order => 'zorder(a,b)')",
            Procs.RewriteDataFiles(Events, new RewriteDataFilesOptions { SortOrder = RewriteDataFilesOptions.ZOrder("a", "b") }).ToSql());
        Assert.Equal("CALL `glue`.system.`rewrite_data_files`(table => '`analytics`.`events`')", Procs.RewriteDataFiles(Events).ToSql());
        Assert.Throws<ArgumentException>(() => Procs.RewriteDataFiles(Events, new RewriteDataFilesOptions { Strategy = RewriteStrategy.BinPack, SortOrder = "id" }));
    }

    [Fact]
    public void Maintenance_procedures()
    {
        var cutoff = new DateTimeOffset(2026, 10, 20, 0, 0, 0, TimeSpan.Zero);
        Assert.Equal("CALL `glue`.system.`expire_snapshots`(table => '`analytics`.`events`', older_than => TIMESTAMP '2026-10-20 00:00:00.000000Z', retain_last => 5, " +
            "snapshot_ids => array(1L, 2L), clean_expired_metadata => true)",
            Procs.ExpireSnapshots(Events, new ExpireSnapshotsOptions { OlderThan = cutoff, RetainLast = 5, SnapshotIds = [1, 2], CleanExpiredMetadata = true }).ToSql());
        Assert.Equal("CALL `glue`.system.`remove_orphan_files`(table => '`analytics`.`events`', older_than => TIMESTAMP '2026-10-20 00:00:00.000000Z', dry_run => true, " +
            "equal_schemes => map('s3a', 's3', 's3n', 's3'), prefix_mismatch_mode => 'IGNORE')",
            Procs.RemoveOrphanFiles(Events, new RemoveOrphanFilesOptions
            {
                OlderThan = cutoff, DryRun = true, EqualSchemes = { ["s3n"] = "s3", ["s3a"] = "s3" }, PrefixMismatchMode = "IGNORE",
            }).ToSql());
        Assert.Equal("CALL `glue`.system.`rewrite_manifests`(table => '`analytics`.`events`', use_caching => false)", Procs.RewriteManifests(Events, useCaching: false).ToSql());
        Assert.Equal("CALL `glue`.system.`rewrite_position_delete_files`(table => '`analytics`.`events`', options => map('rewrite-all', 'true'))",
            Procs.RewritePositionDeleteFiles(Events, new SparkOptions { ["rewrite-all"] = "true" }).ToSql());
    }

    [Fact]
    public void Snapshot_procedures()
    {
        Assert.Equal("CALL `glue`.system.`cherrypick_snapshot`(table => '`analytics`.`events`', snapshot_id => 42L)", Procs.CherrypickSnapshot(Events, 42).ToSql());
        Assert.Equal("CALL `glue`.system.`rollback_to_snapshot`(table => '`analytics`.`events`', snapshot_id => 42L)", Procs.RollbackToSnapshot(Events, 42).ToSql());
        Assert.Equal("CALL `glue`.system.`rollback_to_timestamp`(table => '`analytics`.`events`', timestamp => TIMESTAMP '2026-01-02 03:04:05.006000Z')",
            Procs.RollbackToTimestamp(Events, new DateTimeOffset(2026, 1, 2, 3, 4, 5, 6, TimeSpan.Zero)).ToSql());
        Assert.Equal("CALL `glue`.system.`set_current_snapshot`(table => '`analytics`.`events`', ref => 'audit')", Procs.SetCurrentSnapshot(Events, @ref: "audit").ToSql());
        Assert.Throws<ArgumentException>(() => Procs.SetCurrentSnapshot(Events));
        Assert.Equal("CALL `glue`.system.`fast_forward`(table => '`analytics`.`events`', branch => 'main', to => 'audit')", Procs.FastForward(Events, "main", "audit").ToSql());
        Assert.Equal("CALL `glue`.system.`publish_changes`(table => '`analytics`.`events`', wap_id => 'job-7')", Procs.PublishChanges(Events, "job-7").ToSql());
        Assert.Equal("CALL `glue`.system.`create_changelog_view`(table => '`analytics`.`events`', changelog_view => 'events_changes', " +
            "options => map('end-snapshot-id', '9', 'start-snapshot-id', '3'), compute_updates => true, identifier_columns => array('id'))",
            Procs.CreateChangelogView(Events, new ChangelogViewOptions
            {
                ViewName = "events_changes", StartSnapshotId = 3, EndSnapshotId = 9, ComputeUpdates = true, IdentifierColumns = ["id"],
            }).ToSql());
        Assert.Equal("CALL `glue`.system.`ancestors_of`(table => '`analytics`.`events`')", Procs.AncestorsOf(Events).ToSql());
        Assert.Equal("CALL `glue`.system.`register_table`(table => '`analytics`.`events`', metadata_file => 's3://b/m/v1.metadata.json')",
            Procs.RegisterTable(Events, "s3://b/m/v1.metadata.json").ToSql());
        Assert.Equal("CALL `glue`.system.`add_files`(table => '`analytics`.`events`', source_table => '`parquet`.`s3://b/in/`', check_duplicate_files => true)",
            Procs.AddFiles(Events, "`parquet`.`s3://b/in/`", checkDuplicateFiles: true).ToSql());
    }

    [Fact]
    public void Procedures_refuse_a_table_of_another_catalog()
    {
        Assert.Throws<ArgumentException>(() => Procs.RewriteManifests(new SparkTable("other", "db", "t")));
    }

    [Fact]
    public void Catalog_config_renders_spark_submit_and_glue_conf_arguments()
    {
        var conf = SparkCatalogConfig.Glue("glue", "s3://lake/warehouse/");
        Assert.Equal(
            "--conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog " +
            "--conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue.warehouse=s3://lake/warehouse/ " +
            "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            conf.ToSparkSubmitParameters());
        Assert.StartsWith("spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue.catalog-impl=", conf.ToGlueConfArgument());
        Assert.Throws<ArgumentException>(() => conf.Set("spark.app.name", "has space").ToSparkSubmitParameters());
    }

    [Theory]
    [InlineData("table) ; DROP TABLE t; --")]
    [InlineData("a b")]
    [InlineData("")]
    public void Procedure_argument_names_must_be_plain_identifiers(string name) =>
        Assert.Throws<ArgumentException>(() => new SparkProcedureCall("glue", "expire_snapshots", [new(name, 1)]));

    [Fact]
    public async Task Stager_writes_a_content_addressed_driver_and_statements_as_json()
    {
        var dir = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var stager = new SparkScriptStager(new LocalFileIO(), dir);
            var driver = await stager.StageDriverAsync();
            Assert.Equal(driver, await stager.StageDriverAsync());
            Assert.Matches(@"iceberg_sql_driver-[0-9a-f]{12}\.py$", driver);
            Assert.Contains("--statements", await File.ReadAllTextAsync(driver));
            // A file already at the driver's name proves nothing about its content: staging replaces it.
            var genuine = await File.ReadAllTextAsync(driver);
            await File.WriteAllTextAsync(driver, "import os; os.system('planted')");
            await stager.StageDriverAsync();
            Assert.Equal(genuine, await File.ReadAllTextAsync(driver));

            var script = new SparkSqlScript().Add(Procs.RewriteManifests(Events)).Add("SELECT 'a;b';");
            var location = await stager.StageStatementsAsync(script);
            Assert.Equal(["CALL `glue`.system.`rewrite_manifests`(table => '`analytics`.`events`')", "SELECT 'a;b'"],
                JsonSerializer.Deserialize<string[]>(await File.ReadAllTextAsync(location))!);
            await Assert.ThrowsAsync<ArgumentException>(() => stager.StageStatementsAsync(new SparkSqlScript()));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }
}
