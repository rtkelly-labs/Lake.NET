using System.Diagnostics;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Spark;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.Ecosystem.Tests;

/// <summary>
/// The generated procedure calls, run by Spark 4 + Iceberg 1.12 (the Spark oracle image and jar) through the same
/// driver the EMR Serverless and Glue runners stage: one JVM runs a script that builds a table with a hyphenated name,
/// mutates it, and calls every maintenance and snapshot procedure on it. Then this library reads what Spark left.
/// </summary>
public sealed class SparkProcedureOracleTests
{
    private const string Catalog = "fx";

    [SkippableFact]
    public async Task Spark_runs_the_generated_procedure_calls()
    {
        Skip.If(SparkOracle.SkipReason is not null, SparkOracle.SkipReason);
        var root = Path.Combine(SparkOracle.SharedRoot, "procedures-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(root);
        try
        {
            var warehouse = Path.Combine(root, "warehouse");
            var t = new SparkTable(Catalog, "db", "t-1");
            var p = new IcebergProcedures(Catalog);
            var future = DateTimeOffset.UtcNow.AddDays(1);
            var script = new SparkSqlScript()
                .Add($"CREATE TABLE {t.QualifiedName} (id BIGINT, name STRING) USING iceberg TBLPROPERTIES " +
                     "('format-version'='2', 'write.delete.mode'='merge-on-read', 'write.wap.enabled'='true')")
                .Add($"INSERT INTO {t.QualifiedName} VALUES (1, 'a'), (2, 'b')")
                .Add($"ALTER TABLE {t.QualifiedName} CREATE TAG `first`")
                .Add($"INSERT INTO {t.QualifiedName} VALUES (3, 'a;b'), (4, 'it''s')")
                .Add($"INSERT INTO {t.QualifiedName} VALUES (5, 'e')")
                // Before the DELETE: Java's changelog scan refuses snapshots with delete files.
                .Add(p.CreateChangelogView(t, new ChangelogViewOptions { ViewName = "t_changes", IdentifierColumns = ["id"] }))
                .Add("SELECT count(*) AS n FROM t_changes")
                .Add($"DELETE FROM {t.QualifiedName} WHERE id = 2")
                .Add(p.RewritePositionDeleteFiles(t, new SparkOptions { ["rewrite-all"] = "true" }))
                .Add(p.RewriteDataFiles(t, new RewriteDataFilesOptions { MinInputFiles = 2, Where = "id >= 0 AND name <> 'x;y\\'z'" }))
                .Add(p.RewriteDataFiles(t, new RewriteDataFilesOptions { SortOrder = "id DESC NULLS LAST", RewriteAll = true }))
                .Add(p.RewriteManifests(t, useCaching: false))
                .Add(p.AncestorsOf(t))
                .Add("SET spark.wap.id = job-7")
                .Add($"INSERT INTO {t.QualifiedName} VALUES (6, 'staged')")
                .Add("RESET spark.wap.id")
                .Add($"SELECT count(*) AS n FROM {t.QualifiedName}")
                .Add(p.PublishChanges(t, "job-7"))
                .Add($"ALTER TABLE {t.QualifiedName} CREATE BRANCH `audit`")
                .Add($"INSERT INTO {SparkSql.Identifier([Catalog, "db", "t-1", "branch_audit"])} VALUES (7, 'audited')")
                .Add(p.FastForward(t, "main", "audit"))
                .Add($"SELECT count(*) AS n FROM {t.QualifiedName}")
                .Add(p.ExpireSnapshots(t, new ExpireSnapshotsOptions { OlderThan = future, RetainLast = 1 }))
                .Add(p.RemoveOrphanFiles(t, new RemoveOrphanFilesOptions { OlderThan = DateTimeOffset.UtcNow.AddDays(-2), DryRun = true })) // Iceberg refuses under 24 hours
                .Add(p.SetCurrentSnapshot(t, @ref: "first"))
                .Add($"SELECT id, name FROM {t.QualifiedName} ORDER BY id");

            var results = await RunAsync(script, SparkCatalogConfig.Hadoop(Catalog, "file://" + warehouse), root);

            JsonNode Result(string startsWith, int skip = 0) =>
                results.Where(r => r!["statement"]!.GetValue<string>().StartsWith(startsWith, StringComparison.Ordinal)).Skip(skip).First()!;
            var compaction = Result("CALL `fx`.system.`rewrite_data_files`")["rows"]![0]!;
            Assert.True(compaction["rewritten_data_files_count"]!.GetValue<int>() >= 2, compaction.ToJsonString());
            Assert.NotNull(Result("CALL `fx`.system.`rewrite_position_delete_files`")["rows"]![0]!["rewritten_delete_files_count"]);
            Assert.Equal("t_changes", Result("CALL `fx`.system.`create_changelog_view`")["rows"]![0]!["changelog_view"]!.GetValue<string>());
            Assert.Equal(5L, Result("SELECT count(*) AS n FROM t_changes")["rows"]![0]!["n"]!.GetValue<long>()); // five inserted rows
            Assert.True(Result("CALL `fx`.system.`ancestors_of`")["rows"]!.AsArray().Count >= 5);
            // The WAP insert is staged, not visible, until publish_changes; the audit branch's row arrives by fast-forward.
            Assert.Equal(4L, Result("SELECT count(*) AS n FROM `fx`")["rows"]![0]!["n"]!.GetValue<long>());
            Assert.Equal(6L, Result("SELECT count(*) AS n FROM `fx`", 1)["rows"]![0]!["n"]!.GetValue<long>());
            Assert.True(Result("CALL `fx`.system.`expire_snapshots`")["rows"]![0]!["deleted_manifest_lists_count"]!.GetValue<long>() > 0);
            var rows = Result("SELECT id, name FROM")["rows"]!.AsArray().Select(r => (r!["id"]!.GetValue<long>(), r["name"]!.GetValue<string>())).ToList();
            Assert.Equal([(1L, "a"), (2L, "b")], rows); // set_current_snapshot to the tag

            // This library reads the table Spark compacted, expired and rolled to the tag.
            var metadata = Directory.GetFiles(Path.Combine(warehouse, "db", "t-1", "metadata"), "v*.metadata.json")
                .OrderBy(f => int.Parse(Path.GetFileName(f)[1..^".metadata.json".Length], System.Globalization.CultureInfo.InvariantCulture)).Last();
            using var catalog = new InMemoryCatalog(Path.Combine(root, "dotnet"));
            await catalog.CreateNamespaceAsync(Namespace.Of("db"));
            var table = await catalog.RegisterTableAsync(TableIdentifier.Of("db", "t"), metadata);
            var ours = (await TableScan.ReadAllAsync(table)).Select(r => ((long)r[0]!, (string)r[1]!)).OrderBy(r => r.Item1).ToList();
            Assert.Equal(rows, ours);
        }
        finally
        {
            try { Directory.Delete(root, true); } catch (IOException) { } catch (UnauthorizedAccessException) { } // files the root container wrote
        }
    }

    /// <summary>Run <paramref name="script"/> with the staged driver in the Spark oracle image; the driver's per-statement results.</summary>
    internal static async Task<JsonArray> RunAsync(SparkSqlScript script, SparkCatalogConfig conf, string stagingDir)
    {
        var stager = new SparkScriptStager(new LocalFileIO(), stagingDir);
        var driver = await stager.StageDriverAsync();
        var statements = await stager.StageStatementsAsync(script);
        var jar = Environment.GetEnvironmentVariable("ICEBERG_SPARK_JAR") ?? Path.Combine(TestEnvironment.RepoDir(), "emulation", ".cache", SparkOracle.JarName);
        string[] args =
        [
            "run", "--rm", "--user", "root", "--network", "none", "--hostname", "localhost", "-e", "SPARK_LOCAL_IP=127.0.0.1",
            "-v", $"{Path.GetDirectoryName(jar)}:/jars:ro", "-v", $"{SparkOracle.SharedRoot}:{SparkOracle.SharedRoot}",
            SparkOracle.Image, "/opt/spark/bin/spark-submit", "--master", "local[2]", "--jars", $"/jars/{Path.GetFileName(jar)}",
            "--conf", "spark.ui.enabled=false", "--conf", "spark.sql.shuffle.partitions=2", "--conf", "spark.log.level=ERROR", .. conf.ToArguments(),
            driver, "--statements", statements,
        ];
        var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(10));
        var output = await stdout;
        var marker = output.LastIndexOf("@@RESULTS@@", StringComparison.Ordinal);
        if (process.ExitCode != 0 || marker < 0)
            throw new InvalidOperationException($"Spark driver failed ({process.ExitCode}):\n{string.Join('\n', (await stderr).Split('\n').TakeLast(40))}\n{string.Join('\n', output.Split('\n').TakeLast(20))}");
        return JsonNode.Parse(output[(marker + "@@RESULTS@@".Length)..])!.AsArray();
    }
}
