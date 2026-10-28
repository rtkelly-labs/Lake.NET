using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Microsoft.Data.Sqlite;

namespace Iceberg.Net.RestServer.Tests;

public sealed class RecoveryTests
{
    [Fact]
    public async Task A_live_sqlite_backup_restores_table_data_and_idempotency_responses()
    {
        var root = Path.Combine(Path.GetTempPath(), "iceberg-rest-recovery", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var backup = Path.Combine(root, "backup.db");
        var id = TableIdentifier.Of("db", "events");
        try
        {
            await using (var host = ServerHost.Start("sqlite", root: root))
            {
                using var catalog = await host.ConnectAsync();
                await catalog.CreateNamespaceAsync(Namespace.Of("db"));
                var table = await catalog.CreateTableAsync(new CreateTableRequest(id, ScanPlanningTests.Schema));
                await TableMutations.InsertAsync(table, ScanPlanningTests.Rows(0, 3));
                using var http = host.Http();
                using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/namespaces");
                request.Headers.Add("Idempotency-Key", "restore-key");
                request.Content = ProtocolTests.JsonBody(new JsonObject { ["namespace"] = new JsonArray("retained") });
                using var response = await http.SendAsync(request);
                response.EnsureSuccessStatusCode();

                // SQLite's online-backup API takes a consistent database snapshot, including WAL-backed state.
                using var source = new SqliteConnection($"Data Source={host.Service<RestServerOptions>().SqlitePath}");
                using var target = new SqliteConnection($"Data Source={backup}");
                await source.OpenAsync();
                await target.OpenAsync();
                source.BackupDatabase(target);
            }
            await using (var restored = ServerHost.Start("sqlite", new Dictionary<string, string> { ["SqlitePath"] = backup }, root: root))
            {
                using var catalog = await restored.ConnectAsync();
                var table = await catalog.LoadTableAsync(id);
                Assert.Equal(3, (await TableScan.ReadAllAsync(table)).Count);
                Assert.Single(table.Metadata.Snapshots);
                using var http = restored.Http();
                using var request = new HttpRequestMessage(HttpMethod.Post, "/v1/namespaces");
                request.Headers.Add("Idempotency-Key", "restore-key");
                request.Content = ProtocolTests.JsonBody(new JsonObject { ["namespace"] = new JsonArray("retained") });
                using var response = await http.SendAsync(request);
                response.EnsureSuccessStatusCode();
                Assert.Equal("true", response.Headers.GetValues("Idempotent-Replayed").Single());
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Directory.Delete(root, recursive: true);
        }
    }
}
