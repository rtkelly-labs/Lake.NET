using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Json;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// A refused location is a 400 that says only that it clashes: the table, view or namespace it clashes with may be one
/// the caller cannot read, so neither its name nor its location is in the response (they are in the server log).
/// </summary>
public sealed class ErrorDisclosureTests
{
    private static readonly Dictionary<string, string> Settings = new()
    {
        ["Auth:Clients:root"] = "root-secret",
        ["Auth:Clients:owner"] = "owner-secret",
        ["Auth:SigningKey"] = "an-error-disclosure-test-signing-key-32-bytes",
        ["Authorization:Grants:root:0"] = "admin:*",
        ["Authorization:Grants:owner:0"] = "admin:sales",
    };

    private static async Task<(ServerHost Host, HttpClient Http, string Token)> StartAsync()
    {
        var host = ServerHost.Start(settings: Settings);
        using (var root = await host.ConnectAsync(credential: "root:root-secret"))
        {
            await root.CreateNamespaceAsync(Namespace.Of("sales"));
            await root.CreateNamespaceAsync(Namespace.Of("vault"));
            // Created in sales, then moved to vault: its files stay in sales' directory, under a name owner cannot see.
            await root.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("sales", "hidden"), ScanPlanningTests.Schema));
            await root.RenameTableAsync(TableIdentifier.Of("sales", "hidden"), TableIdentifier.Of("vault", "payroll"));
        }
        var http = host.Http();
        using var resp = await http.PostAsync("/v1/oauth/tokens", new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials", ["client_id"] = "owner", ["client_secret"] = "owner-secret",
        }));
        return (host, http, JsonNode.Parse(await resp.Content.ReadAsStringAsync())!["access_token"]!.GetValue<string>());
    }

    private static async Task<string> RefusedAsync(HttpClient http, string token, string path, JsonObject body)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, path) { Content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json") };
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var resp = await http.SendAsync(req);
        var text = await resp.Content.ReadAsStringAsync();
        Assert.True(resp.StatusCode == HttpStatusCode.BadRequest, text);
        return text;
    }

    private static void AssertNamesNothingOutsideSales(string error)
    {
        Assert.DoesNotContain("vault", error, StringComparison.Ordinal);
        Assert.DoesNotContain("payroll", error, StringComparison.Ordinal);
        Assert.Contains("overlaps the location of another table, view or namespace", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_table_overlapping_a_table_the_caller_cannot_read_is_refused_without_naming_it()
    {
        var (host, http, token) = await StartAsync();
        await using var _ = host;
        using var __ = http;
        var error = await RefusedAsync(http, token, "/v1/namespaces/sales/tables", new JsonObject
        {
            ["name"] = "mine",
            ["schema"] = TypeJson.SchemaToJson(ScanPlanningTests.Schema),
            ["location"] = $"{host.Warehouse}/sales/hidden/mine",
        });
        AssertNamesNothingOutsideSales(error);
    }

    [Fact]
    public async Task A_namespace_overlapping_entries_the_caller_cannot_read_is_refused_without_naming_them()
    {
        var (host, http, token) = await StartAsync();
        await using var _ = host;
        using var __ = http;
        // Over the moved table's directory.
        AssertNamesNothingOutsideSales(await RefusedAsync(http, token, "/v1/namespaces", new JsonObject
        {
            ["namespace"] = new JsonArray("sales", "over_table"),
            ["properties"] = new JsonObject { ["location"] = $"{host.Warehouse}/sales/hidden" },
        }));
        // Inside another namespace's root ({warehouse}/vault by default). The caller wrote that path, but not the
        // namespace's name, which the old message gave away.
        var error = await RefusedAsync(http, token, "/v1/namespaces", new JsonObject
        {
            ["namespace"] = new JsonArray("sales", "over_namespace"),
            ["properties"] = new JsonObject { ["location"] = $"{host.Warehouse}/vault/x" },
        });
        Assert.DoesNotContain("namespace vault", error, StringComparison.Ordinal);
        Assert.Contains("overlaps the location of another table, view or namespace", error, StringComparison.Ordinal);
    }
}
