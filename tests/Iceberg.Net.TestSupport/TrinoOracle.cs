using System.Net.Http.Json;
using System.Text;
using System.Text.Json.Nodes;

namespace Iceberg.Net.TestSupport;

/// <summary>
/// Trino (emulation/up.sh --trino) as a third engine: its Iceberg connector reads and writes through the same REST
/// fixture and moto S3 as the suite. Statements go over Trino's HTTP protocol (POST /v1/statement, follow nextUri).
/// </summary>
public static class TrinoOracle
{
    public static Uri Uri { get; } = new(Environment.GetEnvironmentVariable("ICEBERG_TRINO_URI") ?? "http://localhost:8080");

    /// <summary>The Trino catalog configured over the REST fixture (emulation/trino/catalog/iceberg.properties).</summary>
    public const string Catalog = "iceberg";

    private static readonly Lazy<string?> Skip = new(() =>
        TestEnvironment.Current.RestSkipReason
        ?? (TestEnvironment.Current.Target != Target.Emulation ? "Trino oracle runs in emulation only" : null)
        ?? (TestEnvironment.Reachable(Uri) ? null : $"Trino not reachable at {Uri} (run emulation/up.sh --trino)"));

    public static string? SkipReason => Skip.Value;

    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromMinutes(5) };

    public sealed record Result(IReadOnlyList<string> Columns, IReadOnlyList<JsonArray> Rows)
    {
        public IEnumerable<JsonNode?> Column(string name) => Rows.Select(r => r[Columns.ToList().IndexOf(name)]);
    }

    public static string Table(string ns, string table) => $"{Catalog}.\"{ns}\".\"{table}\"";

    /// <summary>Run one statement to completion. Trino errors become <see cref="InvalidOperationException"/> with Trino's message.</summary>
    public static async Task<Result> QueryAsync(string sql)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(Uri, "/v1/statement")) { Content = new StringContent(sql, Encoding.UTF8) };
        request.Headers.Add("X-Trino-User", "iceberg-dotnet");
        var response = await Send(request);
        var columns = new List<string>();
        var rows = new List<JsonArray>();
        while (true)
        {
            if (response["error"] is { } error)
                throw new InvalidOperationException($"Trino: {error["message"]?.GetValue<string>()} ({error["errorName"]?.GetValue<string>()})");
            if (columns.Count == 0 && response["columns"] is JsonArray cols) columns.AddRange(cols.Select(c => c!["name"]!.GetValue<string>()));
            if (response["data"] is JsonArray data) rows.AddRange(data.Select(r => r!.AsArray()));
            if (response["nextUri"]?.GetValue<string>() is not { } next) return new Result(columns, rows);
            using var poll = new HttpRequestMessage(HttpMethod.Get, next);
            poll.Headers.Add("X-Trino-User", "iceberg-dotnet");
            response = await Send(poll);
        }
    }

    private static async Task<JsonNode> Send(HttpRequestMessage request)
    {
        using var response = await Http.SendAsync(request);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<JsonNode>())!;
    }
}
