using System.Net.Http.Json;
using System.Text.Json.Nodes;
using Amazon.Athena;
using Amazon.EMRServerless;
using Amazon.Glue;
using Amazon.Runtime;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// Real AWS SDK clients for the managed services the Spark and Athena runners drive (EMR Serverless, Glue jobs,
/// Athena): pointed at moto in emulation, at the region's endpoints against AWS.
/// </summary>
internal static class AwsServiceClients
{
    private static T Configure<T>(T config) where T : ClientConfig
    {
        var env = TestEnvironment.Current;
        config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(env.Region);
        if (env.GlueEndpoint is not null)
        {
            config.ServiceURL = env.GlueEndpoint; // moto serves every service on one endpoint
            config.AuthenticationRegion = env.Region;
        }
        return config;
    }

    public static AmazonEMRServerlessClient EmrServerless() => new(Configure(new AmazonEMRServerlessConfig()));
    public static AmazonGlueClient Glue() => new(Configure(new AmazonGlueConfig()));
    public static AmazonAthenaClient Athena() => new(Configure(new AmazonAthenaConfig()));
}

/// <summary>
/// moto's control API (emulation only): state transitions of job runs and query executions, queued Athena results,
/// and the shim list emulation/moto_server.py serves. moto's state is process-wide, so every class that changes it
/// runs in the <see cref="MotoServiceStateCollection"/>.
/// </summary>
internal static class MotoApi
{
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(30) };

    // moto (werkzeug) reads no chunked bodies, so send the JSON with a Content-Length.
    private static Task<HttpResponseMessage> PostAsync(string path, JsonNode body) =>
        Http.PostAsync(Endpoint(path), new StringContent(body.ToJsonString(), System.Text.Encoding.UTF8, "application/json"));

    private static Uri Endpoint(string path) => new(new Uri(TestEnvironment.Current.S3Endpoint ?? throw new InvalidOperationException("moto API needs emulation")), path);

    /// <summary>
    /// Make <paramref name="model"/> (e.g. <c>glue::job_run</c>, <c>athena::execution</c>) advance one state every
    /// <paramref name="describes"/> describe calls, instead of jumping to its final state at once.
    /// </summary>
    public static async Task SetManualTransitionAsync(string model, int describes)
    {
        using var response = await PostAsync("/moto-api/state-manager/set-transition",
            new JsonObject { ["model_name"] = model, ["transition"] = new JsonObject { ["progression"] = "manual", ["times"] = describes } });
        response.EnsureSuccessStatusCode();
    }

    public static async Task UnsetTransitionAsync(string model)
    {
        using var response = await PostAsync("/moto-api/state-manager/unset-transition", new JsonObject { ["model_name"] = model });
        response.EnsureSuccessStatusCode();
    }

    /// <summary>
    /// Queue the rows the next Athena query returns (moto executes nothing: each StartQueryExecution takes the next
    /// queued result). A null cell is an Athena NULL (a datum without VarCharValue).
    /// </summary>
    public static async Task QueueAthenaResultAsync(IReadOnlyList<string> columns, IEnumerable<IReadOnlyList<string?>> rows, bool withHeader = true)
    {
        static JsonObject Row(IEnumerable<string?> cells) => new()
        {
            ["Data"] = new JsonArray(cells.Select(c => (JsonNode)(c is null ? new JsonObject() : new JsonObject { ["VarCharValue"] = c })).ToArray()),
        };
        var all = new JsonArray();
        if (withHeader) all.Add(Row(columns)); // Athena's SELECT results start with the column names
        foreach (var r in rows) all.Add(Row(r));
        var body = new JsonObject
        {
            ["region"] = TestEnvironment.Current.Region,
            ["results"] = new JsonArray(new JsonObject
            {
                ["rows"] = all,
                ["column_info"] = new JsonArray(columns.Select(c => (JsonNode)new JsonObject { ["Name"] = c, ["Label"] = c, ["Type"] = "varchar" }).ToArray()),
            }),
        };
        using var response = await PostAsync("/moto-api/static/athena/query-results", body);
        response.EnsureSuccessStatusCode();
    }

    /// <summary>The fidelity shims the running moto applies, or none for a moto started without emulation/moto_server.py's listing.</summary>
    public static async Task<IReadOnlyList<string>> ShimsAsync()
    {
        using var response = await Http.GetAsync(Endpoint("/__shims"));
        return response.IsSuccessStatusCode ? (await response.Content.ReadFromJsonAsync<List<string>>())! : [];
    }
}

/// <summary>Serialises the classes that change moto's process-wide state transitions or Athena result queue.</summary>
[CollectionDefinition(Name)]
public sealed class MotoServiceStateCollection
{
    public const string Name = "moto service state";
}
