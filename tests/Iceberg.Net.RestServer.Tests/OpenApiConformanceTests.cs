using System.Text.Json.Nodes;
using Iceberg.Net.TestSupport;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// The server's generated OpenAPI document (<c>/openapi/v1.json</c>) against the reference REST spec at the pinned ref
/// (docs/spec/open-api/rest-catalog-open-api.json, refreshed by tools/openapi_reference.py). Every operation, route,
/// success status, documented functional error and query/header parameter must match, except the reviewed differences
/// in docs/spec/openapi-deviations.json, each with its reason. A difference that has gone away fails too, so that list
/// cannot outlive what it excuses.
/// </summary>
public sealed class OpenApiConformanceTests
{
    /// <summary>Errors a client acts on differently; 419, 503 and 5XX are infrastructure and not compared.</summary>
    private static readonly HashSet<string> FunctionalErrors = ["400", "401", "403", "404", "406", "409", "422"];

    private sealed record Op(string Id, string Route, string Success, HashSet<string> Responses, HashSet<string> Parameters);

    // Not TestEnvironment.RepoDir(): this project copies interop/ into its output, which that search would stop at.
    private static JsonNode Load(string relative)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "docs", "spec"))) dir = dir.Parent;
        return JsonNode.Parse(File.ReadAllText(Path.Combine(dir?.FullName ?? throw new DirectoryNotFoundException("docs/spec not found above test output"), relative)))!;
    }

    private static Dictionary<string, Op> Operations(JsonNode doc, bool reference)
    {
        JsonNode Resolve(JsonNode p) => p["$ref"]?.GetValue<string>() is { } r ? doc["components"]!["parameters"]![r.Split('/')[^1]]! : p;
        var result = new Dictionary<string, Op>(StringComparer.Ordinal);
        foreach (var (path, item) in doc["paths"]!.AsObject())
        {
            if (path.StartsWith("/openapi", StringComparison.Ordinal)) continue;
            var route = path.Replace("/v1/{prefix}", "/v1", StringComparison.Ordinal);
            var shared = item!["parameters"]?.AsArray().Select(p => Resolve(p!)).ToList() ?? [];
            foreach (var (method, opNode) in item.AsObject())
            {
                if (method is "parameters" or "summary" or "description" || opNode?["operationId"] is null && reference) continue;
                var parameters = shared.Concat(opNode!["parameters"]?.AsArray().Select(p => Resolve(p!)) ?? [])
                    .Where(p => p["in"]!.GetValue<string>() is "query" or "header")
                    .Select(p => $"{p["in"]!.GetValue<string>()} {p["name"]!.GetValue<string>()}").ToHashSet(StringComparer.Ordinal);
                var codes = opNode["responses"]?.AsObject().Select(kv => kv.Key).ToHashSet(StringComparer.Ordinal) ?? [];
                var success = codes.Where(c => c.StartsWith('2')).Order().FirstOrDefault() ?? "";
                var id = opNode["operationId"]?.GetValue<string>() ?? $"(unnamed {method.ToUpperInvariant()} {route})";
                result[id] = new Op(id, $"{method.ToUpperInvariant()} {route}", success, codes, parameters);
            }
        }
        return result;
    }

    [Fact]
    public void The_reference_copy_is_the_spec_version_the_repository_pins() =>
        Assert.Equal(Load("docs/spec/sections.json")["ref"]!.GetValue<string>(),
            Load("docs/spec/open-api/rest-catalog-open-api.json")["x-iceberg-net-source"]!["ref"]!.GetValue<string>());

    [Spec("rest: getConfig")]
    [Fact]
    public async Task The_generated_document_matches_the_reference_spec()
    {
        // Every optional route on: scan planning (default), remote signing and credential vending (not called here).
        await using var host = ServerHost.Start(settings: new Dictionary<string, string>
        {
            ["RemoteSigning:Enabled"] = "true",
            ["FileIO:s3.access-key-id"] = "AKSERVER",
            ["FileIO:s3.secret-access-key"] = "server-secret",
            ["FileIO:client.region"] = "us-east-1",
            ["CredentialVending:RoleArn"] = "arn:aws:iam::123456789012:role/unused",
            ["CredentialVending:StsEndpoint"] = "http://sts.test",
            ["CredentialVending:Region"] = "us-east-1",
        });
        var generated = Operations(JsonNode.Parse(await host.Http().GetStringAsync("openapi/v1.json"))!, reference: false);
        var reference = Operations(Load("docs/spec/open-api/rest-catalog-open-api.json"), reference: true);
        var deviations = Load("docs/spec/openapi-deviations.json").AsObject();
        HashSet<string> Allowed(string kind) => deviations[kind]?.AsObject().Select(kv => kv.Key).ToHashSet(StringComparer.Ordinal) ?? [];
        var used = new HashSet<string>(StringComparer.Ordinal);
        var problems = new List<string>();
        void Check(string kind, string what, string message)
        {
            if (Allowed(kind).Contains(what)) used.Add($"{kind}: {what}");
            else problems.Add(message);
        }

        foreach (var (id, expected) in reference)
        {
            if (!generated.TryGetValue(id, out var actual))
            {
                Check("missing-operations", id, $"missing operation {id} ({expected.Route})");
                continue;
            }
            if (actual.Route != expected.Route) problems.Add($"{id}: route {actual.Route}, reference {expected.Route}");
            if (actual.Success != expected.Success) problems.Add($"{id}: success {actual.Success}, reference {expected.Success}");
            foreach (var code in actual.Responses.Except(expected.Responses))
                if (!(code.StartsWith('5') && expected.Responses.Contains("5XX")))
                    Check("extra-responses", $"{id} {code}", $"{id}: declares {code}, which the reference does not list");
            foreach (var code in expected.Responses.Intersect(FunctionalErrors).Except(actual.Responses))
                Check("missing-responses", $"{id} {code}", $"{id}: does not declare {code}, which the reference lists");
            foreach (var p in actual.Parameters.Except(expected.Parameters))
                Check("extra-parameters", $"{id} {p}", $"{id}: reads {p}, which the reference does not define");
            foreach (var p in expected.Parameters.Except(actual.Parameters))
                Check("missing-parameters", $"{id} {p}", $"{id}: does not read {p}, which the reference defines");
        }
        foreach (var (id, actual) in generated.Where(kv => !reference.ContainsKey(kv.Key)))
            problems.Add($"operation {id} ({actual.Route}) is not in the reference spec");

        foreach (var (kind, entries) in deviations.Where(kv => !kv.Key.StartsWith('$')))
            foreach (var (what, _) in entries!.AsObject())
                if (!used.Contains($"{kind}: {what}")) problems.Add($"stale deviation {kind}: {what} (the difference it excuses is gone)");

        Assert.True(problems.Count == 0, "OpenAPI differences from the reference:\n" + string.Join('\n', problems.Order()));
    }
}
