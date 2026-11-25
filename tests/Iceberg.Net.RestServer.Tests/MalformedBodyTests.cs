using System.Net;
using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Iceberg.Net.Types;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// Every request body the server parses, damaged one node at a time: each value is replaced by every other JSON shape
/// (null, small and huge numbers, a string, a boolean, an array, an object) and each property is removed. A malformed
/// request must get a 4xx, never a 500: Iceberg's REST clients read a 500 on a commit as "commit state unknown", so a
/// parsing crash would leave a client unsure whether its own commit landed. This is the REST JSON fuzz target, run
/// exhaustively over the shapes rather than at random.
/// </summary>
public sealed class MalformedBodyTests
{
    private static readonly JsonNode?[] Replacements =
    [
        null, JsonValue.Create(7), JsonValue.Create(-1), JsonValue.Create(1e300), JsonValue.Create(long.MaxValue),
        JsonValue.Create("x"), JsonValue.Create(true), new JsonArray(), new JsonObject(),
    ];

    private static JsonObject Schema() => TypeJson.SchemaToJson(CatalogContractTests.Schema);

    /// <summary>(method, path, body) seeds: requests against namespace <c>db</c> and tables <c>db.t</c> and <c>db.t2</c>.</summary>
    private static IEnumerable<(HttpMethod Method, string Path, JsonObject Body)> Seeds()
    {
        yield return (HttpMethod.Post, "/v1/namespaces", new JsonObject
        {
            ["namespace"] = new JsonArray("m"), ["properties"] = new JsonObject { ["a"] = "1" },
        });
        yield return (HttpMethod.Post, "/v1/namespaces/db/properties", new JsonObject
        {
            ["updates"] = new JsonObject { ["a"] = "1" }, ["removals"] = new JsonArray("b"),
        });
        yield return (HttpMethod.Post, "/v1/namespaces/db/tables", new JsonObject
        {
            ["name"] = "c",
            ["schema"] = Schema(),
            ["partition-spec"] = new JsonObject
            {
                ["spec-id"] = 0,
                ["fields"] = new JsonArray(new JsonObject { ["source-id"] = 1, ["field-id"] = 1000, ["name"] = "b", ["transform"] = "bucket[4]" }),
            },
            ["write-order"] = new JsonObject
            {
                ["order-id"] = 1,
                ["fields"] = new JsonArray(new JsonObject { ["source-id"] = 1, ["transform"] = "identity", ["direction"] = "asc", ["null-order"] = "nulls-first" }),
            },
            ["stage-create"] = true,
            ["properties"] = new JsonObject { ["owner"] = "tests" },
        });
        yield return (HttpMethod.Post, "/v1/namespaces/db/register", new JsonObject
        {
            ["name"] = "r", ["metadata-location"] = "file:///nowhere/v1.metadata.json",
        });
        yield return (HttpMethod.Post, "/v1/tables/rename", new JsonObject
        {
            ["source"] = new JsonObject { ["namespace"] = new JsonArray("db"), ["name"] = "missing" },
            ["destination"] = new JsonObject { ["namespace"] = new JsonArray("db"), ["name"] = "moved" },
        });
        yield return (HttpMethod.Post, "/v1/namespaces/db/tables/t", Commit());
        yield return (HttpMethod.Post, "/v1/namespaces/db/tables/t/plan", new JsonObject
        {
            ["select"] = new JsonArray("id"),
            ["filter"] = new JsonObject
            {
                ["type"] = "and",
                ["left"] = new JsonObject { ["type"] = "gt", ["term"] = "id", ["value"] = 3 },
                ["right"] = new JsonObject { ["type"] = "in", ["term"] = "data", ["values"] = new JsonArray("a", "b") },
            },
            ["case-sensitive"] = true,
        });
        yield return (HttpMethod.Post, "/v1/transactions/commit", new JsonObject
        {
            ["table-changes"] = new JsonArray(Commit(table: "t2")),
        });
    }

    private static JsonObject Commit(string? table = null)
    {
        var commit = new JsonObject
        {
            ["requirements"] = new JsonArray(
                new JsonObject { ["type"] = "assert-current-schema-id", ["current-schema-id"] = 0 },
                new JsonObject { ["type"] = "assert-ref-snapshot-id", ["ref"] = "main", ["snapshot-id"] = null }),
            ["updates"] = new JsonArray(
                new JsonObject { ["action"] = "set-properties", ["updates"] = new JsonObject { ["k"] = "v" } },
                new JsonObject { ["action"] = "remove-properties", ["removals"] = new JsonArray("gone") },
                new JsonObject { ["action"] = "add-schema", ["schema"] = Schema(), ["last-column-id"] = 2 },
                new JsonObject { ["action"] = "set-current-schema", ["schema-id"] = -1 },
                new JsonObject { ["action"] = "remove-snapshot-ref", ["ref-name"] = "b" }),
        };
        if (table is not null) commit["identifier"] = new JsonObject { ["namespace"] = new JsonArray("db"), ["name"] = table };
        return commit;
    }

    /// <summary>Every (path, node) below the root; a path is the property names and array indexes leading there.</summary>
    private static IEnumerable<object[]> Paths(JsonNode node, List<object> path)
    {
        switch (node)
        {
            case JsonObject o:
                foreach (var (key, child) in o.ToList())
                {
                    var p = new List<object>(path) { key };
                    yield return p.ToArray();
                    if (child is not null) foreach (var deeper in Paths(child, p)) yield return deeper;
                }
                break;
            case JsonArray a:
                for (var i = 0; i < a.Count; i++)
                {
                    var p = new List<object>(path) { i };
                    yield return p.ToArray();
                    if (a[i] is { } child) foreach (var deeper in Paths(child, p)) yield return deeper;
                }
                break;
        }
    }

    /// <summary>A copy of <paramref name="root"/> with the node at <paramref name="path"/> replaced, or removed when <paramref name="remove"/>.</summary>
    private static JsonObject Mutate(JsonObject root, object[] path, JsonNode? replacement, bool remove)
    {
        var copy = (JsonObject)root.DeepClone();
        JsonNode parent = copy;
        foreach (var step in path[..^1]) parent = step is string k ? parent[k]! : parent[(int)step]!;
        switch (path[^1], parent)
        {
            case (string key, JsonObject o):
                if (remove) o.Remove(key);
                else o[key] = replacement?.DeepClone();
                break;
            case (int index, JsonArray a):
                if (remove) a.RemoveAt(index);
                else a[index] = replacement?.DeepClone();
                break;
        }
        return copy;
    }

    [Fact]
    public async Task No_damaged_request_body_gets_a_server_error()
    {
        await using var host = ServerHost.Start("sqlite");
        using var http = host.Http();
        Assert.Equal(HttpStatusCode.OK, (await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces", new JsonObject { ["namespace"] = new JsonArray("db") })).Status);
        foreach (var table in new[] { "t", "t2" }) // t2 is the transaction's, so the commit seed's schema change can't fail it
            Assert.Equal(HttpStatusCode.OK, (await ProtocolTests.SendAsync(http, HttpMethod.Post, "/v1/namespaces/db/tables", new JsonObject
            {
                ["name"] = table, ["schema"] = Schema(),
            })).Status);

        var failures = new List<string>();
        var sent = 0;
        foreach (var (method, path, seed) in Seeds())
        {
            // Commits and plans must be valid as seeded, so their damaged copies reach the update, requirement and
            // expression parsers instead of stopping at the first check.
            var (seedStatus, seedBody) = await ProtocolTests.SendAsync(http, method, path, (JsonObject)seed.DeepClone());
            if (path.EndsWith("/t", StringComparison.Ordinal) || path.EndsWith("/plan", StringComparison.Ordinal) || path.EndsWith("/commit", StringComparison.Ordinal))
                Assert.True(seedStatus is HttpStatusCode.OK or HttpStatusCode.NoContent, $"seed {method} {path}: {(int)seedStatus} {seedBody?["error"]?["message"]}");
            foreach (var nodePath in Paths(seed, []))
            {
                var where = string.Join('.', nodePath);
                foreach (var body in Replacements.Select(r => Mutate(seed, nodePath, r, remove: false)).Append(Mutate(seed, nodePath, null, remove: true)))
                {
                    sent++;
                    var (status, response) = await ProtocolTests.SendAsync(http, method, path, body);
                    if ((int)status >= 500)
                        failures.Add($"{method} {path} with {where} changed: {(int)status} {response?["error"]?["message"]} -- {body.ToJsonString()}");
                }
            }
        }

        Assert.True(sent > 500, $"only {sent} requests were sent");
        Assert.True(failures.Count == 0, $"{failures.Count} of {sent} damaged requests got a server error:\n" + string.Join("\n", failures.Take(25)));
    }
}
