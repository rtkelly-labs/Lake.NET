using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Iceberg.Net.Catalog;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.RestServer;

/// <summary>The REST catalog routes (open-api/rest-catalog-open-api.yaml), mapped onto <see cref="CatalogService"/>.</summary>
public static class Endpoints
{
    private const string Ns = "/namespaces/{namespace}";
    private const string Tbl = Ns + "/tables/{table}";
    private const string Vw = Ns + "/views/{view}";

    /// <summary>What <c>GET /v1/config</c> advertises: every route below.</summary>
    public static readonly string[] Advertised =
    [
        "GET /v1/{prefix}/namespaces",
        "POST /v1/{prefix}/namespaces",
        "GET /v1/{prefix}/namespaces/{namespace}",
        "HEAD /v1/{prefix}/namespaces/{namespace}",
        "DELETE /v1/{prefix}/namespaces/{namespace}",
        "POST /v1/{prefix}/namespaces/{namespace}/properties",
        "GET /v1/{prefix}/namespaces/{namespace}/tables",
        "POST /v1/{prefix}/namespaces/{namespace}/tables",
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "POST /v1/{prefix}/namespaces/{namespace}/register",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
        "POST /v1/{prefix}/tables/rename",
        "POST /v1/{prefix}/transactions/commit",
        "GET /v1/{prefix}/namespaces/{namespace}/views",
        "POST /v1/{prefix}/namespaces/{namespace}/views",
        "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
        "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
        "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
        "POST /v1/{prefix}/views/rename",
    ];

    public static void MapIcebergRestCatalog(this WebApplication app)
    {
        var options = app.Services.GetRequiredService<RestServerOptions>();
        var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Iceberg.Net.RestServer");
        // Create the store's tables before the first request.
        app.Services.GetRequiredService<ICatalogStore>().InitializeAsync().GetAwaiter().GetResult();

        // Every failure becomes the spec's ErrorModel; a 500 is logged (on a commit it means "outcome unknown").
        app.Use(async (ctx, next) =>
        {
            try
            {
                await next();
            }
            catch (Exception e) when (!ctx.Response.HasStarted)
            {
                var (status, type) = ErrorModel.Classify(e);
                if (status >= 500) logger.LogError(e, "{Method} {Path} failed", ctx.Request.Method, ctx.Request.Path);
                await ErrorModel.WriteAsync(ctx, status, type, e.Message);
            }
        });

        app.MapPost("/v1/oauth/tokens", AuthSetup.TokenAsync).AllowAnonymous();

        var config = app.MapGroup("/v1/config");
        var v1 = app.MapGroup(options.Prefix is { Length: > 0 } p ? $"/v1/{Uri.EscapeDataString(p)}" : "/v1");
        if (options.Auth.Enabled)
        {
            config.RequireAuthorization(AuthSetup.Policy);
            v1.RequireAuthorization(AuthSetup.Policy);
        }

        config.MapGet("", (HttpRequest req) =>
        {
            var overrides = new Dictionary<string, string>(options.Overrides);
            if (options.Prefix is { Length: > 0 } prefix) overrides["prefix"] = prefix;
            return Json(new JsonObject
            {
                ["defaults"] = MetadataJson.StringMap(options.Defaults),
                ["overrides"] = MetadataJson.StringMap(overrides),
                ["endpoints"] = new JsonArray(Advertised.Select(e => (JsonNode)e).ToArray()),
            });
        });

        // ---------- namespaces ----------

        v1.MapGet("/namespaces", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var parent = req.Query["parent"].ToString() is { Length: > 0 } raw ? ParseNamespace(raw) : null;
            var all = await svc.ListNamespacesAsync(parent, ct);
            var (page, next) = Page(all, n => string.Join('\u001f', n.Levels), req);
            return Json(new JsonObject
            {
                ["namespaces"] = new JsonArray(page.Select(n => (JsonNode)Levels(n)).ToArray()),
                ["next-page-token"] = next,
            });
        });

        v1.MapPost("/namespaces", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (ns, props) = Parse(() => (
                new Namespace(body["namespace"]!.AsArray().Select(l => l!.GetValue<string>()).ToList()),
                MetadataJson.StringMapFrom(body["properties"])));
            await svc.CreateNamespaceAsync(ns, props, ct);
            return Json(new JsonObject { ["namespace"] = Levels(ns), ["properties"] = MetadataJson.StringMap(props) });
        });

        v1.MapGet(Ns, async (string @namespace, CatalogService svc, CancellationToken ct) =>
        {
            var ns = ParseNamespace(@namespace);
            var props = await svc.LoadNamespaceAsync(ns, ct);
            return Json(new JsonObject { ["namespace"] = Levels(ns), ["properties"] = MetadataJson.StringMap(props) });
        });

        v1.MapMethods(Ns, ["HEAD"], async (string @namespace, CatalogService svc, CancellationToken ct) =>
            await svc.NamespaceExistsAsync(ParseNamespace(@namespace), ct) ? Results.NoContent() : Results.NotFound());

        v1.MapDelete(Ns, async (string @namespace, CatalogService svc, CancellationToken ct) =>
        {
            await svc.DropNamespaceAsync(ParseNamespace(@namespace), ct);
            return Results.NoContent();
        });

        v1.MapPost(Ns + "/properties", async (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (updates, removals) = Parse(() => (
                MetadataJson.StringMapFrom(body["updates"]),
                body["removals"]?.AsArray().Select(r => r!.GetValue<string>()).ToList() ?? []));
            var (updated, removed, missing) = await svc.UpdateNamespacePropertiesAsync(ParseNamespace(@namespace), updates, removals, ct);
            return Json(new JsonObject
            {
                ["updated"] = Strings(updated),
                ["removed"] = Strings(removed),
                ["missing"] = missing.Count == 0 ? null : Strings(missing),
            });
        });

        // ---------- tables ----------

        v1.MapGet(Ns + "/tables", (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
            ListEntriesAsync(@namespace, CatalogEntryKind.Table, req, svc, ct));

        v1.MapPost(Ns + "/tables", async (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var spec = Parse(() => new CreateTableSpec(
                body["name"]!.GetValue<string>(),
                TypeJson.SchemaFromJson(body["schema"]!),
                body["partition-spec"] is { } ps ? TypeJson.SpecFromJson(ps) : null,
                body["write-order"] is { } wo ? TypeJson.SortOrderFromJson(wo) : null,
                body["location"]?.GetValue<string>(),
                MetadataJson.StringMapFrom(body["properties"]),
                body["stage-create"]?.GetValue<bool>() ?? false));
            var table = await svc.CreateTableAsync(ParseNamespace(@namespace), spec, ct);
            return TableResult(req, table, options);
        });

        v1.MapPost(Ns + "/register", async (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (name, location, overwrite) = Parse(() => (
                body["name"]!.GetValue<string>(), body["metadata-location"]!.GetValue<string>(), body["overwrite"]?.GetValue<bool>() ?? false));
            return TableResult(req, await svc.RegisterTableAsync(ParseNamespace(@namespace), name, location, overwrite, ct), options);
        });

        v1.MapGet(Tbl, async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var loaded = await svc.LoadTableAsync(Id(@namespace, table), ct);
            var mode = req.Query["snapshots"].ToString() is { Length: > 0 } s ? s : "all";
            if (mode is not ("all" or "refs")) throw new BadRequestException($"snapshots must be 'all' or 'refs', not '{mode}'");
            var etag = ETag(loaded.MetadataLocation!, mode);
            if (req.Headers.IfNoneMatch.Any(v => v is not null && (v == etag || v == "*"))) return Results.StatusCode(StatusCodes.Status304NotModified);
            if (mode == "refs")
            {
                var referenced = loaded.Metadata.Refs.Values.Select(r => r.SnapshotId).ToHashSet();
                loaded = loaded with { Metadata = loaded.Metadata with { Snapshots = loaded.Metadata.Snapshots.Where(sn => referenced.Contains(sn.SnapshotId)).ToList() } };
            }
            return TableResult(req, loaded, options, etag);
        });

        v1.MapMethods(Tbl, ["HEAD"], async (string @namespace, string table, CatalogService svc, CancellationToken ct) =>
            await svc.TableExistsAsync(Id(@namespace, table), ct) ? Results.NoContent() : Results.NotFound());

        v1.MapPost(Tbl, async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var id = Id(@namespace, table);
            if (body["identifier"] is { } given && !ParseIdentifier(given).Equals(id))
                throw new BadRequestException($"The body's identifier {ParseIdentifier(given)} does not match the path's {id}");
            var committed = await svc.CommitTableAsync(ParseCommit(id, body), ct);
            return TableResult(req, committed, options, includeConfig: false);
        });

        v1.MapDelete(Tbl, async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var purge = bool.TryParse(req.Query["purgeRequested"].ToString(), out var p) && p;
            await svc.DropTableAsync(Id(@namespace, table), purge, ct);
            return Results.NoContent();
        });

        v1.MapPost(Tbl + "/unregister", async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
            TableResult(req, await svc.UnregisterTableAsync(Id(@namespace, table), ct), options, includeConfig: false));

        // Metrics reports (scan/commit) are accepted and discarded, as the spec allows.
        v1.MapPost(Tbl + "/metrics", async (string @namespace, string table, CatalogService svc, CancellationToken ct) =>
            await svc.TableExistsAsync(Id(@namespace, table), ct) ? Results.NoContent() : throw new NoSuchTableException($"Table does not exist: {Id(@namespace, table)}"));

        v1.MapPost("/tables/rename", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (from, to) = Parse(() => (ParseIdentifier(body["source"]!), ParseIdentifier(body["destination"]!)));
            await svc.RenameAsync(from, to, CatalogEntryKind.Table, ct);
            return Results.NoContent();
        });

        v1.MapPost("/transactions/commit", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var commits = Parse(() => body["table-changes"]!.AsArray().Select(c => ParseCommit(ParseIdentifier(c!["identifier"]!), c)).ToList());
            await svc.CommitAsync(commits, ct);
            return Results.NoContent();
        });

        // ---------- views ----------

        v1.MapGet(Ns + "/views", (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
            ListEntriesAsync(@namespace, CatalogEntryKind.View, req, svc, ct));

        v1.MapPost(Ns + "/views", async (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var ns = ParseNamespace(@namespace);
            var (name, location, schema, version, props) = Parse(() => (
                body["name"]!.GetValue<string>(),
                body["location"]?.GetValue<string>(),
                TypeJson.SchemaFromJson(body["schema"]!),
                ViewJson.VersionFromJson(body["view-version"]!),
                MetadataJson.StringMapFrom(body["properties"])));
            return ViewResult(await svc.CreateViewAsync(new TableIdentifier(ns, name), location, schema, version, props, ct), options);
        });

        v1.MapGet(Vw, async (string @namespace, string view, CatalogService svc, CancellationToken ct) =>
            ViewResult(await svc.LoadViewAsync(Id(@namespace, view), ct), options));

        v1.MapMethods(Vw, ["HEAD"], async (string @namespace, string view, CatalogService svc, CancellationToken ct) =>
            await svc.ViewExistsAsync(Id(@namespace, view), ct) ? Results.NoContent() : Results.NotFound());

        v1.MapPost(Vw, async (string @namespace, string view, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (requirements, updates) = Parse(() => (
                body["requirements"]?.AsArray().Select(r => ViewRequirement.FromJson(r!)).ToList() ?? [],
                body["updates"]!.AsArray().Select(u => ViewUpdate.FromJson(u!)).ToList()));
            return ViewResult(await svc.ReplaceViewAsync(Id(@namespace, view), requirements, updates, ct), options);
        });

        v1.MapDelete(Vw, async (string @namespace, string view, CatalogService svc, CancellationToken ct) =>
        {
            await svc.DropViewAsync(Id(@namespace, view), ct);
            return Results.NoContent();
        });

        v1.MapPost("/views/rename", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (from, to) = Parse(() => (ParseIdentifier(body["source"]!), ParseIdentifier(body["destination"]!)));
            await svc.RenameAsync(from, to, CatalogEntryKind.View, ct);
            return Results.NoContent();
        });
    }

    // ---------- helpers ----------

    private static async Task<IResult> ListEntriesAsync(string @namespace, CatalogEntryKind kind, HttpRequest req, CatalogService svc, CancellationToken ct)
    {
        var ns = ParseNamespace(@namespace);
        var (page, next) = Page(await svc.ListAsync(ns, kind, ct), n => n, req);
        return Json(new JsonObject
        {
            ["identifiers"] = new JsonArray(page.Select(n => (JsonNode)new JsonObject { ["namespace"] = Levels(ns), ["name"] = n }).ToArray()),
            ["next-page-token"] = next,
        });
    }

    /// <summary>
    /// Keyset paging over an ordinally sorted list: the token is the last key returned. Without <c>pageSize</c>
    /// everything comes back in one page.
    /// </summary>
    private static (IReadOnlyList<T> Page, string? Next) Page<T>(IReadOnlyList<T> sorted, Func<T, string> key, HttpRequest req)
    {
        IEnumerable<T> rest = sorted;
        if (req.Query["pageToken"].ToString() is { Length: > 0 } token)
        {
            string after;
            try { after = Encoding.UTF8.GetString(Convert.FromBase64String(token)); }
            catch (FormatException) { throw new BadRequestException("Invalid pageToken"); }
            rest = sorted.Where(x => string.CompareOrdinal(key(x), after) > 0);
        }
        var size = req.Query["pageSize"].ToString() is { Length: > 0 } s
            ? int.TryParse(s, out var n) && n > 0 ? n : throw new BadRequestException("pageSize must be a positive integer")
            : int.MaxValue;
        var remaining = rest.ToList();
        var page = remaining.Take(size).ToList();
        var next = remaining.Count > page.Count ? Convert.ToBase64String(Encoding.UTF8.GetBytes(key(page[^1]))) : null;
        return (page, next);
    }

    /// <summary>Levels are joined by the unit separator 0x1F (the spec's default <c>namespace-separator</c>), already URL-decoded by routing.</summary>
    public static Namespace ParseNamespace(string raw)
    {
        var levels = raw.Replace("%1F", "\u001f", StringComparison.OrdinalIgnoreCase).Split('\u001f');
        if (levels.Any(l => l.Length == 0)) throw new BadRequestException($"Invalid namespace '{raw}'");
        return new Namespace(levels);
    }

    private static TableIdentifier Id(string @namespace, string name) => new(ParseNamespace(@namespace), name.Replace("%2F", "/", StringComparison.OrdinalIgnoreCase));

    private static TableIdentifier ParseIdentifier(JsonNode n) =>
        new(new Namespace(n["namespace"]!.AsArray().Select(l => l!.GetValue<string>()).ToList()), n["name"]!.GetValue<string>());

    private static TableCommit ParseCommit(TableIdentifier id, JsonNode body) => Parse(() => new TableCommit(
        id,
        body["requirements"]?.AsArray().Select(r => UpdateJson.RequirementFromJson(r!)).ToList() ?? [],
        body["updates"]?.AsArray().Select(u => UpdateJson.UpdateFromJson(u!)).ToList() ?? []));

    private static JsonArray Levels(Namespace ns) => new(ns.Levels.Select(l => (JsonNode)l).ToArray());
    private static JsonArray Strings(IEnumerable<string> items) => new(items.Select(i => (JsonNode)i).ToArray());

    private static async Task<JsonNode> BodyAsync(HttpRequest req, CancellationToken ct)
    {
        try
        {
            return await JsonNode.ParseAsync(req.Body, cancellationToken: ct) ?? throw new BadRequestException("Request body must be a JSON object");
        }
        catch (System.Text.Json.JsonException e)
        {
            throw new BadRequestException($"Malformed JSON: {e.Message}");
        }
    }

    /// <summary>Missing or mistyped fields in a request body are a 400, not a 500.</summary>
    private static T Parse<T>(Func<T> f)
    {
        try
        {
            return f();
        }
        catch (Exception e) when (e is NullReferenceException or InvalidOperationException or FormatException or KeyNotFoundException
                                       or System.Text.Json.JsonException or InvalidDataException or ArgumentException or NotSupportedException)
        {
            throw new BadRequestException($"Invalid request: {e.Message}");
        }
    }

    private static string ETag(string metadataLocation, string mode) =>
        $"\"{Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes($"{mode}:{metadataLocation}")))[..32].ToLowerInvariant()}\"";

    private static IResult TableResult(HttpRequest req, LoadedTable t, RestServerOptions options, string? etag = null, bool includeConfig = true)
    {
        var body = new JsonObject
        {
            ["metadata-location"] = t.MetadataLocation,
            ["metadata"] = MetadataJson.ToJson(t.Metadata),
        };
        if (includeConfig) body["config"] = MetadataJson.StringMap(options.TableConfig);
        etag ??= t.MetadataLocation is null ? null : ETag(t.MetadataLocation, "all");
        if (etag is not null) req.HttpContext.Response.Headers.ETag = etag;
        return Json(body);
    }

    private static IResult ViewResult(LoadedView v, RestServerOptions options) => Json(new JsonObject
    {
        ["metadata-location"] = v.MetadataLocation,
        ["metadata"] = ViewJson.ToJson(v.Metadata),
        ["config"] = MetadataJson.StringMap(options.TableConfig),
    });

    private static IResult Json(JsonNode body) => Results.Content(body.ToJsonString(), "application/json");
}
