using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
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

    /// <summary>The largest request body accepted.</summary>
    public const long MaxRequestBodyBytes = 16 * 1024 * 1024;

    /// <summary>The largest page a list returns, and the page size when the client asks for none.</summary>
    public const int MaxPageSize = 1000;
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

    /// <summary>The scan planning routes, advertised when <see cref="ScanPlanningOptions.Enabled"/>.</summary>
    public static readonly string[] Planning =
    [
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
        "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
    ];

    /// <summary>The remote signing route, advertised when remote signing is on.</summary>
    public const string Sign = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign";

    /// <summary>The credentials route, advertised when credentials are vended.</summary>
    public const string Credentials = "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials";

    /// <summary>Configurations that would quietly widen access are refused at start-up (docs/rest-server.md, Authentication).</summary>
    private static void ValidateSecurity(RestServerOptions options)
    {
        if (options.Auth.Authority is not null && options.Auth.Audience is null && !options.Auth.AllowAnyAudience)
            throw new InvalidOperationException("Iceberg:Auth:Authority is set without Iceberg:Auth:Audience: tokens issued for any application would be accepted. Set the audience, or Iceberg:Auth:AllowAnyAudience=true.");
        if (options.Auth.Enabled && !options.Authorization.Enabled && !options.Authorization.AllowAll)
            throw new InvalidOperationException("Authentication is on but no Iceberg:Authorization:Grants are configured, which would make every authenticated principal an admin. Configure grants, or set Iceberg:Authorization:AllowAll=true.");
    }

    public static void MapIcebergRestCatalog(this WebApplication app)
    {
        var options = app.Services.GetRequiredService<RestServerOptions>();
        ValidateSecurity(options);
        var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Iceberg.Net.RestServer");
        var vendor = app.Services.GetRequiredService<IStorageCredentialVendor>();
        var vending = vendor is not NoCredentialVendor;
        var signer = options.RemoteSigning.Enabled ? app.Services.GetRequiredService<S3RequestSigner>() : null;
        // Create the store's tables before the first request.
        app.Services.GetRequiredService<ICatalogStore>().InitializeAsync().GetAwaiter().GetResult();

        // Requests are JSON documents of kilobytes; nothing a client sends needs more than this. First of all, so no
        // middleware (idempotency hashes the body) reads more than the limit; a body over it is a 413.
        app.Use(async (ctx, next) =>
        {
            if (ctx.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } limit)
                limit.MaxRequestBodySize = MaxRequestBodyBytes;
            try
            {
                await next(ctx);
            }
            catch (Microsoft.AspNetCore.Http.BadHttpRequestException e) when (!ctx.Response.HasStarted)
            {
                var (status, type) = ErrorModel.Classify(e);
                await ErrorModel.WriteAsync(ctx, status, type, e.Message);
            }
        });

        // Outside the error handler, so the response it keeps is the final one (errors included).
        if (options.Idempotency.Enabled)
        {
            var idempotency = app.Services.GetRequiredService<IdempotencyStore>();
            app.Use((ctx, next) => IdempotencyStore.Applies(ctx.Request) ? idempotency.InvokeAsync(ctx, () => next(ctx)) : next(ctx));
        }

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
                if (status >= 500)
                {
                    // The path is the client's: no line breaks reach the log. The details stay in the log, not the response.
                    var path = ctx.Request.Path.ToString().Replace('\r', ' ').Replace('\n', ' ');
                    logger.LogError(e, "{Method} {Path} failed ({TraceId})", ctx.Request.Method, path, ctx.TraceIdentifier);
                    await ErrorModel.WriteAsync(ctx, status, type, $"Internal server error (trace {ctx.TraceIdentifier})");
                }
                else await ErrorModel.WriteAsync(ctx, status, type, e.Message);
            }
        });

        app.MapPost("/v1/oauth/tokens", AuthSetup.TokenAsync).AllowAnonymous();
        // The server's own description of these routes (OpenApiOperations), public like the spec it follows.
        app.MapOpenApi("/openapi/{documentName}.json").AllowAnonymous();

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
            var endpoints = Advertised.Concat(options.ScanPlanning.Enabled ? Planning : []).Concat(vending ? [Credentials] : []).Concat(signer is not null ? [Sign] : []);
            var config = new JsonObject
            {
                ["defaults"] = MetadataJson.StringMap(options.Defaults),
                ["overrides"] = MetadataJson.StringMap(overrides),
                ["endpoints"] = new JsonArray(endpoints.Select(e => (JsonNode)e).ToArray()),
            };
            if (options.Idempotency.Enabled) config["idempotency-key-lifetime"] = System.Xml.XmlConvert.ToString(options.Idempotency.Lifetime);
            return Json(config);
        });

        // ---------- namespaces ----------

        v1.MapGet("/namespaces", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var parent = req.Query["parent"].ToString() is { Length: > 0 } raw ? ParseNamespace(raw) : null;
            if (parent is not null) await req.HttpContext.RequireAsync(CatalogPrivilege.Browse, parent);
            var all = await FilterAsync(await svc.ListNamespacesAsync(parent, ct), n => req.HttpContext.AllowsAsync(CatalogPrivilege.Browse, n));
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
            await req.HttpContext.RequireAsync(CatalogPrivilege.Admin, ns);
            await svc.CreateNamespaceAsync(ns, props, ct);
            return Json(new JsonObject { ["namespace"] = Levels(ns), ["properties"] = MetadataJson.StringMap(props) });
        });

        v1.MapGet(Ns, async (string @namespace, HttpContext http, CatalogService svc, CancellationToken ct) =>
        {
            var ns = ParseNamespace(@namespace);
            await http.RequireAsync(CatalogPrivilege.Browse, ns);
            var props = await svc.LoadNamespaceAsync(ns, ct);
            return Json(new JsonObject { ["namespace"] = Levels(ns), ["properties"] = MetadataJson.StringMap(props) });
        });

        v1.MapMethods(Ns, ["HEAD"], async (string @namespace, HttpContext http, CatalogService svc, CancellationToken ct) =>
        {
            var ns = ParseNamespace(@namespace);
            await http.RequireAsync(CatalogPrivilege.Browse, ns);
            return await svc.NamespaceExistsAsync(ns, ct) ? Results.NoContent() : Results.NotFound();
        });

        v1.MapDelete(Ns, async (string @namespace, HttpContext http, CatalogService svc, CancellationToken ct) =>
        {
            var ns = ParseNamespace(@namespace);
            await http.RequireAsync(CatalogPrivilege.Admin, ns);
            await svc.DropNamespaceAsync(ns, ct);
            return Results.NoContent();
        });

        v1.MapPost(Ns + "/properties", async (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            await req.HttpContext.RequireAsync(CatalogPrivilege.Admin, ParseNamespace(@namespace));
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
            var ns = ParseNamespace(@namespace);
            var body = await BodyAsync(req, ct);
            var spec = Parse(() => new CreateTableSpec(
                body["name"]!.GetValue<string>(),
                TypeJson.SchemaFromJson(body["schema"]!),
                body["partition-spec"] is { } ps ? TypeJson.SpecFromJson(ps) : null,
                body["write-order"] is { } wo ? TypeJson.SortOrderFromJson(wo) : null,
                body["location"]?.GetValue<string>(),
                MetadataJson.StringMapFrom(body["properties"]),
                body["stage-create"]?.GetValue<bool>() ?? false));
            await req.HttpContext.RequireAsync(CatalogPrivilege.Write, ns, spec.Name);
            var table = await svc.CreateTableAsync(ns, spec, ct);
            return await TableResultAsync(req, table, options, vendor, signer);
        });

        v1.MapPost(Ns + "/register", async (string @namespace, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (name, location, overwrite) = Parse(() => (
                body["name"]!.GetValue<string>(), body["metadata-location"]!.GetValue<string>(), body["overwrite"]?.GetValue<bool>() ?? false));
            var ns = ParseNamespace(@namespace);
            await req.HttpContext.RequireAsync(overwrite ? CatalogPrivilege.Admin : CatalogPrivilege.Write, ns, name);
            return await TableResultAsync(req, await svc.RegisterTableAsync(ns, name, location, overwrite, ct), options, vendor, signer);
        });

        v1.MapGet(Tbl, async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var id = await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Read, @namespace, table);
            var loaded = await svc.LoadTableAsync(id, ct);
            var mode = req.Query["snapshots"].ToString() is { Length: > 0 } s ? s : "all";
            if (mode is not ("all" or "refs")) throw new BadRequestException($"snapshots must be 'all' or 'refs', not '{mode}'");
            var etag = ETag(loaded.MetadataLocation!, mode);
            if (req.Headers.IfNoneMatch.Any(v => v is not null && (v == etag || v == "*"))) return Results.StatusCode(StatusCodes.Status304NotModified);
            if (mode == "refs")
            {
                var referenced = loaded.Metadata.Refs.Values.Select(r => r.SnapshotId).ToHashSet();
                loaded = loaded with { Metadata = loaded.Metadata with { Snapshots = loaded.Metadata.Snapshots.Where(sn => referenced.Contains(sn.SnapshotId)).ToList() } };
            }
            return await TableResultAsync(req, loaded, options, vendor, signer, etag);
        });

        v1.MapMethods(Tbl, ["HEAD"], async (string @namespace, string table, HttpContext http, CatalogService svc, CancellationToken ct) =>
            await svc.TableExistsAsync(await AuthorizedAsync(http, CatalogPrivilege.Read, @namespace, table), ct) ? Results.NoContent() : Results.NotFound());

        v1.MapPost(Tbl, async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var id = await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Write, @namespace, table);
            var body = await BodyAsync(req, ct);
            if (body["identifier"] is { } given && !ParseIdentifier(given).Equals(id))
                throw new BadRequestException($"The body's identifier {ParseIdentifier(given)} does not match the path's {id}");
            var committed = await svc.CommitTableAsync(ParseCommit(id, body), ct);
            return await TableResultAsync(req, committed, options, vendor, signer, includeConfig: false);
        });

        v1.MapDelete(Tbl, async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var purge = bool.TryParse(req.Query["purgeRequested"].ToString(), out var p) && p;
            await svc.DropTableAsync(await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Admin, @namespace, table), purge, ct);
            return Results.NoContent();
        });

        v1.MapPost(Tbl + "/unregister", async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
            await TableResultAsync(req, await svc.UnregisterTableAsync(await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Admin, @namespace, table), ct), options, vendor,
                signer, includeConfig: false));

        // Metrics reports (scan/commit) are accepted and discarded, as the spec allows.
        v1.MapPost(Tbl + "/metrics", async (string @namespace, string table, HttpContext http, CatalogService svc, CancellationToken ct) =>
            await svc.TableExistsAsync(await AuthorizedAsync(http, CatalogPrivilege.Read, @namespace, table), ct)
                ? Results.NoContent()
                : throw new NoSuchTableException($"Table does not exist: {Id(@namespace, table)}"));

        if (options.ScanPlanning.Enabled)
        {
            v1.MapPost(Tbl + "/plan", async (string @namespace, string table, HttpRequest req, ScanPlanner planner, CancellationToken ct) =>
            {
                var id = await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Read, @namespace, table);
                var body = await BodyAsync(req, ct);
                return Json(await planner.PlanAsync(id, Parse(() => PlanRequest.Parse(body)), Principals.Name(req.HttpContext.User), ct));
            });

            v1.MapGet(Tbl + "/plan/{plan-id}", async (string @namespace, string table, [FromRoute(Name = "plan-id")] string planId, HttpContext http, ScanPlanner planner, CancellationToken ct) =>
                Json(await planner.FetchAsync(await AuthorizedAsync(http, CatalogPrivilege.Read, @namespace, table), planId, Principals.Name(http.User), ct)));

            v1.MapDelete(Tbl + "/plan/{plan-id}", async (string @namespace, string table, [FromRoute(Name = "plan-id")] string planId, HttpContext http, ScanPlanner planner, CancellationToken ct) =>
            {
                await planner.CancelAsync(await AuthorizedAsync(http, CatalogPrivilege.Read, @namespace, table), planId, Principals.Name(http.User), ct);
                return Results.NoContent();
            });

            v1.MapPost(Tbl + "/tasks", async (string @namespace, string table, HttpRequest req, ScanPlanner planner, CancellationToken ct) =>
            {
                var id = await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Read, @namespace, table);
                var body = await BodyAsync(req, ct);
                return Json(await planner.FetchTasksAsync(id, Parse(() => body["plan-task"]!.GetValue<string>()), ct));
            });
        }

        if (vending)
        {
            v1.MapGet(Tbl + "/credentials", async (string @namespace, string table, HttpContext http, CatalogService svc, CancellationToken ct) =>
            {
                var id = await AuthorizedAsync(http, CatalogPrivilege.Read, @namespace, table);
                var loaded = await svc.LoadTableAsync(id, ct);
                var write = await http.AllowsAsync(CatalogPrivilege.Write, id.Namespace, id.Name);
                var credentials = await vendor.VendAsync(id, loaded.Metadata.Location, write, ct);
                return Json(new JsonObject { ["storage-credentials"] = new JsonArray(credentials.Select(c => (JsonNode)c.ToJson()).ToArray()) });
            });
        }

        if (signer is not null)
        {
            v1.MapPost(Tbl + "/sign", async (string @namespace, string table, HttpRequest req, CatalogService svc, CancellationToken ct) =>
            {
                var id = await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Read, @namespace, table);
                var body = await BodyAsync(req, ct);
                var request = Parse(() => RemoteSignRequest.Parse(body));
                var loaded = await svc.LoadTableAsync(id, ct);
                if (request.Writes) await req.HttpContext.RequireAsync(CatalogPrivilege.Write, id.Namespace, id.Name);
                var (result, cacheControl) = await signer.SignAsync(request, S3RequestSigner.TablePrefixes(loaded.Metadata), ct);
                req.HttpContext.Response.Headers.CacheControl = cacheControl;
                return Json(result);
            });
        }

        v1.MapPost("/tables/rename", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (from, to) = Parse(() => (ParseIdentifier(body["source"]!), ParseIdentifier(body["destination"]!)));
            await req.HttpContext.RequireAsync(CatalogPrivilege.Admin, from.Namespace, from.Name);
            await req.HttpContext.RequireAsync(CatalogPrivilege.Write, to.Namespace, to.Name);
            await svc.RenameAsync(from, to, CatalogEntryKind.Table, ct);
            return Results.NoContent();
        });

        v1.MapPost("/transactions/commit", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var commits = Parse(() => body["table-changes"]!.AsArray().Select(c => ParseCommit(ParseIdentifier(c!["identifier"]!), c)).ToList());
            foreach (var c in commits) await req.HttpContext.RequireAsync(CatalogPrivilege.Write, c.Identifier.Namespace, c.Identifier.Name);
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
            await req.HttpContext.RequireAsync(CatalogPrivilege.Write, ns, name);
            return ViewResult(await svc.CreateViewAsync(new TableIdentifier(ns, name), location, schema, version, props, ct), options);
        });

        v1.MapGet(Vw, async (string @namespace, string view, HttpContext http, CatalogService svc, CancellationToken ct) =>
            ViewResult(await svc.LoadViewAsync(await AuthorizedAsync(http, CatalogPrivilege.Read, @namespace, view), ct), options));

        v1.MapMethods(Vw, ["HEAD"], async (string @namespace, string view, HttpContext http, CatalogService svc, CancellationToken ct) =>
            await svc.ViewExistsAsync(await AuthorizedAsync(http, CatalogPrivilege.Read, @namespace, view), ct) ? Results.NoContent() : Results.NotFound());

        v1.MapPost(Vw, async (string @namespace, string view, HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var id = await AuthorizedAsync(req.HttpContext, CatalogPrivilege.Write, @namespace, view);
            var body = await BodyAsync(req, ct);
            var (requirements, updates) = Parse(() => (
                body["requirements"]?.AsArray().Select(r => ViewRequirement.FromJson(r!)).ToList() ?? [],
                body["updates"]!.AsArray().Select(u => ViewUpdate.FromJson(u!)).ToList()));
            return ViewResult(await svc.ReplaceViewAsync(id, requirements, updates, ct), options);
        });

        v1.MapDelete(Vw, async (string @namespace, string view, HttpContext http, CatalogService svc, CancellationToken ct) =>
        {
            await svc.DropViewAsync(await AuthorizedAsync(http, CatalogPrivilege.Admin, @namespace, view), ct);
            return Results.NoContent();
        });

        v1.MapPost("/views/rename", async (HttpRequest req, CatalogService svc, CancellationToken ct) =>
        {
            var body = await BodyAsync(req, ct);
            var (from, to) = Parse(() => (ParseIdentifier(body["source"]!), ParseIdentifier(body["destination"]!)));
            await req.HttpContext.RequireAsync(CatalogPrivilege.Admin, from.Namespace, from.Name);
            await req.HttpContext.RequireAsync(CatalogPrivilege.Write, to.Namespace, to.Name);
            await svc.RenameAsync(from, to, CatalogEntryKind.View, ct);
            return Results.NoContent();
        });
    }

    // ---------- helpers ----------

    private static async Task<IResult> ListEntriesAsync(string @namespace, CatalogEntryKind kind, HttpRequest req, CatalogService svc, CancellationToken ct)
    {
        var ns = ParseNamespace(@namespace);
        await req.HttpContext.RequireAsync(CatalogPrivilege.Browse, ns);
        var all = await FilterAsync(await svc.ListAsync(ns, kind, ct), n => req.HttpContext.AllowsAsync(CatalogPrivilege.Read, ns, n));
        var (page, next) = Page(all, n => n, req);
        return Json(new JsonObject
        {
            ["identifiers"] = new JsonArray(page.Select(n => (JsonNode)new JsonObject { ["namespace"] = Levels(ns), ["name"] = n }).ToArray()),
            ["next-page-token"] = next,
        });
    }

    /// <summary>
    /// Keyset paging over an ordinally sorted list: the token is the last key returned. Pages hold at most
    /// <see cref="MaxPageSize"/> entries, which is also the size without <c>pageSize</c>.
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
            ? int.TryParse(s, out var n) && n > 0 ? Math.Min(n, MaxPageSize) : throw new BadRequestException("pageSize must be a positive integer")
            : MaxPageSize;
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

    private static async Task<IReadOnlyList<T>> FilterAsync<T>(IReadOnlyList<T> items, Func<T, Task<bool>> allowed)
    {
        var kept = new List<T>(items.Count);
        foreach (var item in items) if (await allowed(item)) kept.Add(item);
        return kept;
    }

    /// <summary>The table or view the route names, once the request's principal is known to hold <paramref name="privilege"/> on it.</summary>
    private static async Task<TableIdentifier> AuthorizedAsync(HttpContext http, CatalogPrivilege privilege, string @namespace, string name)
    {
        var id = Id(@namespace, name);
        await http.RequireAsync(privilege, id.Namespace, id.Name);
        return id;
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

    /// <summary>The deepest nesting a request body may have: the deepest the spec's documents need is far less.</summary>
    public const int MaxJsonDepth = 64;

    private static readonly System.Text.Json.JsonDocumentOptions JsonLimits = new() { MaxDepth = MaxJsonDepth };

    private static async Task<JsonNode> BodyAsync(HttpRequest req, CancellationToken ct)
    {
        try
        {
            return await JsonNode.ParseAsync(req.Body, documentOptions: JsonLimits, cancellationToken: ct) ?? throw new BadRequestException("Request body must be a JSON object");
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

    /// <summary>
    /// A <c>LoadTableResult</c>. Its <c>config</c> is <see cref="RestServerOptions.TableConfig"/>, plus
    /// <c>scan-planning-mode</c> when configured, plus vended credentials when the client asked for them
    /// (<c>X-Iceberg-Access-Delegation: vended-credentials</c>) and a vendor is configured: those also go in
    /// <c>storage-credentials</c>, scoped to the table's location, writable if the principal may write the table.
    /// Otherwise, when the client asked for <c>remote-signing</c> and the server signs, the remote signing settings.
    /// </summary>
    private static async Task<IResult> TableResultAsync(HttpRequest req, LoadedTable t, RestServerOptions options, IStorageCredentialVendor vendor,
        S3RequestSigner? signer, string? etag = null, bool includeConfig = true)
    {
        var body = new JsonObject
        {
            ["metadata-location"] = t.MetadataLocation,
            ["metadata"] = MetadataJson.ToJson(t.Metadata),
        };
        if (includeConfig)
        {
            var config = new Dictionary<string, string>(options.TableConfig);
            if (options.ScanPlanning.Enabled && options.ScanPlanning.Mode is { Length: > 0 } mode) config["scan-planning-mode"] = mode;
            var vended = false;
            if (vendor is not NoCredentialVendor && Wants(req, "vended-credentials"))
            {
                var write = await req.HttpContext.AllowsAsync(CatalogPrivilege.Write, t.Id.Namespace, t.Id.Name);
                var credentials = await vendor.VendAsync(t.Id, t.Metadata.Location, write, req.HttpContext.RequestAborted);
                if (credentials.Count > 0)
                {
                    vended = true;
                    body["storage-credentials"] = new JsonArray(credentials.Select(c => (JsonNode)c.ToJson()).ToArray());
                    // Clients that predate storage-credentials read the keys from config; the longest prefix is the table's own.
                    foreach (var (k, v) in credentials.OrderByDescending(c => c.Prefix.Length).First().Config) config[k] = v;
                }
            }
            // Remote signing when asked for and no credentials were vended instead, for tables on S3.
            if (!vended && signer is not null && Wants(req, "remote-signing") && StsCredentialVendor.S3Location(t.Metadata.Location) is not null)
                foreach (var (k, v) in signer.TableConfig(options.Prefix, t.Id)) config[k] = v;
            body["config"] = MetadataJson.StringMap(config);
        }
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

    private static bool Wants(HttpRequest req, string mechanism) =>
        req.Headers["X-Iceberg-Access-Delegation"].SelectMany(v => (v ?? "").Split(',')).Any(v => v.Trim().Equals(mechanism, StringComparison.OrdinalIgnoreCase));

    private static IResult Json(JsonNode body) => Results.Content(body.ToJsonString(), "application/json");
}
