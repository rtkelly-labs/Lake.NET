using Microsoft.AspNetCore.OpenApi;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi;

namespace Iceberg.Net.RestServer;

/// <summary>
/// What the server's OpenAPI document (<c>/openapi/v1.json</c>) says about each route beyond what routing knows: the
/// spec's <c>operationId</c>, the query and header parameters the handler reads, and the statuses it can answer with.
/// Paths, methods and path parameters come from the routes themselves, so a route that drifts from the spec shows in
/// the document. <c>OpenApiConformanceTests</c> diffs the document against the reference spec.
/// </summary>
public static class OpenApiOperations
{
    /// <summary>One operation: its spec name, success status, other statuses, query parameters and request headers.</summary>
    public sealed record Operation(string OperationId, int Success, int[] Errors, string[] Query, string[] Headers);

    private const string Idem = "Idempotency-Key";
    private const string Delegation = "X-Iceberg-Access-Delegation";

    // Every catalog route can answer 400 (malformed request), 401 (authentication on, no or bad token) and 403 (no
    // grant); getConfig and getToken are listed in full.
    private static Operation Op(string id, int success, int[] errors, string[]? query = null, string[]? headers = null) =>
        new(id, success, [400, 401, 403, .. errors], query ?? [], headers ?? []);

    /// <summary>By <c>METHOD /v1/{prefix}/…</c>, the form <see cref="Endpoints.Advertised"/> uses.</summary>
    public static readonly IReadOnlyDictionary<string, Operation> ByRoute = new Dictionary<string, Operation>(StringComparer.Ordinal)
    {
        // The server holds one warehouse: the parameter is accepted and does not select anything.
        ["GET /v1/config"] = new("getConfig", 200, [401], ["warehouse"], []),
        ["POST /v1/oauth/tokens"] = new("getToken", 200, [400, 401], [], []),

        ["GET /v1/{prefix}/namespaces"] = Op("listNamespaces", 200, [404], ["pageToken", "pageSize", "parent"]),
        ["POST /v1/{prefix}/namespaces"] = Op("createNamespace", 200, [409], headers: [Idem]),
        ["GET /v1/{prefix}/namespaces/{namespace}"] = Op("loadNamespaceMetadata", 200, [404]),
        ["HEAD /v1/{prefix}/namespaces/{namespace}"] = Op("namespaceExists", 204, [404]),
        ["DELETE /v1/{prefix}/namespaces/{namespace}"] = Op("dropNamespace", 204, [404, 409], headers: [Idem]),
        ["POST /v1/{prefix}/namespaces/{namespace}/properties"] = Op("updateProperties", 200, [404, 422], headers: [Idem]),

        ["GET /v1/{prefix}/namespaces/{namespace}/tables"] = Op("listTables", 200, [404], ["pageToken", "pageSize"]),
        ["POST /v1/{prefix}/namespaces/{namespace}/tables"] = Op("createTable", 200, [404, 409], headers: [Delegation, Idem]),
        ["POST /v1/{prefix}/namespaces/{namespace}/register"] = Op("registerTable", 200, [404, 409], headers: [Delegation, Idem]),
        ["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"] = Op("loadTable", 200, [304, 404], ["snapshots"], [Delegation, "If-None-Match"]),
        ["HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"] = Op("tableExists", 204, [404]),
        ["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"] = Op("updateTable", 200, [404, 409, 500], headers: [Idem]),
        ["DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"] = Op("dropTable", 204, [404], ["purgeRequested"], [Idem]),
        ["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister"] = Op("unregisterTable", 200, [404], headers: [Idem]),
        ["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"] = Op("reportMetrics", 204, [404], headers: [Idem]),
        ["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"] = Op("loadCredentials", 200, [404]),
        ["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign"] = Op("signRequest", 200, [404]),
        ["POST /v1/{prefix}/tables/rename"] = Op("renameTable", 204, [404, 409], headers: [Idem]),
        ["POST /v1/{prefix}/transactions/commit"] = Op("commitTransaction", 204, [404, 409, 500], headers: [Idem]),

        ["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"] = Op("planTableScan", 200, [404]),
        ["GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"] = Op("fetchPlanningResult", 200, [404]),
        ["DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}"] = Op("cancelPlanning", 204, [404]),
        ["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks"] = Op("fetchScanTasks", 200, [404]),

        ["GET /v1/{prefix}/namespaces/{namespace}/views"] = Op("listViews", 200, [404], ["pageToken", "pageSize"]),
        ["POST /v1/{prefix}/namespaces/{namespace}/views"] = Op("createView", 200, [404, 409], headers: [Idem]),
        ["GET /v1/{prefix}/namespaces/{namespace}/views/{view}"] = Op("loadView", 200, [404]),
        ["HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}"] = Op("viewExists", 204, [404]),
        ["POST /v1/{prefix}/namespaces/{namespace}/views/{view}"] = Op("replaceView", 200, [404, 409], headers: [Idem]),
        ["DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}"] = Op("dropView", 204, [404], headers: [Idem]),
        ["POST /v1/{prefix}/views/rename"] = Op("renameView", 204, [404, 409], headers: [Idem]),
    };

    /// <summary>The route key of a generated operation: its method and path, with the configured prefix as <c>{prefix}</c>.</summary>
    public static string RouteKey(string method, string relativePath, string? prefix)
    {
        var path = "/" + relativePath.Trim('/');
        // The config and token routes never take the prefix, even when it shares their spelling (Iceberg:Prefix=oauth).
        if (path is "/v1/config" or "/v1/oauth/tokens") return $"{method.ToUpperInvariant()} {path}";
        if (prefix is { Length: > 0 } p && path.StartsWith($"/v1/{Uri.EscapeDataString(p)}/", StringComparison.Ordinal))
            path = "/v1/{prefix}/" + path[($"/v1/{Uri.EscapeDataString(p)}/".Length)..];
        else if (path.StartsWith("/v1/", StringComparison.Ordinal))
            path = "/v1/{prefix}/" + path["/v1/".Length..];
        return $"{method.ToUpperInvariant()} {path}";
    }

    /// <summary>Register the OpenAPI document, with each operation described from <see cref="ByRoute"/>.</summary>
    public static IServiceCollection AddIcebergOpenApi(this IServiceCollection services) => services.AddOpenApi("v1", o =>
    {
        o.AddDocumentTransformer((doc, _, _) =>
        {
            doc.Info = new OpenApiInfo { Title = "Apache Iceberg REST Catalog API (Iceberg.Net.RestServer)", Version = "0.0.1" };
            return Task.CompletedTask;
        });
        o.AddOperationTransformer((op, ctx, _) =>
        {
            var options = ctx.ApplicationServices.GetRequiredService<RestServerOptions>();
            var key = RouteKey(ctx.Description.HttpMethod ?? "GET", ctx.Description.RelativePath ?? "", options.Prefix);
            if (!ByRoute.TryGetValue(key, out var spec)) return Task.CompletedTask;
            op.OperationId = spec.OperationId;
            op.Parameters ??= [];
            foreach (var q in spec.Query)
                op.Parameters.Add(new OpenApiParameter { Name = q, In = ParameterLocation.Query, Schema = new OpenApiSchema { Type = JsonSchemaType.String } });
            foreach (var h in spec.Headers)
                op.Parameters.Add(new OpenApiParameter { Name = h, In = ParameterLocation.Header, Schema = new OpenApiSchema { Type = JsonSchemaType.String } });
            op.Responses = new OpenApiResponses { [spec.Success.ToString(System.Globalization.CultureInfo.InvariantCulture)] = new OpenApiResponse { Description = "Success" } };
            foreach (var code in spec.Errors)
                op.Responses[code == 500 ? "5XX" : code.ToString(System.Globalization.CultureInfo.InvariantCulture)] = new OpenApiResponse
                {
                    Description = code switch
                    {
                        304 => "Not modified (If-None-Match)",
                        500 => "Server error; for a commit, its outcome is unknown",
                        _ => "ErrorModel",
                    },
                };
            return Task.CompletedTask;
        });
    });
}
