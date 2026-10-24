using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.RestServer;

/// <summary>A malformed or invalid request: HTTP 400 <c>BadRequestException</c>.</summary>
public sealed class BadRequestException(string message) : Exception(message);

/// <summary>Dropping a namespace that still holds tables, views or child namespaces: HTTP 409.</summary>
public sealed class NamespaceNotEmptyException(string message) : Exception(message);

/// <summary>A request that is well-formed but contradicts itself (a property both set and removed): HTTP 422.</summary>
public sealed class UnprocessableEntityException(string message) : Exception(message);

/// <summary>A view that does not exist: HTTP 404 <c>NoSuchViewException</c> (a <see cref="NoSuchTableException"/> to the library's catalogs).</summary>
public sealed class NoSuchViewException(string message) : NoSuchTableException(message);

/// <summary>The spec's <c>IcebergErrorResponse</c>: <c>{"error": {"message", "type", "code"}}</c>, and the exception → status mapping.</summary>
public static class ErrorModel
{
    public static (int Status, string Type) Classify(Exception e) => e switch
    {
        NoSuchViewException => (404, "NoSuchViewException"),
        NoSuchTableException => (404, "NoSuchTableException"),
        NoSuchNamespaceException => (404, "NoSuchNamespaceException"),
        AlreadyExistsException => (409, "AlreadyExistsException"),
        NamespaceNotEmptyException => (409, "NamespaceNotEmptyException"),
        CommitFailedException => (409, "CommitFailedException"),
        UnprocessableEntityException => (422, "UnprocessableEntityException"),
        BadRequestException or ArgumentException or FormatException or System.Text.Json.JsonException => (400, "BadRequestException"),
        NotSupportedException => (406, "UnsupportedOperationException"),
        UnauthorizedAccessException => (403, "ForbiddenException"),
        CommitStateUnknownException => (500, "CommitStateUnknownException"),
        _ => (500, "InternalServerError"),
    };

    public static JsonObject Body(int status, string type, string message) => new()
    {
        ["error"] = new JsonObject { ["message"] = message, ["type"] = type, ["code"] = status },
    };

    public static IResult Result(int status, string type, string message) =>
        Results.Content(Body(status, type, message).ToJsonString(), "application/json", statusCode: status);

    public static async Task WriteAsync(HttpContext context, int status, string type, string message)
    {
        context.Response.StatusCode = status;
        // HEAD responses carry no body; the status alone answers an existence check.
        if (HttpMethods.IsHead(context.Request.Method)) return;
        context.Response.ContentType = "application/json";
        await context.Response.WriteAsync(Body(status, type, message).ToJsonString());
    }
}
