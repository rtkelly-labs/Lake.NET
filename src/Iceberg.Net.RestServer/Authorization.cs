using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestServer;

/// <summary>What a request needs on a resource. Each level includes the ones before it.</summary>
public enum CatalogPrivilege
{
    /// <summary>Know that a namespace exists and list into it: granted by any grant on, above or below it.</summary>
    Browse,
    /// <summary>Load tables and views, plan scans, get read credentials.</summary>
    Read,
    /// <summary>Create, register and commit to tables and views; get write credentials.</summary>
    Write,
    /// <summary>Create, drop and change namespaces; drop, unregister and rename tables and views.</summary>
    Admin,
}

/// <summary>A namespace (<see cref="Name"/> null) or a table or view in it. The catalog root is the empty namespace.</summary>
public sealed record CatalogResource(Namespace Namespace, string? Name = null)
{
    public IReadOnlyList<string> Path => Name is null ? Namespace.Levels : [.. Namespace.Levels, Name];

    public override string ToString() => string.Join('.', Path) is { Length: > 0 } p ? p : "the catalog";
}

/// <summary>
/// Decides whether a principal may do something to a resource. Every endpoint asks it before touching the store; a
/// refusal is a 403 <c>ForbiddenException</c>. Register your own implementation to replace the static grants.
/// </summary>
public interface IAuthorizer
{
    ValueTask<bool> IsAllowedAsync(ClaimsPrincipal principal, CatalogPrivilege privilege, CatalogResource resource, CancellationToken ct = default);
}

/// <summary>The request is authenticated but its principal may not do this: HTTP 403.</summary>
public sealed class ForbiddenException(string message) : Exception(message);

/// <summary>No authorization: every (authenticated) principal may do everything.</summary>
public sealed class AllowAllAuthorizer : IAuthorizer
{
    public ValueTask<bool> IsAllowedAsync(ClaimsPrincipal principal, CatalogPrivilege privilege, CatalogResource resource, CancellationToken ct = default) =>
        ValueTask.FromResult(true);
}

/// <summary>
/// Grants from configuration (<see cref="RestServerAuthorizationOptions"/>): <c>{read|write|admin}:{scope}</c> per
/// principal, where a scope covers its own path and everything beneath it.
/// </summary>
public sealed class StaticAuthorizer : IAuthorizer
{
    private readonly Dictionary<string, List<(CatalogPrivilege Level, string[]? Scope)>> _grants = new(StringComparer.Ordinal);

    public StaticAuthorizer(RestServerAuthorizationOptions options)
    {
        foreach (var (principal, grants) in options.Grants)
            _grants[principal] = grants.Select(Parse).ToList();
    }

    /// <summary><c>read:sales.orders</c> → (Read, ["sales", "orders"]); <c>admin:*</c> → (Admin, null).</summary>
    public static (CatalogPrivilege Level, string[]? Scope) Parse(string grant)
    {
        var colon = grant.IndexOf(':', StringComparison.Ordinal);
        if (colon <= 0 || colon == grant.Length - 1) throw new ArgumentException($"Grant '{grant}' must be '{{read|write|admin}}:{{scope}}'");
        var level = grant[..colon].Trim().ToLowerInvariant() switch
        {
            "read" => CatalogPrivilege.Read,
            "write" => CatalogPrivilege.Write,
            "admin" => CatalogPrivilege.Admin,
            var l => throw new ArgumentException($"Grant '{grant}': unknown level '{l}' (read, write or admin)"),
        };
        var scope = grant[(colon + 1)..].Trim();
        if (scope == "*") return (level, null);
        var levels = scope.Split('.');
        if (levels.Any(l => l.Length == 0)) throw new ArgumentException($"Grant '{grant}': invalid scope '{scope}'");
        return (level, levels);
    }

    public ValueTask<bool> IsAllowedAsync(ClaimsPrincipal principal, CatalogPrivilege privilege, CatalogResource resource, CancellationToken ct = default)
    {
        var path = resource.Path;
        foreach (var (level, scope) in GrantsOf(Principals.Name(principal)))
        {
            if (scope is null || IsPrefix(scope, path))
            {
                if (level >= privilege) return ValueTask.FromResult(true);
            }
            // A grant somewhere below a namespace lets its holder see the namespace (to list down to what they may use).
            else if (privilege == CatalogPrivilege.Browse && resource.Name is null && IsPrefix(path, scope))
            {
                return ValueTask.FromResult(true);
            }
        }
        return ValueTask.FromResult(false);
    }

    private IEnumerable<(CatalogPrivilege Level, string[]? Scope)> GrantsOf(string principal) =>
        (_grants.GetValueOrDefault(principal) ?? []).Concat(_grants.GetValueOrDefault("*") ?? []);

    private static bool IsPrefix(IReadOnlyList<string> prefix, IReadOnlyList<string> path) =>
        prefix.Count <= path.Count && prefix.Select((l, i) => l == path[i]).All(x => x);
}

/// <summary>Who a request comes from, as authorization and idempotency see it.</summary>
public static class Principals
{
    /// <summary>The claim authentication sets to the request's principal; one in a token itself is discarded.</summary>
    public const string PrincipalClaim = "iceberg_principal";

    /// <summary>What names the principals of the external OpenID Connect issuer (<see cref="RestServerAuthOptions.Authority"/>).</summary>
    public const string ExternalPrefix = "external/";

    /// <summary>
    /// The principal of subject <paramref name="subject"/> of the external issuer: <c>external/{sub}</c>, never the same
    /// name as one of this server's own clients, whose principal is their bare client ID.
    /// </summary>
    public static string External(string subject) => ExternalPrefix + subject;

    /// <summary>
    /// The principal name grants and idempotency keys are held by: a client ID for tokens this server issued,
    /// <c>static-token</c>, <c>external/{sub}</c> for the external issuer's tokens, or <c>anonymous</c>.
    /// </summary>
    public static string Name(ClaimsPrincipal principal) =>
        principal.Identity?.IsAuthenticated == true
            ? principal.FindFirst(PrincipalClaim)?.Value ?? principal.FindFirst("sub")?.Value ?? principal.Identity.Name ?? "unknown"
            : "anonymous";

    /// <summary>Throw <see cref="ForbiddenException"/> unless the request's principal holds <paramref name="privilege"/> on the resource.</summary>
    public static async Task RequireAsync(this HttpContext http, CatalogPrivilege privilege, Namespace ns, string? name = null)
    {
        var resource = new CatalogResource(ns, name);
        var authorizer = http.RequestServices.GetRequiredService<IAuthorizer>();
        if (!await authorizer.IsAllowedAsync(http.User, privilege, resource, http.RequestAborted))
            throw new ForbiddenException($"{Name(http.User)} may not {privilege.ToString().ToLowerInvariant()} {resource}");
    }

    public static async Task<bool> AllowsAsync(this HttpContext http, CatalogPrivilege privilege, Namespace ns, string? name = null) =>
        await http.RequestServices.GetRequiredService<IAuthorizer>().IsAllowedAsync(http.User, privilege, new CatalogResource(ns, name), http.RequestAborted);
}
