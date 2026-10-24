using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestServer;

/// <summary>
/// Vends Azure (ADLS Gen2 / Blob) SAS tokens for tables at <c>abfs[s]://{container}@{account}.{host}/{path}</c> or
/// <c>wasb[s]://…</c> locations, signed with the storage account's shared key. A service SAS: read and list for readers,
/// plus add, create, write and delete for writers, expiring after <see cref="AdlsCredentialVendingOptions.DurationSeconds"/>.
/// <list type="bullet">
/// <item><c>directory</c> scope (default): a directory SAS (<c>sr=d</c>) on the table's path. Needs a hierarchical
/// namespace (ADLS Gen2) account; Azurite does not support it.</item>
/// <item><c>container</c> scope: a container SAS (<c>sr=c</c>), for flat-namespace accounts. It reaches every table in the
/// container: use one container per trust boundary.</item>
/// </list>
/// Returned as Iceberg's <c>adls.sas-token.{account host}</c> and <c>adls.sas-token-expires-at-ms.{account host}</c>,
/// which Iceberg Java's ADLSFileIO and PyIceberg read. Accounts without a configured key, and other schemes, get none.
/// Tokens are cached per (location, access) until a quarter of their lifetime remains.
/// </summary>
public sealed class AdlsSasCredentialVendor(AdlsCredentialVendingOptions options, Func<DateTimeOffset>? clock = null) : IStorageCredentialVendor
{
    private readonly Func<DateTimeOffset> _clock = clock ?? (() => DateTimeOffset.UtcNow);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<(string Location, bool Write), VendedCredential> _cache = new();

    public const string ReadPermissions = "rl";
    public const string WritePermissions = "racwdl";

    public Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default)
    {
        if (AzureLocation(location) is not var (container, accountHost, path)) return Task.FromResult<IReadOnlyList<VendedCredential>>([]);
        var account = accountHost.Split('.')[0];
        if (!options.Accounts.TryGetValue(account, out var key)) return Task.FromResult<IReadOnlyList<VendedCredential>>([]);
        var prefix = location.TrimEnd('/') + "/";
        var now = _clock();
        if (_cache.TryGetValue((prefix, write), out var cached) && cached.Expiration is { } exp && exp - now > TimeSpan.FromSeconds(options.DurationSeconds / 4.0))
            return Task.FromResult<IReadOnlyList<VendedCredential>>([cached]);

        // Whole seconds: the SAS carries no fractions.
        var expiry = DateTimeOffset.FromUnixTimeSeconds((now + TimeSpan.FromSeconds(options.DurationSeconds)).ToUnixTimeSeconds());
        var permissions = write ? WritePermissions : ReadPermissions;
        var directory = path.Trim('/');
        var sas = options.Scope.ToLowerInvariant() switch
        {
            "container" => Sas(account, key, container, null, permissions, expiry, options.Version, options.Protocol),
            "directory" => Sas(account, key, container, directory, permissions, expiry, options.Version, options.Protocol),
            var s => throw new InvalidOperationException($"CredentialVending:Adls:Scope must be 'directory' or 'container', not '{s}'"),
        };
        var vended = new VendedCredential(prefix, new Dictionary<string, string>
        {
            [$"adls.sas-token.{accountHost}"] = sas,
            [$"adls.sas-token-expires-at-ms.{accountHost}"] = expiry.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
        }, expiry);
        _cache[(prefix, write)] = vended;
        return Task.FromResult<IReadOnlyList<VendedCredential>>([vended]);
    }

    /// <summary>
    /// A service SAS query string (without the leading <c>?</c>) signed with the account key: for the container, or (with
    /// <paramref name="directory"/>) a directory SAS on that path. The string to sign follows the service SAS layout of
    /// version 2020-12-06 and later.
    /// </summary>
    public static string Sas(string account, string base64Key, string container, string? directory, string permissions, DateTimeOffset expiry, string version,
        string? protocol = null)
    {
        var se = expiry.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var resource = directory is null ? "c" : "d";
        var canonical = directory is null ? $"/blob/{account}/{container}" : $"/blob/{account}/{container}/{directory}";
        var toSign = string.Join('\n',
            permissions, "", se, canonical, "", "", protocol ?? "", version, resource, "", "", "", "", "", "", "");
        var signature = Convert.ToBase64String(HMACSHA256.HashData(Convert.FromBase64String(base64Key), Encoding.UTF8.GetBytes(toSign)));
        var query = new List<(string, string)> { ("sv", version), ("sr", resource), ("sp", permissions), ("se", se) };
        if (directory is not null) query.Add(("sdd", directory.Length == 0 ? "0" : directory.Split('/').Length.ToString(CultureInfo.InvariantCulture)));
        if (protocol is not null) query.Add(("spr", protocol));
        query.Add(("sig", signature));
        return string.Join('&', query.Select(q => $"{q.Item1}={Uri.EscapeDataString(q.Item2)}"));
    }

    /// <summary>(container, account host, path) of an <c>abfs[s]://</c> or <c>wasb[s]://</c> location; null otherwise.</summary>
    public static (string Container, string AccountHost, string Path)? AzureLocation(string location)
    {
        var scheme = location.IndexOf("://", StringComparison.Ordinal);
        if (scheme < 0 || location[..scheme] is not ("abfs" or "abfss" or "wasb" or "wasbs")) return null;
        var rest = location[(scheme + 3)..];
        var slash = rest.IndexOf('/');
        var authority = slash < 0 ? rest : rest[..slash];
        var at = authority.IndexOf('@');
        if (at <= 0 || at == authority.Length - 1) return null;
        return (authority[..at], authority[(at + 1)..], slash < 0 ? "" : rest[(slash + 1)..]);
    }
}

/// <summary>Several vendors, one per kind of storage: every credential any of them vends for the location.</summary>
public sealed class CompositeCredentialVendor(IReadOnlyList<IStorageCredentialVendor> vendors) : IStorageCredentialVendor
{
    public async Task<IReadOnlyList<VendedCredential>> VendAsync(TableIdentifier table, string location, bool write, CancellationToken ct = default)
    {
        var all = new List<VendedCredential>();
        foreach (var vendor in vendors) all.AddRange(await vendor.VendAsync(table, location, write, ct));
        return all;
    }
}
