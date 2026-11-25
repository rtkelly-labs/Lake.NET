using Microsoft.Extensions.Logging;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.RestServer;

/// <summary>
/// Where namespaces, tables and views may put their files. The server reads and writes metadata, vends credentials,
/// signs storage requests and purges with its own access, all scoped by these locations, so a location a client
/// chooses is held to three rules (docs/rest-server.md, Storage locations):
/// <list type="number">
/// <item>it is strictly below an allowed root (<see cref="RestServerOptions.Warehouse"/>, <see cref="LocationOptions.Allowed"/>), without dot segments;</item>
/// <item>a table's or view's locations are below its namespace's root, and a namespace's root does not overlap another
/// namespace's except along the hierarchy;</item>
/// <item>no two tables or views, and no table and child namespace, share any directory.</item>
/// </list>
/// </summary>
public sealed partial class CatalogService
{
    /// <summary>
    /// Characters with a meaning of their own in the policies that scope vended credentials and signed requests: IAM
    /// <c>Resource</c> and <c>StringLike</c> wildcards (<c>*</c>, <c>?</c>) and policy variables (<c>${…}</c>). A name or
    /// location carrying one would widen a policy built from it to other tables' files, so none may.
    /// </summary>
    internal const string PolicyMetacharacters = "*?$";

    private static bool HasPolicyMetacharacter(string s) => s.AsSpan().IndexOfAny(PolicyMetacharacters) >= 0;

    /// <summary>A table or view name becomes a directory under its namespace: it must not reshape the path.</summary>
    private static void RequireEntryName(string name, string what)
    {
        if (string.IsNullOrEmpty(name)) throw new BadRequestException($"{what} name must not be empty");
        if (!IsPathName(name)) throw new BadRequestException($"{what} name '{name}' cannot be used in a storage path");
    }

    /// <summary>Every namespace level becomes a directory too (<c>{warehouse}/{levels…}</c>): the same rules as a name.</summary>
    private static void RequireNamespaceLevels(Namespace ns)
    {
        if (ns.Levels.Count == 0 || ns.Levels.Any(l => l.Length == 0)) throw new BadRequestException("Namespace must have at least one non-empty level");
        foreach (var level in ns.Levels)
            if (!IsPathName(level)) throw new BadRequestException($"Namespace level '{level}' cannot be used in a storage path");
    }

    private static bool IsPathName(string name) =>
        name is not ("." or "..") && !name.Any(c => c is '/' or '\\' || char.IsControl(c)) && !HasPolicyMetacharacter(name);

    private static string Root(string location) => TableLocations.Normalise(location).TrimEnd('/') + "/";

    private static bool Overlaps(string a, string b)
    {
        var (x, y) = (Root(a), Root(b));
        return x.StartsWith(y, StringComparison.Ordinal) || y.StartsWith(x, StringComparison.Ordinal);
    }

    private IReadOnlyList<string> AllowedRoots() =>
        new[] { options.Warehouse }.Concat(options.Locations.Allowed).Where(r => !string.IsNullOrEmpty(r)).Select(Root).Distinct(StringComparer.Ordinal).ToList();

    private void RequireAllowed(string location, string what)
    {
        try
        {
            TableLocations.RequireSafeRoot(location);
        }
        catch (ArgumentException e)
        {
            throw new BadRequestException($"{what}: {e.Message}");
        }
        if (HasPolicyMetacharacter(location))
            throw new BadRequestException($"{what} {location} contains '*', '?' or '$', which credential and signing policies read as wildcards");
        // A local file system reads a//b as a/b, so two spellings would name one directory: one spelling only.
        if (PathPart(TableLocations.Normalise(location).TrimEnd('/')).Contains("//", StringComparison.Ordinal))
            throw new BadRequestException($"{what} {location} has an empty path segment");
        var root = Root(location);
        if (!AllowedRoots().Any(r => root.Length > r.Length && root.StartsWith(r, StringComparison.Ordinal)))
            throw new BadRequestException($"{what} {location} is outside the storage this server allows (Iceberg:Warehouse, Iceberg:Locations:Allowed)");
    }

    /// <summary>The part after <c>scheme://authority</c>, or the whole of a local path.</summary>
    private static string PathPart(string normalised)
    {
        var scheme = normalised.IndexOf("://", StringComparison.Ordinal);
        if (scheme < 0) return normalised;
        var slash = normalised.IndexOf('/', scheme + 3);
        return slash < 0 ? "" : normalised[slash..];
    }

    /// <summary>A namespace's root: its <c>location</c> property, else <c>{warehouse}/{levels}</c>; null without either.</summary>
    private string? NamespaceRoot(Namespace ns, IReadOnlyDictionary<string, string> properties) =>
        properties.TryGetValue("location", out var l) && l.Length > 0 ? l
        : string.IsNullOrEmpty(options.Warehouse) ? null
        : $"{options.Warehouse.TrimEnd('/')}/{string.Join('/', ns.Levels)}";

    private static bool IsAncestor(Namespace ancestor, Namespace ns) =>
        ancestor.Levels.Count < ns.Levels.Count && ancestor.Levels.SequenceEqual(ns.Levels.Take(ancestor.Levels.Count));

    private async Task<List<Namespace>> AllNamespacesAsync(CancellationToken ct)
    {
        var all = new List<Namespace>();
        var queue = new Queue<Namespace?>([null]);
        while (queue.Count > 0)
            foreach (var child in await Store.ListNamespacesAsync(queue.Dequeue(), ct))
            {
                all.Add(child);
                queue.Enqueue(child);
            }
        return all;
    }

    /// <summary>The directories a table writes to: its location and the <c>write.*.path</c> properties.</summary>
    private static IEnumerable<string> TableDirectories(TableMetadata m) =>
        new[] { m.Location }.Concat(TableLocations.PathProperties.Select(p => m.Properties.GetValueOrDefault(p))).OfType<string>().Where(l => l.Length > 0);

    /// <summary>
    /// The directories of every table and view in any namespace (a rename keeps a table's directories, so its current
    /// namespace says nothing about where its files are), except <paramref name="self"/> and the entries of
    /// <paramref name="skip"/>'s namespaces.
    /// </summary>
    private async Task<List<(string What, string Location)>> OccupiedAnywhereAsync((TableIdentifier Id, CatalogEntryKind Kind)? self, Func<Namespace, bool> skip, CancellationToken ct)
    {
        var occupied = new List<(string, string)>();
        foreach (var ns in await AllNamespacesAsync(ct))
            if (!skip(ns)) occupied.AddRange(await OccupiedAsync(ns, self, ct));
        return occupied;
    }

    /// <summary>
    /// Table directories by metadata file. A metadata file is never rewritten, so its directories never change: caching
    /// them makes the overlap check of a create read only the tables committed since the last check, not every table's
    /// metadata every time. Bounded: past <see cref="MaxCachedDirectories"/> entries the cache starts again.
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, IReadOnlyList<string>> _directoriesByMetadata = new(StringComparer.Ordinal);

    internal const int MaxCachedDirectories = 100_000;

    private async Task<IReadOnlyList<string>> DirectoriesOfAsync(string metadataLocation, CancellationToken ct)
    {
        if (_directoriesByMetadata.TryGetValue(metadataLocation, out var cached)) return cached;
        var directories = TableDirectories(await ReadMetadataAsync(metadataLocation, ct)).ToList();
        if (_directoriesByMetadata.Count >= MaxCachedDirectories) _directoriesByMetadata.Clear();
        _directoriesByMetadata[metadataLocation] = directories;
        return directories;
    }

    /// <summary>The directories of every table and view in <paramref name="ns"/>, except <paramref name="self"/>.</summary>
    private async Task<List<(string What, string Location)>> OccupiedAsync(Namespace ns, (TableIdentifier Id, CatalogEntryKind Kind)? self, CancellationToken ct)
    {
        var occupied = new List<(string, string)>();
        foreach (var name in await Store.ListAsync(ns, CatalogEntryKind.Table, ct))
        {
            var id = new TableIdentifier(ns, name);
            if (self is { Kind: CatalogEntryKind.Table } s && s.Id == id) continue;
            if (await Store.GetAsync(id, CatalogEntryKind.Table, ct) is not { } pointer) continue;
            occupied.AddRange((await DirectoriesOfAsync(pointer, ct)).Select(l => ($"table {id}", l)));
        }
        foreach (var name in await Store.ListAsync(ns, CatalogEntryKind.View, ct))
        {
            var id = new TableIdentifier(ns, name);
            if (self is { Kind: CatalogEntryKind.View } s && s.Id == id) continue;
            occupied.Add(($"view {id}", (await LoadViewAsync(id, ct)).Metadata.Location));
        }
        return occupied;
    }

    /// <summary>Rules 1–3 for the directories of table or view <paramref name="id"/>.</summary>
    private async Task RequireEntryLocationsAsync(TableIdentifier id, CatalogEntryKind kind, IReadOnlyCollection<string> directories, CancellationToken ct)
    {
        var what = kind == CatalogEntryKind.View ? $"View {id}" : $"Table {id}";
        var nsRoot = NamespaceRoot(id.Namespace, await LoadNamespaceAsync(id.Namespace, ct))
            ?? throw new BadRequestException("The server has no warehouse configured and the namespace has no location");
        foreach (var location in directories)
        {
            RequireAllowed(location, $"{what} location");
            var root = Root(location);
            if (!(root.Length > Root(nsRoot).Length && root.StartsWith(Root(nsRoot), StringComparison.Ordinal)))
                throw new BadRequestException($"{what} location {location} is outside its namespace's location {nsRoot}");
        }
        foreach (var (other, location) in await OccupiedAnywhereAsync((id, kind), _ => false, ct))
            if (directories.FirstOrDefault(d => Overlaps(d, location)) is { } clash)
                throw Overlap(what, clash, other, location);
        foreach (var child in await Store.ListNamespacesAsync(id.Namespace, ct))
            // An implied namespace (a.b when only a and a.b.c were created) has no properties, so its root is the default.
            if (NamespaceRoot(child, await Store.LoadNamespaceAsync(child, ct) ?? new Dictionary<string, string>()) is { } childRoot && directories.FirstOrDefault(d => Overlaps(d, childRoot)) is { } clash)
                throw Overlap(what, clash, $"namespace {child}", childRoot);
    }

    /// <summary>
    /// A location clashes with another entry's. The client hears only that it does: the other entry may be in a
    /// namespace it cannot read, so its name and location go to the server log, not the response.
    /// </summary>
    private BadRequestException Overlap(string what, string location, string other, string otherLocation)
    {
        Logger?.LogInformation("Refused {What} location {Location}: it overlaps {Other} at {OtherLocation}", what, location, other, otherLocation);
        return new BadRequestException($"{what} location {location} overlaps the location of another table, view or namespace");
    }

    /// <summary>
    /// Rule 3 within one transaction: the store holds none of its changes until they all swap in, so the tables whose
    /// directories it creates or moves are checked against every other table in it.
    /// </summary>
    private static void RequireDisjointInTransaction(IReadOnlyList<(TableIdentifier Id, TableMetadata Metadata, bool Moved)> tables)
    {
        for (var i = 0; i < tables.Count; i++)
            for (var j = i + 1; j < tables.Count; j++)
            {
                if (!tables[i].Moved && !tables[j].Moved) continue;
                foreach (var a in TableDirectories(tables[i].Metadata))
                    if (TableDirectories(tables[j].Metadata).FirstOrDefault(b => Overlaps(a, b)) is { } b)
                        throw new BadRequestException($"Table {tables[i].Id} location {a} overlaps table {tables[j].Id} at {b} in the same transaction");
            }
    }

    /// <summary>
    /// The files a table's metadata points the server at, besides its directories: manifest lists (and a v1 snapshot's
    /// embedded manifests), statistics and partition statistics. Scan planning reads them with the server's access.
    /// </summary>
    private static IEnumerable<(string What, string Path)> ReferencedFiles(TableMetadata m) =>
        m.Snapshots.SelectMany(s => (s.ManifestList is { } l ? [("manifest list", l)] : Array.Empty<(string, string)>())
                .Concat((s.Manifests ?? []).Select(f => ("manifest", f))))
            .Concat(m.Statistics.Select(s => ("statistics file", s.StatisticsPath)))
            .Concat(m.PartitionStatistics.Select(s => ("partition statistics file", s.StatisticsPath)));

    /// <summary>
    /// Every file <paramref name="after"/> references that <paramref name="before"/> did not (all of them for a create or
    /// a register) lies inside the table's own directories: otherwise a client could point the server, which reads
    /// these with its own access, at another table's manifests or statistics.
    /// </summary>
    private static void RequireOwnFiles(TableIdentifier id, TableMetadata? before, TableMetadata after)
    {
        var known = before is null ? [] : ReferencedFiles(before).Select(f => f.Path).ToHashSet(StringComparer.Ordinal);
        var prefixes = TableLocations.Prefixes(after);
        foreach (var (what, path) in ReferencedFiles(after))
            if (!known.Contains(path) && !IsOwnFile(path, prefixes))
                throw new BadRequestException($"Table {id} {what} {path} is outside the table's directories");
    }

    /// <summary>
    /// Strictly below one of <paramref name="prefixes"/> in the one spelling <see cref="TableLocations"/> accepts, and
    /// without dot segments even once percent-decoded (a store or proxy that decodes <c>%2e%2e</c> would climb out).
    /// </summary>
    internal static bool IsOwnFile(string path, IReadOnlyList<string> prefixes) =>
        TableLocations.IsUnder(path, prefixes)
        && !PathPart(Uri.UnescapeDataString(TableLocations.Normalise(path))).Split('/', '\\').Any(s => s is "." or "..");

    private Task RequireTableLocationsAsync(TableIdentifier id, TableMetadata metadata, CancellationToken ct) =>
        RequireEntryLocationsAsync(id, CatalogEntryKind.Table, TableDirectories(metadata).ToList(), ct);

    /// <summary>A namespace whose root is <paramref name="root"/>: rules 1 and 2, and clear of its ancestors' tables and views.</summary>
    private async Task RequireNamespaceRootAsync(Namespace ns, string? root, bool explicitLocation, CancellationToken ct)
    {
        if (root is null) return;
        if (explicitLocation) RequireAllowed(root, $"Namespace {ns} location");
        foreach (var other in await AllNamespacesAsync(ct))
        {
            if (other == ns || IsAncestor(other, ns) || IsAncestor(ns, other)) continue;
            if (NamespaceRoot(other, await Store.LoadNamespaceAsync(other, ct) ?? new Dictionary<string, string>()) is { } otherRoot && Overlaps(root, otherRoot))
                throw Overlap($"Namespace {ns}", root, $"namespace {other}", otherRoot);
        }
        // Its own and its descendants' entries belong under it; anyone else's, a renamed table's included, must not.
        foreach (var (what, location) in await OccupiedAnywhereAsync(null, other => other == ns || IsAncestor(ns, other), ct))
            if (Overlaps(root, location)) throw Overlap($"Namespace {ns}", root, what, location);
    }
}
