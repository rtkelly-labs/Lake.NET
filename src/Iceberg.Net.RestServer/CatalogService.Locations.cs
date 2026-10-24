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
    /// <summary>A table or view name becomes a directory under its namespace: it must not reshape the path.</summary>
    private static void RequireEntryName(string name, string what)
    {
        if (string.IsNullOrEmpty(name)) throw new BadRequestException($"{what} name must not be empty");
        if (name is "." or ".." || name.Any(c => c is '/' or '\\' || char.IsControl(c)))
            throw new BadRequestException($"{what} name '{name}' cannot be used in a storage path");
    }

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
        var root = Root(location);
        if (!AllowedRoots().Any(r => root.Length > r.Length && root.StartsWith(r, StringComparison.Ordinal)))
            throw new BadRequestException($"{what} {location} is outside the storage this server allows (Iceberg:Warehouse, Iceberg:Locations:Allowed)");
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

    /// <summary>The directories of every table and view in <paramref name="ns"/>, except <paramref name="self"/>.</summary>
    private async Task<List<(string What, string Location)>> OccupiedAsync(Namespace ns, (TableIdentifier Id, CatalogEntryKind Kind)? self, CancellationToken ct)
    {
        var occupied = new List<(string, string)>();
        foreach (var name in await Store.ListAsync(ns, CatalogEntryKind.Table, ct))
        {
            var id = new TableIdentifier(ns, name);
            if (self is { Kind: CatalogEntryKind.Table } s && s.Id == id) continue;
            if (await Store.GetAsync(id, CatalogEntryKind.Table, ct) is not { } pointer) continue;
            occupied.AddRange(TableDirectories(await ReadMetadataAsync(pointer, ct)).Select(l => ($"table {id}", l)));
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
        foreach (var (other, location) in await OccupiedAsync(id.Namespace, (id, kind), ct))
            if (directories.FirstOrDefault(d => Overlaps(d, location)) is { } clash)
                throw new BadRequestException($"{what} location {clash} overlaps {other} at {location}");
        foreach (var child in await Store.ListNamespacesAsync(id.Namespace, ct))
            if (NamespaceRoot(child, await LoadNamespaceAsync(child, ct)) is { } childRoot && directories.FirstOrDefault(d => Overlaps(d, childRoot)) is { } clash)
                throw new BadRequestException($"{what} location {clash} overlaps namespace {child} at {childRoot}");
    }

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
                throw new BadRequestException($"Namespace {ns} location {root} overlaps namespace {other} at {otherRoot}");
        }
        for (var depth = ns.Levels.Count - 1; depth >= 1; depth--)
        {
            var ancestor = Namespace.Of([.. ns.Levels.Take(depth)]);
            if (await Store.LoadNamespaceAsync(ancestor, ct) is null) continue;
            foreach (var (what, location) in await OccupiedAsync(ancestor, null, ct))
                if (Overlaps(root, location)) throw new BadRequestException($"Namespace {ns} location {root} overlaps {what} at {location}");
        }
    }
}
