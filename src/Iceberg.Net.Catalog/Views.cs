using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Catalog;

/// <summary>A loaded view: identity and its metadata.</summary>
public sealed record View(TableIdentifier Identifier, ViewMetadata Metadata)
{
    public string Sql(string dialect) => Metadata.CurrentVersion.For(dialect)?.Sql
        ?? throw new KeyNotFoundException($"View {Identifier} has no {dialect} representation (has: {string.Join(", ", Metadata.CurrentVersion.Representations.Select(r => r.Dialect))})");
}

/// <summary>What a view version says: its output schema and SQL per dialect, resolved against a default namespace.</summary>
public sealed record ViewDefinition(
    Schema Schema,
    IReadOnlyList<ViewRepresentation> Representations,
    IReadOnlyList<string> DefaultNamespace,
    string? DefaultCatalog = null,
    IReadOnlyDictionary<string, string>? Summary = null)
{
    public ViewDefinition(Schema schema, string sql, string dialect, IReadOnlyList<string> defaultNamespace)
        : this(schema, [new ViewRepresentation(sql, dialect)], defaultNamespace) { }

    internal ViewVersion ToVersion() => new(0, 0, 0,
        Summary ?? new Dictionary<string, string> { ["engine-name"] = "iceberg-net" }, Representations, DefaultNamespace, DefaultCatalog);
}

/// <summary>Catalogs that store Iceberg views (view spec; REST view endpoints).</summary>
public interface IViewCatalog
{
    Task<IReadOnlyList<TableIdentifier>> ListViewsAsync(Namespace ns, CancellationToken ct = default);
    Task<View> CreateViewAsync(TableIdentifier id, ViewDefinition definition, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default);
    Task<View> LoadViewAsync(TableIdentifier id, CancellationToken ct = default);
    Task<bool> ViewExistsAsync(TableIdentifier id, CancellationToken ct = default);
    /// <summary>Make <paramref name="definition"/> the current version, failing if the view was replaced by another UUID.</summary>
    Task<View> ReplaceViewAsync(TableIdentifier id, ViewDefinition definition, CancellationToken ct = default);
    Task DropViewAsync(TableIdentifier id, CancellationToken ct = default);
    Task RenameViewAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default);
}
