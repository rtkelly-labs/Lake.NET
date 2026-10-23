using System.Data;
using System.Data.Common;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.Catalog;

/// <summary>
/// The JDBC/SQL catalog: Java's <c>JdbcCatalog</c> and PyIceberg's <c>SqlCatalog</c> tables, over any ADO.NET provider
/// (SQLite, PostgreSQL, SQL Server, MySQL), so the same database can be shared with Java and Python engines.
/// <code>
/// iceberg_tables(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)
/// iceberg_namespace_properties(catalog_name, namespace, property_key, property_value)
/// </code>
/// (the "V0" layout both default to). Commits are a compare-and-swap <c>UPDATE … WHERE metadata_location = @expected</c>;
/// a namespace exists if it has a property row (<c>exists=true</c> is written on create) or a table. Namespaces are
/// stored dot-joined.
/// <para>
/// With <c>views: true</c> the catalog uses Java's "V1" layout (<c>jdbc.schema-version=V1</c>): an extra
/// <c>iceberg_type</c> column marks rows <c>TABLE</c> or <c>VIEW</c> (NULL is a table, so V0 rows and writers that
/// do not know the column — PyIceberg — keep working). <see cref="InitializeAsync"/> adds the column to an existing V0
/// database. PyIceberg's <c>list_tables</c> does not filter on it and would list views as tables.
/// </para>
/// </summary>
public sealed class JdbcCatalog : MetastoreCatalog, IViewCatalog
{
    private const string TableRows = "(iceberg_type = 'TABLE' OR iceberg_type IS NULL)";

    private readonly Func<DbConnection> _connect;
    private readonly string _warehouse;
    private readonly string _catalogName;
    private readonly bool _views;

    public JdbcCatalog(Func<DbConnection> connect, string warehouse, IFileIO io, string name = "jdbc", bool views = false) : base(name, io)
    {
        _connect = connect;
        _warehouse = warehouse.TrimEnd('/');
        _catalogName = name;
        _views = views;
    }

    /// <summary>The row filter selecting tables: every row in V0, non-view rows in V1.</summary>
    private string Tables => _views ? $" AND {TableRows}" : "";

    /// <summary>Create the catalog tables if they do not exist (as Java and PyIceberg do on first use).</summary>
    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, """
            CREATE TABLE IF NOT EXISTS iceberg_tables (
              catalog_name VARCHAR(255) NOT NULL,
              table_namespace VARCHAR(255) NOT NULL,
              table_name VARCHAR(255) NOT NULL,
              metadata_location VARCHAR(1000),
              previous_metadata_location VARCHAR(1000),
              PRIMARY KEY (catalog_name, table_namespace, table_name))
            """, [], ct);
        await ExecuteAsync(c, """
            CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
              catalog_name VARCHAR(255) NOT NULL,
              namespace VARCHAR(255) NOT NULL,
              property_key VARCHAR(255),
              property_value VARCHAR(1000),
              PRIMARY KEY (catalog_name, namespace, property_key))
            """, [], ct);
        if (!_views) return;
        try
        {
            await QueryAsync(c, "SELECT iceberg_type FROM iceberg_tables WHERE 1 = 0", [], ct);
        }
        catch (DbException)
        {
            await ExecuteAsync(c, "ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(5)", [], ct); // Java's V0 → V1 migration
        }
    }

    private async Task<DbConnection> OpenAsync(CancellationToken ct)
    {
        var c = _connect();
        if (c.State != ConnectionState.Open) await c.OpenAsync(ct);
        return c;
    }

    private static DbCommand Command(DbConnection c, string sql, (string Name, object? Value)[] args, DbTransaction? tx = null)
    {
        var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        cmd.Transaction = tx;
        foreach (var (n, v) in args)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = n;
            p.Value = v ?? DBNull.Value;
            cmd.Parameters.Add(p);
        }
        return cmd;
    }

    private static async Task<int> ExecuteAsync(DbConnection c, string sql, (string, object?)[] args, CancellationToken ct, DbTransaction? tx = null)
    {
        await using var cmd = Command(c, sql, args, tx);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<string?[]>> QueryAsync(DbConnection c, string sql, (string, object?)[] args, CancellationToken ct)
    {
        await using var cmd = Command(c, sql, args);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var rows = new List<string?[]>();
        while (await r.ReadAsync(ct))
        {
            var row = new string?[r.FieldCount];
            for (var i = 0; i < row.Length; i++) row[i] = r.IsDBNull(i) ? null : r.GetString(i);
            rows.Add(row);
        }
        return rows;
    }

    private static string Ns(Namespace ns) => string.Join('.', ns.Levels);

    public override async Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default)
    {
        if (await NamespaceExistsAsync(ns, ct)) throw new AlreadyExistsException($"Namespace {ns} already exists");
        await using var c = await OpenAsync(ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        var props = new Dictionary<string, string> { ["exists"] = "true" };
        foreach (var (k, v) in properties ?? new Dictionary<string, string>()) props[k] = v;
        foreach (var (k, v) in props)
            await ExecuteAsync(c, "INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (@c, @n, @k, @v)",
                [("@c", _catalogName), ("@n", Ns(ns)), ("@k", k), ("@v", v)], ct, tx);
        await tx.CommitAsync(ct);
    }

    public override async Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var all = await QueryAsync(c, """
            SELECT namespace FROM iceberg_namespace_properties WHERE catalog_name = @c
            UNION SELECT table_namespace FROM iceberg_tables WHERE catalog_name = @c
            """, [("@c", _catalogName)], ct); // views keep their namespace alive too, as in Java
        var depth = parent?.Levels.Count ?? 0;
        return all.Select(r => Namespace.Of(r[0]!.Split('.')))
            .SelectMany(n => Enumerable.Range(1, n.Levels.Count).Select(k => Namespace.Of([.. n.Levels.Take(k)]))) // implied parents
            .Where(n => n.Levels.Count == depth + 1 && (parent is null || n.Levels.Take(depth).SequenceEqual(parent.Levels)))
            .Distinct()
            .ToList();
    }

    public override async Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, """
            SELECT namespace FROM iceberg_namespace_properties WHERE catalog_name = @c AND (namespace = @n OR namespace LIKE @prefix ESCAPE '\')
            UNION SELECT table_namespace FROM iceberg_tables WHERE catalog_name = @c AND (table_namespace = @n OR table_namespace LIKE @prefix ESCAPE '\')
            """, [("@c", _catalogName), ("@n", Ns(ns)), ("@prefix", Ns(ns).Replace("%", "\\%").Replace("_", "\\_") + ".%")], ct);
        return rows.Count > 0;
    }

    public override async Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        if ((await ListTablesAsync(ns, ct)).Count > 0 || _views && (await ListViewsAsync(ns, ct)).Count > 0)
            throw new InvalidOperationException($"Namespace {ns} is not empty");
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, "DELETE FROM iceberg_namespace_properties WHERE catalog_name = @c AND namespace = @n", [("@c", _catalogName), ("@n", Ns(ns))], ct);
    }

    public override async Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, $"SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n{Tables}",
            [("@c", _catalogName), ("@n", Ns(ns))], ct);
        return rows.Select(r => new TableIdentifier(ns, r[0]!)).ToList();
    }

    public override Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default) => RenameAsync(from, to, Tables, "Table", ct);

    private async Task RenameAsync(TableIdentifier from, TableIdentifier to, string kindFilter, string kind, CancellationToken ct)
    {
        if (await EntryExistsAsync(to, ct)) throw new AlreadyExistsException($"{to} already exists");
        if (!await NamespaceExistsAsync(to.Namespace, ct)) throw new NoSuchNamespaceException($"Namespace {to.Namespace} does not exist");
        await using var c = await OpenAsync(ct);
        var n = await ExecuteAsync(c, $"""
            UPDATE iceberg_tables SET table_namespace = @tn, table_name = @tt
            WHERE catalog_name = @c AND table_namespace = @fn AND table_name = @ft{kindFilter}
            """, [("@c", _catalogName), ("@fn", Ns(from.Namespace)), ("@ft", from.Name), ("@tn", Ns(to.Namespace)), ("@tt", to.Name)], ct);
        if (n == 0) throw new NoSuchTableException($"{kind} {from} does not exist");
    }

    /// <summary>Whether any row (table or view) holds the name: both share the primary key.</summary>
    private async Task<bool> EntryExistsAsync(TableIdentifier id, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        return (await QueryAsync(c, "SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t",
            [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct)).Count > 0;
    }

    protected override Task<string?> CurrentMetadataLocationAsync(TableIdentifier id, CancellationToken ct) => LocationAsync(id, Tables, ct);

    private async Task<string?> LocationAsync(TableIdentifier id, string kindFilter, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, $"SELECT metadata_location FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t{kindFilter}",
            [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct);
        return rows.Count == 0 ? null : rows[0][0];
    }

    protected override Task SwapMetadataLocationAsync(TableIdentifier id, string? expectedLocation, string newLocation, TableMetadata metadata, CancellationToken ct) =>
        SwapAsync(id, expectedLocation, newLocation, view: false, ct);

    private async Task SwapAsync(TableIdentifier id, string? expectedLocation, string newLocation, bool view, CancellationToken ct)
    {
        var kind = view ? "View" : "Table";
        await using var c = await OpenAsync(ct);
        int changed;
        if (expectedLocation is null)
        {
            if (!await NamespaceExistsAsync(id.Namespace, ct)) throw new NoSuchNamespaceException($"Namespace {id.Namespace} does not exist");
            try
            {
                changed = _views
                    ? await ExecuteAsync(c, """
                        INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type)
                        VALUES (@c, @n, @t, @new, NULL, @type)
                        """, [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name), ("@new", newLocation), ("@type", view ? "VIEW" : "TABLE")], ct)
                    : await ExecuteAsync(c, """
                        INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)
                        VALUES (@c, @n, @t, @new, NULL)
                        """, [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name), ("@new", newLocation)], ct);
            }
            catch (DbException e)
            {
                if (!await EntryExistsAsync(id, ct)) throw;
                throw new CommitFailedException($"{id} was created concurrently", e); // primary key violation: a table or view took the name
            }
        }
        else
        {
            changed = await ExecuteAsync(c, $"""
                UPDATE iceberg_tables SET metadata_location = @new, previous_metadata_location = @old
                WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND metadata_location = @old{(view ? " AND iceberg_type = 'VIEW'" : Tables)}
                """, [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name), ("@new", newLocation), ("@old", expectedLocation)], ct);
        }
        if (changed != 1) throw new CommitFailedException($"{kind} {id} metadata is no longer {expectedLocation ?? "(none)"}");
    }

    /// <summary>All pointer moves in one database transaction: any stale row rolls every move back.</summary>
    protected override async Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        foreach (var swap in swaps)
        {
            var changed = await ExecuteAsync(c, $"""
                UPDATE iceberg_tables SET metadata_location = @new, previous_metadata_location = @old
                WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND metadata_location = @old{Tables}
                """, [("@c", _catalogName), ("@n", Ns(swap.Id.Namespace)), ("@t", swap.Id.Name), ("@new", swap.NewLocation), ("@old", swap.ExpectedLocation)], ct, tx);
            if (changed != 1)
            {
                await tx.RollbackAsync(ct);
                throw new CommitFailedException($"Table {swap.Id} changed concurrently");
            }
        }
        await tx.CommitAsync(ct);
    }

    protected override async Task RemoveTableEntryAsync(TableIdentifier id, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, $"DELETE FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t{Tables}",
            [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct);
    }

    private const string ViewRows = " AND iceberg_type = 'VIEW'";

    private void RequireViews()
    {
        if (!_views) throw new NotSupportedException("Views need the V1 catalog layout: construct the JdbcCatalog with views: true");
    }

    public async Task<IReadOnlyList<TableIdentifier>> ListViewsAsync(Namespace ns, CancellationToken ct = default)
    {
        RequireViews();
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, $"SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n{ViewRows}",
            [("@c", _catalogName), ("@n", Ns(ns))], ct);
        return rows.Select(r => new TableIdentifier(ns, r[0]!)).ToList();
    }

    public async Task<View> CreateViewAsync(TableIdentifier id, ViewDefinition definition, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default)
    {
        RequireViews();
        if (!await NamespaceExistsAsync(id.Namespace, ct)) throw new NoSuchNamespaceException($"Namespace {id.Namespace} does not exist");
        if (await EntryExistsAsync(id, ct)) throw new AlreadyExistsException($"{id} already exists");
        var metadata = ViewMetadata.Create(await DefaultTableLocationAsync(id, ct), definition.Schema, definition.ToVersion(), properties, Clock());
        return await WriteViewAsync(id, null, metadata, ct);
    }

    public async Task<View> LoadViewAsync(TableIdentifier id, CancellationToken ct = default)
    {
        RequireViews();
        var location = await LocationAsync(id, ViewRows, ct) ?? throw new NoSuchTableException($"View {id} does not exist");
        return new View(id, await ViewFiles.ReadAsync(FileIO, location, ct));
    }

    public async Task<bool> ViewExistsAsync(TableIdentifier id, CancellationToken ct = default)
    {
        RequireViews();
        return await LocationAsync(id, ViewRows, ct) is not null;
    }

    public async Task<View> ReplaceViewAsync(TableIdentifier id, ViewDefinition definition, CancellationToken ct = default)
    {
        var current = await LoadViewAsync(id, ct);
        return await WriteViewAsync(id, current.Metadata.MetadataFileLocation, current.Metadata.Replace(definition.Schema, definition.ToVersion(), Clock()), ct);
    }

    public async Task DropViewAsync(TableIdentifier id, CancellationToken ct = default)
    {
        RequireViews();
        await using var c = await OpenAsync(ct);
        var n = await ExecuteAsync(c, $"DELETE FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t{ViewRows}",
            [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct);
        if (n == 0) throw new NoSuchTableException($"View {id} does not exist");
    }

    public Task RenameViewAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        RequireViews();
        return RenameAsync(from, to, ViewRows, "View", ct);
    }

    private async Task<View> WriteViewAsync(TableIdentifier id, string? expected, ViewMetadata metadata, CancellationToken ct)
    {
        var location = await ViewFiles.WriteNextAsync(FileIO, metadata, expected, ct);
        await SwapAsync(id, expected, location, view: true, ct);
        return new View(id, metadata with { MetadataFileLocation = location });
    }

    protected override Task<string> DefaultTableLocationAsync(TableIdentifier id, CancellationToken ct) =>
        Task.FromResult($"{_warehouse}/{string.Join('/', id.Namespace.Levels)}.db/{id.Name}");
}
