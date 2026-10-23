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
/// </summary>
public sealed class JdbcCatalog : MetastoreCatalog
{
    private readonly Func<DbConnection> _connect;
    private readonly string _warehouse;
    private readonly string _catalogName;

    public JdbcCatalog(Func<DbConnection> connect, string warehouse, IFileIO io, string name = "jdbc") : base(name, io)
    {
        _connect = connect;
        _warehouse = warehouse.TrimEnd('/');
        _catalogName = name;
    }

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
            """, [("@c", _catalogName)], ct);
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
        if ((await ListTablesAsync(ns, ct)).Count > 0) throw new InvalidOperationException($"Namespace {ns} is not empty");
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, "DELETE FROM iceberg_namespace_properties WHERE catalog_name = @c AND namespace = @n", [("@c", _catalogName), ("@n", Ns(ns))], ct);
    }

    public override async Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, "SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n",
            [("@c", _catalogName), ("@n", Ns(ns))], ct);
        return rows.Select(r => new TableIdentifier(ns, r[0]!)).ToList();
    }

    public override async Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        if (await CurrentMetadataLocationAsync(to, ct) is not null) throw new AlreadyExistsException($"Table {to} already exists");
        if (!await NamespaceExistsAsync(to.Namespace, ct)) throw new NoSuchNamespaceException($"Namespace {to.Namespace} does not exist");
        await using var c = await OpenAsync(ct);
        var n = await ExecuteAsync(c, """
            UPDATE iceberg_tables SET table_namespace = @tn, table_name = @tt
            WHERE catalog_name = @c AND table_namespace = @fn AND table_name = @ft
            """, [("@c", _catalogName), ("@fn", Ns(from.Namespace)), ("@ft", from.Name), ("@tn", Ns(to.Namespace)), ("@tt", to.Name)], ct);
        if (n == 0) throw new NoSuchTableException($"Table {from} does not exist");
    }

    protected override async Task<string?> CurrentMetadataLocationAsync(TableIdentifier id, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, "SELECT metadata_location FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t",
            [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct);
        return rows.Count == 0 ? null : rows[0][0];
    }

    protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expectedLocation, string newLocation, TableMetadata metadata, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        int changed;
        if (expectedLocation is null)
        {
            if (!await NamespaceExistsAsync(id.Namespace, ct)) throw new NoSuchNamespaceException($"Namespace {id.Namespace} does not exist");
            try
            {
                changed = await ExecuteAsync(c, """
                    INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)
                    VALUES (@c, @n, @t, @new, NULL)
                    """, [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name), ("@new", newLocation)], ct);
            }
            catch (DbException e)
            {
                if (await CurrentMetadataLocationAsync(id, ct) is null) throw;
                throw new CommitFailedException($"Table {id} was created concurrently", e); // primary key violation
            }
        }
        else
        {
            changed = await ExecuteAsync(c, """
                UPDATE iceberg_tables SET metadata_location = @new, previous_metadata_location = @old
                WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND metadata_location = @old
                """, [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name), ("@new", newLocation), ("@old", expectedLocation)], ct);
        }
        if (changed != 1) throw new CommitFailedException($"Table {id} metadata is no longer {expectedLocation ?? "(none)"}");
    }

    /// <summary>All pointer moves in one database transaction: any stale row rolls every move back.</summary>
    protected override async Task SwapMetadataLocationsAsync(IReadOnlyList<MetadataSwap> swaps, CancellationToken ct)
    {
        await using var c = await OpenAsync(ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        foreach (var swap in swaps)
        {
            var changed = await ExecuteAsync(c, """
                UPDATE iceberg_tables SET metadata_location = @new, previous_metadata_location = @old
                WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND metadata_location = @old
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
        await ExecuteAsync(c, "DELETE FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t",
            [("@c", _catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct);
    }

    protected override Task<string> DefaultTableLocationAsync(TableIdentifier id, CancellationToken ct) =>
        Task.FromResult($"{_warehouse}/{string.Join('/', id.Namespace.Levels)}.db/{id.Name}");
}
