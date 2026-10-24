using System.Data;
using System.Data.Common;
using Iceberg.Net.Catalog;
using Iceberg.Net.Metadata;

namespace Iceberg.Net.RestServer;

/// <summary>
/// The JDBC catalog's tables, over any ADO.NET provider (SQLite is what the server ships with), in Java's "V1" layout
/// (<c>jdbc.schema-version=V1</c>):
/// <code>
/// iceberg_tables(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type)
/// iceberg_namespace_properties(catalog_name, namespace, property_key, property_value)
/// </code>
/// so Java's <c>JdbcCatalog</c>, PyIceberg's <c>SqlCatalog</c> and this library's <c>JdbcCatalog(views: true)</c> can
/// open the same database. Namespaces are stored dot-joined (as all three do), so a namespace level may not contain a
/// dot. A namespace exists when it has a property row (an <c>exists=true</c> marker is written on create), a table, or a
/// child namespace. Every pointer move is <c>UPDATE … WHERE metadata_location = @expected</c> inside one database
/// transaction, and a create is an <c>INSERT</c> guarded by the primary key. The server's own state (idempotency keys,
/// scan plans) lives in two more tables, <c>iceberg_rest_idempotency</c> and <c>iceberg_rest_plans</c>, which the other
/// catalogs ignore.
/// </summary>
public sealed partial class SqlCatalogStore(Func<DbConnection> connect, string catalogName = "rest") : ICatalogStore, IServerStateStore
{
    private const string ExistsMarker = "exists";

    /// <summary>A SQLite database file (created if missing), with a busy timeout so concurrent writers wait instead of failing.</summary>
    public static SqlCatalogStore Sqlite(string path, string catalogName = "rest")
    {
        var full = Path.GetFullPath(path);
        if (Path.GetDirectoryName(full) is { } dir) Directory.CreateDirectory(dir);
        return new SqlCatalogStore(() => new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={full};Default Timeout=30;Pooling=True"), catalogName);
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, null, """
            CREATE TABLE IF NOT EXISTS iceberg_tables (
              catalog_name VARCHAR(255) NOT NULL,
              table_namespace VARCHAR(255) NOT NULL,
              table_name VARCHAR(255) NOT NULL,
              metadata_location VARCHAR(1000),
              previous_metadata_location VARCHAR(1000),
              iceberg_type VARCHAR(5),
              PRIMARY KEY (catalog_name, table_namespace, table_name))
            """, [], ct);
        await ExecuteAsync(c, null, """
            CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
              catalog_name VARCHAR(255) NOT NULL,
              namespace VARCHAR(255) NOT NULL,
              property_key VARCHAR(255),
              property_value VARCHAR(1000),
              PRIMARY KEY (catalog_name, namespace, property_key))
            """, [], ct);
        await InitializeServerStateAsync(c, ct);
        try
        {
            await QueryAsync(c, "SELECT iceberg_type FROM iceberg_tables WHERE 1 = 0", [], ct);
        }
        catch (DbException)
        {
            // A V0 database (written by PyIceberg or an older Java): Java's own V0 → V1 migration.
            await ExecuteAsync(c, null, "ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(5)", [], ct);
        }
    }

    private static string Ns(Namespace ns)
    {
        foreach (var level in ns.Levels)
            if (level.Contains('.', StringComparison.Ordinal) || level.Length == 0)
                throw new ArgumentException($"Namespace level '{level}' cannot be stored: the SQL catalog joins levels with '.'");
        return string.Join('.', ns.Levels);
    }

    private static string Like(string ns) => ns.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_") + ".%";

    private static string KindFilter(CatalogEntryKind kind) =>
        kind == CatalogEntryKind.View ? "iceberg_type = 'VIEW'" : "(iceberg_type = 'TABLE' OR iceberg_type IS NULL)";

    private static string TypeOf(CatalogEntryKind kind) => kind == CatalogEntryKind.View ? "VIEW" : "TABLE";

    public async Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string> properties, CancellationToken ct = default)
    {
        var name = Ns(ns);
        await using var c = await OpenAsync(ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        if (await NamespaceExistsAsync(c, tx, name, ct)) throw new AlreadyExistsException($"Namespace already exists: {ns}");
        var props = new Dictionary<string, string> { [ExistsMarker] = "true" };
        foreach (var (k, v) in properties) props[k] = v;
        try
        {
            foreach (var (k, v) in props) await InsertPropertyAsync(c, tx, name, k, v, ct);
            await tx.CommitAsync(ct);
        }
        catch (DbException e)
        {
            throw new AlreadyExistsException($"Namespace already exists: {ns} ({e.Message})");
        }
    }

    private Task InsertPropertyAsync(DbConnection c, DbTransaction tx, string ns, string key, string value, CancellationToken ct) =>
        ExecuteAsync(c, tx, "INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (@c, @n, @k, @v)",
            [("@c", catalogName), ("@n", ns), ("@k", key), ("@v", value)], ct);

    private async Task<bool> NamespaceExistsAsync(DbConnection c, DbTransaction? tx, string ns, CancellationToken ct) =>
        (await QueryAsync(c, """
            SELECT namespace FROM iceberg_namespace_properties WHERE catalog_name = @c AND (namespace = @n OR namespace LIKE @p ESCAPE '\')
            UNION SELECT table_namespace FROM iceberg_tables WHERE catalog_name = @c AND (table_namespace = @n OR table_namespace LIKE @p ESCAPE '\')
            """, [("@c", catalogName), ("@n", ns), ("@p", Like(ns))], ct, tx)).Count > 0;

    public async Task<IReadOnlyDictionary<string, string>?> LoadNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        var name = Ns(ns);
        await using var c = await OpenAsync(ct);
        if (!await NamespaceExistsAsync(c, null, name, ct)) return null;
        var rows = await QueryAsync(c, "SELECT property_key, property_value FROM iceberg_namespace_properties WHERE catalog_name = @c AND namespace = @n",
            [("@c", catalogName), ("@n", name)], ct);
        return rows.Where(r => r[0] is not null && r[0] != ExistsMarker).ToDictionary(r => r[0]!, r => r[1] ?? "");
    }

    public async Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var depth = parent?.Levels.Count ?? 0;
        var all = await QueryAsync(c, """
            SELECT namespace FROM iceberg_namespace_properties WHERE catalog_name = @c
            UNION SELECT table_namespace FROM iceberg_tables WHERE catalog_name = @c
            """, [("@c", catalogName)], ct);
        return all.Select(r => new Namespace(r[0]!.Split('.')))
            .Where(n => n.Levels.Count > depth && (parent is null || n.Levels.Take(depth).SequenceEqual(parent.Levels)))
            .Select(n => new Namespace(n.Levels.Take(depth + 1).ToList()))
            .Distinct()
            .ToList();
    }

    public async Task UpdateNamespacePropertiesAsync(Namespace ns, IReadOnlyDictionary<string, string> updates, IReadOnlyCollection<string> removals, CancellationToken ct = default)
    {
        var name = Ns(ns);
        await using var c = await OpenAsync(ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        if (!await NamespaceExistsAsync(c, tx, name, ct)) throw new NoSuchNamespaceException($"Namespace does not exist: {ns}");
        foreach (var key in removals.Concat(updates.Keys))
            await ExecuteAsync(c, tx, "DELETE FROM iceberg_namespace_properties WHERE catalog_name = @c AND namespace = @n AND property_key = @k",
                [("@c", catalogName), ("@n", name), ("@k", key)], ct);
        foreach (var (k, v) in updates) await InsertPropertyAsync(c, tx, name, k, v, ct);
        await tx.CommitAsync(ct);
    }

    public async Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        var name = Ns(ns);
        await using var c = await OpenAsync(ct);
        if (!await NamespaceExistsAsync(c, null, name, ct)) throw new NoSuchNamespaceException($"Namespace does not exist: {ns}");
        await ExecuteAsync(c, null, "DELETE FROM iceberg_namespace_properties WHERE catalog_name = @c AND namespace = @n", [("@c", catalogName), ("@n", name)], ct);
    }

    public async Task<IReadOnlyList<string>> ListAsync(Namespace ns, CatalogEntryKind kind, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, $"SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND {KindFilter(kind)}",
            [("@c", catalogName), ("@n", Ns(ns))], ct);
        return rows.Select(r => r[0]!).ToList();
    }

    public async Task<string?> GetAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, $"SELECT metadata_location FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND {KindFilter(kind)}",
            [("@c", catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct);
        return rows.Count == 0 ? null : rows[0][0];
    }

    public async Task<CatalogEntryKind?> KindAsync(TableIdentifier id, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, "SELECT iceberg_type FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t",
            [("@c", catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct);
        return rows.Count == 0 ? null : rows[0][0] == "VIEW" ? CatalogEntryKind.View : CatalogEntryKind.Table;
    }

    public async Task SwapAsync(IReadOnlyList<PointerSwap> swaps, CatalogEntryKind kind, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        foreach (var s in swaps)
        {
            var ns = Ns(s.Id.Namespace);
            if (s.Expected is null)
            {
                if (!await NamespaceExistsAsync(c, tx, ns, ct)) throw new NoSuchNamespaceException($"Namespace does not exist: {s.Id.Namespace}");
                var taken = await QueryAsync(c, "SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t",
                    [("@c", catalogName), ("@n", ns), ("@t", s.Id.Name)], ct, tx);
                if (taken.Count > 0) throw new CommitFailedException($"{s.Id} was created concurrently");
                try
                {
                    await ExecuteAsync(c, tx, """
                        INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type)
                        VALUES (@c, @n, @t, @new, NULL, @type)
                        """, [("@c", catalogName), ("@n", ns), ("@t", s.Id.Name), ("@new", s.NewLocation), ("@type", TypeOf(kind))], ct);
                }
                catch (DbException e)
                {
                    throw new CommitFailedException($"{s.Id} was created concurrently", e); // primary key violation
                }
            }
            else
            {
                var changed = await ExecuteAsync(c, tx, $"""
                    UPDATE iceberg_tables SET metadata_location = @new, previous_metadata_location = @old
                    WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND metadata_location = @old AND {KindFilter(kind)}
                    """, [("@c", catalogName), ("@n", ns), ("@t", s.Id.Name), ("@new", s.NewLocation), ("@old", s.Expected)], ct);
                if (changed != 1) throw new CommitFailedException($"{kind} {s.Id} changed concurrently: it no longer points at {s.Expected}");
            }
        }
        await tx.CommitAsync(ct); // an exception above disposes the transaction uncommitted: nothing moved
    }

    public async Task<bool> DeleteAsync(TableIdentifier id, CatalogEntryKind kind, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        return await ExecuteAsync(c, null, $"DELETE FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND {KindFilter(kind)}",
            [("@c", catalogName), ("@n", Ns(id.Namespace)), ("@t", id.Name)], ct) > 0;
    }

    public async Task RenameAsync(TableIdentifier from, TableIdentifier to, CatalogEntryKind kind, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        var target = Ns(to.Namespace);
        if ((await QueryAsync(c, $"SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t AND {KindFilter(kind)}",
                [("@c", catalogName), ("@n", Ns(from.Namespace)), ("@t", from.Name)], ct, tx)).Count == 0)
            throw new NoSuchTableException($"{kind} does not exist: {from}");
        if (!await NamespaceExistsAsync(c, tx, target, ct)) throw new NoSuchNamespaceException($"Namespace does not exist: {to.Namespace}");
        if ((await QueryAsync(c, "SELECT table_name FROM iceberg_tables WHERE catalog_name = @c AND table_namespace = @n AND table_name = @t",
                [("@c", catalogName), ("@n", target), ("@t", to.Name)], ct, tx)).Count > 0)
            throw new AlreadyExistsException($"Table or view already exists: {to}");
        await ExecuteAsync(c, tx, $"""
            UPDATE iceberg_tables SET table_namespace = @tn, table_name = @tt
            WHERE catalog_name = @c AND table_namespace = @fn AND table_name = @ft AND {KindFilter(kind)}
            """, [("@c", catalogName), ("@fn", Ns(from.Namespace)), ("@ft", from.Name), ("@tn", target), ("@tt", to.Name)], ct);
        await tx.CommitAsync(ct);
    }

    // ---------- ADO.NET plumbing ----------

    private async Task<DbConnection> OpenAsync(CancellationToken ct)
    {
        var c = connect();
        if (c.State != ConnectionState.Open) await c.OpenAsync(ct);
        return c;
    }

    private static DbCommand Command(DbConnection c, DbTransaction? tx, string sql, (string Name, object? Value)[] args)
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

    private static async Task<int> ExecuteAsync(DbConnection c, DbTransaction? tx, string sql, (string, object?)[] args, CancellationToken ct)
    {
        await using var cmd = Command(c, tx, sql, args);
        return await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<string?[]>> QueryAsync(DbConnection c, string sql, (string, object?)[] args, CancellationToken ct, DbTransaction? tx = null)
    {
        await using var cmd = Command(c, tx, sql, args);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var rows = new List<string?[]>();
        while (await r.ReadAsync(ct))
        {
            var row = new string?[r.FieldCount];
            for (var i = 0; i < row.Length; i++) row[i] = r.IsDBNull(i) ? null : Convert.ToString(r.GetValue(i), System.Globalization.CultureInfo.InvariantCulture);
            rows.Add(row);
        }
        return rows;
    }
}
