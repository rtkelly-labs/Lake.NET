using System.Data.Common;
using System.Globalization;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Types;
using Microsoft.EntityFrameworkCore;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Parquet delete files (spec <c>ducklake_delete_file</c>, format <c>parquet</c>). The spec does not lay the file out;
/// DuckDB writes Iceberg's positional delete layout, <c>(file_path VARCHAR, pos BIGINT)</c>, with <c>pos</c> the
/// 0-based row position in the data file, but with field IDs 2147483646 / 2147483645 rather than Iceberg's reserved
/// 2147483546 / 2147483545. This writes DuckDB's IDs and reads either.
/// </summary>
public static class DuckLakeDeleteFiles
{
    public const int FilePathFieldId = 2147483646;
    public const int PosFieldId = 2147483645;

    private static readonly Schema WriteSchema = new(
        NestedField.Optional(FilePathFieldId, "file_path", PrimitiveType.String),
        NestedField.Optional(PosFieldId, "pos", PrimitiveType.Long));

    private static readonly Schema ReadSchema = new(
        NestedField.Optional(FilePathFieldId, "file_path", PrimitiveType.String),
        NestedField.Optional(PosFieldId, "pos", PrimitiveType.Long),
        NestedField.Optional(PositionDeleteFiles.FilePathFieldId, "iceberg_file_path", PrimitiveType.String),
        NestedField.Optional(PositionDeleteFiles.PosFieldId, "iceberg_pos", PrimitiveType.Long),
        NestedField.Optional(DuckLakeCatalog.SnapshotIdFieldId, "_ducklake_internal_snapshot_id", PrimitiveType.Long));

    /// <summary>Write the deleted positions of one data file, sorted. Returns (size in bytes, footer size).</summary>
    public static async Task<(long Size, long FooterSize)> WriteAsync(IFileIO io, string location, string dataFileLocation, IEnumerable<long> positions,
        CancellationToken ct = default)
    {
        var rows = positions.Distinct().Order().Select(p => new object?[] { dataFileLocation, p }).ToList();
        var written = await ParquetFiles.WriteFileAsync(io, location, WriteSchema, 0, [], rows, ParquetWriteOptions.Default with { Metrics = false }, ct);
        return (written.File.FileSizeInBytes, written.FooterSize);
    }

    private static readonly Schema PartialWriteSchema = new(
        NestedField.Optional(FilePathFieldId, "file_path", PrimitiveType.String),
        NestedField.Optional(PosFieldId, "pos", PrimitiveType.Long),
        NestedField.Optional(DuckLakeCatalog.SnapshotIdFieldId, "_ducklake_internal_snapshot_id", PrimitiveType.Long));

    /// <summary>
    /// Write a partial delete file (as DuckDB's flush does): each position with the snapshot that deleted it, so a read at
    /// an earlier snapshot ignores later deletions. Returns (size in bytes, footer size).
    /// </summary>
    public static async Task<(long Size, long FooterSize)> WritePartialAsync(IFileIO io, string location, string dataFileLocation,
        IEnumerable<(long Position, long Snapshot)> entries, CancellationToken ct = default)
    {
        var rows = entries.GroupBy(e => e.Position).Select(g => (Position: g.Key, Snapshot: g.Min(x => x.Snapshot))).OrderBy(e => e.Position)
            .Select(e => new object?[] { dataFileLocation, e.Position, e.Snapshot }).ToList();
        var written = await ParquetFiles.WriteFileAsync(io, location, PartialWriteSchema, 0, [], rows, ParquetWriteOptions.Default with { Metrics = false }, ct);
        return (written.File.FileSizeInBytes, written.FooterSize);
    }

    /// <summary>Every deleted position of a delete file, with the snapshot that deleted it when the file records one (partial delete files, Puffin vectors).</summary>
    public static async Task<List<(long Position, long? Snapshot)>> ReadEntriesAsync(IFileIO io, string location, CancellationToken ct = default)
    {
        var result = new List<(long, long?)>();
        if (await VectorsAsync(io, location, ct) is { } vectors)
        {
            // Cumulative vectors: a position was deleted by the earliest vector holding it.
            var seen = new HashSet<long>();
            foreach (var (snapshot, bitmap) in vectors)
                foreach (var pos in bitmap.Positions())
                    if (seen.Add(pos)) result.Add((pos, snapshot));
            return result;
        }
        foreach (var row in await ParquetFiles.ReadAsync(io, location, ReadSchema, ct))
        {
            var pos = row[1] ?? row[3] ?? throw new InvalidDataException($"Delete file {location} has a row without a position");
            result.Add(((long)pos, row[4] as long?));
        }
        return result;
    }

    /// <summary>
    /// The deleted positions in effect at <paramref name="snapshot"/>. A partial delete file (spec <c>partial_max</c>)
    /// records the snapshot of each deletion in <c>_ducklake_internal_snapshot_id</c>; later ones are not yet in effect.
    /// A Puffin file's vector is the one with the greatest <c>ducklake-snapshot-id</c> not after the snapshot.
    /// </summary>
    public static async Task<HashSet<long>> ReadPositionsAsync(IFileIO io, string location, long snapshot = long.MaxValue, CancellationToken ct = default)
    {
        var positions = new HashSet<long>();
        if (await VectorsAsync(io, location, ct) is { } vectors)
        {
            var current = vectors.LastOrDefault(v => v.Snapshot is null || v.Snapshot <= snapshot);
            if (current.Positions is not null) positions.UnionWith(current.Positions.Positions());
            return positions;
        }
        foreach (var row in await ParquetFiles.ReadAsync(io, location, ReadSchema, ct))
        {
            var pos = row[1] ?? row[3] ?? throw new InvalidDataException($"Delete file {location} has a row without a position");
            if (row[4] is long deletedIn && deletedIn > snapshot) continue;
            positions.Add((long)pos);
        }
        return positions;
    }

    /// <summary>The Iceberg <c>deletion-vector-v1</c> blob type, and DuckLake's per-vector snapshot property (format 1.1).</summary>
    public const string DeletionVectorBlob = "deletion-vector-v1";
    public const string SnapshotProperty = "ducklake-snapshot-id";

    /// <summary>
    /// A delete file's deletion vectors (spec <c>ducklake_delete_file</c>, format <c>puffin</c>), oldest first, each with
    /// its snapshot (null for a bare vector: the file's <c>begin_snapshot</c>); null for a Parquet delete file. Detected
    /// from the content: Parquet starts with <c>PAR1</c>, a Puffin container with <c>PFA1</c>, a bare vector with neither.
    /// </summary>
    internal static async Task<List<(long? Snapshot, Puffin.RoaringPositionBitmap Positions)>?> VectorsAsync(IFileIO io, string location, CancellationToken ct)
    {
        var head = await io.ReadRangeAsync(location, 0, 4, ct);
        if (head.AsSpan().SequenceEqual("PAR1"u8)) return null;
        var bytes = await io.ReadAllAsync(location, ct);
        try
        {
            if (!head.AsSpan().SequenceEqual(Puffin.PuffinFile.Magic)) return [(null, Puffin.DeletionVector.Decode(bytes))];
            var contents = Puffin.PuffinFile.Read(bytes);
            var vectors = new List<(long? Snapshot, Puffin.RoaringPositionBitmap Positions)>();
            foreach (var blob in contents.Blobs.Where(b => b.Type == DeletionVectorBlob))
            {
                long? snapshot = blob.Properties is { } props && props.TryGetValue(SnapshotProperty, out var text)
                    ? long.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture) : null;
                vectors.Add((snapshot, Puffin.DeletionVector.Decode(contents.Payload(blob))));
            }
            return vectors.OrderBy(v => v.Snapshot ?? long.MinValue).ToList();
        }
        catch (FormatException e)
        {
            throw new InvalidDataException($"Delete file {location} is not a Parquet delete file or a valid deletion vector: {e.Message}", e);
        }
    }
}

/// <summary>
/// Rows DuckDB keeps in the catalog database instead of a Parquet file ("data inlining"): one table per table and
/// schema version, <c>ducklake_inlined_data_{table_id}_{schema_version}</c>, listed in <c>ducklake_inlined_data_tables</c>,
/// with <c>row_id</c>, <c>begin_snapshot</c>, <c>end_snapshot</c> and one column per top-level column of that schema
/// version, named as the column was then. Values are native where the catalog database has the type and DuckDB's
/// text encoding otherwise (<see cref="DuckLakeNestedText"/> for lists, structs and maps). Read always; written when
/// inlining is on (<see cref="DuckLakeOptions.DataInliningRowLimit"/> or the lake's persisted
/// <c>data_inlining_row_limit</c>).
/// </summary>
internal static class DuckLakeInlinedData
{
    /// <summary>The system columns of inlined data tables: renamed with a <c>_ducklake_</c> prefix in 1.1.</summary>
    internal static (string RowId, string Begin, string End) SystemColumns(bool v11) =>
        v11 ? ("_ducklake_row_id", "_ducklake_begin_snapshot", "_ducklake_end_snapshot") : ("row_id", "begin_snapshot", "end_snapshot");

    public static async Task<List<(long RowId, object?[] Values)>> ReadAsync(DuckLakeDbContext db, DuckLakeTable table, string inlinedTable, CancellationToken ct)
    {
        // The columns of the schema version the inlined table was created for, to map its columns to column IDs.
        var version = await db.InlinedDataTables.Where(i => i.TableId == table.TableId && i.TableName == inlinedTable).Select(i => (long?)i.SchemaVersion)
            .FirstOrDefaultAsync(ct);
        if (version is null) return [];
        var at = await db.SchemaVersions.Where(v => v.TableId == table.TableId && v.SchemaVersion == version).MinAsync(v => (long?)v.BeginSnapshot, ct)
            ?? await db.Snapshots.Where(s => s.SchemaVersion == version).MinAsync(s => (long?)s.SnapshotId, ct)
            ?? throw new InvalidDataException($"{inlinedTable}: no snapshot has schema version {version}");
        var then = await DuckLakeCatalog.ColumnsAsync(db, table.TableId, at, ct);

        // Current column position of each column ID the inlined table has (dropped columns are skipped).
        var positionById = table.Columns.Select((col, i) => (col.Id, i)).ToDictionary(x => x.Id, x => x.i);
        var mapped = then.Where(col => positionById.ContainsKey(col.Id)).ToList();
        var (rowId, begin, end) = SystemColumns(db.Is11);
        var select = string.Join(", ", mapped.Select(col => SelectSql(db.Dialect, col)).Prepend(rowId));
        var sql = $"SELECT {select} FROM {Db.Quote(inlinedTable)} WHERE @s >= {begin} AND ({end} IS NULL OR @s < {end}) ORDER BY {rowId}";
        await db.Database.OpenConnectionAsync(ct);
        var (c, t) = db.Raw();
        return await Db.QueryAsync(c, t, sql, r =>
        {
            var values = new object?[table.Columns.Count];
            for (var i = 0; i < values.Length; i++) values[i] = Default(table.Columns[i]);
            for (var k = 0; k < mapped.Count; k++)
            {
                var pos = positionById[mapped[k].Id];
                values[pos] = Convert(table.Columns[pos], r.IsDBNull(k + 1) ? null : r.GetValue(k + 1), inlinedTable);
            }
            return (Db.ReqLong(r, 0), values);
        }, ct, table.SnapshotId);
    }

    /// <summary>
    /// Every row version of an inlined data table (deleted ones too), with the columns of the schema version it was
    /// created for, in that version's column order: what a flush writes to Parquet.
    /// </summary>
    internal static async Task<(IReadOnlyList<DuckLakeColumn> Columns, List<(long RowId, long Begin, long? End, object?[] Values)> Rows)> ReadAllAsync(
        DuckLakeDbContext db, long tableId, string inlinedTable, long schemaVersion, CancellationToken ct)
    {
        var at = await db.SchemaVersions.Where(v => v.TableId == tableId && v.SchemaVersion == schemaVersion).MinAsync(v => (long?)v.BeginSnapshot, ct)
            ?? await db.Snapshots.Where(s => s.SchemaVersion == schemaVersion).MinAsync(s => (long?)s.SnapshotId, ct)
            ?? throw new InvalidDataException($"{inlinedTable}: no snapshot has schema version {schemaVersion}");
        var columns = await DuckLakeCatalog.ColumnsAsync(db, tableId, at, ct);
        var (rowId, begin, end) = SystemColumns(db.Is11);
        var sql = $"SELECT {string.Join(", ", columns.Select(c => SelectSql(db.Dialect, c)).Prepend(end).Prepend(begin).Prepend(rowId))} FROM {Db.Quote(inlinedTable)}";
        await db.Database.OpenConnectionAsync(ct);
        var (c, t) = db.Raw();
        var rows = await Db.QueryAsync(c, t, sql, r =>
        {
            var values = new object?[columns.Count];
            for (var k = 0; k < columns.Count; k++) values[k] = Convert(columns[k], r.IsDBNull(k + 3) ? null : r.GetValue(k + 3), inlinedTable);
            return (Db.ReqLong(r, 0), Db.ReqLong(r, 1), Db.Long(r, 2), values);
        }, ct);
        return (columns, rows);
    }

    // ---- writing (spec "Type Encoding for Data Inlining"; column types as DuckDB 1.5.5 creates them) ----------------

    /// <summary>PostgreSQL's native TIMETZ and INTERVAL columns, which are read and written as text (DuckDB's text parses as both).</summary>
    private static string? NativeTextType(DuckLakeDialect dialect, DuckLakeType type) =>
        dialect == DuckLakeDialect.PostgreSql && type is DuckLakePrimitive { Name: "timetz" or "interval" } p ? SqlType(dialect, p) : null;

    private static string SelectSql(DuckLakeDialect dialect, DuckLakeColumn c) =>
        NativeTextType(dialect, c.Type) is null ? Db.Quote(c.Name) : $"CAST({Db.Quote(c.Name)} AS TEXT)";

    /// <summary>Whether rows of these columns can be inlined in the catalog database (otherwise they go to a file).</summary>
    internal static bool CanInline(DuckLakeDialect dialect, IReadOnlyList<DuckLakeColumn> columns) =>
        columns.All(c => c.Type is not DuckLakePrimitive p || p.Name is not ("variant" or "geometry")
                                                           && !(dialect == DuckLakeDialect.PostgreSql && DuckLakeTypes.ToIceberg(p) is DecimalType { Precision: > 28 }));

    /// <summary>The column type of an inlined data table: DuckDB's choice for each catalog database.</summary>
    internal static string SqlType(DuckLakeDialect dialect, DuckLakeType type)
    {
        if (type is not DuckLakePrimitive p) return "VARCHAR"; // nested values as DuckDB's text cast
        if (dialect == DuckLakeDialect.Sqlite)
            return p.Name switch
            {
                "boolean" or "int8" or "int16" or "int32" or "int64" or "uint8" or "uint16" or "uint32" or "null" => "BIGINT",
                "blob" => "BLOB",
                _ => "VARCHAR",
            };
        return p.Name switch
        {
            "boolean" => "BOOLEAN",
            "int8" or "int16" => "SMALLINT",
            "int32" or "uint8" or "uint16" or "null" => "INTEGER",
            "int64" or "uint32" => "BIGINT",
            "timetz" => "TIME WITH TIME ZONE",
            "uint64" => "VARCHAR", // DuckDB's choice, though NUMERIC(20,0) would hold it
            "interval" => "INTERVAL",
            "float32" => "REAL",
            "float64" => "DOUBLE PRECISION",
            "uuid" => "UUID",
            "time" => "TIME",
            "varchar" or "json" or "blob" => "BYTEA",
            _ when DuckLakeTypes.ToIceberg(p) is DecimalType d => $"NUMERIC({d.Precision},{d.Scale})",
            _ => "VARCHAR", // dates, timestamps: DuckDB's text
        };
    }

    /// <summary>A physical value as the catalog database stores it in an inlined data table.</summary>
    internal static object? Encode(DuckLakeDialect dialect, DuckLakeType type, object? value)
    {
        if (value is null) return null;
        if (type is not DuckLakePrimitive p) return DuckLakeNestedText.Format(type, value);
        if (dialect == DuckLakeDialect.Sqlite)
            return (p.Name, value) switch
            {
                ("boolean", bool b) => b ? 1L : 0L,
                ("int8" or "int16" or "int32" or "uint8" or "uint16", int i) => (long)i,
                ("int64" or "uint32", long l) => l,
                ("blob", byte[] bytes) => bytes,
                ("varchar" or "json", string text) => text,
                _ => DuckLakeValues.InlineText(p, value),
            };
        return (p.Name, value) switch
        {
            ("boolean", bool b) => b,
            ("int8" or "int16", int i) => (short)i,
            ("int32" or "uint8" or "uint16", int i) => i,
            ("int64" or "uint32", long l) => l,
            ("float32", float f) => f,
            ("float64", double d) => d,
            ("uuid", Guid g) => g,
            ("time", long micros) => TimeSpan.FromTicks(micros * 10),
            ("varchar" or "json", string text) => System.Text.Encoding.UTF8.GetBytes(text),
            ("blob", byte[] bytes) => bytes,
            ("uint64", IcebergDecimal d) => d.Unscaled.ToString(CultureInfo.InvariantCulture),
            (_, IcebergDecimal d) => d.ToDecimal(),
            _ => DuckLakeValues.InlineText(p, value),
        };
    }

    /// <summary>The <c>CREATE TABLE</c> of an inlined data table for a table's columns at one schema version.</summary>
    internal static string CreateTableSql(DuckLakeDialect dialect, bool v11, string name, IReadOnlyList<DuckLakeColumn> columns)
    {
        var (rowId, begin, end) = SystemColumns(v11);
        var cols = columns.Select(c => $"{Db.Quote(c.Name)} {SqlType(dialect, c.Type)}").Prepend($"{rowId} BIGINT, {begin} BIGINT, {end} BIGINT");
        return $"CREATE TABLE IF NOT EXISTS {Db.Quote(name)}({string.Join(", ", cols)})";
    }

    /// <summary>Insert inlined rows (multi-row statements, under the parameter limits of both databases).</summary>
    internal static async Task InsertAsync(DuckLakeDbContext db, string name, IReadOnlyList<DuckLakeColumn> columns,
        IReadOnlyList<(long RowId, long Begin, long? End, object?[] Values)> rows, CancellationToken ct)
    {
        var (rowId, begin, end) = SystemColumns(db.Is11);
        var width = columns.Count + 3;
        var head = $"INSERT INTO {Db.Quote(name)} ({string.Join(", ", columns.Select(c => Db.Quote(c.Name)).Prepend(end).Prepend(begin).Prepend(rowId))}) VALUES ";
        var casts = columns.Select(c => NativeTextType(db.Dialect, c.Type)).ToArray();
        foreach (var chunk in rows.Chunk(Math.Max(1, Math.Min(1000, 30_000 / width))))
        {
            var args = new object?[chunk.Length * width];
            var sql = new System.Text.StringBuilder(head);
            for (var r = 0; r < chunk.Length; r++)
            {
                var (id, b, e, values) = chunk[r];
                args[r * width] = id;
                args[r * width + 1] = b;
                args[r * width + 2] = e;
                for (var c = 0; c < columns.Count; c++) args[r * width + 3 + c] = Encode(db.Dialect, columns[c].Type, values[c]);
                sql.Append(r == 0 ? "(" : ", (");
                for (var c = 0; c < width; c++)
                {
                    var p = "@p" + (r * width + c).ToString(CultureInfo.InvariantCulture);
                    if (c > 0) sql.Append(", ");
                    sql.Append(c >= 3 && casts[c - 3] is { } cast ? $"CAST({p} AS {cast})" : p);
                }
                sql.Append(')');
            }
            await db.Database.OpenConnectionAsync(ct);
            var (connection, tx) = db.Raw();
            await Db.ExecAsync(connection, tx, sql.ToString(), ct, args);
        }
    }

    /// <summary>Record deleted positions of data files in the table's inlined deletion table.</summary>
    internal static async Task InsertDeletionsAsync(DuckLakeDbContext db, long tableId, IReadOnlyList<(long FileId, long Position)> deletions, long snapshot,
        CancellationToken ct)
    {
        var head = $"INSERT INTO {Db.Quote(DuckLakeCatalog.InlinedDeleteTable(tableId))} (file_id, row_id, begin_snapshot) VALUES ";
        foreach (var chunk in deletions.Chunk(1000))
        {
            var args = new object?[chunk.Length * 3];
            var sql = new System.Text.StringBuilder(head);
            for (var i = 0; i < chunk.Length; i++)
            {
                (args[i * 3], args[i * 3 + 1], args[i * 3 + 2]) = (chunk[i].FileId, chunk[i].Position, snapshot);
                sql.Append(i == 0 ? "(" : ", (").Append(CultureInfo.InvariantCulture, $"@p{i * 3}, @p{i * 3 + 1}, @p{i * 3 + 2})");
            }
            var (connection, tx) = db.Raw();
            await Db.ExecAsync(connection, tx, sql.ToString(), ct, args);
        }
    }

    /// <summary>The <c>CREATE TABLE</c> of a table's inlined deletion table (<c>file_id</c>, position <c>row_id</c>, <c>begin_snapshot</c>).</summary>
    internal static string CreateDeleteTableSql(long tableId) =>
        $"CREATE TABLE IF NOT EXISTS {Db.Quote(DuckLakeCatalog.InlinedDeleteTable(tableId))}(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT)";

    private static object? Default(DuckLakeColumn column) =>
        column.InitialDefault is { } text && column.Type is DuckLakePrimitive p && DuckLakeValues.TryParse(p, text, out var v) ? v : null;

    private static object? Convert(DuckLakeColumn column, object? raw, string table)
    {
        if (raw is null) return null;
        // PostgreSQL catalogs hold inlined VARCHAR and JSON as BYTEA (spec "Type Encoding for Data Inlining").
        if (raw is byte[] utf8 && !(column.Type is DuckLakePrimitive { Name: "blob" })) raw = System.Text.Encoding.UTF8.GetString(utf8);
        if (column.Type is not DuckLakePrimitive p)
        {
            var nested = raw as string ?? throw new InvalidDataException($"{table}: nested column {column.Name} holds a {raw.GetType().Name}");
            try { return DuckLakeNestedText.Parse(column.Type, nested); }
            catch (FormatException e) { throw new InvalidDataException($"{table}: column {column.Name}: {e.Message}", e); }
        }
        switch (raw)
        {
            case byte[] bytes when p.Name == "blob":
                return bytes;
            case long or int or short or byte when p.Name == "boolean":
                return System.Convert.ToInt64(raw, CultureInfo.InvariantCulture) != 0;
            case long or int or short or byte or double or float or decimal when p.Name is not ("varchar" or "json"):
                return DuckLakeValues.Normalize(p, raw, column.Name);
            case bool b when p.Name == "boolean":
                return b;
            // Native types of catalogs that have them (PostgreSQL UUID, NUMERIC, DATE/TIME/TIMESTAMP).
            case Guid or DateTime or DateTimeOffset or DateOnly or TimeOnly or TimeSpan when p.Name is not ("varchar" or "json"):
                try { return DuckLakeValues.Normalize(p, raw, column.Name); }
                catch (ArgumentException e) { throw new InvalidDataException($"{table}: column {column.Name}: {e.Message}", e); }
        }
        var text = System.Convert.ToString(raw, CultureInfo.InvariantCulture)!;
        if (p.Name is "varchar" or "json") return text;
        return DuckLakeValues.TryParse(p, text, out var value)
            ? value
            : throw new InvalidDataException($"{table}: '{text}' is not a valid {p} for column {column.Name}");
    }
}
