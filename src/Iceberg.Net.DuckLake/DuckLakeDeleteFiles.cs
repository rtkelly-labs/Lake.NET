using System.Data.Common;
using System.Globalization;
using Iceberg.Net.Data;
using Iceberg.Net.IO;
using Iceberg.Net.Types;

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
        var file = await ParquetFiles.WriteAsync(io, location, WriteSchema, 0, [], rows, ct);
        return (file.FileSizeInBytes, await FooterSizeAsync(io, location, file.FileSizeInBytes, ct));
    }

    /// <summary>
    /// The deleted positions in effect at <paramref name="snapshot"/>. A partial delete file (spec <c>partial_max</c>)
    /// records the snapshot of each deletion in <c>_ducklake_internal_snapshot_id</c>; later ones are not yet in effect.
    /// </summary>
    public static async Task<HashSet<long>> ReadPositionsAsync(IFileIO io, string location, long snapshot = long.MaxValue, CancellationToken ct = default)
    {
        var positions = new HashSet<long>();
        foreach (var row in await ParquetFiles.ReadAsync(io, location, ReadSchema, ct))
        {
            var pos = row[1] ?? row[3] ?? throw new InvalidDataException($"Delete file {location} has a row without a position");
            if (row[4] is long deletedIn && deletedIn > snapshot) continue;
            positions.Add((long)pos);
        }
        return positions;
    }

    /// <summary>The Parquet footer length (the 4 bytes before the trailing <c>PAR1</c>), as <c>footer_size</c> records it.</summary>
    internal static async Task<long> FooterSizeAsync(IFileIO io, string location, long size, CancellationToken ct)
    {
        if (size < 12) throw new InvalidDataException($"{location} is too small to be Parquet");
        var tail = await io.ReadRangeAsync(location, size - 8, 8, ct);
        if (tail.Length != 8 || tail[4] != 'P' || tail[5] != 'A' || tail[6] != 'R' || tail[7] != '1')
            throw new InvalidDataException($"{location} does not end in PAR1");
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(tail.AsSpan(0, 4));
    }
}

/// <summary>
/// Rows DuckDB keeps in the catalog database instead of a Parquet file ("data inlining"): one table per table and
/// schema version, <c>ducklake_inlined_data_{table_id}_{schema_version}</c>, listed in <c>ducklake_inlined_data_tables</c>,
/// with <c>row_id</c>, <c>begin_snapshot</c>, <c>end_snapshot</c> and one column per top-level column of that schema
/// version, named as the column was then. Values are native where the catalog database has the type and DuckDB's
/// text encoding otherwise (<see cref="DuckLakeNestedText"/> for lists, structs and maps). This library reads inlined rows (every DuckDB-written lake has them for small inserts)
/// but writes Parquet only, which the spec allows.
/// </summary>
internal static class DuckLakeInlinedData
{
    public static async Task<List<(long RowId, object?[] Values)>> ReadAsync(DbConnection c, DuckLakeTable table, string inlinedTable, CancellationToken ct)
    {
        // The columns of the schema version the inlined table was created for, to map its columns to column IDs.
        var version = await Db.QueryAsync(c, null, "SELECT schema_version FROM ducklake_inlined_data_tables WHERE table_id = @p0 AND table_name = @p1",
            r => Db.ReqLong(r, 0), ct, null, table.TableId, inlinedTable);
        if (version.Count == 0) return [];
        var begin = await Db.QueryAsync(c, null, "SELECT min(begin_snapshot) FROM ducklake_schema_versions WHERE table_id = @p0 AND schema_version = @p1",
            r => Db.Long(r, 0), ct, null, table.TableId, version[0]);
        var at = begin.FirstOrDefault()
            ?? (await Db.QueryAsync(c, null, "SELECT min(snapshot_id) FROM ducklake_snapshot WHERE schema_version = @p0", r => Db.Long(r, 0), ct, null, version[0])).FirstOrDefault()
            ?? throw new InvalidDataException($"{inlinedTable}: no snapshot has schema version {version[0]}");
        var then = await DuckLakeCatalog.ColumnsAsync(c, null, table.TableId, at, ct);

        // Current column position of each column ID the inlined table has (dropped columns are skipped).
        var positionById = table.Columns.Select((col, i) => (col.Id, i)).ToDictionary(x => x.Id, x => x.i);
        var mapped = then.Where(col => positionById.ContainsKey(col.Id)).ToList();
        var select = string.Join(", ", mapped.Select(col => Db.Quote(col.Name)).Prepend("row_id"));
        var sql = $"SELECT {select} FROM {Db.Quote(inlinedTable)} WHERE {Db.Live} ORDER BY row_id";
        return await Db.QueryAsync(c, null, sql, r =>
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

    private static object? Default(DuckLakeColumn column) =>
        column.InitialDefault is { } text && column.Type is DuckLakePrimitive p && DuckLakeValues.TryParse(p, text, out var v) ? v : null;

    private static object? Convert(DuckLakeColumn column, object? raw, string table)
    {
        if (raw is null) return null;
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
        }
        var text = System.Convert.ToString(raw, CultureInfo.InvariantCulture)!;
        if (p.Name is "varchar" or "json") return text;
        return DuckLakeValues.TryParse(p, text, out var value)
            ? value
            : throw new InvalidDataException($"{table}: '{text}' is not a valid {p} for column {column.Name}");
    }
}
