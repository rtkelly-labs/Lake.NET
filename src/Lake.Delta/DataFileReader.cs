using Apache.Arrow;
using Apache.Arrow.Types;
using Iceberg.Net.IO;
using Iceberg.Net.Puffin;
using ArrowField = Apache.Arrow.Field;

namespace Lake.Delta;

/// <summary>
/// Reads one data file into rows of the table schema. Columns are resolved per PROTOCOL.md "Reader Requirements for
/// Column Mapping": by display name (<c>none</c>), physical name (<c>name</c>) or Parquet field ID (<c>id</c>), nested
/// struct fields included; a column the file lacks reads as null. Values are converted to the schema's type, which also
/// covers files written before a supported type widening.
/// </summary>
internal static class DataFileReader
{
    /// <summary>How one schema type is read from one file field; a child index of -1 is a field the file lacks (read as null).</summary>
    private sealed class Plan
    {
        public required DeltaType Type { get; init; }
        public required string Path { get; init; }
        public int[] ChildIndex { get; init; } = [];
        public Plan[] Children { get; init; } = [];
        public Plan? Element { get; init; }
        public Plan? Key { get; init; }
        public Plan? Value { get; init; }
    }

    /// <param name="partitionValues">Per schema field: the parsed partition value (used where <paramref name="isPartition"/> is set).</param>
    /// <param name="deleted">Rows (by position in the file) a deletion vector removes; null when none.</param>
    public static async IAsyncEnumerable<object?[]> ReadAsync(
        IFileIO io, string location, DeltaStructType schema, ColumnMappingMode mode, object?[] partitionValues, bool[] isPartition,
        RoaringPositionBitmap? deleted, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        long position = 0;
        int[]? columns = null;
        Plan?[]? plans = null;
        await foreach (var (arrowSchema, batch) in ParquetSource.ReadAsync(io, location, "data file", ct).ConfigureAwait(false))
        {
            if (plans is null)
            {
                (columns, plans) = Corruption.Guard($"data file {location}", () => PlanTopLevel(arrowSchema.FieldsList, schema, mode, isPartition, location));
            }
            var rows = new List<object?[]>(batch.Length);
            Corruption.Guard($"data file {location}", () =>
            {
                for (var r = 0; r < batch.Length; r++, position++)
                {
                    if (deleted is not null && deleted.Contains(position)) continue;
                    var row = new object?[schema.Fields.Count];
                    for (var f = 0; f < row.Length; f++)
                    {
                        if (isPartition[f]) row[f] = partitionValues[f];
                        else if (columns![f] >= 0) row[f] = Value(batch.Column(columns[f]), r, plans![f]!);
                    }
                    rows.Add(row);
                }
                return 0;
            });
            foreach (var row in rows) yield return row;
        }
    }

    private static (int[] Columns, Plan?[] Plans) PlanTopLevel(IReadOnlyList<ArrowField> fileFields, DeltaStructType schema, ColumnMappingMode mode, bool[] isPartition, string location)
    {
        if (mode == ColumnMappingMode.Id && fileFields.Count > 0 && fileFields.All(f => ParquetSource.FieldId(f) is null))
            throw new InvalidDataException($"Data file {location} has no Parquet field IDs, but the table uses column mapping mode id");
        var columns = new int[schema.Fields.Count];
        var plans = new Plan?[schema.Fields.Count];
        for (var i = 0; i < schema.Fields.Count; i++)
        {
            columns[i] = -1;
            if (isPartition[i]) continue;
            var field = schema.Fields[i];
            columns[i] = Resolve(fileFields, field, mode);
            if (columns[i] >= 0) plans[i] = Build(fileFields[columns[i]], field.Type, mode, field.Name);
        }
        return (columns, plans);
    }

    private static int Resolve(IReadOnlyList<ArrowField> fileFields, DeltaField field, ColumnMappingMode mode)
    {
        if (mode == ColumnMappingMode.Id)
        {
            var id = field.ColumnMappingId ?? throw new InvalidDataException($"Field '{field.Name}' has no {DeltaField.ColumnMappingIdKey} although the table uses column mapping mode id");
            for (var i = 0; i < fileFields.Count; i++)
                if (ParquetSource.FieldId(fileFields[i]) == id) return i;
            return -1;
        }
        var name = field.PhysicalName(mode);
        for (var i = 0; i < fileFields.Count; i++)
            if (fileFields[i].Name == name) return i;
        for (var i = 0; i < fileFields.Count; i++)
            if (string.Equals(fileFields[i].Name, name, StringComparison.OrdinalIgnoreCase)) return i; // names are unique regardless of case
        return -1;
    }

    private static Plan Build(ArrowField file, DeltaType type, ColumnMappingMode mode, string path)
    {
        switch (type)
        {
            case DeltaStructType st:
            {
                if (file.DataType is not StructType fileStruct) throw Mismatch(path, file, type);
                var index = new int[st.Fields.Count];
                var children = new Plan[st.Fields.Count];
                for (var i = 0; i < st.Fields.Count; i++)
                {
                    index[i] = Resolve(fileStruct.Fields, st.Fields[i], mode);
                    if (index[i] >= 0) children[i] = Build(fileStruct.Fields[index[i]], st.Fields[i].Type, mode, path + "." + st.Fields[i].Name);
                }
                return new Plan { Type = type, Path = path, ChildIndex = index, Children = children };
            }
            case DeltaArrayType at:
                return file.DataType switch
                {
                    ListType lt => new Plan { Type = type, Path = path, Element = Build(lt.ValueField, at.ElementType, mode, path + ".element") },
                    _ => throw Mismatch(path, file, type),
                };
            case DeltaMapType mt:
                return file.DataType switch
                {
                    MapType m => new Plan
                    {
                        Type = type,
                        Path = path,
                        Key = Build(m.KeyField, mt.KeyType, mode, path + ".key"),
                        Value = Build(m.ValueField, mt.ValueType, mode, path + ".value"),
                    },
                    _ => throw Mismatch(path, file, type),
                };
            default:
                if (file.DataType is StructType or ListType or MapType) throw Mismatch(path, file, type);
                return new Plan { Type = type, Path = path };
        }
    }

    private static InvalidDataException Mismatch(string path, ArrowField file, DeltaType type) =>
        new($"Column '{path}': the file's {file.DataType.Name} cannot be read as {type}");

    private static object? Value(IArrowArray a, int i, Plan p)
    {
        if (a.IsNull(i)) return null;
        switch (a)
        {
            case StructArray s:
            {
                var result = new object?[p.ChildIndex.Length];
                for (var k = 0; k < result.Length; k++)
                    if (p.ChildIndex[k] >= 0) result[k] = Value(s.Fields[p.ChildIndex[k]], i + s.Offset, p.Children[k]);
                return result;
            }
            case MapArray m:
            {
                var result = new Dictionary<object, object?>();
                for (var j = m.ValueOffsets[i]; j < m.ValueOffsets[i + 1]; j++)
                    result[Value(m.Keys, j, p.Key!) ?? throw new InvalidDataException($"Column '{p.Path}': null map key")] = Value(m.Values, j, p.Value!);
                return result;
            }
            case ListArray l:
            {
                var result = new List<object?>(l.GetValueLength(i));
                for (var j = l.ValueOffsets[i]; j < l.ValueOffsets[i + 1]; j++) result.Add(Value(l.Values, j, p.Element!));
                return result;
            }
            default:
                return Coerce(ParquetSource.Primitive(a, i), p.Type, p.Path);
        }
    }

    /// <summary>A file value as the schema type: identity, or one of the protocol's type widenings.</summary>
    internal static object? Coerce(object? value, DeltaType type, string path)
    {
        if (value is null || type == DeltaPrimitiveType.Void) return null;
        object? result = type switch
        {
            DeltaPrimitiveType t when t == DeltaPrimitiveType.String => value as string,
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Binary => value as byte[],
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Boolean => value as bool?,
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Byte => value as sbyte?,
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Short => value switch { sbyte v => (short)v, short v => v, _ => null },
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Integer => value switch { sbyte v => (int)v, short v => (int)v, int v => v, _ => null },
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Long => value switch { sbyte v => (long)v, short v => (long)v, int v => (long)v, long v => v, _ => null },
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Float => value as float?,
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Double => value switch { float v => (double)v, double v => v, sbyte v => (double)v, short v => (double)v, int v => (double)v, _ => null },
            DeltaDecimalType => value switch { decimal v => v, sbyte v => (decimal)v, short v => (decimal)v, int v => (decimal)v, long v => (decimal)v, _ => null },
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Date => value as DateOnly?,
            DeltaPrimitiveType t when t == DeltaPrimitiveType.Timestamp => value as DateTimeOffset?,
            DeltaPrimitiveType t when t == DeltaPrimitiveType.TimestampNtz => value switch
            {
                DateTimeOffset v => DateTime.SpecifyKind(v.UtcDateTime, DateTimeKind.Unspecified),
                DateOnly d => d.ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified),
                _ => null,
            },
            _ => null,
        };
        return result ?? throw new InvalidDataException($"Column '{path}': a {value.GetType().Name} in the file cannot be read as {type}");
    }
}
