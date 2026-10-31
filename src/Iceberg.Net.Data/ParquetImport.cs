using ParquetSharp;
using Iceberg.Net.Catalog;
using Iceberg.Net.Manifests;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>One reason a Parquet file cannot be adopted into an Iceberg table.</summary>
public sealed record ImportProblem(string Column, string Problem);

public sealed class ParquetImportException(string location, IReadOnlyList<ImportProblem> problems)
    : Exception($"{location} cannot be added to the table:\n" + string.Join('\n', problems.Select(p => $"  - {p.Column}: {p.Problem}")))
{
    public IReadOnlyList<ImportProblem> Problems { get; } = problems;
}

/// <summary>
/// Adopts Parquet files written by other tools — e.g. the Parquet.SourceGenerator emitters, pyarrow, Spark —
/// into a table without rewriting them (the equivalent of Java/PyIceberg <c>add_files</c>). Every column is
/// checked against the Iceberg type's required Parquet representation (spec "Parquet" type mapping), because
/// a file that merely *reads* in .NET can still be misread by Athena or Spark. Metrics and the partition tuple
/// are computed from the data.
/// </summary>
public static class ParquetImport
{
    /// <summary>Validate the file and build its <see cref="DataFile"/>. Commit it with <see cref="SnapshotProducer.AddFiles"/>.</summary>
    public static async Task<DataFile> ToDataFileAsync(Table table, string location, CancellationToken ct = default)
    {
        var schema = table.Schema;
        var bytes = await table.IO.ReadAllAsync(location, ct);
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var problems = new List<ImportProblem>();

        using (var reader = await ParquetFiles.Backend.OpenAsync(new MemoryStream(bytes), null, ct))
        {
            var columns = ParquetFiles.LeavesOf(reader.Schema);
            var hasIds = columns.Any(c => c.Node.FieldId >= 0);
            if (!hasIds && mapping is null)
                problems.Add(new ImportProblem("*", $"the file has no Parquet field IDs and the table has no name mapping; set {NameMapping.Property} (NameMapping.FromSchema) so every engine resolves its columns the same way"));
            if (columns.Any(c => c.Path.Contains('.', StringComparison.Ordinal)))
                problems.Add(new ImportProblem("*", "nested columns are not supported by the importer yet"));

            if (problems.Count == 0)
            {
                foreach (var field in schema.Columns)
                {
                    var column = columns.FirstOrDefault(c =>
                        hasIds ? c.Node.FieldId == field.Id : (mapping!.NamesFor(field.Id) is { Count: > 0 } names ? names : [field.Name]).Contains(c.Path));
                    if (column is null)
                    {
                        if (field.Required && field.InitialDefault is null) problems.Add(new ImportProblem(field.Name, "required column is missing from the file"));
                        continue;
                    }
                    if (CheckType(field.Type, column.Node) is { } problem) problems.Add(new ImportProblem(field.Name, problem));
                    if (field.Required && column.MaxDef > 0 && NullCount(reader, columns.IndexOf(column)) > 0)
                        problems.Add(new ImportProblem(field.Name, "column is required in the table but the file contains nulls"));
                }
            }
        }
        if (problems.Count > 0) throw new ParquetImportException(location, problems);

        var rows = await ParquetFiles.ReadAsync(table.IO, location, schema, mapping, ct);
        var spec = table.Spec;
        var positions = schema.Columns.Select((c, i) => (c.Id, i)).ToDictionary(x => x.Id, x => x.i);
        var partitions = rows.Select(r => spec.Partition(schema, id => r[positions[id]])).ToList();
        if (partitions.Select(p => string.Join('\u0001', p.Select(v => v?.ToString()))).Distinct().Count() > 1)
            throw new ParquetImportException(location, [new ImportProblem("*", $"rows span more than one partition of spec {spec.SpecId}; an Iceberg data file belongs to exactly one")]);

        var metrics = Metrics.Compute(schema, rows);
        return new DataFile
        {
            FilePath = location,
            FileFormat = "PARQUET",
            SpecId = spec.SpecId,
            Partition = partitions.FirstOrDefault() ?? new object?[spec.Fields.Count],
            RecordCount = rows.Count,
            FileSizeInBytes = bytes.Length,
            ValueCounts = metrics.ValueCounts,
            NullValueCounts = metrics.NullCounts,
            NanValueCounts = metrics.NanCounts.Count > 0 ? metrics.NanCounts : null,
            LowerBounds = metrics.Lower,
            UpperBounds = metrics.Upper,
        };
    }

    private static long NullCount(IPqReader reader, int column)
    {
        long nulls = 0;
        for (var rg = 0; rg < reader.RowGroups; rg++)
            nulls += reader.Chunk(rg, column).Statistics?.NullCount ?? 1; // unknown ⇒ assume nulls
        return nulls;
    }

    /// <summary>The Parquet representation Iceberg requires for each type, or the reason this column is not it.</summary>
    public static string? CheckType(IcebergType type, ColumnDescriptor column)
    {
        using var node = column.SchemaNode;
        return CheckType(type, ParquetSharpBackend.ToPq(node));
    }

    internal static string? CheckType(IcebergType type, PqNode column)
    {
        var physical = column.PhysicalType;
        var logical = column.Logical;
        if (physical == PqPhysical.Int96)
            return "INT96 timestamps are deprecated and Iceberg writers must not produce them; write INT64 with a TIMESTAMP(MICROS) annotation";

        string Expect(string what) => $"expected {what} for Iceberg {type}, found {physical} {Describe(logical)}";
        static bool SignedInt(PqLogical l, int bits) => l.Kind == PqLogicalKind.None || (l.Kind == PqLogicalKind.Int && l.Signed && l.BitWidth <= bits);
        var unsigned = logical is { Kind: PqLogicalKind.Int, Signed: false };

        return type.TypeId switch
        {
            TypeId.Boolean when physical == PqPhysical.Boolean => null,
            TypeId.Int when physical == PqPhysical.Int32 && SignedInt(logical, 32) => null,
            TypeId.Int when unsigned => "unsigned integers have no Iceberg type; widen to a signed type",
            TypeId.Long when (physical == PqPhysical.Int64 && SignedInt(logical, 64)) || (physical == PqPhysical.Int32 && SignedInt(logical, 32)) => null,
            TypeId.Long when unsigned => "unsigned integers have no Iceberg type; widen to a signed type",
            TypeId.Float when physical == PqPhysical.Float => null,
            TypeId.Double when physical is PqPhysical.Double or PqPhysical.Float => null,
            TypeId.String when physical == PqPhysical.ByteArray && logical.Kind == PqLogicalKind.String => null,
            TypeId.Binary when physical == PqPhysical.ByteArray && logical.Kind == PqLogicalKind.None => null,
            TypeId.Date when physical == PqPhysical.Int32 && logical.Kind == PqLogicalKind.Date => null,
            TypeId.Time when physical == PqPhysical.Int64 && logical is { Kind: PqLogicalKind.Time, Unit: PqTimeUnit.Micros } => null,
            TypeId.Time when logical.Kind == PqLogicalKind.Time => $"TIME({logical.Unit}) found; Iceberg time is INT64 TIME(MICROS)",
            TypeId.Timestamp or TypeId.TimestampTz when physical == PqPhysical.Int64 && logical is { Kind: PqLogicalKind.Timestamp, Unit: PqTimeUnit.Micros } =>
                logical.UtcAdjusted == (type.TypeId == TypeId.TimestampTz) ? null
                    : logical.UtcAdjusted ? "the file's timestamp is UTC-adjusted (timestamptz); the table column is timestamp without time zone"
                    : "the file's timestamp is not UTC-adjusted (timestamp); the table column is timestamptz",
            TypeId.Timestamp or TypeId.TimestampTz when logical.Kind == PqLogicalKind.Timestamp => $"TIMESTAMP({logical.Unit}) found; Iceberg {type} is microseconds",
            TypeId.TimestampNs or TypeId.TimestampTzNs when physical == PqPhysical.Int64 && logical is { Kind: PqLogicalKind.Timestamp, Unit: PqTimeUnit.Nanos } =>
                logical.UtcAdjusted == (type.TypeId == TypeId.TimestampTzNs) ? null : "UTC adjustment does not match the column's time zone semantics",
            TypeId.Uuid when physical == PqPhysical.FixedLenByteArray && column.TypeLength == 16 && logical.Kind == PqLogicalKind.Uuid => null,
            TypeId.Decimal when logical.Kind == PqLogicalKind.Decimal && type is DecimalType dt =>
                logical.Precision == dt.Precision && logical.Scale == dt.Scale ? null : $"DECIMAL({logical.Precision},{logical.Scale}) found; the table column is {type}",
            TypeId.Fixed when physical == PqPhysical.FixedLenByteArray && type is FixedType f && column.TypeLength == f.Length => null,
            _ => Expect(type.TypeId switch
            {
                TypeId.Boolean => "BOOLEAN",
                TypeId.Int => "INT32",
                TypeId.Long => "INT64",
                TypeId.Float => "FLOAT",
                TypeId.Double => "DOUBLE",
                TypeId.String => "BYTE_ARRAY STRING",
                TypeId.Binary => "BYTE_ARRAY",
                TypeId.Date => "INT32 DATE",
                TypeId.Time => "INT64 TIME(MICROS)",
                TypeId.Timestamp => "INT64 TIMESTAMP(MICROS, isAdjustedToUTC=false)",
                TypeId.TimestampTz => "INT64 TIMESTAMP(MICROS, isAdjustedToUTC=true)",
                TypeId.Uuid => "FIXED_LEN_BYTE_ARRAY(16) UUID",
                TypeId.Decimal => "a DECIMAL annotation",
                _ => "a supported representation",
            }),
        };
    }

    private static string Describe(PqLogical l) => l.Kind switch
    {
        PqLogicalKind.None => "(no annotation)",
        PqLogicalKind.Timestamp => $"TIMESTAMP({l.Unit}, isAdjustedToUTC={l.UtcAdjusted})",
        PqLogicalKind.Time => $"TIME({l.Unit})",
        PqLogicalKind.Decimal => $"DECIMAL({l.Precision},{l.Scale})",
        PqLogicalKind.Int => $"INT({l.BitWidth}, signed={l.Signed})",
        _ => l.Kind.ToString().ToUpperInvariant(),
    };

    /// <summary>Set the table's default name mapping from its current schema (needed before adopting files without field IDs).</summary>
    public static Task<TableMetadata> ApplyNameMappingAsync(Table table, CancellationToken ct = default) =>
        table.CommitAsync([new UpdateRequirement.AssertCurrentSchemaId(table.Metadata.CurrentSchemaId)],
            [new MetadataUpdate.SetProperties(new Dictionary<string, string> { [NameMapping.Property] = NameMapping.FromSchema(table.Schema).ToJson() })], ct);
}
