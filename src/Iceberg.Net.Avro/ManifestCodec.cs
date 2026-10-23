using System.Globalization;
using System.Numerics;
using System.Text.Json.Nodes;
using Iceberg.Net.Json;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Avro;

/// <summary>
/// Reads and writes manifests and manifest lists. Reading resolves columns by Iceberg field ID (never by
/// position or name) so files from any writer and any format version decode the same way; writing
/// supports v2 and v3.
/// </summary>
public static class ManifestCodec
{
    // ---------- schema construction ----------

    private static AvroSchema MapOf(int keyId, int valueId, AvroSchema valueSchema) =>
        new AvroArray(new AvroRecord($"k{keyId}_v{valueId}", [new AvroField("key", AvroSchema.Int, keyId), new AvroField("value", valueSchema, valueId)]))
            .WithLogical("map");

    private static AvroSchema ListOf(int elementId, AvroSchema element) => new AvroArray(element).WithProp("element-id", elementId);

    private static AvroField Opt(string name, AvroSchema s, int id) => new(name, AvroSchema.Optional(s), id, DefaultNull: true);

    /// <summary>Avro encoding of an Iceberg primitive (as used for partition tuples).</summary>
    public static AvroSchema ToAvro(IcebergType t) => t switch
    {
        { TypeId: TypeId.Boolean } => AvroSchema.Boolean,
        { TypeId: TypeId.Int } => AvroSchema.Int,
        { TypeId: TypeId.Long } => AvroSchema.Long,
        { TypeId: TypeId.Float } => AvroSchema.Float,
        { TypeId: TypeId.Double } => AvroSchema.Double,
        { TypeId: TypeId.Date } => AvroSchema.Int.WithLogical("date"),
        { TypeId: TypeId.Time } => AvroSchema.Long.WithLogical("time-micros"),
        { TypeId: TypeId.Timestamp } => AvroSchema.Long.WithLogical("timestamp-micros", ("adjust-to-utc", false)),
        { TypeId: TypeId.TimestampTz } => AvroSchema.Long.WithLogical("timestamp-micros", ("adjust-to-utc", true)),
        { TypeId: TypeId.TimestampNs } => AvroSchema.Long.WithLogical("timestamp-nanos", ("adjust-to-utc", false)),
        { TypeId: TypeId.TimestampTzNs } => AvroSchema.Long.WithLogical("timestamp-nanos", ("adjust-to-utc", true)),
        { TypeId: TypeId.String } => AvroSchema.String,
        { TypeId: TypeId.Uuid } => new AvroFixed("uuid_fixed", 16).WithLogical("uuid"),
        FixedType f => new AvroFixed($"fixed_{f.Length}", f.Length),
        { TypeId: TypeId.Binary } => AvroSchema.Bytes,
        DecimalType d => new AvroFixed($"decimal_{d.Precision}_{d.Scale}", DecimalRequiredBytes(d.Precision))
            .WithLogical("decimal", ("precision", d.Precision), ("scale", d.Scale)),
        _ => throw new NotSupportedException($"No Avro mapping for {t}"),
    };

    /// <summary>Smallest byte count whose signed range holds 10^precision - 1 (matches Java TypeUtil.decimalRequiredBytes).</summary>
    public static int DecimalRequiredBytes(int precision)
    {
        var max = BigInteger.Pow(10, precision);
        var bytes = 1;
        while (BigInteger.Pow(2, 8 * bytes - 1) < max) bytes++;
        return bytes;
    }

    public static AvroRecord PartitionSchema(StructType partitionType) =>
        new("r102", partitionType.Fields.Select(f => Opt(f.Name, ToAvro(f.Type), f.Id)).ToList());

    public static AvroRecord ManifestEntrySchema(int formatVersion, StructType partitionType)
    {
        if (formatVersion < 2) throw new NotSupportedException("Writing v1 manifests is not supported; v1 tables are read-only in this library");
        var dataFile = new List<AvroField>
        {
            new("content", AvroSchema.Int, 134),
            new("file_path", AvroSchema.String, 100),
            new("file_format", AvroSchema.String, 101),
            new("partition", PartitionSchema(partitionType), 102),
            new("record_count", AvroSchema.Long, 103),
            new("file_size_in_bytes", AvroSchema.Long, 104),
            Opt("column_sizes", MapOf(117, 118, AvroSchema.Long), 108),
            Opt("value_counts", MapOf(119, 120, AvroSchema.Long), 109),
            Opt("null_value_counts", MapOf(121, 122, AvroSchema.Long), 110),
            Opt("nan_value_counts", MapOf(138, 139, AvroSchema.Long), 137),
            Opt("lower_bounds", MapOf(126, 127, AvroSchema.Bytes), 125),
            Opt("upper_bounds", MapOf(129, 130, AvroSchema.Bytes), 128),
            Opt("key_metadata", AvroSchema.Bytes, 131),
            Opt("split_offsets", ListOf(133, AvroSchema.Long), 132),
            Opt("equality_ids", ListOf(136, AvroSchema.Int), 135),
            Opt("sort_order_id", AvroSchema.Int, 140),
        };
        if (formatVersion >= 3)
        {
            dataFile.Add(Opt("first_row_id", AvroSchema.Long, 142));
            dataFile.Add(Opt("referenced_data_file", AvroSchema.String, 143));
            dataFile.Add(Opt("content_offset", AvroSchema.Long, 144));
            dataFile.Add(Opt("content_size_in_bytes", AvroSchema.Long, 145));
        }
        return new AvroRecord("manifest_entry",
        [
            new("status", AvroSchema.Int, 0),
            Opt("snapshot_id", AvroSchema.Long, 1),
            Opt("sequence_number", AvroSchema.Long, 3),
            Opt("file_sequence_number", AvroSchema.Long, 4),
            new("data_file", new AvroRecord("r2", dataFile), 2),
        ]);
    }

    public static AvroRecord ManifestListSchema(int formatVersion)
    {
        if (formatVersion < 2) throw new NotSupportedException("Writing v1 manifest lists is not supported");
        var summary = new AvroRecord("r508",
        [
            new("contains_null", AvroSchema.Boolean, 509),
            Opt("contains_nan", AvroSchema.Boolean, 518),
            Opt("lower_bound", AvroSchema.Bytes, 510),
            Opt("upper_bound", AvroSchema.Bytes, 511),
        ]);
        var fields = new List<AvroField>
        {
            new("manifest_path", AvroSchema.String, 500),
            new("manifest_length", AvroSchema.Long, 501),
            new("partition_spec_id", AvroSchema.Int, 502),
            new("content", AvroSchema.Int, 517),
            new("sequence_number", AvroSchema.Long, 515),
            new("min_sequence_number", AvroSchema.Long, 516),
            new("added_snapshot_id", AvroSchema.Long, 503),
            new("added_files_count", AvroSchema.Int, 504),
            new("existing_files_count", AvroSchema.Int, 505),
            new("deleted_files_count", AvroSchema.Int, 506),
            new("added_rows_count", AvroSchema.Long, 512),
            new("existing_rows_count", AvroSchema.Long, 513),
            new("deleted_rows_count", AvroSchema.Long, 514),
            Opt("partitions", ListOf(508, summary), 507),
            Opt("key_metadata", AvroSchema.Bytes, 519),
        };
        if (formatVersion >= 3) fields.Add(Opt("first_row_id", AvroSchema.Long, 520));
        return new AvroRecord("manifest_file", fields);
    }

    // ---------- writing ----------

    public static byte[] WriteManifest(
        int formatVersion,
        Schema schema,
        PartitionSpec spec,
        ManifestContent content,
        IEnumerable<ManifestEntry> entries)
    {
        var partitionType = spec.PartitionType(schema);
        var avro = ManifestEntrySchema(formatVersion, partitionType);
        var metadata = new Dictionary<string, string>
        {
            ["schema"] = TypeJson.SchemaToJson(schema).ToJsonString(),
            ["schema-id"] = schema.SchemaId.ToString(CultureInfo.InvariantCulture),
            ["partition-spec"] = TypeJson.SpecFieldsToJson(spec).ToJsonString(),
            ["partition-spec-id"] = spec.SpecId.ToString(CultureInfo.InvariantCulture),
            ["format-version"] = formatVersion.ToString(CultureInfo.InvariantCulture),
            ["content"] = content == ManifestContent.Data ? "data" : "deletes",
        };
        return AvroContainer.Write(avro, metadata, entries.Select(e => (object?)EncodeEntry(formatVersion, partitionType, e)));
    }

    private static object?[] EncodeEntry(int formatVersion, StructType partitionType, ManifestEntry e)
    {
        var f = e.DataFile;
        var partition = partitionType.Fields.Select((pf, i) => ToAvroValue(pf.Type, f.Partition.Count > i ? f.Partition[i] : null)).ToArray();
        var dataFile = new List<object?>
        {
            (int)f.Content, f.FilePath, f.FileFormat, partition, f.RecordCount, f.FileSizeInBytes,
            Kv(f.ColumnSizes), Kv(f.ValueCounts), Kv(f.NullValueCounts), Kv(f.NanValueCounts),
            Kv(f.LowerBounds), Kv(f.UpperBounds), f.KeyMetadata,
            f.SplitOffsets?.Cast<object?>().ToList(), f.EqualityIds?.Cast<object?>().ToList(), f.SortOrderId,
        };
        if (formatVersion >= 3) dataFile.AddRange([f.FirstRowId, f.ReferencedDataFile, f.ContentOffset, f.ContentSizeInBytes]);
        return [(int)e.Status, e.SnapshotId, e.SequenceNumber, e.FileSequenceNumber, dataFile.ToArray()];
    }

    private static List<object?>? Kv<T>(IReadOnlyDictionary<int, T>? map) =>
        map?.OrderBy(kv => kv.Key).Select(kv => (object?)new object?[] { kv.Key, kv.Value }).ToList();

    public static byte[] WriteManifestList(int formatVersion, long snapshotId, long? parentSnapshotId, long sequenceNumber, IEnumerable<ManifestFile> manifests, long? firstRowId = null)
    {
        var avro = ManifestListSchema(formatVersion);
        var metadata = new Dictionary<string, string>
        {
            ["snapshot-id"] = snapshotId.ToString(CultureInfo.InvariantCulture),
            ["parent-snapshot-id"] = parentSnapshotId?.ToString(CultureInfo.InvariantCulture) ?? "null",
            ["sequence-number"] = sequenceNumber.ToString(CultureInfo.InvariantCulture),
            ["format-version"] = formatVersion.ToString(CultureInfo.InvariantCulture),
        };
        if (formatVersion >= 3) metadata["first-row-id"] = (firstRowId ?? 0).ToString(CultureInfo.InvariantCulture);
        return AvroContainer.Write(avro, metadata, manifests.Select(m =>
        {
            var row = new List<object?>
            {
                m.ManifestPath, m.ManifestLength, m.PartitionSpecId, (int)m.Content, m.SequenceNumber, m.MinSequenceNumber,
                m.AddedSnapshotId, m.AddedFilesCount, m.ExistingFilesCount, m.DeletedFilesCount,
                m.AddedRowsCount, m.ExistingRowsCount, m.DeletedRowsCount,
                m.Partitions?.Select(p => (object?)new object?[] { p.ContainsNull, p.ContainsNan, p.LowerBound, p.UpperBound }).ToList(),
                m.KeyMetadata,
            };
            if (formatVersion >= 3) row.Add(m.FirstRowId);
            return (object?)row.ToArray();
        }));
    }

    // ---------- reading ----------

    /// <summary>Accessor over a decoded record that resolves fields by Iceberg field ID.</summary>
    private readonly struct ById(AvroRecord schema, object?[] values)
    {
        public T? Get<T>(int id)
        {
            var idx = IndexOf(id);
            return idx < 0 || values[idx] is null ? default : (T)values[idx]!;
        }

        public T Required<T>(int id) =>
            IndexOf(id) is var idx and >= 0 ? (T)values[idx]! : throw new FormatException($"Manifest is missing required field {id}");

        public long? GetLong(int id) => IndexOf(id) is var idx and >= 0 && values[idx] is { } v ? Convert.ToInt64(v, CultureInfo.InvariantCulture) : null;

        public ById Nested(int id)
        {
            var idx = IndexOf(id);
            return new ById((AvroRecord)schema.Fields[idx].Schema.NonNull, (object?[])values[idx]!);
        }

        public (AvroSchema Schema, object? Value) Raw(int id)
        {
            var idx = IndexOf(id);
            return idx < 0 ? (AvroSchema.Null, null) : (schema.Fields[idx].Schema.NonNull, values[idx]);
        }

        private int IndexOf(int id)
        {
            for (var i = 0; i < schema.Fields.Count; i++) if (schema.Fields[i].FieldId == id) return i;
            return -1;
        }
    }

    public sealed record ManifestListContents(IReadOnlyList<ManifestFile> Manifests, IReadOnlyDictionary<string, string> Metadata);

    public static ManifestListContents ReadManifestList(byte[] bytes) =>
        Corruption.Guard("manifest list", () => ReadManifestListUnguarded(bytes));

    private static ManifestListContents ReadManifestListUnguarded(byte[] bytes)
    {
        var c = AvroContainer.Read(bytes);
        var schema = (AvroRecord)c.Schema;
        var formatVersion = int.Parse(c.Metadata.GetValueOrDefault("format-version", "1"), CultureInfo.InvariantCulture);
        var manifests = c.Records.Select(r =>
        {
            var m = new ById(schema, (object?[])r!);
            var partitions = m.Get<List<object?>>(507);
            List<FieldSummary>? summaries = null;
            if (partitions is not null)
            {
                var (ps, _) = m.Raw(507);
                var elemSchema = (AvroRecord)((AvroArray)ps).Items.NonNull;
                summaries = partitions.Select(p =>
                {
                    var s = new ById(elemSchema, (object?[])p!);
                    return new FieldSummary(s.Required<bool>(509), s.Get<bool?>(518), s.Get<byte[]>(510), s.Get<byte[]>(511));
                }).ToList();
            }
            return new ManifestFile
            {
                ManifestPath = m.Required<string>(500),
                ManifestLength = m.Required<long>(501),
                PartitionSpecId = m.Required<int>(502),
                Content = (ManifestContent)(m.Get<int?>(517) ?? 0),
                SequenceNumber = formatVersion > 1 ? m.GetLong(515) ?? 0 : 0,
                MinSequenceNumber = formatVersion > 1 ? m.GetLong(516) ?? 0 : 0,
                AddedSnapshotId = m.Required<long>(503),
                AddedFilesCount = m.Get<int?>(504) ?? 0,
                ExistingFilesCount = m.Get<int?>(505) ?? 0,
                DeletedFilesCount = m.Get<int?>(506) ?? 0,
                AddedRowsCount = m.GetLong(512) ?? 0,
                ExistingRowsCount = m.GetLong(513) ?? 0,
                DeletedRowsCount = m.GetLong(514) ?? 0,
                Partitions = summaries,
                KeyMetadata = m.Get<byte[]>(519),
                FirstRowId = m.GetLong(520),
            };
        }).ToList();
        return new ManifestListContents(manifests, c.Metadata);
    }

    public sealed record ManifestContents(IReadOnlyList<ManifestEntry> Entries, IReadOnlyDictionary<string, string> Metadata);

    /// <summary>
    /// Read a manifest. When <paramref name="manifest"/> is given, null snapshot IDs and sequence numbers on
    /// ADDED entries are inherited from it, and v3 <c>first_row_id</c>s are assigned, as the spec requires.
    /// </summary>
    public static ManifestContents ReadManifest(byte[] bytes, PartitionSpec spec, Schema schema, ManifestFile? manifest = null) =>
        Corruption.Guard("manifest", () => ReadManifestUnguarded(bytes, spec, schema, manifest));

    private static ManifestContents ReadManifestUnguarded(byte[] bytes, PartitionSpec spec, Schema schema, ManifestFile? manifest)
    {
        var c = AvroContainer.Read(bytes);
        var root = (AvroRecord)c.Schema;
        var partitionType = spec.PartitionType(schema);
        var formatVersion = int.Parse(c.Metadata.GetValueOrDefault("format-version", "1"), CultureInfo.InvariantCulture);
        var nextRowId = manifest?.FirstRowId;

        var entries = new List<ManifestEntry>();
        foreach (var r in c.Records)
        {
            var e = new ById(root, (object?[])r!);
            var status = (ManifestEntryStatus)e.Required<int>(0);
            var f = e.Nested(2);
            var partition = ReadPartition(f, partitionType);
            var recordCount = f.Required<long>(103);

            long? snapshotId = e.GetLong(1);
            long? seq = e.GetLong(3);
            long? fileSeq = e.GetLong(4);
            if (manifest is not null)
            {
                snapshotId ??= manifest.AddedSnapshotId;
                if (formatVersion == 1) { seq = 0; fileSeq = 0; }
                else if (status == ManifestEntryStatus.Added)
                {
                    seq ??= manifest.SequenceNumber;
                    fileSeq ??= manifest.SequenceNumber;
                }
            }

            var content = (FileContent)(f.Get<int?>(134) ?? 0);
            var firstRowId = f.GetLong(142);
            if (firstRowId is null && content == FileContent.Data && status != ManifestEntryStatus.Deleted && nextRowId is { } next)
            {
                firstRowId = next;
                nextRowId = next + recordCount;
            }

            entries.Add(new ManifestEntry(status, snapshotId, seq, fileSeq, new DataFile
            {
                Content = content,
                FilePath = f.Required<string>(100),
                FileFormat = f.Required<string>(101),
                SpecId = spec.SpecId,
                Partition = partition,
                RecordCount = recordCount,
                FileSizeInBytes = f.Required<long>(104),
                ColumnSizes = ReadKv<long>(f, 108),
                ValueCounts = ReadKv<long>(f, 109),
                NullValueCounts = ReadKv<long>(f, 110),
                NanValueCounts = ReadKv<long>(f, 137),
                LowerBounds = ReadKv<byte[]>(f, 125),
                UpperBounds = ReadKv<byte[]>(f, 128),
                KeyMetadata = f.Get<byte[]>(131),
                SplitOffsets = f.Get<List<object?>>(132)?.Select(o => Convert.ToInt64(o, CultureInfo.InvariantCulture)).ToList(),
                EqualityIds = f.Get<List<object?>>(135)?.Select(o => Convert.ToInt32(o, CultureInfo.InvariantCulture)).ToList(),
                SortOrderId = f.Get<int?>(140),
                FirstRowId = firstRowId,
                ReferencedDataFile = f.Get<string>(143),
                ContentOffset = f.GetLong(144),
                ContentSizeInBytes = f.GetLong(145),
            }));
        }
        return new ManifestContents(entries, c.Metadata);
    }

    private static IReadOnlyDictionary<int, T>? ReadKv<T>(ById f, int id)
    {
        var (_, value) = f.Raw(id);
        if (value is null) return null;
        var result = new Dictionary<int, T>();
        if (value is List<object?> pairs)
        {
            foreach (var p in pairs)
            {
                var kv = (object?[])p!;
                result[Convert.ToInt32(kv[0], CultureInfo.InvariantCulture)] = (T)ConvertNumeric<T>(kv[1]!);
            }
        }
        else if (value is Dictionary<string, object?> map) // tolerate string-keyed maps from non-conforming writers
        {
            foreach (var (k, v) in map) result[int.Parse(k, CultureInfo.InvariantCulture)] = (T)ConvertNumeric<T>(v!);
        }
        return result;
    }

    private static object ConvertNumeric<T>(object v) => typeof(T) == typeof(long) ? Convert.ToInt64(v, CultureInfo.InvariantCulture) : v;

    private static object?[] ReadPartition(ById dataFile, StructType partitionType)
    {
        var (schema, value) = dataFile.Raw(102);
        if (value is null || schema is not AvroRecord ps) return new object?[partitionType.Fields.Count];
        var p = new ById(ps, (object?[])value);
        return partitionType.Fields.Select(pf =>
        {
            var (_, raw) = p.Raw(pf.Id);
            return FromAvroValue(pf.Type, raw);
        }).ToArray();
    }

    public static object? ToAvroValue(IcebergType type, object? value) => (type, value) switch
    {
        (_, null) => null,
        ({ TypeId: TypeId.Uuid }, Guid g) => g.ToByteArray(bigEndian: true),
        (DecimalType d, IcebergDecimal v) => SignExtend(v.Unscaled.ToByteArray(isUnsigned: false, isBigEndian: true), DecimalRequiredBytes(d.Precision), v.Unscaled.Sign < 0),
        _ => value,
    };

    public static object? FromAvroValue(IcebergType type, object? value) => (type, value) switch
    {
        (_, null) => null,
        ({ TypeId: TypeId.Uuid }, byte[] b) => Compat.GuidFromBigEndian(b),
        ({ TypeId: TypeId.Uuid }, string s) => Guid.Parse(s),
        (DecimalType d, byte[] b) => new IcebergDecimal(Compat.SignedBigEndian(b), d.Scale),
        ({ TypeId: TypeId.Long or TypeId.Timestamp or TypeId.TimestampTz or TypeId.Time }, int i) => (long)i, // int->long promotion
        ({ TypeId: TypeId.Double }, float f) => (double)f,
        _ => value,
    };

    private static byte[] SignExtend(byte[] bigEndian, int size, bool negative)
    {
        if (bigEndian.Length > size) throw new ArgumentException("Decimal value exceeds its precision");
        var result = new byte[size];
        if (negative) result.AsSpan().Fill(0xFF);
        bigEndian.CopyTo(result, size - bigEndian.Length);
        return result;
    }

    /// <summary>Parses the partition spec embedded in a manifest's metadata (useful when reading orphaned manifests).</summary>
    public static PartitionSpec EmbeddedSpec(IReadOnlyDictionary<string, string> metadata) =>
        new(int.Parse(metadata.GetValueOrDefault("partition-spec-id", "0"), CultureInfo.InvariantCulture),
            TypeJson.SpecFieldsFromJson(JsonNode.Parse(metadata["partition-spec"])!.AsArray()));
}
