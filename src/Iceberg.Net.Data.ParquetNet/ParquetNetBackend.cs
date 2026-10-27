using Parquet;
using Parquet.Data;
using Parquet.Encryption;
using Parquet.Meta;
using Parquet.Schema;
using PqType = Parquet.Meta.Type;

namespace Iceberg.Net.Data.ParquetNet;

/// <summary>
/// Parquet.Net (fully managed: no native binaries, so it runs wherever .NET does) as the Parquet library of
/// <c>Iceberg.Net.Data</c>. It writes the same files as the default ParquetSharp backend (schema, field IDs, annotations,
/// statistics, Zstd, encryption) and reads every file that one reads.
/// </summary>
public static class ParquetNet
{
    /// <summary>Use Parquet.Net for every Parquet file read or written from now on (process-wide).</summary>
    public static void UseAsDefault() => ParquetFiles.Backend = ParquetNetBackend.Instance;

    /// <summary>Go back to ParquetSharp.</summary>
    public static void UseParquetSharp() => ParquetFiles.Backend = ParquetSharpBackend.Instance;

    /// <summary>Use Parquet.Net in the current async flow only (this code and what it awaits), until disposed.</summary>
    public static IDisposable Scope() => ParquetFiles.Use(ParquetNetBackend.Instance);

    /// <summary>Use ParquetSharp in the current async flow only, until disposed.</summary>
    public static IDisposable ParquetSharpScope() => ParquetFiles.Use(ParquetSharpBackend.Instance);
}

internal sealed class ParquetNetBackend : IParquetBackend
{
    public static readonly ParquetNetBackend Instance = new();

    public string Name => "Parquet.Net";

    public async ValueTask<IPqReader> OpenAsync(Stream stream, byte[]? decryptionKey, CancellationToken ct)
    {
        // parquet-cpp does not read the header magic either: over ranged storage that is a request saved
        var options = new ParquetOptions { ValidateHeaderMagic = false };
        if (decryptionKey is not null) options.Decryption = new ParquetDecryptionOptions { FooterKey = decryptionKey };
        var reader = await ParquetReader.CreateAsync(stream, options, leaveStreamOpen: true, ct);
        return new Reader(reader);
    }

    public IPqWriter Create(PqNode schema, PqWriteSettings settings) => new Writer(schema, settings);

    // ---------- schema ----------

    /// <summary>The schema tree of a thrift schema (the root element first, children by <c>num_children</c>).</summary>
    internal static PqNode ToPq(List<SchemaElement> elements)
    {
        var index = 0;
        PqNode Next()
        {
            var se = elements[index++];
            var children = new List<PqNode>();
            for (var i = 0; i < (se.NumChildren ?? 0); i++) children.Add(Next());
            var repetition = se.RepetitionType switch
            {
                FieldRepetitionType.OPTIONAL => PqRepetition.Optional,
                FieldRepetitionType.REPEATED => PqRepetition.Repeated,
                _ => PqRepetition.Required,
            };
            if (se.Type is null || se.NumChildren is > 0)
                return new PqNode { Name = se.Name, Repetition = repetition, FieldId = se.FieldId ?? -1, Logical = ToPq(se), Children = children };
            return new PqNode
            {
                Name = se.Name, Repetition = repetition, FieldId = se.FieldId ?? -1, Logical = ToPq(se), Physical = ToPq(se.Type.Value),
                TypeLength = se.Type == PqType.FIXED_LEN_BYTE_ARRAY ? se.TypeLength ?? 0 : 0,
            };
        }
        return Next();
    }

    private static PqPhysical ToPq(PqType t) => t switch
    {
        PqType.BOOLEAN => PqPhysical.Boolean,
        PqType.INT32 => PqPhysical.Int32,
        PqType.INT64 => PqPhysical.Int64,
        PqType.INT96 => PqPhysical.Int96,
        PqType.FLOAT => PqPhysical.Float,
        PqType.DOUBLE => PqPhysical.Double,
        PqType.BYTE_ARRAY => PqPhysical.ByteArray,
        _ => PqPhysical.FixedLenByteArray,
    };

    private static PqTimeUnit ToPq(Parquet.Meta.TimeUnit u) =>
        u.MILLIS is not null ? PqTimeUnit.Millis : u.MICROS is not null ? PqTimeUnit.Micros : PqTimeUnit.Nanos;

    /// <summary>The annotation, from the logical type, or else from the legacy converted type (as parquet-cpp reads them).</summary>
    private static PqLogical ToPq(SchemaElement se)
    {
        var l = se.LogicalType;
        if (l is not null)
        {
            if (l.STRING is not null) return PqLogical.String;
            if (l.ENUM is not null) return new PqLogical(PqLogicalKind.Enum);
            if (l.JSON is not null) return new PqLogical(PqLogicalKind.Json);
            if (l.BSON is not null) return new PqLogical(PqLogicalKind.Bson);
            if (l.UUID is not null) return PqLogical.Uuid;
            if (l.DATE is not null) return PqLogical.Date;
            if (l.TIME is { } time) return PqLogical.Time(time.IsAdjustedToUTC, ToPq(time.Unit));
            if (l.TIMESTAMP is { } ts) return PqLogical.Timestamp(ts.IsAdjustedToUTC, ToPq(ts.Unit));
            if (l.DECIMAL is { } d) return PqLogical.Decimal(d.Precision, d.Scale);
            if (l.INTEGER is { } i) return PqLogical.Int(i.BitWidth, i.IsSigned);
            if (l.LIST is not null) return PqLogical.List;
            if (l.MAP is not null) return PqLogical.Map;
            if (l.FLOAT16 is not null) return new PqLogical(PqLogicalKind.Float16);
            return new PqLogical(PqLogicalKind.Other);
        }
        return se.ConvertedType switch
        {
            null => PqLogical.None,
            ConvertedType.UTF8 => PqLogical.String,
            ConvertedType.ENUM => new PqLogical(PqLogicalKind.Enum),
            ConvertedType.JSON => new PqLogical(PqLogicalKind.Json),
            ConvertedType.BSON => new PqLogical(PqLogicalKind.Bson),
            ConvertedType.DATE => PqLogical.Date,
            ConvertedType.TIME_MILLIS => PqLogical.Time(true, PqTimeUnit.Millis),
            ConvertedType.TIME_MICROS => PqLogical.Time(true, PqTimeUnit.Micros),
            ConvertedType.TIMESTAMP_MILLIS => PqLogical.Timestamp(true, PqTimeUnit.Millis),
            ConvertedType.TIMESTAMP_MICROS => PqLogical.Timestamp(true, PqTimeUnit.Micros),
            ConvertedType.DECIMAL => PqLogical.Decimal(se.Precision ?? 0, se.Scale ?? 0),
            ConvertedType.INT_8 => PqLogical.Int(8, true),
            ConvertedType.INT_16 => PqLogical.Int(16, true),
            ConvertedType.INT_32 => PqLogical.Int(32, true),
            ConvertedType.INT_64 => PqLogical.Int(64, true),
            ConvertedType.UINT_8 => PqLogical.Int(8, false),
            ConvertedType.UINT_16 => PqLogical.Int(16, false),
            ConvertedType.UINT_32 => PqLogical.Int(32, false),
            ConvertedType.UINT_64 => PqLogical.Int(64, false),
            ConvertedType.INTERVAL => PqLogical.Interval,
            ConvertedType.LIST => PqLogical.List,
            ConvertedType.MAP or ConvertedType.MAP_KEY_VALUE => PqLogical.Map,
            _ => new PqLogical(PqLogicalKind.Other),
        };
    }

    private static Parquet.Meta.TimeUnit ToThrift(PqTimeUnit u) => u switch
    {
        PqTimeUnit.Millis => new Parquet.Meta.TimeUnit { MILLIS = new MilliSeconds() },
        PqTimeUnit.Micros => new Parquet.Meta.TimeUnit { MICROS = new MicroSeconds() },
        _ => new Parquet.Meta.TimeUnit { NANOS = new NanoSeconds() },
    };

    /// <summary>The logical type and the converted type written with it, as parquet-cpp writes them.</summary>
    private static (LogicalType? Logical, ConvertedType? Converted) ToThrift(PqLogical l) => l.Kind switch
    {
        PqLogicalKind.None => (null, null),
        PqLogicalKind.String => (new LogicalType { STRING = new StringType() }, ConvertedType.UTF8),
        PqLogicalKind.Enum => (new LogicalType { ENUM = new EnumType() }, ConvertedType.ENUM),
        PqLogicalKind.Json => (new LogicalType { JSON = new JsonType() }, ConvertedType.JSON),
        PqLogicalKind.Bson => (new LogicalType { BSON = new BsonType() }, ConvertedType.BSON),
        PqLogicalKind.Uuid => (new LogicalType { UUID = new UUIDType() }, null),
        PqLogicalKind.Date => (new LogicalType { DATE = new DateType() }, ConvertedType.DATE),
        // the converted TIME_*/TIMESTAMP_* types mean UTC-adjusted, so they go only with adjusted millis and micros
        PqLogicalKind.Time => (new LogicalType { TIME = new TimeType { IsAdjustedToUTC = l.UtcAdjusted, Unit = ToThrift(l.Unit) } },
            l.UtcAdjusted && l.Unit == PqTimeUnit.Millis ? ConvertedType.TIME_MILLIS : l.UtcAdjusted && l.Unit == PqTimeUnit.Micros ? ConvertedType.TIME_MICROS : null),
        PqLogicalKind.Timestamp => (new LogicalType { TIMESTAMP = new TimestampType { IsAdjustedToUTC = l.UtcAdjusted, Unit = ToThrift(l.Unit) } },
            l.UtcAdjusted && l.Unit == PqTimeUnit.Millis ? ConvertedType.TIMESTAMP_MILLIS : l.UtcAdjusted && l.Unit == PqTimeUnit.Micros ? ConvertedType.TIMESTAMP_MICROS : null),
        PqLogicalKind.Decimal => (new LogicalType { DECIMAL = new DecimalType { Precision = l.Precision, Scale = l.Scale } }, ConvertedType.DECIMAL),
        PqLogicalKind.Int => (new LogicalType { INTEGER = new IntType { BitWidth = (sbyte)l.BitWidth, IsSigned = l.Signed } }, (l.BitWidth, l.Signed) switch
        {
            (8, true) => ConvertedType.INT_8,
            (16, true) => ConvertedType.INT_16,
            (32, true) => ConvertedType.INT_32,
            (64, true) => ConvertedType.INT_64,
            (8, false) => ConvertedType.UINT_8,
            (16, false) => ConvertedType.UINT_16,
            (32, false) => ConvertedType.UINT_32,
            _ => ConvertedType.UINT_64,
        }),
        // INTERVAL has no logical type, only the converted one
        PqLogicalKind.Interval => (null, ConvertedType.INTERVAL),
        _ => throw new NotSupportedException($"Writing the {l} annotation"),
    };

    private static PqType ToThrift(PqPhysical p) => p switch
    {
        PqPhysical.Boolean => PqType.BOOLEAN,
        PqPhysical.Int32 => PqType.INT32,
        PqPhysical.Int64 => PqType.INT64,
        PqPhysical.Int96 => PqType.INT96,
        PqPhysical.Float => PqType.FLOAT,
        PqPhysical.Double => PqType.DOUBLE,
        PqPhysical.ByteArray => PqType.BYTE_ARRAY,
        _ => PqType.FIXED_LEN_BYTE_ARRAY,
    };

    /// <summary>
    /// A Parquet.Net field for a schema node: primitives as <see cref="RawDataField"/> (exactly the declared physical
    /// type and annotations), lists as <c>&lt;list&gt; { repeated list { element } }</c>, maps as
    /// <c>&lt;map&gt; { repeated key_value { key; value } }</c>, other groups as structs; field IDs on every node.
    /// </summary>
    private static Field ToField(PqNode n)
    {
        var nullable = n.Repetition == PqRepetition.Optional;
        Field field;
        if (!n.IsGroup)
        {
            if (n.Repetition == PqRepetition.Repeated) throw new NotSupportedException($"Writing the repeated primitive {n.Name}");
            var (logical, converted) = ToThrift(n.Logical);
            field = new RawDataField(n.Name, ToThrift(n.PhysicalType), nullable, logical, converted,
                n.PhysicalType == PqPhysical.FixedLenByteArray ? n.TypeLength : null);
        }
        else if (n.Logical.Kind == PqLogicalKind.List)
        {
            var repeated = n.Children.Single();
            field = new ListField(n.Name, ToField(repeated.Children.Single()), repeated.Name, nullable);
        }
        else if (n.Logical.Kind == PqLogicalKind.Map)
        {
            var kv = n.Children.Single();
            if (kv.Name != "key_value") throw new NotSupportedException($"Map {n.Name}: Parquet.Net names the repeated group key_value, not {kv.Name}");
            field = new MapField(n.Name, ToField(kv.Children[0]), ToField(kv.Children[1]), nullable);
        }
        else field = new StructField(n.Name, n.Children.Select(ToField).ToList(), nullable);
        field.FieldId = n.FieldId;
        return field;
    }

    // ---------- reading ----------

    private sealed class Reader : IPqReader
    {
        private readonly ParquetReader _reader;
        private readonly DataField[] _fields;
        private readonly List<PqNode> _leaves;

        public Reader(ParquetReader reader)
        {
            _reader = reader;
            Schema = ToPq(reader.Metadata!.Schema);
            _leaves = Schema.Leaves().ToList();
            // Leaves by their path in the file (group names included), as Parquet.Net's model schema names them.
            var byPath = reader.Schema.GetDataFields().ToDictionary(f => string.Join('\u0001', f.Path.ToList()));
            var paths = new List<string>();
            void Walk(PqNode node, List<string> prefix)
            {
                var path = new List<string>(prefix) { node.Name };
                if (!node.IsGroup) paths.Add(string.Join('\u0001', path));
                else foreach (var c in node.Children) Walk(c, path);
            }
            foreach (var c in Schema.Children) Walk(c, []);
            _fields = paths.Select(p => byPath.TryGetValue(p, out var f) ? f : null!).ToArray();
        }

        public PqNode Schema { get; }
        public int RowGroups => _reader.RowGroupCount;
        public long NumRows => _reader.Metadata!.NumRows;
        public long RowGroupRows(int rowGroup) => _reader.RowGroups[rowGroup].RowCount;

        private DataField Field(int leaf) =>
            _fields[leaf] ?? throw new NotSupportedException($"Parquet.Net does not read the column {string.Join('.', _leaves[leaf].Name)} of this file's schema");

        public PqColumnChunk Chunk(int rowGroup, int leaf)
        {
            var group = _reader.RowGroups[rowGroup];
            var meta = group.RowGroup.Columns[leaf].MetaData
                ?? throw new InvalidDataException($"Column chunk {leaf} of row group {rowGroup} has no metadata (an encrypted column without its key)");
            PqStatistics? statistics = null;
            if (meta.Statistics is not null && group.GetStatistics(Field(leaf)) is { } s)
                statistics = s.HasMinMax ? new PqStatistics(s.NullCount ?? 0, s.PhysicalMin, s.PhysicalMax) : new PqStatistics(s.NullCount ?? 0, null, null);
            // the chunk starts at its dictionary page if it has one (some writers leave the offset 0 when not)
            var offset = meta.DictionaryPageOffset is > 0 and var d && d < meta.DataPageOffset ? d : meta.DataPageOffset;
            return new PqColumnChunk(meta.TotalCompressedSize, meta.NumValues, statistics) { Offset = offset };
        }

        public async ValueTask<PqLeafData> ReadLeafAsync(int rowGroup, int leaf, bool strings, CancellationToken ct)
        {
            var field = Field(leaf);
            var group = _reader.RowGroups[rowGroup];
            var levels = checked((int)group.RowGroup.Columns[leaf].MetaData!.NumValues);
            var defs = field.MaxDefinitionLevel > 0 ? new int[levels] : null;
            var reps = field.MaxRepetitionLevel > 0 ? new int[levels] : null;

            async ValueTask<T[]> Read<T>() where T : struct
            {
                var values = new T[levels];
                await group.ReadRawAsync<T>(field, values, defs is null ? (Memory<int>?)null : defs, reps is null ? (Memory<int>?)null : reps, ct);
                var present = levels;
                if (defs is not null)
                {
                    present = 0;
                    var max = field.MaxDefinitionLevel;
                    foreach (var d in defs) if (d == max) present++;
                }
                return present == levels ? values : values[..present];
            }

            Array result = _leaves[leaf].PhysicalType switch
            {
                PqPhysical.Boolean => await Read<bool>(),
                PqPhysical.Int32 => await Read<int>(),
                PqPhysical.Int64 => await Read<long>(),
                PqPhysical.Float => await Read<float>(),
                PqPhysical.Double => await Read<double>(),
                PqPhysical.ByteArray when strings && field.ClrType == typeof(string) => Array.ConvertAll(await Read<ReadOnlyMemory<char>>(), v => v.ToString()),
                PqPhysical.ByteArray when strings => Array.ConvertAll(await Read<ReadOnlyMemory<byte>>(), v => System.Text.Encoding.UTF8.GetString(v.Span)),
                PqPhysical.ByteArray or PqPhysical.FixedLenByteArray => Array.ConvertAll(await Read<ReadOnlyMemory<byte>>(), v => v.ToArray()),
                var p => throw new NotSupportedException($"Reading {p} columns"),
            };
            return new PqLeafData(levels, defs is null ? null : Array.ConvertAll(defs, d => (short)d), reps is null ? null : Array.ConvertAll(reps, r => (short)r), result);
        }

        public void Dispose() => _reader.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    // ---------- writing ----------

    private sealed class Writer : IPqWriter
    {
        private readonly MemoryStream _ms = new();
        private readonly ParquetSchema _schema;
        private readonly DataField[] _leaves;
        private readonly ParquetOptions _options;
        private ParquetWriter? _writer;

        public Writer(PqNode schema, PqWriteSettings settings)
        {
            _schema = new ParquetSchema(schema.Children.Select(ToField).ToArray());
            _leaves = _schema.GetDataFields();
            _options = new ParquetOptions
            {
                // parquet-cpp's default Zstd level (1)
                CompressionMethod = CompressionMethod.Zstd,
                CompressionLevel = System.IO.Compression.CompressionLevel.Fastest,
                WriteStatistics = settings.Statistics,
            };
            if (settings.EncryptionKey is { } key) _options.Encryption = new ParquetEncryptionOptions { FooterKey = key };
            // Parquet.Net dictionary-encodes strings when asked (parquet-cpp tries every column)
            if (settings.Dictionary)
                foreach (var f in _leaves.Where(f => f.ClrType == typeof(string)))
                    _options.ColumnEncodingHints[f.Path.ToString()] = EncodingHint.Dictionary;
        }

        public async ValueTask WriteRowGroupAsync(IReadOnlyList<PqLeafData> leaves, CancellationToken ct)
        {
            _writer ??= await ParquetWriter.CreateAsync(_schema, _ms, _options, cancellationToken: ct);
            using var rg = _writer.CreateRowGroup();
            for (var i = 0; i < leaves.Count; i++)
            {
                var leaf = leaves[i];
                var field = _leaves[i];
                // (null itself would convert, through the int[] operator, to an empty ReadOnlyMemory that is not null)
                var defs = leaf.Defs is null ? (ReadOnlyMemory<int>?)null : Array.ConvertAll(leaf.Defs, d => (int)d);
                var reps = leaf.Reps is null ? (ReadOnlyMemory<int>?)null : Array.ConvertAll(leaf.Reps, r => (int)r);
                switch (leaf.Values)
                {
                    case bool[] v: await rg.WriteAllPartsAsync<bool>(field, v, defs, reps, ct); break;
                    case int[] v: await rg.WriteAllPartsAsync<int>(field, v, defs, reps, ct); break;
                    case long[] v: await rg.WriteAllPartsAsync<long>(field, v, defs, reps, ct); break;
                    case float[] v: await rg.WriteAllPartsAsync<float>(field, v, defs, reps, ct); break;
                    case double[] v: await rg.WriteAllPartsAsync<double>(field, v, defs, reps, ct); break;
                    case string[] v: await rg.WriteAllPartsAsync<ReadOnlyMemory<char>>(field, Array.ConvertAll(v, s => s.AsMemory()), defs, reps, ct); break;
                    case byte[][] v when field.ClrType == typeof(string):
                        await rg.WriteAllPartsAsync<ReadOnlyMemory<char>>(field, Array.ConvertAll(v, b => System.Text.Encoding.UTF8.GetString(b).AsMemory()), defs, reps, ct);
                        break;
                    case byte[][] v: await rg.WriteAllPartsAsync<ReadOnlyMemory<byte>>(field, Array.ConvertAll(v, b => (ReadOnlyMemory<byte>)b), defs, reps, ct); break;
                    default: throw new NotSupportedException($"Writing {leaf.Values.GetType().Name} values");
                }
            }
            rg.CompleteValidate();
        }

        public async ValueTask<byte[]> FinishAsync(CancellationToken ct)
        {
            // a file with no row group still needs its schema and footer
            _writer ??= await ParquetWriter.CreateAsync(_schema, _ms, _options, cancellationToken: ct);
            await _writer.DisposeAsync();
            return _ms.ToArray();
        }

        public void Dispose() => _ms.Dispose();
    }
}
