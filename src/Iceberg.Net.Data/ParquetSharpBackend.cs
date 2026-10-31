using System.Runtime.InteropServices;
using ParquetSharp;
using ParquetSharp.IO;
using ParquetSharp.Schema;
using TimeUnit = ParquetSharp.TimeUnit;

namespace Iceberg.Net.Data;

/// <summary>The data layer over ParquetSharp (parquet-cpp), the default backend.</summary>
internal sealed class ParquetSharpBackend : IParquetBackend
{
    public static readonly ParquetSharpBackend Instance = new();

    public string Name => "ParquetSharp";

    public ValueTask<IPqReader> OpenAsync(Stream stream, byte[]? decryptionKey, CancellationToken ct) =>
        ValueTask.FromResult<IPqReader>(new Reader(stream, decryptionKey));

    public IPqWriter Create(PqNode schema, PqWriteSettings settings) => new Writer(schema, settings);

    // ---------- schema ----------

    internal static PqNode ToPq(Node node)
    {
        var repetition = node.Repetition switch
        {
            Repetition.Required => PqRepetition.Required,
            Repetition.Optional => PqRepetition.Optional,
            _ => PqRepetition.Repeated,
        };
        using var logical = node.LogicalType;
        if (node is PrimitiveNode p)
            return new PqNode
            {
                Name = p.Name, Repetition = repetition, FieldId = p.FieldId, Logical = ToPq(logical), Physical = ToPq(p.PhysicalType),
                TypeLength = p.PhysicalType == PhysicalType.FixedLenByteArray ? p.TypeLength : 0,
            };
        var g = (GroupNode)node;
        return new PqNode { Name = g.Name, Repetition = repetition, FieldId = g.FieldId, Logical = ToPq(logical), Children = g.Fields.Select(ToPq).ToList() };
    }

    internal static PqPhysical ToPq(PhysicalType t) => t switch
    {
        PhysicalType.Boolean => PqPhysical.Boolean,
        PhysicalType.Int32 => PqPhysical.Int32,
        PhysicalType.Int64 => PqPhysical.Int64,
        PhysicalType.Int96 => PqPhysical.Int96,
        PhysicalType.Float => PqPhysical.Float,
        PhysicalType.Double => PqPhysical.Double,
        PhysicalType.ByteArray => PqPhysical.ByteArray,
        PhysicalType.FixedLenByteArray => PqPhysical.FixedLenByteArray,
        _ => throw new NotSupportedException($"Parquet physical type {t}"),
    };

    private static PqTimeUnit ToPq(TimeUnit u) => u switch
    {
        TimeUnit.Millis => PqTimeUnit.Millis,
        TimeUnit.Micros => PqTimeUnit.Micros,
        _ => PqTimeUnit.Nanos,
    };

    internal static PqLogical ToPq(LogicalType logical) => logical switch
    {
        NoneLogicalType => PqLogical.None,
        StringLogicalType => PqLogical.String,
        EnumLogicalType => new PqLogical(PqLogicalKind.Enum),
        JsonLogicalType => new PqLogical(PqLogicalKind.Json),
        BsonLogicalType => new PqLogical(PqLogicalKind.Bson),
        UuidLogicalType => PqLogical.Uuid,
        DateLogicalType => PqLogical.Date,
        TimeLogicalType t => PqLogical.Time(t.IsAdjustedToUtc, ToPq(t.TimeUnit)),
        TimestampLogicalType t => PqLogical.Timestamp(t.IsAdjustedToUtc, ToPq(t.TimeUnit)),
        DecimalLogicalType d => PqLogical.Decimal(d.Precision, d.Scale),
        IntLogicalType i => PqLogical.Int(i.BitWidth, i.IsSigned),
        IntervalLogicalType => PqLogical.Interval,
        ListLogicalType => PqLogical.List,
        MapLogicalType => PqLogical.Map,
        _ => new PqLogical(PqLogicalKind.Other),
    };

    private static TimeUnit ToSharp(PqTimeUnit u) => u switch
    {
        PqTimeUnit.Millis => TimeUnit.Millis,
        PqTimeUnit.Micros => TimeUnit.Micros,
        _ => TimeUnit.Nanos,
    };

    private static LogicalType ToSharp(PqLogical l) => l.Kind switch
    {
        PqLogicalKind.None => LogicalType.None(),
        PqLogicalKind.String => LogicalType.String(),
        PqLogicalKind.Enum => LogicalType.Enum(),
        PqLogicalKind.Json => LogicalType.Json(),
        PqLogicalKind.Bson => LogicalType.Bson(),
        PqLogicalKind.Uuid => LogicalType.Uuid(),
        PqLogicalKind.Date => LogicalType.Date(),
        PqLogicalKind.Time => LogicalType.Time(l.UtcAdjusted, ToSharp(l.Unit)),
        PqLogicalKind.Timestamp => LogicalType.Timestamp(l.UtcAdjusted, ToSharp(l.Unit)),
        PqLogicalKind.Decimal => LogicalType.Decimal(l.Precision, l.Scale),
        PqLogicalKind.Int => LogicalType.Int(l.BitWidth, l.Signed),
        PqLogicalKind.Interval => LogicalType.Interval(),
        PqLogicalKind.List => LogicalType.List(),
        PqLogicalKind.Map => LogicalType.Map(),
        _ => throw new NotSupportedException($"Writing the {l} annotation"),
    };

    private static PhysicalType ToSharp(PqPhysical p) => p switch
    {
        PqPhysical.Boolean => PhysicalType.Boolean,
        PqPhysical.Int32 => PhysicalType.Int32,
        PqPhysical.Int64 => PhysicalType.Int64,
        PqPhysical.Int96 => PhysicalType.Int96,
        PqPhysical.Float => PhysicalType.Float,
        PqPhysical.Double => PhysicalType.Double,
        PqPhysical.ByteArray => PhysicalType.ByteArray,
        _ => PhysicalType.FixedLenByteArray,
    };

    private static Node ToSharp(PqNode n)
    {
        var repetition = n.Repetition switch
        {
            PqRepetition.Required => Repetition.Required,
            PqRepetition.Optional => Repetition.Optional,
            _ => Repetition.Repeated,
        };
        using var logical = ToSharp(n.Logical);
        if (!n.IsGroup)
            return new PrimitiveNode(n.Name, repetition, logical, ToSharp(n.PhysicalType), n.PhysicalType == PqPhysical.FixedLenByteArray ? n.TypeLength : -1, n.FieldId);
        var children = n.Children.Select(ToSharp).ToArray();
        try
        {
            return new GroupNode(n.Name, repetition, children, n.Logical.Kind == PqLogicalKind.None ? null : logical, n.FieldId);
        }
        finally
        {
            foreach (var c in children) c.Dispose();
        }
    }

    // ---------- reading ----------

    private sealed class Reader : IPqReader
    {
        private readonly ManagedRandomAccessFile _input;
        private readonly ReaderProperties _properties;
        private readonly ParquetFileReader _reader;
        private readonly FileMetaData _meta;
        private readonly RowGroupReader?[] _groups;
        private readonly List<PqNode> _leaves;

        public Reader(Stream stream, byte[]? key)
        {
            _input = new ManagedRandomAccessFile(stream);
            _properties = ParquetWriteOptions.ReaderProperties(key);
            _reader = new ParquetFileReader(_input, _properties);
            _meta = _reader.FileMetaData;
            using var root = _meta.Schema.GroupNode;
            Schema = ToPq(root);
            _leaves = Schema.Leaves().ToList();
            _groups = new RowGroupReader?[_meta.NumRowGroups];
        }

        public PqNode Schema { get; }
        public int RowGroups => _meta.NumRowGroups;
        public long NumRows => _meta.NumRows;

        private RowGroupReader Group(int rg) => _groups[rg] ??= _reader.RowGroup(rg);

        public long RowGroupRows(int rowGroup) => Group(rowGroup).MetaData.NumRows;

        public PqColumnChunk Chunk(int rowGroup, int leaf)
        {
            using var chunk = Group(rowGroup).MetaData.GetColumnChunkMetaData(leaf);
            PqStatistics? statistics = null;
            if (chunk.IsStatsSet)
            {
                using var stats = chunk.Statistics;
                if (stats is not null)
                {
                    var (min, max) = stats.HasMinMax ? MinMax(stats, _leaves[leaf].TypeLength) : (null, null);
                    statistics = new PqStatistics(stats.NullCount, min, max);
                }
            }
            return new PqColumnChunk(chunk.TotalCompressedSize, chunk.NumValues, statistics);
        }

        private static (object?, object?) MinMax(Statistics stats, int width)
        {
            switch (stats)
            {
                case Statistics<bool> b: return (b.Min, b.Max);
                case Statistics<int> i: return (i.Min, i.Max);
                case Statistics<long> l: return (l.Min, l.Max);
                case Statistics<float> f: return (f.Min, f.Max);
                case Statistics<double> d: return (d.Min, d.Max);
                case Statistics<ByteArray> ba: return (Copy(ba.Min), Copy(ba.Max));
                case Statistics<FixedLenByteArray> fl: return (Copy(fl.Min.Pointer, width), Copy(fl.Max.Pointer, width));
                default: return (null, null);
            }
        }

        public ValueTask<PqLeafData> ReadLeafAsync(int rowGroup, int leaf, PqValues wanted, CancellationToken ct)
        {
            using var col = Group(rowGroup).Column(leaf);
            var descr = col.ColumnDescriptor;
            int levels;
            using (var chunk = Group(rowGroup).MetaData.GetColumnChunkMetaData(leaf)) levels = checked((int)chunk.NumValues);
            var defs = new short[levels];
            var reps = new short[levels];
            Array values = col switch
            {
                ColumnReader<bool> r => ReadAll(r, defs, reps, levels),
                ColumnReader<int> r => ReadAll(r, defs, reps, levels),
                ColumnReader<long> r => ReadAll(r, defs, reps, levels),
                ColumnReader<float> r => ReadAll(r, defs, reps, levels),
                ColumnReader<double> r => ReadAll(r, defs, reps, levels),
                ColumnReader<ByteArray> r when wanted == PqValues.Strings => ReadBytes(r, defs, reps, levels, v => v.Length == 0 ? "" : Marshal.PtrToStringUTF8(v.Pointer, v.Length)),
                ColumnReader<ByteArray> r => ReadBytes(r, defs, reps, levels, v => Copy(v)),
                ColumnReader<FixedLenByteArray> r when wanted == PqValues.Uuids && descr.TypeLength == 16 => ReadBytes(r, defs, reps, levels, v => Uuid(v.Pointer)),
                ColumnReader<FixedLenByteArray> r => ReadBytes(r, defs, reps, levels, v => Copy(v.Pointer, descr.TypeLength)),
                _ => throw new NotSupportedException($"Reading {descr.PhysicalType} columns"),
            };
            return ValueTask.FromResult(new PqLeafData(levels,
                descr.MaxDefinitionLevel > 0 ? defs : null, descr.MaxRepetitionLevel > 0 ? reps : null, values));
        }

        private static T[] ReadAll<T>(ColumnReader<T> reader, short[] defs, short[] reps, int levels) where T : unmanaged
        {
            var values = new T[levels];
            int read = 0, valuesRead = 0;
            while (read < levels && reader.HasNext)
            {
                var n = (int)reader.ReadBatch(levels - read, defs.AsSpan(read), reps.AsSpan(read), values.AsSpan(valuesRead), out var v);
                if (n == 0) break;
                read += n;
                valuesRead += (int)v;
            }
            return valuesRead == values.Length ? values : values[..valuesRead];
        }

        private static TOut[] ReadBytes<TIn, TOut>(ColumnReader<TIn> reader, short[] defs, short[] reps, int levels, Func<TIn, TOut> copy) where TIn : unmanaged
        {
            const int batch = 4096;
            var buffer = new TIn[Math.Min(batch, Math.Max(1, levels))];
            var values = new List<TOut>(levels);
            var read = 0;
            while (read < levels && reader.HasNext)
            {
                var n = (int)reader.ReadBatch(Math.Min(buffer.Length, levels - read), defs.AsSpan(read), reps.AsSpan(read), buffer, out var v);
                if (n == 0) break;
                read += n;
                for (var i = 0; i < v; i++) values.Add(copy(buffer[i])); // copy now: the buffers are reused by the next batch
            }
            return values.ToArray();
        }

        public void Dispose()
        {
            foreach (var g in _groups) g?.Dispose();
            _meta.Dispose();
            _reader.Dispose();
            _properties.Dispose();
            _input.Dispose();
        }
    }

    private static byte[] Copy(ByteArray v) => Copy(v.Pointer, v.Length);

    /// <summary>A UUID from its 16 big-endian bytes, without an intermediate array.</summary>
    private static Guid Uuid(IntPtr pointer)
    {
        Span<byte> bytes = stackalloc byte[16];
        for (var i = 0; i < 16; i++) bytes[i] = Marshal.ReadByte(pointer, i);
        return new Guid(bytes, bigEndian: true);
    }

    private static byte[] Copy(IntPtr pointer, int length)
    {
        var bytes = new byte[length];
        if (length > 0) Marshal.Copy(pointer, bytes, 0, length);
        return bytes;
    }

    // ---------- writing ----------

    private sealed class Writer : IPqWriter
    {
        private readonly MemoryStream _ms = new();
        private readonly ManagedOutputStream _output;
        private readonly GroupNode _root;
        private readonly FileEncryptionProperties? _encryption;
        private readonly WriterProperties _properties;
        private readonly ParquetFileWriter _writer;
        private bool _closed;

        public Writer(PqNode schema, PqWriteSettings settings)
        {
            _root = (GroupNode)ToSharp(schema);
            using var builder = new WriterPropertiesBuilder().Compression(Compression.Zstd);
            if (!settings.Statistics) builder.DisableStatistics();
            if (!settings.Dictionary) builder.DisableDictionary();
            if (settings.EncryptionKey is { } key)
            {
                using var encryption = new FileEncryptionPropertiesBuilder(key);
                _encryption = encryption.Build();
                builder.Encryption(_encryption);
            }
            _properties = builder.Build();
            _output = new ManagedOutputStream(_ms, leaveOpen: true);
            _writer = new ParquetFileWriter(_output, _root, _properties);
        }

        public ValueTask WriteRowGroupAsync(IReadOnlyList<PqLeafData> leaves, CancellationToken ct)
        {
            using var rg = _writer.AppendRowGroup();
            foreach (var leaf in leaves)
            {
                using var col = rg.NextColumn();
                Write(col, leaf);
            }
            return ValueTask.CompletedTask;
        }

        private static void Write(ColumnWriter col, PqLeafData leaf)
        {
            var defs = leaf.Defs is null ? ReadOnlySpan<short>.Empty : leaf.Defs;
            var reps = leaf.Reps is null ? ReadOnlySpan<short>.Empty : leaf.Reps;
            switch (col)
            {
                case ColumnWriter<bool> w: w.WriteBatch(leaf.Levels, defs, reps, (bool[])leaf.Values); break;
                case ColumnWriter<int> w: w.WriteBatch(leaf.Levels, defs, reps, (int[])leaf.Values); break;
                case ColumnWriter<long> w: w.WriteBatch(leaf.Levels, defs, reps, (long[])leaf.Values); break;
                case ColumnWriter<float> w: w.WriteBatch(leaf.Levels, defs, reps, (float[])leaf.Values); break;
                case ColumnWriter<double> w: w.WriteBatch(leaf.Levels, defs, reps, (double[])leaf.Values); break;
                case ColumnWriter<ByteArray> or ColumnWriter<FixedLenByteArray>:
                {
                    var bytes = leaf.Values is string[] s ? s.Select(System.Text.Encoding.UTF8.GetBytes).ToArray() : (byte[][])leaf.Values;
                    var fixedLength = col.ColumnDescriptor.PhysicalType == PhysicalType.FixedLenByteArray ? col.ColumnDescriptor.TypeLength : -1;
                    if (fixedLength >= 0 && bytes.Any(b => b.Length != fixedLength))
                        throw new ArgumentException($"Column {col.ColumnDescriptor.Path.ToDotString()} expects {fixedLength}-byte values");
                    var buffer = new byte[Math.Max(1, bytes.Sum(b => (long)b.Length))];
                    var offsets = new int[bytes.Length];
                    for (int i = 0, at = 0; i < bytes.Length; at += bytes[i].Length, i++) { offsets[i] = at; bytes[i].CopyTo(buffer, at); }
                    var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                    try
                    {
                        var start = handle.AddrOfPinnedObject();
                        if (col is ColumnWriter<ByteArray> bw)
                            bw.WriteBatch(leaf.Levels, defs, reps, bytes.Select((b, i) => new ByteArray(start + offsets[i], b.Length)).ToArray());
                        else
                            ((ColumnWriter<FixedLenByteArray>)col).WriteBatch(leaf.Levels, defs, reps, bytes.Select((_, i) => new FixedLenByteArray(start + offsets[i])).ToArray());
                    }
                    finally
                    {
                        handle.Free();
                    }
                    break;
                }
                default: throw new NotSupportedException($"Writing {col.ColumnDescriptor.PhysicalType} columns");
            }
        }

        public ValueTask<byte[]> FinishAsync(CancellationToken ct)
        {
            if (!_closed)
            {
                _writer.Close();
                _closed = true;
            }
            _output.Dispose();
            return ValueTask.FromResult(_ms.ToArray());
        }

        public void Dispose()
        {
            _writer.Dispose();
            _output.Dispose();
            _properties.Dispose();
            _encryption?.Dispose();
            _root.Dispose();
            _ms.Dispose();
        }
    }
}
