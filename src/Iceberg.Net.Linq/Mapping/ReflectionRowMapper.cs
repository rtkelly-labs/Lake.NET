using System.Collections;
using System.Collections.Concurrent;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq;

/// <summary>
/// The reflection <see cref="IIcebergRowMapper{T}"/>. A property binds to a field by <c>[IcebergField(id)]</c>, else by
/// name: <see cref="IcebergFieldAttribute.Name"/> or the (converted) property name, matched exactly against the column
/// names, then against the table's name-mapping aliases, then (with <see cref="IcebergMapperOptions.CaseInsensitive"/>)
/// case-insensitively. Struct columns map to nested classes or records, lists to arrays/<c>List&lt;T&gt;</c>/sets and
/// maps to dictionaries, recursively by the same rules.
/// </summary>
public sealed class ReflectionRowMapper<T> : IIcebergRowMapper<T>
{
    private readonly ConcurrentDictionary<(Schema, NameMapping?), IIcebergRowBinding<T>> _bindings = new();

    public ReflectionRowMapper(IcebergMapperOptions? options = null) => Options = options ?? IcebergMapperOptions.Default;

    public IcebergMapperOptions Options { get; }

    public IIcebergRowBinding<T> Bind(Schema schema, NameMapping? nameMapping = null) =>
        _bindings.GetOrAdd((schema, nameMapping), k => new Binding(k.Item1, StructBinding.Create(typeof(T), k.Item1.Columns, k.Item2?.Fields, Options, "")));

    private sealed class Binding(Schema schema, StructBinding inner) : IIcebergRowBinding<T>
    {
        private readonly ConcurrentDictionary<Schema, Func<object?[], T>> _readers = new(ReferenceEqualityComparer.Instance);

        public Schema Schema { get; } = schema;
        public IReadOnlyList<IcebergMemberBinding> Members => inner.Members;

        public Func<object?[], T> CreateReader(Schema rowSchema) => _readers.GetOrAdd(rowSchema, s =>
        {
            var read = inner.CreateReader(s.Columns);
            return row => (T)read(row);
        });

        public object?[] ToRow(T item) => inner.ToRow(item!);
    }
}

/// <summary>Non-generic core of the reflection mapper: one CLR type bound to the fields of one struct (or schema).</summary>
internal sealed class StructBinding
{
    private readonly TypeModel _model;
    private readonly IReadOnlyList<NestedField> _fields;
    private readonly (ModelMember Member, int Position, Func<object?, object?> Write)[] _writes;
    private Func<object?[], object>? _factory;

    public IReadOnlyList<IcebergMemberBinding> Members { get; }

    private readonly IReadOnlyList<Conversion> _conversions;

    private StructBinding(TypeModel model, IReadOnlyList<NestedField> fields, IReadOnlyList<IcebergMemberBinding> members,
        (ModelMember, int, Func<object?, object?>)[] writes, IReadOnlyList<Conversion> conversions)
    {
        _conversions = conversions;
        _model = model;
        _fields = fields;
        Members = members;
        _writes = writes;
    }

    public static StructBinding Create(Type type, IReadOnlyList<NestedField> fields, IReadOnlyList<MappedField>? mapped, IcebergMapperOptions options, string path)
    {
        var model = TypeModel.For(type);
        var members = new List<IcebergMemberBinding>();
        var writes = new List<(ModelMember, int, Func<object?, object?>)>();
        var byId = new Dictionary<int, ModelMember>();
        var conversions = new List<Conversion>();
        foreach (var m in model.Members)
        {
            var field = Resolve(m, fields, mapped, options, path, type);
            if (field is null) continue;
            if (byId.TryGetValue(field.Id, out var other))
                throw new InvalidOperationException($"{type.Name}.{m.Name} and {type.Name}.{other.Name} both map to field {field.Id} '{path}{field.Name}'");
            byId[field.Id] = m;
            var childMapped = mapped?.FirstOrDefault(x => x.FieldId == field.Id)?.Fields;
            var conv = Converters(m.Type, field.Type, childMapped, options, $"{path}{field.Name}");
            members.Add(new IcebergMemberBinding(m.Property, m.Type, field, conv.Write, conv.Read, conv.Children));
            conversions.Add(conv);
            writes.Add((m, fields.ToList().FindIndex(f => f.Id == field.Id), conv.Write));
        }
        return new StructBinding(model, fields, members, [.. writes], conversions);
    }

    private static NestedField? Resolve(ModelMember m, IReadOnlyList<NestedField> fields, IReadOnlyList<MappedField>? mapped, IcebergMapperOptions options, string path, Type type)
    {
        var explicitId = m.Attribute is { Id: > 0 } a ? a.Id : path.Length == 0 ? options.FieldIds?.Invoke(m.Name) : null;
        if (explicitId is { } fieldId)
            return fields.FirstOrDefault(f => f.Id == fieldId)
                ?? (options.IgnoreUnmatchedMembers ? null : throw new InvalidOperationException($"{type.Name}.{m.Name} is bound to field ID {fieldId}, which is not in the schema{(path.Length > 0 ? $" at '{path.TrimEnd('.')}'" : "")}"));
        var name = m.Attribute?.Name ?? options.NameConverter?.Invoke(m.Name) ?? m.Name;
        var exact = fields.FirstOrDefault(f => f.Name == name);
        if (exact is not null) return exact;
        if (mapped?.FirstOrDefault(x => x.FieldId is not null && x.Names.Contains(name)) is { FieldId: { } aliasId } && fields.FirstOrDefault(f => f.Id == aliasId) is { } alias)
            return alias;
        if (options.CaseInsensitive)
        {
            var matches = fields.Where(f => string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase)).ToList();
            if (matches.Count == 1) return matches[0];
            if (matches.Count > 1) throw new InvalidOperationException($"{type.Name}.{m.Name} matches {matches.Count} columns case-insensitively: {string.Join(", ", matches.Select(f => f.Name))}");
        }
        return options.IgnoreUnmatchedMembers ? null
            : throw new InvalidOperationException($"{type.Name}.{m.Name} matches no column (looked for '{path}{name}'; columns: {string.Join(", ", fields.Select(f => f.Name))}). " +
                "Use [IcebergField(id)], [IcebergField(Name = ...)], [IcebergIgnore], or IcebergMapperOptions.");
    }

    internal sealed record Conversion(Func<object?, object?> Write, Func<object?, object?> Read, Func<IcebergType, Func<object?, object?>> ReadFor,
        IReadOnlyList<IcebergMemberBinding>? Children);

    /// <summary>
    /// Write and read conversions for a member of <paramref name="clr"/> bound to a field of <paramref name="type"/>;
    /// <see cref="Conversion.ReadFor"/> reads the same field as another schema has it (promoted, or a struct projected or
    /// from an older schema), matching nested fields by ID.
    /// </summary>
    internal static Conversion Converters(Type clr, IcebergType type, IReadOnlyList<MappedField>? mapped, IcebergMapperOptions options, string column)
    {
        switch (type)
        {
            case PrimitiveType p:
            {
                var scalar = ValueConversions.For(clr, p)
                    ?? throw new InvalidOperationException($"Column '{column}' is {p}; a {Describe(clr)} member cannot hold it (see docs/linq.md for the supported pairs)");
                Func<object?, object?> read = v => v is null ? null : scalar.From(v);
                return new(v => v is null ? null : scalar.To(v), read, t =>
                {
                    if (t.Equals(p)) return read;
                    var other = t is PrimitiveType tp ? ValueConversions.For(clr, tp) : null;
                    if (other is null) throw new InvalidOperationException($"Column '{column}' is {t} in the schema read; a {Describe(clr)} member cannot hold it");
                    return v => v is null ? null : other.From(v);
                }, null);
            }
            case StructType s:
            {
                if (ValueConversions.IsScalar(clr) || TypeModel.ListElement(clr) is not null || TypeModel.MapTypes(clr) is not null)
                    throw new InvalidOperationException($"Column '{column}' is a struct; a {Describe(clr)} member cannot hold it");
                var nested = Create(ValueConversions.Underlying(clr), s.Fields, mapped, options, column + ".");
                Func<object?, object?> Reader(StructType st)
                {
                    var r = nested.CreateReader(st.Fields);
                    return v => v is null ? null : r((object?[])v);
                }
                var read = Reader(s);
                return new(v => v is null ? null : nested.ToRow(v), read,
                    t => t.Equals(s) ? read : Reader(t as StructType ?? throw new InvalidOperationException($"Column '{column}' is {t} in the schema read, not a struct")),
                    nested.Members);
            }
            case ListType l:
            {
                var element = TypeModel.ListElement(clr) ?? throw new InvalidOperationException($"Column '{column}' is a list; a {Describe(clr)} member cannot hold it");
                var e = Converters(element, l.ElementType, mapped?.FirstOrDefault(x => x.FieldId == l.ElementId)?.Fields, options, column + ".element");
                var make = TypeModel.ListFactory(clr, element);
                Func<object?, object?> Reader(Func<object?, object?> er) => v => v is null ? null : make(((IEnumerable)v).Cast<object?>().Select(er));
                var read = Reader(e.Read);
                return new(v => v is null ? null : ((IEnumerable)v).Cast<object?>().Select(e.Write).ToList(), read,
                    t => t.Equals(l) ? read : Reader(e.ReadFor((t as ListType ?? throw new InvalidOperationException($"Column '{column}' is {t} in the schema read, not a list")).ElementType)),
                    null);
            }
            case MapType mt:
            {
                var kv = TypeModel.MapTypes(clr) ?? throw new InvalidOperationException($"Column '{column}' is a map; a {Describe(clr)} member cannot hold it");
                var k = Converters(kv.Key, mt.KeyType, mapped?.FirstOrDefault(x => x.FieldId == mt.KeyId)?.Fields, options, column + ".key");
                var val = Converters(kv.Value, mt.ValueType, mapped?.FirstOrDefault(x => x.FieldId == mt.ValueId)?.Fields, options, column + ".value");
                var makeMap = TypeModel.MapFactory(kv.Key, kv.Value);
                Func<object?, object?> Reader(Func<object?, object?> kr, Func<object?, object?> vr) =>
                    v => v is null ? null : makeMap(Entries(v).Select(e => new KeyValuePair<object, object?>(kr(e.Key)!, vr(e.Value))));
                var read = Reader(k.Read, val.Read);
                return new(v => v is null ? null : Entries(v).ToDictionary(e => k.Write(e.Key)!, e => val.Write(e.Value)), read,
                    t => t.Equals(mt) ? read : t is MapType other ? Reader(k.ReadFor(other.KeyType), val.ReadFor(other.ValueType))
                        : throw new InvalidOperationException($"Column '{column}' is {t} in the schema read, not a map"),
                    null);
            }
            default:
                throw new InvalidOperationException($"Column '{column}' has unsupported type {type}");
        }
    }

    /// <summary>The entries of any dictionary value (<see cref="IDictionary"/>, or an enumerable of <c>KeyValuePair</c>).</summary>
    private static IEnumerable<(object Key, object? Value)> Entries(object v)
    {
        if (v is IDictionary d)
        {
            var e = d.GetEnumerator();
            while (e.MoveNext()) yield return (e.Key, e.Value);
            yield break;
        }
        foreach (var item in (IEnumerable)v)
        {
            var t = item!.GetType();
            yield return (t.GetProperty("Key")!.GetValue(item)!, t.GetProperty("Value")!.GetValue(item));
        }
    }

    private static string Describe(Type t) => Nullable.GetUnderlyingType(t) is { } u ? u.Name + "?" : t.Name;

    /// <summary>The struct value (or row) for <paramref name="item"/>, aligned to the bound fields.</summary>
    public object?[] ToRow(object item)
    {
        var row = new object?[_fields.Count];
        foreach (var (member, position, write) in _writes)
        {
            try
            {
                row[position] = write(member.Getter(item));
            }
            catch (Exception e) when (e is ArgumentException or InvalidCastException or OverflowException or FormatException)
            {
                throw new ArgumentException($"{_model.Type.Name}.{member.Name}: {e.Message}", e);
            }
        }
        return row;
    }

    /// <summary>
    /// A reader for struct values aligned to <paramref name="readFields"/> (the struct as a scan returns it: possibly
    /// projected, or from an older schema). Members are found by field ID; conversions follow the read field's type, so
    /// promoted columns (int → long, float → double, wider decimals) still read.
    /// </summary>
    public Func<object?[], object> CreateReader(IReadOnlyList<NestedField> readFields)
    {
        _factory ??= _model.CreateFactory();
        var factory = _factory;
        var plan = new List<(int MemberIndex, int Position, Func<object?, object?> Read)>();
        for (var i = 0; i < _model.Members.Count; i++)
        {
            var member = _model.Members[i];
            var bi = -1;
            for (var b = 0; b < Members.Count; b++) if (Members[b].Member == member.Property) { bi = b; break; }
            if (bi < 0) continue;
            var bound = Members[bi];
            var position = -1;
            for (var p = 0; p < readFields.Count; p++) if (readFields[p].Id == bound.Field.Id) { position = p; break; }
            if (position < 0) continue;
            var read = _conversions[bi].ReadFor(readFields[position].Type);
            plan.Add((i, position, read));
        }
        var count = _model.Members.Count;
        var typeName = _model.Type.Name;
        var planArray = plan.ToArray();
        return row =>
        {
            var values = new object?[count];
            Array.Fill(values, TypeModel.Missing);
            foreach (var (memberIndex, position, read) in planArray)
            {
                try
                {
                    values[memberIndex] = read(row[position]);
                }
                catch (Exception e) when (e is InvalidCastException or OverflowException or FormatException or ArgumentException)
                {
                    throw new InvalidOperationException($"{typeName}.{_model.Members[memberIndex].Name}: cannot read {row[position]} ({e.Message})", e);
                }
            }
            return factory(values);
        };
    }
}
