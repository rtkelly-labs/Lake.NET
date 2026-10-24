using System.Reflection;
using System.Text.Json.Nodes;
using Iceberg.Net.Catalog;
using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq;

/// <summary>Iceberg schemas derived from C# models.</summary>
public static class IcebergSchema
{
    /// <summary>
    /// The schema for <typeparamref name="T"/>: one column per mapped property (see <see cref="ReflectionRowMapper{T}"/>), with
    /// field IDs from <c>[IcebergField(id)]</c> and the rest assigned from the lowest free ID, siblings before children —
    /// the order catalogs use when they assign IDs on create, so a model without explicit IDs keeps its IDs.
    /// Types follow the member type unless <see cref="IcebergFieldAttribute.Type"/> says otherwise; nullability follows
    /// the member type (nullable value types and nullable reference types are optional). <c>[IcebergKey]</c> members
    /// become identifier fields.
    /// </summary>
    public static Schema FromType<T>(IcebergMapperOptions? options = null) => FromType(typeof(T), options);

    public static Schema FromType(Type type, IcebergMapperOptions? options = null)
    {
        options ??= IcebergMapperOptions.Default;
        var nullability = new NullabilityInfoContext();
        var root = StructOf(type, options, nullability, []);
        var used = new HashSet<int>();
        void Collect(Node n)
        {
            if (n.Id > 0 && !used.Add(n.Id)) throw new InvalidOperationException($"Field ID {n.Id} is used twice in {type.Name}");
            foreach (var c in n.Children) Collect(c);
        }
        foreach (var n in root) Collect(n);
        var next = 0;
        int Fresh()
        {
            do next++; while (used.Contains(next));
            return next;
        }
        void AssignLevel(IReadOnlyList<Node> siblings)
        {
            foreach (var n in siblings) if (n.Id <= 0) n.Id = Fresh();
            foreach (var n in siblings) AssignLevel(n.Children);
        }
        AssignLevel(root);
        var columns = root.Select(FieldOf).ToList();
        var keys = root.Where(n => n.IsKey).ToList();
        foreach (var k in keys.Where(k => !k.Required))
            throw new InvalidOperationException($"{type.Name}.{k.Member} is an [IcebergKey] but optional; identifier fields must be required");
        return new Schema(0, columns, keys.Select(k => k.Id));
    }

    /// <summary>
    /// Create a table whose schema is <see cref="FromType{T}"/>, keeping the model's field IDs
    /// (<see cref="CreateTableRequest.PreserveFieldIds"/>). Catalogs that build the metadata locally (in-memory, Glue,
    /// JDBC) keep them as given. A REST server builds the metadata itself and may renumber, so for other catalogs a model
    /// whose explicit IDs are not the ones a server would assign (1, 2, … siblings before children) is refused before
    /// anything is created.
    /// </summary>
    public static Task<Table> CreateTableAsync<T>(this ICatalog catalog, TableIdentifier identifier, IcebergMapperOptions? options = null,
        IReadOnlyDictionary<string, string>? properties = null, int formatVersion = TableMetadata.DefaultFormatVersion, CancellationToken ct = default) =>
        catalog.CreateTableAsync(identifier, FromType<T>(options), typeof(T).Name, spec: null, properties, formatVersion, ct);

    /// <summary>As <see cref="CreateTableAsync{T}"/> for a schema already derived from a model (and an optional partition spec).</summary>
    public static async Task<Table> CreateTableAsync(this ICatalog catalog, TableIdentifier identifier, Schema schema, string modelName, PartitionSpec? spec,
        IReadOnlyDictionary<string, string>? properties = null, int formatVersion = TableMetadata.DefaultFormatVersion, CancellationToken ct = default)
    {
        var keepsIds = catalog is MetastoreCatalog;
        if (!keepsIds)
        {
            var last = 0;
            var fresh = TypeUtil.AssignFreshIds(schema, () => ++last);
            if (!fresh.SameSchema(schema))
                throw new InvalidOperationException(
                    $"{modelName}'s explicit field IDs are not the ones a REST catalog assigns on create (1, 2, … siblings before children): " +
                    "create the table from the schema and bind the model by name, or number the model's fields in that order");
        }
        var table = await catalog.CreateTableAsync(new CreateTableRequest(identifier, schema, spec, Properties: properties, FormatVersion: formatVersion,
            PreserveFieldIds: true), ct);
        if (!table.Schema.SameSchema(schema))
            throw new InvalidOperationException($"{catalog.Name} created {identifier} with field IDs other than {modelName}'s; bind the model by name");
        return table;
    }

    private sealed class Node
    {
        public required string Name { get; init; }
        public required string Member { get; init; }
        public int Id { get; set; }
        public bool Required { get; init; }
        public bool IsKey { get; init; }
        public string? Doc { get; init; }
        public required Func<Node, IcebergType> Type { get; init; }
        public List<Node> Children { get; } = [];
    }

    private static IcebergType Build(Node n) => n.Type(n);

    private static NestedField FieldOf(Node n) => new(n.Id, n.Name, n.Type(n), n.Required, n.Doc);

    private static List<Node> StructOf(Type type, IcebergMapperOptions options, NullabilityInfoContext ctx, HashSet<Type> visiting)
    {
        if (!visiting.Add(type)) throw new InvalidOperationException($"{type.Name} contains itself; recursive types have no Iceberg schema");
        var nodes = new List<Node>();
        foreach (var m in TypeModel.For(type).Members)
        {
            var attr = m.Attribute;
            var info = ctx.Create(m.Property);
            var required = attr?.Nullability switch
            {
                IcebergNullability.Required => true,
                IcebergNullability.Optional => false,
                _ => !IsNullable(m.Type, info),
            };
            var node = TypeNode(m.Type, info, attr?.Type, options, ctx, visiting, attr?.Name ?? options.NameConverter?.Invoke(m.Name) ?? m.Name, $"{type.Name}.{m.Name}", required);
            nodes.Add(new Node
            {
                Name = node.Name, Member = m.Name, Id = attr?.Id ?? 0, Required = required, IsKey = m.IsKey, Doc = attr?.Doc, Type = node.Type,
            });
            nodes[^1].Children.AddRange(node.Children);
        }
        visiting.Remove(type);
        return nodes;
    }

    private static bool IsNullable(Type t, NullabilityInfo? info) =>
        Nullable.GetUnderlyingType(t) is not null || (!t.IsValueType && info?.ReadState != NullabilityState.NotNull);

    /// <summary>A node for a value of <paramref name="t"/>; its children are the nested fields, elements or key/value it introduces.</summary>
    private static Node TypeNode(Type t, NullabilityInfo? info, string? declared, IcebergMapperOptions options, NullabilityInfoContext ctx,
        HashSet<Type> visiting, string name, string where, bool required)
    {
        if (declared is not null)
        {
            var parsed = TypeJson.TypeFromJson(JsonValue.Create(declared)!);
            if (parsed is not PrimitiveType p || ValueConversions.For(t, p) is null)
                throw new InvalidOperationException($"{where}: [IcebergField(Type = \"{declared}\")] does not fit a {t.Name} member");
            return new Node { Name = name, Member = where, Required = required, Type = _ => parsed };
        }
        if (ValueConversions.DefaultType(t) is { } primitive)
            return new Node { Name = name, Member = where, Required = required, Type = _ => primitive };
        if (TypeModel.MapTypes(t) is { } kv)
        {
            var args = info?.GenericTypeArguments;
            var key = TypeNode(kv.Key, args?.ElementAtOrDefault(0), null, options, ctx, visiting, "key", where + "[key]", true);
            var valueRequired = !IsNullable(kv.Value, args?.ElementAtOrDefault(1));
            var value = TypeNode(kv.Value, args?.ElementAtOrDefault(1), null, options, ctx, visiting, "value", where + "[value]", valueRequired);
            var node = new Node
            {
                Name = name, Member = where, Required = required,
                Type = n => new MapType(n.Children[0].Id, Build(n.Children[0]), n.Children[1].Id, Build(n.Children[1]), n.Children[1].Required),
            };
            node.Children.Add(key);
            node.Children.Add(value);
            return node;
        }
        if (TypeModel.ListElement(t) is { } element)
        {
            var elementInfo = info?.ElementType ?? info?.GenericTypeArguments.ElementAtOrDefault(0);
            var elementRequired = !IsNullable(element, elementInfo);
            var e = TypeNode(element, elementInfo, null, options, ctx, visiting, "element", where + "[]", elementRequired);
            var node = new Node
            {
                Name = name, Member = where, Required = required,
                Type = n => new ListType(n.Children[0].Id, Build(n.Children[0]), n.Children[0].Required),
            };
            node.Children.Add(e);
            return node;
        }
        if (ValueConversions.IsScalar(t))
            throw new InvalidOperationException($"{where}: {t.Name} has no Iceberg type (see docs/linq.md)");
        var fields = StructOf(ValueConversions.Underlying(t), options, ctx, visiting);
        var s = new Node { Name = name, Member = where, Required = required, Type = n => new StructType(n.Children.Select(FieldOf)) };
        s.Children.AddRange(fields);
        return s;
    }
}
