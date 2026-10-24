using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Json;
using Iceberg.Net.Linq;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;
using LinqSetPropertyCalls = Iceberg.Net.Linq;

namespace Iceberg.Net.EntityFrameworkCore.Metadata;

/// <summary>
/// An entity type resolved to an Iceberg table: table name, the schema the model implies (for <c>EnsureCreated</c>),
/// partitioning, key columns, and the row mapper queries and writes use. Built once per entity type of a model.
/// </summary>
internal abstract class IcebergEntityMapping
{
    private static readonly ConditionalWeakTable<IReadOnlyEntityType, IcebergEntityMapping> Cache = new();

    public static IcebergEntityMapping For(IReadOnlyEntityType entityType) =>
        Cache.GetValue(entityType, et => (IcebergEntityMapping)Activator.CreateInstance(
            typeof(IcebergEntityMapping<>).MakeGenericType(et.ClrType), BindingFlags.NonPublic | BindingFlags.Instance, null, [et], null)!);

    protected IcebergEntityMapping(IReadOnlyEntityType entityType)
    {
        EntityType = entityType;
        var snake = entityType.Model.FindAnnotation(IcebergAnnotationNames.Naming)?.Value as string == "snake_case";
        TableName = entityType.GetIcebergTableName() ?? (snake ? IcebergNaming.SnakeCase(entityType.ClrType.Name) : entityType.ClrType.Name);

        // Columns follow the CLR declaration order (EF Core orders properties key-first, then alphabetically).
        var members = TypeModel.For(entityType.ClrType).Members;
        var properties = entityType.GetProperties().ToList();
        var ordered = members.Select(m => properties.FirstOrDefault(p => p.PropertyInfo == m.Property || (p.PropertyInfo is null && p.Name == m.Name)))
            .Where(p => p is not null).Select(p => p!).ToList();
        var unmapped = properties.Except(ordered).ToList();
        if (unmapped.Count > 0)
            throw new InvalidOperationException(
                $"{entityType.DisplayName()}: {string.Join(", ", unmapped.Select(p => p.Name))} cannot be mapped to Iceberg columns: a property needs a public getter " +
                "and a public setter (or a constructor parameter); shadow properties are not supported");
        Properties = ordered;

        var columns = new Dictionary<string, string>();
        var nodes = new List<(IReadOnlyProperty Property, string Name, int Id, IcebergType Type, bool Required)>();
        foreach (var p in ordered)
        {
            var member = members.First(m => m.Name == p.Name);
            var attr = member.Attribute;
            var name = p.GetIcebergColumnName() ?? attr?.Name ?? (snake ? IcebergNaming.SnakeCase(p.Name) : p.Name);
            columns[p.Name] = name;
            var id = p.GetIcebergFieldId() ?? (attr is { Id: > 0 } ? attr.Id : 0);
            nodes.Add((p, name, id, TypeOf(p, attr?.Type), !p.IsNullable));
        }
        ColumnNames = columns;
        var used = new HashSet<int>();
        foreach (var n in nodes.Where(n => n.Id > 0))
            if (!used.Add(n.Id)) throw new InvalidOperationException($"{entityType.DisplayName()}: field ID {n.Id} is used twice");
        var next = 0;
        var fields = new List<NestedField>();
        var fieldIds = new Dictionary<string, int>();
        foreach (var n in nodes)
        {
            var id = n.Id;
            if (id <= 0)
            {
                do next++; while (used.Contains(next));
                id = next;
            }
            fields.Add(new NestedField(id, n.Name, n.Type, n.Required));
            if (n.Id > 0) fieldIds[n.Property.Name] = id;
        }
        var key = entityType.FindPrimaryKey();
        KeyProperties = key?.Properties.ToList() ?? [];
        Schema = new Schema(0, fields, KeyProperties.Select(k => fields.First(f => f.Name == columns[k.Name]).Id));
        if (KeyProperties.Count == 0) Schema = new Schema(0, fields);

        var partition = entityType.GetIcebergPartition();
        if (partition.Count > 0)
        {
            var builder = PartitionSpec.BuilderFor(Schema);
            foreach (var entry in partition)
            {
                var colon = entry.IndexOf(':');
                var property = entry[..colon];
                if (!columns.TryGetValue(property, out var column))
                    throw new InvalidOperationException($"{entityType.DisplayName()}: partition source '{property}' is not a mapped property");
                builder.Add(column, Transform.Parse(entry[(colon + 1)..]));
            }
            Spec = builder.Build();
        }

        // Members bind by the explicit field IDs where the model gives them, else by the column names above; members
        // EF Core does not map are left out.
        MapperOptions = new IcebergMapperOptions
        {
            NameConverter = member => columns.TryGetValue(member, out var c) ? c : "\0unmapped:" + member,
            FieldIds = member => fieldIds.TryGetValue(member, out var id) ? id : null,
            IgnoreUnmatchedMembers = true,
        };
    }

    public IReadOnlyEntityType EntityType { get; }
    public string TableName { get; }
    public IReadOnlyList<IReadOnlyProperty> Properties { get; }
    /// <summary>Property name → column name in the model's schema (the table's current names come from the binding).</summary>
    public IReadOnlyDictionary<string, string> ColumnNames { get; }
    public IReadOnlyList<IReadOnlyProperty> KeyProperties { get; }
    public Schema Schema { get; }
    public PartitionSpec? Spec { get; }
    public IcebergMapperOptions MapperOptions { get; }

    public TableIdentifier Identifier(Namespace ns) => new(ns, TableName);

    private IcebergType TypeOf(IReadOnlyProperty p, string? declared)
    {
        var text = p.GetIcebergType() ?? declared;
        if (text is not null)
        {
            var parsed = TypeJson.TypeFromJson(JsonValue.Create(text)!);
            if (parsed is not PrimitiveType pt || ValueConversions.For(p.ClrType, pt) is null)
                throw new InvalidOperationException($"{EntityType.DisplayName()}.{p.Name}: Iceberg type '{text}' does not fit a {p.ClrType.Name} property");
            return pt;
        }
        var clr = ValueConversions.Underlying(p.ClrType);
        if (clr == typeof(decimal) && p.GetPrecision() is { } precision) return new DecimalType(precision, p.GetScale() ?? 0);
        return ValueConversions.DefaultType(p.ClrType)
            ?? throw new InvalidOperationException($"{EntityType.DisplayName()}.{p.Name}: {p.ClrType.Name} has no Iceberg type (see docs/linq.md)");
    }

    // ---------- per-table operations (generic in the CLR type) ----------

    /// <summary>The table's rows as the entity type, with the snapshot selected.</summary>
    public abstract IQueryable CreateQuery(Table table, IcebergQueryOptions options, SnapshotSelector snapshot);

    /// <summary>The entity's row, aligned to the table's current schema.</summary>
    public abstract object?[] ToRow(Table table, object entity);

    /// <summary>The mapped members bound to the table's current schema; throws when a mapped property has no column.</summary>
    public abstract IReadOnlyList<IcebergMemberBinding> Members(Table table);

    public abstract Task<MutationResult> ExecuteDeleteAsync(IQueryable query, DeleteStrategy strategy, CancellationToken ct);

    /// <summary>EF Core's <c>ExecuteUpdate</c> setters (EF Core 10's array of <c>(property, value)</c> tuples) run as the LINQ layer's.</summary>
    public abstract Task<MergeResult> ExecuteUpdateAsync(IQueryable query, NewArrayExpression setters, DeleteStrategy strategy, CancellationToken ct);

    /// <summary>A tracker for query results: identity resolution against entities the context already tracks, then attach.</summary>
    public abstract Func<object?, object?> Tracker(DbContext context);
}

internal sealed class IcebergEntityMapping<T> : IcebergEntityMapping where T : class
{
    private readonly ReflectionRowMapper<T> _mapper;
    private readonly ConditionalWeakTable<Schema, IIcebergRowBinding<T>> _bindings = new();

    private IcebergEntityMapping(IReadOnlyEntityType entityType) : base(entityType) => _mapper = new ReflectionRowMapper<T>(MapperOptions);

    private IIcebergRowBinding<T> Binding(Table table) => _bindings.GetValue(table.Schema, schema =>
    {
        var binding = _mapper.Bind(schema, NameMapping.ForTable(table.Metadata.Properties));
        var bound = binding.Members.Select(m => m.Member.Name).ToHashSet();
        var missing = Properties.Where(p => !bound.Contains(p.Name)).Select(p => $"{p.Name} ('{ColumnNames[p.Name]}')").ToList();
        if (missing.Count > 0)
            throw new InvalidOperationException(
                $"{EntityType.DisplayName()} maps {string.Join(", ", missing)}, which table {table.Identifier} does not have. Evolve the table " +
                "(Table.UpdateSchemaAsync) or map the property to an existing column (HasIcebergColumnName / HasIcebergFieldId).");
        return binding;
    });

    public override IQueryable CreateQuery(Table table, IcebergQueryOptions options, SnapshotSelector snapshot)
    {
        Binding(table);
        var q = table.AsQueryable(_mapper, options);
        return snapshot switch
        {
            SnapshotSelector.IdSelector id => q.AtSnapshot(id.SnapshotId),
            SnapshotSelector.RefSelector r => q.AtRef(r.Name),
            SnapshotSelector.AsOfSelector a => q.AsOf(a.Instant),
            _ => q,
        };
    }

    public override object?[] ToRow(Table table, object entity) => Binding(table).ToRow((T)entity);

    public override IReadOnlyList<IcebergMemberBinding> Members(Table table) => Binding(table).Members;

    public override Task<MutationResult> ExecuteDeleteAsync(IQueryable query, DeleteStrategy strategy, CancellationToken ct) =>
        ((IQueryable<T>)query).ExecuteDeleteAsync(strategy, ct);

    public override Task<MergeResult> ExecuteUpdateAsync(IQueryable query, NewArrayExpression setters, DeleteStrategy strategy, CancellationToken ct) =>
        ((IQueryable<T>)query).ExecuteUpdateAsync(SetterTranslator.Translate(setters), strategy, ct);

    public override Func<object?, object?> Tracker(DbContext context)
    {
        var local = context.Set<T>().Local;
        var key = EntityType.FindPrimaryKey();
        if (key is null) return e => e; // keyless entity types are never tracked
        var properties = key.Properties.Cast<IProperty>().ToList();
        var getters = properties.Select(p => p.GetGetter()).ToList();
        return e =>
        {
            if (e is not T entity) return e;
            var tracked = local.FindEntry(properties, getters.Select(g => g.GetClrValue(entity)));
            if (tracked is not null) return tracked.Entity;
            context.Attach(entity);
            return entity;
        };
    }

    /// <summary>
    /// Rewrites EF Core 10's setters onto <see cref="LinqSetPropertyCalls.SetPropertyCalls{T}"/>. EF Core records each
    /// <c>SetProperty</c> as <c>new Tuple&lt;Delegate, object&gt;(property, value)</c>, where the value is a lambda over
    /// the entity or a plain expression boxed to <c>object</c>; each becomes the matching <c>SetProperty</c> call.
    /// </summary>
    private static class SetterTranslator
    {
        public static Expression<Func<LinqSetPropertyCalls.SetPropertyCalls<T>, LinqSetPropertyCalls.SetPropertyCalls<T>>> Translate(NewArrayExpression setters)
        {
            var calls = Expression.Parameter(typeof(LinqSetPropertyCalls.SetPropertyCalls<T>), "s");
            Expression body = calls;
            foreach (var setter in setters.Expressions)
            {
                if (setter is not NewExpression { Arguments: [LambdaExpression property, var value] })
                    throw new NotSupportedException($"Unexpected ExecuteUpdate setter {setter}");
                var valueIsLambda = value is LambdaExpression;
                if (!valueIsLambda)
                {
                    if (value is UnaryExpression { NodeType: ExpressionType.Convert } boxed && value.Type == typeof(object)) value = boxed.Operand;
                    if (value.Type != property.ReturnType) value = Expression.Convert(value, property.ReturnType);
                }
                var target = typeof(LinqSetPropertyCalls.SetPropertyCalls<T>).GetMethods()
                    .Single(m => m.Name == "SetProperty" && m.GetParameters()[1].ParameterType.IsGenericType == valueIsLambda)
                    .MakeGenericMethod(property.ReturnType);
                body = Expression.Call(body, target, property, value);
            }
            return Expression.Lambda<Func<LinqSetPropertyCalls.SetPropertyCalls<T>, LinqSetPropertyCalls.SetPropertyCalls<T>>>(body, calls);
        }
    }
}
