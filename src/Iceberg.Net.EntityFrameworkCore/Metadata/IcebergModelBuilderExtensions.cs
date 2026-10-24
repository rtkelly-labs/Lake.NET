using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Iceberg.Net.Transforms;

// Same namespace as EF Core's builder extensions.
namespace Microsoft.EntityFrameworkCore;

/// <summary>Annotation names the provider reads from the model.</summary>
public static class IcebergAnnotationNames
{
    public const string Prefix = "Iceberg:";
    public const string TableName = Prefix + "TableName";
    public const string ColumnName = Prefix + "ColumnName";
    public const string FieldId = Prefix + "FieldId";
    public const string FieldType = Prefix + "FieldType";
    /// <summary>On an entity type: the partition fields, as <c>property:transform</c> strings (<c>CreatedAt:day</c>, <c>Id:bucket[16]</c>).</summary>
    public const string Partition = Prefix + "Partition";
    /// <summary>On the model: <c>snake_case</c> to derive table and column names in snake case.</summary>
    public const string Naming = Prefix + "Naming";
}

/// <summary>Model-building API for Iceberg tables.</summary>
public static class IcebergModelBuilderExtensions
{
    /// <summary>Derive table and column names in snake case (<c>OrderLine.UnitPrice</c> → <c>order_line.unit_price</c>) unless set explicitly.</summary>
    public static ModelBuilder UseIcebergSnakeCaseNames(this ModelBuilder builder)
    {
        builder.HasAnnotation(IcebergAnnotationNames.Naming, "snake_case");
        return builder;
    }

    /// <summary>The Iceberg table (in the context's namespace) the entity type maps to. Default: the CLR type name.</summary>
    public static EntityTypeBuilder ToIcebergTable(this EntityTypeBuilder builder, string name)
    {
        builder.HasAnnotation(IcebergAnnotationNames.TableName, name);
        return builder;
    }

    public static EntityTypeBuilder<T> ToIcebergTable<T>(this EntityTypeBuilder<T> builder, string name) where T : class =>
        (EntityTypeBuilder<T>)((EntityTypeBuilder)builder).ToIcebergTable(name);

    /// <summary>
    /// Add a partition field over <paramref name="property"/> with an Iceberg transform in the spec's text form:
    /// <c>identity</c> (default), <c>year</c>, <c>month</c>, <c>day</c>, <c>hour</c>, <c>bucket[N]</c>, <c>truncate[W]</c>.
    /// Calls add fields in order. Used when <c>EnsureCreated</c> creates the table.
    /// </summary>
    public static EntityTypeBuilder<T> HasIcebergPartition<T>(this EntityTypeBuilder<T> builder, Expression<Func<T, object?>> property, string transform = "identity")
        where T : class
    {
        var body = property.Body is UnaryExpression { NodeType: ExpressionType.Convert } u ? u.Operand : property.Body;
        if (body is not MemberExpression m || m.Expression != property.Parameters[0])
            throw new ArgumentException($"Expected a property of {typeof(T).Name}: {property}", nameof(property));
        Transform.Parse(transform); // fail at model building, not at EnsureCreated
        var existing = builder.Metadata.FindAnnotation(IcebergAnnotationNames.Partition)?.Value as string[] ?? [];
        builder.HasAnnotation(IcebergAnnotationNames.Partition, (string[])[.. existing, $"{m.Member.Name}:{transform}"]);
        return builder;
    }

    /// <summary>The Iceberg column name. Default: the property name (snake-cased with <see cref="UseIcebergSnakeCaseNames"/>), or <c>[IcebergField(Name = ...)]</c>.</summary>
    public static PropertyBuilder HasIcebergColumnName(this PropertyBuilder builder, string name)
    {
        builder.HasAnnotation(IcebergAnnotationNames.ColumnName, name);
        return builder;
    }

    public static PropertyBuilder<TProperty> HasIcebergColumnName<TProperty>(this PropertyBuilder<TProperty> builder, string name) =>
        (PropertyBuilder<TProperty>)((PropertyBuilder)builder).HasIcebergColumnName(name);

    /// <summary>The Iceberg field ID (as <c>[IcebergField(id)]</c>): kept when the table is created, and the binding that survives renames.</summary>
    public static PropertyBuilder HasIcebergFieldId(this PropertyBuilder builder, int id)
    {
        if (id <= 0) throw new ArgumentOutOfRangeException(nameof(id));
        builder.HasAnnotation(IcebergAnnotationNames.FieldId, id);
        return builder;
    }

    public static PropertyBuilder<TProperty> HasIcebergFieldId<TProperty>(this PropertyBuilder<TProperty> builder, int id) =>
        (PropertyBuilder<TProperty>)((PropertyBuilder)builder).HasIcebergFieldId(id);

    /// <summary>The Iceberg type in the spec's text form (<c>decimal(12, 2)</c>, <c>timestamptz</c>, <c>string</c> for an enum by name).</summary>
    public static PropertyBuilder HasIcebergType(this PropertyBuilder builder, string type)
    {
        builder.HasAnnotation(IcebergAnnotationNames.FieldType, type);
        return builder;
    }

    public static PropertyBuilder<TProperty> HasIcebergType<TProperty>(this PropertyBuilder<TProperty> builder, string type) =>
        (PropertyBuilder<TProperty>)((PropertyBuilder)builder).HasIcebergType(type);

    // ---------- reading ----------

    public static string? GetIcebergTableName(this IReadOnlyEntityType entityType) => entityType.FindAnnotation(IcebergAnnotationNames.TableName)?.Value as string;
    public static string? GetIcebergColumnName(this IReadOnlyProperty property) => property.FindAnnotation(IcebergAnnotationNames.ColumnName)?.Value as string;
    public static int? GetIcebergFieldId(this IReadOnlyProperty property) => property.FindAnnotation(IcebergAnnotationNames.FieldId)?.Value as int?;
    public static string? GetIcebergType(this IReadOnlyProperty property) => property.FindAnnotation(IcebergAnnotationNames.FieldType)?.Value as string;
    public static IReadOnlyList<string> GetIcebergPartition(this IReadOnlyEntityType entityType) =>
        entityType.FindAnnotation(IcebergAnnotationNames.Partition)?.Value as string[] ?? [];
}
