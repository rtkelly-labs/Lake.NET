using System.Reflection;
using Iceberg.Net.Types;

namespace Iceberg.Net.Linq;

/// <summary>
/// Maps a C# type to and from Iceberg rows (<c>object?[]</c> of physical values, aligned to a schema's top-level
/// columns, as <c>TableScan.ReadRowsAsync</c> returns and <c>TableMutations</c> accepts). Members are resolved to
/// fields once per schema (<see cref="Bind"/>); every read and write after that goes by field ID.
/// <para>The reflection implementation is <see cref="ReflectionRowMapper{T}"/>. A compile-time implementation (e.g. one
/// generated from <c>[ParquetColumn(FieldId = n)]</c> models) implements the same two interfaces: it knows its field IDs
/// statically, so <see cref="Bind"/> only has to check them against the schema.</para>
/// </summary>
public interface IIcebergRowMapper<T>
{
    /// <summary>Resolve the members of <typeparamref name="T"/> against <paramref name="schema"/> (normally the table's current schema).</summary>
    /// <param name="nameMapping">The table's <c>schema.name-mapping.default</c>: extra names (aliases) members may match.</param>
    IIcebergRowBinding<T> Bind(Schema schema, NameMapping? nameMapping = null);
}

/// <summary><typeparamref name="T"/>'s members resolved to the fields of one schema.</summary>
public interface IIcebergRowBinding<T>
{
    /// <summary>The schema the members were resolved against.</summary>
    Schema Schema { get; }

    /// <summary>The mapped members, in declaration order. Query translation uses them to turn member paths into fields.</summary>
    IReadOnlyList<IcebergMemberBinding> Members { get; }

    /// <summary>
    /// A reader for rows aligned to <paramref name="rowSchema"/> — the schema of a scan, which may be a projection or an
    /// older snapshot's schema. Members are matched by field ID; members whose field is absent get their default.
    /// </summary>
    Func<object?[], T> CreateReader(Schema rowSchema);

    /// <summary>The row for <paramref name="item"/>, aligned to <see cref="Schema"/>; unmapped columns are null.</summary>
    object?[] ToRow(T item);
}

/// <summary>One member bound to one field.</summary>
public sealed class IcebergMemberBinding
{
    public IcebergMemberBinding(MemberInfo member, Type clrType, NestedField field, Func<object?, object?> toIceberg,
        Func<object?, object?> fromIceberg, IReadOnlyList<IcebergMemberBinding>? children = null)
    {
        Member = member;
        ClrType = clrType;
        Field = field;
        ToIceberg = toIceberg;
        FromIceberg = fromIceberg;
        Children = children;
    }

    public MemberInfo Member { get; }
    public Type ClrType { get; }
    public NestedField Field { get; }

    /// <summary>The member's value (null allowed) as the physical value of <see cref="Field"/>'s type.</summary>
    public Func<object?, object?> ToIceberg { get; }

    /// <summary>A physical value of <see cref="Field"/>'s type (null allowed) as the member's type.</summary>
    public Func<object?, object?> FromIceberg { get; }

    /// <summary>For a member mapped to a struct: the nested type's members bound to the struct's fields.</summary>
    public IReadOnlyList<IcebergMemberBinding>? Children { get; }

    public override string ToString() => $"{Member.Name} -> {Field}";
}

/// <summary>How the reflection mapper matches members without an explicit field ID to columns.</summary>
public sealed record IcebergMapperOptions
{
    public static IcebergMapperOptions Default { get; } = new();

    /// <summary>Match names case-insensitively when there is no exact match (and the match is unambiguous).</summary>
    public bool CaseInsensitive { get; init; }

    /// <summary>Turns a property name into a column name, e.g. <see cref="IcebergNaming.SnakeCase"/>. Not applied to <see cref="IcebergFieldAttribute.Name"/>.</summary>
    public Func<string, string>? NameConverter { get; init; }

    /// <summary>Skip members that match no column, instead of failing the bind.</summary>
    public bool IgnoreUnmatchedMembers { get; init; }

    /// <summary>
    /// Field IDs for top-level members without <c>[IcebergField(id)]</c>, by property name (null: bind by name). Lets a
    /// model configured elsewhere (an EF Core model, say) bind by ID, which survives renames.
    /// </summary>
    public Func<string, int?>? FieldIds { get; init; }
}

public static class IcebergNaming
{
    /// <summary><c>OrderId</c> → <c>order_id</c>, <c>HTTPCode</c> → <c>http_code</c>.</summary>
    public static string SnakeCase(string name)
    {
        var sb = new System.Text.StringBuilder(name.Length + 4);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                var prevLowerOrDigit = i > 0 && (char.IsLower(name[i - 1]) || char.IsDigit(name[i - 1]));
                var nextLower = i + 1 < name.Length && char.IsLower(name[i + 1]) && i > 0 && char.IsUpper(name[i - 1]);
                if (sb.Length > 0 && sb[^1] != '_' && (prevLowerOrDigit || nextLower)) sb.Append('_');
                sb.Append(char.ToLowerInvariant(c));
            }
            else sb.Append(c);
        }
        return sb.ToString();
    }
}
