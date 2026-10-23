using System.Collections;
using System.Collections.Concurrent;
using System.Reflection;
using LinqExpression = System.Linq.Expressions.Expression;

namespace Iceberg.Net.Linq;

/// <summary>
/// The mappable shape of a CLR type: its members in declaration order and how to construct it (a parameterless
/// constructor plus setters, or a constructor whose parameters match properties — positional records — plus setters
/// or <c>init</c> accessors for the rest).
/// </summary>
internal sealed class TypeModel
{
    private static readonly ConcurrentDictionary<Type, TypeModel> Cache = new();

    public static TypeModel For(Type type) => Cache.GetOrAdd(type, t => new TypeModel(t));

    public Type Type { get; }
    public IReadOnlyList<ModelMember> Members { get; }
    public ConstructorInfo? Constructor { get; }
    /// <summary>For each constructor parameter, the member it initialises.</summary>
    public IReadOnlyList<ModelMember> ConstructorMembers { get; }

    private TypeModel(Type type)
    {
        Type = type;
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.GetIndexParameters().Length == 0 && p.GetMethod is { IsPublic: true } && p.GetCustomAttribute<IcebergIgnoreAttribute>() is null)
            .GroupBy(p => p.Name).Select(g => g.OrderByDescending(p => Depth(p.DeclaringType!)).First()) // `new` hides the base member
            .OrderBy(p => Depth(p.DeclaringType!)).ThenBy(p => p.MetadataToken)
            .ToList();

        var ctors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
        var parameterless = ctors.FirstOrDefault(c => c.GetParameters().Length == 0);
        ConstructorInfo? chosen = null;
        List<PropertyInfo> ctorProps = [];
        foreach (var c in ctors.Where(c => c.GetParameters().Length > 0).OrderByDescending(c => c.GetParameters().Length))
        {
            var matched = c.GetParameters().Select(p => props.FirstOrDefault(pr => string.Equals(pr.Name, p.Name, StringComparison.OrdinalIgnoreCase) && pr.PropertyType == p.ParameterType)).ToList();
            if (matched.All(m => m is not null)) { chosen = c; ctorProps = matched!; break; }
        }
        // A parameterless constructor wins when every member can be set through it (classic POCOs).
        if (parameterless is not null && (chosen is null || props.All(p => p.SetMethod is { IsPublic: true })))
            (chosen, ctorProps) = (parameterless, []);
        if (chosen is null && !type.IsValueType)
            throw new InvalidOperationException($"{type.Name} cannot be constructed: it needs a public parameterless constructor or one whose parameters match its properties by name");

        var members = new List<ModelMember>();
        foreach (var p in props)
        {
            var ctorIndex = ctorProps.IndexOf(p);
            var settable = p.SetMethod is { IsPublic: true };
            // Get-only properties that no constructor parameter fills are computed, not stored.
            if (ctorIndex < 0 && !settable) continue;
            var parameter = ctorIndex >= 0 ? chosen!.GetParameters()[ctorIndex] : null;
            members.Add(new ModelMember(p, ctorIndex, p.GetCustomAttribute<IcebergFieldAttribute>() ?? parameter?.GetCustomAttribute<IcebergFieldAttribute>(),
                p.GetCustomAttribute<IcebergKeyAttribute>() is not null, parameter));
        }
        Members = members;
        Constructor = chosen;
        ConstructorMembers = ctorProps.Select(p => members.First(m => m.Property == p)).ToList();
    }

    private static int Depth(Type t)
    {
        var d = 0;
        for (var b = t.BaseType; b is not null; b = b.BaseType) d++;
        return d;
    }

    /// <summary>
    /// A factory from member values: <c>values[i]</c> is the value for <see cref="Members"/>[i], or <see cref="Missing"/>
    /// to leave the member at its default (the constructor parameter's default value when it has one).
    /// </summary>
    public Func<object?[], object> CreateFactory()
    {
        var values = LinqExpression.Parameter(typeof(object?[]), "values");
        var obj = LinqExpression.Variable(Type, "obj");
        var body = new List<LinqExpression>();
        LinqExpression Value(ModelMember m, int index)
        {
            var raw = LinqExpression.ArrayIndex(values, LinqExpression.Constant(index));
            var fallback = m.Parameter is { HasDefaultValue: true } p && p.DefaultValue is not DBNull
                ? LinqExpression.Constant(p.DefaultValue, typeof(object))
                : (LinqExpression)LinqExpression.Constant(null, typeof(object));
            var orDefault = LinqExpression.Condition(LinqExpression.ReferenceEqual(raw, LinqExpression.Constant(Missing)), fallback, raw);
            return LinqExpression.Call(AsMethod.MakeGenericMethod(m.Type), orDefault);
        }
        if (Constructor is not null)
            body.Add(LinqExpression.Assign(obj, LinqExpression.New(Constructor, ConstructorMembers.Select(m => Value(m, IndexOf(m))))));
        else
            body.Add(LinqExpression.Assign(obj, LinqExpression.Default(Type)));
        for (var i = 0; i < Members.Count; i++)
        {
            var m = Members[i];
            if (m.ConstructorIndex >= 0) continue;
            var raw = LinqExpression.ArrayIndex(values, LinqExpression.Constant(i));
            body.Add(LinqExpression.IfThen(
                LinqExpression.Not(LinqExpression.ReferenceEqual(raw, LinqExpression.Constant(Missing))),
                LinqExpression.Assign(LinqExpression.Property(obj, m.Property), LinqExpression.Call(AsMethod.MakeGenericMethod(m.Type), raw))));
        }
        body.Add(LinqExpression.Convert(obj, typeof(object)));
        return LinqExpression.Lambda<Func<object?[], object>>(LinqExpression.Block([obj], body), values).Compile();
    }

    private int IndexOf(ModelMember m)
    {
        for (var i = 0; i < Members.Count; i++) if (ReferenceEquals(Members[i], m)) return i;
        return -1;
    }

    /// <summary>Marks a member value as absent (not read), so the member keeps its default.</summary>
    public static readonly object Missing = new();

    private static readonly MethodInfo AsMethod = typeof(TypeModel).GetMethod(nameof(As), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static TV As<TV>(object? v) => v is null ? default! : (TV)v;

    // ---------- collection shapes ----------

    /// <summary>Element type for arrays, <c>List&lt;T&gt;</c> and the interfaces <c>List&lt;T&gt;</c> implements; never for string or byte[].</summary>
    public static Type? ListElement(Type t)
    {
        if (t == typeof(string) || t == typeof(byte[])) return null;
        if (t.IsArray && t.GetArrayRank() == 1) return t.GetElementType();
        if (!t.IsGenericType) return null;
        var def = t.GetGenericTypeDefinition();
        return def == typeof(List<>) || def == typeof(IList<>) || def == typeof(IReadOnlyList<>) || def == typeof(ICollection<>)
            || def == typeof(IReadOnlyCollection<>) || def == typeof(IEnumerable<>) || def == typeof(HashSet<>) || def == typeof(ISet<>)
            ? t.GetGenericArguments()[0]
            : null;
    }

    public static (Type Key, Type Value)? MapTypes(Type t)
    {
        if (!t.IsGenericType) return null;
        var def = t.GetGenericTypeDefinition();
        return def == typeof(Dictionary<,>) || def == typeof(IDictionary<,>) || def == typeof(IReadOnlyDictionary<,>)
            ? (t.GetGenericArguments()[0], t.GetGenericArguments()[1])
            : null;
    }

    /// <summary>Builds a collection of type <paramref name="target"/> from converted elements.</summary>
    public static Func<IEnumerable<object?>, object> ListFactory(Type target, Type element)
    {
        var listType = typeof(List<>).MakeGenericType(element);
        if (target.IsArray)
            return items =>
            {
                var list = items.ToList();
                var array = Array.CreateInstance(element, list.Count);
                for (var i = 0; i < list.Count; i++) array.SetValue(list[i], i);
                return array;
            };
        var isSet = target.GetGenericTypeDefinition() is var d && (d == typeof(HashSet<>) || d == typeof(ISet<>));
        var setType = typeof(HashSet<>).MakeGenericType(element);
        return items =>
        {
            var list = (IList)Activator.CreateInstance(listType)!;
            foreach (var i in items) list.Add(i);
            return isSet ? Activator.CreateInstance(setType, list)! : list;
        };
    }

    public static Func<IEnumerable<KeyValuePair<object, object?>>, object> MapFactory(Type key, Type value)
    {
        var dictType = typeof(Dictionary<,>).MakeGenericType(key, value);
        return entries =>
        {
            var dict = (IDictionary)Activator.CreateInstance(dictType)!;
            foreach (var e in entries) dict.Add(e.Key, e.Value);
            return dict;
        };
    }
}

internal sealed class ModelMember(PropertyInfo property, int constructorIndex, IcebergFieldAttribute? attribute, bool isKey, ParameterInfo? parameter)
{
    public PropertyInfo Property { get; } = property;
    public string Name => Property.Name;
    public Type Type => Property.PropertyType;
    public int ConstructorIndex { get; } = constructorIndex;
    public IcebergFieldAttribute? Attribute { get; } = attribute;
    public bool IsKey { get; } = isKey;
    public ParameterInfo? Parameter { get; } = parameter;

    private Func<object, object?>? _getter;

    public Func<object, object?> Getter => _getter ??= CompileGetter();

    private Func<object, object?> CompileGetter()
    {
        var o = LinqExpression.Parameter(typeof(object), "o");
        return LinqExpression.Lambda<Func<object, object?>>(
            LinqExpression.Convert(LinqExpression.Property(LinqExpression.Convert(o, Property.DeclaringType!), Property), typeof(object)), o).Compile();
    }
}
