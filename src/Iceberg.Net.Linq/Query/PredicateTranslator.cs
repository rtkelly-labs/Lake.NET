using System.Collections;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;
using IcebergExpression = Iceberg.Net.Expressions.Expression;
using LinqExpression = System.Linq.Expressions.Expression;

namespace Iceberg.Net.Linq.Query;

/// <summary>
/// The Iceberg filter for a C# predicate over a mapped type. <see cref="Translation.Exact"/> filters
/// select exactly the rows the predicate keeps; inexact ones are a superset (a sound pre-filter: they never drop a row
/// the predicate keeps), and the predicate still runs on the materialized rows.
/// <para>Exactness is checked against C# semantics, not SQL's: a lifted comparison with a null member is false, its
/// negation true; a non-nullable member reads a null column as its default; a constant that the column cannot hold
/// exactly (2.5 for an int column, sub-microsecond ticks for a timestamp) is not pushed down.</para>
/// </summary>
internal sealed class PredicateTranslator
{
    internal readonly record struct Translation(IcebergExpression Filter, bool Exact)
    {
        public static readonly Translation None = new(IcebergExpression.True, false);
    }

    private sealed record Column(string Name, NestedField Field, IcebergMemberBinding Binding, Type Clr, bool ColumnMayBeNull)
    {
        public bool MemberNullable => !Clr.IsValueType || Nullable.GetUnderlyingType(Clr) is not null;
        public Type Underlying => Nullable.GetUnderlyingType(Clr) ?? Clr;
        public bool IsFloating => Field.Type.TypeId is TypeId.Float or TypeId.Double;
    }

    private readonly IReadOnlyList<IcebergMemberBinding> _members;
    private readonly Schema _read;
    private readonly Schema _current;
    private readonly Dictionary<string, Column> _columns = [];
    private ParameterExpression _parameter = null!;

    /// <param name="read">The schema rows are read with (the selected snapshot's).</param>
    /// <param name="current">The table's current schema: file planning binds filters to it, so a pushed-down column must have the same name in both.</param>
    public PredicateTranslator(IReadOnlyList<IcebergMemberBinding> members, Schema read, Schema current)
    {
        _members = members;
        _read = read;
        _current = current;
    }

    public Translation Translate(LambdaExpression predicate)
    {
        _parameter = predicate.Parameters[0];
        return Visit(PartialEvaluator.Evaluate(predicate.Body));
    }

    /// <summary>The top-level conjuncts of a predicate body (<c>a &amp;&amp; (b &amp;&amp; c)</c> → a, b, c).</summary>
    public static IEnumerable<LinqExpression> Conjuncts(LinqExpression body) =>
        body is BinaryExpression { NodeType: ExpressionType.AndAlso } b ? Conjuncts(b.Left).Concat(Conjuncts(b.Right)) : [body];

    private Translation Visit(LinqExpression e)
    {
        try
        {
            return VisitCore(e) ?? Translation.None;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidCastException or OverflowException or FormatException or NotSupportedException or InvalidOperationException)
        {
            return Translation.None;
        }
    }

    private Translation? VisitCore(LinqExpression e)
    {
        switch (e)
        {
            case ConstantExpression { Value: bool b }:
                return new(b ? IcebergExpression.True : IcebergExpression.False, true);
            case BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.And } a when a.Type == typeof(bool):
            {
                var l = Visit(a.Left);
                var r = Visit(a.Right);
                return new(Expr.And(l.Filter, r.Filter), l.Exact && r.Exact);
            }
            case BinaryExpression { NodeType: ExpressionType.OrElse or ExpressionType.Or } o when o.Type == typeof(bool):
            {
                // A superset of (a or b) needs a superset of both sides: one untranslatable side makes the whole OR unfilterable.
                var l = Visit(o.Left);
                var r = Visit(o.Right);
                return new(Expr.Or(l.Filter, r.Filter), l.Exact && r.Exact);
            }
            case UnaryExpression { NodeType: ExpressionType.Not } n when n.Type == typeof(bool):
            {
                // Only an exact filter can be negated: the complement of a superset is not a superset of the complement.
                var inner = Visit(n.Operand);
                return inner.Exact ? new(Negate(inner.Filter), true) : Translation.None;
            }
            case BinaryExpression { NodeType: ExpressionType.Equal or ExpressionType.NotEqual or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual } c:
                return Comparison(c.NodeType, c.Left, c.Right);
            case MemberExpression { Member.Name: "HasValue" } hv when hv.Expression is { } target && Nullable.GetUnderlyingType(target.Type) is not null:
                return ColumnOf(target) is { } col ? NullCheck(col, isNull: false) : null;
            case MethodCallExpression m:
                return Method(m);
            case MemberExpression when e.Type == typeof(bool) || e.Type == typeof(bool?):
                return ColumnOf(e) is { } boolCol ? Compare(boolCol, ExpressionType.Equal, true) : null;
            case UnaryExpression { NodeType: ExpressionType.Convert } u when u.Type == typeof(bool) && u.Method is null:
                return VisitCore(u.Operand);
            default:
                return null;
        }
    }

    // ---------- comparisons ----------

    private Translation? Comparison(ExpressionType op, LinqExpression left, LinqExpression right)
    {
        if (ColumnOf(left) is { } col && ConstantOf(right, out var value)) return Compare(col, op, value);
        if (ColumnOf(right) is { } col2 && ConstantOf(left, out var value2)) return Compare(col2, Flip(op), value2);
        return null;
    }

    private static ExpressionType Flip(ExpressionType op) => op switch
    {
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        _ => op,
    };

    private Translation? Compare(Column col, ExpressionType op, object? constant)
    {
        if (constant is null)
        {
            // Lifted comparisons with null: == null / != null test for null; every ordering with null is false.
            if (op is not (ExpressionType.Equal or ExpressionType.NotEqual)) return new(IcebergExpression.False, true);
            if (!col.MemberNullable) return new(op == ExpressionType.Equal ? IcebergExpression.False : IcebergExpression.True, true);
            return NullCheck(col, isNull: op == ExpressionType.Equal);
        }
        var u = col.Underlying;
        var ordered = op is not (ExpressionType.Equal or ExpressionType.NotEqual);
        if (u == typeof(byte[])) return null; // == on arrays is reference equality
        if (ordered && !IsOrdered(u, col.Field.Type)) return null;
        if (!TryMemberValue(constant, u, out var member)) return null;
        var literal = Literal(col, member);
        if (literal is null) return null;
        var iceberg = op switch
        {
            ExpressionType.Equal => Expr.Equal(col.Name, literal),
            ExpressionType.NotEqual => Expr.NotEqual(col.Name, literal),
            ExpressionType.LessThan => Expr.LessThan(col.Name, literal),
            ExpressionType.LessThanOrEqual => Expr.LessThanOrEqual(col.Name, literal),
            ExpressionType.GreaterThan => Expr.GreaterThan(col.Name, literal),
            _ => Expr.GreaterThanOrEqual(col.Name, literal),
        };
        bool NullRowMatches()
        {
            if (col.MemberNullable) return op == ExpressionType.NotEqual; // C#: null != c is true, every other comparison false
            var d = Activator.CreateInstance(u)!;
            var cmp = ((IComparable)d).CompareTo(member);
            return op switch
            {
                ExpressionType.Equal => cmp == 0,
                ExpressionType.NotEqual => cmp != 0,
                ExpressionType.LessThan => cmp < 0,
                ExpressionType.LessThanOrEqual => cmp <= 0,
                ExpressionType.GreaterThan => cmp > 0,
                _ => cmp >= 0,
            };
        }
        return new(WithNulls(col, iceberg, NullRowMatches()), true);
    }

    /// <summary>
    /// Makes <paramref name="filter"/> (exact for non-null values) exact for rows where the column is null: those rows read
    /// as null, or as the member's default when the member cannot hold null, and the C# predicate decides them by that.
    /// </summary>
    private static IcebergExpression WithNulls(Column col, IcebergExpression filter, bool nullRowMatches)
    {
        if (!col.ColumnMayBeNull) return filter;
        var icebergTrueOnNull = filter is UnboundPredicate { Op: Operation.NotEq or Operation.NotIn or Operation.NotStartsWith or Operation.IsNull };
        if (nullRowMatches == icebergTrueOnNull) return filter;
        return nullRowMatches ? Expr.Or(filter, Expr.IsNull(col.Name)) : Expr.And(filter, Expr.NotNull(col.Name));
    }

    private Translation? NullCheck(Column col, bool isNull)
    {
        if (!col.MemberNullable) return new(isNull ? IcebergExpression.False : IcebergExpression.True, true);
        return new(isNull ? Expr.IsNull(col.Name) : Expr.NotNull(col.Name), true);
    }

    private static bool IsOrdered(Type u, IcebergType type)
    {
        if (u.IsEnum) return type.TypeId is TypeId.Int or TypeId.Long; // names do not sort like values
        return u == typeof(int) || u == typeof(long) || u == typeof(short) || u == typeof(byte) || u == typeof(sbyte) || u == typeof(ushort) || u == typeof(uint)
            || u == typeof(float) || u == typeof(double) || u == typeof(decimal)
            || u == typeof(DateOnly) || u == typeof(DateTime) || u == typeof(DateTimeOffset) || u == typeof(TimeOnly) || u == typeof(TimeSpan);
    }

    /// <summary>
    /// <paramref name="value"/> (of the comparison's type, e.g. <c>long</c> for an <c>int</c> member compared with a
    /// <c>long</c>) as the member's type, only when that is exact.
    /// </summary>
    private static bool TryMemberValue(object value, Type u, out object member)
    {
        member = value;
        var t = value.GetType();
        if (t == u) return true;
        try
        {
            if (u.IsEnum)
            {
                if (t.IsEnum) return false;
                var asEnum = Enum.ToObject(u, value);
                member = asEnum;
                return Convert.ToDecimal(asEnum, CultureInfo.InvariantCulture) == Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            }
            if (value is not IConvertible || !(u.IsPrimitive || u == typeof(decimal))) return false;
            var converted = Convert.ChangeType(value, u, CultureInfo.InvariantCulture);
            if (!Equals(Convert.ChangeType(converted, t, CultureInfo.InvariantCulture), value)) return false;
            member = converted;
            return true;
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException or FormatException or ArgumentException)
        {
            return false;
        }
    }

    /// <summary>The physical literal for a member value, or null when the column cannot represent it exactly.</summary>
    private static object? Literal(Column col, object member)
    {
        if (member is float f && float.IsNaN(f) || member is double d && double.IsNaN(d)) return null;
        object? physical;
        try
        {
            physical = col.Binding.ToIceberg(member);
        }
        catch (Exception e) when (e is ArgumentException or InvalidCastException or OverflowException or FormatException)
        {
            return null;
        }
        if (physical is null) return null;
        var back = col.Binding.FromIceberg(physical);
        return Equals(back, member) ? physical : null;
    }

    // ---------- methods ----------

    private Translation? Method(MethodCallExpression m)
    {
        var method = m.Method;
        if (method.DeclaringType == typeof(string))
        {
            switch (method.Name)
            {
                case nameof(string.StartsWith) when m.Object is not null && m.Arguments.Count >= 1
                    && (m.Arguments.Count == 1 || m.Arguments.Count == 2 && ConstantOf(m.Arguments[1], out var cmp) && cmp is StringComparison.Ordinal)
                    && m.Arguments[0].Type == typeof(string):
                    // One-argument StartsWith is culture-sensitive in .NET; Iceberg compares ordinally, as EF Core providers do (docs/linq.md).
                    if (ColumnOf(m.Object) is { } col && ConstantOf(m.Arguments[0], out var prefix) && prefix is string p)
                        return new(WithNulls(col, Expr.StartsWith(col.Name, p), false), true);
                    return null;
                case nameof(string.IsNullOrEmpty) when m.Object is null && m.Arguments.Count == 1:
                    if (ColumnOf(m.Arguments[0]) is { } s)
                        return new(Expr.Or(Expr.IsNull(s.Name), Expr.Equal(s.Name, "")), true);
                    return null;
                case nameof(string.Equals) when m.Object is not null && m.Arguments.Count == 1:
                    return Comparison(ExpressionType.Equal, m.Object, m.Arguments[0]);
                case nameof(string.Equals) when m.Object is null && m.Arguments.Count == 2 && m.Arguments.All(a => a.Type == typeof(string)):
                    return Comparison(ExpressionType.Equal, m.Arguments[0], m.Arguments[1]);
            }
            return null;
        }
        if (method.Name == "Equals" && m.Object is not null && m.Arguments.Count == 1 && !method.IsStatic && m.Object.Type.IsValueType)
            return Comparison(ExpressionType.Equal, m.Object, StripConvert(m.Arguments[0]));
        if (method.Name == "Contains")
        {
            // Enumerable.Contains(collection, x.Col) or collection.Contains(x.Col) on a captured collection.
            LinqExpression? source = null, item = null;
            if (method.IsStatic && m.Arguments.Count == 2 && method.DeclaringType == typeof(Enumerable)) (source, item) = (m.Arguments[0], m.Arguments[1]);
            else if (!method.IsStatic && m.Object is not null && m.Arguments.Count == 1 && m.Object.Type != typeof(string)) (source, item) = (m.Object, m.Arguments[0]);
            if (source is null || item is null || !ConstantOf(source, out var collection) || collection is not IEnumerable values || ColumnOf(item) is not { } col)
                return null;
            return In(col, values);
        }
        return null;
    }

    private Translation? In(Column col, IEnumerable values)
    {
        var u = col.Underlying;
        if (u == typeof(byte[])) return null;
        var literals = new List<object>();
        var members = new List<object?>();
        foreach (var v in values)
        {
            if (v is null) { members.Add(null); continue; }
            if (!TryMemberValue(v, u, out var member)) return null;
            var literal = Literal(col, member);
            if (literal is null) return null;
            literals.Add(literal);
            members.Add(member);
        }
        var nullRowValue = col.MemberNullable ? null : Activator.CreateInstance(u);
        var nullRowMatches = members.Any(m => Equals(m, nullRowValue));
        var filter = Expr.In(col.Name, literals);
        if (!col.ColumnMayBeNull) return new(filter, true);
        return new(nullRowMatches ? Expr.Or(filter, Expr.IsNull(col.Name)) : filter, true);
    }

    // ---------- operands ----------

    private static LinqExpression StripConvert(LinqExpression e)
    {
        while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked, Method: null } u) e = u.Operand;
        return e;
    }

    private static bool ConstantOf(LinqExpression e, out object? value)
    {
        e = StripConvert(e);
        if (e is ConstantExpression c)
        {
            value = c.Value;
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>A member path from the parameter (<c>x.A</c>, <c>x.A.B</c> through struct columns, <c>x.N.Value</c>), resolved to a pushable column.</summary>
    private Column? ColumnOf(LinqExpression e)
    {
        e = StripConvert(e);
        if (e is MemberExpression { Member.Name: "Value" } v && v.Expression is { } inner && Nullable.GetUnderlyingType(inner.Type) is not null)
            e = StripConvert(inner);
        var path = new List<MemberInfo>();
        while (e is MemberExpression me && me.Expression is not null)
        {
            path.Add(me.Member);
            e = me.Expression;
        }
        if (e != _parameter || path.Count == 0) return null;
        path.Reverse();
        var key = string.Join(".", path.Select(p => p.Name));
        if (_columns.TryGetValue(key, out var cached)) return cached;

        IReadOnlyList<IcebergMemberBinding>? level = _members;
        IcebergMemberBinding? binding = null;
        foreach (var member in path)
        {
            binding = level?.FirstOrDefault(b => b.Member.Name == member.Name);
            if (binding is null) return null;
            level = binding.Children;
        }
        if (binding!.Field.Type is not PrimitiveType) return null;
        var id = binding.Field.Id;
        var readName = _read.FindColumnName(id);
        // Planning binds to the current schema and row filtering to the read schema, by name: both must agree.
        if (readName is null || readName != _current.FindColumnName(id)) return null;
        if (_read.FindField(id)!.Type is not PrimitiveType readType || !readType.Equals(binding.Field.Type)) return null;
        var col = new Column(readName, binding.Field, binding, binding.ClrType, !_read.AlwaysPresent(id));
        _columns[key] = col;
        return col;
    }

    // ---------- negation ----------

    /// <summary>
    /// The exact complement of <paramref name="e"/> under Iceberg's row semantics, where every comparison with null or
    /// NaN is false: so <c>not (a &lt; 5)</c> is <c>a &gt;= 5 or a is null or a is nan</c>, not just <c>a &gt;= 5</c>.
    /// </summary>
    private IcebergExpression Negate(IcebergExpression e) => e switch
    {
        AlwaysTrue => IcebergExpression.False,
        AlwaysFalse => IcebergExpression.True,
        And a => Expr.Or(Negate(a.Left), Negate(a.Right)),
        Or o => Expr.And(Negate(o.Left), Negate(o.Right)),
        Not n => n.Child,
        UnboundPredicate { Op: Operation.Lt or Operation.LtEq or Operation.Gt or Operation.GtEq } p => NegateOrdered(p),
        UnboundPredicate p => p with { Op = Expr.Complement(p.Op) },
        _ => throw new NotSupportedException($"Cannot negate {e}"),
    };

    private IcebergExpression NegateOrdered(UnboundPredicate p)
    {
        var complement = p with { Op = Expr.Complement(p.Op) };
        var col = _columns.Values.First(c => c.Name == p.Column);
        IcebergExpression result = complement;
        if (col.ColumnMayBeNull) result = Expr.Or(result, Expr.IsNull(p.Column));
        if (col.IsFloating) result = Expr.Or(result, Expr.IsNaN(p.Column));
        return result;
    }
}
