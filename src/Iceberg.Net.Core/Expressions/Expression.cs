using System.Globalization;
using Iceberg.Net.Types;

namespace Iceberg.Net.Expressions;

public enum Operation
{
    IsNull, NotNull, IsNaN, NotNaN,
    Lt, LtEq, Gt, GtEq, Eq, NotEq,
    In, NotIn,
    StartsWith, NotStartsWith,
}

/// <summary>
/// Row filter, as in the Java/PyIceberg expression APIs: build it unbound over column names (<see cref="Expr"/>),
/// bind it to a schema, then evaluate it against rows (<see cref="RowEvaluator"/>), file metrics
/// (<see cref="MetricsEvaluator"/>) or — after <see cref="Projections.Inclusive"/> — partition tuples and manifest
/// partition summaries.
/// <para>Semantics (shared by every evaluator, and matching PyIceberg): a comparison with a null value is false; a
/// comparison involving NaN is false (IEEE); <c>NotEq</c>/<c>NotIn</c>/<c>NotStartsWith</c> are the complements of
/// <c>Eq</c>/<c>In</c>/<c>StartsWith</c>, so they are true for nulls and NaN. NaN literals are rejected: use
/// <see cref="Expr.IsNaN"/>.</para>
/// </summary>
public abstract record Expression
{
    public static readonly Expression True = new AlwaysTrue();
    public static readonly Expression False = new AlwaysFalse();

    public Expression And(Expression other) => Expr.And(this, other);
    public Expression Or(Expression other) => Expr.Or(this, other);
}

public sealed record AlwaysTrue : Expression
{
    public override string ToString() => "true";
}

public sealed record AlwaysFalse : Expression
{
    public override string ToString() => "false";
}

public sealed record And(Expression Left, Expression Right) : Expression
{
    public override string ToString() => $"({Left} and {Right})";
}

public sealed record Or(Expression Left, Expression Right) : Expression
{
    public override string ToString() => $"({Left} or {Right})";
}

public sealed record Not(Expression Child) : Expression
{
    public override string ToString() => $"not({Child})";
}

/// <summary>A predicate over a column name, not yet resolved against a schema.</summary>
public sealed record UnboundPredicate(string Column, Operation Op, IReadOnlyList<object?> Literals) : Expression
{
    public override string ToString() => Describe(Column, Op, Literals);

    internal static string Describe(string term, Operation op, IReadOnlyList<object?> lits) => op switch
    {
        Operation.IsNull => $"{term} is null",
        Operation.NotNull => $"{term} is not null",
        Operation.IsNaN => $"{term} is nan",
        Operation.NotNaN => $"{term} is not nan",
        Operation.In => $"{term} in ({string.Join(", ", lits.Select(Show))})",
        Operation.NotIn => $"{term} not in ({string.Join(", ", lits.Select(Show))})",
        Operation.StartsWith => $"{term} starts with {Show(lits[0])}",
        Operation.NotStartsWith => $"{term} not starts with {Show(lits[0])}",
        _ => $"{term} {op switch { Operation.Lt => "<", Operation.LtEq => "<=", Operation.Gt => ">", Operation.GtEq => ">=", Operation.Eq => "==", _ => "!=" }} {Show(lits[0])}",
    };

    private static string Show(object? v) => v switch
    {
        string s => $"'{s}'",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => v?.ToString() ?? "null",
    };
}

/// <summary>
/// A predicate resolved to a field: <see cref="Position"/> is the column index in the tuple it is evaluated on (a
/// top-level schema column, or a partition field after projection); <see cref="FieldId"/> keys file metrics.
/// Literals are already converted to <see cref="Type"/>'s physical representation.
/// </summary>
public sealed record BoundPredicate(int Position, int FieldId, string Name, IcebergType Type, Operation Op, IReadOnlyList<object> Literals) : Expression
{
    public object Literal => Literals[0];

    /// <summary>For a field inside structs, the positions from the row down to it (<see cref="Position"/> is the first).</summary>
    public IReadOnlyList<int>? StructPath { get; init; }

    /// <summary>The predicate's value in <paramref name="row"/>.</summary>
    public object? ValueIn(IReadOnlyList<object?> row) => StructPath is { } path ? Schema.Navigate(row, path) : row[Position];

    public override string ToString() => UnboundPredicate.Describe(Name, Op, Literals);
}

/// <summary>Factory for unbound expressions, with the usual simplifications (<c>x and true</c> → <c>x</c>, …).</summary>
public static class Expr
{
    public static Expression IsNull(string column) => new UnboundPredicate(column, Operation.IsNull, []);
    public static Expression NotNull(string column) => new UnboundPredicate(column, Operation.NotNull, []);
    public static Expression IsNaN(string column) => new UnboundPredicate(column, Operation.IsNaN, []);
    public static Expression NotNaN(string column) => new UnboundPredicate(column, Operation.NotNaN, []);
    public static Expression LessThan(string column, object value) => new UnboundPredicate(column, Operation.Lt, [value]);
    public static Expression LessThanOrEqual(string column, object value) => new UnboundPredicate(column, Operation.LtEq, [value]);
    public static Expression GreaterThan(string column, object value) => new UnboundPredicate(column, Operation.Gt, [value]);
    public static Expression GreaterThanOrEqual(string column, object value) => new UnboundPredicate(column, Operation.GtEq, [value]);
    public static Expression Equal(string column, object value) => new UnboundPredicate(column, Operation.Eq, [value]);
    public static Expression NotEqual(string column, object value) => new UnboundPredicate(column, Operation.NotEq, [value]);
    public static Expression StartsWith(string column, string prefix) => new UnboundPredicate(column, Operation.StartsWith, [prefix]);
    public static Expression NotStartsWith(string column, string prefix) => new UnboundPredicate(column, Operation.NotStartsWith, [prefix]);

    public static Expression In(string column, IEnumerable<object> values)
    {
        var list = values.Cast<object?>().Distinct().ToList();
        return list.Count == 0 ? Expression.False : new UnboundPredicate(column, Operation.In, list);
    }

    public static Expression NotIn(string column, IEnumerable<object> values)
    {
        var list = values.Cast<object?>().Distinct().ToList();
        return list.Count == 0 ? Expression.True : new UnboundPredicate(column, Operation.NotIn, list);
    }

    public static Expression In(string column, params object[] values) => In(column, (IEnumerable<object>)values);
    public static Expression NotIn(string column, params object[] values) => NotIn(column, (IEnumerable<object>)values);

    public static Expression And(Expression left, Expression right) => (left, right) switch
    {
        (AlwaysFalse, _) or (_, AlwaysFalse) => Expression.False,
        (AlwaysTrue, _) => right,
        (_, AlwaysTrue) => left,
        _ => new And(left, right),
    };

    public static Expression Or(Expression left, Expression right) => (left, right) switch
    {
        (AlwaysTrue, _) or (_, AlwaysTrue) => Expression.True,
        (AlwaysFalse, _) => right,
        (_, AlwaysFalse) => left,
        _ => new Or(left, right),
    };

    public static Expression And(params Expression[] all) => all.Aggregate(Expression.True, And);
    public static Expression Or(params Expression[] all) => all.Aggregate(Expression.False, Or);

    public static Expression Not(Expression e) => e switch
    {
        AlwaysTrue => Expression.False,
        AlwaysFalse => Expression.True,
        Not n => n.Child,
        _ => new Not(e),
    };

    /// <summary>Push negations down to predicates (De Morgan + operator complements), so evaluators never see <see cref="Not"/>.</summary>
    public static Expression RewriteNot(Expression e) => e switch
    {
        Not n => Negate(RewriteNot(n.Child)),
        And a => And(RewriteNot(a.Left), RewriteNot(a.Right)),
        Or o => Or(RewriteNot(o.Left), RewriteNot(o.Right)),
        _ => e,
    };

    private static Expression Negate(Expression e) => e switch
    {
        AlwaysTrue => Expression.False,
        AlwaysFalse => Expression.True,
        And a => Or(Negate(a.Left), Negate(a.Right)),
        Or o => And(Negate(o.Left), Negate(o.Right)),
        Not n => n.Child,
        UnboundPredicate p => p with { Op = Complement(p.Op) },
        BoundPredicate p => p with { Op = Complement(p.Op) },
        _ => throw new ArgumentException($"Cannot negate {e}"),
    };

    public static Operation Complement(Operation op) => op switch
    {
        Operation.IsNull => Operation.NotNull,
        Operation.NotNull => Operation.IsNull,
        Operation.IsNaN => Operation.NotNaN,
        Operation.NotNaN => Operation.IsNaN,
        Operation.Lt => Operation.GtEq,
        Operation.LtEq => Operation.Gt,
        Operation.Gt => Operation.LtEq,
        Operation.GtEq => Operation.Lt,
        Operation.Eq => Operation.NotEq,
        Operation.NotEq => Operation.Eq,
        Operation.In => Operation.NotIn,
        Operation.NotIn => Operation.In,
        Operation.StartsWith => Operation.NotStartsWith,
        Operation.NotStartsWith => Operation.StartsWith,
        _ => throw new ArgumentOutOfRangeException(nameof(op)),
    };
}

/// <summary>Resolves column names against a schema and converts literals to the column's type.</summary>
public static class Binder
{
    /// <summary>
    /// Bind to the top-level columns of <paramref name="schema"/> (positions are column indexes). Names match exactly,
    /// else case-insensitively when unambiguous. Negations are pushed down first.
    /// </summary>
    /// <remarks>A dotted name (<c>location.lat</c>) that is not a top-level column binds to a field inside structs.</remarks>
    public static Expression Bind(Schema schema, Expression expression) => BindTo(Expr.RewriteNot(expression), schema.Columns, schema);

    private static Expression BindTo(Expression e, IReadOnlyList<NestedField> columns, Schema schema) => e switch
    {
        AlwaysTrue or AlwaysFalse or BoundPredicate => e,
        And a => Expr.And(BindTo(a.Left, columns, schema), BindTo(a.Right, columns, schema)),
        Or o => Expr.Or(BindTo(o.Left, columns, schema), BindTo(o.Right, columns, schema)),
        UnboundPredicate p => BindPredicate(p, columns, schema),
        _ => throw new ArgumentException($"Cannot bind {e}"),
    };

    private static Expression BindPredicate(UnboundPredicate p, IReadOnlyList<NestedField> columns, Schema schema)
    {
        var position = -1;
        for (var i = 0; i < columns.Count; i++)
            if (columns[i].Name == p.Column) { position = i; break; }
        IReadOnlyList<int>? path = null;
        NestedField field;
        if (position < 0 && schema.FindField(p.Column) is { } nested && schema.StructPath(nested.Id) is { Count: > 1 } structPath)
        {
            // A field inside structs, by its full dotted name.
            (path, position, field) = (structPath, structPath[0], nested);
        }
        else
        {
            if (position < 0)
            {
                var matches = columns.Select((c, i) => (c, i)).Where(x => string.Equals(x.c.Name, p.Column, StringComparison.OrdinalIgnoreCase)).ToList();
                position = matches.Count == 1 ? matches[0].i : throw new ArgumentException($"Cannot find column '{p.Column}' in the schema" + (matches.Count > 1 ? " (ambiguous case-insensitively)" : ""));
            }
            field = columns[position];
        }
        if (field.Type is not PrimitiveType type)
            throw new NotSupportedException($"Filters on '{p.Column}' are not supported: it is a {field.Type.TypeId}, not a primitive (filter on a field inside it, e.g. '{p.Column}.<field>')");
        // A required field inside an optional struct is still null when the struct is.
        var alwaysPresent = path is null ? field.Required : schema.AlwaysPresent(field.Id);

        switch (p.Op)
        {
            case Operation.IsNaN or Operation.NotNaN when type.TypeId is not (TypeId.Float or TypeId.Double):
                // Only floating-point values can be NaN.
                return p.Op == Operation.IsNaN ? Expression.False : Expression.True;
            case Operation.IsNull when alwaysPresent:
                return Expression.False;
            case Operation.NotNull when alwaysPresent:
                return Expression.True;
            case Operation.StartsWith or Operation.NotStartsWith when type.TypeId != TypeId.String:
                throw new ArgumentException($"{p.Op} needs a string column; '{field.Name}' is {type}");
        }
        var literals = p.Literals.Select(l => Literals.Convert(type, l, field.Name)).ToList();
        return new BoundPredicate(position, field.Id, p.Column, type, p.Op, literals) { StructPath = path };
    }
}

/// <summary>Converts user literals to the physical representation used in rows (days, micros, unscaled decimals…).</summary>
public static class Literals
{
    public static object Convert(IcebergType type, object? value, string column = "?")
    {
        if (value is null) throw new ArgumentException($"Null literal for '{column}': use IsNull / NotNull");
        if (value is float f && float.IsNaN(f) || value is double d && double.IsNaN(d))
            throw new ArgumentException($"NaN literal for '{column}': use IsNaN / NotNaN");
        var t = (PrimitiveType)type;
        object? converted = (t.TypeId, value) switch
        {
            (TypeId.Boolean, bool b) => b,
            (TypeId.Int, int i) => i,
            (TypeId.Int, long l) when l is >= int.MinValue and <= int.MaxValue => (int)l,
            (TypeId.Long, int i) => (long)i,
            (TypeId.Long, long l) => l,
            (TypeId.Float, float x) => x,
            (TypeId.Float, double x) => (float)x,
            (TypeId.Float, int or long) => System.Convert.ToSingle(value, CultureInfo.InvariantCulture),
            (TypeId.Double, double x) => x,
            (TypeId.Double, float x) => (double)x,
            (TypeId.Double, int or long) => System.Convert.ToDouble(value, CultureInfo.InvariantCulture),
            (TypeId.Decimal, IcebergDecimal m) => Rescale(m, ((DecimalType)t).Scale, column),
            (TypeId.Decimal, decimal m) => Rescale(IcebergDecimal.Parse(m.ToString(CultureInfo.InvariantCulture)), ((DecimalType)t).Scale, column),
            (TypeId.Decimal, string s) => Rescale(IcebergDecimal.Parse(s), ((DecimalType)t).Scale, column),
            (TypeId.Decimal, int or long) => Rescale(new IcebergDecimal(System.Convert.ToInt64(value, CultureInfo.InvariantCulture), 0), ((DecimalType)t).Scale, column),
            (TypeId.Date, int days) => days,
#if NET6_0_OR_GREATER
            (TypeId.Date, DateOnly date) => IcebergTime.DaysFromDate(date),
            (TypeId.Time, TimeOnly time) => IcebergTime.MicrosFromTime(time),
#else
            // The netstandard2.0 build loaded by a .NET 6+ app still gets DateOnly/TimeOnly values.
            (TypeId.Date, _) when value.GetType().FullName == "System.DateOnly" => (int)value.GetType().GetProperty("DayNumber")!.GetValue(value)! - 719_162,
            (TypeId.Time, _) when value.GetType().FullName == "System.TimeOnly" => (long)value.GetType().GetProperty("Ticks")!.GetValue(value)! / 10,
#endif
            (TypeId.Date, DateTime dt) => IcebergTime.DaysFromDate(dt),
            (TypeId.Date, string s) => IcebergTime.DaysFromIsoDate(s),
            (TypeId.Time, long us) => us,
            (TypeId.Time, TimeSpan time) => time.Ticks / 10,
            (TypeId.Timestamp or TypeId.TimestampTz, long us) => us,
            (TypeId.Timestamp or TypeId.TimestampTz, DateTime dt) => IcebergTime.MicrosFromTimestamp(dt),
            (TypeId.Timestamp or TypeId.TimestampTz, DateTimeOffset dto) => IcebergTime.MicrosFromTimestamp(dto),
            (TypeId.Timestamp or TypeId.TimestampTz, string s) => IcebergTime.MicrosFromTimestamp(DateTimeOffset.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal)),
            (TypeId.TimestampNs or TypeId.TimestampTzNs, long ns) => ns,
            (TypeId.TimestampNs or TypeId.TimestampTzNs, DateTime dt) => IcebergTime.NanosFromTimestamp(dt),
            (TypeId.TimestampNs or TypeId.TimestampTzNs, DateTimeOffset dto) => IcebergTime.NanosFromTimestamp(dto.UtcDateTime),
            (TypeId.String, string s) => s,
            (TypeId.Uuid, Guid g) => g,
            (TypeId.Uuid, string s) => Guid.Parse(s),
            (TypeId.Binary or TypeId.Fixed, byte[] b) => b,
            _ => null,
        };
        return converted ?? throw new ArgumentException($"Cannot use {value.GetType().Name} '{value}' as a {type} literal for '{column}'");
    }

    private static IcebergDecimal Rescale(IcebergDecimal m, int scale, string column)
    {
        if (m.Scale == scale) return m;
        if (m.Scale < scale) return new IcebergDecimal(m.Unscaled * System.Numerics.BigInteger.Pow(10, scale - m.Scale), scale);
        var factor = System.Numerics.BigInteger.Pow(10, m.Scale - scale);
        return m.Unscaled % factor == 0
            ? new IcebergDecimal(m.Unscaled / factor, scale)
            : throw new ArgumentException($"Decimal literal {m} has more scale than '{column}' (scale {scale})");
    }
}
