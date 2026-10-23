using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Expressions;

/// <summary>Total order on Iceberg values in their physical representation (spec "Sorting" / bound ordering).</summary>
public static class ValueComparer
{
    public static int Compare(object a, object b) => (a, b) switch
    {
        (string x, string y) => CompareCodePoints(x, y),
        (byte[] x, byte[] y) => x.AsSpan().SequenceCompareTo(y),
        (Guid x, Guid y) => x.ToByteArray(bigEndian: true).AsSpan().SequenceCompareTo(y.ToByteArray(bigEndian: true)),
        (int x, long y) => ((long)x).CompareTo(y),
        (long x, int y) => x.CompareTo(y),
        (float x, double y) => ((double)x).CompareTo(y),
        (double x, float y) => x.CompareTo(y),
        _ => ((IComparable)a).CompareTo(b),
    };

    /// <summary>Unicode code-point order (= UTF-8 byte order), which ordinal UTF-16 comparison is not above U+E000.</summary>
    public static int CompareCodePoints(string a, string b)
    {
        var n = Math.Min(a.Length, b.Length);
        for (var i = 0; i < n; i++)
        {
            if (a[i] == b[i]) continue;
            return Fix(a[i]) - Fix(b[i]);
        }
        return a.Length - b.Length;

        // Move surrogates (U+D800–U+DFFF, which encode code points ≥ U+10000) above U+E000–U+FFFF.
        static int Fix(char c) => c >= 0xD800 ? (c >= 0xE000 ? c - 0x800 : c + 0x2000) : c;
    }

    public static bool IsNaN(object? v) => v is float f && float.IsNaN(f) || v is double d && double.IsNaN(d);
}

/// <summary>Evaluates a bound expression against tuples (rows, or partition tuples after projection).</summary>
public static class RowEvaluator
{
    public static Func<IReadOnlyList<object?>, bool> Compile(Expression bound) => bound switch
    {
        AlwaysTrue => _ => true,
        AlwaysFalse => _ => false,
        And a => Combine(Compile(a.Left), Compile(a.Right), and: true),
        Or o => Combine(Compile(o.Left), Compile(o.Right), and: false),
        BoundPredicate p => row => Test(p, p.ValueIn(row)),
        Not n => Compile(Expr.RewriteNot(n)),
        _ => throw new ArgumentException($"Expression is not bound: {bound}"),
    };

    private static Func<IReadOnlyList<object?>, bool> Combine(Func<IReadOnlyList<object?>, bool> l, Func<IReadOnlyList<object?>, bool> r, bool and) =>
        and ? row => l(row) && r(row) : row => l(row) || r(row);

    public static bool Test(BoundPredicate p, object? v)
    {
        switch (p.Op)
        {
            case Operation.IsNull: return v is null;
            case Operation.NotNull: return v is not null;
            case Operation.IsNaN: return ValueComparer.IsNaN(v);
            case Operation.NotNaN: return !ValueComparer.IsNaN(v);
            case Operation.NotEq: return !Test(p with { Op = Operation.Eq }, v);
            case Operation.NotIn: return !Test(p with { Op = Operation.In }, v);
            case Operation.NotStartsWith: return !Test(p with { Op = Operation.StartsWith }, v);
        }
        if (v is null || ValueComparer.IsNaN(v)) return false;
        return p.Op switch
        {
            Operation.Lt => ValueComparer.Compare(v, p.Literal) < 0,
            Operation.LtEq => ValueComparer.Compare(v, p.Literal) <= 0,
            Operation.Gt => ValueComparer.Compare(v, p.Literal) > 0,
            Operation.GtEq => ValueComparer.Compare(v, p.Literal) >= 0,
            Operation.Eq => ValueComparer.Compare(v, p.Literal) == 0,
            Operation.In => p.Literals.Any(l => ValueComparer.Compare(v, l) == 0),
            Operation.StartsWith => v is string s && s.StartsWith((string)p.Literal, StringComparison.Ordinal),
            _ => throw new ArgumentOutOfRangeException(nameof(p)),
        };
    }
}

/// <summary>
/// Inclusive pruning on value bounds: false only when no value in [lower, upper] (plus the null/NaN counts) can
/// satisfy the predicate. Shared by file metrics (<see cref="MetricsEvaluator"/>) and manifest partition summaries
/// (<see cref="ManifestEvaluator"/>). Bounds exclude NaN by spec; a NaN bound from another writer is ignored.
/// </summary>
internal static class BoundsPruning
{
    /// <param name="noComparableValues">Every value is null or NaN, so no comparison can be true.</param>
    public static bool MightMatch(BoundPredicate p, object? lower, object? upper, bool noComparableValues)
    {
        if (ValueComparer.IsNaN(lower)) lower = null;
        if (ValueComparer.IsNaN(upper)) upper = null;
        switch (p.Op)
        {
            case Operation.NotEq or Operation.NotIn or Operation.NotStartsWith:
                return true;
            case Operation.IsNull or Operation.NotNull or Operation.IsNaN or Operation.NotNaN:
                throw new ArgumentException("null/NaN predicates are decided by counts, not bounds");
        }
        if (noComparableValues) return false;
        int Cmp(object bound, object lit) => ValueComparer.Compare(bound, lit);
        return p.Op switch
        {
            Operation.Lt => lower is null || Cmp(lower, p.Literal) < 0,
            Operation.LtEq => lower is null || Cmp(lower, p.Literal) <= 0,
            Operation.Gt => upper is null || Cmp(upper, p.Literal) > 0,
            Operation.GtEq => upper is null || Cmp(upper, p.Literal) >= 0,
            Operation.Eq => (lower is null || Cmp(lower, p.Literal) <= 0) && (upper is null || Cmp(upper, p.Literal) >= 0),
            Operation.In => p.Literals.Any(l => (lower is null || Cmp(lower, l) <= 0) && (upper is null || Cmp(upper, l) >= 0)),
            Operation.StartsWith => StartsWithMightMatch((string)p.Literal, lower as string, upper as string),
            _ => true,
        };
    }

    private static bool StartsWithMightMatch(string prefix, string? lower, string? upper)
    {
        // Values starting with prefix lie in [prefix, prefix + max]: compare the bounds cut to the prefix length.
        if (lower is not null && ValueComparer.CompareCodePoints(lower[..Math.Min(lower.Length, prefix.Length)], prefix) > 0) return false;
        if (upper is not null && ValueComparer.CompareCodePoints(upper[..Math.Min(upper.Length, prefix.Length)], prefix) < 0) return false;
        return true;
    }

    public static object? Decode(IcebergType type, byte[]? bytes)
    {
        if (bytes is null) return null;
        try
        {
            return Conversions.FromBytes(type, bytes);
        }
        catch (Exception e) when (e is ArgumentException or FormatException or InvalidDataException or NotSupportedException)
        {
            return null; // undecodable bound: no information, never prune on it
        }
    }
}

/// <summary>Can a data or delete file contain rows matching a bound expression, judging by its column metrics?</summary>
public sealed class MetricsEvaluator(Expression bound)
{
    private readonly Expression _expr = Expr.RewriteNot(bound);

    public bool MightMatch(DataFile file) => file.RecordCount != 0 && Eval(_expr, file);

    private static bool Eval(Expression e, DataFile f) => e switch
    {
        AlwaysTrue => true,
        AlwaysFalse => false,
        And a => Eval(a.Left, f) && Eval(a.Right, f),
        Or o => Eval(o.Left, f) || Eval(o.Right, f),
        BoundPredicate p => Test(p, f),
        _ => throw new ArgumentException($"Expression is not bound: {e}"),
    };

    private static bool Test(BoundPredicate p, DataFile f)
    {
        static long? Count(IReadOnlyDictionary<int, long>? counts, int id) => counts is not null && counts.TryGetValue(id, out var n) ? n : null;
        var values = Count(f.ValueCounts, p.FieldId);
        var nulls = Count(f.NullValueCounts, p.FieldId);
        var nans = Count(f.NanValueCounts, p.FieldId);
        var allNull = values is not null && nulls == values;
        switch (p.Op)
        {
            case Operation.IsNull: return nulls != 0;
            case Operation.NotNull: return !allNull;
            case Operation.IsNaN: return nans != 0 && !allNull;
            case Operation.NotNaN: return !(values is not null && nans == values);
        }
        var noComparable = values is not null && (nulls ?? 0) + (nans ?? 0) == values && (nulls is not null || nans is not null);
        return BoundsPruning.MightMatch(p,
            BoundsPruning.Decode(p.Type, f.LowerBounds?.GetValueOrDefault(p.FieldId)),
            BoundsPruning.Decode(p.Type, f.UpperBounds?.GetValueOrDefault(p.FieldId)),
            noComparable);
    }
}

/// <summary>Can a manifest contain matching files, judging by its partition field summaries? Takes a projected expression.</summary>
public sealed class ManifestEvaluator(Expression projected)
{
    public bool MightMatch(IReadOnlyList<FieldSummary>? summaries) => summaries is null || Eval(projected, summaries);

    private static bool Eval(Expression e, IReadOnlyList<FieldSummary> s) => e switch
    {
        AlwaysTrue => true,
        AlwaysFalse => false,
        And a => Eval(a.Left, s) && Eval(a.Right, s),
        Or o => Eval(o.Left, s) || Eval(o.Right, s),
        BoundPredicate p => p.Position >= s.Count || Test(p, s[p.Position]),
        _ => throw new ArgumentException($"Expression is not bound: {e}"),
    };

    private static bool Test(BoundPredicate p, FieldSummary s)
    {
        switch (p.Op)
        {
            case Operation.IsNull: return s.ContainsNull;
            case Operation.IsNaN: return s.ContainsNan != false;
            case Operation.NotNull or Operation.NotNaN: return true; // absent bounds do not prove "only nulls" for every writer
        }
        return BoundsPruning.MightMatch(p, BoundsPruning.Decode(p.Type, s.LowerBound), BoundsPruning.Decode(p.Type, s.UpperBound), noComparableValues: false);
    }
}

/// <summary>
/// Inclusive projection of a row filter onto a partition spec (spec "Scan Planning"): an expression over partition
/// tuples that is true for the partition of every row the filter can match. Bound to partition-field positions.
/// </summary>
public static class Projections
{
    public static Expression Inclusive(PartitionSpec spec, Schema schema, Expression bound)
    {
        var partitionType = spec.PartitionType(schema);
        return Project(Expr.RewriteNot(bound), spec, schema, partitionType);
    }

    private static Expression Project(Expression e, PartitionSpec spec, Schema schema, StructType partitionType) => e switch
    {
        AlwaysTrue or AlwaysFalse => e,
        And a => Expr.And(Project(a.Left, spec, schema, partitionType), Project(a.Right, spec, schema, partitionType)),
        Or o => Expr.Or(Project(o.Left, spec, schema, partitionType), Project(o.Right, spec, schema, partitionType)),
        BoundPredicate p => spec.Fields
            .Select((f, i) => (f, i))
            .Where(x => x.f.AllSourceIds.Count == 1 && x.f.SourceId == p.FieldId)
            .Select(x => ProjectField(p, x.f, x.i, partitionType.Fields[x.i].Type))
            .Aggregate(Expression.True, Expr.And),
        _ => throw new ArgumentException($"Expression is not bound: {e}"),
    };

    private static Expression ProjectField(BoundPredicate p, PartitionField field, int position, IcebergType resultType)
    {
        Expression Pred(Operation op, params object[] lits) => new BoundPredicate(position, field.FieldId, field.Name, resultType, op, lits);
        object? Apply(object v) => field.Transform.Apply(p.Type, v);
        Expression Applied(Operation op, object v) => Apply(v) is { } r ? Pred(op, r) : Expression.True;
        Expression AppliedAll(Operation op) => Pred(op, p.Literals.Select(Apply).Distinct().Cast<object>().ToArray());

        switch (field.Transform)
        {
            case IdentityTransform:
                return Pred(p.Op, [.. p.Literals]);
            case VoidTransform or UnknownTransform:
                return Expression.True;
        }
        if (p.Op is Operation.IsNull or Operation.NotNull) return Pred(p.Op); // transforms map null to null and only null
        switch (field.Transform)
        {
            case BucketTransform:
                return p.Op switch { Operation.Eq => Applied(Operation.Eq, p.Literal), Operation.In => AppliedAll(Operation.In), _ => Expression.True };

            case TruncateTransform t when p.Type.TypeId is TypeId.String or TypeId.Binary:
                return p.Op switch
                {
                    Operation.Lt or Operation.LtEq => Applied(Operation.LtEq, p.Literal),
                    Operation.Gt or Operation.GtEq => Applied(Operation.GtEq, p.Literal),
                    Operation.Eq => Applied(Operation.Eq, p.Literal),
                    Operation.In => AppliedAll(Operation.In),
                    // A prefix at least as long as the width fixes the partition; a shorter one is a prefix of it.
                    Operation.StartsWith => ((string)p.Literal).EnumerateRunes().Count() >= t.Width
                        ? Applied(Operation.Eq, p.Literal)
                        : Pred(Operation.StartsWith, p.Literal),
                    _ => Expression.True,
                };

            case TruncateTransform or TimeTransform:
                // Order-preserving and integer-valued: x < v ⇒ f(x) ≤ f(v − 1); x > v ⇒ f(x) ≥ f(v + 1).
                return p.Op switch
                {
                    Operation.Lt => Step(p.Literal, -1) is { } prev ? Applied(Operation.LtEq, prev) : Expression.True,
                    Operation.LtEq => Applied(Operation.LtEq, p.Literal),
                    Operation.Gt => Step(p.Literal, +1) is { } next ? Applied(Operation.GtEq, next) : Expression.True,
                    Operation.GtEq => Applied(Operation.GtEq, p.Literal),
                    Operation.Eq => Applied(Operation.Eq, p.Literal),
                    Operation.In => AppliedAll(Operation.In),
                    _ => Expression.True,
                };

            default:
                return Expression.True;
        }
    }

    /// <summary>The adjacent value in the source domain (days, micros, nanos, unscaled decimals), or null on overflow.</summary>
    private static object? Step(object v, int delta) => v switch
    {
        int i => (long)i + delta is var n && n is >= int.MinValue and <= int.MaxValue ? (int)n : null,
        long l => delta > 0 ? (l == long.MaxValue ? null : l + 1) : (l == long.MinValue ? null : l - 1),
        IcebergDecimal d => new IcebergDecimal(d.Unscaled + delta, d.Scale),
        _ => null,
    };
}
