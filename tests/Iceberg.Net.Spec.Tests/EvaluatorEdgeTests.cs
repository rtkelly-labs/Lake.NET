using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// Edges of comparison, metrics pruning, manifest pruning, projection and literal conversion (written from the
/// survivors of the stryker-core mutation run). Every "might match" here is checked against what pruning may skip.
/// </summary>
[Spec("Scan Planning")]
[Spec("Partition Transforms")]
public sealed class EvaluatorEdgeTests
{
    private static readonly Schema Schema = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "x", PrimitiveType.Double),
        NestedField.Optional(3, "s", PrimitiveType.String),
        NestedField.Optional(4, "n", PrimitiveType.Int),
        NestedField.Optional(5, "d", new DecimalType(9, 2)),
        NestedField.Optional(6, "u", PrimitiveType.Uuid),
    ]);

    [Fact]
    public void Uuids_compare_as_unsigned_big_endian_and_strings_by_code_point()
    {
        var low = Guid.Parse("7fffffff-0000-0000-0000-000000000000");
        var high = Guid.Parse("80000000-0000-0000-0000-000000000000");
        Assert.True(ValueComparer.Compare(low, high) < 0);
        Assert.True(ValueComparer.Compare(high, low) > 0);
        Assert.Equal(0, ValueComparer.Compare(low, low));
        // U+FFFF < U+10000 by code point (UTF-16 order says otherwise); U+E000 > U+D7FF; surrogate pairs above both.
        Assert.True(ValueComparer.CompareCodePoints("￿", "\U00010000") < 0);
        Assert.True(ValueComparer.CompareCodePoints("", "퟿") > 0);
        Assert.True(ValueComparer.CompareCodePoints("\U00010000", "") > 0);
        Assert.True(ValueComparer.CompareCodePoints("퟿", "\U00010000") < 0);
    }

    private static DataFile File(int id, object? lower, object? upper, long values, long? nulls = 0, long? nans = null, IcebergType? type = null)
    {
        var t = type ?? Schema.FindType(id)!;
        return new DataFile
        {
            FilePath = "f", RecordCount = values, FileSizeInBytes = 1,
            ValueCounts = new Dictionary<int, long> { [id] = values },
            NullValueCounts = nulls is { } n ? new Dictionary<int, long> { [id] = n } : null,
            NanValueCounts = nans is { } a ? new Dictionary<int, long> { [id] = a } : null,
            LowerBounds = lower is null ? null : new Dictionary<int, byte[]> { [id] = Conversions.ToBytes(t, lower) },
            UpperBounds = upper is null ? null : new Dictionary<int, byte[]> { [id] = Conversions.ToBytes(t, upper) },
        };
    }

    private static bool Might(Expression e, DataFile f) => new MetricsEvaluator(Binder.Bind(Schema, e)).MightMatch(f);

    [Fact]
    public void Inclusive_comparisons_match_at_the_bound()
    {
        var f = File(1, 10L, 20L, 5);
        Assert.True(Might(Expr.LessThanOrEqual("id", 10L), f));
        Assert.False(Might(Expr.LessThan("id", 10L), f));
        Assert.True(Might(Expr.GreaterThanOrEqual("id", 20L), f));
        Assert.False(Might(Expr.GreaterThan("id", 20L), f));
        Assert.True(Might(Expr.In("id", 1L, 15L), f));
        Assert.False(Might(Expr.In("id", 1L, 21L), f));
        Assert.True(Might(Expr.NotEqual("id", 15L), f)); // never pruned by bounds
    }

    [Fact]
    public void Counts_decide_null_and_nan_predicates()
    {
        var allNull = File(2, null, null, 4, nulls: 4, nans: 0);
        Assert.False(Might(Expr.LessThan("x", 1.0), allNull)); // nothing comparable
        Assert.False(Might(Expr.IsNaN("x"), allNull));
        Assert.True(Might(Expr.IsNull("x"), allNull));
        Assert.False(Might(Expr.NotNull("x"), allNull));

        var nullsAndNans = File(2, null, null, 4, nulls: 2, nans: 2);
        Assert.False(Might(Expr.GreaterThan("x", 0.0), nullsAndNans));
        Assert.True(Might(Expr.IsNaN("x"), nullsAndNans));
        Assert.True(Might(Expr.NotNaN("x"), nullsAndNans));

        var allNan = File(2, null, null, 3, nulls: 0, nans: 3);
        Assert.False(Might(Expr.NotNaN("x"), allNan));
        Assert.True(Might(Expr.IsNaN("x"), allNan));

        // No counts at all: nothing can be ruled out.
        var unknown = new DataFile { FilePath = "f", RecordCount = 3, FileSizeInBytes = 1 };
        Assert.True(Might(Expr.LessThan("x", 1.0), unknown));
        Assert.True(Might(Expr.IsNull("x"), unknown));
        Assert.True(Might(Expr.NotNaN("x"), unknown));
        // NaN counts without null counts still say "only NaN" when they cover every value.
        Assert.False(Might(Expr.LessThan("x", 1.0), File(2, null, null, 3, nulls: null, nans: 3)));
        Assert.True(Might(Expr.LessThan("x", 1.0), File(2, null, null, 3, nulls: null, nans: 2)));
    }

    [Fact]
    public void Undecodable_bounds_never_prune()
    {
        var f = new DataFile
        {
            FilePath = "f", RecordCount = 1, FileSizeInBytes = 1,
            ValueCounts = new Dictionary<int, long> { [4] = 1 }, NullValueCounts = new Dictionary<int, long> { [4] = 0 },
            LowerBounds = new Dictionary<int, byte[]> { [4] = [1, 2] }, UpperBounds = new Dictionary<int, byte[]> { [4] = [1, 2] }, // not 4-byte ints
        };
        Assert.True(Might(Expr.Equal("n", 999), f));
    }

    [Fact]
    public void Manifest_summaries_prune_only_what_they_prove()
    {
        var spec = PartitionSpec.BuilderFor(Schema).Identity("n").Identity("s").Build();
        ManifestEvaluator Eval(Expression e) => new(Projections.Inclusive(spec, Schema, Binder.Bind(Schema, e)));
        var summaries = new[]
        {
            new FieldSummary(false, null, Conversions.ToBytes(PrimitiveType.Int, 10), Conversions.ToBytes(PrimitiveType.Int, 20)),
            new FieldSummary(true, false, Conversions.ToBytes(PrimitiveType.String, "a"), Conversions.ToBytes(PrimitiveType.String, "c")),
        };
        Assert.False(Eval(Expression.False).MightMatch(summaries));
        Assert.True(Eval(Expression.True).MightMatch(summaries));
        Assert.False(Eval(Expr.And(Expr.Equal("n", 15), Expr.Equal("s", "z"))).MightMatch(summaries)); // both must hold
        Assert.True(Eval(Expr.Or(Expr.Equal("n", 99), Expr.Equal("s", "b"))).MightMatch(summaries));
        Assert.False(Eval(Expr.IsNull("n")).MightMatch(summaries));
        Assert.True(Eval(Expr.IsNull("s")).MightMatch(summaries));
        Assert.True(Eval(Expr.Equal("n", 15)).MightMatch(null)); // no summaries: no information
        // A predicate on a partition field the summaries do not cover (older manifest) cannot prune.
        Assert.True(Eval(Expr.Equal("s", "z")).MightMatch([summaries[0]]));

        var dspec = PartitionSpec.BuilderFor(Schema).Identity("x").Build();
        var nanFree = new[] { new FieldSummary(false, false, Conversions.ToBytes(PrimitiveType.Double, 1.0), Conversions.ToBytes(PrimitiveType.Double, 2.0)) };
        Assert.False(new ManifestEvaluator(Projections.Inclusive(dspec, Schema, Binder.Bind(Schema, Expr.IsNaN("x")))).MightMatch(nanFree));
        var unknownNan = new[] { new FieldSummary(false, null, null, null) };
        Assert.True(new ManifestEvaluator(Projections.Inclusive(dspec, Schema, Binder.Bind(Schema, Expr.IsNaN("x")))).MightMatch(unknownNan));
    }

    [Fact]
    public void Strict_comparisons_project_through_order_preserving_transforms_without_overflow()
    {
        var spec = PartitionSpec.BuilderFor(Schema).Truncate("n", 10).Truncate("id", 10).Truncate("d", 100).Build();
        Expression P(Expression e) => Projections.Inclusive(spec, Schema, Binder.Bind(Schema, e));
        // n < 15 means n <= 14, whose truncation is <= 10.
        Assert.Equal("n_trunc <= 10", P(Expr.LessThan("n", 15)).ToString());
        Assert.Equal("n_trunc >= 10", P(Expr.GreaterThan("n", 15)).ToString());
        // At the domain edge there is no adjacent value: the projection gives up (true) instead of wrapping.
        Assert.Equal("true", P(Expr.LessThan("n", int.MinValue)).ToString());
        Assert.Equal("true", P(Expr.GreaterThan("n", int.MaxValue)).ToString());
        Assert.Equal("true", P(Expr.LessThan("id", long.MinValue)).ToString());
        Assert.Equal("true", P(Expr.GreaterThan("id", long.MaxValue)).ToString());
        Assert.Equal("id_trunc <= 0", P(Expr.LessThan("id", 1L)).ToString());
        // Decimals step by one unscaled unit: d > 1.99 means d >= 2.00.
        Assert.Equal("d_trunc >= 2.00", P(Expr.GreaterThan("d", IcebergDecimal.Parse("1.99"))).ToString());
        Assert.Equal("d_trunc <= 0.00", P(Expr.LessThan("d", IcebergDecimal.Parse("1.00"))).ToString());
        // starts-with through truncate: an equal-or-longer prefix becomes equality on the truncated value.
        var sspec = PartitionSpec.BuilderFor(Schema).Truncate("s", 2).Build();
        Assert.Equal("s_trunc == 'ab'", Projections.Inclusive(sspec, Schema, Binder.Bind(Schema, Expr.StartsWith("s", "abc"))).ToString());
        Assert.Equal("s_trunc == 'ab'", Projections.Inclusive(sspec, Schema, Binder.Bind(Schema, Expr.StartsWith("s", "ab"))).ToString());
        Assert.StartsWith("s_trunc starts with", Projections.Inclusive(sspec, Schema, Binder.Bind(Schema, Expr.StartsWith("s", "a"))).ToString());
    }

    [Fact]
    public void Factory_simplifies_empty_sets_and_constants()
    {
        Assert.Same(Expression.False, Expr.In("id", Array.Empty<object>()));
        Assert.Same(Expression.True, Expr.NotIn("id", Array.Empty<object>()));
        Assert.IsType<UnboundPredicate>(Expr.In("id", 1L));
        Assert.IsType<UnboundPredicate>(Expr.NotIn("id", 1L));
        var p = Expr.Equal("id", 1L);
        Assert.Same(Expression.False, Expr.And(Expression.False, p));
        Assert.Same(Expression.False, Expr.And(p, Expression.False));
        Assert.Same(p, Expr.And(Expression.True, p));
        Assert.Same(Expression.True, Expr.Or(Expression.True, p));
        Assert.Same(Expression.True, Expr.Or(p, Expression.True));
        Assert.Same(p, Expr.Or(Expression.False, p));
        Assert.Equal("x == null", new UnboundPredicate("x", Operation.Eq, [null]).ToString());
    }

    [Fact]
    public void Literals_convert_within_range_and_rescale_exactly()
    {
        Assert.Equal(5, Literals.Convert(PrimitiveType.Int, 5L));
        Assert.Equal(int.MaxValue, Literals.Convert(PrimitiveType.Int, (long)int.MaxValue));
        Assert.Equal(int.MinValue, Literals.Convert(PrimitiveType.Int, (long)int.MinValue));
        Assert.ThrowsAny<ArgumentException>(() => Literals.Convert(PrimitiveType.Int, (long)int.MaxValue + 1));
        Assert.ThrowsAny<ArgumentException>(() => Literals.Convert(PrimitiveType.Int, (long)int.MinValue - 1));
        Assert.Throws<ArgumentException>(() => Literals.Convert(PrimitiveType.Int, null));
        var d = new DecimalType(9, 2);
        Assert.Equal(IcebergDecimal.Parse("1.50"), Literals.Convert(d, IcebergDecimal.Parse("1.5")));
        Assert.Equal(IcebergDecimal.Parse("1.50"), Literals.Convert(d, IcebergDecimal.Parse("1.500")));
        Assert.Equal(IcebergDecimal.Parse("-12.30"), Literals.Convert(d, IcebergDecimal.Parse("-12.3000")));
        Assert.ThrowsAny<ArgumentException>(() => Literals.Convert(d, IcebergDecimal.Parse("1.505"))); // would lose a digit
    }

    [Fact]
    public void Transforms_validate_parameters_and_report_order_and_types()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Transform.Bucket(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Transform.Truncate(0));
        Transform.Bucket(1);
        Transform.Truncate(1);
        Assert.Null(Transform.Bucket(4).Apply(PrimitiveType.Long, null));
        Assert.Equal(new byte[] { 1, 2 }, (byte[])Transform.Truncate(2).Apply(PrimitiveType.Binary, new byte[] { 1, 2 })!);
        Assert.Equal(new byte[] { 1, 2 }, (byte[])Transform.Truncate(2).Apply(PrimitiveType.Binary, new byte[] { 1, 2, 3 })!);
        Assert.Equal(new byte[] { 1 }, (byte[])Transform.Truncate(2).Apply(PrimitiveType.Binary, new byte[] { 1 })!);
        Assert.True(Transform.Identity.PreservesOrder);
        Assert.True(Transform.Truncate(3).PreservesOrder);
        Assert.True(Transform.Day.PreservesOrder);
        Assert.False(Transform.Bucket(3).PreservesOrder);
        Assert.False(Transform.Void.PreservesOrder);
        Assert.True(Transform.Void.CanTransform(new StructType([NestedField.Optional(1, "a", PrimitiveType.Int)])));
        Assert.False(Transform.Identity.CanTransform(new StructType([NestedField.Optional(1, "a", PrimitiveType.Int)])));
        Assert.True(Transform.Identity.CanTransform(PrimitiveType.String));
        Assert.Equal("true", Transform.Identity.ToHumanString(PrimitiveType.Boolean, true));
        Assert.Equal("false", Transform.Identity.ToHumanString(PrimitiveType.Boolean, false));
    }
}
