using Iceberg.Net.Expressions;
using Iceberg.Net.Manifests;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>Expression binding, row semantics (nulls, NaN), metrics pruning and inclusive partition projection.</summary>
[Spec("Scan Planning")]
public sealed class ExpressionTests
{
    private static readonly Schema S = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "name", PrimitiveType.String),
        NestedField.Optional(3, "score", PrimitiveType.Double),
        NestedField.Optional(4, "ts", PrimitiveType.TimestampTz),
        NestedField.Optional(5, "day", PrimitiveType.Date),
        NestedField.Optional(6, "amount", new DecimalType(9, 2)),
        NestedField.Optional(7, "n", PrimitiveType.Int),
    ]);

    private static bool Row(Expression e, params object?[] row) => RowEvaluator.Compile(Binder.Bind(S, e))(row);

    private static object?[] R(long id, string? name = null, double? score = null) => [id, name, score, null, null, null, null];

    [Fact]
    public void Comparisons_with_null_or_nan_are_false_and_negations_are_their_complements()
    {
        Assert.False(Row(Expr.Equal("name", "a"), R(1)));
        Assert.True(Row(Expr.NotEqual("name", "a"), R(1)));
        Assert.False(Row(Expr.LessThan("score", 1.0), R(1, score: double.NaN)));
        Assert.False(Row(Expr.GreaterThan("score", 1.0), R(1, score: double.NaN)));
        Assert.True(Row(Expr.NotEqual("score", 1.0), R(1, score: double.NaN)));
        Assert.True(Row(Expr.IsNaN("score"), R(1, score: double.NaN)));
        Assert.True(Row(Expr.Not(Expr.LessThan("id", 5)), R(5)));
        Assert.False(Row(Expr.Not(Expr.In("id", 1L, 5L)), R(5)));
        Assert.True(Row(Expr.NotIn("name", "x"), R(1)));
        Assert.True(Row(Expr.StartsWith("name", "ab"), R(1, "abc")));
        Assert.False(Row(Expr.Not(Expr.StartsWith("name", "ab")), R(1, "abc")));
        Assert.True(Row(Expr.Or(Expr.IsNull("name"), Expr.Equal("id", 9L)), R(1)));
    }

    [Fact]
    public void Binding_converts_literals_and_rejects_what_cannot_be_compared()
    {
        var bound = (BoundPredicate)Binder.Bind(S, Expr.Equal("DAY", new DateOnly(1970, 1, 3))); // case-insensitive fallback
        Assert.Equal(2, bound.Literal);
        Assert.Equal(new IcebergDecimal(1230, 2), ((BoundPredicate)Binder.Bind(S, Expr.Equal("amount", "12.3"))).Literal);
        Assert.Equal(86_400_000_000L, ((BoundPredicate)Binder.Bind(S, Expr.LessThan("ts", "1970-01-02T00:00:00Z"))).Literal);
        Assert.Equal(5L, ((BoundPredicate)Binder.Bind(S, Expr.Equal("id", 5))).Literal); // int literal on a long column
        Assert.Throws<ArgumentException>(() => Binder.Bind(S, Expr.Equal("missing", 1)));
        Assert.Throws<ArgumentException>(() => Binder.Bind(S, Expr.LessThan("score", double.NaN)));
        Assert.Throws<ArgumentException>(() => Binder.Bind(S, Expr.Equal("amount", "1.234"))); // more scale than the column
        Assert.Throws<ArgumentException>(() => Binder.Bind(S, Expr.StartsWith("id", "1")));
        // Facts known from the schema simplify away.
        Assert.Equal(Expression.False, Binder.Bind(S, Expr.IsNull("id")));
        Assert.Equal(Expression.False, Binder.Bind(S, Expr.IsNaN("name")));
    }

    [Fact]
    public void String_order_is_code_point_order()
    {
        // U+FFFD sorts below U+1F600 (😀, a surrogate pair) in code points, above it in UTF-16 ordinal order.
        Assert.True(ValueComparer.CompareCodePoints("�", "😀") < 0);
        Assert.True(string.CompareOrdinal("�", "😀") > 0);
        Assert.True(ValueComparer.CompareCodePoints("ab", "abc") < 0);
    }

    private static DataFile File(long? lower, long? upper, long nulls = 0, long values = 10) => new()
    {
        FilePath = "f", RecordCount = values, FileSizeInBytes = 1,
        ValueCounts = new Dictionary<int, long> { [1] = values, [2] = values },
        NullValueCounts = new Dictionary<int, long> { [1] = 0, [2] = nulls },
        LowerBounds = lower is null ? null : new Dictionary<int, byte[]> { [1] = Conversions.ToBytes(PrimitiveType.Long, lower.Value) },
        UpperBounds = upper is null ? null : new Dictionary<int, byte[]> { [1] = Conversions.ToBytes(PrimitiveType.Long, upper.Value) },
    };

    private static bool Metrics(Expression e, DataFile f) => new MetricsEvaluator(Binder.Bind(S, e)).MightMatch(f);

    [Fact]
    public void Metrics_prune_only_files_that_cannot_match()
    {
        var f = File(10, 20);
        Assert.False(Metrics(Expr.LessThan("id", 10), f));
        Assert.True(Metrics(Expr.LessThanOrEqual("id", 10), f));
        Assert.False(Metrics(Expr.GreaterThan("id", 20), f));
        Assert.True(Metrics(Expr.GreaterThanOrEqual("id", 20), f));
        Assert.False(Metrics(Expr.Equal("id", 21), f));
        Assert.True(Metrics(Expr.Equal("id", 15), f));
        Assert.False(Metrics(Expr.In("id", 1L, 2L, 30L), f));
        Assert.True(Metrics(Expr.In("id", 1L, 12L), f));
        Assert.True(Metrics(Expr.NotEqual("id", 15), f));
        Assert.True(Metrics(Expr.LessThan("id", 0), File(null, null))); // no bounds: no information
        Assert.False(Metrics(Expr.IsNull("name"), f)); // null count 0
        Assert.False(Metrics(Expr.NotNull("name"), File(10, 20, nulls: 10))); // all null
        Assert.False(Metrics(Expr.Or(Expr.LessThan("id", 5), Expr.GreaterThan("id", 25)), f));
        Assert.False(Metrics(Expr.Not(Expr.And(Expr.GreaterThanOrEqual("id", 10), Expr.LessThanOrEqual("id", 20))), f));
        Assert.False(Metrics(Expr.Equal("id", 1), File(10, 20) with { RecordCount = 0 }));
    }

    [Fact]
    public void Starts_with_prunes_on_prefix_ranges()
    {
        var f = new DataFile
        {
            FilePath = "f", RecordCount = 3, FileSizeInBytes = 1,
            LowerBounds = new Dictionary<int, byte[]> { [2] = Conversions.ToBytes(PrimitiveType.String, "banana") },
            UpperBounds = new Dictionary<int, byte[]> { [2] = Conversions.ToBytes(PrimitiveType.String, "cherry") },
        };
        Assert.False(Metrics(Expr.StartsWith("name", "apple"), f));
        Assert.False(Metrics(Expr.StartsWith("name", "date"), f));
        Assert.True(Metrics(Expr.StartsWith("name", "b"), f));
        Assert.True(Metrics(Expr.StartsWith("name", "ch"), f));
        Assert.True(Metrics(Expr.StartsWith("name", "bananaz"), f));
    }

    private static Expression Project(PartitionSpec spec, Expression e) => Projections.Inclusive(spec, S, Binder.Bind(S, e));

    [Fact]
    public void Projection_through_transforms_keeps_every_matching_partition()
    {
        var day = PartitionSpec.BuilderFor(S).Day("ts").Build();
        var dayOf = (int)Transform.Day.Apply(PrimitiveType.TimestampTz, 3 * 86_400_000_000L)!;
        // ts < midnight of day 3  ⇒  day(ts) <= 2 ; ts <= midnight ⇒ day(ts) <= 3
        Assert.Equal("ts_day <= 2", Project(day, Expr.LessThan("ts", 3 * 86_400_000_000L)).ToString());
        Assert.Equal($"ts_day <= {dayOf}", Project(day, Expr.LessThanOrEqual("ts", 3 * 86_400_000_000L)).ToString());
        Assert.Equal("ts_day >= 3", Project(day, Expr.GreaterThan("ts", 3 * 86_400_000_000L - 1)).ToString());
        Assert.Equal(Expression.True, Project(day, Expr.NotEqual("ts", 0L)));

        var bucket = PartitionSpec.BuilderFor(S).Bucket("id", 16).Build();
        var b = BucketTransform.Hash(PrimitiveType.Long, 34L) & int.MaxValue;
        Assert.Equal($"id_bucket == {b % 16}", Project(bucket, Expr.Equal("id", 34L)).ToString());
        Assert.Equal(Expression.True, Project(bucket, Expr.LessThan("id", 34L)));

        var trunc = PartitionSpec.BuilderFor(S).Truncate("name", 3).Build();
        Assert.Equal("name_trunc == 'abc'", Project(trunc, Expr.StartsWith("name", "abcdef")).ToString());
        Assert.Equal("name_trunc starts with 'ab'", Project(trunc, Expr.StartsWith("name", "ab")).ToString());
        Assert.Equal("name_trunc >= 'abc'", Project(trunc, Expr.GreaterThan("name", "abcz")).ToString());

        var truncInt = PartitionSpec.BuilderFor(S).Truncate("n", 10).Build();
        Assert.Equal("n_trunc <= 20", Project(truncInt, Expr.LessThan("n", 30)).ToString());
        Assert.Equal("n_trunc >= 30", Project(truncInt, Expr.GreaterThan("n", 29)).ToString());

        var identity = PartitionSpec.BuilderFor(S).Identity("name").Build();
        Assert.Equal("name != 'a'", Project(identity, Expr.Not(Expr.Equal("name", "a"))).ToString());
        Assert.Equal("name is null", Project(identity, Expr.IsNull("name")).ToString());
    }

    [Fact]
    public void Manifest_summaries_prune_on_partition_bounds()
    {
        var spec = PartitionSpec.BuilderFor(S).Identity("n").Build();
        var summary = new FieldSummary(false, null, Conversions.ToBytes(PrimitiveType.Int, 5), Conversions.ToBytes(PrimitiveType.Int, 9));
        bool Might(Expression e) => new ManifestEvaluator(Project(spec, e)).MightMatch([summary]);
        Assert.False(Might(Expr.Equal("n", 4)));
        Assert.True(Might(Expr.Equal("n", 5)));
        Assert.False(Might(Expr.IsNull("n")));
        Assert.True(Might(Expr.NotNull("n")));
        Assert.False(Might(Expr.GreaterThan("n", 9)));
        Assert.True(new ManifestEvaluator(Project(spec, Expr.Equal("n", 4))).MightMatch(null)); // no summaries: no information
    }
}
