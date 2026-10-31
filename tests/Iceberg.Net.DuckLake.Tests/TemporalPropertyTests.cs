using FsCheck;
using FsCheck.Fluent;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>Every <c>interval</c> and <c>timetz</c> value survives its text (statistics, inlining) and Parquet forms.</summary>
public sealed class TemporalPropertyTests
{
    private static readonly Config Config = Config.QuickThrowOnFailure.WithMaxTest(
        int.TryParse(Environment.GetEnvironmentVariable("ICEBERG_PBT_RUNS"), out var n) ? n * 10 : 2000);

    private static readonly Gen<DuckLakeInterval> Intervals =
        from months in Gen.OneOf(Gen.Choose(-1000, 1000), Gen.Elements(int.MinValue + 11, int.MaxValue, 0))
        from days in Gen.OneOf(Gen.Choose(-1000, 1000), Gen.Elements(int.MinValue, int.MaxValue, 0))
        from micros in Gen.OneOf(
            Gen.Choose(-1_000_000, 1_000_000).Select(x => (long)x),
            Gen.Choose(0, int.MaxValue).Select(x => (long)x * 997),
            Gen.Elements(long.MaxValue, long.MinValue + 1, 0L, 86_400_000_000L))
        select new DuckLakeInterval(months, days, micros);

    private static readonly Gen<DuckLakeTimeTz> Times =
        from micros in Gen.OneOf(Gen.Choose(0, int.MaxValue).Select(x => x * 40L), Gen.Elements(0L, DuckLakeTimeTz.MicrosPerDay, 1L))
        from offset in Gen.OneOf(Gen.Choose(-DuckLakeTimeTz.MaxOffsetSeconds, DuckLakeTimeTz.MaxOffsetSeconds), Gen.Elements(0, 3600, -1800))
        select new DuckLakeTimeTz(micros, offset);

    [Fact]
    public void Interval_text_round_trips() =>
        Check.One(Config, Prop.ForAll(Arb.From(Intervals), (DuckLakeInterval i) => DuckLakeInterval.Parse(i.ToString()) == i));

    [Fact]
    public void Timetz_text_round_trips_and_its_instant_is_within_a_day() =>
        Check.One(Config, Prop.ForAll(Arb.From(Times), (DuckLakeTimeTz t) =>
            DuckLakeTimeTz.Parse(t.ToString()) == t && t.UtcMicros is >= 0 and < DuckLakeTimeTz.MicrosPerDay
            && (t.UtcMicros - (t.Micros - t.OffsetSeconds * 1_000_000L)) % DuckLakeTimeTz.MicrosPerDay == 0));

    [Fact]
    public void Non_negative_intervals_keep_all_but_sub_millisecond_digits_in_Parquet() =>
        Check.One(Config, Prop.ForAll(Arb.From(Intervals.Where(i => i.Months >= 0 && i.Days >= 0 && i.Micros >= 0 && i.Micros / 1000 <= uint.MaxValue)),
            (DuckLakeInterval i) => DuckLakeInterval.FromParquet(i.ToParquet()) == i with { Micros = i.Micros / 1000 * 1000 }));

    [Fact]
    public void Nested_text_round_trips_intervals_timetz_and_unsigned_values()
    {
        var type = DuckLakeType.Struct(("i", DuckLakeType.List(DuckLakeType.Interval)), ("t", DuckLakeType.TimeTz), ("u", DuckLakeType.UInt64));
        var values = from i in Gen.ListOf(Intervals)
                     from t in Times
                     from u in Gen.Choose(0, int.MaxValue).Select(x => (ulong)x * 8_589_934_597UL)
                     select new object?[] { i.Select(x => (object?)x).ToList(), t, new Iceberg.Net.Types.IcebergDecimal(u, 0) };
        Check.One(Config, Prop.ForAll(Arb.From(values), (object?[] v) =>
        {
            var back = (object?[])DuckLakeNestedText.Parse(type, DuckLakeNestedText.Format(type, v))!;
            return ((List<object?>)back[0]!).SequenceEqual((List<object?>)v[0]!) && Equals(back[1], v[1]) && Equals(back[2], v[2]);
        }));
    }
}
