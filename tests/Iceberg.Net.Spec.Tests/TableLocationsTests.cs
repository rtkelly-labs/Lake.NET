using Iceberg.Net.Catalog;

namespace Iceberg.Net.Spec.Tests;

/// <summary>What counts as under a table's location for the operations that delete files.</summary>
public sealed class TableLocationsTests
{
    [Theory]
    [InlineData("s3://b/wh/t/data/f.parquet", "s3://b/wh/t", true)]
    [InlineData("s3a://b/wh/t/data/f.parquet", "s3://b/wh/t", true)]
    [InlineData("file:///wh/t/data/f.parquet", "/wh/t", true)]
    [InlineData("/wh/t/data/f.parquet", "file:/wh/t", true)]
    [InlineData("s3://b/wh/t2/data/f.parquet", "s3://b/wh/t", false)]
    [InlineData("s3://b/wh/t", "s3://b/wh/t", false)]
    [InlineData("s3://b/wh/t/../other/f.parquet", "s3://b/wh/t", false)]
    [InlineData("/wh/t/./data/f.parquet", "/wh/t", false)]
    [InlineData("s3://other/wh/t/data/f.parquet", "s3://b/wh/t", false)]
    [InlineData("/etc/passwd", "/wh/t", false)]
    public void Files_count_as_under_a_prefix_only_strictly_below_it_without_dot_segments(string file, string location, bool under) =>
        Assert.Equal(under, TableLocations.IsUnder(file, [TableLocations.Normalise(location).TrimEnd('/') + "/"]));

    [Theory]
    [InlineData("s3://bucket")]
    [InlineData("s3://bucket/")]
    [InlineData("gs://bucket//")]
    [InlineData("abfss://c@acct.dfs.core.windows.net/")]
    [InlineData("/")]
    [InlineData("file:///")]
    [InlineData("s3://b/wh/t/..")]
    [InlineData("/wh/./t")]
    public void Roots_and_dot_segments_are_unsafe_to_list_under(string location) =>
        Assert.Throws<ArgumentException>(() => TableLocations.RequireSafeRoot(location));

    [Theory]
    [InlineData("s3://bucket/t")]
    [InlineData("/wh/db.db/t")]
    [InlineData("file:///wh/t")]
    public void Locations_below_a_root_are_safe(string location) => TableLocations.RequireSafeRoot(location);

    [Theory]
    [InlineData(@"C:\Users\r\wh\t\data\f.parquet", "C:/Users/r/wh/t/data/f.parquet")]   // a Windows listing
    [InlineData(@"C:\Users\r\wh\t/data/f.parquet", "C:/Users/r/wh/t/data/f.parquet")]    // metadata: location + "/data/…"
    [InlineData("c:/Users/r/wh/t/data/f.parquet", "C:/Users/r/wh/t/data/f.parquet")]
    [InlineData("file:///C:/Users/r/wh/t/data/f.parquet", "C:/Users/r/wh/t/data/f.parquet")]
    [InlineData("s3://b/k\\x", "s3://b/k\\x")]                                             // object keys are left alone
    public void On_Windows_every_spelling_of_a_local_file_normalises_the_same(string location, string normalised) =>
        Assert.Equal(normalised, TableLocations.Normalise(location, windowsPaths: true));

    [Fact]
    public void Elsewhere_a_backslash_is_part_of_a_file_name()
    {
        Assert.Equal("/wh/t/a\\b.parquet", TableLocations.Normalise("/wh/t/a\\b.parquet", windowsPaths: false));
        Assert.Equal("/wh/t/f.parquet", TableLocations.Normalise("file:///wh/t/f.parquet", windowsPaths: false));
    }

    [Fact]
    public void A_listed_Windows_file_is_under_its_tables_metadata_built_prefix()
    {
        var prefix = TableLocations.Normalise(@"C:\Users\r\wh\t", windowsPaths: true).TrimEnd('/') + "/";
        Assert.StartsWith(prefix, TableLocations.Normalise(@"C:\Users\r\wh\t\data\f.parquet", windowsPaths: true), StringComparison.Ordinal);
    }
}
