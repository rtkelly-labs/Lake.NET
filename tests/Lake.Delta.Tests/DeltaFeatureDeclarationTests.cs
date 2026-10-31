using System.Text.Json.Nodes;

namespace Lake.Delta.Tests;

/// <summary>
/// docs/spec/delta-support.json's <c>reader</c> field says what the reader does with each feature; this keeps it equal to
/// what <see cref="DeltaFeatures"/> actually accepts, so the declaration cannot drift from the code.
/// </summary>
public sealed class DeltaFeatureDeclarationTests
{
    private static List<JsonObject> Declared() =>
        JsonNode.Parse(File.ReadAllText(Path.Combine(Fixtures.RepositoryRoot, "docs", "spec", "delta-support.json")))!["features"]!
            .AsArray().Select(f => f!.AsObject()).ToList();

    [Fact]
    public void The_declared_reader_support_is_what_the_reader_accepts()
    {
        foreach (var feature in Declared())
        {
            var name = (string)feature["name"]!;
            var reader = (string)feature["reader"]!;
            if ((string)feature["kind"]! == "writer")
            {
                Assert.Equal("not-applicable", reader);
                Assert.DoesNotContain(name, DeltaFeatures.ReaderWriterFeatures);
                continue;
            }
            Assert.Contains(name, DeltaFeatures.ReaderWriterFeatures);
            Assert.True(reader == (DeltaFeatures.SupportedReaderFeatures.Contains(name) ? "supported" : "refused"), $"{name}: declared {reader}");
            Assert.False(string.IsNullOrWhiteSpace((string?)feature["readerEvidence"]), $"{name} has no reader evidence");
        }
    }

    [Fact]
    public void Every_reader_feature_the_reader_knows_is_declared()
    {
        var declared = Declared().Where(f => (string)f["kind"]! == "reader-writer").Select(f => (string)f["name"]!).ToHashSet();
        Assert.Empty(DeltaFeatures.ReaderWriterFeatures.Except(declared));
        Assert.Empty(DeltaFeatures.SupportedReaderFeatures.Except(declared));
    }

    [Fact]
    public async Task Each_refused_reader_feature_is_refused_by_name()
    {
        foreach (var name in Declared().Where(f => (string)f["reader"]! == "refused").Select(f => (string)f["name"]!))
        {
            var table = Fixtures.CopyToTemp(Fixtures.Dat("basic_append"));
            Fixtures.EditCommit(table, 0, line => line["protocol"] is null ? line : JsonNode.Parse(
                """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["@@"],"writerFeatures":["@@"]}}""".Replace("@@", name, StringComparison.Ordinal))!.AsObject());
            var e = await Assert.ThrowsAsync<DeltaUnsupportedFeatureException>(() => DeltaTable.OpenLocal(table).GetSnapshotAsync());
            Assert.Equal([name], e.Features);
        }
    }
}
