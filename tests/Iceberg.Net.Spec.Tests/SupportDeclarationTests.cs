using System.Text.Json.Nodes;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The per-format support declarations under <c>docs/spec/</c> (docs/lake/goal.md, G1) are data that CI reads, so they
/// are checked like code: every entry has a known status and kind, and a writer obligation saying what a writer must do
/// or refuse. When a format's code lands, its tests link each <c>supported</c> or <c>refused</c> entry.
/// </summary>
public sealed class SupportDeclarationTests
{
    private static readonly string[] Statuses = ["supported", "refused", "planned"];

    private static string RepositoryRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "Iceberg.Net.slnx"))) return dir.FullName;
        throw new InvalidOperationException("Iceberg.Net.slnx not found above " + AppContext.BaseDirectory);
    }

    [Fact]
    public void Delta_table_features_are_each_declared_once_with_a_status_and_an_obligation()
    {
        var doc = JsonNode.Parse(File.ReadAllText(Path.Combine(RepositoryRoot(), "docs", "spec", "delta-support.json")))!;
        var features = doc["features"]!.AsArray().Select(f => f!.AsObject()).ToList();
        Assert.NotEmpty(features);
        Assert.Equal(features.Count, features.Select(f => (string)f["name"]!).Distinct(StringComparer.Ordinal).Count());
        Assert.All(features, f =>
        {
            Assert.Contains((string)f["status"]!, Statuses);
            Assert.Contains((string)f["kind"]!, new[] { "writer", "reader-writer" });
            Assert.False(string.IsNullOrWhiteSpace((string?)f["writerObligation"]), $"{f["name"]} has no writer obligation");
        });
        // Every writer feature Delta defines at protocol writer version 7 that a writer can silently violate is listed.
        foreach (var required in new[] { "appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "identityColumns", "columnMapping", "deletionVectors", "rowTracking" })
            Assert.Contains(required, features.Select(f => (string)f["name"]!));
    }
}
