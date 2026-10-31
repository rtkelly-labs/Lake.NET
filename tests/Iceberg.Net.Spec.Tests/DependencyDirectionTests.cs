using System.Xml.Linq;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The project graph under <c>src/</c> points one way (VISION.md, "How we know it's working"): every reference goes to a
/// lower layer, shared projects reference only shared projects, and one format never references another. A new project
/// fails this test until it is given a layer and a role below, which is the moment to decide where it belongs.
/// </summary>
public sealed class DependencyDirectionTests
{
    private const string Shared = "shared";

    /// <summary>
    /// Every library and where it sits. Role is <c>shared</c> (no format's model in it; DuckLake and Delta may use it) or
    /// the format it belongs to. IO and Puffin are already format-neutral; Core, Avro and Data hold Iceberg's model until
    /// the phase 3 extraction moves their shared parts into Lake.Core and Lake.Data (docs/lake/shared-layers.md).
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, (int Layer, string Role)> Projects = new Dictionary<string, (int, string)>
    {
        ["Iceberg.Net.IO"] = (0, Shared),
        ["Iceberg.Net.Puffin"] = (0, Shared),
        ["Iceberg.Net.Core"] = (1, "iceberg"),
        ["Iceberg.Net.Avro"] = (2, "iceberg"),
        ["Iceberg.Net.Athena"] = (2, "iceberg"),
        ["Iceberg.Net.Catalog"] = (3, "iceberg"),
        ["Iceberg.Net.Spark"] = (4, "iceberg"),
        ["Iceberg.Net.Data"] = (4, "iceberg"),
        ["Iceberg.Net.Arrow"] = (5, "iceberg"),
        ["Iceberg.Net.Linq"] = (5, "iceberg"),
        ["Iceberg.Net.RestServer"] = (5, "iceberg"),
        ["Iceberg.Net.EntityFrameworkCore"] = (6, "iceberg"),
    };

    /// <summary>
    /// Edges that break a rule on purpose, each with the plan that removes it. Empty on main; the DuckLake branch needs
    /// <c>DuckLake → Iceberg.Net.Data</c> here until phase 3.
    /// </summary>
    private static readonly HashSet<(string From, string To)> Exceptions = [];

    private static string RepositoryRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "Iceberg.Net.slnx"))) return dir.FullName;
        throw new InvalidOperationException("Iceberg.Net.slnx not found above " + AppContext.BaseDirectory);
    }

    internal static Dictionary<string, List<string>> ReadGraph(string root) =>
        Directory.EnumerateFiles(Path.Combine(root, "src"), "*.csproj", SearchOption.AllDirectories)
            .ToDictionary(
                p => Path.GetFileNameWithoutExtension(p),
                p => XDocument.Load(p).Descendants("ProjectReference")
                    .Select(r => Path.GetFileNameWithoutExtension(r.Attribute("Include")!.Value.Replace('\\', '/')))
                    .ToList());

    [Fact]
    public void Every_library_has_a_declared_layer_and_role()
    {
        var graph = ReadGraph(RepositoryRoot());
        Assert.Empty(graph.Keys.Except(Projects.Keys).Order());
        Assert.Empty(Projects.Keys.Except(graph.Keys).Order()); // a stale entry hides a renamed project
    }

    [Fact]
    public void References_point_down_stay_shared_and_never_cross_formats()
    {
        var violations = new List<string>();
        foreach (var (from, references) in ReadGraph(RepositoryRoot()))
        {
            if (!Projects.TryGetValue(from, out var source)) continue; // reported by the test above
            foreach (var to in references)
            {
                if (Exceptions.Contains((from, to))) continue;
                if (!Projects.TryGetValue(to, out var target))
                {
                    violations.Add($"{from} → {to}: not a library under src/ (tests and tools are never referenced)");
                    continue;
                }
                if (target.Layer >= source.Layer)
                    violations.Add($"{from} (layer {source.Layer}) → {to} (layer {target.Layer}): references must point to a lower layer");
                if (source.Role == Shared && target.Role != Shared)
                    violations.Add($"{from} → {to}: a shared project references the {target.Role} format");
                if (source.Role != Shared && target.Role != Shared && source.Role != target.Role)
                    violations.Add($"{from} → {to}: the {source.Role} format references the {target.Role} format");
            }
        }
        Assert.True(violations.Count == 0, string.Join("\n", violations));
    }

    [Fact]
    public void The_format_neutral_test_harness_references_no_format()
    {
        // Lake.TestSupport holds IFormatDriver and the common scenarios; a driver lives with its format's tests.
        var csproj = Path.Combine(RepositoryRoot(), "tests", "Lake.TestSupport", "Lake.TestSupport.csproj");
        Assert.True(File.Exists(csproj), csproj);
        Assert.Empty(XDocument.Load(csproj).Descendants("ProjectReference"));
    }

    [Fact]
    public void The_rules_catch_what_they_are_meant_to()
    {
        // Negative controls, so the test above cannot pass by checking nothing.
        var graph = ReadGraph(RepositoryRoot());
        Assert.Contains("Iceberg.Net.IO", graph["Iceberg.Net.Catalog"]);
        Assert.True(Projects["Iceberg.Net.Catalog"].Layer > Projects["Iceberg.Net.IO"].Layer);
        Assert.Equal(Shared, Projects["Iceberg.Net.IO"].Role);
        Assert.NotEqual(Shared, Projects["Iceberg.Net.Core"].Role);
    }
}
