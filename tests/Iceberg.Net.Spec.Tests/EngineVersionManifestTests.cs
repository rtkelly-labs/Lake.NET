using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// <c>interop/engines.json</c> is the one list of reference-engine and emulator versions (docs/lake/goal.md W5). Every
/// file that pins one must carry the manifest's version, and no file may name a pinned image with another tag, so the
/// suites can't quietly run against two versions of one engine.
/// </summary>
public sealed partial class EngineVersionManifestTests
{
    private static string Root()
    {
        for (var d = new DirectoryInfo(AppContext.BaseDirectory); d is not null; d = d.Parent)
            if (File.Exists(Path.Combine(d.FullName, "Iceberg.Net.slnx"))) return d.FullName;
        throw new InvalidOperationException("Iceberg.Net.slnx not found");
    }

    private static JsonArray Engines() =>
        JsonNode.Parse(File.ReadAllText(Path.Combine(Root(), "interop", "engines.json")))!["engines"]!.AsArray();

    [Fact]
    public void Every_pin_carries_the_manifest_version()
    {
        var root = Root();
        var problems = new List<string>();
        foreach (var engine in Engines())
        {
            var version = engine!["version"]!.GetValue<string>();
            foreach (var pin in engine["pins"]!.AsArray())
            {
                var file = pin!["file"]!.GetValue<string>();
                var text = pin["text"]!.GetValue<string>().Replace("{version}", version, StringComparison.Ordinal);
                var path = Path.Combine(root, file);
                if (!File.Exists(path)) problems.Add($"{engine["name"]}: {file} does not exist");
                else if (!File.ReadAllText(path).Contains(text, StringComparison.Ordinal)) problems.Add($"{engine["name"]} {version}: {file} lacks `{text}`");
            }
        }
        Assert.True(problems.Count == 0, string.Join("\n", problems));
    }

    [Fact]
    public void No_file_names_a_pinned_image_with_another_tag()
    {
        var root = Root();
        var images = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var engine in Engines())
            foreach (var pin in engine!["pins"]!.AsArray())
                if (ImageRef().Match(pin!["text"]!.GetValue<string>()) is { Success: true } m)
                    images[m.Groups["image"].Value] = engine["version"]!.GetValue<string>();
        Assert.NotEmpty(images);

        var problems = new List<string>();
        var files = new[] { "emulation", "tests", "tools", "interop", ".github" }
            .Select(d => Path.Combine(root, d)).Where(Directory.Exists)
            .SelectMany(d => Directory.EnumerateFiles(d, "*", SearchOption.AllDirectories))
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                && !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                && !f.Contains($"{Path.DirectorySeparatorChar}fixtures{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                && Path.GetExtension(f) is ".cs" or ".yml" or ".yaml" or ".sh" or ".py" or ".toml");
        foreach (var file in files)
        {
            var text = File.ReadAllText(file);
            foreach (var (image, version) in images)
                foreach (Match m in Regex.Matches(text, Regex.Escape(image) + @":(?<tag>v?[0-9][0-9A-Za-z.\-]*)"))
                    if (m.Groups["tag"].Value != version)
                        problems.Add($"{Path.GetRelativePath(root, file)}: {image}:{m.Groups["tag"].Value} (manifest: {version})");
        }
        Assert.True(problems.Count == 0, string.Join("\n", problems));
    }

    [GeneratedRegex(@"(?<image>[a-z0-9.\-]+(/[a-z0-9.\-]+)+):(\$\{[A-Z_]+:-)?\{version\}")]
    private static partial Regex ImageRef();
}
