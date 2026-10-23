using System.Text.RegularExpressions;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Iceberg.Net.TestSupport;

/// <summary>
/// Links a test (or every test in a class) to the specification section it verifies, for the traceability matrix
/// (tools/spec_trace.py → docs/spec-traceability.md) and the scheduled spec-drift check. <paramref name="section"/>
/// is a heading of the upstream document, e.g. <c>[Spec("Row Lineage")]</c>, <c>[Spec("puffin: deletion-vector-v1 blob type")]</c>
/// or <c>[Spec("rest: commitTable")]</c> (document prefixes: table spec by default, <c>puffin:</c>, <c>rest:</c> for
/// OpenAPI operation ids). Also an xunit trait: <c>dotnet test --filter Spec=row-lineage</c>.
/// </summary>
[TraitDiscoverer("Iceberg.Net.TestSupport.SpecTraitDiscoverer", "Iceberg.Net.TestSupport")]
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = true)]
public sealed class SpecAttribute(string section) : Attribute, ITraitAttribute
{
    public string Section { get; } = section;

    /// <summary>Same normalisation as tools/spec_trace.py: "puffin: `deletion-vector-v1` blob type" → "puffin:deletion-vector-v1-blob-type".</summary>
    public static string Slug(string section)
    {
        var (doc, heading) = section.IndexOf(':') is var i and > 0 && section[..i] is "spec" or "puffin" or "rest"
            ? (section[..i], section[(i + 1)..])
            : ("spec", section);
        var slug = Regex.Replace(heading.ToLowerInvariant(), "[^a-z0-9_]+", "-").Trim('-');
        return doc == "spec" ? slug : $"{doc}:{slug}";
    }
}

public sealed class SpecTraitDiscoverer : ITraitDiscoverer
{
    public IEnumerable<KeyValuePair<string, string>> GetTraits(IAttributeInfo traitAttribute)
    {
        yield return new("Spec", SpecAttribute.Slug(traitAttribute.GetConstructorArguments().Single()!.ToString()!));
    }
}
