namespace Iceberg.Net.TestSupport;

/// <summary>The linked tests' <c>[Spec]</c> tags; traceability is reported from the main spec suite, so a plain attribute here.</summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = true)]
public sealed class SpecAttribute(string section) : Attribute
{
    public string Section { get; } = section;
}
