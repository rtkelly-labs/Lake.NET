"""Check API contracts against a real SDK-built assembly, including source-breaking metadata changes."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class ApiSurfaceContract(unittest.TestCase):
    def test_exported_shape_and_real_metadata_drift(self):
        env = dict(os.environ, DOTNET_CLI_TELEMETRY_OPTOUT="1")

        def run(args, cwd=ROOT):
            return subprocess.run(
                args,
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )

        def succeeds(args, cwd=ROOT):
            result = run(args, cwd)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return result

        succeeds(["dotnet", "build", "tools/ApiSurface", "--nologo", "-v", "quiet"])
        (ROOT / "temp").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(
            dir=ROOT / "temp", prefix="api-contract-"
        ) as work:
            root = Path(work)
            project = root / "src/Contracts"
            project.mkdir(parents=True)
            (root / "Directory.Build.props").write_text(
                "<Project><PropertyGroup><TargetFramework>net10.0</TargetFramework><Nullable>enable</Nullable></PropertyGroup></Project>"
            )
            (root / "Directory.Packages.props").write_text("<Project />")
            (root / "NuGet.Config").write_text(
                "<configuration><packageSources><clear /></packageSources></configuration>"
            )
            (root / "Iceberg.Net.slnx").write_text(
                '<Solution><Project Path="src/Contracts/Contracts.csproj" /></Solution>'
            )
            (project / "Contracts.csproj").write_text(
                '<Project Sdk="Microsoft.NET.Sdk" />'
            )
            source = """namespace Contracts;
public class Item
{
    public const int Answer = 42;
    public string? Name { get; set; }
    protected virtual T Create<T>() where T : class, new() => new T();
    private void Hidden() { }
}
internal class InternalItem { public void VisibleOnlyInside() { } }
public static class Extensions
{
    private static int _value;
    public static int Size(this string value) => value.Length;
    public static ref int Value() => ref _value;
}
"""
            code = project / "Contract.cs"
            code.write_text(source)
            build = ["dotnet", "build", "src/Contracts", "--nologo", "-v", "quiet"]
            succeeds(build, root)
            command = [
                "dotnet",
                "run",
                "--project",
                str(ROOT / "tools/ApiSurface"),
                "--no-build",
                "--",
            ]
            succeeds(command + ["update", str(root)])
            snapshot = (root / "docs/api/Contracts.net10.0.txt").read_text()
            self.assertIn("this string value", snapshot)
            self.assertIn("ref int", snapshot)
            self.assertIn("string?", snapshot)
            self.assertIn("where T : class, new()", snapshot)
            self.assertIn("Answer = 42", snapshot)
            self.assertNotIn("Hidden", snapshot)
            self.assertNotIn("InternalItem", snapshot)
            succeeds(command + ["check", str(root)])

            # Private implementation changes do not churn a public contract.
            code.write_text(source.replace("Hidden()", "HiddenRenamed()"))
            succeeds(build, root)
            succeeds(command + ["check", str(root)])
            # Removing `this` preserves the CLR signature but breaks extension-call source compatibility.
            code.write_text(source.replace("this string value", "string value"))
            succeeds(build, root)
            changed = run(command + ["check", str(root)])
            self.assertEqual(changed.returncode, 1, changed.stdout + changed.stderr)
            self.assertIn("API surface changed", changed.stderr)


if __name__ == "__main__":
    unittest.main()
