# Public API snapshots

The snapshots capture the exported type/member shape for the solution's source projects and their declared
target frameworks (currently 12 projects / 14 files). They are generated from compiled assemblies using the Roslyn compiler shipped with the
SDK pinned in `global.json`. Parameter names/defaults, extension-method markers, ref returns, generic constraints, accessibility, modifiers and
nullable reference types are included. A real compiled fixture checks these details, ignores private-only changes,
and verifies that removing `this` is rejected even though the underlying CLR signature is unchanged. This is an API review gate, not proof of behavioral or wire-format compatibility.

```bash
dotnet build Iceberg.Net.slnx
dotnet run --project tools/ApiSurface -- check "$PWD"
# After reviewing an intentional API change:
dotnet run --project tools/ApiSurface -- update "$PWD"
```

Commit the regenerated files with intentional API changes. Review signatures and the affected capability scenarios;
do not accept a snapshot merely to silence CI. The initial baseline is pre-0.0.1 and does not promise that the API
is stable. No deletion, signature change, behavior change or package-boundary change should be called compatible
without review. XML comments and these snapshots complement behavioral, protocol and cross-engine tests.
