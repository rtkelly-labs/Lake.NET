# Rename and release (phases 0–2)

**Status:** target state. Covers what to merge before the rename, every name other people can see and what
happens to it, how we know the rename is done, and the NuGet steps for retiring `Iceberg.Net.*`. The new
names are in [packages.md](packages.md).

## Phase 0: land what is in flight

The rename touches almost every file. Open branches merge first, and nothing else merges while the rename
pull request is open.

| Branch | State on 2026-10-28 |
|---|---|
| `ccr-87fadcf9-bxi9wr` | DuckLake 1.0/1.1, the Parquet backends and the Parquet.Net fork submodule: 50 commits, about 18k lines, no pull request yet. Before merging, repoint `.gitmodules` from the session branch to a stable fork branch |
| `rtkelly13/perf-aot-reflection` | PR #24, open |
| `rtkelly13/typemodel-sourcegenerator` | Worktree with no commits beyond `main` yet |
| `rtkelly13/pickup-handover-tasks`, `rtkelly13/eval-formal-methods-conformance` | Older worktrees: merge or close |

On the DuckLake branch, `Iceberg.Net` appears in about 430 files: 171 in `src`, 201 in `tests`, 25 in `docs`,
10 in `tools`, and the rest in CI, interop, mutation, emulation and root files.

## Phase 1: mechanical rename

This is one pull request on its own branch, `rtkelly13/lake-rename`, and it changes no library behaviour. It also
sets up the version tooling ([releases.md](releases.md#when-to-introduce)), so the first `Lake.*` packages are
released the new way. It renames:

- projects, folders and namespaces;
- the solution file;
- the docs;
- the root changelog, split into one changelog per package, with a version and the current public API recorded in each project ([releases.md](releases.md#when-to-introduce));
- every name in the table below.

The GitHub repository was renamed to `rtkelly-labs/Lake.NET` on 2026-10-28, ahead of this PR. GitHub redirects the
old `rtkelly-labs/Iceberg.NET` URLs, including git remotes, so this PR points every URL at the new name directly.

### Names other people can see

Each of these is visible to a consumer, a CI run or a tool, so each needs a deliberate decision rather than
find-and-replace.

| Name | Where | Phase 1 |
|---|---|---|
| Package IDs | `src/Directory.Build.props` (`PackageId = $(MSBuildProjectName)`) | Follow the project rename |
| `PackageTags` | `src/Directory.Build.props` | Shared tags keep `lakehouse`. Each format adds its own (`iceberg`, `ducklake`, `delta`) |
| `PackageProjectUrl`, `RepositoryUrl` | `src/Directory.Build.props` | `rtkelly-labs/Lake.NET`. GitHub redirects the old URL; metadata in packages already published doesn't change |
| Namespaces | every `.cs` | Follow the project. Public type names (`IcebergType`, `TableScan`) stay the same in phase 1 |
| ActivitySource / Meter `"Iceberg.Net"` | `IcebergTelemetry.Name`, `docs/telemetry.md` | [D3](decisions.md#d3) |
| `InternalsVisibleTo` | csproj files | Follow the project rename |
| `<CiSuite>`, `tools/test-projects.sh` | test csproj files | Project names change; the suites don't |
| `Iceberg.Net.slnx` | root | Becomes `Lake.Net.slnx` |
| Mutation configs | `mutation/stryker-*.json` | Update paths and project names |
| Spec traceability | `tools/spec_trace.py`, `docs/spec/*.json` | Update paths |
| Release | `.github/workflows/release.yml`, `scripts/ParseChangelog.cs`, the smoke test | Update the package list and the IDs the smoke test checks |
| `PACKAGE_README.md`, `README.md`, `CHANGELOG.md` | root | Rewrite for Lake.NET; the changelog gets a "Renamed from Iceberg.Net" entry |
| `AGENTS.md` | root | Change "a C# port of Apache Iceberg" to describe the [vision](../../VISION.md); the rules stay the same |
| Environment variables (`ICEBERG_*`, about 50) | tests, `emulation/`, CI | [D5](decisions.md#d5) |

### Done when

- `dotnet build` passes with warnings as errors.
- Every CI suite (`unit`, `cross-engine`, `regression`) passes.
- `dotnet pack` produces exactly the expected list of `Lake.*` packages.
- A diff of the public API shows only namespace changes.
- `git grep Iceberg.Net` matches only the changelog, these pages and history.

## Phase 2: publish and retire the old packages

1. Release the `Lake.*` packages (version line: [D6](decisions.md#d6)), each with its own changelog and tag ([releases.md](releases.md)).
2. Reserve the `Lake.` prefix on nuget.org. A prefix can only be reserved once a package under it has been
   published, so this has to come after step 1.
3. Deprecate each `Iceberg.Net.*` `0.0.1-alpha.1` as **Legacy**, naming its `Lake.*` successor as the
   alternative.
4. Unlist them. NuGet can't delete packages, and an unlisted version still restores when it is pinned exactly.
