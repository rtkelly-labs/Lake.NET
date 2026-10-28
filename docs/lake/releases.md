# Releases: one version per package

**Status:** target state ([D11](decisions.md#d11)). Covers how each `Lake.*` package gets its own version,
changelog and public API record, and how CI works out which packages need a release. Today every package shares
one version and the root `CHANGELOG.md`, and `release.yml` packs the whole solution with one `-p:Version`.
The package names are in [packages.md](packages.md).

## Why

There will be about fifteen packages across three formats. Under one shared version, a fix to `Lake.DuckLake`
re-releases `Lake.Iceberg.RestServer` with no changes, and the version number stops telling consumers what
changed. With a version per package, each version describes that package. Recording the API surface in files
makes "does this need a release, and how big a bump?" something a machine can answer.

## Three files per package

Each packable project under `src/` keeps three files next to its `.csproj`:

| File | Holds | Enforced by |
|---|---|---|
| `CHANGELOG.md` | Keep a Changelog: `[Unreleased]`, then cut sections `## [x.y.z] - yyyy-mm-dd` | `scripts/ParseChangelog.cs` (already takes `--changelog <path>`) |
| `PublicAPI.Shipped.txt` | The public API as of the package's last release | `Microsoft.CodeAnalysis.PublicApiAnalyzers`, as in Parquet.SourceGenerator |
| `PublicAPI.Unshipped.txt` | Public API added or removed since that release | RS0016 (undeclared public API) and RS0017 (removed API not declared). Warnings are errors here, so an unrecorded change fails the build |

The version itself is in the project (`<VersionPrefix>` in the `.csproj`, or a `version.props` next to it), so MSBuild
knows it. A project reference then becomes a package dependency on the version it was built against, with no
extra tooling. CI checks that the version matches the changelog's latest cut section.

The root `CHANGELOG.md` keeps only repository-level changes: tooling, CI, docs and licence. It also has a table
linking each package's changelog and current version.

## Tools, not hand-rolled scripts

Versions across about fifteen packages, plus the Parquet.Net fork, Parquet.SourceGenerator, Parquet.TypeProvider
and the pinned engines, are too much to manage by hand or with home-grown scripts alone. Four layers handle it:

| Layer | Does | Tool |
|---|---|---|
| 1. Version families | Packages released together share one version line, about 6 instead of 15 ([D12](decisions.md#d12)) | release-please `linked-versions` |
| 2. Versions and changelogs | Each squash-merged PR title (`feat(ducklake): …`, `fix!: …`) becomes a changelog line for the packages whose files it touched. A standing release PR per family holds the bump and the changelog, and merging it releases them | **release-please** in manifest (monorepo) mode, with conventional PR titles enforced as Parquet.SourceGenerator already does (`pr-title.yml`) |
| 3. Dependencies across repositories (this repository: `.github/renovate.json5`) | Update PRs when Parquet.Net upstream releases (to the fork), when the fork moves (the submodule pin here), when `Lake.*` releases (to Parquet.SourceGenerator), and for engine and image pins | **Renovate**, one shared preset for every `rtkelly-labs` repository; it handles git submodules and custom version files such as `interop/engines.json` |
| 4. One view of the ecosystem | Every package's version and unreleased changes, pending release PRs, the fork commit each package was built with, dependencies that are behind | A scheduled job that publishes a dashboard |

What release-please can't know is how big a change really is. That is what the public API files and
`ReleasePlan.cs` are for (below).

## Deciding when a release is needed

`scripts/ReleasePlan.cs` (C#, as the script policy requires) runs on every PR. It checks the bump declared in the PR title against the API files, and works out which dependents a breaking change cascades to; release-please does the rest. For each packable project it compares `HEAD` with the package's last tag:

| Signal | Means |
|---|---|
| `PublicAPI.Unshipped.txt` has `*REMOVED*` entries, or Shipped lines were removed | Breaking change |
| `PublicAPI.Unshipped.txt` has new entries | Additive change |
| Files under the project changed, but the API didn't | Fix or internal change |
| A dependency is released with a breaking change | Dependent must be rebuilt and released, even with no changes of its own |
| `[Unreleased]` in its changelog has entries | The author has described the change |

From these it works out the bump, using SemVer with the pre-1.0 convention:

| Change | Before 1.0 | From 1.0 |
|---|---|---|
| Breaking | minor | major |
| Additive | patch | minor |
| Fix | patch | patch |

It fails the PR on two mismatches:
- **The API files show a breaking change, but the PR title isn't marked breaking** (`!` or `BREAKING CHANGE`).
- **A package's files changed, but the PR title's type wouldn't release it** (for example `chore:` on library code).

On `main` it writes a release plan as an artifact: the packages that need a release, the suggested version for
each, and why.

## Cutting and publishing

1. **Cut.** release-please's release PR for a family writes the changelog sections and versions. A step on that PR
   moves `PublicAPI.Unshipped.txt` into `PublicAPI.Shipped.txt`.
2. **Release.** `release.yml` reads the plan, then builds, packs and smoke-tests only the packages whose
   changelog has a cut section without a tag. Each gets a tag of the form `<PackageId>/v<version>`, for example
   `Lake.Iceberg.Catalog/v0.2.0`, and a GitHub release whose notes are that section. The existing safeguards
   carry over: release only from `main`, refuse to re-release an existing tag, and require a green CI on the
   commit.
3. **Order.** Dependencies publish before the packages that depend on them, in one run, so a dependent never
   references a version that isn't on nuget.org yet.

## API compatibility alongside the text files

The text files show the API surface but can't catch every binary break. Each packable project also sets
`EnablePackageValidation` with `PackageValidationBaselineVersion` set to its own last released version. Pack then
fails on a breaking change unless it is suppressed with a reason. Before 1.0, the release plan accepts that
suppression only together with a minor bump.

## When to introduce

Layers 1 and 2 land on the rename branch (`rtkelly13/lake-rename`), in the same pull request as the rename.
Layers 3 and 4 follow once the packages are on NuGet.

- **During the [phase 1 rename](rename.md#phase-1-mechanical-rename):** split the root changelog, and add a
  changelog and `<VersionPrefix>` to each project. Add PublicApiAnalyzers with the whole current surface in
  `PublicAPI.Unshipped.txt`. Adding them before the rename would record every namespace twice.
- **At the [first `Lake.*` release](rename.md#phase-2-publish-and-retire-the-old-packages):** every package ships at
  `0.0.1-alpha.1` ([D6](decisions.md#d6)), and its Unshipped file moves to Shipped. From then on the release plan
  decides.
- **Later, if it proves useful:** an API ledger like Parquet.SourceGenerator's `docs/api/LEDGER.md`, mapping each
  API change to its PR.

## Done when

- Every packable project has a `CHANGELOG.md`, `PublicAPI.*.txt` files and its own version.
- RS0016 and RS0017 fail the build on unrecorded API changes.
- release-please manages versions and changelogs for each family, and `ReleasePlan.cs` checks every PR's declared bump.
- `release.yml` publishes per package, in dependency order, with per-package tags.
- Renovate runs across every `rtkelly-labs` repository, and the ecosystem dashboard is published on a schedule.
- Package validation runs against each package's own previous release.
