# Lake.NET: target state

**Status:** this is the plan, and none of it is done yet. This folder explains how Iceberg.NET becomes Lake.NET,
a library for Iceberg, DuckLake and Delta Lake built on shared layers. Read [VISION.md](../../VISION.md) first
for why. The working checklist is the gitignored root `TODO.md`; these pages are the committed copy of the plan.

**Baseline.** "Today" means `main` plus the DuckLake branch (`ccr-87fadcf9-bxi9wr`, not merged yet). That branch
adds `Iceberg.Net.DuckLake`, puts Parquet behind `IParquetBackend` (`ParquetSharpBackend`, and
`Iceberg.Net.Data.ParquetNet` over the `rtkelly-labs/parquet-dotnet` fork), and documents them in
`docs/ducklake.md` and `docs/parquet-net-gaps.md`.

## Principles

1. **Rename first, restructure second.** The rename is one mechanical pull request with no change in
   behaviour. Moving code into shared layers comes afterwards, one pull request at a time.
2. **Neutral names only for neutral code.** A project gets a format-free name only if it has no Iceberg
   dependency today. Everything else is named `Lake.Iceberg.*` until its shared part is extracted.
3. **Extract when a second format needs it.** DuckLake is already a second user of the Parquet data plane and
   Puffin. Roaring64 without Puffin waits for Delta.
4. **Dependencies point one way.** Shared layers never reference a format, and formats never reference each
   other. A test enforces both.

## Phases

| Phase | What | Detail |
|---|---|---|
| 0 | Merge the work in flight (DuckLake branch, PR #24, open worktrees); then freeze | [rename.md](rename.md#phase-0-land-what-is-in-flight) |
| 1 | Mechanical rename to `Lake.*`, plus the GitHub repository | [rename.md](rename.md#phase-1-mechanical-rename) |
| 2 | Publish `Lake.*`; deprecate and unlist `Iceberg.Net.*` | [rename.md](rename.md#phase-2-publish-and-retire-the-old-packages) |
| 3 | Extract `Lake.Core` and `Lake.Data`; DuckLake drops its Iceberg dependency | [shared-layers.md](shared-layers.md#phase-3-extraction) |
| 4 | Delta Lake: read, then write, then maintenance | [delta.md](delta.md) |
| 5 | Optional cross-format work: a shared typed layer, Arrow for all formats | [shared-layers.md](shared-layers.md#typed-access) |

## Pages

| Page | Read it for |
|---|---|
| [packages.md](packages.md) | Every project's name in each phase, who publishes which package, the target dependency graph |
| [shared-layers.md](shared-layers.md) | What the three formats share and where it lives, the extraction steps, typed access |
| [rename.md](rename.md) | Phases 0–2: what to merge first, every externally visible name and what happens to it, the NuGet sequence |
| [delta.md](delta.md) | Phase 4: the Delta reader, writer and maintenance, and how they are verified |
| [releases.md](releases.md) | A version per package: changelogs, public API files, working out which packages need a release, per-package tags |
| [verification/](verification/README.md) | How every format is verified: spec alignment, BDD, regression, integration and code coverage, chaos, benchmarks |
| [decisions.md](decisions.md) | Decisions D1–D12, each with options, a recommendation and an outcome (D8 ORC, D9 licence, D10 the Parquet.Net fork and D11 versioning are decided) |
