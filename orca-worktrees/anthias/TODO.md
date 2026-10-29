# TODO — Lake.NET redesign

Local checklist (gitignored). The committed plan is VISION.md and docs/lake/; keep this in step with it.

## Now: vision and docs
- [x] VISION.md (level 0 target state)
- [x] docs/lake/ set: README, packages, shared-layers, rename, delta, decisions
- [x] docs/standards.md (doc expectations, progressive disclosure)
- [x] Link from AGENTS.md, docs/README.md, docs/roadmap.md
- [ ] Review and commit on rtkelly13/refactor-rename-library; open PR
- [ ] Decide D1–D7 (docs/lake/decisions.md); record outcomes + dates
- [ ] Decide D12 version families (before the rename branch sets up release-please)
- [x] D8 ORC: port Apache ORC Java as Lake.Orc
- [x] D10 Parquet.Net fork: two tiers (fork = behaviour, upstream identity; this repo's submodule build = rewrite)
- [ ] Fork tier 1: confirm the fork carries behaviour only (no Lake identity/renaming in its source)
- [x] D11 versioning: a version, CHANGELOG.md and PublicAPI files per package (docs/lake/releases.md)
- [x] D9 licence: Apache-2.0 (LICENSE, NOTICE packed, PackageLicenseExpression, AI_POLICY, PACKAGE_README, CHANGELOG)
- [ ] Audit code ported from Iceberg Java; keep NOTICE accurate
- [ ] Bring existing docs in line with docs/standards.md (summary paragraph + status on every page; split rest-server.md at ~495 lines)

## Verification (docs/lake/verification/) — runs alongside every phase
- [ ] Add `schedule:` triggers: nightly (engines, soak, chaos, benchmarks), weekly (fuzz, mutation, upkeep, drift)
- [ ] Spec: multi-spec spec_trace (prefixed tags), per-format support.json, DuckLake drift job, Parquet + Delta section maps
- [ ] BDD: Lake.Behaviour.Shared step library + IFormatDriver; Common/ features run per format; @pathway/@chaos/@slow tags; LivingDoc artifact
- [ ] Regression: DuckLake DuckDB 1.5/2.0 fixtures committed; interop/engines.json version pins; format-neutral ShadowCompare
- [ ] API compat: EnablePackageValidation + PublicAPI files (after rename)
- [ ] Coverage: coverlet in every test project, ReportGenerator merge, PR diff coverage (80% changed lines), per-package ratchet
- [ ] Pathways: pathways.json, [Pathway] attribute, tools/pathway_trace.py reading TRX; tag existing parameterised suites
- [ ] Mutation: extend to commit protocols, delete applicability, DuckLake commits; ratchet weekly
- [ ] Chaos: FaultInjectingFileIO → Lake.TestSupport; DuckLake DbCommand/DbTransaction interceptors; Toxiproxy in emulation; tools/ChaosWriter kill -9 harness; TimeProvider through library
- [ ] Benchmarks: Lake.Benchmarks.Micro (BenchmarkDotNet, MemoryDiagnoser), Lake.Benchmarks.Scenarios replacing DuckLakeBench/ParquetBackendBench; allocation + request-count PR gates; nightly wall-clock trend; resource-envelope.md
- [ ] CI timing: per-suite duration budget from TRX

## Phase 0: land in-flight work, then freeze
- [ ] DuckLake branch ccr-87fadcf9-bxi9wr: open PR, review, CI green
  - [ ] Repoint .gitmodules off the session branch to a stable fork branch
  - [ ] Merge
- [ ] PR #24 rtkelly13/perf-aot-reflection: merge or park
- [ ] rtkelly13/typemodel-sourcegenerator (humpback): land or rebase after rename
- [ ] rtkelly13/pickup-handover-tasks, rtkelly13/eval-formal-methods-conformance: merge or close
- [ ] Announce freeze for the rename PR

## Phase 1: mechanical rename + version tooling (branch rtkelly13/lake-rename, created by plan unit N1; execution plan: temp/lake-plan/)
- [ ] Rename projects + folders per docs/lake/packages.md (IO → Lake.IO, Puffin → Lake.Puffin, rest → Lake.Iceberg.*, DuckLake → Lake.DuckLake, ParquetNet → Lake.Data.ParquetNet)
- [ ] Rename test projects (Lake.Iceberg.*.Tests, Lake.DuckLake.*, Lake.TestSupport, Lake.Fuzz)
- [ ] Namespaces in every .cs (public type names unchanged)
- [ ] Iceberg.Net.slnx → Lake.Net.slnx; tools/test-projects.sh; <CiSuite> unchanged
- [ ] InternalsVisibleTo
- [ ] src/Directory.Build.props: PackageProjectUrl/RepositoryUrl, PackageTags per format
- [ ] Telemetry source names (D3) + docs/telemetry.md
- [ ] .github/workflows/* (ci, release, fuzz, mutation, aws, upkeep)
- [ ] scripts/ParseChangelog.cs, release smoke test package IDs
- [ ] mutation/stryker-*.json paths
- [ ] tools/spec_trace.py, tools/fuzz.sh, tools/ShadowCompare paths
- [ ] interop/, emulation/, run-tests.sh references
- [ ] README.md, PACKAGE_README.md, CHANGELOG.md ("Renamed from Iceberg.Net"), AGENTS.md reframe
- [ ] All docs/ references
- [ ] release-please manifest mode: one component per package, linked-versions per family (D12), extra-files for <VersionPrefix>
- [ ] Conventional PR titles enforced (port pr-title.yml from Parquet.SourceGenerator)
- [ ] Per-package releases (D11): split root CHANGELOG.md into src/*/CHANGELOG.md; root keeps repo-level changes + package table
- [ ] <VersionPrefix> per project (or version.props); CI check version == changelog cut
- [ ] PublicApiAnalyzers on every packable project; current surface into PublicAPI.Unshipped.txt (after the rename, so namespaces are final)
- [ ] scripts/ReleasePlan.cs: PR title's declared bump vs PublicAPI files; breaking-change cascade to dependents
- [ ] ParseChangelog.cs: validate every package changelog
- [ ] Verify: build (warnings as errors), unit + cross-engine + regression suites, pack list, public API diff, `git grep Iceberg.Net` clean
- [x] Rename GitHub repo rtkelly-labs/Iceberg.NET → rtkelly-labs/Lake.NET (2026-10-28, `gh repo rename`; redirects keep old URLs working)
- [ ] Update local remotes/worktrees (rig estate rename-repo, plan unit R4x)

## Phase 2: publish and retire
- [ ] release.yml per package: plan-driven, dependency order, tags <PackageId>/v<version>, per-package GitHub release notes
- [ ] Release Lake.* 0.0.1-alpha.1 (D6); move PublicAPI Unshipped → Shipped
- [ ] EnablePackageValidation with per-package baseline from the next release on
- [x] Renovate config for this repo (.github/renovate.json5, docs/dependencies.md); validated and dry-run locally 2026-10-28
- [x] Renovate: hosted Mend app chosen for the rtkelly-labs estate (2026-10-28)
- [ ] Install the Mend Renovate GitHub App on the rtkelly-labs org (GitHub UI; all repos or selected)
- [ ] Confirm `ci` is a required check so Renovate's auto-merge waits for CI
- [ ] Merge .github/renovate.json5 to main; check the first Dependency Dashboard issue and the first Monday batch
- [ ] Parquet.SourceGenerator: replace dependabot.yml with Renovate (shared preset)
- [ ] (Planning) Maven jars via Renovate: emulation/jars.json manifest, maven custom manager, checksum refresh as a CI workflow on Renovate PRs (hosted app can't run post-upgrade tasks), coupling rules (docs/dependencies.md#planned-maven-jars-through-renovate); after the DuckLake merge and self-hosted Renovate
- [ ] Renovate: shared preset repo rtkelly-labs/renovate-config (app needs access to it); each repo extends github>rtkelly-labs/renovate-config (fork upstream, submodule pin, Lake.* in Parquet.SourceGenerator, engines.json, images)
- [ ] Ecosystem dashboard: scheduled job over every repo's changelogs, tags and pins
- [ ] Reserve the `Lake.` prefix on nuget.org (needs a published package first)
- [ ] Deprecate each Iceberg.Net.* 0.0.1-alpha.1 as Legacy → Lake.* alternative
- [ ] Unlist Iceberg.Net.*
- [ ] Fork tier 2 (D10): rewrite at build in external/Directory.Build.targets (IL merge + internalize into Lake.Data.ParquetNet, or source namespace rewrite)
- [ ] Side-by-side test: upstream Parquet.Net + Lake.Data.ParquetNet in one process, each reads the other's files
- [ ] NOTICE + package metadata: bundled MIT Parquet.Net code
- [ ] Pack Lake.Data.ParquetNet
- [ ] Decide whether the optional drop-in Lake.Parquet (Parquet.SourceGenerator lake/ step) is still published

## Phase 3: extract shared layers (one PR each)
- [ ] Lake.Data: ParquetFiles, NestedColumns, ParquetModel/IParquetBackend, ParquetSharpBackend, ParquetInspection out of Lake.Iceberg.Data
- [ ] Lake.Core: expressions/evaluators, physical values, one Corruption, telemetry helpers, type model per D1; keep netstandard2.0
- [ ] Lake.DuckLake references only shared layers
- [ ] Dependency-direction test over src/**/*.csproj
- [ ] Optional: merge Lake.Iceberg.Core/Avro/Data (D4)
- [ ] Lake.Arrow over Lake.Data rows (D2)
- [ ] Both Parquet backends pass every suite after each step

## Phase 4: Delta (docs/lake/delta.md)
- [ ] Delta protocol section map for tools/spec_trace.py
- [ ] DAT fixtures + delta-rs oracle in interop/
- [ ] Verify Roaring64 against Delta DV test files; move Roaring64 to Lake.Core
- [ ] Reader: log replay, checkpoints (classic, multi-part, V2 + sidecars), log compaction, table features, column mapping, stats pruning, DVs, time travel
- [ ] Writer: append/overwrite/delete via put-if-absent, conflict retry, checkpoints, DVs, row tracking
- [ ] Maintenance: vacuum, checkpoint/log cleanup, optimize
- [ ] Corruption contract + fuzz targets for log/checkpoint/DV decoders
- [ ] EmulatorFidelityTests for _delta_log put-if-absent
- [ ] Spark + Delta in the cross-engine suite

## ORC (D8) — after phase 3, before or alongside Delta
- [ ] Pin an Apache ORC Java release to port from; record it in NOTICE and docs
- [ ] Spark-written ORC Iceberg fixtures (with iceberg.id attributes) under tests/**/fixtures, via interop/spark_oracle.py
- [ ] Lake.Orc reader: postscript/footer (protobuf), stripes, RLE v1/v2, dictionary, compression (zlib, snappy, zstd, lz4), nested types, column statistics, type attributes
- [ ] Corruption contract + fuzz targets for the ORC decoders
- [ ] Lake.Data: ORC as a second file format; Iceberg stops refusing ORC once fixtures pass
- [ ] Lake.Orc writer; Spark + Trino read our ORC files
- [ ] ORC spec section map for spec_trace.py

## Phase 5: across formats (optional)
- [ ] Shared typed layer over a common scan/row contract
- [ ] Lake.Arrow for every format
- [ ] Format conversion only after the above
