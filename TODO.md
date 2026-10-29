# TODO: ICEBERG.NET (handover)

Local and gitignored; don't commit it. This is the handover from the 24–25 October 2026 session that split the
project out, hardened it and set up CI. A new session starts here, then reads [AGENTS.md](AGENTS.md) for
conventions and [docs/roadmap.md](docs/roadmap.md) for the plan.

> `docs/handover.md` in the repo is **stale**: it describes the pre-split branch in the original repository. This file
> replaces it. Rewrite or delete that doc in a pull request once you decide what a public handover should say.

## Pick up here

- **Checkout:** `~/code/rtkelly-labs/Iceberg.NET`, on `main` at `62abdf2`, clean.
- **`main` is green.** `ci` passed on `62abdf2`, which includes #9 and #18. No pull requests are open, and nothing is
  queued or running.
- **Local leftovers to tidy** (the permission classifier blocked removing them):
  - Local branches already merged to `main`. Merges are squash-only, so `git branch --merged` won't list them. Delete
    them by name: `build/dotnet-10 build/release build/slnx chore/coderabbit chore/dependency-advisories
    ci/merge-group ci/split-tests docs/handover feat/openapi fix/client-credentials fix/crossengine-flakes
    fix/decoder-limits fix/deletion-safety fix/rest-server-defaults fix/rest-server-locations
    test/rest-catalog-behaviour test/rest-catalog-integration test/rest-catalog-specs`.
    `scratch/bdd-vs-6a` is a throwaway.
  - `temp/handover-draft.md`: an earlier draft of this handover, written as a repo doc. Use it for a `docs/handover.md`
    pull request, or delete it.
- **Rules carried over from the owner:**
  - Push and merge only in rtkelly13 or rtkelly-labs repositories.
  - No version cut while pull requests are open.
  - Confirm with the owner before the real NuGet publish.
  - Fix security findings in pull requests with neutral descriptions, never in issues, because the repo is public.

## Where things are

- **Repository:** `rtkelly-labs/Iceberg.NET`. It is public, its history was rewritten when it was split out, and the
  package prefix is `Iceberg.Net`.
- **No package has been published yet.** See [Next actions](#next-actions).
- **Target:** .NET 10, plus netstandard2.0 for Core and Avro (`global.json` pins the SDK).
- **What the library does:** the README's "What works now" table, with one doc per area under `docs/`
  (`docs/README.md` indexes them).

## How work lands

`main` is protected by one ruleset ("Main Branch Governance Ruleset", id 25791513):

- Every change goes through a pull request. Merges are squash-only and history stays linear.
- **Required check: `ci`.** It is the aggregate job in `.github/workflows/ci.yml`. It needs `unit`, `cross-engine`
  and `regression`, and fails if any of them fails or is cancelled.
- **Merge queue** (switched on 25 October):
  - The queue squash-merges, builds up to 5 entries, and "all green" is the grouping rule. Strict "up to date with
    main" is off.
  - Turn on auto-merge (`gh pr merge N --squash --auto`). The pull request joins the queue once its own `ci` passes.
  - GitHub refuses pushes to a branch while its pull request is queued; dequeue it first.
  - The ruleset as it was before the switch is in the session scratchpad. It's only needed for a rollback: remove the
    `merge_queue` rule and set `strict_required_status_checks_policy: true`.
- **Review threads must be resolved.**
  - Codex, CodeRabbit (configured in `.coderabbit.yaml`) and CodeQL all review pull requests. An open thread from
    any of them blocks the merge.
  - Fix the finding, reply with the commit, and resolve the thread.
  - Dismiss a CodeQL alert only when its premise is false, and give the reason in the dismissal. Alert 4
    (log forging) was dismissed that way: the path has CR/LF stripped and is a structured-logging argument.
- **CI takes about 10 minutes per pull request.** The `regression` job is the critical path.

### CI layout

| Job | Runs |
|---|---|
| `unit` | CHANGELOG parse, spec traceability check (uploads the matrix as the `spec-traceability` artifact), Spec, NetStandard, LINQ, EF Core, REST catalog behaviour (Reqnroll), package smoke test |
| `cross-engine` | Emulators plus Spark, PyIceberg and DuckDB oracles; Ecosystem, Behaviour (BDD) and RestServer suites |
| `regression` | Emulators with `ICEBERG_REQUIRE_TARGET=1` (an unreachable target fails rather than skips); RestProtocol and L1/L2 Regression |

- **The test-project list is the solution.** `tools/test-projects.sh` reads `Iceberg.Net.slnx` and groups projects by
  each csproj's `<CiSuite>` (`unit`, `cross-engine` or `regression`). To add a test project, run
  `dotnet sln Iceberg.Net.slnx add <csproj>` and set `<CiSuite>`. A test project without one fails CI.
- **Generated files are not committed** and are gitignored:
  - `docs/spec-traceability.md`: run `python3 tools/spec_trace.py matrix`; CI runs `--check`.
  - Reqnroll `*.feature.cs` files: regenerated on every build.
- Other workflows:
  - `release.yml`: manual dispatch, dry run by default.
  - `aws.yml`: nightly, idle until the AWS sandbox exists.
  - `fuzz.yml`, `mutation.yml`, `upkeep.yml`.

## What this session did

| PR | What |
|---|---|
| #1–#2 | Split out with history (names fixed, internal content scrubbed, repo deleted and recreated to drop old PR refs); CI ported; repo made public |
| #3 | Deletion safety: deletes and listings stay inside the table location; `gc.enabled`; orphan cleanup has a minimum age and refreshes first; names validated |
| #4 | Decoder limits: Avro, deflate, Puffin zstd, Roaring, deletion vectors and timestamps are all bounded; corruption tests for each |
| #5 | Dependency advisories: AWSSDK.Core, and SQLite via Microsoft.Data.Sqlite 8.0.32 (bundles SQLite 3.53.3) |
| #6 | Target .NET 10 (EF Core 10 provider changes) |
| #7 | REST client credentials: no secrets over plain http; signer and storage endpoints must match the catalog unless allowed; options redact secrets |
| #8 | `Iceberg.Net.RestProtocol.Tests`: 110 operation-level tests against this server and the reference servers |
| #9 | The REST server publishes OpenAPI at `/openapi/v1.json`; `OpenApiConformanceTests` diffs it against the pinned Apache reference, with 25 reviewed deviations |
| #10 | Package metadata, SourceLink, `CHANGELOG.md`-driven `release.yml`, first package set; `release` environment created |
| #11 | `Iceberg.Net.RestCatalogBehaviour.Tests`: Reqnroll specs (7 features, 89 cases) |
| #12 | REST server storage locations confined (allowed roots, namespace roots, no overlaps across namespaces, renames or a single transaction); S3 signer restricted |
| #13 | REST server defaults: grants required with auth, audience required, session-bounded token exchange, credentials kept out of stored idempotent responses, generic 500 bodies, size and page limits |
| #14 | `.sln` replaced by `Iceberg.Net.slnx` |
| #15, #17 | CI: `merge_group` trigger, three parallel jobs, single test-project list, generated files dropped |
| #16 | CodeRabbit configuration |
| #18 | PyIceberg remote-signing flake fixed; `MOTO_PORT` |

The library was also registered in rig's `config/managed-projects.json` (rig#332, branch `config/iceberg-net-packages`),
so `workspace packages audit` checks it.

### Flakes that were investigated

- **`CrossEngineTests.PyIceberg_signs_s3_requests_through_the_server`, 403 on HeadObject.** This was an upstream
  PyIceberg race ([apache/iceberg-python#3896](https://github.com/apache/iceberg-python/issues/3896)). PyIceberg
  re-registers its S3 signer on a botocore client that fsspec shares across threads, so a request can go out unsigned
  in between. The test client now sets fsspec's `skip_instance_cache` for `s3` (#18). If it comes back, check the
  PyIceberg pin before suspecting the server's signatures.
- **`GcsCrossEngineTests`.** Not a flake: the test's warehouse didn't satisfy #12's location rules. Fixed in #12.
- **`CrossServerContractTests.Missing_namespaces_and_tables_are_told_apart`.** Not a flake: Nessie returns an empty
  list for a missing namespace, and the test now expects that (`ListingAMissingNamespaceFails`).
- **`CrossEngineTests` on #13.** Not a flake: four test servers turned auth on without grants, which #13 makes the
  server refuse. Fixed in #13.

## Working locally

```bash
dotnet build Iceberg.Net.slnx
tools/test-projects.sh test unit                       # no Docker or Python needed
MOTO_PORT=5050 ./emulation/up.sh                       # emulators; docs/emulation.md has the engine flags
export MOTO_PORT=5050 ICEBERG_PYTHON=$PWD/interop/.venv/bin/python   # after: cd interop && uv sync
tools/test-projects.sh test cross-engine
```

- **This Mac has no Docker.** Docker-backed tests skip locally, and only CI's `regression` job turns a missing target
  into a failure. Rely on CI for the cross-engine and regression suites.
- **macOS AirPlay holds port 5000.** Use `MOTO_PORT`. Without it, the moto tests fail locally with 403 or connection
  errors that CI doesn't see.
- **A spec section link** (`[Spec("…")]` on a test) must name a section in `docs/spec/sections.json`. Run
  `python3 tools/spec_trace.py matrix --check` before pushing.
- **The OpenAPI reference** is pinned to the same Apache ref as `sections.json`. Refresh it with
  `uv run --with pyyaml --no-project python -I tools/openapi_reference.py`, then review
  `docs/spec/openapi-deviations.json`. The test fails on stale deviations too.
- **A build regenerates `.feature.cs` files.** They're gitignored on `main`; on an old branch, revert them before
  committing.
- **Environment variables:**
  - `ICEBERG_PYTHON`
  - `ICEBERG_DUCKDB_PYTHON`: after `uv sync --project interop --group duckdb`.
  - `ICEBERG_SPARK_JAR`: not set in CI, so 5 RestServer Spark tests skip there.
  - `MOTO_PORT`

## Next actions

In order.

### 1. First preview release
- [ ] **Owner:** add the NuGet trusted-publishing policy on nuget.org: owner `rtkelly-labs`, repository `Iceberg.NET`,
  workflow `release.yml`, environment `release`. The workflow logs in as `secrets.NUGET_USER`, or `rtk_13` if that
  secret isn't set.
- [ ] In a pull request, cut `CHANGELOG.md`: move `[Unreleased]` under `## [0.1.0-preview.1] - <date>`.
  - Fix the stale line: the REST server is still unpublished, but by choice now, not because its location checks are
    missing.
  - Say that Athena, Glue and S3 are tested against emulators only.
- [ ] Dispatch `release.yml` from `main` as a dry run (the default). It must pack exactly the seven packages (Core,
  Avro, Puffin, IO, Catalog, Data, Linq) and verify `ci` on the commit.
- [ ] **Owner confirms**, then dispatch with `dry_run` unticked.
- [ ] Afterwards, run `workspace packages audit` from rig to confirm the published packages match the repo.

### 2. Follow-ups
- [ ] Rewrite or delete the stale `docs/handover.md` (see the note at the top).
- [ ] Set `ICEBERG_SPARK_JAR` in CI so the 5 skipped RestServer Spark tests run.
- [ ] Regenerate the fixtures that still embed the old `/tmp/sentric-*-fixture-root` folder name in compressed Avro.
  Search and replace breaks the compressed blocks, so they have to be regenerated.
- [ ] Fuzz seeds for zstd Puffin blobs, Roaring run containers and zero-width Avro schemas.
- [ ] REST server idempotency: a per-principal cap on stored responses.
- [ ] Dependabot version-updates config (security updates are already on).
- [ ] Mention `MaxRecordsPerBlock` in the decoder limits documentation.
- [ ] Packages after the first preview: EntityFrameworkCore, Spark, Athena, Arrow (all `IsPackable=false` for now).
  The REST server stays unpublished.
- [ ] Optional: measure the new CI job times (`gh run view <id> --json jobs`) and rebalance if `regression` stays
  well over the other jobs.

### 3. Needs cloud accounts
- **AWS sandbox** for `aws.yml`:
  - an OIDC role with S3 on a regression prefix, Glue on `regr_*` and an Athena v3 workgroup;
  - the repository variables listed at the top of the workflow;
  - a 3-day lifecycle rule on the prefix.

  It covers the L3 Athena tests, real Glue, the Glue REST endpoint, and the EMR Serverless and Glue runners.
- **GCP:** run the REST server's GCS credential vending once against a real bucket (`docs/rest-server.md`, Google
  Cloud Storage).

### 4. Longer-running items
- REST server gaps: `docs/rest-server.md`, "Not implemented".
- Iceberg v4 read support waits until Java writes complete v4 tables (`docs/iceberg-v4.md`, Plan).
- Upstream tickets are drafted in `docs/upstream/README.md`; none is filed.
- Parquet.SourceGenerator: raise the test pin once a release carries field IDs, local timestamps and INT32 dates.
- The rest is in `docs/roadmap.md`, under "Gaps the phases left open".

## Test inventory

| Suite | CI job | Needs |
|---|---|---|
| `Iceberg.Net.Spec.Tests` | unit | nothing |
| `Iceberg.Net.NetStandard.Tests` | unit | nothing |
| `Iceberg.Net.Linq.Tests` | unit | nothing |
| `Iceberg.Net.EntityFrameworkCore.Tests` | unit | nothing |
| `Iceberg.Net.RestCatalogBehaviour.Tests` (Reqnroll) | unit | nothing (in-process server) |
| `Iceberg.Net.Behaviour.Tests` (BDD) | cross-engine | `ICEBERG_PYTHON`; Trino for some cases |
| `Iceberg.Net.Ecosystem.Tests` | cross-engine | Docker (Spark), `ICEBERG_DUCKDB_PYTHON` |
| `Iceberg.Net.RestServer.Tests` | cross-engine | `ICEBERG_PYTHON`, moto, Docker for Java/Gravitino checks, `ICEBERG_SPARK_JAR` for Spark cases |
| `Iceberg.Net.RestProtocol.Tests` | regression | the reference REST servers in the emulators |
| `Iceberg.Net.Regression.Tests` | regression | the emulators; Athena cases need AWS, and Nessie doesn't do v3 |

## Done (security pass and NuGet readiness, 24 October)

- [x] Deletion safety (#3), decoder limits (#4), client credentials (#7)
- [x] Dependency advisories (#5); secret scanning, push protection and CodeQL default setup enabled
- [x] .NET 10 (#6)
- [x] Package metadata, SourceLink, PACKAGE_README, changelog-driven release, first package set (#10); rig registration (rig#332)
- [x] REST catalog test suites (#8 code, #11 BDD), wired into CI (#17)
- [x] OpenAPI generation and enforcement (#9)
- [x] REST server locations and signer (#12), defaults (#13)
- [x] CI stability: merge queue, split jobs, single test list, generated files out of git, CodeRabbit, flakes fixed (#14–#18)
