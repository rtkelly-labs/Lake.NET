# Handover

The state of Iceberg.NET on 25 October 2026, for whoever picks it up next. This covers the session that split the project out
of its original repository, hardened it and set up CI. Read this first, then [AGENTS.md](../AGENTS.md) for
conventions and [roadmap.md](roadmap.md) for the plan.

## Where things are

- **Repository:** `rtkelly-labs/Iceberg.NET`. It is public, its history was rewritten when it was split out, and the
  package prefix is `Iceberg.Net`.
- **`main` is green, and no pull requests are open.** Everything from the split-out session is merged (#1–#18).
- **No package has been published yet.** The release workflow exists, and the first preview is waiting on the steps
  under [Next actions](#next-actions).
- **Target:** .NET 10, plus netstandard2.0 for Core and Avro (`global.json` pins the SDK).
- **What the library does:** the README's ["What works now"](../README.md#what-works-now-m0) table, with one doc per
  area under `docs/` ([docs/README.md](README.md) indexes them).

## How work lands

`main` is protected by one ruleset ("Main Branch Governance Ruleset"):

- Every change goes through a pull request. Merges are squash-only and history stays linear.
- **Required check: `ci`.** It is the aggregate job in `.github/workflows/ci.yml`. It needs `unit`, `cross-engine`
  and `regression`, and fails if any of them fails or is cancelled.
- **Merge queue.** "Up to date with `main`" isn't required: the queue tests each pull request on top of `main` before
  merging it. Turn on auto-merge (`gh pr merge N --squash --auto`) and the pull request joins the queue once its own
  `ci` passes. GitHub refuses pushes to a branch while its pull request is queued; dequeue it first.
- **Review threads must be resolved.** Codex, CodeRabbit (configured in `.coderabbit.yaml`) and CodeQL all review
  pull requests, and an open thread from any of them blocks the merge. Fix the finding, reply with the commit, and
  resolve the thread. Dismiss a CodeQL alert only when its premise is false, and say why in the dismissal.
- **The repo is public.** Fix security findings in pull requests with neutral descriptions. Don't open issues for
  them before the fix ships.

### CI layout

| Job | Runs | Typical time |
|---|---|---|
| `unit` | CHANGELOG parse, spec traceability check (uploads the matrix as the `spec-traceability` artifact), Spec, NetStandard, LINQ, EF Core, REST catalog behaviour (Reqnroll), package smoke test | a few minutes |
| `cross-engine` | Emulators plus Spark, PyIceberg and DuckDB oracles; Ecosystem, Behaviour (BDD) and RestServer suites | about 5–6 min |
| `regression` | Emulators with `ICEBERG_REQUIRE_TARGET=1` (an unreachable target fails rather than skips); RestProtocol and L1/L2 Regression | about 9–10 min, the critical path |

- **The test-project list is the solution.** `tools/test-projects.sh` reads `Iceberg.Net.slnx` and groups projects by
  the `<CiSuite>` property in each csproj (`unit`, `cross-engine` or `regression`). To add a test project, run
  `dotnet sln Iceberg.Net.slnx add <csproj>` and set `<CiSuite>`. A test project without one fails CI.
- **Generated files are not committed.** These include `docs/spec-traceability.md` (`python3 tools/spec_trace.py
  matrix` writes it; CI runs `--check`) and the Reqnroll `*.feature.cs` files (written on every build). Both are
  gitignored.
- Other workflows:
  - `release.yml`: manual dispatch, dry run by default.
  - `aws.yml`: nightly, and idle until the AWS sandbox exists.
  - `fuzz.yml`, `mutation.yml`, `upkeep.yml`.

## What was done in this session

| PR | What |
|---|---|
| #1–#2 | Split out of the original repository with history; CI ported; repo made public |
| #3 | Deletion safety: deletes and listings stay inside the table location; `gc.enabled`; orphan cleanup has a minimum age and refreshes first; names validated |
| #4 | Decoder limits: Avro, deflate, Puffin zstd, Roaring, deletion vectors and timestamps are all bounded; corruption tests for each |
| #5 | Dependency advisories (AWSSDK.Core, SQLite via Microsoft.Data.Sqlite) |
| #6 | Target .NET 10 (EF Core 10 provider changes) |
| #7 | REST client credentials: no secrets over plain http; signer and storage endpoints must match the catalog unless allowed; options redact secrets |
| #8 | `Iceberg.Net.RestProtocol.Tests`: 110 operation-level tests, run against this server and the reference servers |
| #9 | The REST server publishes an OpenAPI document (`/openapi/v1.json`); `OpenApiConformanceTests` diffs it against the pinned Apache reference, with 25 reviewed deviations in `docs/spec/openapi-deviations.json` |
| #10 | Package metadata, SourceLink, `CHANGELOG.md`-driven `release.yml`, the first package set |
| #11 | `Iceberg.Net.RestCatalogBehaviour.Tests`: Reqnroll specs (7 features, 89 cases) |
| #12 | REST server storage locations: client-chosen locations are confined to allowed roots and namespaces and must not overlap; S3 signer restricted |
| #13 | REST server defaults: grants required with auth, audience required, session-bounded token exchange, credentials kept out of stored idempotent responses, generic 500 bodies, size and page limits |
| #14 | `.sln` replaced by `Iceberg.Net.slnx` |
| #15–#17 | CI: `merge_group` trigger, three parallel jobs, the single test-project list, generated files dropped |
| #16 | CodeRabbit configuration |
| #18 | The PyIceberg remote-signing flake (below) and `MOTO_PORT` |

### Flakes that were investigated

- **`CrossEngineTests.PyIceberg_signs_s3_requests_through_the_server`, 403 on HeadObject.** This was an upstream
  PyIceberg race ([apache/iceberg-python#3896](https://github.com/apache/iceberg-python/issues/3896)). PyIceberg
  re-registers its S3 signer on a botocore client that fsspec shares across threads, so a request can go out unsigned
  in between. The test client now sets fsspec's `skip_instance_cache` for `s3` (#18). If this test fails again, check
  whether the PyIceberg pin changed before suspecting the server's signatures.
- **`GcsCrossEngineTests`.** This wasn't a flake: the test's warehouse didn't satisfy #12's location rules. Fixed in #12.
- **`CrossServerContractTests.Missing_namespaces_and_tables_are_told_apart`.** This wasn't a flake either. Nessie
  returns an empty list for a missing namespace where other servers return 404, and the test now expects that
  (`ListingAMissingNamespaceFails`).

## Working locally

```bash
dotnet build Iceberg.Net.slnx
tools/test-projects.sh test unit                 # no Docker or Python needed
./emulation/up.sh                                # emulators; see docs/emulation.md for engine flags
export ICEBERG_PYTHON=$PWD/interop/.venv/bin/python    # after: cd interop && uv sync
tools/test-projects.sh test cross-engine
```

- **macOS: port 5000 belongs to AirPlay.** Run the emulators with `MOTO_PORT=5050 ./emulation/up.sh`, and export the
  same `MOTO_PORT` for the tests. Without it, every moto-backed test fails locally with 403 or connection errors that
  CI doesn't see.
- **Docker-backed tests skip** when Docker or an engine isn't there. Only the `regression` job turns a missing target
  into a failure. A green local run with skips proves less than CI.
- **A spec section link** (`[Spec("…")]` on a test) must name a section in `docs/spec/sections.json`, or the `unit`
  job fails. Run `python3 tools/spec_trace.py matrix --check` before pushing.
- **The OpenAPI reference** is pinned to the same Apache Iceberg ref as `sections.json`. When moving the pin, refresh
  it with `uv run --with pyyaml --no-project python -I tools/openapi_reference.py` and review
  `docs/spec/openapi-deviations.json`. The conformance test fails on a stale deviation as well as a new one.
- **Environment variables used by suites:**
  - `ICEBERG_PYTHON`: the PyIceberg interop venv.
  - `ICEBERG_DUCKDB_PYTHON`: after `uv sync --project interop --group duckdb`.
  - `ICEBERG_SPARK_JAR`: Spark runtime jar. It isn't set in CI, so 5 RestServer Spark tests skip there.
  - `MOTO_PORT`: defaults to 5000.

## Next actions

In order. Tick them here as they're done.

### 1. First preview release
- [ ] **Owner:** add the NuGet trusted-publishing policy on nuget.org: owner `rtkelly-labs`, repository `Iceberg.NET`,
  workflow `release.yml`, environment `release`. The workflow logs in as `secrets.NUGET_USER`, or `rtk_13` if that
  secret isn't set.
- [ ] In a pull request, cut `CHANGELOG.md`: move `[Unreleased]` under `## [0.1.0-preview.1] - <date>`.
  - Bring its notes up to date: the REST server is still unpublished, by choice now rather than because its location
    checks are missing.
  - Say that Athena, Glue and S3 are tested against emulators only.
- [ ] Dispatch `release.yml` from `main` with the default dry run. It must build, test, pack exactly the seven
  packages (Core, Avro, Puffin, IO, Catalog, Data, Linq) and verify `ci` on the commit.
- [ ] **Owner confirms**, then dispatch again with `dry_run` unticked.

### 2. Follow-ups
- [ ] Set `ICEBERG_SPARK_JAR` in CI so the 5 skipped RestServer Spark tests run.
- [ ] Regenerate the fixtures that still embed an old temporary root folder name in compressed Avro. Plain search and
  replace breaks the compressed blocks, so they have to be regenerated.
- [ ] Fuzz seeds for zstd Puffin blobs, Roaring run containers and zero-width Avro schemas.
- [ ] REST server idempotency: a per-principal cap on stored responses.
- [ ] Dependabot version-updates config (security updates are already on).
- [ ] Mention `MaxRecordsPerBlock` in the decoder limits documentation.
- [ ] Packages after the first preview: EntityFrameworkCore, Spark, Athena, Arrow (all `IsPackable=false` for now).
  The REST server stays unpublished.

### 3. Needs cloud accounts
- **AWS sandbox** for `aws.yml`:
  - an OIDC role with S3 on a regression prefix, Glue on `regr_*` and an Athena v3 workgroup;
  - the repository variables listed at the top of the workflow;
  - a 3-day lifecycle rule on the prefix.

  It covers the L3 Athena tests, real Glue, the Glue REST endpoint, and the EMR Serverless and Glue runners against
  the real services ([spark-and-athena.md](spark-and-athena.md)).
- **GCP:** run the REST server's GCS credential vending once against a real bucket
  ([rest-server.md](rest-server.md#google-cloud-storage)).

### 4. Longer-running items
- REST server gaps: [rest-server.md](rest-server.md#not-implemented).
- Iceberg v4 read support waits until Java writes complete v4 tables ([iceberg-v4.md](iceberg-v4.md#plan)).
- Upstream tickets are drafted in [upstream/README.md](upstream/README.md); none is filed.
- Parquet.SourceGenerator: raise the test pin once a release carries field IDs, local timestamps and INT32 dates
  ([parquet-sourcegenerator.md](parquet-sourcegenerator.md)).
- The rest is in [roadmap.md](roadmap.md), under "Gaps the phases left open".

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
| `Iceberg.Net.RestServer.Tests` | cross-engine | `ICEBERG_PYTHON`, moto, Docker for Java/Gravitino checks, `ICEBERG_SPARK_JAR` for the Spark cases |
| `Iceberg.Net.RestProtocol.Tests` | regression | the reference REST servers in the emulators |
| `Iceberg.Net.Regression.Tests` | regression | the emulators; Athena cases need AWS, and Nessie doesn't do v3 |
