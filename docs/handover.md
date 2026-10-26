# Handover

Iceberg.NET is a self-contained .NET implementation of Apache Iceberg, maintained in
[`rtkelly-labs/Iceberg.NET`](https://github.com/rtkelly-labs/Iceberg.NET). The repository split, .NET 10 migration
and initial hardening are complete. Start with [AGENTS.md](../AGENTS.md), then [roadmap.md](roadmap.md) for delivery
and verification plans. Check GitHub for current branches, pull requests and CI results.

## What is implemented

The README's ["What works now"](../README.md#what-works-now-m0) table summarizes the library. The
[documentation index](README.md) covers metadata and manifests, catalogs, reads and writes, snapshot management,
LINQ, EF Core, the REST server, telemetry, and Spark/Athena helpers.

The library reads and writes Iceberg v1–v3. The [v4 work](iceberg-v4.md) is preparation; complete v4 table support
waits for reference files from Java. Real AWS and GCP verification remains outstanding: S3, Glue and the Athena
runner have been exercised against emulators; Athena SQL has unit and Trino checks, not a real Athena run.

## Releases

`CHANGELOG.md` controls release versions and notes. The initial `0.0.1-alpha.1` release exercised the release
path and NuGet trusted publishing. The `0.1.0-preview.1` changelog cut prepares the first preview; a cut alone
does not mean packages have been published. Check NuGet and repository tags for publication status.

The first package set is `Iceberg.Net.Core`, `Avro`, `Puffin`, `IO`, `Catalog`, `Data` and `Linq`.
`EntityFrameworkCore`, `Spark`, `Athena` and `Arrow` remain `IsPackable=false` for a later preview. The REST
server remains unpublished by choice; its storage-location checks are implemented. Run it from source using
[rest-server.md](rest-server.md).

To release:

1. Finish open pull requests before cutting a new version in a pull request.
2. Configure NuGet trusted publishing for repository `rtkelly-labs/Iceberg.NET`, workflow `release.yml`,
   environment `release`. The login user is `NUGET_USER`, or `rtk_13` when that secret is absent.
3. After merging the cut, dispatch `.github/workflows/release.yml` from `main` with its default `dry_run=true`.
   Check that exactly the seven packages above are packed and that the commit's `ci` check passed. Dry runs
   report missing or failing CI as a warning, so inspect that verdict too.
4. Obtain the owner's confirmation before dispatching a real NuGet publish with `dry_run=false`.

## How changes land

Changes go through pull requests with linear history and squash merges; delete the feature branch after merging.
Push and merge only in `rtkelly13` or `rtkelly-labs` repositories. Rebase onto `main` before merging.

The required check is `ci`, which aggregates `unit`, `cross-engine` and `regression`. CI runs for pull requests,
merge queue entries and pushes to `main`. Auto-merge may be enabled with `gh pr merge N --squash --auto`.
Resolve review threads after addressing findings. Report security fixes in pull requests with neutral descriptions,
not in public issues.

## Verification and local setup

The solution, `Iceberg.Net.slnx`, is CI's test-project inventory. Every test project needs a `<CiSuite>` value:

| Suite | Projects | Requirements |
|---|---|---|
| `unit` | Spec, NetStandard, LINQ, EntityFrameworkCore, RestCatalogBehaviour | .NET SDK from `global.json` |
| `cross-engine` | Ecosystem, Behaviour, RestServer | Python oracles, Docker and emulators; Spark runtime for some REST server cases |
| `regression` | RestProtocol, Regression | Emulators; AWS for L3 Athena cases |

```bash
dotnet build Iceberg.Net.slnx
tools/test-projects.sh test unit

# With Docker available:
MOTO_PORT=5050 ./emulation/up.sh --spark --trino --polaris --gravitino --nessie --lakekeeper
uv sync --project interop --group duckdb
export MOTO_PORT=5050
export ICEBERG_PYTHON=$PWD/interop/.venv/bin/python
export ICEBERG_DUCKDB_PYTHON=$ICEBERG_PYTHON
export ICEBERG_SPARK_JAR=$PWD/emulation/.cache/iceberg-spark-runtime-4.0_2.13-1.12.0.jar
tools/test-projects.sh test cross-engine
ICEBERG_TEST_TARGET=emulation ICEBERG_REQUIRE_TARGET=1 tools/test-projects.sh test regression
```

See [regression-suite.md](regression-suite.md) for test tiers, fuzzing, package smoke tests and environment variables,
and [emulation.md](emulation.md) for engine flags and fidelity shims. Important local details:

- macOS AirPlay may hold port 5000; set `MOTO_PORT=5050` for both emulators and tests.
- Tests needing unavailable engines may skip. CI's regression job uses `ICEBERG_REQUIRE_TARGET=1` to fail on an
  unreachable target. Check skips when claiming cross-engine verification.
- Generated Reqnroll `*.feature.cs` files and `docs/spec-traceability.md` are gitignored. Run
  `python3 tools/spec_trace.py matrix --check` to validate section links; CI uploads the matrix as an artifact.
- `tools/package-smoke.sh` packs the seven public libraries and builds consumers against a local feed.
- CI sets `ICEBERG_SPARK_JAR` explicitly to the runtime downloaded by `emulation/up.sh --spark`; the REST
  server test output has its own `interop/` directory, which can confuse the default repository-root lookup.
- Keep temporary files and worktrees in the gitignored `temp/` directory and local task tracking in `TODO.md`.
- Clean build outputs only under `src/` and `tests/`; deleting every directory named `bin` also destroys
  `interop/.venv/bin`.

## Remaining work

The [roadmap](roadmap.md) records the broader gaps. The immediate follow-ups after release preparation are:

- Regenerate fixtures that still embed the old fixture-root name; compressed Avro cannot be repaired by text replacement.
- Add fuzz seeds for zstd Puffin blobs, Roaring run containers and zero-width Avro schemas.
- Cap stored idempotent responses per principal in the REST server.
- Configure Dependabot version updates (in addition to security updates).

External setup remains necessary for the [AWS workflow](../.github/workflows/aws.yml): an OIDC role, S3 regression
prefix with a three-day lifecycle, Glue access and an Athena v3 workgroup, plus the workflow's repository variables.
Then run real Athena, Glue and managed Spark jobs, and choose production tables for read-only shadowing.
GCS credential vending also needs one run against a real bucket; current checks use a scripted STS exchange.

The [upstream tickets](upstream/README.md) remain drafts. Raise the Parquet.SourceGenerator test pin when a
release includes field IDs, local timestamps and INT32 dates. Continue v4 support in the order described in
[iceberg-v4.md](iceberg-v4.md#plan).
