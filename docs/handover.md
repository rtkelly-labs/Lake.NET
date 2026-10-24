# Handover

The state of `iceberg-dotnet` at the end of the build-out sessions (22–24 October 2026), for whoever picks it up next.
Read this first, then [AGENTS.md](../AGENTS.md) for conventions and [roadmap.md](roadmap.md) for the plan.

## Where things are

- **Branch:** `ccr-cb295622-bcczx3`, about 110 commits ahead of `main`. Phases A–G are merged into it.
- **Not yet reviewed or merged to `main`.** No pull request is open.
- **CI has never run on this work.** `.github/workflows/iceberg-dotnet.yml` runs on pull requests and on pushes to
  `main`, so opening the pull request is the first step: it runs every suite below against fresh emulators.
- **Pre-existing local change:** `.github/copilot-instructions.md` shows as a typechange (symlink → regular file). It
  was like that before this work began, and the owner chose to leave it. Don't commit it by accident.

## What was built

The README's ["What works now"](../README.md#what-works-now-m0) table is the summary. By phase:

| Phase | What | Doc |
|---|---|---|
| A | Conflict hardening: expired starting snapshot, a missing base file treated as a lost race, overwrite validation; a deterministic interleaving suite | [state-management.md](state-management.md) |
| B | UPDATE and MERGE (all clauses, cardinality check, lineage), including nested struct fields | [update-merge.md](update-merge.md) |
| C | Read path: projection, streaming, row-group pruning, ranged reads (Parquet chunks, Avro streaming), time travel with the snapshot's schema | [read-path.md](read-path.md) |
| D | Snapshot management: branches, tags, rollback, cherry-pick (also across partition specs), WAP, retention, metadata tables, change feed | [snapshot-management.md](snapshot-management.md) |
| E | Typed layer: row mapping by field ID, `AsQueryable<T>()`, typed writes | [linq.md](linq.md) |
| F | EF Core 8 provider, with `SaveChanges` atomic across tables on catalogs with multi-table transactions | [efcore.md](efcore.md) |
| G | REST catalog server; telemetry; Spark procedure SQL and EMR Serverless / Glue runners; Athena SQL and runner; DuckDB oracle | [rest-server.md](rest-server.md), [telemetry.md](telemetry.md), [spark-and-athena.md](spark-and-athena.md) |
| v4 prep | Strictly increasing snapshot timestamps, manifest abstraction, typed column statistics, `content_stats` layout, `file` type, Mumbling bitmap | [iceberg-v4.md](iceberg-v4.md) |

## Test inventory

The counts are from the last local run on the branch. Every suite passed.

| Suite | Tests | Needs |
|---|---|---|
| `Iceberg.Net.Spec.Tests` | 752 | nothing |
| `Iceberg.Net.NetStandard.Tests` | 109 | nothing |
| `Iceberg.Net.Linq.Tests` | 84 | nothing |
| `Iceberg.Net.EntityFrameworkCore.Tests` | 33 | nothing |
| `Iceberg.Net.Behaviour.Tests` (BDD) | 65 | `ICEBERG_PYTHON` |
| `Iceberg.Net.RestServer.Tests` | 125 | `ICEBERG_PYTHON`, `ICEBERG_SPARK_JAR`, Docker, moto for the vending cases, the `apache/gravitino-iceberg-rest:1.3.1` image for the Java GCS check; skips otherwise |
| `Iceberg.Net.Ecosystem.Tests` | 55 | Docker (Spark), `ICEBERG_DUCKDB_PYTHON` (DuckDB); skips otherwise |
| `Iceberg.Net.Regression.Tests` | 267 passed, 3 skipped | the emulators. The skips are expected: two Athena tests need AWS, and Nessie doesn't do v3 |

Two more checks ran on the final code:
- **Package smoke test** (`tools/package-smoke.sh`, also run in CI). Every library packs. Throwaway console apps
  install them from the local feed and run:
  - a Data consumer (create, insert, upsert, filtered read);
  - an EF Core consumer (`EnsureCreated`, `SaveChanges`, a filtered query), which also uses the Spark and Athena SQL
    helpers;
  - a netstandard2.0 library built against Core and Avro.
- **Fuzzing** (`tools/fuzz.sh`, libFuzzer): no findings.
  - Mumbling bitmap: 2.8 M runs in 3 minutes.
  - Manifest, Puffin, deletion-vector and metadata decoders: 0.9–1.6 M runs each in 90 seconds.

`./run-tests.sh emulation` runs them all. The settings used locally:

```bash
export ICEBERG_PYTHON=$PWD/interop/.venv/bin/python
export ICEBERG_DUCKDB_PYTHON=$ICEBERG_PYTHON          # after: uv sync --project interop --group duckdb
export ICEBERG_SPARK_JAR=$PWD/emulation/.cache/iceberg-spark-runtime-4.0_2.13-1.12.0.jar
```

## Environment notes (cloud sandbox)

These cost time during the build-out, and may not apply on a normal workstation or in CI.

- **Docker daemon dies under load.** Containers exit with code 255 and `docker ps` can't connect. Restart it:
  `rm -f /var/run/docker.pid; setsid nohup dockerd > /tmp/dockerd.log 2>&1 < /dev/null & disown`. Then recreate the
  REST fixture, which otherwise fails with `SQLITE_BUSY`: `docker compose -f emulation/docker-compose.yml rm -fsv rest`.
  Then run `LAKEKEEPER_IMAGE=iceberg-dotnet/lakekeeper:v0.14.1 ./emulation/up.sh --trino --polaris --gravitino --nessie --lakekeeper`.
- **quay.io is blocked**, so Lakekeeper runs from a locally built image (`emulation/lakekeeper_image.sh`; the variable
  is above).
- **extensions.duckdb.org is blocked.** The DuckDB oracle loads the iceberg extension from the
  `duckdb-extension-iceberg` / `duckdb-extension-avro` wheels in the `duckdb` dependency group instead.
- **Don't delete `bin/` folders blindly.** `find -name bin` also matches `interop/.venv/bin`. If the venv breaks,
  rebuild it with `cd interop && uv sync --group duckdb`. Clean only `src/` and `tests/`.
- **`uv run` at the repository root** creates a 1.6 GB `.venv` there. Delete it after linting.
- **Building regenerates Behaviour `*.feature.cs` files** with no real change. Revert them before committing:
  `git checkout -- tests/Iceberg.Net.Behaviour.Tests/Features/`.
- **Disk is tight** (about 6 GB free). Each full build tree is about 0.8 GB.

## Open items

- **AWS (needs an account):**
  - The L3 Athena tests and the real Glue catalog.
  - The EMR Serverless and Glue runners use the real SDK clients against moto, and the staged job runs in Spark
    against moto's Glue and S3. What only AWS can show: the services running the job themselves, their reported
    outcome, and Glue `UpdateJob` / `BatchStopJobRun` (moto lacks both).
  - The Athena runner uses the real SDK against moto, and Athena's query SQL runs on Trino. Athena's own DDL,
    `OPTIMIZE` and `VACUUM` still need real Athena.
  - The remaining roadmap action is the AWS sandbox (#4).
- **GCP (needs a project):** the REST server's GCS credential vending is checked offline only (the token exchange
  against a scripted STS; PyIceberg and Iceberg Java sending the vended token). Run it once against a real bucket:
  the exchange, then a reader's and a writer's token inside and outside the table's prefix
  ([rest-server.md](rest-server.md#google-cloud-storage)).
- **REST server:** see [rest-server.md](rest-server.md#not-implemented) for what is still missing.
- **Change feed with delete files:** Spark can't check it, because Java's changelog scan refuses delete files. Our
  replay test covers it.
- **Upstream tickets:** proposed in [upstream/README.md](upstream/README.md). None is filed; filing is the team's call.
- **Iceberg v4:** read support waits until Java writes complete v4 tables; [iceberg-v4.md](iceberg-v4.md#plan) has
  the order.
- The full list is in [roadmap.md](roadmap.md), under "Gaps the phases left open".

## Handover actions

The list is in order. Each action says who has to do it and how you know it's done. Tick the boxes in this file as
they're done.

### 1. Get CI to run the branch
- **Who:** anyone with push access.
- **How:**
  1. Open a pull request from `ccr-cb295622-bcczx3` to `main`. Changes outside `iceberg-dotnet/` are only
     `.github/workflows/iceberg-dotnet*.yml`.
  2. The `iceberg-dotnet` workflow runs every suite in the [test inventory](#test-inventory) against fresh emulators.
- **Watch for:** image tags pulled for the first time, timing-sensitive tests, and the `duckdb` dependency group.
  None of these have been through GitHub's runners yet.
- **Done when:** the workflow is green on the pull request's head.
- [ ] Done

### 2. Review the code
- **Who:** the team.
- **How:** it is big, so review it phase by phase. Each phase is one merge commit, listed by
  `git log --merges main..ccr-cb295622-bcczx3`.
- **The public API to settle before a first package:**
  - `TableMutations` (including the staged `Stage*Async` methods and `StagedCommit`);
  - `ScanRequest` / `TableScan`;
  - `SnapshotManagement`, the LINQ entry points, the EF Core options builder;
  - the REST server's configuration keys ([rest-server.md](rest-server.md#configuration-iceberg-section)).
- **Done when:** the pull request is approved and merged.
- [ ] Done

### 3. Configure the AWS sandbox
- **Who:** someone with AWS admin rights. This is roadmap action #4.
- **How:**
  1. Create an OIDC role trusted by this repository. It needs S3 on a regression prefix, Glue on `regr_*` databases,
     and one Athena engine v3 workgroup.
  2. Set the repository variables listed at the top of `.github/workflows/iceberg-dotnet-aws.yml`:
     - `ICEBERG_AWS_ROLE_ARN`, `ICEBERG_AWS_WAREHOUSE`, `ICEBERG_ATHENA_WORKGROUP`;
     - optionally `ICEBERG_ATHENA_OUTPUT` and `ICEBERG_GLUE_REST_WAREHOUSE`.
  3. Add an S3 lifecycle rule on the prefix that expires objects after 3 days.
- **Why it matters:** the workflow is nightly and only runs from `main`, so it starts after action 2. Until then,
  trigger it by hand (`workflow_dispatch`).
- **It covers:** the L3 Athena tests, real Glue, the Glue Iceberg REST endpoint, and the Athena runner in
  `Iceberg.Net.Athena`.
- **Done when:** a nightly run is green with no Athena skips.
- [ ] Done

### 4. Run a real EMR Serverless or Glue job
- **Who:** someone with the sandbox from action 3.
- **How:**
  1. Generate a maintenance script with `IcebergProcedures` (for example `rewrite_data_files` on a regression table).
  2. Submit it with `EmrServerlessSqlJobRunner` or `GlueSqlJobRunner`, following
     [spark-and-athena.md](spark-and-athena.md).
  3. Check the table afterwards with `TableValidator`.
- **Done when:** the job succeeds and the validator reports no errors. Then note the result in
  [roadmap.md](roadmap.md).
- [ ] Done

### 5. Choose shadow tables
- **Who:** the data platform owners.
- **How:** pick the production tables to shadow-read nightly (reads only), and set `ICEBERG_SHADOW_TABLES` to them
  as `glue_db.table` names separated by spaces.
- **Done when:** the shadow job in the AWS workflow reports agreement.
- [ ] Done

### 6. Decide on the upstream tickets
- **Who:** the team.
- **How:** the tickets are in [upstream/README.md](upstream/README.md), with filing steps. Filing is public, so it's
  the team's decision. Then update each page's status line.
- **Done when:** each ticket is filed or explicitly dropped.
- [ ] Done

### 7. Bump Parquet.SourceGenerator
- **Who:** the maintainer of that project.
- **How:** release a version carrying #712 (field IDs, local timestamps, INT32 dates), then raise the test pin from
  0.0.3 ([parquet-sourcegenerator.md](parquet-sourcegenerator.md)).
- **Done when:** the import tests run against the release.
- [ ] Done

### 8. Decide where the project lives
- **Who:** the team.
- **How:** decide whether `iceberg-dotnet/` moves to its own repository before NuGet publishing (roadmap,
  "Breaking out"). The folder is self-contained, so it's a move, not a refactor.
- **Done when:** decided and recorded in [roadmap.md](roadmap.md).
- [ ] Done

### 9. Tidy the local checkout
- **Who:** the owner of the working copy.
- **How:** the `.github/copilot-instructions.md` typechange (symlink → file) predates this work, and the owner chose
  to leave it. Either restore it (`git checkout -- .github/copilot-instructions.md`) or commit it deliberately.
- [ ] Done
