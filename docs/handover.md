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
| `Iceberg.Net.RestServer.Tests` | 79 | `ICEBERG_PYTHON`, `ICEBERG_SPARK_JAR`, Docker, moto for the vending cases; skips otherwise |
| `Iceberg.Net.Ecosystem.Tests` | 52 | Docker (Spark), `ICEBERG_DUCKDB_PYTHON` (DuckDB); skips otherwise |
| `Iceberg.Net.Regression.Tests` | 256 passed, 3 skipped | the emulators. The skips are expected: two Athena tests need AWS, and Nessie doesn't do v3 |

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
  - The EMR Serverless and Glue job runners. They run against moto and a local Spark stand-in, not real services.
  - The Athena runner, against real Athena. The remaining roadmap action is the AWS sandbox (#4).
- **REST server:** see [rest-server.md](rest-server.md#not-implemented) for what is still missing.
- **Change feed with delete files:** Spark can't check it, because Java's changelog scan refuses delete files. Our
  replay test covers it.
- **Upstream tickets:** proposed in [upstream/README.md](upstream/README.md). None is filed; filing is the team's call.
- **Iceberg v4:** read support waits until Java writes complete v4 tables; [iceberg-v4.md](iceberg-v4.md#plan) has
  the order.
- The full list is in [roadmap.md](roadmap.md), under "Gaps the phases left open".

## Suggested next steps

1. **Open the pull request** and let CI run. Fix anything CI's fresh environment catches, such as image versions or
   timing.
2. **Review the public API** before a first package release. Things to look at:
   - `TableMutations`, `StagedCommit`, `ScanRequest`;
   - the EF Core options;
   - the REST server's configuration keys.
3. **Run the AWS sandbox** (roadmap action #4). That covers Athena, Glue and S3 Tables, plus a real EMR Serverless job
   from `Iceberg.Net.Spark`.
4. **Decide on the upstream tickets**, and file those you want.
