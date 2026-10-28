# Emulation

Goal: run as much of the real behaviour as possible locally and in CI, with **standard, off-the-shelf
emulators**, and make every place the emulation is weaker than AWS explicit and tested.

## The stack (`emulation/docker-compose.yml`)

| Service | Image | Emulates | Why this one |
|---|---|---|---|
| `moto` | `motoserver/moto:5.2.4` via `emulation/moto_server.py` | S3 (incl. conditional `If-None-Match` writes, ranged GETs), Glue Data Catalog, STS — on one endpoint | The standard AWS emulator in the Python ecosystem; the only free emulator with a usable Glue Data Catalog; also pip-installable (`moto[server]`) so it runs without Docker |
| `rest` | `apache/iceberg-rest-fixture:1.10.0` | An Iceberg REST catalog | The Iceberg project's own test server (Java reference `CatalogHandlers` over JDBC) — used by iceberg-go and DuckDB CI. Server-side commits are applied by *Java* code, so passing against it is strong evidence of protocol correctness |
| `polaris` (`--polaris`) | `apache/polaris:1.8.0` + `polaris_setup.py` | A second, independently implemented REST catalog | Different server, different interpretation of the protocol (it refuses overlapping table locations, and dropping with purge unless enabled). Bootstrapped with catalog `regression` on moto S3, no STS, so no credential vending |
| `trino` (`--trino`) | `trinodb/trino:478` | A query engine over the REST fixture + moto S3 | Athena's engine family; reads and writes merge-on-read tables, so it is both an oracle and a reference writer |
| Spark (`--spark`, not a service) | `apache/spark:4.0.1` + `iceberg-spark-runtime-4.0_2.13-1.12.0.jar` (SHA-256 pinned, cached in `.cache/`) | Iceberg Java, the reference implementation | Run per test session with `docker run --network none`; tables are shared through a bind mount at the same absolute path |

All are configured to store data in moto's S3 (`http://moto:5000` inside the network, `http://localhost:5000` from
the host), so tests read every catalog's tables from the same object store as Glue tables. `MOTO_PORT` moves the host
port (on macOS, AirPlay Receiver holds 5000): `MOTO_PORT=5050 ./emulation/up.sh`, then run the tests with the same
`MOTO_PORT` exported. The test host, the Polaris, Nessie and Lakekeeper client endpoints and `moto_server.py` all follow it. Image tags are pinned;
`MOTO_TAG`, `REST_FIXTURE_TAG`, `POLARIS_TAG` and `TRINO_TAG` override them, and the weekly upkeep workflow runs the
regression suite against `:latest` so emulator changes show up before a pin bump.

**REST fixture under contention.** The fixture's default in-memory SQLite fails concurrent commits with
`SQLITE_BUSY`, which surfaces as HTTP 500. Clients must treat that as an unknown commit state, not a failure. The
soak tests found it. The compose file therefore uses a file database with WAL and a busy timeout
(`CATALOG_URI=jdbc:sqlite:/tmp/rest-catalog.db?journal_mode=WAL&busy_timeout=30000`).

Alternatives considered:

| Option | Verdict |
|---|---|
| MinIO | Good S3 fidelity but no Glue. Pulling `minio/minio` from Docker Hub and quay.io both failed while building M0 (MinIO has reportedly stopped publishing community images; unverified) — avoid as a hard dependency |
| LocalStack | S3 + many services, but Glue is not in the free tier (unverified for current releases); heavier |
| Adobe S3Mock, RustFS, SeaweedFS | S3 only |
| More REST servers | All five are in: Polaris and Gravitino (Docker Hub), Nessie (`--nessie`: the Maven Central runner jar, checksum-verified, on the Polaris JDK image) and Lakekeeper (`--lakekeeper`: the quay.io image in CI; `lakekeeper_image.sh` builds the same tag from source where quay.io is unreachable, as in the development sandbox) |

## Fidelity shims

moto implements APIs, not every concurrency guarantee. Where an Iceberg commit depends on a guarantee moto skips,
`emulation/moto_server.py` patches moto to enforce the **documented AWS behaviour**. Each shim has a fidelity test
in `EmulatorFidelityTests` that also runs against AWS, which is how we know the shim matches reality.

| # | AWS behaviour | moto 5.2.4 | Effect without shim | Shim | Fidelity test |
|---|---|---|---|---|---|
| 1 | Glue `UpdateTable` rejects a stale `VersionId` with `ConcurrentModificationException` | ignores `VersionId` | Concurrent Glue commits **silently lose snapshots** (observed: 4 writers → 3 snapshots) | check `VersionId` against the table's current version under a lock | `Glue_update_table_enforces_version_id` |
| 2 | Glue `CreateTable` is atomic per name | racy under threads | two concurrent creates can both succeed | serialise create | (covered by conformance create tests) |
| 3 | Athena `GetQueryResults` returns at most 1000 rows per page (`MaxResults` ≤ 1000, a SELECT's header row counting) with a `NextToken` | returns every row at once, ignores `MaxResults` | a client that ignores `NextToken` passes in emulation and drops rows on AWS | page the stored result by `MaxResults`/`NextToken`, refuse `MaxResults` > 1000 | `Athena_get_query_results_pages_at_1000_rows` |
| 4 | Athena reuses the execution for the same `ClientRequestToken` and rejects changed parameters | ignores the token | recovering a lost submission creates duplicate executions | serialise token lookup/creation per account/region backend; compare complete request parameters | `Athena_submission_token_recovers_the_same_execution_and_rejects_changed_parameters` (same test in AWS mode; real AWS run outstanding) |

`moto_server.py` lists the shims it applies at `GET /__shims`; a test of a shim skips (with "restart it") when the
running moto predates that shim, rather than failing on a stale emulator.

Verified *without* a shim (moto already matches AWS):

| AWS behaviour | Test |
|---|---|
| S3 `PutObject` with `If-None-Match: *` fails with 412 when the key exists | `S3_conditional_create_rejects_existing_objects` |
| S3 ranged GET, HEAD length, delete | `S3_ranged_reads_and_lengths` |

## Known gaps (no emulator)

| Gap | Impact | Covered by |
|---|---|---|
| **Athena** (the engine) | Cannot check that our tables are readable by the production engine, nor read Athena-written tables (position deletes). moto's Athena API records queries and serves results it is given (`/moto-api/static/athena/query-results`) but executes nothing; Trino 478 runs the query dialect instead (`AthenaSqlOnTrinoTests`) | `AthenaTests` — AWS mode only |
| **EMR Serverless / Glue job execution** | moto records job runs but runs nothing, and its run states are canned (EMR: `SUCCESS` at once; Glue: `STARTING → RUNNING → SUCCEEDED`, steppable with `/moto-api/state-manager/set-transition`), so a run's state never reflects what Spark did. moto 5.2.4 has no Glue `UpdateJob` or `BatchStopJobRun` | `ManagedSparkJobTests` run the staged job in Spark with `SparkJobServiceEmulator` (below); the state link needs AWS |
| Lake Formation, IAM policies | Permission failures only show up in AWS | AWS mode |
| Glue Iceberg REST / S3 Tables REST endpoints (SigV4) | No emulator | AWS mode once SigV4 lands |
| S3 consistency/latency, throttling (503 SlowDown) | Retry paths under load | AWS mode; `FaultInjectingFileIO` covers write failures, not throttling yet |
| Trino reading deletion vectors | Trino 478 refuses v3 DVs (`Unexpected PUFFIN file format`) | Spark reads them; a characterization test flags a Trino upgrade that adds support |

## Managed Spark job emulation

`tests/Iceberg.Net.Regression.Tests/SparkJobServiceEmulator.cs` plays EMR Serverless or Glue for a run moto
accepted: it reads the run (and Glue job) back from moto through the SDK and `spark-submit`s exactly what it holds —
the `s3://` entry point and statements the runner staged in moto S3, the `sparkSubmitParameters` or Glue `--conf`
— in `apache/spark:4.0.1` on the host network. It adds what the service's environment would: Iceberg's AWS
integration (`iceberg-aws-bundle-1.12.0`; EMR 7 and Glue `--datalake-formats iceberg` ship it), an `s3://` file system
(S3A from `hadoop-aws-3.4.1` plus the SDK's `s3-transfer-manager` and `apache-client` 2.54.17, the SDK version inside
the bundle) and, for every Iceberg catalog in the job's configuration, Glue/S3 endpoint overrides to moto. It refuses
a Glue job that is not `glueetl` / Python 3 or lacks `--datalake-formats iceberg`, as Glue could not run it as an
Iceberg job. The jars are fetched, SHA-256 pinned, by `emulation/up.sh --spark`.

## Running without Docker

```bash
uv run --project interop python -I emulation/moto_server.py --port 5000 &   # or export MOTO_PORT
aws --endpoint-url http://localhost:5000 s3 mb s3://iceberg-regression   # or boto3
# REST tests will skip unless a REST catalog is reachable at ICEBERG_REST_URI
```
