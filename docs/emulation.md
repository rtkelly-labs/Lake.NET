# Emulation

Goal: run as much of the real behaviour as possible locally and in CI, with **standard, off-the-shelf
emulators**, and make every place the emulation is weaker than AWS explicit and tested.

## The stack (`emulation/docker-compose.yml`)

| Service | Image | Emulates | Why this one |
|---|---|---|---|
| `moto` | `motoserver/moto:5.2.4` via `emulation/moto_server.py` | S3 (incl. conditional `If-None-Match` writes, ranged GETs), Glue Data Catalog, STS — on one endpoint | The standard AWS emulator in the Python ecosystem; the only free emulator with a usable Glue Data Catalog; also pip-installable (`moto[server]`) so it runs without Docker |
| `rest` | `apache/iceberg-rest-fixture:1.10.0` | An Iceberg REST catalog | The Iceberg project's own test server (Java reference `CatalogHandlers` over JDBC) — used by iceberg-go and DuckDB CI. Server-side commits are applied by *Java* code, so passing against it is strong evidence of protocol correctness |

Both are configured so the REST fixture stores its metadata in moto's S3 (`CATALOG_S3_ENDPOINT=http://moto:5000`),
so tests read REST-catalog tables from the same object store as Glue tables.

Alternatives considered:

| Option | Verdict |
|---|---|
| MinIO | Good S3 fidelity but no Glue. Pulling `minio/minio` from Docker Hub and quay.io both failed while building M0 (MinIO has reportedly stopped publishing community images; unverified) — avoid as a hard dependency |
| LocalStack | S3 + many services, but Glue is not in the free tier (unverified for current releases); heavier |
| Adobe S3Mock, RustFS, SeaweedFS | S3 only |
| Lakekeeper / Polaris / Nessie containers | Excellent *additional* REST implementations to add to the conformance matrix (DuckDB's CI does) — next tier |

## Fidelity shims

moto implements APIs, not every concurrency guarantee. Where an Iceberg commit depends on a guarantee moto skips,
`emulation/moto_server.py` patches moto to enforce the **documented AWS behaviour**. Each shim has a fidelity test
in `EmulatorFidelityTests` that also runs against AWS, which is how we know the shim matches reality.

| # | AWS behaviour | moto 5.2.4 | Effect without shim | Shim | Fidelity test |
|---|---|---|---|---|---|
| 1 | Glue `UpdateTable` rejects a stale `VersionId` with `ConcurrentModificationException` | ignores `VersionId` | Concurrent Glue commits **silently lose snapshots** (observed: 4 writers → 3 snapshots) | check `VersionId` against the table's current version under a lock | `Glue_update_table_enforces_version_id` |
| 2 | Glue `CreateTable` is atomic per name | racy under threads | two concurrent creates can both succeed | serialise create | (covered by conformance create tests) |

Verified *without* a shim (moto already matches AWS):

| AWS behaviour | Test |
|---|---|
| S3 `PutObject` with `If-None-Match: *` fails with 412 when the key exists | `S3_conditional_create_rejects_existing_objects` |
| S3 ranged GET, HEAD length, delete | `S3_ranged_reads_and_lengths` |

## Known gaps (no emulator)

| Gap | Impact | Covered by |
|---|---|---|
| **Athena** | Cannot check that our tables are readable by the production engine, nor read Athena-written tables (position deletes) | `AthenaTests` — AWS mode only |
| Lake Formation, IAM policies | Permission failures only show up in AWS | AWS mode |
| Glue Iceberg REST / S3 Tables REST endpoints (SigV4) | No emulator | AWS mode once SigV4 lands |
| S3 consistency/latency, throttling (503 SlowDown) | Retry paths under load | AWS mode; consider fault-injection FileIO for L0 |
| Spark 4 / Trino as readers | v3 DV and variant interop | future tier (large images) |

## Running without Docker

```bash
uv run --project interop python -I emulation/moto_server.py --port 5000 &
aws --endpoint-url http://localhost:5000 s3 mb s3://iceberg-regression   # or boto3
# REST tests will skip unless a REST catalog is reachable at ICEBERG_REST_URI
```
