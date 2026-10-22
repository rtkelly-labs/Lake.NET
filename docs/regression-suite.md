# Regression suite

One suite, two targets. The **same test binaries** run in full emulation (default, local and CI) and against AWS
(manual / scheduled), selected by environment variables. Tests that need something the target cannot provide
**skip with a reason**; with `ICEBERG_REQUIRE_TARGET=1` (set by `run-tests.sh` and CI) an unreachable target is
a failure instead.

## Tiers

| Tier | Project / class | Needs | What it proves | Runtime |
|---|---|---|---|---|
| **L0 spec** | `Iceberg.Net.Spec.Tests` | nothing | Spec vectors (Appendix B/D, truncate), metadata v1/v2/v3 JSON, state machine, Avro/manifest codecs, local end-to-end with in-memory catalog, commit-protocol failure handling, **reading committed PyIceberg-written tables**, Puffin/DV/Theta against DataSketches + pyroaring reference bytes, row deletes and the v2→v3 converter, name mapping and import | ~1 s |
| **L1 catalog conformance** | `CatalogConformanceTests` | S3 + Glue (moto or AWS); REST catalog | Identical scenarios against Glue (client-side commit) and the Java REST fixture (server-side commit): lifecycle, round trip, 4 concurrent writers, stale requirements, overwrite + evolution + time travel, rename/register/drop, v3 row lineage | ~10 s |
| **L1 fidelity** | `EmulatorFidelityTests` | S3 + Glue | The storage/catalog guarantees commits rely on (conditional create, Glue VersionId); same assertions in both modes | ~1 s |
| **L2 cross-engine** | `CrossEngineTests` | L1 + PyIceberg oracle | C# writes → PyIceberg reads (rows, values, spec, summary) and PyIceberg writes → C# reads, re-partitions identically, appends → PyIceberg reads | ~5 s |
| **L2 local cross-engine** | `PuffinCrossEngineTests`, `SourceGeneratorInteropTests` | PyIceberg oracle only (local filesystem) | PyIceberg applies our position deletes and DVs, reads converted tables; DataSketches reads our NDV blobs; Parquet.SourceGenerator files are adopted and read by PyIceberg; generator gaps pinned | ~5 s |
| **L3 AWS-only** | `AthenaTests` | AWS + Athena workgroup | Athena reads our tables (counts, sums, partition pruning on our transforms); we read Athena-written tables including its position deletes, delete on top, and Athena reads the result | minutes |

Current results (emulation): **L0 111/111**, **L1+L2 32/32**, L3 skipped (requires AWS).

## Environment variables

| Variable | Default (emulation) | AWS | Meaning |
|---|---|---|---|
| `ICEBERG_TEST_TARGET` | `emulation` | `aws` | target selection |
| `ICEBERG_REQUIRE_TARGET` | unset | `1` | fail instead of skip if the target is unreachable |
| `AWS_REGION` | `eu-west-1` | your region | |
| `ICEBERG_S3_ENDPOINT` | `http://localhost:5000` | unset | S3 endpoint override |
| `ICEBERG_GLUE_ENDPOINT` | = S3 endpoint | unset | Glue endpoint override |
| `ICEBERG_REST_URI` | `http://localhost:8181` | optional | REST catalog under test |
| `ICEBERG_WAREHOUSE` | `s3://iceberg-regression/warehouse` | **required** `s3://bucket/prefix` | where test tables live (Glue tables under `/glue`) |
| `ICEBERG_ATHENA_WORKGROUP` | — | e.g. `primary` | enables L3 |
| `ICEBERG_ATHENA_OUTPUT` | — | optional `s3://…` | Athena result location if the workgroup has none |
| `ICEBERG_PYTHON` | — | — | interpreter with `interop/` deps; otherwise `uv run --project interop` is used |
| AWS credentials | `test`/`test` (set automatically) | standard SDK chain | |

## Running

```bash
./run-tests.sh spec         # L0
./run-tests.sh emulation    # L0 + L1 + L2 (starts docker compose if needed)

# AWS: use a sandbox account/bucket. Every run creates Glue databases named regr_<purpose>_<yyyyMMddHHmm>_<id>
# and removes them (tables purged) at the end of each test.
export AWS_PROFILE=data-sandbox ICEBERG_WAREHOUSE=s3://iceberg-net-regression/runs ICEBERG_ATHENA_WORKGROUP=primary
./run-tests.sh aws
```

AWS permissions needed: `s3:GetObject/PutObject/DeleteObject/ListBucket` on the warehouse prefix;
`glue:*Database*`, `glue:*Table*` on `regr_*`; `athena:StartQueryExecution/GetQueryExecution/GetQueryResults`
on the workgroup (and its output bucket). Cost per run: a handful of Athena queries over KB-sized tables (cents).

Leftovers: if a run is killed, delete Glue databases matching `regr_*` older than a day and the corresponding
`$ICEBERG_WAREHOUSE` prefixes. Data files are not deleted by `DropTable(purge)` yet (metadata only) — set an S3
lifecycle rule on the regression prefix (e.g. expire after 3 days).

## CI

`.github/workflows/iceberg-dotnet.yml` runs L0 + emulation on every PR touching `iceberg-dotnet/**` (path-filtered,
so it does not affect the platform's CI time budget). AWS mode is not wired to CI: it needs an OIDC role in a
sandbox account — add a `workflow_dispatch`/scheduled job with `aws-actions/configure-aws-credentials` once a role
exists.

## Adding tests

- Spec behaviour → L0, citing the spec section; prefer the spec's vectors.
- Anything that touches a catalog → add to `CatalogConformanceTests` (it runs for every catalog), not a new class.
- Anything another engine observes → add a `CrossEngineTests` case (extend `interop/pyiceberg_oracle.py`).
- An AWS guarantee the emulator lacks → shim + `EmulatorFidelityTests` ([emulation.md](emulation.md)).
- Regenerate L0 PyIceberg fixtures with the oracle's `fixtures` command when its scenarios change; they are
  committed so L0 stays network-free.

## Future tiers

| Tier | Adds | Blocker |
|---|---|---|
| More REST servers | Lakekeeper (Rust), Polaris, Nessie in compose, added to the `Catalogs` theory | none — container time |
| Spark 4 oracle | v3 DV and variant cross-checks; Spark procedures as maintenance reference | ~1 GB image; run nightly |
| Fault injection | FileIO/catalog wrappers that throttle, time out, or fail after apply | small; extends `CommitProtocolTests` |
| Property-based | random schemas/partition specs × values, compared with PyIceberg | oracle throughput |
