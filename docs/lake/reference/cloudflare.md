# Cloudflare: R2, Basin Catalog, Pipelines and SQL

**Status:** evaluation and reference, checked 2026-10-31 against Cloudflare's documentation source
(`cloudflare/cloudflare-docs` at `6ccb2d1`). Facts about Cloudflare are from those docs. Facts about Lake.NET name the
code and the test. Nothing here has run against a real Cloudflare account yet; the `cloudflare` workflow does that once
credentials are configured ([Verification](#verification)).

> **Naming.** Cloudflare renamed its data products: R2 Data Catalog is now **Basin Catalog** (generally available),
> Cloudflare Pipelines is now **Basin Pipelines** (beta), and R2 SQL is now **Basin SQL**. Existing configurations and APIs
> keep working; Cloudflare says the old names will be deprecated. R2 itself keeps its name.

## Verdict

| Question | Answer |
|---|---|
| Is R2 a viable S3 replacement for Lake.NET? | **Yes, for all three formats.** It has the one guarantee every commit protocol here needs: an atomic `If-None-Match: *` on `PutObject`, at most one concurrent creator wins. It is strongly consistent for reads, overwrites, deletes and listings. One SDK setting differs from AWS (checksums), and `S3FileIOOptions.ForR2` sets it. |
| Can Basin Catalog be Lake.NET's Iceberg catalog? | **Yes, as an Iceberg REST catalog** with bearer-token auth and vended credentials: what `RestCatalog` already speaks. `RestCatalogOptions.ForBasinCatalog` configures it. Which optional REST features it implements (v3, views, multi-table transactions, scan planning) is unknown and recorded by tests rather than assumed. |
| Can Cloudflare be a *source*? | **Yes.** Basin Pipelines ingests events (HTTP or Workers bindings), transforms them with SQL and commits Iceberg tables to Basin Catalog, exactly once, at most once a minute. Lake.NET reads those tables like any other Iceberg table, incrementally with append scans. |
| Can Cloudflare read what Lake.NET writes? | **Yes.** Basin SQL is a read-only, distributed SQL engine over Basin Catalog tables (Parquet only). It is a candidate oracle, alongside DuckDB, which attaches Basin Catalog through its `iceberg` extension. |
| Main risks | Vended credentials are as wide as the API token, not per table. Managed compaction and expiry commit concurrently with your writers. Unicode keys are NFC-normalised. Delta needs a strict put-if-absent, which `S3FileIO` doesn't give today. See [Risks](#risks-and-how-lakenet-handles-them). |

## How the pieces fit

```
  producers ──HTTP / Workers──▶ Basin Pipelines (stream → SQL → sink)
                                      │ commits Iceberg snapshots (≥ 60 s apart)
                                      ▼
  Lake.NET writer ──REST (bearer token)──▶ Basin Catalog ◀──REST── DuckDB, Spark, Trino, PyIceberg, Snowflake
        │                                       │  (managed compaction, snapshot expiry)
        │ vended SigV4 credentials              ▼
        └──────────── S3 API ───────────▶  R2 bucket  ◀── Basin SQL (read-only)
                                     (data, metadata, manifests)
```

| Cloudflare product | Role for Lake.NET | Formats |
|---|---|---|
| **R2** (S3 API) | Object storage for data and metadata files | Iceberg, DuckLake (data files), Delta (data and `_delta_log`) |
| **Basin Catalog** | Managed Iceberg REST catalog inside an R2 bucket; automatic compaction and snapshot expiry | Iceberg |
| **Basin Pipelines** | Managed streaming ingestion into Iceberg tables: a source Lake.NET reads from | Iceberg |
| **Basin SQL** | Read-only SQL over Basin Catalog tables: a reader and possible oracle | Iceberg |
| R2 temporary credentials | Short-lived SigV4 credentials bound to one bucket, a set of operations and optionally path prefixes | Credential vending by Lake.NET's own REST server (planned) |

## R2 against what Lake.NET needs from storage

Each row is a property the commit and maintenance code relies on, checked by `FileIOConformanceTests`, which runs against
R2 as `R2FileIOConformanceTests`.

| Need | R2 | Lake.NET |
|---|---|---|
| Create-if-absent, atomically (Iceberg metadata files, Delta log entries, Puffin files) | ✅ `If-None-Match: *` on `PutObject` is evaluated atomically against committed state; of concurrent creators at most one succeeds, the rest get `412` | `S3FileIO.WriteAsync(overwrite: false)` sends `If-None-Match: *` and maps `412`/`409` to `FileAlreadyExistsException` |
| Read-after-write, overwrite, delete, listing consistency | ✅ Strongly consistent, globally. (A custom domain with caching relaxes this; the S3 API never goes through the cache) | Orphan cleanup and expiry rely on listings being complete |
| A lost response to a create | R2 documents that a `404` doesn't prove an in-flight `PUT` failed, and recommends retrying with `If-None-Match: *` | `S3FileIO` already does this: a retried create that gets `412` reads the object back and accepts it if the bytes are its own (`ThrottlingTests`). This is safe for Iceberg's unique file names; **not** for Delta (see risks) |
| Ranged reads | ✅ `Range` on `GetObject` | Exact-length check added after the conformance suite found S3 returns short ranges silently (`EndOfStreamException`) |
| Listing by prefix with paging | ✅ `ListObjectsV2` with continuation tokens, `prefix`, `start-after` | `S3FileIO.ListAsync` |
| Region | `auto` (`us-east-1` and empty alias to it) | `ForR2` sets `auto` |
| Checksums | CRC64NVME full-object; CRC32/CRC32C/SHA only as composite (multipart) checksums | The AWS SDK v4 sends a CRC32 trailer on every upload by default; `S3FileIOOptions.ChecksumsOnlyWhenRequired` (on for `ForR2` and, by default, for any non-AWS endpoint) turns that off |
| Object size | Single `PutObject` up to 5 GiB less a little (unverified); multipart above | Lake.NET writes each file with one `PutObject` (gap review §6: no multipart yet) |
| Unicode keys | Keys are NFC-normalised on store; a listing returns the encoding last uploaded | Two Unicode-equivalent paths are one object on R2 and two on S3. Lake.NET never generates such pairs, but a user-chosen partition value could. The conformance suite checks that listings spell keys as written |
| Data residency | Jurisdictions (`eu`, `fedramp`) with their own endpoints | `ForR2(..., jurisdiction: "eu")` |
| Server-side encryption | SSE-C only (no KMS headers) | Not used; DuckLake encrypted lakes and Parquet modular encryption encrypt client-side |
| Egress | No egress fees | Readers in other clouds pay nothing to read |

## Basin Catalog as an Iceberg REST catalog

| Topic | Basin Catalog | Lake.NET |
|---|---|---|
| Endpoint and warehouse | A catalog URI and a warehouse name, printed by `wrangler basin catalog enable <bucket>` or shown in the dashboard | `RestCatalogOptions.ForBasinCatalog(catalogUri, warehouse, token)` |
| Authentication | `Authorization: Bearer <Cloudflare API token>`; the token needs **both** catalog and R2 storage permissions (read for readers, read-write for writers) | `RestCatalogOptions.Token` |
| Storage access | Vended SigV4 credentials (`X-Iceberg-Access-Delegation: vended-credentials`); Cloudflare's Spark example disables remote signing | `AccessDelegation = "vended-credentials"`; the vended `s3.access-key-id` and endpoint pass `RestCatalog`'s endpoint check because they come with credentials |
| Scope of vended credentials | They **inherit the token's R2 permissions**. Cloudflare's docs describe no narrowing to the table (unverified beyond that wording) | Use a read-only token for readers, and one token per trust boundary. Contrast Lake.NET's own REST server, which narrows STS credentials to the table prefix |
| Table maintenance | Managed compaction (target 64–512 MB, default 128 MB; output files prefixed `compacted-` under `/data/`) and snapshot expiry (`--older-than-days`, default 30; `--retain-last`, default 5). Both need a service token with catalog and storage write | Lake.NET's writers re-plan on conflict, so a compaction commit between their read and their commit is retried, not lost (`InterleavingTests`, `OverwriteValidationTests`). Don't run Lake.NET's own expiry or compaction on the same tables as well |
| Deletes and purge | Through the catalog only; `DROP TABLE … PURGE` deletes the files | `DropTableAsync(purge: true)` |
| Format v3, views, multi-table transactions, server-side scan planning | Not stated in the docs | Recorded by `BasinCatalogTests.Format_version_3_is_created_or_refused_cleanly`; the others to be added as the first live run shows what is advertised |
| Pricing (in addition to R2 storage and operations) | Catalog operations: 1 M/month included, then $9.00 per million. Compaction: 10 GB and 1 M objects/month included, then $0.005/GB and $2.00 per million objects | A Lake.NET commit is a handful of catalog operations (load, commit, sometimes a config call); a re-plan after a conflict adds a load and a commit |

## Basin Pipelines as a source

- **Shape.** Streams (HTTP endpoint or Workers binding) buffer events; a pipeline's SQL transforms them per record; a
  sink commits them as an Iceberg table in Basin Catalog (or as Parquet/JSON files in R2).
- **Guarantees.** Exactly-once delivery to R2. Commits are at least 60 seconds apart, a minimum set "to prevent
  compaction issues". Events that don't match the sink's schema are accepted at ingestion and dropped at delivery, with
  dropped-event metrics.
- **Lake.NET reading it.** Load the table through `ForBasinCatalog`, then read it with the ordinary scan API. To consume
  only new data, use incremental append scans (`TableScan.PlanAppendsAsync` / `AppendSnapshots`) from the last snapshot
  processed. Their gaps are noted in gap review P2.
- **Writing to the same table.** Possible in principle (optimistic concurrency), but the pipeline, managed compaction
  and your writer then all commit to one table. Prefer separate tables, and join or merge downstream.

## Basin SQL

Read-only (`INSERT`, `UPDATE` and `DELETE` are refused), Parquet only, partitioned and unpartitioned tables, joins and
CTEs, with timeouts on heavy queries. For Lake.NET it is a second reader of the tables it writes to Basin Catalog: a
useful smoke oracle (row counts, aggregates) once the live suite runs. It is not a spec oracle: Spark and PyIceberg stay
that.

## The formats on Cloudflare

| Format | Storage | Catalog | Status in Lake.NET |
|---|---|---|---|
| **Iceberg** | R2 | Basin Catalog (managed), or Lake.NET's own REST server or a JDBC catalog pointed at R2 | `ForR2` + `ForBasinCatalog`; live tests written, not yet run |
| **DuckLake** | R2 for data files | A PostgreSQL or SQLite catalog database anywhere (DuckDB can attach the same lake with an R2 secret) | Not tested on any object store yet (gap review ST2); `ForR2` gives the FileIO |
| **Delta** | R2, including `_delta_log` | none needed | Not built. R2's atomic `If-None-Match` is enough for Delta's put-if-absent commit (delta-rs supports R2 with `conditional_put=etag`), but see the strict-create risk below |

## Risks, and how Lake.NET handles them

1. **Over-broad vended credentials.** Basin Catalog's credentials carry the API token's R2 permissions. A reader with a
   read-write storage token can write anywhere in the bucket. *Handling:* document per-workload tokens. For per-table
   isolation, run Lake.NET's own REST server against R2 and vend **R2 temporary credentials** scoped to the table's
   prefix: R2 supports path-scoped temporary credentials, and the server's STS vendor is the model for this (planned).
2. **Concurrent managed maintenance.** Compaction and expiry commit while your writers do. *Handling:* re-plan on
   conflict is covered by the interleaving and overwrite-validation suites. Lake.NET's orphan cleanup must not run on a
   Basin-maintained table: Basin's compaction writes `compacted-*` files before committing them, and a cleanup with a
   short grace period could delete them.
3. **Strict put-if-absent for Delta.** `S3FileIO.WriteAsync(overwrite: false)` accepts a `412` when the existing object
   holds the same bytes, which recovers a create whose response was lost. Iceberg file names are unique per write, so
   that is safe there. A Delta commit file (`_delta_log/N.json`) is a shared name. *Handling (decided):* every Delta
   commit attempt embeds a fresh `commitInfo.txnId`, so two writers never write the same bytes, and byte equality
   again proves ownership ([specs/delta.md](specs/delta.md), writer invariant 11). This applies on S3 as much as R2.
4. **Checksums.** Fixed by `ChecksumsOnlyWhenRequired`. Integrity then relies on TLS plus Parquet's and Avro's own
   framing. Re-enable it (`ChecksumsOnlyWhenRequired = false`) if Cloudflare documents full-object CRC32 trailers.
5. **Unicode equivalence.** Covered for single keys by the conformance suite. Partition values that differ only by
   normalisation would collide on R2: refuse or normalise them before they become paths (open).
6. **Product maturity.** Basin Catalog is generally available; Pipelines is beta; Basin SQL is evolving. Pin behaviour
   with the live tests, not with documentation.

## Recommended setups

| Setup | When | Pieces |
|---|---|---|
| **A. Managed** | You want Cloudflare to run the catalog and maintenance | R2 + Basin Catalog; Lake.NET writes with `ForBasinCatalog`; Pipelines for event ingestion; Basin SQL, DuckDB or Spark read |
| **B. Self-hosted catalog on R2** | You need per-table credentials, multi-table transactions or server-side planning you control | R2 + Lake.NET's REST server (`Iceberg.Net.RestServer`) with an R2 FileIO and, once built, R2 temporary-credential vending |
| **C. DuckLake on R2** | SQL-catalog simplicity, small frequent writes (inlining) | R2 for data + PostgreSQL for the catalog; Lake.NET and DuckDB share the lake |

.NET does not run inside Workers (JavaScript and WebAssembly runtimes). Lake.NET runs where your .NET services run and
reaches R2 over its S3 API; Workers send events into Pipelines.

## What is built

| Piece | Where | Tested by |
|---|---|---|
| `S3FileIOOptions.ForR2(account, key, secret, jurisdiction?, sessionToken?)` | `src/Iceberg.Net.IO/FileIO.cs` | `CloudflarePresetTests` (unit) |
| `S3FileIOOptions.ChecksumsOnlyWhenRequired` (auto for non-AWS endpoints) | same | `CloudflarePresetTests`; the S3 regression suite on moto |
| `RestCatalogOptions.ForBasinCatalog(uri, warehouse, token)` | `src/Iceberg.Net.Catalog/RestCatalog.cs` | `CloudflarePresetTests` |
| Live suite: `R2FileIOConformanceTests`, `BasinCatalogTests` | `tests/Iceberg.Net.Regression.Tests/CloudflareR2Tests.cs` | `.github/workflows/cloudflare.yml` (manual dispatch) |

## Verification

The live tests read their target from the environment, and skip with a reason when it isn't configured. With
`LAKE_CLOUDFLARE_REQUIRED=1`, as in the workflow, a missing setting fails instead.

| Variable | Meaning |
|---|---|
| `LAKE_R2_ACCOUNT_ID`, `LAKE_R2_BUCKET` | The account and a bucket used only for tests |
| `LAKE_R2_ACCESS_KEY_ID`, `LAKE_R2_SECRET_ACCESS_KEY` | An R2 API token's S3 credentials, read-write on that bucket |
| `LAKE_R2_JURISDICTION` | Optional: `eu` or `fedramp` |
| `LAKE_BASIN_CATALOG_URI`, `LAKE_BASIN_WAREHOUSE`, `LAKE_BASIN_TOKEN` | Basin Catalog on a test bucket, and a token with catalog and storage read-write |

Run locally:
`LAKE_R2_ACCOUNT_ID=… dotnet test tests/Iceberg.Net.Regression.Tests --filter Category=Cloudflare`.

Next, in order:

1. Run the live suite once, and record what Basin Catalog advertises (v3, views, transactions, planning) in this page.
2. DuckLake on R2 (gap review ST2).
3. R2 temporary-credential vending in the REST server.
4. Basin SQL as a smoke oracle.
5. A strict create for Delta.

## Sources

- Cloudflare docs (source, `6ccb2d1`): `basin/index`, `basin-catalog/{index,manage-catalogs,table-maintenance,deleting-data,platform/pricing,config-examples/*}`, `basin-pipelines/sinks/available-sinks/r2-data-catalog`, `basin-sql/reference/limitations-best-practices`, `r2/api/s3/{api,temporary-credentials}`, `r2/reference/{consistency,unicode-interoperability,data-location}`.
- Published pages: [Basin Catalog](https://developers.cloudflare.com/basin-catalog/), [R2 S3 API compatibility](https://developers.cloudflare.com/r2/api/s3/api/), [R2 consistency](https://developers.cloudflare.com/r2/reference/consistency/), [Basin Pipelines](https://developers.cloudflare.com/basin-pipelines/).
- delta-rs R2 support: [delta-io/delta-rs](https://github.com/delta-io/delta-rs).
