# Catalog providers — integration challenges

A catalog does exactly one essential job: atomically map a table name to its current metadata file. Everything
else (namespaces, auth, credential vending, governance) is where providers differ, and where a port spends most
of its integration time.

## Two families

| Family | Providers | Who applies commits | Atomicity primitive |
|---|---|---|---|
| **Metastore** (client-side commit) | AWS Glue Data Catalog (API), Hive Metastore, JDBC/SQL, DynamoDB, Hadoop/file-system | this library (`TableMetadataBuilder`) | provider-specific CAS on a pointer |
| **REST** (server-side commit) | Apache Polaris / Snowflake Open Catalog & Horizon, Databricks Unity Catalog, AWS Glue IRC endpoint, AWS S3 Tables, Nessie, Lakekeeper, Gravitino, Iceberg REST fixture | the server | the HTTP request |

The port implements one of each family end to end (Glue, REST) plus an in-memory reference, and every one runs
the same `CatalogConformanceTests`. New catalogs are added to that theory, never given their own fork of it.

## Provider-by-provider

### AWS Glue Data Catalog (API) — implemented, primary

| Challenge | Detail | Handling |
|---|---|---|
| CAS | `UpdateTable` with `VersionId` is the only optimistic lock; Java's `GlueTableOperations` also checks `metadata_location` | Both, `ConcurrentModificationException` → `CommitFailedException` |
| Emulation gap | moto ignores `VersionId` ⇒ silent lost updates in tests | Fidelity shim + test that runs in AWS mode too ([emulation.md](emulation.md)) |
| Namespaces | Single level only (Glue database); names lowercase `[a-z0-9_]` | Multi-level namespaces rejected with a clear error |
| Table parameters | `table_type=ICEBERG`, `metadata_location`, `previous_metadata_location`; other tools (Athena, Lake Formation) add their own | Preserve unknown parameters on every update |
| Columns | Glue columns are informational for Iceberg but used by Lake Formation column grants, the console, crawlers | Written on every commit with `iceberg.field.id` / `iceberg.field.optional` parameters (Java convention) |
| Table versions | Every `UpdateTable` archives a version unless `SkipArchive`; Iceberg commits are frequent | `SkipArchive = true` by default (Java `glue.skip-archive`) |
| Rename | Not supported by Glue; implemented as create + delete — **not atomic** | Same as Java; documented |
| Lake Formation | Permissions may hide tables/columns, and LF-managed locations need credential vending | Out of scope for M0; the Glue IRC endpoint is AWS's recommended route for LF governance |
| Cross-account | `CatalogId` (Java `glue.id`) | Supported |
| Multi-table transactions | Glue has no atomic update of several tables | `SupportsMultiTableTransactions` is false and `CommitTransactionAsync` throws `NotSupportedException`; EF Core's `SaveChanges` falls back to table by table |

### AWS Glue Iceberg REST endpoint and S3 Tables — next

Both are REST catalogs (`https://glue.<region>.amazonaws.com/iceberg`, `https://s3tables.<region>.amazonaws.com/iceberg`)
authenticated by **SigV4**, not OAuth2. AWS recommends the S3 Tables endpoint for single table buckets and the
Glue endpoint for Lake Formation governance. Challenges:

- **SigV4 request signing**: ✅ `SigV4Handler` (a `DelegatingHandler`; canonical request with double-encoded path
  segments, payload hash, session token; an OAuth `Authorization` moves to `Original-Authorization` as in Java).
  `RestCatalogOptions.ForGlue(region, accountId)` / `ForS3Tables(region, tableBucketArn)` wire it up. Verified against
  AWS's published vectors and botocore; not yet against the live endpoints (AWS mode: `ICEBERG_GLUE_REST_WAREHOUSE`).
- **Warehouse/prefix mapping**: `warehouse` is an account/catalog ARN-ish identifier; the server returns `prefix`
  in `/v1/config`, which must be honoured on every path (`RestCatalog` does).
- **S3 Tables managed maintenance** (compaction, snapshot expiry) runs server-side — writers must expect
  snapshots they did not create and files disappearing after expiry.
- **v3**: S3 Tables supports v3 types; Athena does not. Choose per table.

### Apache Polaris / Snowflake Open Catalog / Horizon

- OAuth2 client credentials (`credential = id:secret`, `scope = PRINCIPAL_ROLE:...`); token endpoint configurable
  because `/v1/oauth/tokens` is deprecated upstream.
- **Credential vending** is the norm: `X-Iceberg-Access-Delegation: vended-credentials`, per-table
  `storage-credentials` with prefixes; the client must pick the longest matching prefix and refresh before expiry
  (`loadCredentials`). Selection is implemented; refresh is not.
- Snowflake Horizon: external engines cannot write v3 tables yet.

### Databricks Unity Catalog

- REST at `/api/2.1/unity-catalog/iceberg-rest`; PAT or OAuth M2M tokens.
- Managed Iceberg tables accept external writes; v3 status varies by runtime (unverified).
- Three-level namespace (`catalog.schema.table`) maps to a `warehouse` (catalog) + one-level namespace.

### Nessie

- REST with git-like semantics: refs are *catalog* branches, distinct from Iceberg table refs. A commit can
  conflict on the catalog branch even when the table did not change — surfaces as 409, handled as a retry.
- Still actively released (0.109).
- Emulated (`emulation/up.sh --nessie`; the Maven runner jar on a JDK image) and in the conformance matrix. Findings from
  running it, all handled:
  - It rejects an `add-schema` whose ID is taken ("A schema with ID 0 already exists") rather than renumbering it, so
    the client sends schema, spec and sort-order evolutions with the next free ID, as Java does.
  - The metadata it serves carries only the current snapshot and an empty `metadata-log` (history lives in Nessie
    commits), so time travel by snapshot ID through Nessie only reaches the current snapshot.
  - 0.109 has no format v3: a create asking for v3 silently returns v2, and `upgrade-format-version` to 3 fails with 500.
  - `create-table` must carry `stage-create`; its load-table config pins PyIceberg to `FsspecFileIO` (needs `s3fs`).

### Lakekeeper, Gravitino

- Lakekeeper (Rust, on iceberg-rust): OpenFGA authorisation, remote signing for S3; good second REST server for
  CI because it is a *different* implementation from the Java fixture.
- Emulated (`emulation/up.sh --lakekeeper`; 0.14.1, Postgres, warehouse `regression` on moto without STS). It
  passed every conformance, view, soak and PyIceberg cross-engine test the first time, v3 tables included. The
  setup needs three things:
  - One S3 endpoint serves both Lakekeeper and its clients, so it runs on the host network against
    `localhost:5000` (`$MOTO_PORT`).
  - The warehouse uses a `hard` delete profile, because the default soft-delete defers the purge.
  - Remote signing is switched off (`remote-signing-enabled: false`). Lakekeeper enables it by default even
    without STS, and then pins PyIceberg to `FsspecFileIO` and signs every S3 request. Those signed requests
    occasionally came back 403 from moto. Our client brings its own credentials there; it can sign remotely
    (`AccessDelegation = "remote-signing"`), which is tested against this repository's server.
- Gravitino: federated REST across underlying catalogs; multi-level namespaces; some optional endpoints were
  historically unimplemented — rely on the `/v1/config` `endpoints` list, not assumptions.

### This repository's REST server (`Iceberg.Net.RestServer`)

- An ASP.NET Core server for the REST spec. It applies commits with the same `TableMetadataBuilder` the metastore
  catalogs use, and moves pointers in an `ICatalogStore`: Java JdbcCatalog's tables in SQLite (V1 layout, so the same
  database opens in `JdbcCatalog`, Java and PyIceberg), or in memory. Details: [rest-server.md](rest-server.md).
- It is in the conformance matrix as `dotnet-rest`, hosted by the regression suite on moto, and passes the
  conformance, view, soak and PyIceberg cross-engine suites. Its own tests also drive it from PyIceberg 0.12 and from
  Spark 4.0.1 (Iceberg 1.12 `RESTCatalog`), both of which can also plan their scans on it.
- It plans scans on the server (`plan`, `tasks`; stateless plan tasks; incremental append scans), enforces static role
  grants through a pluggable `IAuthorizer` (403 `ForbiddenException`), vends STS credentials scoped to the table's
  prefix, Azure SAS tokens and downscoped GCS tokens, signs S3 requests for clients that hold no credentials (remote signing: our client,
  Spark and PyIceberg, on a moto that checks signatures), and replays responses by `Idempotency-Key`. Idempotency keys
  and plan IDs live in its database, so instances behind a load balancer share them.
- GCS token vending is implemented and checked offline; real Google Cloud verification remains outstanding. Signing for anything but S3 is not implemented.

### Hive Metastore

- Thrift API with `metadata_location` table parameter and **explicit locks** (`lock`/`checkLock`/`unlock`) rather
  than CAS. Needs a Thrift client for .NET (Apache Thrift C# generator works). Low priority.

### JDBC / SQL catalog

- A single table `iceberg_tables(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)`;
  CAS is `UPDATE … WHERE metadata_location = :expected` and checking the row count. ✅ `JdbcCatalog` over any
  ADO.NET provider, in the "V0" layout Java and PyIceberg default to (no `iceberg_type` column), plus
  `iceberg_namespace_properties` with the `exists` marker row. Runs the full conformance suite and soak over SQLite,
  and PyIceberg's `SqlCatalog` reads and writes the same database (`CrossEngineTests`).
- Views: `new JdbcCatalog(..., views: true)` switches to Java's "V1" layout (`jdbc.schema-version=V1`): an
  `iceberg_type` column marks rows `TABLE` or `VIEW`, NULL counting as a table, and `InitializeAsync` adds the column
  to an existing V0 database with Java's `ALTER TABLE`. Tables and views share the primary key, so one name is
  either. PyIceberg's `SqlCatalog` ignores the column and keeps working (the regression suite runs it against a V1
  database), but its `list_tables` would also list views. Tested in `JdbcCatalogTests` and `ViewConformanceTests`.

### DynamoDB, Hadoop/file-system

- DynamoDB catalog: conditional `PutItem` on version; PyIceberg supports it.
- File-system ("Hadoop") catalog: `version-hint.text` + rename — **unsafe on S3** (no atomic rename); read-only
  support at most.

## Cross-cutting challenges

| Challenge | Notes |
|---|---|
| Error semantics | REST: 409 = conflict (retry), 500/502/503/504 on commit = **unknown**; metastores: provider exceptions must be classified the same way — a misclassified timeout is how tables get corrupted |
| Namespace encoding | REST multi-level namespaces joined with `0x1F` then URL-encoded; Glue single-level; Unity three-level |
| Default locations | Glue/Hive convention `{warehouse}/{db}.db/{table}`; REST servers assign their own — never assume |
| Capability negotiation | Use `/v1/config` `endpoints`; absent list ⇒ assume the default set |
| Multi-table transactions | `ICatalog.SupportsMultiTableTransactions`: in-memory (one lock), JDBC (one database transaction), REST when the server advertises `/transactions/commit` (absent list ⇒ assumed); Glue no. `StagedCommit.CommitAllAsync` and EF Core's `SaveChanges` use it |
| Idempotency | `Idempotency-Key` (spec 1.11+) — safe retries after timeouts on servers that support it |
| Credentials lifetime | Vended credentials expire mid-job; long writers must refresh |
| Credential exposure | The REST client sends a token or OAuth credential only over https or to loopback (`AllowInsecureCredentials` to override); a `signer.uri` must be the catalog's own origin (`AllowCrossHostSigner`); a server-set `s3.endpoint` is refused when the client would sign for it with its own AWS credentials, unless it is AWS or loopback (`AllowServerStorageEndpoint`). Option records redact secrets in `ToString()` |
| Governance | Lake Formation / Unity / Polaris RBAC may make a table visible but not writable — map 403 to a clear error |
