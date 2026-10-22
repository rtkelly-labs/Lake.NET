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

### AWS Glue Iceberg REST endpoint and S3 Tables — next

Both are REST catalogs (`https://glue.<region>.amazonaws.com/iceberg`, `https://s3tables.<region>.amazonaws.com/iceberg`)
authenticated by **SigV4**, not OAuth2. AWS recommends the S3 Tables endpoint for single table buckets and the
Glue endpoint for Lake Formation governance. Challenges:

- **SigV4 request signing** for an `HttpClient`: AWSSDK.Core's signer is internal-ish API; the robust option is
  a small `DelegatingHandler` implementing SigV4 (canonical request, payload hash) — plug into
  `RestCatalogOptions.Handler`. Signing name `glue` vs `s3tables`.
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

### Lakekeeper, Gravitino

- Lakekeeper (Rust, on iceberg-rust): OpenFGA authorisation, remote signing for S3; good second REST server for
  CI because it is a *different* implementation from the Java fixture.
- Gravitino: federated REST across underlying catalogs; multi-level namespaces; some optional endpoints were
  historically unimplemented — rely on the `/v1/config` `endpoints` list, not assumptions.

### Hive Metastore

- Thrift API with `metadata_location` table parameter and **explicit locks** (`lock`/`checkLock`/`unlock`) rather
  than CAS. Needs a Thrift client for .NET (Apache Thrift C# generator works). Low priority.

### JDBC / SQL catalog

- A single table `iceberg_tables(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)`;
  CAS is `UPDATE … WHERE metadata_location = :expected` and checking the row count. Trivial to implement on
  `MetastoreCatalog` with any ADO.NET provider (Postgres, SQL Server, SQLite) — useful as a **local, durable**
  catalog that PyIceberg's `SqlCatalog` can share. Good M1 candidate.

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
| Idempotency | `Idempotency-Key` (spec 1.11+) — safe retries after timeouts on servers that support it |
| Credentials lifetime | Vended credentials expire mid-job; long writers must refresh |
| Governance | Lake Formation / Unity / Polaris RBAC may make a table visible but not writable — map 403 to a clear error |
