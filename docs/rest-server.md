# REST catalog server

`src/Iceberg.Net.RestServer` is an Iceberg REST catalog server in ASP.NET Core. It serves the REST catalog
OpenAPI spec (`open-api/rest-catalog-open-api.yaml`, as of Java 1.12) over this library's own commit machinery. Any
REST client can use it: Spark and Trino through Iceberg Java's `RESTCatalog`, PyIceberg, and this library's
`RestCatalog`.

It is a **server-side-commit** catalog (see [catalogs.md](catalogs.md#two-families)). Clients send requirements and
updates, and the server applies them the way the metastore catalogs here do:

1. read the table's current metadata file;
2. check each `UpdateRequirement` against it (a failure is `409 CommitFailedException`);
3. apply the `MetadataUpdate`s with `TableMetadataBuilder`, the same code `MetastoreCatalog` uses, so a commit gives
   the same metadata whichever side applies it;
4. write the next `{version:05d}-{uuid}.metadata.json` through `IFileIO` with `overwrite: false`;
5. compare-and-swap the table's pointer in the `ICatalogStore`.

If the swap loses (another commit moved the pointer first), the server answers 409 and deletes the file it wrote,
because nothing references it. If the swap fails any other way, the server answers 500 and keeps the file: the swap
may have happened. Clients treat 500 on a commit as "outcome unknown", so they don't retry blindly.

## Running it

```bash
dotnet run --project src/Iceberg.Net.RestServer -- --urls http://localhost:8181 \
  --Iceberg:Warehouse=/data/warehouse --Iceberg:SqlitePath=/data/catalog.db
```

You can also put the configuration in `appsettings.json` under the `Iceberg` section, or in environment variables
(`Iceberg__Warehouse=s3://bucket/wh`, `Iceberg__Auth__Clients__spark=secret`). To embed the server in another
ASP.NET Core application, call `services.AddIcebergRestCatalog()` and then `app.MapIcebergRestCatalog()`. A store
(`ICatalogStore`) or FileIO (`IFileIO`) that you register yourself replaces the default one.

Clients point at the base URI:

| Client | Configuration |
|---|---|
| Spark | `spark.sql.catalog.c=org.apache.iceberg.spark.SparkCatalog`, `.type=rest`, `.uri=http://host:8181`, optionally `.credential=id:secret` |
| PyIceberg | `load_catalog("c", type="rest", uri="http://host:8181", credential="id:secret")` |
| This library | `RestCatalog.ConnectAsync(new RestCatalogOptions { Uri = new("http://host:8181"), Credential = "id:secret" })` |

## Configuration (`Iceberg` section)

| Key | Default | Meaning |
|---|---|---|
| `Warehouse` | — | Root for tables that have no location of their own and whose namespace has no `location` property. A local path or `s3://bucket/prefix`. A new table lands at `{warehouse}/{ns levels…}/{table}`, which is Java `JdbcCatalog`'s layout |
| `Store` | `sqlite` | `sqlite` or `memory` |
| `SqlitePath` | `catalog.db` | The SQLite database file |
| `CatalogName` | `rest` | The `catalog_name` column, so several catalogs can share one database |
| `Prefix` | none | Serve under `/v1/{prefix}/…`; `/v1/config` returns it as an override |
| `DefaultFormatVersion` | 2 | Used when a create sends no `format-version` property |
| `Defaults`, `Overrides` | empty | `/v1/config` `defaults` and `overrides` |
| `TableConfig` | empty | `config` returned with every loaded table, e.g. `s3.endpoint` and `s3.path-style-access`, so clients reach the same object store. Every authenticated client gets it; vended credentials are added per request (see [Credential vending](#credential-vending)) |
| `FileIO` | empty | The server's own S3 settings (`s3.endpoint`, `s3.path-style-access`, `client.region`, `s3.access-key-id`, …). Local paths need none; otherwise the AWS SDK's default credential chain applies |
| `Auth:*` | off | See [Authentication](#authentication) |
| `Authorization:Grants:{principal}:{n}` | none (all allowed) | See [Authorization](#authorization) |
| `ScanPlanning:*` | on | See [Server-side scan planning](#server-side-scan-planning) |
| `CredentialVending:*` | off | See [Credential vending](#credential-vending) |
| `RemoteSigning:*` | off | See [Remote signing](#remote-signing) |
| `Idempotency:Enabled`, `Idempotency:Lifetime`, `Idempotency:PendingTimeout` | on, `00:30:00`, `00:05:00` | See [Idempotency keys](#idempotency-keys) |
| `Locations:Allowed:{n}` | none | Storage roots besides `Warehouse` that namespace, table and view locations may use, e.g. `gs://lake/wh`. See [Storage locations](#storage-locations) |

## Storage locations

The server acts on locations with its own access: it reads and writes metadata there, vends credentials and signs
requests scoped to them, and purges under them. So every location a client chooses is held to three rules, checked when
a namespace is created or its `location` changes, when a table or view is created, registered or staged, and on any
commit that changes a table's `location`, `write.data.path`, `write.metadata.path` or `write.object-storage.path`:

1. **Allowed storage.** Strictly below `Warehouse` or one of `Locations:Allowed`, with no `.` or `..` segments
   and no empty ones (a local file system reads `a//b` as `a/b`). Another bucket, `s3://*`, a bucket root or a local path elsewhere is refused.
2. **Its namespace.** A namespace's root is its `location` property, else `{Warehouse}/{levels}`. A table's or view's
   locations must be below its namespace's root, so each namespace has one storage root; put tables on other storage in
   a namespace rooted there. Namespace roots may nest only along the namespace hierarchy, and a namespace that holds
   tables or views cannot change its `location`.
3. **Its own directories.** No two tables or views, and no table and child namespace, share a directory. A new table
   cannot take a renamed table's old directory, even after a rename into another namespace, and a second name cannot
   be registered onto files a table still owns (unregister it first). The tables in one transaction are checked against
   each other as well as against the catalog. The check reads every table's metadata, so creates and moves cost more as
   the catalog grows.

Register checks the metadata location against rules 1 and 2 before reading anything. Table, view and namespace names
that would reshape a path (`..`, `/`, `\`, empty, control characters) are refused. Every refusal is a 400.

The S3 signer applies the same scoping to requests: a table's own objects only, with no sub-resource beyond what clients
send (multipart, versions, the Java SDK's `x-id`) and no header that changes access or retention (`x-amz-acl`,
`x-amz-grant-*`, `x-amz-object-lock-*`); bucket requests only for the table's buckets, with each operation's own
parameters.

## Storage

`ICatalogStore` is the whole persistence contract. It holds namespaces with their properties, plus a pointer from
each table or view name to its current metadata file. `SwapAsync` is the one concurrency primitive: it moves every
pointer it is given, or none (a pointer with no expected location is a create). Tables and views share names: a name
is either a table or a view, never both.

- **`SqlCatalogStore`** keeps Java `JdbcCatalog`'s tables in the "V1" layout (`jdbc.schema-version=V1`):
  `iceberg_tables(catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location, iceberg_type)`
  and `iceberg_namespace_properties`. Each swap runs as `UPDATE … WHERE metadata_location = @expected` inside one
  database transaction, and a create is an `INSERT` that the primary key guards. On start-up it upgrades a V0
  database (PyIceberg's `SqlCatalog`, older Java) by adding the `iceberg_type` column, as Java does. The same database
  therefore opens in this library's `JdbcCatalog(views: true)`, in Java's `JdbcCatalog` and in PyIceberg's
  `SqlCatalog` (tests: `SqlStoreTests`). Namespaces are stored dot-joined, as all three do, so a namespace level
  cannot contain `.`; a request with one gets a 400.
  - The constructor takes any ADO.NET `Func<DbConnection>`; `SqlCatalogStore.Sqlite(path)` is the ready-made one. Use
    PostgreSQL or SQL Server through it when you embed the server.
  - Several server processes may share one SQLite file on one host, and the database arbitrates between them
    (`Two_servers_over_one_database_serialise_commits`). SQLite on a network file system is not safe; use a server
    database there.
- **`InMemoryCatalogStore`**: a lock and dictionaries. For tests and throwaway servers.

Both stores also keep the server's own state between requests (`IServerStateStore`): `Idempotency-Key` claims and
responses, and scan plans by plan ID, each with an expiry. `SqlCatalogStore` keeps them in two more tables,
`iceberg_rest_idempotency` and `iceberg_rest_plans` (keyed by `catalog_name` too), which Java's `JdbcCatalog` and
PyIceberg ignore; `InMemoryCatalogStore` keeps them in memory. So every server instance over one database replays the
others' idempotency keys and answers their plan IDs (`SharedStateTests`, over one SQLite file and over one shared
in-memory store). A store of your own that does not implement `IServerStateStore` gets process-local state.

Metadata and data files go through `IFileIO`. The default is `ResolvingFileIO`, which sends `s3://` locations to
`S3FileIO` and everything else to the local file system.

## Authentication

With nothing configured, the server is open. Configuring any of these turns on bearer authentication for every
endpoint except the token endpoint:

| Key | Meaning |
|---|---|
| `Auth:Tokens:0`, `:1`, … | Static bearer tokens (the client's `token` property) |
| `Auth:Clients:{id}` = secret | OAuth2 clients for `POST /v1/oauth/tokens` (`credential = id:secret`) |
| `Auth:SigningKey` | HMAC-SHA256 key, at least 32 bytes, for the tokens the server issues. If unset, the key is random per process, so tokens do not survive a restart and are not shared between instances |
| `Auth:TokenLifetime` | Default `01:00:00` |
| `Auth:Authority`, `Auth:Audience` | Also accept JWTs from an external OpenID Connect issuer (its keys come from discovery). `Auth:RequireHttpsMetadata=false` allows an `http://` authority |

`/v1/oauth/tokens` is deprecated in the spec, but Spark, PyIceberg and this library still use it by default:

- It implements the `client_credentials` grant, with the client ID and secret in the form or in HTTP Basic.
- It implements token exchange of a token the server issued, which is how Java clients refresh.
- Errors use the OAuth2 shape (`{"error": "invalid_client", …}`).

Every other unauthenticated request gets a 401 in the spec's `ErrorModel` shape. What an authenticated client may do is
decided by [authorization](#authorization).

## Authorization

Every endpoint asks an `IAuthorizer` before it touches the store, and a refusal is `403 ForbiddenException` in the
`ErrorModel` shape. The check comes first, so a refused principal cannot learn whether a table exists. With no grants
configured every principal may do everything (`AllowAllAuthorizer`). Configuring any grant switches on the static
authorizer:

```
Iceberg__Authorization__Grants__analyst__0=read:sales
Iceberg__Authorization__Grants__etl__0=write:sales.staging
Iceberg__Authorization__Grants__root__0=admin:*
Iceberg__Authorization__Grants__*__0=read:public        # every principal
```

- A **principal** is the token's `sub`: the OAuth2 client ID for tokens from `/v1/oauth/tokens`, the external IdP's
  subject, `static-token` for a static token, and `anonymous` when authentication is off.
- A **grant** is `{level}:{scope}`. The scope is `*` or a dotted namespace or table path, and covers everything beneath
  it (`sales` covers `sales.orders` and `sales.staging.events`).
- The **levels** nest, `read` < `write` < `admin`:

| Level | Allows |
|---|---|
| (browse) | Implied by any grant at, above or below a namespace: load it, check it exists, list into it. Lists only show the namespaces a principal can browse and the tables and views it can read |
| `read` | load, check, plan scans (`plan`, `plan/{id}`, `tasks`), report metrics, read-only vended credentials, signed reads (`GET`/`HEAD`) |
| `write` | create and register tables and views, commit (every table of a multi-table transaction), replace views, rename *to*; read-write vended credentials, signed writes |
| `admin` | create, drop and change namespaces; drop, purge, unregister and rename *from*; register with `overwrite` |

`/v1/config` and the token endpoint need no grant. Grants that do not parse fail at start-up. To use your own model
(OpenFGA, a database of roles), register an `IAuthorizer`; it receives the `ClaimsPrincipal`, a `CatalogPrivilege` and
a `CatalogResource` (namespace plus optional name).

## Server-side scan planning

`POST …/tables/{t}/plan` plans a scan with the library's own planner: the manifests, partitions and column metrics are
pruned exactly as a local `TableScan` prunes them (`TableScan.PlanManifestsAsync`), and the delete files for each data
file come from `DeleteIndex.DeleteFilesFor`, which applies the spec's rules (sequence numbers, partition scope, a DV
replacing position delete files, `file_path` bounds and `referenced_data_file`). Content files are serialized in the
spec's form (`ContentFileJson.ToJson`: lower-case `file-format`, `partition` as an array, metric maps, hex bounds), and
the filter in the spec's expression JSON (`ExpressionJson`).

- **Request fields:** `snapshot-id`, `filter`, `select` (validated), `case-sensitive` (names resolved ignoring case
  when false), `use-snapshot-schema` (bind to the snapshot's schema instead of the current one) and `stats-fields`
  (data files carry only those columns' metrics; without it, none; delete files always carry theirs).
  `min-rows-requested` is ignored.
- **Incremental scans:** `start-snapshot-id` (exclusive) and `end-snapshot-id` (inclusive) plan Java's
  `IncrementalAppendScan`: the data files that the `append` snapshots among the end snapshot's ancestors after the start
  added (read from the manifests those snapshots wrote, entries `ADDED` by one of them), pruned by the filter like any
  scan. Overwrites, deletes and replaces in the range contribute nothing, and no delete files are attached, so rows
  deleted later still come back: it is "what was appended", as in Java. `end-snapshot-id` alone covers every append up
  to it; a start without an end, a start that is not an ancestor of the end, or `snapshot-id` as well is a 400. Large
  incremental plans are split into plan tasks too. The library exposes the same planning as
  `TableScan.PlanAppendsAsync`, and our client sends it with `ScanPlanRequest { StartSnapshotId, EndSnapshotId }`.
  Checked against the change feed's inserts of the append snapshots (`IncrementalScanPlanningTests`) and against Spark
  4 / Iceberg 1.12's incremental reads (`CrossEngineTests.Spark_incremental_reads_match_server_incremental_plans`).
  Java 1.12 itself sends incremental plans to the server only from `table.newScan().appendsBetween(…)`; Spark's
  `start-snapshot-id` reads use `newIncrementalAppendScan()`, which `RESTTable` does not route to the server, so they
  plan on the client.
- **Residuals:** each task's `residual-filter` is the request's filter, or `true` without one. Clients must still
  filter rows; nothing is lost, but no per-partition simplification is done.
- **Plan tasks:** the data manifests that survive pruning are grouped by the file counts in the manifest list.
  One group (at most `ScanPlanning:FilesPerPlanTask`, default 5000 files) is answered inline. More become
  `plan-tasks`, one per group, which clients fetch from `POST …/tasks`. A plan task is self-contained: base64url JSON
  naming the table UUID, the snapshot, the manifest paths and the filter. Any server instance can answer it, and no
  state is kept. It is checked on use: a token for another table, a snapshot that has expired or a manifest that is not
  in the snapshot is `404 NoSuchPlanTaskException`.
- **Plan IDs:** every plan gets a `plan-id`. `GET …/plan/{id}` returns its result and `DELETE` cancels and forgets
  it. Plans are kept in the store (see [Storage](#storage)) for `ScanPlanning:PlanLifetime` (10 minutes) after they
  finish, so any instance over the same database answers any plan ID; an unknown or expired ID is
  `404 NoSuchPlanIdException`. A cancel on another instance forgets the plan, and the instance planning it discards its
  result when it finishes. A plan still `submitted` after `PlanLifetime` (its server went away) is reported `failed`.
- **Asynchronous mode:** with `ScanPlanning:Async=true` the server plans in the background and answers `submitted`;
  clients poll. It mainly exists to test clients' polling. Synchronous is the default.
- **Telling clients:** the four routes are listed in `/v1/config` `endpoints` (`ScanPlanning:Enabled=false` removes
  them). Iceberg Java 1.12 plans on the server only when told: `ScanPlanning:Mode=server` adds
  `scan-planning-mode=server` to every loaded table's `config`, and Java then sends its scans here. PyIceberg 0.12
  needs `scan-planning-mode=server` in its own catalog properties (or in `Defaults`); it does not poll `submitted`
  plans, so keep it synchronous. This library uses `RestCatalog.PlanTableScanAsync` and reads with `ServerSideScan`.

## Credential vending

When the client sends `X-Iceberg-Access-Delegation: vended-credentials` and a vendor is configured, the
`LoadTableResult` of loadTable, createTable and registerTable carries `storage-credentials` scoped to the table's
location, and the same keys in `config` for clients that predate `storage-credentials`. `GET …/tables/{t}/credentials`
(loadCredentials) returns them on their own and is advertised only when vending is on. Commit responses carry no
`config`, so no credentials. Clients must refresh before the credentials expire
(`s3.session-token-expires-at-ms`, `adls.sas-token-expires-at-ms.*`, `gcs.oauth2.token-expires-at`). This library's client does not refresh yet
([catalogs.md](catalogs.md)), so long writers should reload the table.

The S3 vendor (`StsCredentialVendor`) calls STS `AssumeRole` (the Query API over HTTP, signed with the library's
`SigV4Handler`):

| Key | Meaning |
|---|---|
| `CredentialVending:RoleArn` | The role to assume. Setting it turns S3 vending on |
| `CredentialVending:Region`, `:StsEndpoint` | Default `us-east-1` and `https://sts.{region}.amazonaws.com` |
| `CredentialVending:DurationSeconds`, `:ExternalId` | 3600; optional external ID |
| `CredentialVending:AccessKeyId`, `:SecretAccessKey`, `:SessionToken` | The server's own credentials for STS. Default: the `FileIO` keys, else the AWS SDK default chain |

- **Session policy:** each `AssumeRole` sends one that narrows the role to the table's prefix: `s3:GetObject` and
  `ListBucket` on that prefix for principals with `read`, plus `PutObject`, `DeleteObject` and
  `AbortMultipartUpload` for principals with `write` on the table.
- **Returned config:** `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`,
  `s3.session-token-expires-at-ms` and `client.region`. Put `s3.endpoint` and the like in `TableConfig`.
- **Caching and scope:** credentials are cached per (location, access) until a quarter of their lifetime remains.
  Locations other than `s3://` get none from it.

### Azure (ADLS Gen2 / Blob)

`AdlsSasCredentialVendor` vends SAS tokens for tables at `abfs[s]://{container}@{account}.{host}/{path}` (and
`wasb[s]://`), signed with the storage account's shared key. Configuring any account turns it on; with S3 vending on as
well, `CompositeCredentialVendor` combines both, and each answers only for its own schemes.

| Key | Meaning |
|---|---|
| `CredentialVending:Adls:Accounts:{account}` | The account's shared key (base64) |
| `CredentialVending:Adls:Scope` | `directory` (default): a directory SAS (`sr=d`) on the table's path, for hierarchical-namespace (ADLS Gen2) accounts. `container`: a container SAS, for flat accounts; it reaches every table in the container, so use one container per trust boundary |
| `CredentialVending:Adls:DurationSeconds`, `:Version`, `:Protocol` | 3600; the SAS version (`2023-11-03`); `https` to refuse plain HTTP (unset allows both) |

- **Permissions:** `rl` (read, list) for principals with `read`; `racwdl` (plus add, create, write, delete) for
  principals with `write` on the table.
- **Returned config:** `adls.sas-token.{account host}` and `adls.sas-token-expires-at-ms.{account host}` (for example
  `adls.sas-token.mylake.dfs.core.windows.net`), the keys Iceberg Java's `ADLSFileIO` and PyIceberg 0.12 read. Tokens
  are cached like the STS ones.
- **How it is checked:** tokens equal the Azure SDK's for the same inputs (`AzureCredentialVendingTests`, vectors from
  azure-storage-blob 12.31 with the version pinned). Azurite (`mcr.microsoft.com/azure-storage/azurite:3.35.0`, run as
  a one-off container) enforces container-scoped tokens: a writer's uploads and reads, a reader's reads but cannot
  write or delete, and tampered or expired tokens are refused. Azurite has no hierarchical namespace and refuses every
  directory SAS, so the default directory scope is checked only against the SDK vector, not by an emulator. This library
  has no ADLS FileIO, so the test drives Azurite's Blob REST API directly.

### Google Cloud Storage

`GcsDownscopedCredentialVendor` vends downscoped OAuth access tokens for tables at `gs://{bucket}/{path}` (PyIceberg's
`gcs://` too), through Google's [Credential Access Boundary](https://cloud.google.com/iam/docs/downscoping-short-lived-credentials)
flow: the server's own token is exchanged at Google's STS for one that can do less. Configuring a source token (or
registering an `IGcpAccessTokenSource`) turns it on; with S3 or Azure vending on as well, `CompositeCredentialVendor`
combines them, and each answers only for its own schemes. There is no Google SDK dependency: the exchange and the
source tokens are plain HTTP.

| Key | Meaning |
|---|---|
| `CredentialVending:Gcs:AccessToken`, `:AccessTokenExpiresAt` | A fixed source token and its expiry (tests and short runs: Google tokens live an hour at most) |
| `CredentialVending:Gcs:ServiceAccountKeyFile` | A service account JSON key; source tokens are minted with an RS256 JWT assertion (JWT bearer grant to the key's `token_uri`, scope `cloud-platform`) |
| `CredentialVending:Gcs:UseMetadataServer`, `:MetadataEndpoint` | The GCE / GKE / Cloud Run metadata server's default service account (`$GCE_METADATA_HOST`, else `metadata.google.internal`) |
| `CredentialVending:Gcs:StsEndpoint` | `https://sts.googleapis.com/v1/token` |

The first one set wins. Source tokens are cached until a quarter of their lifetime (at least a minute) remains. The
source principal needs the `cloud-platform` scope and must itself hold everything a vended token may do (object admin
on the buckets, for writers).

- **The exchange:** `POST` to the STS endpoint, form-encoded: `grant_type=urn:ietf:params:oauth:grant-type:token-exchange`,
  `subject_token_type` and `requested_token_type` `urn:ietf:params:oauth:token-type:access_token`, `subject_token` (the
  source token) and `options`, the URL-encoded access boundary:

  ```json
  {"accessBoundary":{"accessBoundaryRules":[{
    "availablePermissions":["inRole:roles/storage.objectViewer"],
    "availableResource":"//storage.googleapis.com/projects/_/buckets/lake",
    "availabilityCondition":{"expression":"resource.name.startsWith('projects/_/buckets/lake/objects/wh/db/t/') || api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('wh/db/t/')"}}]}}
  ```

- **Permissions:** `roles/storage.objectViewer` for principals with `read`, `roles/storage.objectAdmin` (create,
  overwrite, delete) for principals with `write` on the table. The condition is the same for both: objects under the
  table's prefix, and list requests whose `prefix` starts with it (listing is checked on the bucket, so the object
  clause alone would refuse it). A list request for the table location without the trailing `/` is refused, as with
  the S3 session policy. Quotes, backslashes and control characters in the path are escaped in the CEL strings.
- **Returned config:** `gcs.oauth2.token` and `gcs.oauth2.token-expires-at` (milliseconds since the epoch), under
  `storage-credentials` with prefix `gs://{bucket}/{path}/` and in `config`. These are the keys of Iceberg Java's
  `GCPProperties` (1.11 parses the expiry with `Long.parseLong` into a `Date`; `GCSFileIO` implements
  `SupportsStorageCredentials` and picks the longest matching prefix) and of PyIceberg 0.12 (`GCS_TOKEN`,
  `GCS_TOKEN_EXPIRES_AT_MS`, passed to PyArrow's `GcsFileSystem` as `access_token` and
  `credential_token_expiration`). Put `gcs.project-id` or `gcs.service.host` in `TableConfig`. The server sends no
  `gcs.oauth2.refresh-credentials-endpoint`, so Java does not refresh the token itself (as with S3 and Azure).
- **Expiry and caching:** the STS response's `expires_in` when present, capped by the source token's expiry (Google's
  STS omits `expires_in` for some source tokens, and a downscoped token never outlives its source); with neither, no
  expiry key is sent and nothing is cached. Tokens are cached per (location, access) until a quarter of their lifetime
  remains.
- **How it is checked, and what is not:** `GcsCredentialVendingTests` assert the exact exchange request against a
  scripted STS (the form fields, the boundary JSON, the CEL condition, the role per access level, escaping),
  response parsing, expiry, caching, errors (which never echo the source token), the metadata-server and
  service-account requests (the JWT is verified with the public key), and an in-process server that combines it with
  the S3 and Azure vendors and returns reader and writer tokens from loadTable, createTable and loadCredentials.
  `GcsCrossEngineTests` load a `gs://` table with PyIceberg 0.12 (`PyArrowFileIO`) and with Iceberg Java 1.11
  (`RESTCatalog`'s `ResolvingFileIO` delegating to `GCSFileIO`, run from the `apache/gravitino-iceberg-rest:1.3.1`
  image's jars) and point `gcs.service.host` at a recording HTTP endpoint: both send the vended token as
  `Authorization: Bearer` for objects under the table, so the key names, prefix and expiry format are what they read.
  **Nothing here talks to Google.** The request shape follows Google's documented format but no real STS has accepted
  it, and no real bucket has enforced a vended token's role or condition (in particular the `objectListPrefix` clause,
  and whether a bucket needs uniform bucket-level access for the condition). `fsouza/fake-gcs-server` ignores
  authentication, so it cannot check either. A first run against a GCP project should confirm: the exchange succeeds,
  a reader's token reads and lists under the table and is refused writes and other prefixes, and a writer's token
  writes and deletes only under it.

### Other storage

For anything else (or a credential broker), register an `IStorageCredentialVendor`; it replaces the built-in ones.

## Remote signing

With `RemoteSigning:Enabled=true` the server signs clients' S3 requests itself, so clients hold no storage credentials
at all: `POST …/tables/{table}/sign` (the spec's `signRequest`) takes a `RemoteSignRequest` (`region`, `uri`, `method`,
`headers`, optional `body` and `provider`) and returns the URI and headers to send, with `Cache-Control: private` for
reads and `no-cache` for writes.

| Key | Meaning |
|---|---|
| `RemoteSigning:Enabled` | Serve and advertise the route; offer signing to clients that ask for it |
| `RemoteSigning:Region` | The region requests are signed for, and returned as `client.region`. Default: the `FileIO`'s `client.region`; a request for another region is a 400 |
| `RemoteSigning:AccessKeyId`, `:SecretAccessKey`, `:SessionToken` | The credentials requests are signed with. Default: the `FileIO`'s `s3.*` keys, else the AWS SDK's default chain |

- **Telling clients:** a client that sends `X-Iceberg-Access-Delegation: remote-signing` (and gets no vended
  credentials instead: vending wins when both are asked for and available) loads S3 tables with
  `s3.remote-signing-enabled=true`, which Iceberg Java 1.12 follows by signing through the table's own `…/sign` route
  (`rest.remote-signing.endpoint`); `s3.signer=S3V4RestSigner` and `s3.signer.endpoint` for PyIceberg, plus
  `py-io-impl` set to its fsspec FileIO, the one that can sign remotely; and `client.region`. Put `s3.endpoint` and the
  like in `TableConfig`. This library's `RestCatalog` signs remotely by itself when it builds the default FileIO
  (`S3FileIOOptions.RequestSigner`: the AWS SDK sends unsigned, `UNSIGNED-PAYLOAD` requests that the catalog has
  signed); a custom FileIO factory can call `RestCatalog.RemoteSigner`.
- **What is signed:** SigV4 for `s3`, with S3's single path encoding, over the client's headers except those that
  change between attempts (`amz-sdk-*`, `range`, `expect`, `user-agent` …). The payload is `UNSIGNED-PAYLOAD`, the
  SHA-256 of `body` when one is sent (the spec asks for it on `DeleteObjects`), or `STREAMING-UNSIGNED-PAYLOAD-TRAILER`
  when the client uploads that way. A `host` header is never taken from the client: it comes from the URI.
- **What may be signed:** the URI must address the configured `FileIO:s3.endpoint` (path-style or virtual-hosted), or
  AWS S3 over HTTPS when none is set. Object requests must name a key under the table's location, `write.data.path` or
  `write.metadata.path` (no `.` or `..` segments), and so must a copy's `x-amz-copy-source`. Bucket requests are limited
  to listing with such a `prefix`, `DeleteObjects` whose body names only such keys, `GetBucketLocation` and
  `HeadBucket`. Anything else is a 403, as is a write from a principal without `write` on the table.
- **How it is checked:** the S3 signer matches AWS's documented "GET Object" SigV4 example, and the authorization rules
  have their own tests (`RemoteSigningTests`). End to end, against a moto that checks signatures (a one-off
  `motoserver/moto` container with authentication on, bootstrapped with an IAM user: `AuthenticatingMoto`; the shared
  emulator accepts any signature): our client with no keys creates, appends, deletes, reads (locally and through
  server-side planning) and purges a table, and a reader's client cannot write; Spark 4 / Iceberg 1.12 (with
  `iceberg-aws-bundle`, no AWS credentials in the container) creates, inserts, deletes merge-on-read and reads, and reads
  our append; PyIceberg 0.12 appends to and reads our table (`CrossEngineTests`).
- **Limits:** signing is per table, for tables that exist. Spark's `DROP TABLE … PURGE` drops the table first and then
  deletes its files through the table's FileIO, so those deletes cannot be signed and the statement fails after the
  drop; drop with purge through the server instead (`purgeRequested=true`, which this library's client sends). The
  same goes for writes to a staged (not yet committed) create. Only S3 is signed (`provider` other than `s3` is a 400).

## Idempotency keys

Mutating requests (`POST`, `DELETE`) that carry an `Idempotency-Key` header are deduplicated, as the spec describes:

- **Kept and replayed:** the first request runs, and its final response is kept and replayed to any later request with
  the same key from the same principal, without running again. That covers 2xx and 4xx, a 409 included. Replays
  carry `Idempotent-Replayed: true`.
- **Not kept:** a 5xx, so a retry runs again. A duplicate that arrives while the first is still running waits for it.
- **Refused:** the same key with a different method, path or body is a 422. A key longer than 128 characters is a
  400.
- **Not covered:** planning and signing routes are excluded (they change nothing), and so is the token endpoint.

`/v1/config` advertises `idempotency-key-lifetime` (`Idempotency:Lifetime`, default `PT30M`); responses are kept that
long plus a minute. Clients only retry mutations when it is advertised. This library's `RestCatalog` then resends a
commit whose response was lost with the same key, and gets the original answer instead of
`CommitStateUnknownException` (`IdempotencyTests`).

Keys are claimed in the store (see [Storage](#storage)), so server instances over one database share them: a retry
that a load balancer sends to another instance is replayed there, and a duplicate waits for the first request even when
another instance runs it (`SharedStateTests`, including a commit whose response is lost on one server and retried on
the other). A claim whose request never finished, because its server went away mid-request, is treated like a 5xx
after `Idempotency:PendingTimeout` (5 minutes): the next request with the key runs.

## Endpoints

| Operation | Route | Notes |
|---|---|---|
| getConfig | `GET /v1/config` | `defaults`, `overrides` (with `prefix`), `endpoints` listing every route below. `warehouse` is accepted and ignored (one catalog per server) |
| getToken | `POST /v1/oauth/tokens` | see above |
| listNamespaces | `GET …/namespaces?parent=` | direct children; parents are implied (`a.b` lists under `a`); paged |
| createNamespace / loadNamespaceMetadata / namespaceExists / dropNamespace | `POST …/namespaces`, `GET`/`HEAD`/`DELETE …/namespaces/{ns}` | Levels are joined with `0x1F` (`%1F`). A drop with tables, views or child namespaces is `409 NamespaceNotEmptyException` |
| updateProperties | `POST …/namespaces/{ns}/properties` | `updated`, `removed`, `missing`; a key that is both set and removed is a 422 |
| listTables | `GET …/namespaces/{ns}/tables` | `pageToken` / `pageSize` (keyset paging) |
| createTable | `POST …/namespaces/{ns}/tables` | Fresh field, partition-field and sort-order IDs, as Java assigns them. `format-version` is taken from the properties. `stage-create: true` returns the metadata and writes nothing |
| loadTable | `GET …/tables/{t}` | `ETag` header; `If-None-Match` gets a 304; `snapshots=refs` returns only snapshots that a ref points at |
| tableExists | `HEAD …/tables/{t}` | |
| updateTable | `POST …/tables/{t}` | requirements and updates as above. With `assert-create` it commits a staged create: fresh metadata built from the create-shaped updates (schema, spec, order, location, format version, UUID), then every update applied on top |
| dropTable | `DELETE …/tables/{t}?purgeRequested=` | With purge, the entry is removed first, then every reachable file under the table's directories is deleted: metadata files, manifest lists, manifests, data, deletes and statistics (data and deletes only with `gc.enabled`) |
| registerTable | `POST …/namespaces/{ns}/register` | `overwrite` supported |
| unregisterTable | `POST …/tables/{t}/unregister` | Returns the last metadata; files are kept |
| reportMetrics | `POST …/tables/{t}/metrics` | Accepted and discarded (204) |
| planTableScan / fetchPlanningResult / cancelPlanning / fetchScanTasks | `POST …/tables/{t}/plan`, `GET`/`DELETE …/plan/{id}`, `POST …/tables/{t}/tasks` | See [Server-side scan planning](#server-side-scan-planning) |
| loadCredentials | `GET …/tables/{t}/credentials` | Only with a credential vendor; see [Credential vending](#credential-vending) |
| signRequest | `POST …/tables/{t}/sign` | Only with remote signing on; see [Remote signing](#remote-signing) |
| renameTable | `POST …/tables/rename` | Atomic in either store, across namespaces too |
| commitTransaction | `POST …/transactions/commit` | Every requirement is checked and every file written first; then all pointers move in one `SwapAsync`, or none do. Staged creates may take part |
| listViews / createView / loadView / viewExists / replaceView / dropView / renameView | `…/views…`, `POST …/views/rename` | View updates follow Java's `ViewMetadata.Builder`: identical schemas and versions are reused, `-1` means "added in this commit", and history is trimmed to `version.history.num-entries` |

Errors always have the `{"error": {"message", "type", "code"}}` shape:

| Status | Types |
|---|---|
| 400 | `BadRequestException`: malformed JSON, missing fields, or updates that cannot apply (unknown IDs, downgrades) |
| 401 | `NotAuthorizedException` |
| 403 | `ForbiddenException`: the principal lacks the grant |
| 404 | `NoSuchNamespaceException`, `NoSuchTableException`, `NoSuchViewException`, `NoSuchPlanIdException`, `NoSuchPlanTaskException` |
| 406 | `UnsupportedOperationException` |
| 409 | `AlreadyExistsException`, `CommitFailedException`, `NamespaceNotEmptyException` |
| 422 | `UnprocessableEntityException` |
| 500 | anything else, logged |

## OpenAPI document

The server describes itself at `GET /openapi/v1.json` (no authentication, like the spec it follows). Paths, methods and
path parameters come from the routes; each operation's `operationId`, the query and header parameters its handler
reads, and the statuses it can answer with come from one table, `OpenApiOperations.ByRoute`. Change a route or a handler's
inputs or errors, and update that table in the same change.

`OpenApiConformanceTests` diffs the document against the reference spec at the pinned ref
(`docs/spec/open-api/rest-catalog-open-api.json`, the same ref as `docs/spec/sections.json`): every operation, route,
success status, functional error (400, 401, 403, 404, 406, 409, 422; 419, 503 and 5XX are not compared) and query or
header parameter must match. The reviewed differences, each with its reason, are in `docs/spec/openapi-deviations.json`;
an entry whose difference has gone away fails the test too. When the pinned ref moves, refresh the copy with
`uv run --with pyyaml --no-project python -I tools/openapi_reference.py`.

## Not implemented

- Remote signing for anything but S3, the spec's newer `remote-signing-config` field (Iceberg 1.12 works without it),
  and signing for staged creates or just-dropped tables (see [Remote signing](#remote-signing)).
- Per-partition residual simplification, and `storage-credentials` in planning responses (clients use the table's).
- GCS vending checked against Google itself (the exchange and the clients' use of the tokens are checked offline; see
  [Google Cloud Storage](#google-cloud-storage)), and an emulator-checked ADLS directory SAS (Azurite cannot). Register
  an `IStorageCredentialVendor` for other storage.
- Functions, `register-view`, and a custom `namespace-separator`.
- Audit logging and storing metrics reports.
- Several warehouses behind one server. Run one server per warehouse, or share a database with a different
  `CatalogName`.

## Tests

| Where | What |
|---|---|
| `tests/Iceberg.Net.RestServer.Tests` | `CatalogContractTests` run the catalog contract (namespaces, round trip, concurrent writers, stale requirements, time travel, rename/register/purge, v3, multi-table transactions, views) through `RestCatalog` against the server under `WebApplicationFactory`, over both stores. `ProtocolTests` cover status codes, the ErrorModel, paging, ETag, staged creates and registration. `ConcurrencyTests` cover two racing commits (exactly one 409), a lost compare-and-swap (409, its file removed) and two servers over one database. `AuthTests` and `SqlStoreTests` cover the rest. `ScanPlanningTests` read server plans with our client and compare them with local scans (v2/v3, position deletes, DVs, equality deletes, filters, time travel, plan tasks, async), plus the protocol (plan IDs, plan-task tokens, `stats-fields`, case-insensitivity, 400/404). `IncrementalScanPlanningTests` check incremental plans against the change feed. `AuthorizationTests`, `CredentialVendingTests` (fake vendor, scripted STS, and moto's STS when the emulators run: the vended keys are then used for S3), `AzureCredentialVendingTests` (Azure SDK vectors; Azurite when its image is present), `GcsCredentialVendingTests` (the token exchange against a scripted STS, the source tokens, the combined vendors in the server), `RemoteSigningTests` (AWS's example, the authorization rules, and our client against `AuthenticatingMoto`), `IdempotencyTests` (replays, 422, concurrent duplicates, a lost commit response) and `SharedStateTests` (two servers sharing keys and plan IDs; the state store contract) cover the rest |
| `CrossEngineTests` (same project) | The server on a Kestrel port. **PyIceberg 0.12** (OAuth2 client credentials) creates, appends, evolves and reads tables interleaved with our client, including a staged create transaction, reads through server-side scan planning (`scan-planning-mode=server`, split into plan tasks, with position deletes), and appends and reads through remote signing. **Spark 4.0.1 / Iceberg 1.12** (apache/spark image on the host network) creates, inserts, deletes with merge-on-read position deletes, reads our appends, alters and inserts into a table we created, and (told `scan-planning-mode=server` by the table config) reads tables with v2 position deletes and v3 DVs through server-side planning and plan tasks, matches our incremental plans with its incremental reads, and reads and writes S3 through remote signing (with `iceberg-aws-bundle-1.12.0.jar` next to the runtime jar). They need `ICEBERG_PYTHON` and the Spark oracle (`emulation/up.sh --spark`, or `ICEBERG_SPARK_JAR`), and skip without them; the signing tests also need Docker and the `motoserver/moto:5.2.4` image. `ICEBERG_SPARK_LOG=<file>` keeps the full Spark output of a failed run |
| `GcsCrossEngineTests` (same project) | PyIceberg 0.12 and Iceberg Java 1.11 (`GCSFileIO`, from the `apache/gravitino-iceberg-rest:1.3.1` image's jars, `docker run --network host`) load a `gs://` table with vended credentials and send the token to a recording GCS endpoint. Need `ICEBERG_PYTHON` and that image respectively; skip without them |
| `tests/Iceberg.Net.RestProtocol.Tests` | Operation by operation against the OpenAPI: each operation's success response and its documented errors (status, error model, type), the authorization matrix, the client's status mapping against a scripted server, and the error contract across every REST server the suite can reach |
| `tests/Iceberg.Net.Regression.Tests` | The `dotnet-rest` catalog kind (`DotnetRestServer.cs`): the server on a free port with its warehouse on moto. It runs `CatalogConformanceTests`, `ViewConformanceTests`, the PyIceberg `CrossEngineTests` and the soak test |

```bash
ICEBERG_PYTHON=interop/.venv/bin/python dotnet test tests/Iceberg.Net.RestServer.Tests
```
