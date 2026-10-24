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
| `Idempotency:Enabled`, `Idempotency:Lifetime` | on, `00:30:00` | See [Idempotency keys](#idempotency-keys) |

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
| `read` | load, check, plan scans (`plan`, `plan/{id}`, `tasks`), report metrics, read-only vended credentials |
| `write` | create and register tables and views, commit (every table of a multi-table transaction), replace views, rename *to*; read-write vended credentials |
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
  `min-rows-requested` is ignored. Incremental scans (`start-snapshot-id` / `end-snapshot-id`) are a 406.
- **Residuals:** each task's `residual-filter` is the request's filter, or `true` without one. Clients must still
  filter rows; nothing is lost, but no per-partition simplification is done.
- **Plan tasks:** the data manifests that survive pruning are grouped by the file counts in the manifest list.
  One group (at most `ScanPlanning:FilesPerPlanTask`, default 5000 files) is answered inline. More become
  `plan-tasks`, one per group, which clients fetch from `POST …/tasks`. A plan task is self-contained: base64url JSON
  naming the table UUID, the snapshot, the manifest paths and the filter. Any server instance can answer it, and no
  state is kept. It is checked on use: a token for another table, a snapshot that has expired or a manifest that is not
  in the snapshot is `404 NoSuchPlanTaskException`.
- **Plan IDs:** every plan gets a `plan-id`. `GET …/plan/{id}` returns its result and `DELETE` cancels and forgets
  it. Results stay in memory for `ScanPlanning:PlanLifetime` (10 minutes) after they finish; an unknown or expired ID
  is `404 NoSuchPlanIdException`.
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
`config`, so no credentials.

The built-in vendor calls STS `AssumeRole` (the Query API over HTTP, signed with the library's `SigV4Handler`):

| Key | Meaning |
|---|---|
| `CredentialVending:RoleArn` | The role to assume. Setting it turns vending on |
| `CredentialVending:Region`, `:StsEndpoint` | Default `us-east-1` and `https://sts.{region}.amazonaws.com` |
| `CredentialVending:DurationSeconds`, `:ExternalId` | 3600; optional external ID |
| `CredentialVending:AccessKeyId`, `:SecretAccessKey`, `:SessionToken` | The server's own credentials for STS. Default: the `FileIO` keys, else the AWS SDK default chain |

- **Session policy:** each `AssumeRole` sends one that narrows the role to the table's prefix: `s3:GetObject` and
  `ListBucket` on that prefix for principals with `read`, plus `PutObject`, `DeleteObject` and
  `AbortMultipartUpload` for principals with `write` on the table.
- **Returned config:** `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`,
  `s3.session-token-expires-at-ms` and `client.region`. Put `s3.endpoint` and the like in `TableConfig`.
- **Caching and scope:** credentials are cached per (location, access) until a quarter of their lifetime remains.
  Locations other than `s3://` get none.
- **Your own vendor:** register an `IStorageCredentialVendor` (Azure SAS, GCS, a broker) to replace this one.

Clients must refresh before `s3.session-token-expires-at-ms`. This library's client does not refresh yet
([catalogs.md](catalogs.md)), so long writers should reload the table.

## Idempotency keys

Mutating requests (`POST`, `DELETE`) that carry an `Idempotency-Key` header are deduplicated, as the spec describes:

- **Kept and replayed:** the first request runs, and its final response is kept and replayed to any later request with
  the same key from the same principal, without running again. That covers 2xx and 4xx, a 409 included. Replays
  carry `Idempotent-Replayed: true`.
- **Not kept:** a 5xx, so a retry runs again. A duplicate that arrives while the first is still running waits for it.
- **Refused:** the same key with a different method, path or body is a 422. A key longer than 128 characters is a
  400.
- **Not covered:** planning routes are excluded (they change nothing), and so is the token endpoint.

`/v1/config` advertises `idempotency-key-lifetime` (`Idempotency:Lifetime`, default `PT30M`); responses are kept that
long plus a minute. Clients only retry mutations when it is advertised. This library's `RestCatalog` then resends a
commit whose response was lost with the same key, and gets the original answer instead of
`CommitStateUnknownException` (`IdempotencyTests`). Keys live in the process's memory, so servers sharing one
database do not share them; behind a load balancer, use sticky routing per client, or turn it off.

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
| dropTable | `DELETE …/tables/{t}?purgeRequested=` | With purge, the entry is removed first, then every reachable file is deleted: metadata files, manifest lists, manifests, data, deletes and statistics |
| registerTable | `POST …/namespaces/{ns}/register` | `overwrite` supported |
| unregisterTable | `POST …/tables/{t}/unregister` | Returns the last metadata; files are kept |
| reportMetrics | `POST …/tables/{t}/metrics` | Accepted and discarded (204) |
| planTableScan / fetchPlanningResult / cancelPlanning / fetchScanTasks | `POST …/tables/{t}/plan`, `GET`/`DELETE …/plan/{id}`, `POST …/tables/{t}/tasks` | See [Server-side scan planning](#server-side-scan-planning) |
| loadCredentials | `GET …/tables/{t}/credentials` | Only with a credential vendor; see [Credential vending](#credential-vending) |
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
| 406 | `UnsupportedOperationException` (e.g. incremental scan planning) |
| 409 | `AlreadyExistsException`, `CommitFailedException`, `NamespaceNotEmptyException` |
| 422 | `UnprocessableEntityException` |
| 500 | anything else, logged |

## Not implemented

- Remote signing (`/sign`). Vended credentials cover the same need for S3.
- Incremental scan planning (`start-snapshot-id` / `end-snapshot-id`), per-partition residual simplification, and
  `storage-credentials` in planning responses (clients use the table's).
- Credentials for non-S3 storage. Register an `IStorageCredentialVendor` for them.
- Idempotency keys shared between server instances. They are per process.
- Durable plan IDs. Plan tasks are stateless, but `plan/{id}` results live in the process that made them.
- Functions, `register-view`, and a custom `namespace-separator`.
- Audit logging and storing metrics reports.
- Several warehouses behind one server. Run one server per warehouse, or share a database with a different
  `CatalogName`.

## Tests

| Where | What |
|---|---|
| `tests/Iceberg.Net.RestServer.Tests` | `CatalogContractTests` run the catalog contract (namespaces, round trip, concurrent writers, stale requirements, time travel, rename/register/purge, v3, multi-table transactions, views) through `RestCatalog` against the server under `WebApplicationFactory`, over both stores. `ProtocolTests` cover status codes, the ErrorModel, paging, ETag, staged creates and registration. `ConcurrencyTests` cover two racing commits (exactly one 409), a lost compare-and-swap (409, its file removed) and two servers over one database. `AuthTests` and `SqlStoreTests` cover the rest. `ScanPlanningTests` read server plans with our client and compare them with local scans (v2/v3, position deletes, DVs, equality deletes, filters, time travel, plan tasks, async), plus the protocol (plan IDs, plan-task tokens, `stats-fields`, case-insensitivity, 400/404/406). `AuthorizationTests`, `CredentialVendingTests` (fake vendor, scripted STS, and moto's STS when the emulators run: the vended keys are then used for S3) and `IdempotencyTests` (replays, 422, concurrent duplicates, a lost commit response) cover the rest |
| `CrossEngineTests` (same project) | The server on a Kestrel port. **PyIceberg 0.12** (OAuth2 client credentials) creates, appends, evolves and reads tables interleaved with our client, including a staged create transaction, and reads through server-side scan planning (`scan-planning-mode=server`, split into plan tasks, with position deletes). **Spark 4.0.1 / Iceberg 1.12** (apache/spark image on the host network) creates, inserts, deletes with merge-on-read position deletes, reads our appends, alters and inserts into a table we created, and (told `scan-planning-mode=server` by the table config) reads tables with v2 position deletes and v3 DVs through server-side planning and plan tasks. They need `ICEBERG_PYTHON` and the Spark oracle (`emulation/up.sh --spark`, or `ICEBERG_SPARK_JAR`), and skip without them |
| `tests/Iceberg.Net.Regression.Tests` | The `dotnet-rest` catalog kind (`DotnetRestServer.cs`): the server on a free port with its warehouse on moto. It runs `CatalogConformanceTests`, `ViewConformanceTests`, the PyIceberg `CrossEngineTests` and the soak test |

```bash
ICEBERG_PYTHON=interop/.venv/bin/python dotnet test tests/Iceberg.Net.RestServer.Tests
```
