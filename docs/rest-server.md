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
| `TableConfig` | empty | `config` returned with every loaded table, e.g. `s3.endpoint` and `s3.path-style-access`, so clients reach the same object store. Every authenticated client gets it; it is not credential vending |
| `FileIO` | empty | The server's own S3 settings (`s3.endpoint`, `s3.path-style-access`, `client.region`, `s3.access-key-id`, …). Local paths need none; otherwise the AWS SDK's default credential chain applies |
| `Auth:*` | off | See [Authentication](#authentication) |

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

Every other unauthenticated request gets a 401 in the spec's `ErrorModel` shape. Authentication is all-or-nothing:
there is no authorization model, so any authenticated client can do anything.

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
| renameTable | `POST …/tables/rename` | Atomic in either store, across namespaces too |
| commitTransaction | `POST …/transactions/commit` | Every requirement is checked and every file written first; then all pointers move in one `SwapAsync`, or none do. Staged creates may take part |
| listViews / createView / loadView / viewExists / replaceView / dropView / renameView | `…/views…`, `POST …/views/rename` | View updates follow Java's `ViewMetadata.Builder`: identical schemas and versions are reused, `-1` means "added in this commit", and history is trimmed to `version.history.num-entries` |

Errors always have the `{"error": {"message", "type", "code"}}` shape:

| Status | Types |
|---|---|
| 400 | `BadRequestException`: malformed JSON, missing fields, or updates that cannot apply (unknown IDs, downgrades) |
| 404 | `NoSuchNamespaceException`, `NoSuchTableException`, `NoSuchViewException` |
| 406 | `UnsupportedOperationException` |
| 409 | `AlreadyExistsException`, `CommitFailedException`, `NamespaceNotEmptyException` |
| 422 | `UnprocessableEntityException` |
| 500 | anything else, logged |

## Not implemented

- **Server-side scan planning** (`plan`, `tasks`). The client half exists (`RestCatalog.PlanTableScanAsync`); clients
  plan their own scans here.
- **Credential vending** (`loadCredentials`, `storage-credentials`, `X-Iceberg-Access-Delegation`): the header is
  ignored. Clients bring their own storage credentials, or get static settings from `TableConfig`.
- Remote signing (`/sign`).
- `Idempotency-Key` deduplication. The server does not advertise `idempotency-key-lifetime`, so clients do not replay
  commits after transport errors.
- Functions, `register-view`, and a custom `namespace-separator`.
- Authorization (roles, grants), audit logging, and storing metrics reports.
- Several warehouses behind one server. Run one server per warehouse, or share a database with a different
  `CatalogName`.

## Tests

| Where | What |
|---|---|
| `tests/Iceberg.Net.RestServer.Tests` | `CatalogContractTests` run the catalog contract (namespaces, round trip, concurrent writers, stale requirements, time travel, rename/register/purge, v3, multi-table transactions, views) through `RestCatalog` against the server under `WebApplicationFactory`, over both stores. `ProtocolTests` cover status codes, the ErrorModel, paging, ETag, staged creates and registration. `ConcurrencyTests` cover two racing commits (exactly one 409), a lost compare-and-swap (409, its file removed) and two servers over one database. `AuthTests` and `SqlStoreTests` cover the rest |
| `CrossEngineTests` (same project) | The server on a Kestrel port. **PyIceberg 0.12** (OAuth2 client credentials) creates, appends, evolves and reads tables interleaved with our client, including a staged create transaction. **Spark 4.0.1 / Iceberg 1.12** (apache/spark image on the host network) creates, inserts, deletes with merge-on-read position deletes, reads our appends, alters and inserts into a table we created. They need `ICEBERG_PYTHON` and the Spark oracle (`emulation/up.sh --spark`, or `ICEBERG_SPARK_JAR`), and skip without them |
| `tests/Iceberg.Net.Regression.Tests` | The `dotnet-rest` catalog kind (`DotnetRestServer.cs`): the server on a free port with its warehouse on moto. It runs `CatalogConformanceTests`, `ViewConformanceTests`, the PyIceberg `CrossEngineTests` and the soak test |

```bash
ICEBERG_PYTHON=interop/.venv/bin/python dotnet test tests/Iceberg.Net.RestServer.Tests
```
