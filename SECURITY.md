# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| Latest  | :white_check_mark: |

## Reporting a Vulnerability

We take the security of `rtkelly-labs/Iceberg.NET` seriously. If you believe you have found a security vulnerability, please report it responsibly:

1. **Do NOT open a public GitHub issue.**
2. Report vulnerabilities privately via **GitHub private vulnerability reporting** on the repository, or email the maintainer at `ryan@ryankelly.dev`.
3. Please include detailed steps to reproduce the issue, along with any proof-of-concept scripts.

We will acknowledge receipt of your report within 48 hours and work on a fix promptly.

## Threat model

This section says what Lake.NET defends against and what it trusts, so you can deploy it inside the right boundary.
The details of each control are in [docs/rest-server.md](docs/rest-server.md) and [docs/catalogs.md](docs/catalogs.md);
the open items are tracked in [docs/lake/verification/gap-review-2026-10.md](docs/lake/verification/gap-review-2026-10.md).

### Assets

- **Table data and metadata** in object storage (S3, R2, ADLS, GCS) or on a local disk.
- **The catalog database** (SQLite, SQL Server, PostgreSQL, JDBC): table pointers, namespaces, idempotency records,
  scan plans and, for DuckLake, encryption keys.
- **Credentials:** the REST server's own storage credentials (`FileIO`, STS role, Azure shared keys, GCP source
  token), its token-signing key, OAuth client secrets and static tokens, and the short-lived credentials it vends.

### The REST catalog server is open by default

With no `Auth:*` setting, `Iceberg.Net.RestServer` authenticates nobody: every request is `anonymous`, and with no
grants `AllowAllAuthorizer` lets it do everything, including dropping and purging tables and receiving vended
credentials. That is for local development and tests only. On any network:

- turn authentication on (`Auth:Clients`, `Auth:Tokens` or `Auth:Authority` + `Auth:Audience`); the server then
  refuses to start without grants unless `Authorization:AllowAll=true` says so;
- configure grants per principal (`read`/`write`/`admin` on a namespace or table);
- serve it over TLS (a reverse proxy is fine) and keep `Auth:SigningKey` secret and the same on every instance.

### Trust boundaries

| Boundary | What crosses it | What protects it |
|---|---|---|
| Client → REST server | Bearer tokens, client secrets, table names and locations, commits, files referenced by commits | Authentication; a grant check before every store access; locations confined to allowed roots, the namespace and the table's own directories, with no policy wildcards (`*`, `?`, `$`); referenced manifests and statistics confined to the table; a 16 MiB body cap and 64-level JSON depth enforced before anything reads the body; a per-address limit on the token endpoint; errors that name only what the caller can read |
| External IdP → REST server | JWTs | Signature from the IdP's published keys, issuer, audience and expiry checked; the IdP's principals are `external/{sub}`, never the server's own clients |
| REST server → object storage | The server's own credentials | Every read the server makes for a client (metadata, scan planning) is confined to the table's directories |
| REST server → client (vended credentials, signing) | Short-lived storage credentials or signatures | Scoped to the table's location and the principal's level (below); the signer signs only the table's objects, an allow-list of headers and the methods tables use |
| REST server → client (configuration) | `s3.endpoint`, `prefix`, `signer.uri`, table locations | The client refuses a server-chosen endpoint it would sign for with its own AWS keys unless it is an S3 service endpoint or loopback, a signer on another origin, a `prefix` that changes the request path, and sends credentials only over https or to loopback; it follows no redirects |
| Anyone → catalog database | Every pointer and, for DuckLake, every key | Not defended by Lake.NET: whoever can write the catalog database controls the lake (below) |

### Scope of vended credentials

Vended credentials are only as narrow as the storage service allows:

- **S3 (STS):** a session policy limited to the table's prefix: read and list for `read`, plus put and delete for `write`.
  The role's own policy is the ceiling.
- **Azure:** with `CredentialVending:Adls:Scope=directory` (the default, for hierarchical-namespace accounts) a
  directory SAS on the table's path. With `Scope=container` (flat accounts) the SAS covers **the whole container**,
  every other table in it included: use one container per trust boundary.
- **GCS:** a downscoped token whose access boundary is the table's prefix.
- **Cloudflare Basin Catalog** (formerly R2 Data Catalog, a third-party catalog `RestCatalogOptions.ForBasinCatalog`
  connects to): its vended credentials carry the API token's R2 permissions, which are **bucket-wide**, not per table
  ([docs/lake/reference/cloudflare.md](docs/lake/reference/cloudflare.md)). A reader holding a read-write token can
  write anywhere in the bucket. Use a token per workload and trust boundary, or run Lake.NET's own server for per-table
  scoping.
- Vended credentials live until they expire (an hour by default); revoking a grant does not revoke credentials
  already vended.

### DuckLake: the catalog database is fully trusted

This applies to Lake.NET's DuckLake support (developed on its own branch stack, not yet on `main`) as it does to DuckDB.
A DuckLake catalog database is not a service with its own access control: Lake.NET (like DuckDB) reads table and file
paths, inlined data, schema and change records from it and acts on them. Anyone who can write the catalog database
can make a reader read, and maintenance delete, files of their choosing, and can rewrite any table. Grant catalog
write access only to the writers you trust with all the data, and keep the catalog database and the data bucket in one
trust boundary. Rows read from the catalog are decoded as untrusted bytes (format errors, not crashes); confining the
paths they name is open (gap review S10, S11).

### Keys in the catalog

An encrypted DuckLake stores every file's key in the catalog database (`encryption_key`), as DuckDB does. Read access
to the catalog database is therefore read access to the data: encryption protects data at rest in the bucket from
whoever can read the bucket but not the catalog, nothing more. Back up, replicate and grant the catalog accordingly.
Tests of the failure cases (wrong key, missing key, tampering) are open (gap review S12).

### Out of scope

Denial of service by an authenticated principal (large tables, expensive plans; only the token endpoint is rate
limited), the security of the storage service and IdP themselves, and side channels on the host.
