# REST server operation before 0.0.1

The full-stack scope includes a runnable server as well as its protocol implementation. This document specifies
current operating boundaries. It does not turn unverified cloud paths into supported production integrations.
The detailed protocol/configuration reference is [rest-server.md](rest-server.md).

## Deployment artifact and configuration

The release build publishes a framework-dependent `Iceberg.Net.RestServer.<version>.tar.gz` artifact alongside the
11 library packages. The host needs .NET 10 and a platform supported by the ParquetSharp native runtime. Local checks:

```bash
dotnet publish src/Iceberg.Net.RestServer -c Release -o temp/server
python3 tools/server-smoke.py temp/server temp
# Configure before binding a non-loopback address:
dotnet temp/server/Iceberg.Net.RestServer.dll --urls http://127.0.0.1:8181
```

The smoke test creates a namespace/table through the published executable, restarts it with the same SQLite state,
then checks table identity and replay of a stored idempotent request. The release workflow runs this check.
Use a persistent, absolute warehouse path and `Iceberg:SqlitePath`, not files inside the replaceable deployment
folder. A source deployment can use the same publish command; an artifact alone does not configure storage or IAM.

## Single instance and multiple instances

- `memory` state is disposable. Use it for local evaluation, not durable catalog storage.
- The built-in persistent store is SQLite. SQLite is verified; a generic ADO.NET factory is not a guarantee for
  every SQL dialect. Protect the database files and use storage with SQLite's required locking semantics.
- Tests run multiple server instances over one SQLite database. This is evidence for shared pointer, plan and
  idempotency behavior, not certification of arbitrary network filesystems or a multi-region database.
- Instances must share the catalog name, database, warehouse/location policy, grants and credential-vendor settings.
  Configure the same stable `Auth:SigningKey`, issuer and token policy across instances. The default random signing
  key changes on restart and cannot validate tokens issued by a different process.
- The server has one warehouse. Separate deployments/catalog names are the boundary for multiple warehouses.

## Bounded idempotency state

`Idempotency:MaxClaimsPerPrincipal` defaults to 1,024 unexpired keys, counting pending and completed requests. The
budget is enforced atomically in both stores and is shared across instances using the same SQL catalog. Different
principals have separate budgets; anonymous callers share the anonymous principal's budget.

At capacity, a new key gets HTTP 429 with an ErrorModel and `Retry-After: 60`; its mutation is not applied. Existing
keys still replay, and unexpired responses are never evicted to make room. Expiry/release makes room for later
requests. Retention includes the advertised lifetime plus the existing grace period. Each retained response is at
most 1 MiB; this per-principal count is not a global memory/disk limit. Set the limit for the number of mutating
requests a principal must retain in that interval. Custom state stores must implement the bounded claim overload.

Credential fields are removed before storage. A replayed create/register is followed by a fresh table load in our
client. `RestCatalog.LoadCredentialsAsync` explicitly requests refreshed prefix/config credentials; reload a table
with `LoadTableAsync` to get a new table handle and FileIO. Existing handles/in-flight operations are unchanged.
Automatic background credential renewal is not provided.

Retained replay is not a general exactly-once transaction across the catalog, external storage and response state.
A process dying after a mutation applies but before its response is stored can leave an uncertain outcome. Pending
claims time out; callers must reconcile unknown commits instead of assuming a failed response means no change.
The library's unknown-commit contract and immutable metadata rules still apply.

## Backup and recovery

1. Suspend catalog writers and maintenance/GC while establishing a recovery point. Preserve the corresponding
   warehouse objects (metadata, manifests, data, deletes and statistics), including their locations and permissions.
2. Use SQLite's online backup API or CLI `.backup` command, not an uncoordinated copy of a live database file.
   SQLite may have committed state in WAL files. For example, with the SQLite CLI installed:

   ```bash
   sqlite3 /absolute/catalog.db ".backup '/absolute/backups/catalog.db'"
   ```

3. Retain the matching configuration/signing keys separately in the deployment's secret/configuration system. The
   catalog database is not a backup of these settings or of S3/local warehouse contents.
4. Restore into an isolated deployment first. Restore the database and referenced warehouse objects together;
   block writers/GC until pointer loads, table validation, expected reads and identity/grant checks succeed.
5. Run `TableValidator` against restored tables before reopening writes. An old database backup can refer to files
   later removed by retention/GC; keep file/object versions for the recovery horizon.

`RecoveryTests` exercises the SQLite online-backup API while the server is running, then restores table rows and
stored replay state. `server-smoke.py` verifies restart persistence of the published executable. Neither establishes
an off-site recovery time/recovery point objective or restores objects deleted from a cloud bucket.

## Observability and service boundary

The server uses ASP.NET Core structured logs and the library's `Iceberg.Net` ActivitySource/Meter. Attach your
collector/listeners and retain logs in the host environment. The REST metrics-report endpoint accepts/discards
reports; it is not a metrics database. A dedicated audit event store and per-route request-rate limiter are not
implemented. Reverse-proxy TLS, identity-provider configuration, ingress policy, backups and operational alerting
belong to the deployment and must be explicit in a production support agreement.

S3 credential/signing behavior is emulator-tested; GCS downscoping and ADLS directory-scoped SAS need real-service
validation. These integrations remain experimental until that evidence is recorded. Local/S3 .NET FileIO is
separate from tokens vended to external GCS/Azure-capable clients.
