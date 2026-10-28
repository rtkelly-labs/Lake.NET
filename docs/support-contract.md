# Intended full-stack contract for 0.0.1

This is the concrete support proposal for review before cutting `0.0.1`. It implements the owner's selected scope:
**the full stack, including the REST server**. It is not a release announcement or a production-readiness promise.
The [capability baseline](capability-baseline.md) supplies implementation/test evidence and exclusions for each ID.

## Distribution

All eleven libraries are selected: `Core`, `Avro`, `IO`, `Catalog`, `Puffin`, `Data`, `Linq`, `Arrow`,
`EntityFrameworkCore`, `Spark` and `Athena` (each prefixed `Iceberg.Net`). The REST server is a framework-dependent
executable archive, not a twelfth NuGet library. Release builds verify the exact library set and the published
server. Local packs use `0.0.1-dev.1`; `CHANGELOG.md` still controls any actual release version. No `0.0.1` cut exists.

## Capability selection

“Included” means the implemented behavior and limitations named in the baseline, not every behavior a broad heading
could imply. Experimental integrations are distributed but require real-service evidence before a production claim.

| Capability IDs | Selection | Contract |
|---|---|---|
| C01–C08 | Included with format/type limits | Schema/metadata/manifest and Parquet/Avro read paths, evolution, transforms, scans and time travel. Supported v1 reads only; v2/v3 writes; ordinary primitive/nested data I/O. Extended metadata types are not complete I/O support |
| C09–C14 | Included | Atomic single-table mutations and supported delete strategies; serializable re-planning, lineage and staged multi-table commits only on a transaction-capable catalog |
| C15–C22 | Included with documented limits | Refs/WAP, maintenance/retention, supported metadata tables, change feed, flat foreign-Parquet import, Puffin statistics and validation/fingerprints |
| C23 | Included | In-memory, REST, Glue and SQLite/JDBC catalog APIs, qualified by each backend's capabilities and verification |
| C24 | Experimental real services | Glue/AWS REST/S3 Tables clients exist and have emulated/signing tests; real AWS execution is not yet qualified |
| C25 | SQLite included; other SQL providers unqualified | ADO.NET factory extensibility does not imply dialect portability |
| C26–C27 | Included with protocol limits | REST client, explicit credential refresh/replayed-create recovery and view metadata/catalog APIs. No general .NET SQL execution engine |
| C28 | Local included; real S3 experimental | Native local/S3 FileIO, with emulated S3 guarantees. Native Azure/GCS data FileIO is absent |
| C29 | Named tested combinations only | Compatibility with pinned reference engines/catalogs is not a Cartesian product of every format, type and delete strategy. PyIceberg does not read equality deletes or Avro data; pinned Trino does not read DVs |
| C30–C33 | Included | Typed mapping/query/mutation and supported Arrow batches. Preserve explicit nulls; defaults fill absent members. LINQ can execute residuals/operators in memory |
| C34 | Included with provider limits | EF Core 10 model/query/bulk/tracked save support; catalog-dependent atomicity. No relational feature-completeness, migrations, explicit transactions, relationships or collection mapping |
| C35 | Included | REST catalog/server protocol surface with the reviewed OpenAPI deviation ledger; one warehouse per server |
| C36 | Local identity/grant behavior included; cloud delegation experimental | Bearer/client credentials, grants, confinement and delegation implementation; real S3/GCS/ADLS verification outstanding |
| C37 | Included within operating boundary | Durable SQLite pointers/plans/replay, multi-instance tests and bounded per-principal claims. No general exactly-once guarantee across process failure; audit/metrics storage is not bundled |
| C38–C39 | SQL helpers included; real managed execution experimental | Spark/Athena statements and SDK runners are available; managed services and Athena-only SQL require real-service validation |
| C40 | Included | ActivitySource/Meter instrumentation and structured server logs; host supplies collection, retention and alerting |

Explicit exclusions include ORC, complete v4 tables, v1 snapshot writes/embedded manifests, extended-type data I/O,
collection-element filters/partitions, nested Parquet import, generic SQL-provider certification, native Azure/GCS
FileIO, automatic background credential renewal, arbitrary EF Core features and a hosted monitoring/audit service.
A model/JSON representation or extension interface is not evidence for the excluded implementation.

## Platform, resource and operational conditions

- Core/Avro target `netstandard2.0` and .NET 10. Other components require .NET 10. Native I/O needs an appropriate
  ParquetSharp runtime asset. No complete .NET Framework writer is supplied. General NativeAOT qualification is not claimed merely because
  the ShadowCompare tool opts into AOT publishing.
- The [resource/platform contract](resource-envelope.md) records memory boundaries and diagnostic workload results.
  No universal table-size, throughput, latency or constant-memory guarantee is claimed.
- The [server operating contract](server-operations.md) specifies deployment, shared configuration, quota/replay,
  credential lifetime and coordinated database/warehouse recovery requirements. The built-in persistent SQL provider
  is SQLite; arbitrary network filesystems and multi-region hosting are not qualified.
- Public [API snapshots](api/README.md) are review gates. Before `0.0.1`, intentional API changes still require
  review and corresponding scenario updates; an unchanged signature does not prove unchanged behavior.

## Acceptance before the cut

S01–S12 in the baseline are the observable acceptance scenarios. The cut needs:

1. Green unit, required integration/regression and declared platform checks at the exact proposed commit, with every
   remaining skip explained by the contract rather than an unavailable required dependency.
2. Exact package-set consumption plus published-server client/restart checks, checked API snapshots, and documentation
   consistent with the selected capabilities.
3. Confirmation that experimental cloud/operational exclusions are acceptable, or replacement with real-service
   evidence for any path being promoted to supported status. AWS workflow variables are not configured in the
   connected repository and no isolated sandbox target has been supplied; GCS/ADLS real-service results are also outstanding.
4. Owner review of this support contract before a version cut; no version cut while other PRs remain open. A dry run
   after the cut must verify packages, server artifact and successful CI. A real NuGet publish requires the owner's
   separate confirmation.
