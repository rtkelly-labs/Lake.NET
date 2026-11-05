# Commit/retry/cleanup pilot

This is a bounded protocol model, checked with TLC and linked to executable C# tests. It establishes properties
of the modeled transitions within the bounds below. It does not prove that the full library implements Iceberg.

## Run

From the repository root, with Python 3.10+, curl and Java 11+:

```sh
python3 tools/check_formal.py
dotnet test tests/Iceberg.Net.Spec.Tests --filter FullyQualifiedName~FormalTraceReplayTests
```

Set `JAVA_HOME` or `ICEBERG_TLC_JAVA` if Java is not on PATH. The checker downloads TLC **1.7.4**, verifies SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`, uses one worker and fixed fingerprint/seed
parameters, and writes downloads, generated configs, states and full logs under `temp/formal/`. The downloaded
artifact's SHA-1 was also compared with the upstream release checksum
([TLA+ releases](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4)). No runtime dependency is added to the library.

The commit runner checks three mode combinations, four deliberately broken protocols, and a coverage witness.
The companion `WritePlan` model below adds two negative controls and an archived-spec coverage witness. It exits
nonzero on parser/tool failures, timeouts, unexpected success of a broken protocol, unexpected invariant failures,
or a changed witness. Negative controls must exit with TLC's invariant-violation code and the expected property
name; an arbitrary failed Java process does not count as finding the bug.

CI has a required `formal` job through the existing aggregate `ci` check. It uploads the generated configs and
logs. The regular unit job executes the C# replay tests. Model checking does not require Docker, AWS or Python
Iceberg dependencies.

Local verification on 2026-10-26: the append/append and append/replace configurations each completed with 613
distinct states; replace/replace completed with 267. All four negative controls failed their intended invariant.
The four C# cases passed (trace replay and automatic retry, each on v2 and v3). CI is configured but has not been
run for these changes.

## Bounds and assumptions

- One table; two logical commands; at most two prepared attempts per command; one maintenance action.
- Appends introduce distinct keys. A replacement changes key 0 and preserves unrelated keys. A replacement
  retry reuses prepared work only if its original source files remain live; otherwise it rejects the old plan.
  Full row-level mutation replanning is not modeled yet.
- Each prepared attempt has a fresh immutable artifact identity. Files abstract the snapshot's prepared output;
  a snapshot and its output share an identifier for economy. Metadata JSON, manifests and Parquet encoding are
  not modeled separately. C# replay reuses input data files across attempts, a coarser correspondence rather
  than an artifact-by-artifact refinement proof.
- Catalog compare-and-swap is atomic and metadata reloads are authoritative. `version` is a monotonic metadata
  revision, separate from `head`, the snapshot identity. There is no listing-based commit protocol.
- An applied commit either returns success or loses its response. A lost request may also leave no applied
  commit. The model conservatively leaves this unresolved; a delayed server-side application is not modeled.
- Retained snapshot presence provides positive evidence. Expiry can remove non-head snapshot evidence even
  while its data remains reachable through a later snapshot. Snapshot absence is not negative evidence.
- Cleanup may remove abandoned attempts only when no retained snapshot reaches them and no uncertain commit
  protects them. Uncertain artifacts remain protected for the entire execution, including after resolution.
- There is no rollback, schema/partition evolution, branch, multi-table transaction, delete-file sequence
  applicability, v3 row lineage, orphan-file age policy, process restart or durable deduplication protocol yet.
- Only safety is checked. Deadlock checking is disabled because completed, rejected and unresolved operations
  can legitimately have no next action. No fairness, eventual success, deadline or liveness result is claimed.

## Assurance record

The reference is `apache-iceberg-1.12.0`, matching `docs/spec/sections.json`. The section links below describe
obligations; they do not imply that this small abstraction covers every requirement in each section.

| Property | Model assertion | Implementation correspondence | Executable evidence |
|---|---|---|---|
| Compatible writes do not lose another writer's rows | `RowsMatchSerialIntent` | `SnapshotProducer.CommitAsync`, `TableMutations.RetryAsync`, catalog pointer swaps | Generated append trace, built-in retry test; existing `InterleavingTests` cover more operation pairs |
| Uncertain outcomes cannot cause duplicate logical execution | `AtMostOnce` | Shared `CommitRecovery` preserves missing/mixed evidence as unknown; unresolved instances cannot replay | Model negative control and BDD expiry/error-response reproduction on v2/v3, checked by PyIceberg; no universal implementation proof |
| Published snapshots remain readable | `ReachableFilesExist`, `HeadRetained` | Immutable metadata/file publication and post-commit cleanup | Deep `TableValidator` after every replay step, existing fault injection |
| Uncertain artifacts survive cleanup | `UnknownFilesPreserved` | `CommitStateUnknownException` cleanup contract | Model negative control; existing `CommitProtocolTests` |
| States stay within the declared domains | `TypeOK` | No direct implementation equivalence asserted | Every baseline model run |

Relevant sections: [Commit Conflict Resolution and Retry](https://iceberg.apache.org/spec/#commit-conflict-resolution-and-retry),
[Metastore Tables](https://iceberg.apache.org/spec/#metastore-tables),
[Snapshot Retention Policy](https://iceberg.apache.org/spec/#snapshot-retention-policy), and
[Deleting Files](https://iceberg.apache.org/spec/#deleting-files).
The replay class carries the existing `[Spec]` attributes so it appears in the generated traceability matrix.

The serial-intent invariant compares the visible rows with the commands applied in successful CAS order. It
does not check the return values of arbitrary read-modify-write callbacks or establish general serializability
of transactions with read sets. `AtMostOnce` covers internal retries of each modeled command, not duplicate
submissions by independent callers.

## Negative controls and their counterexamples

| Switch | Deliberate defect | Required failure |
|---|---|---|
| `BugCAS` | Publish prepared metadata after another writer changed the base | `RowsMatchSerialIntent`: one append disappears |
| `BugStaleSources` | Reuse a replacement after another writer replaced its source | `RowsMatchSerialIntent`: both versions of the replaced key survive |
| `BugUnknownRetry` | Interpret absence of an attempted snapshot as definite failure | `AtMostOnce`: an already-applied command executes again |
| `BugCleanup` | Delete files from an unresolved attempt | `UnknownFilesPreserved`: files that may be committed disappear |

The unknown-retry counterexample is particularly relevant to the current implementation:

1. Writer 1 applies an append, then loses its response.
2. Writer 2 commits another snapshot.
3. Maintenance expires writer 1's snapshot evidence, keeping writer 2's head and the appended data.
4. Recovery does not find writer 1's snapshot, classifies the result as failure, and retries.
5. Writer 1 has now committed the logical command twice.

The earlier producer and staged recovery paths converted absent evidence after a service error into a retryable
conflict. This schedule was reproduced by `CommitRecovery.feature` before the fix on v2/v3 for both single-table
and multi-table writes. Shared `CommitRecovery` now leaves absence/mixed evidence unknown, retries failed reads
only, and guards unresolved objects against replay/discard. All four BDD cases pass with PyIceberg reading each
row once. See [the implemented design](../../docs/stability-and-retries.md). This closes the reproduced defect;
it is not a universal refinement proof or real AWS validation.

## Trace correspondence

`NoAppendRetryWitness` is deliberately false once writer 2 has succeeded and writer 1 has retried successfully.
The runner asks TLC for its shortest witness and normalizes the state transitions into
[`traces/append-conflict.json`](traces/append-conflict.json). This is a model-generated schedule, not a
hand-authored example. Regenerate it after reviewing model changes:

```sh
python3 tools/check_formal.py --update-trace
```

`FormalTraceReplayTests.Generated_append_conflict_trace_preserves_both_batches` interprets every action through
real producers, catalog requirements and Parquet I/O for formats v2 and v3. It exposes individual attempts with
`WithRetries(0)`, refreshes at the modeled retry action, and checks published rows and deep table validity after
every step. `Automatic_append_retry_realises_the_generated_conflict_order` also exercises the production
`TableMutations` retry loop with a second commit injected immediately before the first writer's catalog swap.

This is a concrete correspondence check for one schedule. It is neither exhaustive implementation exploration
nor a machine-checked refinement proof. The subsequent recovery hardening is covered separately by the BDD
counterexample and cross-engine row checks above.

## Metadata-only races and retained bindings

`WritePlan.tla` models the writer's original bindings, its refreshed cached schema, the catalog's authoritative
schema, the metadata pointer used for CAS, and a retained file spec separately. Bounds: one writer, one retry,
one schema change and one partition-default change; archived spec 0 remains available when spec 1 becomes default.

This distinction matters because the catalog can load a newer metadata pointer after the writer has already
prepared its change. CAS alone cannot detect a schema change that preceded that catalog load. The safe model
requires both an authoritative schema requirement and a guard against reinterpreting old bindings on refresh.
The current client policy fences any schema-ID change; compatibility proofs could permit more cases later.

`PublishedSchemaCorrect` and `PublishedSpecCorrect` hold in the baseline (66 distinct states locally checked).
Disabling the authoritative requirement (`BugAuthority`) or allowing stale inputs to be rebound after refresh
(`BugRetryRebind`) independently falsifies schema safety. The intentionally false `NoArchivedSpecWitness` produces
a trace where a spec-0 prepared append commits under default spec 1. `WritePlanStability.feature` exercises these
races on v2/v3 with PyIceberg row checks; `WritePlanGuardTests` also covers drop/recreate and whole-file filters.
These are correspondence examples, not exhaustive implementation refinement. UUID identity and elapsed time
are covered by executable tests, not this companion model; it does not prove retry deadlines or liveness.
