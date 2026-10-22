# Managing table state — what the releases teach

Iceberg's state model has been hardened release by release, mostly in response to corruption and
lost-update bugs found in production. A new implementation inherits every one of those lessons or
re-discovers them. This page lists the state-related behaviour a port must reproduce, where it came from,
and where this port stands. Release facts are sourced in [research/ecosystem-2026-10.md](research/ecosystem-2026-10.md).

## State, in one table

| State | Lives in | Advanced by | Invariant |
|---|---|---|---|
| Current metadata pointer | catalog (Glue param, REST server, JDBC row) | atomic swap | only ever moves forward from the version the writer read |
| Table metadata `vN.metadata.json` | object store | new file per commit | never overwritten, never mutated |
| `last-sequence-number` | metadata | +1 per snapshot (v2+) | strictly increasing; data sequence numbers decide which deletes apply |
| `next-row-id` | metadata (v3) | `+added-rows` per snapshot | row IDs are never reused |
| Snapshots, refs (`main`, branches, tags) | metadata | `add-snapshot`, `set-snapshot-ref` | refs point at existing snapshots; `current-snapshot-id` mirrors `main` |
| Snapshot log / metadata log | metadata | appended per commit | trimmed; snapshot log restarts after an expired entry |
| Manifest lists, manifests, data/delete files | object store | written before commit | immutable; reachable only through metadata |

## Lessons from the Java release history

| Release | Change | Why it matters for us | Status here |
|---|---|---|---|
| ≤1.4 | `format-version` default becomes **2** | Athena reads v2, not v3 — new tables default to v2 | ✅ `TableMetadata.DefaultFormatVersion = 2` |
| 1.x (Glue) | Glue commits use `VersionId` optimistic locking, `skip-archive` default | Without the version check, concurrent commits silently lose snapshots | ✅ and pinned by fidelity test (moto lacked it — see [emulation](emulation.md)) |
| 1.8.0 | Deletion vectors; row-lineage metadata fields | v3 write path needs Puffin + Roaring | 🟡 lineage ✅, DVs ⬜ |
| 1.9.0 | Row lineage on for all v3 tables; `current-snapshot-id` written as `null` for v3 | Readers must accept null **and** -1 | ✅ |
| 1.10.0 | Stop retrying REST commits on 502/504; 503 on commit = **unknown** | Retrying an unknown commit can apply it twice or delete live files | ✅ 500/502/503/504 → `CommitStateUnknownException`, never retried |
| 1.10.0 | `cleanExpiredMetadata` in ExpireSnapshots; dangling-DV cleanup on rewrite | Expiry must also prune unused specs/schemas and orphaned deletes | ⬜ (expiry is metadata-only today) |
| 1.11.0 | `Idempotency-Key` on mutating REST calls | Makes retry-after-timeout safe on servers that support it | 🟡 header sent; no retry yet |
| 1.11.0 | Sequence-number conflicts retryable under concurrent commits (#15126) | A stale sequence number is a conflict, not a fatal error | ✅ (builder rejects; producer retries on refresh) |
| 1.11.0 | Duplicate DVs for one data file merged before commit (#15006) | v3 MoR writers must merge, not add | ⬜ (no DVs yet) |
| 1.11.0 | "Load snapshot after it has been committed to prevent accidental cleanup" (#15511) | Post-commit cleanup must use the *committed* state, not the attempted one | ✅ `SnapshotProducer.CleanupAsync` diffs against the reloaded winner |
| 1.12.0 | Position deletes with row data removed; multi-DV validation (#17764); fail scans when deletes don't match the data file's partition (#16957) | Tighter delete-file validation to copy when MoR lands | ⬜ |

## Concurrency and isolation

- **Appends** are conflict-free: on a lost race the producer re-bases onto the new parent and retries.
  `CatalogConformanceTests.Concurrent_commits_serialise_through_the_catalog` runs four writers against Glue
  and REST and checks every row lands exactly once with a linear snapshot chain.
- **Overwrites/deletes** today give *snapshot isolation*: the predicate is re-evaluated on the new parent.
  Java's default is **serializable** (`write.*.isolation-level`): fail if a concurrent commit added data
  matching the predicate. That validation is the next state-management milestone (M2).
- **Metadata-only commits** (properties, schema) are guarded by the relevant requirement
  (`assert-current-schema-id`, `assert-last-assigned-field-id`), so two schema changes conflict while two
  property changes do not — the same as Java.

## Failure taxonomy (what callers must handle)

| Exception | Meaning | Safe action |
|---|---|---|
| `CommitFailedException` | Requirement failed or CAS lost; nothing was applied | refresh, re-apply, retry (producers do this) |
| `CommitStateUnknownException` | The catalog may or may not have applied it | **do not retry blindly, do not delete files**; reload and check whether the snapshot exists |
| `NoSuchTableException` / `NoSuchNamespaceException` / `AlreadyExistsException` | catalog-level state | caller's choice |

For a service that needs exactly-once, the producer should stamp its own idempotency
token into the snapshot summary (`SnapshotProducer.Set("iceberg-net.command-id", …)`) and, after an unknown
outcome, look for it in the table's snapshots before retrying. That pattern needs no extra infrastructure.

## Maintenance state

| Operation | Java/Spark reference | Here |
|---|---|---|
| Expire snapshots (`older_than`, `retain_last`, keep refs) | `expire_snapshots`, defaults 5 days / 1 | ✅ metadata-only; file deletion needs reachability diff (⬜) |
| Remove orphan files | `remove_orphan_files` (needs listing + age threshold) | ⬜ |
| Rewrite data files (bin-pack/sort) | `rewrite_data_files`, `use-starting-sequence-number=true` | ⬜ |
| Rewrite manifests | `rewrite_manifests` | ⬜ (fast-append creates one manifest per commit, so this matters early) |
| Rollback / cherry-pick / fast-forward | procedures of the same names | 🟡 possible via raw `set-snapshot-ref` updates |

Note:Athena's `OPTIMIZE` and `VACUUM` already cover compaction and expiry for Glue tables, so
the port does not need to own maintenance to be useful for producers.
