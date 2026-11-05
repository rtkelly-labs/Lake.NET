# Managing table state — what the releases teach

The implemented [stability and retry design](stability-and-retries.md) records the AWS/Iceberg reference contracts,
read-only status-check budget, uncertainty guards, and Athena submission/recovery identities.

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
| 1.10.0 | `cleanExpiredMetadata` in ExpireSnapshots; dangling-DV cleanup on rewrite | Expiry must also prune unused specs/schemas and orphaned deletes | 🟡 expiry deletes unreachable files; unused specs/schemas are kept |
| 1.11.0 | `Idempotency-Key` on mutating REST calls | Makes retry-after-timeout safe on servers that support it | ✅ UUIDv7 key; same-key retries when the server advertises `idempotency-key-lifetime` |
| 1.11.0 | Sequence-number conflicts retryable under concurrent commits (#15126) | A stale sequence number is a conflict, not a fatal error | ✅ (builder rejects; producer retries on refresh) |
| 1.11.0 | Duplicate DVs for one data file merged before commit (#15006) | v3 MoR writers must merge, not add | ⬜ (no DVs yet) |
| 1.11.0 | "Load snapshot after it has been committed to prevent accidental cleanup" (#15511) | Post-commit cleanup must use the *committed* state, not the attempted one | ✅ `SnapshotProducer.CleanupAsync` diffs against the reloaded winner |
| 1.12.0 | Position deletes with row data removed; multi-DV validation (#17764); fail scans when deletes don't match the data file's partition (#16957) | Tighter delete-file validation to copy when MoR lands | ⬜ |

## Concurrency and isolation

- **Appends** are conflict-free: on a lost race the producer re-bases onto the new parent and retries.
  `CatalogConformanceTests.Concurrent_commits_serialise_through_the_catalog` runs four writers against Glue
  and REST and checks every row lands exactly once with a linear snapshot chain.
- **Row-level mutations** (`TableMutations`: upsert, group replacement, deletes, update, merge) are re-planned from scratch on
  the winner's state after a lost race, so they are serializable without any validation step.
- **Overwrites/file deletes** (`SnapshotProducer.Overwrite` / `DeleteFiles`) are **serializable** by default, as
  Java's are. On retry, `ValidationException` is raised when any of these holds:
  - a concurrent commit added data the filter selects (`validateNoConflictingData`);
  - a concurrent commit added a delete file against a file this commit replaces (`validateNoConflictingDeletes`):
    a position delete or DV naming the file, or any delete file in its partition that names no single file;
  - a file this commit replaces is no longer live, because compaction or another writer removed it
    (`failMissingDeletePaths`);
  - the starting snapshot has expired or is no longer an ancestor of `main`, so the history can't be checked.

  `WithSnapshotIsolation()` opts out of all of them.
- **Ref changes** are guarded by `assert-ref-snapshot-id` on every ref they read or change. Rollback, set-current,
  replace, rename and remove fail with `CommitFailedException` if their ref moved. Fast-forward and cherry-pick are
  re-validated and re-applied.
- **Branch writes** (`ToBranch`) validate against the branch's own history and leave `main` alone. A stale sequence
  number is a retryable conflict (Java #15126).
- **Metadata-only commits** (properties, schema) are guarded by the relevant requirement
  (`assert-current-schema-id`, `assert-last-assigned-field-id`), so two schema changes conflict while two
  property changes do not — the same as Java.

## Conflicts with Athena and Glue, by timing

A writer plans against snapshot S0 and then swaps the metadata pointer. On Glue, the swap is `UpdateTable` with
`VersionId` plus a `metadata_location` check. Glue managed compaction, Athena `OPTIMIZE`/`VACUUM`/DML and other
producers can each commit in between.

| Concurrent change lands | Outcome |
|---|---|
| before we plan | no conflict |
| between plan and swap | the swap fails (`CommitFailedException`): appends re-base, mutations re-plan, overwrites validate as above |
| after our swap, but it started earlier | the other writer must validate. Iceberg's RewriteFiles fails a compaction whose files we added deletes against. Athena DML is expected to fail with `ICEBERG_COMMIT_ERROR` (unverified until the L3 suite runs) |

`InterleavingTests` runs another writer at exactly the "between plan and swap" point. The writers are append,
upsert, delete, compaction, expiry and orphan cleanup, against each of append, upsert (position, copy-on-write,
equality), delete, compaction and overwrite, on v2 and v3. Each pair asserts the serializable result and a table
that passes deep validation.

Two hazards are about wall-clock age, not ordering:
- **Expiry of the starting snapshot.** Glue's snapshot-retention optimizer, `VACUUM`
  (`vacuum_max_snapshot_age_seconds`) and `ExpireAsync` can remove S0 and its files mid-operation. A missing base
  file is treated as a lost race when the table has moved, and the history check above fails rather than guessing.
- **Orphan cleanup of files not yet committed.** Glue's orphan optimizer (3-day default), `VACUUM` and
  `DeleteOrphanFilesAsync` delete unreferenced files under the table location once they are old enough. Our own
  writes commit within seconds, far inside that window. Files left under the table location to be **adopted
  later with `ParquetImport`** are at risk, though. Stage them outside the table location, or adopt them within
  the retention period.

## Failure taxonomy (what callers must handle)

| Exception | Meaning | Safe action |
|---|---|---|
| `CommitFailedException` | Requirement failed or CAS lost; nothing was applied | refresh, re-apply, retry (producers do this) |
| `CommitStateUnknownException` | The catalog may or may not have applied it | **do not retry blindly, do not delete files**; reload and check whether the snapshot exists |
| `ValidationException` | The prepared data, original table identity, schema bindings or file-filter partition assumptions no longer hold | reject the old plan; rebuild the command and its inputs from current metadata |
| `NoSuchTableException` / `NoSuchNamespaceException` / `AlreadyExistsException` | catalog-level state | caller's choice |

Producers and staged transactions now perform read-only recovery for uncertain outcomes, including lost
connections. Positive evidence for every attempted snapshot in the same table UUID resolves success. Absence
or mixed evidence remains unknown; failed reads retry under `commit.status-check.*`. Cancellation during a
catalog swap is also potentially applied. Unresolved producer/staged objects cannot be replayed, and staged
`DiscardAsync` preserves their artifacts.

A command token in a snapshot summary (`SnapshotProducer.Set("iceberg-net.command-id", …)`) helps identify
success, but absence after expiry does not establish failure and concurrent callers can both submit the same
token. Exactly-once delivery needs durable, atomically enforced deduplication and checkpoint semantics beyond
this lookup. See the [recovery design](stability-and-retries.md).

Data-layer write/ref/expiry/compaction retries share `commit.retry.total-timeout-ms` and a retry count through
nested producers and replans. A zero timeout allows the initial attempt only. These limits do not cancel an
in-flight catalog call or replace status recovery. Partial compaction batches each have their own commit budget.

## Maintenance state

| Operation | Java/Spark reference | Here |
|---|---|---|
| Expire snapshots (`older_than`, `retain_last`, keep refs) | `expire_snapshots`, defaults 5 days / 1 | ✅ `Maintenance.ExpireAsync`: deletes files only expired snapshots referenced, after the commit succeeds; honours `history.expire.*` and each ref's `min-snapshots-to-keep` / `max-snapshot-age-ms` / `max-ref-age-ms`, and removes refs that aged out (never `main`) |
| Remove orphan files | `remove_orphan_files` (needs listing + age threshold) | ✅ `Maintenance.DeleteOrphanFilesAsync` (default 3 days, dry run) |
| Rewrite data files (bin-pack/sort) | `rewrite_data_files`, `use-starting-sequence-number=true` | ✅ `Compaction.RewriteDataFilesAsync`, bin-pack and sort (by the table's default sort order); instead of starting sequence numbers it re-plans on any concurrent commit; partial progress ✅ (`PartialProgressEnabled`, `PartialProgressMaxCommits`) |
| Rewrite position deletes | `rewrite_position_delete_files` | ✅ `Compaction.RewritePositionDeletesAsync`: one file per data file on v2, deletion vectors on v3; dangling deletes dropped |
| Rewrite manifests | `rewrite_manifests` | 🟡 merge-append on commit (Java defaults) keeps counts bounded; standalone rewrite ⬜ |
| Rollback / cherry-pick / fast-forward, branches and tags | procedures of the same names | ✅ `table.ManageSnapshots()` (Java's ManageSnapshots and CherryPickOperation); branch writes with `SnapshotProducer.ToBranch`. See [snapshot-management.md](snapshot-management.md) |

Note:Athena's `OPTIMIZE` and `VACUUM` already cover compaction and expiry for Glue tables, so
the port does not need to own maintenance to be useful for producers.
