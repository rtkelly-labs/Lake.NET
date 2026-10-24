# Snapshot management and maintenance

Branches, tags, rollback, cherry-pick, write-audit-publish, retention-aware expiry, metadata tables, the change feed,
and the compaction variants that commit in parts. Each follows the Java reference implementation (named per
section). Spark cross-checks the observable parts in `SparkCrossEngineTests`.

## Refs: `table.ManageSnapshots()`

Java's `ManageSnapshots`. Changes are collected and committed as one metadata update:

```csharp
await table.ManageSnapshots()
    .CreateTag("v1", snapshotId, new RefRetention(MaxRefAgeMs: 30L * 86_400_000))
    .CreateBranch("audit", retention: new RefRetention(MinSnapshotsToKeep: 2, MaxSnapshotAgeMs: 3_600_000))
    .CommitAsync();
```

| Operation | Rule (as Java) |
|---|---|
| `CreateBranch(name, id?, retention?)` / `CreateTag(name, id, retention?)` | the ref must not exist; tags take only `MaxRefAgeMs`; a branch defaults to main's current snapshot |
| `ReplaceBranch(name, id)` / `ReplaceTag` / `ReplaceBranch(name, source)` | the ref must exist with that type; retention settings are kept |
| `RenameBranch`, `RemoveBranch`, `RemoveTag` | `main` cannot be renamed or removed |
| `SetMinSnapshotsToKeep`, `SetMaxSnapshotAgeMs` (branches), `SetMaxRefAgeMs` (any ref) | values must be positive |
| `RollbackTo(id)` | the snapshot must be an ancestor of main (`ValidationException` otherwise) |
| `RollbackToTime(instant)` | main's latest ancestor committed strictly before the instant |
| `SetCurrentSnapshot(id)` | any snapshot |
| `FastForwardBranch(name, source)` | `name` must be an ancestor of `source` |
| `CherryPick(id)` | Java's `CherryPickOperation`; see below. It commits a snapshot of its own, so it is the only change in its manager |

Every snapshot move of `main` appends to the snapshot log, so `SnapshotSelector.AsOf` and the `history` table
reflect the rollback. Every ref that the change set reads or changes is guarded by `assert-ref-snapshot-id`.

**After a lost race:**
- **Fast-forwards, ref creation and retention settings** are re-validated and re-applied on the refreshed table. A
  fast-forward that is no longer one fails with `ArgumentException`. A tag that someone else created first also fails
  with `ArgumentException`.
- **Rollback, set-current, replace, rename and remove** move a ref to a fixed place. If that ref moved since the
  manager was created, they throw `CommitFailedException` and nothing is applied, because applying them to the new head
  would discard the commits made meanwhile.

**Cherry-pick:**
- When the picked snapshot's parent is main's head, main is fast-forwarded to it. This works for any operation.
- Otherwise an `append` is replayed: a new snapshot adds the same data files with new sequence numbers and new row IDs,
  and records `source-snapshot-id`.
- A dynamic partition overwrite (`replace-partitions=true`, as Spark's `INSERT OVERWRITE` writes it) is replayed when
  its parent is an ancestor of main and no file was added to its partitions since. Like Java, the replayed snapshot
  records `source-snapshot-id` but not `replace-partitions`.
- Any other operation fails if it is not a fast-forward.
- Files keep the partition spec they were written with. A snapshot staged before a spec change replays its files
  under their old spec, and the producer writes one added-files manifest per spec. A dynamic overwrite's
  replaced-partition check compares partitions together with their spec, so main's appends under the new spec do not
  block it.
- Picking an ancestor of main, or a snapshot that was already picked, fails.
- **Spark agrees.** `SparkCrossEngineTests.Spark_cherry_picks_snapshots_of_an_older_spec_as_we_do` stages an append and
  a dynamic overwrite under spec 0 (identity `region`), moves the spec to `bucket(4, id)` and appends under it, then
  picks both. Spark runs `CALL system.cherrypick_snapshot` on a copy of the table before our picks (our
  `metadata.json` as `v1` of a Hadoop table: same snapshots and files), and Iceberg Java describes both results: the
  operation, the summary (apart from engine names and Java's manifest counts), every added and removed file with its
  spec, partition, record count and `first_row_id`, the rows and, on v3, the row lineage. They are identical; Spark
  also reads our result, its metadata tables and its changelog as we do. The comparison found one difference, now
  fixed: we used to write `replace-partitions=true` on a replayed overwrite, which Java does not.
- A replay composes with concurrent appends.

## Branch writes and write-audit-publish

`SnapshotProducer.…ToBranch("audit")` works the same way for appends, overwrites, row deltas and row deletes:
- The parent, the serializable validation (conflicting data and deletes, missing files, history), the `total-*`
  summary and the requirement all follow the branch. `main` is not touched.
- A branch that does not exist yet starts at main's current snapshot (Java's `SnapshotUtil.latestSnapshot`).
- Sequence numbers and v3 row IDs stay table-global. A stale sequence number from a commit on another branch is a
  retryable conflict (Java #15126).

Write-audit-publish needs no `wap.id`:

```csharp
await SnapshotProducer.RowDelta(table).DeletePositions(file, [pos]).AddFiles(newFiles).ToBranch("audit").CommitAsync();
await TableValidator.AssertValidAsync(table, new ValidationOptions { AllSnapshots = true, Deep = true }); // + checks on Ref("audit")
await table.ManageSnapshots().FastForwardBranch("main", "audit").RemoveBranch("audit").CommitAsync();
```

`TableMutations` (upsert, group replacement, deletes) still write to `main` only.

## Expiry with per-ref retention

`Maintenance.ExpireAsync(table, ExpireOptions?)` follows Java's `RemoveSnapshots`:

1. Refs other than `main` whose snapshot is older than their `max-ref-age-ms` are removed. The default is
   `history.expire.max-ref-age-ms`, which is unlimited.
2. Each remaining branch keeps its head and its first `min-snapshots-to-keep` ancestors (default
   `history.expire.min-snapshots-to-keep` = 1). It also keeps every further ancestor younger than its
   `max-snapshot-age-ms` (default `history.expire.max-snapshot-age-ms` = 5 days). The walk stops at the first ancestor
   that is neither.
3. Each remaining tag keeps its snapshot.
4. Snapshots that no remaining ref reaches are kept while they are younger than the table-wide age.
5. `ExpireOptions.SnapshotIds` also expires those snapshots. Naming one that a retained ref points at is an error.

`OlderThanMs` and `RetainLast` override only the table-wide defaults, as Java's `expireOlderThan`/`retainLast` do. A
ref's own settings still win for that ref. The existing `ExpireAsync(table, olderThanMs, retainLast)` maps to these
options; omit `retainLast` to use the property.

The commit asserts every ref. After a lost race, the retention is recomputed on the new state. Files are deleted only
after the commit has succeeded, by diffing reachability against the committed metadata. One gap is shared with Java: a
ref created concurrently on a snapshot being expired is dropped with it.

## Metadata tables

`MetadataTables` returns typed rows whose `Columns` use Spark's column names: `Snapshots`, `History`, `Refs`,
`MetadataLogEntries`, `ManifestsAsync`, `FilesAsync`/`DataFilesAsync`/`DeleteFilesAsync`, `EntriesAsync` (includes
deleted entries) and `PartitionsAsync`.
- The manifest-based tables read the snapshot that a `SnapshotSelector` picks, for example `Ref("v1")`.
- `partition` in `files`, `entries` and `partitions` uses the table's unified partition type (every partition field of
  every spec, by field ID), as Spark shows it.
- `partitions` adds `dv_count`, which Spark 1.12 does not have. DVs are also counted as position delete files there.
- `readable_metrics` and the `all_*` tables are not implemented.

Spark compares these tables column by column for eight of our tables (v2 and v3, copy-on-write, position deletes, DVs,
equality deletes, evolved specs, refs) and for a table it built itself.

## Change feed

`ChangeFeed.ChangesAsync(table, fromExclusive, toInclusive, options)` yields `ChangeRow(Row, Insert|Delete,
ChangeOrdinal, CommitSnapshotId)` with the semantics of Spark's `changes` table. It walks each snapshot on the branch's
ancestry, oldest first:
- Rows of added data files are inserts, minus any rows that deletes in the same snapshot remove.
- Rows of removed data files are deletes, with the deletes that applied before the snapshot.
- Rows of carried-over files that the snapshot's new position deletes, DVs or equality deletes remove are deletes. For
  position deletes and DVs, these are exactly the referenced rows.
- `replace` snapshots (compaction, rewrites) are skipped and get no ordinal.

A copy-on-write update reports the whole rewritten file: the unchanged rows appear as delete/insert pairs, as Spark's
`changes` table does. `RemoveCarryovers = true` cancels identical pairs within a snapshot, as `create_changelog_view`
does by default. Like Spark, it also cancels a genuine delete and re-insert of an identical row.

Equality deletes on a column that was later dropped still apply to the feed's rows (see
[read-path.md](read-path.md#column-projection)).

**What Spark can and cannot check.** `SparkCrossEngineTests.Spark_reports_the_same_changes` compares the feed with
Spark's `changes` table only for tables without delete files. There the feed matches exactly, including copy-on-write
carry-overs. The feed for merge-on-read tables (v2 position delete files, v3 deletion vectors, equality deletes)
**cannot be cross-checked with Spark**. Java's `BaseIncrementalChangelogScan` (Iceberg 1.12) throws
`UnsupportedOperationException: Delete files are currently not supported in changelog scans` as soon as a snapshot in
the range has delete manifests, so Spark has no answer to compare with. PyIceberg has no changelog scan either. The
oracle for those cases is the L0 replay: `ChangeFeedTests.Replaying_the_feed_reproduces_every_snapshot` runs every
delete strategy on v2 and v3, applies each snapshot's deletes and inserts to the previous state, and checks that the
result equals a time-travel read of that snapshot. When Java's changelog scan supports delete files, the Spark fixture
should add the merge-on-read tables to its changelog list.

## Compaction in parts, and delete-file rewrites

- `RewriteOptions(PartialProgressEnabled: true, PartialProgressMaxCommits: 10)` (Java's `partial-progress.*`) commits
  the file groups in up to N batches. A batch that loses a race is re-planned on the new state alone, and the batches
  already committed stand. A batch that loses `maxAttempts` times is skipped and counted in
  `RewriteResult.FailedGroups`. `RewriteResult.Commits` lists every snapshot.
- `Compaction.RewritePositionDeletesAsync(table)` (Java's `RewritePositionDeleteFiles`), on v2: every position delete
  file becomes one file per data file. Positions of data files that are gone, or that a delete no longer applies to,
  are dropped. The result is one `replace` snapshot, or no commit when the files are already one per data file. On v3,
  v2 position deletes are converted to DVs and dangling DVs are removed. A concurrent rewrite of the same delete files
  fails it with `ValidationException`. New deletes committed meanwhile are kept.
