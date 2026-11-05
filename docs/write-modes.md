# Write modes and delete mechanisms

`TableMutations` gives C# producers the operations they actually need. Each call commits **one snapshot**, so a
reader sees either all of the change or none of it.

| Mode | API | Use for |
|---|---|---|
| **Pure insertion** | `InsertAsync(table, rows)` | event streams, append-only facts. Never reads or touches existing rows |
| **Upsert by key** | `UpsertAsync(table, rows, keyColumns?, strategy)` | "make these rows current": rows whose key matches are replaced, new keys inserted. Key defaults to the schema's identifier fields; input keys must be unique and non-null |
| **Group key replacement** | `ReplaceGroupsAsync(table, rows, groupColumns, strategy)` | "this batch *is* client 42 / period 2024-09": every existing row of each group present in the input is deleted, then the input inserted. Groups absent from the input are untouched |
| **Delete by key** | `DeleteByKeysAsync(table, keyColumns, keys, strategy)` | GDPR erasure, retractions |
| **Delete by predicate** | `DeleteWhereAsync(table, row => …, strategy)` | arbitrary conditions over full rows |
| **Update** | `UpdateAsync(table, filter, u => u.Set(...), strategy)` | corrections in place; one snapshot, none if nothing matches. See [update-merge.md](update-merge.md) |
| **Merge** | `MergeAsync(table, source, on, m => …, strategy)` | reconcile with a feed: matched update or delete, not-matched insert, not-matched-by-source delete. See [update-merge.md](update-merge.md) |
| **Drop whole files / partitions** | `SnapshotProducer.DeleteFiles(table, file => …)` | retention by partition; metadata-only, reads no data |

## Delete mechanisms

| Strategy | How existing rows go away | Write cost | Read cost | v2 | v3 | Who can read it |
|---|---|---|---|---|---|---|
| `CopyOnWrite` | data files holding deleted rows are rewritten without them (DVs of rewritten files dropped) | high (rewrite whole files) | none | ✅ | ✅ | everyone |
| `PositionDeletes` | v2: position delete file per data file; v3: deletion vector per data file (merged with an existing one) | medium (scan to find positions) | low | ✅ | ✅ | Athena (v2), Spark, Trino, PyIceberg ✅ |
| `EqualityDeletes` | equality delete file with the key values; **no scan** | lowest | highest until compaction | ✅ | ✅ (v4 forbids new ones) | Spark, Trino, Flink; **PyIceberg 0.12 refuses** |
| `Default` | `write.delete.mode` (deletes) / `write.merge.mode` (upsert, replace): `copy-on-write` (default) or `merge-on-read` → `PositionDeletes` | | | | | |

Rules the library enforces:

- **Equality deletes are key-only.** `DeleteWhereAsync` with `EqualityDeletes` is rejected: a predicate cannot be
  written as a set of keys.
- **Equality deletes need the partition in the key.** An equality delete only applies within its own partition (or
  globally, if unpartitioned). So every partition source column must be part of the key, otherwise an existing row in
  another partition would survive. This is the same requirement Flink's upsert mode has. Tables with several
  partitioned specs are rejected for now.
- **Same-commit inserts are safe.** An equality delete applies only to *strictly older* data (spec sequence-number
  rule), so the rows inserted by the same upsert are never deleted by it.
- **At most one deletion vector per data file**, in line with the spec; a later or concurrent delete merges into it.

## Concurrency

On a conflicting concurrent commit the whole mutation is **re-planned** against the new table state and retried,
using the `commit.retry.*` budget. It never replays positions computed from the old state. This gives
serializable semantics:

- Two writers upserting the same new key leave one row, the later writer's
  (`Concurrent upserts of the same key keep a single row`).
- A writer whose target file was rewritten by someone else re-scans instead of deleting from a file that no longer
  exists (`A writer whose target file disappeared does not resurrect deletes`).
- A file removed by compaction and expiry between planning and reading it counts as a lost race, not an error.
- `InterleavingTests` lands append, upsert, delete, compaction, expiry and orphan cleanup between each
  mutation's plan and its commit, on every strategy and on v2 and v3.

Inserted data files are written once and reused across attempts, because they don't depend on table state.

`commit.retry.total-timeout-ms` bounds additional attempts using a shared monotonic budget. Nested producers and
staged replans do not restart it. A schema-ID change rejects the old command before retained keys, predicates or
input arrays can be reinterpreted; rebuild them from the refreshed schema. A changed table UUID also rejects the
command. Prepared append files may retain an archived partition spec after the default changes. See
[stability and retries](stability-and-retries.md) for the exact boundaries and conservative schema policy.

## Choosing

| Situation | Choose |
|---|---|
| Athena-queried Glue table, occasional corrections | `CopyOnWrite` on v2 |
| Frequent small upserts, readers are Spark/Trino | `PositionDeletes` (v3 DVs), compact periodically |
| Very high-rate key upserts, no PyIceberg readers, compaction scheduled | `EqualityDeletes` |
| Replacing a client's or period's whole slice | `ReplaceGroupsAsync`; if the group columns are identity partition columns, `CopyOnWrite` rewrites only those partitions' files |

## Known limits

- Matching reads only files the keys can touch: upserts, deletes by key and group replacement derive a pruning
  filter from the key set (membership, or the min–max range above 1,000 keys); `DeleteWhereAsync(Expression)` prunes
  by the filter. The `Func<object?[], bool>` overload still scans every file.
- Filters follow Java's null semantics: `x != v`, `not in`, `not starts with` are true for nulls. PyIceberg drops
  nulls there (Arrow's three-valued logic), so results can differ on nullable columns.
- v3 copy-on-write rewrites and compaction keep `_row_id` and `_last_updated_sequence_number`, as Java does.
- Equality-delete scans load every applicable delete file's keys into memory.

## Executable specification

`tests/Iceberg.Net.Behaviour.Tests` (Reqnroll, Gherkin) is the readable contract for all of the above: 39
scenarios across format v2/v3 × all strategies, including the PyIceberg checks. Start with
`Features/WriteModes.feature`.
