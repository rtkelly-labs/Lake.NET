# UPDATE and MERGE

`TableMutations.UpdateAsync` and `TableMutations.MergeAsync` are the row-level `UPDATE … SET … WHERE` and
`MERGE INTO … USING … ON` of SQL engines, for C# producers. Like every mutation in
[write-modes.md](write-modes.md), each call commits **one snapshot** or, when it changes nothing, **none**
(`MergeResult.Snapshot` is null).

```csharp
// UPDATE events SET region = 'apac', name = name || '!' WHERE id < 6
await TableMutations.UpdateAsync(table, Expr.LessThan("id", 6L),
    u => u.Set("region", "apac").Set("name", row => $"{row[1]}!"));

// MERGE INTO events t USING feed s ON t.id = s.id
//   WHEN MATCHED AND s.qty = 0 THEN DELETE
//   WHEN MATCHED THEN UPDATE SET *
//   WHEN NOT MATCHED THEN INSERT *
//   WHEN NOT MATCHED BY SOURCE AND t.region = 'eu' THEN DELETE
await TableMutations.MergeAsync(table, feed, ["id"], m => m
    .WhenMatchedDelete((t, s) => (int?)s[5] == 0)
    .WhenMatchedUpdate((t, s) => s)
    .WhenNotMatchedInsert()
    .WhenNotMatchedBySourceDelete(Expr.Equal("region", "eu")));
```

Rows are `object?[]` aligned to the current schema's top-level columns, as for inserts.

## UPDATE semantics

- Files are planned with pruning by the filter (partition summaries, partitions, column metrics); only those are read.
  Existing deletes are applied first, so a row already deleted is never resurrected.
- Every assignment is evaluated against the row **as it was** before the update (`SET a = b, b = a` swaps).
  A target is a top-level column or a field nested in structs, by its dotted path (`u.Set("address.city", "York")`);
  each may be assigned once, and a struct and a field inside it cannot both be assigned. A field inside a list or map
  cannot be targeted (assign the whole column). A nested assignment copies the structs it passes through and keeps
  their other fields; when an enclosing struct is null it becomes a struct whose other fields are null (Spark's
  nested assignment does the same), so leaving one of its required fields unassigned is an `ArgumentException`.
  The same targets work in MERGE's `WhenMatchedUpdate(u => …)`. Values are converted the way
  filter literals are (`int` → `long`, `DateOnly` → days, `DateTime` → micros, decimals rescaled without loss);
  `null` into a required column, an unknown column or a value of the wrong type is an `ArgumentException`, raised
  before anything is written.
- Updating a partition source column moves the row: the replacement is written into the partition its new values
  select.
- `RowsUpdated` is the number of rows changed; `RowsDeleted` and `RowsInserted` are 0.

## MERGE semantics

- `on` lists top-level key columns (default: the identifier fields) compared for equality. **A null key never
  matches**: a target row with a null key is "not matched by source", a source row with a null key is "not matched".
- **Clause order is SQL's.** For each matched (target, source) pair the first `WhenMatchedUpdate` /
  `WhenMatchedDelete`, in the order they were added, whose condition holds applies; if none holds, the row is kept.
  For each unmatched source row the first `WhenNotMatchedInsert` whose condition holds inserts the source row (or
  `row(source)`). With `WhenNotMatchedBySourceDelete(filter)`, target rows that no source row matches and that match
  the filter (any, without one) are deleted.
- `WhenMatchedUpdate(Action<RowUpdate>)` assigns from the target row; `WhenMatchedUpdate((target, source) => row)`
  returns the whole updated row, so `(t, s) => s` is `UPDATE SET *`.
- **Cardinality.** A non-null key that appears twice in the source is a `ValidationException` before anything is
  read or written: it is the only way a target row can match two source rows (Spark's "MERGE cardinality
  violation"). We check the source alone, so a duplicate key fails even when no target row has it, where Spark
  would insert both. One source row may match many target rows (merging on a non-unique column).
- **What is read.** Only files the source keys can reach (the upsert path's key filter: membership, or the min–max
  range above 1,000 keys). A not-matched-by-source clause also reads **every file its filter can reach**; without a
  filter that is the whole table.
- `RowsUpdated`, `RowsDeleted` (matched deletes plus not-matched-by-source deletes) and `RowsInserted` are exact.

## Strategies

| Strategy | UPDATE | MERGE |
|---|---|---|
| `CopyOnWrite` | rewrites each file holding a changed row without it, plus new files for the updated rows | same |
| `PositionDeletes` | v2 position delete files, v3 deletion vectors (merged into an existing DV), plus new files | same |
| `EqualityDeletes` | **rejected** (`ValidationException`): an equality delete removes by key and cannot express a filter | allowed only when every `WHEN MATCHED` clause is unconditional and there is no `WHEN NOT MATCHED BY SOURCE` clause (then every target row of a matched key is removed, which is what an equality delete on that key does). The partition source columns must be in `on`, as for upserts. The merge still reads the reachable files: it must know which source rows match |
| `Default` | `write.update.mode` (`copy-on-write` unless `merge-on-read`), as Java/Spark; `write.merge.mode` is not consulted | `write.merge.mode` |

Updated rows are always re-inserted as new data in the same snapshot. An equality delete applies only to strictly
older data, so it never removes the replacement written with it.

## Row lineage (v3)

An updated row keeps its `_row_id`: the replacement is written with the old `_row_id` stored explicitly and
`_last_updated_sequence_number` **null**, so it inherits the new snapshot's sequence number (what Spark writes).
Rows a copy-on-write rewrite carries over keep both values; inserted rows get fresh ids. A row with no `_row_id`
(a file written before an upgrade to v3) gets a fresh one.

## Concurrency

Both operations use the same re-plan loop as the other mutations (`commit.retry.*`): after a lost race the table is
refreshed and the whole change planned again against the winner's state, never replayed. So they are serializable:

- An UPDATE whose computed assignment reads a column sees the concurrently committed value
  (`InterleavingTests.Update_re_plans_against_the_winners_rows`).
- An UPDATE whose target row was deleted meanwhile commits nothing; a MERGE whose matched row was deleted meanwhile
  inserts the source row instead; a not-matched-by-source delete also removes rows appended meanwhile.
- A base file removed by compaction and expiry between planning and reading counts as a lost race.

Data files written by a lost attempt are not reused (they depend on the table state); they are left for orphan-file
cleanup, which keeps young files.

## Athena and other engines

- Athena's own `UPDATE` and `MERGE INTO` always write **position deletes** (merge-on-read), whatever the table's
  `write.update.mode` / `write.merge.mode`. Our default is copy-on-write unless those properties say
  `merge-on-read`; both are readable by Athena (v2), Spark, Trino and PyIceberg.
- `SparkCrossEngineTests` checks that Spark + Iceberg Java read our UPDATE/MERGE output identically, including row
  lineage, and that Spark running the same MERGE in SQL on a copy of the pre-merge table ends with the same rows
  (`Spark_merge_on_a_copy_matches_ours`). `UpdateMergeCrossEngineTests` checks PyIceberg column for column.
- Equality-delete merges are unreadable by PyIceberg 0.12, as for upserts.

## Executable specification

`tests/Iceberg.Net.Behaviour.Tests/Features/UpdateMerge.feature`; L0 cases in
`tests/Iceberg.Net.Spec.Tests/UpdateMergeTests.cs`.
