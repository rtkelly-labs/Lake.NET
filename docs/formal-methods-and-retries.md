# Formal methods and concurrent-write retries

Evaluation date: 2026-10-25. This is a proposed verification and retry design, not an implemented guarantee.

First pilot implemented 2026-10-26: see [the bounded model and assurance record](../formal/commit-retry/README.md).
It includes reproducible TLC checks, four negative controls, a generated schedule replayed through C# on v2/v3,
and CI integration. The [stability implementation](stability-and-retries.md) subsequently reproduced and fixed
the snapshot-expiry recovery defect, checked by BDD and PyIceberg. Universal implementation correspondence
remains unproven.

## Recommendation

Start with a small TLA+ model of commit, conflict, unknown outcome and cleanup, connected to executable C#
regression scenarios. Then prove selected pure functions, especially delete applicability and pruning, if the
first pilot demonstrates value. A proof of the entire Iceberg implementation would be a much larger undertaking.

The repository already has strong complementary evidence: model-based tests, deterministic interleavings, fault
injection, a deep table validator, cross-engine oracles, mutation testing and spec traceability
([verification roadmap](roadmap.md#verification-roadmap)). Formal methods should target the state spaces and
universal properties these tests cannot exhaust.

## What a guarantee would mean

The unit of assurance should be a specific property, tied to a pinned Iceberg section and an explicit set of
assumptions. For each property record the model or proof, implementation entry points, executable checks,
tool versions, bounds and remaining trust assumptions.

- Model checking establishes properties for the explored model and its configured bounds. It does not prove
  that the C# implementation refines the model.
- A deductive proof can cover unbounded inputs to a precisely specified function. It still depends on the
  specification, verifier and correspondence with the code actually executed.
- Cross-engine tests establish interoperability on the cases exercised. Agreement between engines is evidence,
  not the definition of conformance; the pinned specification remains authoritative.
- Catalog atomicity, storage durability and reader visibility are environmental assumptions unless separately
  verified. A library proof cannot establish AWS service internals.

TLA+ supports state-machine modeling, model checking and proofs; its author describes the distinction in
[A High-Level View of TLA+](https://lamport.azurewebsites.net/tla/high-level-view.html).

## Candidate paths

| Priority | Target in this repository | Property to establish | Suggested mechanism |
|---|---|---|---|
| 1 | `SnapshotProducer`, `TableMutations`, `StagedCommit`, catalog swaps | Successful operations have a legal serial order; conflicts preserve other writers; uncertain commits do not trigger unsafe replay or cleanup | TLA+/TLC plus C# trace replay |
| 2 | `Core/Expressions/Evaluators.cs`, transforms and bounds | Pruning never excludes a file containing a matching row; include null, NaN, negative temporal values and Unicode order | SMT-backed proofs of small pure rules, backed by existing property tests |
| 2 | `Data/DeleteFiles.cs` | Correct sequence-number inequalities, field-ID matching, partition scope and position/DV targeting | Small executable specification and deductive proofs |
| 3 | `Core/Metadata/TableMetadataBuilder.cs` | Valid refs, unique assigned IDs, increasing sequence numbers, correct v3 row-ID allocation | Transition model and invariant checks after every update |
| 4 | Avro/Puffin/DV decoders | Every length and offset stays in bounds before allocation or slicing | Proofs for isolated decoding kernels, retaining corruption tests and fuzzing |

[Dafny](https://dafny.org/latest/DafnyRef/DafnyRef.html) is a candidate for verified pure kernels. Prefer executing
the verified implementation, where practical, over maintaining a proof-only copy beside handwritten C#.
Investigate generated-code integration and target-framework compatibility in a spike before adopting it.

[Microsoft Coyote](https://microsoft.github.io/coyote/) is a useful optional bridge to systematically explore C#
async schedules. Its documentation explicitly distinguishes systematic testing from verification. Evaluate
compatibility with this repository's runtime and dependencies; use controlled catalog/file-I/O boundaries rather
than expecting it to schedule AWS services or native Parquet internals.

## Athena, Glue and multiple inserts

Interpretation: “multiple insert mechanism” means concurrent insert writers to one Iceberg table, including C#,
Athena and Spark/Glue jobs. A single insert containing many rows is already represented by `InsertAsync`.

Athena is a query engine; Glue Data Catalog holds the metadata pointer. Spark running in an AWS Glue job is
another writer using that catalog. They coordinate through optimistic concurrency, not through a common
application queue. Athena requires Glue optimistic locking for these tables; using incompatible locking can
break transactions ([Athena considerations](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)).

For two independent batches A and B prepared against S0:

1. A writes immutable files and commits S1 using a conditional pointer update.
2. B loses its conditional update.
3. B refreshes metadata, validates that its prepared append is still admissible, rebuilds the required metadata
   against S1 and commits S2, reusing compatible data files.

Both batches become visible. This does not enforce unique business keys or deduplicate a second submission of
the same batch. The Iceberg specification permits rebasing appends and requires operation-specific checks for
replacements ([commit conflict resolution](https://iceberg.apache.org/spec/#commit-conflict-resolution-and-retry));
the [reliability documentation](https://iceberg.apache.org/docs/latest/reliability/) explains work reuse.

Do not extend this rule automatically to `INSERT INTO target SELECT ... FROM target`: its computed input depends
on a read snapshot. Pin that input snapshot or explicitly define whether retries rerun the query. Independent
multi-statement inserts also do not acquire all-or-nothing semantics merely by being submitted together.

## Decide from the operation's assumptions and the intervening changes

A changed metadata pointer is a reason to inspect and validate, not enough information to choose a retry.
Partition disjointness alone is insufficient: partition evolution, global equality deletes and predicates can
cross that boundary.

| Change since planning | Independent append | UPDATE / MERGE / upsert / group replacement | Compaction or prepared file replacement |
|---|---|---|---|
| Another append | Rebase compatible prepared files | Reuse only if read/predicate dependencies remain valid; otherwise replan | Reuse only if all source files and relevant delete state remain valid |
| Deletes or rewrites outside the operation's dependencies | Rebase | Validate disjointness, then reuse; conservative fallback is replan | Validate dependencies, then reuse |
| Matching rows added, changed or deleted | Append normally remains independent | Replan only if the API permits applying intent to current state; otherwise conflict | Reject stale replacement; start a fresh compaction plan |
| Target files rewritten | Rebase | Rescan; never replay stale row positions | Reject stale file plan and replan |
| Schema/spec/order/format changed | Check field IDs, types, required fields/defaults, retained spec and format compatibility | Rebind by field ID, validate inputs and replan; reject incompatible changes | Revalidate file/schema/spec and lineage assumptions |
| Table dropped and recreated with another UUID | Reject operation against replacement table | Reject | Reject |
| Rollback or branch divergence | Explicit policy required; do not silently reinterpret the target | Reject old plan; explicit fresh planning if allowed | Reject old plan |
| Starting history expired or unavailable | Append may remain independent if identity and compatibility checks hold | No optimistic claim of disjointness; fresh planning if permitted, otherwise reject | Reject unvalidated old plan |
| Commit outcome unknown | Resolve first | Resolve first | Resolve first |

This table proposes policy boundaries; it is not a claim that every cell is implemented today. Serializable
execution of a replanned command also differs from preserving the caller's original observation. For example,
“set balance to 100 only if version was 7” must preserve that precondition through retries. User callbacks may
run repeatedly: retryable calculations must be deterministic relative to their declared inputs and have no
external side effects. Freeze source batches and external inputs per logical command.

AWS distinguishes retrying a catalog commit from rerunning a transaction after a data conflict. Its examples
use application retries for the latter ([AWS concurrency guidance](https://aws.amazon.com/blogs/big-data/manage-concurrent-write-conflicts-in-apache-iceberg-on-the-aws-glue-data-catalog/)).
Do not assume Spark's retry properties control Athena's internal retry policy.

## What exists, and what merits a focused audit

Code inspected for this evaluation:

- `SnapshotProducer.CommitAsync` refreshes and retries commit conflicts; overwrite/delete validation checks
  intervening data, deletes, missing sources and ancestry. `WithSnapshotIsolation()` disables that conflict
  filter; its semantics deserve a separate review before treating it as equivalent to Java snapshot isolation.
- `TableMutations.RetryAsync` replans after conflicts, and `RunAsync` retains inserted data files across attempts.
  Keys and some predicates are bound before the loop. This makes schema evolution a specific audit target:
  validate retained bindings and prepared files, including a metadata-only change that leaves the branch head
  unchanged. Test both first-attempt commit and conflict retry; do not assume every metadata change causes CAS
  failure at the producer's planning boundary.
- `CommitRetryPolicy` now reads `commit.retry.total-timeout-ms`, validates numeric bounds and uses a shared
  monotonic budget across Data-layer producer/replan/maintenance paths. Status recovery remains independent.
  Catalog schema/spec helpers and whole-job deadlines remain outside that Data-layer budget.
- The original producer/staged snapshot-presence recovery converted absence to a retryable conflict. The
  expiry/error-response combination is now reproduced and fixed: shared `CommitRecovery` requires positive
  evidence for every table, otherwise leaving the outcome unknown. Status reads have an independent retry
  budget and uncertainty protects objects against replay/discard.
- `AthenaQueryRunner` now accepts caller-supplied submission tokens, preserves a token after ambiguous submission,
  exposes service error details, and supports `ResumeAsync` by execution ID. Persistence of the request and ID
  across process restarts remains the application's responsibility.

Athena's [StartQueryExecution contract](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html)
returns the same execution ID for the same request token. Reuse that token to recover a submission whose outcome
is uncertain. A genuinely new execution after a confirmed failed query needs a new token and a separate replay
decision; the old token only retrieves the old execution.

A command ID in snapshot summaries helps detect success, but is not an atomic deduplication constraint. Two
independent callers can both observe an absent ID and append. Expiry can also remove the evidence. Exactly-once
claims need a durable deduplication protocol with atomic enforcement at the commit boundary and a stated
retention/recovery contract, not just a lookup before retry.

## First model and acceptance criteria

Keep the first model finite: two writers, one table, two keys, a few files, bounded commit attempts, plus a
maintenance actor. Abstract file contents as row sets; model metadata versions separately from branch heads.
Include table UUID, schema/spec identity, prepared-file dependencies, sequence numbers, a record of committed
logical commands and an observer's potentially incomplete evidence of them.

Actions: read, prepare, conditional commit, lose CAS, refresh, validate, replan, lose response, resolve, rollback,
expire and cleanup. Start with append and one replacement operation, then add delete applicability and v3
lineage. Model multi-table atomicity separately after the single-table protocol is understood.

Check these safety properties:

1. Readers see complete committed snapshots and immutable metadata.
2. Every successful mutation admits a serial order consistent with completed operations; another writer's
   unrelated rows survive. Record reads and returned outcomes, not just final row counts.
3. A successful append contributes its prepared batch once per logical command **when the modeled deduplication
   protocol is enabled**. Without that protocol, assert only once per successful execution.
4. Cleanup never deletes a file reachable from a retained snapshot/ref or protected by an unresolved commit.
5. Stale positions cannot target rewritten files; deletes obey sequence and partition rules; v3 IDs do not
   collide and rewrites preserve surviving row identity.
6. Reused work remains valid under schema changes and table identity changes.

Check bounded termination separately from eventual success: cancellation and retry deadlines terminate the
operation, but sustained contention need not allow a writer to succeed. Any eventual-success theorem must state
fairness, service availability and eventual quiescence assumptions.

For each counterexample, add a minimal BDD scenario first and replay its schedule through the existing catalog
and file-I/O injection boundaries. Preserve the trace as a regression fixture outside `tests/**/fixtures/`
(those directories are outputs of other engines). Run deep `TableValidator` checks and cross-engine checks for
observable behavior. Demonstrate that deliberately broken CAS, stale-position reuse and unsafe unknown-outcome
retry each fail the model or replay checks.

Pilot exit criteria: documented assumptions and spec links; reproducible model-check command and bounds; at
least one successful model-to-C# trace replay; negative controls caught; and a CI job with pinned dependencies
and action SHAs. Passing this pilot supports the named properties within those limits, not a blanket
“Iceberg conformant” certification.

## Delivery order

1. Audit unknown-outcome evidence, retained schema bindings and retry deadlines; add focused failing scenarios
   for any confirmed defects before changing runtime behavior.
2. Implement the single-table model and trace replay. Extend existing spec tags/traceability with the assurance
   record rather than introducing a competing conformance inventory.
3. Extract an internal retry decision with explicit outcomes: reuse after validation, replan, reject, or remain
   unknown. Capture read predicates and source-file/delete dependencies before attempting selective reuse.
4. Keep full replanning as the conservative default for row mutations; optimize only cases justified by the
   model and measured workloads.
5. Verify against real Glue with simultaneous C#/Athena inserts and compaction/DML overlap. The handover records
   AWS validation as outstanding; emulation cannot close that evidence gap. Add crash/restart submission recovery
   for the Athena runner independently of the C# table-write loop.

The initial evaluation changed documentation only. The linked pilot now supplies bounded model-checking and C#
replay evidence. Subsequent runtime recovery fixes and their validation are recorded in the stability design.
No unbounded proof or real AWS validation is claimed.
