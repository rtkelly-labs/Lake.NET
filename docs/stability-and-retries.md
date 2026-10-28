# Stability and retry mechanisms

Design baseline: 2026-10-26; write-budget/binding hardening: 2026-10-28. This page distinguishes the service contract, the implementation mechanism and the
remaining verification work. It complements [state management](state-management.md) and
[chaos engineering](chaos-engineering.md).

## Separate four recovery decisions

1. **Transport retry:** resend the same request under the AWS SDK or an advertised REST idempotency contract.
2. **Commit retry:** refresh metadata, validate assumptions, and try a new conditional metadata update after a
   definite conflict. Compatible prepared files may be reused.
3. **Transaction replan:** recompute rows, predicates and file positions from the winner's state. This changes
   the plan and is allowed only by the command's semantics.
4. **Status recovery:** read evidence about the original attempt. This must not silently become another write.

AWS's [Iceberg concurrency guidance](https://aws.amazon.com/blogs/big-data/manage-concurrent-write-conflicts-in-apache-iceberg-on-the-aws-glue-data-catalog/)
distinguishes catalog conflicts from data validation conflicts. Iceberg can handle compatible catalog retries;
application-level reruns after data conflicts need a defined semantic policy. A whole Glue job rerun is another
scope again: earlier statements or external effects may already have succeeded.

## Commit and error decision table

| Evidence | Decision encoded here | Mechanism |
|---|---|---|
| Glue metadata location or `VersionId` conflict | Refresh and retry within the commit budget | Existing `GlueCatalog` CAS and `CommitFailedException` |
| Prepared overwrite no longer valid | Reject that plan | Existing `SnapshotProducer` validation of ancestry, source files and concurrent data/deletes |
| Row mutation lost a definite commit race | Replan under its documented command semantics | Existing `TableMutations` / staged retry loops |
| Transport/service error after a possible commit | Run read-only status recovery | Shared `CommitRecovery` for producers and staged transactions |
| Every attempted snapshot positively identified in the same table UUID | Resolve as success | Snapshot evidence after refresh, even if the connection was lost |
| Snapshot absent, history expired, identity changed or evidence mixed | Remain unknown | No new write, no artifact discard |
| Status read fails | Retry the read within a separate budget | `commit.status-check.*`; never increments write attempts |
| Status checks exhaust, configuration is invalid, or recovery is cancelled | Preserve the original unknown outcome | `ResolutionFailure` retains the recovery failure separately |
| Cancellation during a pointer swap/transaction submission | Treat as potentially applied | Unknown-outcome classification and artifact protection |
| Reuse of an unresolved producer/staged transaction | Reject replay as still unknown | Per-instance uncertainty guard; staged `DiscardAsync` also preserves artifacts |

Cancellation before dispatch can still be ordinary cancellation. Once dispatch may have occurred, a cancelled
client wait does not establish rollback. Neither an HTTP response nor `ServerResponded` proves that the commit
failed; that flag is diagnostic only.

The pinned Java implementation provides the reference:
[GlueTableOperations](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/aws/src/main/java/org/apache/iceberg/aws/glue/GlueTableOperations.java)
uses version checks, reconciles ambiguous outcomes, and avoids deleting uncertain metadata. Its general
[BaseMetastoreOperations status check](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/BaseMetastoreOperations.java)
returns unknown when metadata-location evidence is absent. Java also has a strict variant with stronger
request-completion assumptions. We do not infer those assumptions from a generic error response.

This port currently uses **snapshot presence**, rather than Java's current/prior metadata-location evidence, for
its producer-level recovery. That is deliberately conservative: snapshot expiry can leave the outcome unknown
even when metadata history could still establish success. The safe fallback is uncertainty, not a retry.
Metadata-only APIs do not yet share this producer-level resolver.

## Status-check budget and observability

The resolver reads the standard [Iceberg status-check properties](https://iceberg.apache.org/docs/latest/configuration/#table-behavior-properties):

| Property | Default | Behavior |
|---|---|---|
| `commit.status-check.num-retries` | 3 | Up to three additional passes after failed reads |
| `commit.status-check.min-wait-ms` | 1000 | Initial wait before another pass |
| `commit.status-check.max-wait-ms` | 60000 | Cap on exponential waits |
| `commit.status-check.total-timeout-ms` | 1800000 | Monotonic elapsed budget for starting additional passes |

An initial pass always runs. Missing evidence completes that pass as unknown; only a failed read triggers another
pass. Counts/times must be non-negative and minimum wait cannot exceed maximum. Across a staged transaction,
the resolver uses the largest retry count/waits and the shortest elapsed budget. It refreshes every table in a
pass and requires all attempted snapshots; it does not accumulate partial success across passes.

The budget limits extra scheduling, not the duration of an already-running SDK call; SDK request timeouts remain
relevant. Recovery cancellation is wrapped as unknown so callers cannot mistake it for definite non-application.
An `iceberg.commit.status-check` event records table, attempted snapshot, read attempt, success/unknown and any
read-error type. The write-attempt count remains separate. The original commit exception stays in the inner
chain; `CommitStateUnknownException.ResolutionFailure` identifies a failed recovery.

These settings do not replace `commit.retry.*` or an application's overall job deadline.

## Shared write retry budget

`CommitRetryPolicy` now reads `commit.retry.total-timeout-ms` (default 1,800,000 ms) alongside retry count and
minimum/maximum waits. Values must be non-negative 32-bit integers; minimum wait cannot exceed maximum wait.
`CommitRetryBudget` owns one monotonic start time and retry count. Nested Data-layer operations pass that same
object explicitly, so a producer or staged replan cannot restart the clock or acquire another retry allowance.

- The initial attempt is allowed even when the retry timeout is zero. A definite conflict then stops immediately.
- Backoff grows exponentially with up to 10% jitter, capped at the maximum wait. Saturating arithmetic avoids
  integer overflow. A backoff that cannot fit in the remaining time is refused.
- Retry boundaries, refreshed plans and producers check the same elapsed budget before further catalog work.
  Already-dispatched calls are not forcibly cancelled; their service/SDK timeouts still matter. Budget expiry
  never turns an uncertain commit into a definite failure or disables the independent status-check budget.
- Mutation replans, snapshot producers, staged commits, snapshot refs/cherry-pick, expiry and data compaction use
  this budget. A staged transaction takes the shortest elapsed timeout across its tables and retains the existing
  largest-count/wait policy. Initial staging is a separate preparation call; waiting between staging and committing
  does not consume the later commit's budget.
- Each partial-progress compaction batch is an independent commit with its own budget. Exhausted groups are
  reported as failed while prior successful batches remain committed. `maxAttempts` also caps compaction attempts.

The Catalog-layer schema/spec evolution helpers still have their existing explicit retry-count APIs; this
Data-layer budget does not impose a deadline on those independent metadata calls. A whole ingestion job also
needs its own deadline across statements, service calls and checkpoints.

## Fence retained input bindings

`WritePlanGuard` captures the table UUID and schema ID when a producer/mutation/stage is created. Replanning
checks them before reusing input arrays, keys, predicates or prepared work. Producers and staged commits also
send `assert-current-schema-id` with their original UUID requirement: this catches metadata-only schema changes
that leave the data head unchanged and occur before the catalog loads its commit base.

The current policy rejects **any** schema-ID change and asks the caller to rebuild the command and inputs against
the current schema. This includes otherwise compatible renames; it is a conservative binding policy, not a claim
that every schema change makes an old data file unreadable. Schema fields continue to be read by field ID.
A dropped/recreated table is a new identity and cannot inherit the old command. Ref/expiry retries also pin the
original UUID, while permitting schema changes that do not affect those metadata-only operations.

Partition evolution is treated separately. Prepared append files retain their own `spec_id` and can commit under
a new default while the old spec remains archived. Low-level whole-file filters have an additional default-spec
fence because an arbitrary delegate may interpret partition positions. Proving finer-grained schema compatibility
and safely rebinding arbitrary caller callbacks remain future work.

## Athena submission and execution are separate identities

Athena's [StartQueryExecution contract](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html)
reuses the execution ID for a matching request token and rejects changed request parameters. Accordingly:

- Persist a 32–128 character client token, SQL, workgroup, database/catalog and result configuration **before**
  submission. Recover with the same token and identical parameters.
- `StartAsync(sql, token)` and `RunAsync(sql, token)` accept that durable identity. On an ambiguous submission,
  `AthenaSubmissionUnknownException.ClientRequestToken` retains it, including when the runner generated it.
  Recovering a generated token from an exception helps a live process; it is not durable across a process crash.
- Persist the returned execution ID. `ResumeAsync(id)` polls and retrieves that execution without starting a
  query. `AthenaQueryTimedOutException.QueryExecutionId` supports recovery after a local timeout.

The [query-status contract](https://docs.aws.amazon.com/athena/latest/APIReference/API_QueryExecutionStatus.html)
allows `RUNNING` to return to `QUEUED` during transient recovery. Continue polling the same ID. `FAILED` and
`CANCELLED` end that execution. A local wait timeout or caller cancellation does not send a stop request and
does not imply that the server stopped.

[AthenaError](https://docs.aws.amazon.com/athena/latest/APIReference/API_AthenaError.html) supplies category, type,
message and a retryability hint. `AthenaQueryFailedException` preserves them. `Retryable=true` means a new
execution might succeed; it does not prove that replaying an arbitrary mutation or multi-stage pipeline is safe.
The runner therefore does not automatically resubmit terminal failures. A deliberate rerun needs a new token,
a frozen input/replay contract and a bounded application policy. The old token still identifies the old query.

Polling uses a monotonic clock and bounded exponential intervals. SDK transport retries of the submission retain
the request's token. None of these mechanisms claims exactly-once delivery from a source queue through an
external checkpoint and into Iceberg.

## Validation and next mechanisms

The formal expiry counterexample was reproduced in C# before the fix: all four v2/v3 × single/multi-table cases
incorrectly returned success after replay. The BDD scenarios now require unknown, protect staged files and use
PyIceberg to verify rows 0, 1 and 2 appear exactly once. The spec suite tests transient/exhausted status reads,
zero elapsed budget, invalid settings, cancellation, and explicit replay/discard after uncertainty.

Athena tests cover lost submission responses across runner instances, generated-token recovery, terminal
retryable errors, queue regression and timeout/resume. moto 5.2.4 ignored request tokens; its new idempotency shim
and `EmulatorFidelityTests` exercise the actual SDK request contract. The same fidelity case is enabled for AWS;
real AWS validation remains outstanding.

Local checks on 2026-10-26: 847 spec tests, 109 netstandard compatibility tests, 27 selected Athena/telemetry
tests, four PyIceberg-backed expiry-recovery scenarios, and six SDK/emulator checks passed. The three TLC baseline
configurations pass and all four negative controls still produce their intended counterexamples. Spec traceability
and lint checks pass. These are local results; CI and real AWS runs are not claimed.

Local checks on 2026-10-28: all 874 spec tests, ten new PyIceberg-backed binding scenarios, 33 EF Core tests and
27 selected telemetry/Athena tests passed. The companion schema model passes and both new negative controls
fail their intended invariant. One full run encountered filesystem `ENOSPC`; after task-cache cleanup, the clean
rerun passed with `TMPDIR` set to the ignored repository directory `temp/t/`. No test was weakened or skipped.

Next, preserve the same four-layer separation while adding:

1. Metadata-location evidence for recovery after snapshot expiry, and durable reconciliation records for crashes.
2. Extend elapsed budgeting to the Catalog-layer schema/spec evolution helpers and whole-job orchestration.
3. Prove finer-grained schema compatibility before relaxing the conservative binding fence.
4. REST response-loss and process-restart experiments, retaining request identities and unresolved artifacts.
5. A scoped application-level rerun policy for Athena/Glue jobs, with frozen source batches, checkpoint semantics
   and independent sink validation. Do not enable whole-job retries as a substitute for that protocol.
