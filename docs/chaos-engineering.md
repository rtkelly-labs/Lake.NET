# Chaos engineering and error propagation

Formal models define the legal outcomes of a fault. Chaos experiments check how the implementation behaves at
the failure boundary and how that behavior reaches the caller. The useful evidence is the whole chain:

```text
injected storage/catalog fault
    -> catalog error or ambiguous commit
    -> retry, resolution, or propagation
    -> caller result + trace/metrics
    -> independently reloaded table and retained files
```

The repository already has exhaustive write-point injection (`FaultInjectionTests`), deterministic concurrent
writers (`InterleavingTests`), SDK-level S3 throttling (`ThrottlingTests`), telemetry listeners and soak tests.
`ChaosPropagationTests` connects those concerns in a deterministic in-process pilot. It runs in the normal unit
suite without Docker or AWS. This does not yet exercise a deployed multi-service ingestion pipeline.

## First experiment matrix

Every case runs on table formats v2 and v3, starting with row 0 and trying to append row 1.

| Fault boundary and timing | Caller outcome | Durable rows | Retry expectation |
|---|---|---|---|
| Metadata write fails before storage | I/O error | 0 | No transaction retry |
| Metadata write lands, then raises an I/O error | I/O error | 0 | No transaction retry; pointer never advanced |
| Catalog rejects the pointer swap as a definite conflict | Success | 0, 1 | One replan, two attempts |
| Connection lost before the pointer swap applies | Unknown commit outcome | 0 | No blind replay |
| Connection lost after the pointer swap applies | Resolved success from retained snapshot evidence | 0, 1 | No replay |
| Error response after swap, attempted snapshot still retained | Resolved success | 0, 1 | No replay |
| Error response after swap, then recovery reads exhaust their budget | Unknown commit outcome | 0, 1 | Preserve uncertainty and original commit error |
| Cancellation exception at the catalog swap boundary | Unknown commit outcome | 0 | Dispatch may have occurred; no replay |

The subsequent [stability implementation](stability-and-retries.md) resolves positive snapshot evidence even
after connection loss, and treats absent evidence as unknown. The separate BDD expiry schedule reproduces and
guards against the formal model's unsafe-retry counterexample. A caller exception alone cannot establish
whether an insert landed; recovery relies on positive evidence and preserves uncertainty otherwise.

## Evidence collected and asserted

Each experiment has its own application activity, table and run ID. The injection hooks attach a test-only
`chaos.inject` event to the active library span, with boundary, experiment, run and error type. The storage hook
observes the exact exception object before it propagates; the catalog hook does the same around the swap.

The tests check:

- Injection, catalog operation and logical commit share a trace, with the catalog span under the commit span.
- The commit's status, outcome, attempt count, retry events and metric measurements agree with the caller result.
- Wrapping preserves the original injected exception in the caller's exception chain when an error escapes.
  If recovery also fails, the report records that second injection separately.
- Only the definite-conflict case retries; every successful insert becomes visible once.
- Independent reloads see the expected complete table, and deep `TableValidator` checks all retained snapshots.
- Unknown and cancelled attempts preserve every written artifact tracked by the injector.

Every case writes `temp/chaos/<experiment>-v<version>-<run>/report.json`, including on assertion failure. Reports
contain injected boundaries/times, span IDs, parent IDs, tags/events/timing, metric measurements, caller exception
types, visible rows and any assertion failure. Temporary warehouses are removed after the report is written.
The unit CI job uploads the reports as `chaos-error-propagation`. The run IDs separate evidence from repeated
executions; faults are scheduled at explicit boundaries rather than triggered by wall-clock sleeps.

```sh
dotnet test tests/Iceberg.Net.Spec.Tests --filter FullyQualifiedName~ChaosPropagationTests
```

Local validation on 2026-10-26: all 16 chaos cases passed. The combined chaos, existing write-fault and
commit-protocol/recovery selection passed all 46 tests; the spec traceability check found no unknown sections.
CI report upload is configured and has not yet run for these changes.
Subsequent recovery hardening changes the connection-loss/cancellation classifications as shown above; the
847-test spec run includes the updated chaos cases and checks the new status-read events and failure causes.

The `chaos.inject` events are supplied by the test harness. Ordinary production tracing does not acquire these
events from this change. In particular, generic metastore catalog spans currently mark failures without always
recording the exception type; the injection event and enclosing commit span supply that information in these
experiments. This is a diagnostic coverage gap to address separately if production investigations need it.

## Extending this to the actual pipeline

Keep an experiment contract: a stable fault schedule, the expected state transitions, the permitted caller
outcomes, the maximum retry budget, and the independently checked final state. Reuse counterexample schedules
from the formal model when adding multi-fault cases. Shrink a discovered random failure into a deterministic
regression rather than retaining only its random seed.

The next useful stages are:

1. **Compound recovery failures:** the insert/response-loss/advance/expiry counterexample now has a C# BDD
   reproducer and fix on v2/v3 for single and multi-table writes. Extend it to REST faults and process recovery.
2. **REST/SDK boundary faults:** delay or drop responses before and after server-side commit, disconnect during
   refresh, and verify idempotency-key reuse. Correlate client and server spans and independently load the table.
3. **Process recovery:** terminate the producer between file preparation, commit submission, response receipt
   and checkpoint persistence. Restart with the same durable command/input identity; check duplicate prevention
   and which files remain protected. In-process exceptions do not establish crash/restart correctness.
4. **Reader and maintenance interference:** schedule compaction, snapshot expiry and orphan cleanup during the
   writer's prepare/commit/recovery windows; check lineage and delete applicability as well as row equality.
5. **Athena/Glue ingestion:** carry a logical batch ID across producer traces, Athena execution IDs or Glue job
   run IDs and downstream validation. Submission recovery must reuse the original Athena request token;
   starting a new query is a distinct replay decision. Validate at the source/checkpoint and Iceberg sink, since
   table atomicity alone does not establish exactly-once delivery across the pipeline.

Distributed runs need propagated trace context and durable correlation IDs: .NET activity ancestry only links
calls on the local async flow. Treat a healthy retry as a recovered fault rather than an incident, and preserve
the distinction between data loss, duplicate delivery, an unavailable operation and an unresolved outcome in
the experiment result.
