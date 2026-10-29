# Local task tracking

- [x] Build and check the bounded commit/retry/cleanup TLA+ pilot.
- [x] Exercise negative controls and replay a model trace through C# catalog boundaries.
- [x] Add reproducible tooling, CI coverage, and an assurance record with limits.
- [x] Add deterministic chaos experiments linking injected faults, traces, caller errors, retry metrics and durable table state; verify all 16 cases and the related 46-test selection.

## Follow-up

- [x] Reproduce and fix the model's snapshot-expiry/unknown-outcome counterexample against the C# single-table and staged recovery paths; verify BDD and PyIceberg evidence.

## Stability mechanisms

- [x] Reproduce and fix unsafe unknown-outcome retries, including staged discard/replay and expiry.
- [x] Add bounded, read-only commit status recovery and causal diagnostics.
- [x] Add Athena submission identities, structured errors and safe polling/recovery.
- [x] Verify against the local PyIceberg oracle and encode AWS/Iceberg contracts in the design.

## Next mechanisms

- [x] Consolidate a monotonic total commit/replan budget across Data-layer write and maintenance loops, including nested staged replans.
- [x] Fence retained predicates/input arrays by original schema and table UUID, preserve archived-spec appends, and verify schema-only/reorder/drop-recreate races.
- [x] Add the companion schema-fence model, two negative controls, and an archived-spec witness.
- [ ] Extend elapsed budgets to Catalog schema/spec helpers and whole-job orchestration.
- [ ] Prove finer-grained compatible schema changes before relaxing the current conservative fence.
- [ ] Add REST network fault and process-restart experiments with durable recovery identities.
- [ ] Validate the same Athena/Glue fidelity contracts in the configured AWS sandbox.

## Pull request

- [x] Commit changes, rebase onto main at c599bc9, and push f69ae56.
- [x] Open https://github.com/rtkelly-labs/Lake.NET/pull/48 with validation and behavior limits.
- [x] Fix the failing cross-engine REST expectation; assert one-write read-only recovery, same-key replay, and unknown unapplied requests (a7a9172).
- [x] Validate all six idempotency tests and the local REST server suite (148 passed, eight environment-dependent skips).
- [x] Confirm required CI passes on a7a9172 and review any new feedback.

- [x] Rebase PR #48 onto dependency configuration update 38a1604; range-diff confirms unchanged patches (7679b8b).
- [x] Confirm CI passes on rebased head 7679b8b.

- [x] Reproduce both PR review findings with failing Athena/REST regression tests.
- [x] Preserve uncertain cancellation at producer submission; preserve Athena tokens on SDK unmarshalling failure.
- [x] Verify REST idempotency (11), Athena runner (21), PyIceberg recovery BDD (6).
- [x] Push review fixes (3f144f1); all required CI passes and both review threads are resolved. Refreshed CodeRabbit review still running.
