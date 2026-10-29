## Summary

A commit can succeed, lose its response, and have its snapshot evidence expire before recovery. Treating the missing snapshot as a failed commit can replay an already-applied insert. This change keeps absent or mixed evidence unknown and permits success only from positive evidence in the original table identity.

- Share bounded, read-only status recovery across producers and staged transactions; preserve uncertain artifacts and prevent replay/discard of unresolved instances, including cancellation and unexpected multi-table swap errors.
- Share monotonic retry counts/time budgets across Data-layer writes, replans and maintenance. Fence retained input bindings by schema/table identity while preserving valid archived-spec appends.
- Add recoverable Athena submission tokens, execution resumption and structured service errors without automatically resubmitting SQL. Add an Athena idempotency shim and a fidelity test enabled for both emulation and AWS.
- Add bounded TLA+ commit and schema models, negative controls, generated C# trace replay, deterministic chaos reports, cross-engine regression scenarios and a required formal CI job.

See `docs/stability-and-retries.md` for the recovery contracts and implementation boundaries, and `formal/commit-retry/README.md` for the model assumptions.

## Behavior changes and limits

- Any schema-ID change currently rejects a retained write plan, including otherwise compatible renames; callers must rebuild the command and input bindings.
- Cancellation after possible dispatch may surface as an unknown commit/submission outcome rather than definite cancellation. A local Athena timeout does not stop the server-side execution.
- Retry elapsed budgets cover Data-layer paths; Catalog schema/spec helper deadlines, durable crash reconciliation and finer schema compatibility remain follow-ups.
- Model checks establish bounded model properties, not whole-library conformance. Real AWS validation remains outstanding.

## Validation

Rechecked after rebasing onto latest `main` (`c599bc9`):

- Full spec suite: **876 passed**.
- New PyIceberg-backed recovery/binding BDD cases: **14 passed**.
- EF Core: **33 passed**.
- Selected Athena/telemetry tests: **27 passed**.
- TLC: all four baseline configurations pass; six broken-protocol controls violate their intended invariant; both coverage witnesses are reached and the committed replay trace matches.
- Spec traceability: **110 linked sections, no unknown links**; diff checks pass.

Earlier local validation also passed 109 netstandard compatibility tests and six real-SDK emulator checks, including response loss after accepted Athena submission. CI will run the complete configured suites against the rebased tree.

## Linked Issues

No linked issue.

## Type of Change

- [x] `type: bug` (commit/recovery correctness)
- [x] `type: feature` (recovery mechanisms and formal/chaos checks)
- [x] `breaking-change` (stricter schema fencing and uncertain-outcome classification)
- [ ] `type: docs` (documentation only)
- [x] `type: maintenance` (CI and emulation)

## Verification Checklist

- [x] My branch is rebased onto latest `main` with a clean linear history.
- [x] Unit and/or integration tests have been added or updated.
- [ ] All required CI checks pass (`ci` / `test` / `lint`).
- [x] Relevant documentation has been updated.

Squash-only merging and automatic branch deletion are already enabled for the repository.
