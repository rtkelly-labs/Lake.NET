Formalise the full capability set before deciding the `0.0.1` release contract. The owner selected the full stack including the REST server. No new release version is cut; the published alpha changelog remains unchanged.

The capability baseline inventories all 12 source projects and 40 capabilities, distinguishes implementation from verification and distribution, records 13 prioritised findings, and proposes 12 full-stack acceptance scenarios. It includes a confirmed typed-write defect: unmapped columns do not receive their declared write default (optional columns become null, required columns fail). This PR documents that defect; it does not change production write behavior.

Align README, package copy, coverage, catalog, LINQ, roadmap and handover claims with the observed implementation. Correct the CI Spark jar path, which enabled four formerly skipped server cases; the resulting CI run exposed root-owned Spark output, so the REST test harness now uses the established shared-oracle permission handoff while preserving Spark's exit status.

## Verification

- Unit suite: 1,131 passed, no failures/skips (Spec 816; NetStandard 109; LINQ 84; EF Core 33; REST behavior 89).
- REST server with `MOTO_PORT=5050`: 141 passed, 14 dependency-related skips, no failures; the changed harness compiles. Docker is unavailable locally.
- Public typed-insert probe reproduced the write-default defect for optional and required fields.
- Spec matrix: 110 linked sections, zero unknown; changelog parsing, relative documentation file links and diff checks pass.
- CI run 40384002348: Behaviour 65 and Ecosystem 58 passed; server 153 passed, one Spark ownership failure and one Azurite skip. The harness correction needs the new CI run.

The baseline records remaining release decisions, including full-stack distribution, supported platforms/cloud integrations, API and resource guarantees, and server operations. Release preparation is deferred until those criteria are agreed.
