# Work toward the full-stack 0.0.1 capability contract

Owner scope: **full stack, including the REST server**. Work from docs/capability-baseline.md (C01–C40, S01–S12).
No version cut or real publish until the capability contract is agreed; real publishing also needs owner confirmation.
Local tracker: gitignored by repository policy. Previous handover/history: temp/capability-audit/handover-TODO.md.

## Implementation order

- [x] F13 — Apply write defaults to absent typed members, including nested structures; preserve explicit nulls and unmapped values on updates. Add contract, BDD and cross-engine evidence.
- [x] F04 — Start dependencies before dependent test suites; fail early on invalid modes. Verify orchestration without Docker using command stubs.
- [x] F03 — Enforce required integration dependencies in CI and expose skipped tests. Spark path/ownership corrections passed CI 40385929772; retain that evidence.
- [x] F09 — Add Mumbling to CI fuzz matrix; make actual scheduling and informational mutation thresholds explicit; add version-update automation.
- [x] F10 — Bound server idempotency state per principal across both stores; verify multi-instance quota/replay behavior. Document deployment, recovery, shared configuration and observability.
- [x] S06 — Complete client credential-refresh API and replay recovery, with protocol and consumer tests.
- [x] F01/F11 — Deliver the full selected stack (11 libraries plus server), update package smoke/release artifact checks and examples; no release cut.
- [x] F07 — Capture/check public API contracts for the included projects; document compatibility expectations.
- [x] F08/F12 — Document/measure resource envelope and declare a tested runtime/OS/native-dependency matrix; improve defaults only with evidence.
- [x] F05 — Align native local/S3 and SQLite support claims; explicitly qualify other providers and Azure/GCS vending versus FileIO.
- [x] F02 — Keep README/package/coverage/roadmap/handover and capability baseline consistent with the completed changes.
- [ ] F06 — Real AWS/GCS/ADLS verification: inspect available configuration, prepare runnable checks; actual runs require configured cloud accounts and isolated resources. Do not claim emulator evidence as real-service verification.
- [ ] F01 — Finalise C01–C40 included/experimental/excluded contract and S01–S12 acceptance evidence; leave owner/business support decisions explicit.

## Verification and landing

- [x] Targeted tests for each behavior; changed external behavior gets a cross-engine case.
- [x] Full unit suite, spec matrix, package consumers, API snapshot and script checks (rebased local checks pass).
- [x] Required cross-engine/regression and Windows/macOS CI at final SHA 8833cf6; remaining skips explained below.
- [x] PR #21 already merged; follow-up PR #26 is rebased onto c599bc9, pushed and green. No direct main push or merge performed.

## Current evidence

- Assessment baseline: unit 1,131 passed; local REST server 141 passed/14 unavailable-dependency skips.
- CI 40385929772 (2d430f2): unit, cross-engine and regression all passed; Spark ownership correction now verified.
- Confirmed F13 before fix: optional unmapped Extra with write-default=7 writes null; required Extra throws.

## 28 October progress

- PR #21 merged; #23 split Puffin and #24 changed fingerprint/reflection/AOT tooling. Preserved local work on fix/full-stack-readiness and rebased onto c599bc9.
- Implemented F13, F03, F10 and platform/resource checks; local verification passes, final required CI remains pending before marking those complete.
- Full unit suite: 1,150 passed, zero failed/skipped (823 Spec, 109 NetStandard, 95 LINQ, 34 EF, 89 REST BDD).
- All 11 dev packages consumed; packaged EF client exercised through published REST executable; restart preserves table and idempotent replay.
- Five orchestration tests pass. API snapshots generated, checker rejects intentional drift; must regenerate after merged Puffin changes before final push.
- Quota/replay/refresh/online-backup targeted server tests: 23 passed before rebase; full server rerun pending.
- Resource probe recorded deterministic 10k/100k workloads; working set and row-group limits are documented without inventing a universal SLA.
- AWS repository variables remain absent. Asked owner whether a sandbox is available or cloud stays experimental; no cloud resource changes/publish performed.
- Full-stack support proposal is in docs/support-contract.md, including C01–C40 selection and explicit exclusions. Owner review remains pending; no new release cut.

- Rebased full server suite: 147 passed / 14 unavailable-dependency skips, zero failures. Core scenarios include quotas, credential replay/refresh and live backup recovery.
- Regenerated API snapshots after the Puffin split (141 exported Puffin declarations), and the checker passed.
- Remaining qualification: fresh CI cross-engine/Windows/macOS; real cloud deferred pending sandbox details or explicit experimental classification; support proposal owner review.

- Invalid idempotency capacities now fail startup; targeted configuration tests added. Fresh post-rebase full-stack package/server smoke passed.

- Opened PR #26 https://github.com/rtkelly-labs/Iceberg.NET/pull/26 on fix/full-stack-readiness. CI run 40680480644: unit and Windows/macOS platform qualification passed; cross-engine/regression running. Local server startup validation: 10 passed.

- Added a compiled API-contract fixture: nullable/generic/constant/ref/extension signatures are checked, private-only changes are ignored, and removing an extension marker fails the gate. All six Python/tool contracts pass; Ruff checks/formatting pass.

## Final qualification evidence

- PR #26: https://github.com/rtkelly-labs/Lake.NET/pull/26
- Final SHA: 8833cf67bf453fb361d744a5b74ca8c1fd906389.
- CI run 40682028959: all checks passed (unit, cross-engine, regression, Windows, macOS, aggregate ci; CodeQL passed).
- Cross-engine artifacts: Behaviour 67, Ecosystem 58, REST server 163 passed; zero skips. Both new default scenarios and Azurite ran.
- Regression artifacts: 267 passed / 3 expected skips (Nessie format-v3 unsupported; two real-Athena cases need AWS), plus REST protocol 110 passed / zero skips.
- Unit: 1,150 passed with no skips; six tool/Python contract tests pass; API/source-compatibility gate and 11-library/published-server package flows passed in CI.
- Platform qualification passed on both Windows and macOS.
- Downloaded TRX evidence retained under temp/capability-audit/ci-cross-results and ci-regression-results.

## Remaining external/release gates

- [ ] F06: provide an isolated real-cloud sandbox, or accept the explicit experimental classification in the support contract. No AWS role/warehouse repo variables are configured and no cloud operations were performed.
- [ ] Owner review of docs/support-contract.md and remaining capability/operational exclusions before cutting 0.0.1.
- [ ] GitHub now resolves this repository as rtkelly-labs/Lake.NET. Verify repository/package links and NuGet trusted-publishing policy after that rename before any publish; package branding was not renamed by this work.
- [ ] Close outstanding PRs before a version cut, then dry-run the cut and obtain separate owner confirmation for a real publish.

No 0.0.1 release cut, workflow dispatch or real package publication was performed.
