The full-stack capability review found missing typed write defaults, incomplete package delivery, silently skipped integrations, and an undefined server operating boundary. This change closes the actionable local gaps before the `0.0.1` contract is reviewed; it does not cut or publish a release.

Typed/EF writes now default absent members while preserving explicit nulls and existing unmapped values. The REST client reloads credentials after replayed creates/registers and exposes explicit credential refresh. Both server stores enforce an atomic per-principal idempotency budget without evicting replayable keys; new keys at capacity receive 429. Restart and online-backup recovery are tested.

All eleven libraries are packable, with a published REST server archive. Package smoke tests consume every library and run the packaged EF client against the published executable before a restart check. Public API snapshots, diagnostic resource measurements, a full-stack support proposal and operating runbooks make the current boundaries reviewable. CI now starts dependencies before dependent suites, requires named integrations, runs Azurite, records skip results and qualifies native/typed/EF flows on Windows/macOS. Dependency updates and the missing Mumbling fuzz target are configured.

Validation on the branch rebased onto `c599bc9`:

- Full unit suite: 1,150 passed, no failures/skips (Spec 823; NetStandard 109; LINQ 95; EF 34; REST behavior 89).
- Local REST server suite: 147 passed, 14 unavailable-dependency skips; the final startup-config cases passed separately (10 cases).
- Eleven `0.0.1-dev.1` packages consumed, including EF Core/Arrow/Spark/Athena and the published-server consumer/restart flow.
- API checker passes and rejects deliberate drift; five orchestration tests and a compiled API-source-compatibility fixture pass; spec matrix has zero unknown links; changelog, Python, action-SHA and documentation-link checks pass.
- Final SHA `8833cf6`: all required CI checks passed in run [40682028959](https://github.com/rtkelly-labs/Lake.NET/actions/runs/40682028959), including Windows/macOS qualification, public API gate, 11-library consumers and published server/restart flow.
- Cross-engine: Behaviour 67, Ecosystem 58, REST server 163 passed with **zero skips** (including both write-default oracle cases and Azurite).
- Regression: 267 passed / three expected skips (Nessie v3 and two real-Athena cases); REST protocol 110 passed / zero skips.
- Real cloud targets are not configured and remain experimental in the proposed contract. The repository has since been renamed to Lake.NET; repository links/trusted-publishing configuration need checking before a release, independently of these green capability checks.

`docs/support-contract.md` records the concrete capability selection and outstanding owner review. The gitignored root TODO.md tracks progress and external gates. The merged Puffin and performance/AOT changes from #23/#24 are preserved.
