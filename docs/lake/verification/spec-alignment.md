# Spec alignment

**Status:** target state. This page covers how every format's spec is pinned, linked to the tests that verify
it, and watched for upstream changes. How it works for Iceberg today is described in
[roadmap.md](../../roadmap.md#verification-roadmap), path 10.

## Specs we follow

| Spec | Pinned at | Section map | Drift check |
|---|---|---|---|
| Iceberg table spec (v1–v3, v4 watched) | apache-iceberg-1.12.0 | `docs/spec/sections.json` | ✅ `spec_trace.py drift` |
| Iceberg REST OpenAPI | same tag | `docs/spec/open-api/`, `OpenApiConformanceTests` | ✅ `tools/openapi_reference.py` |
| Iceberg view spec | same tag | `sections.json` | ✅ |
| Puffin | same tag | `sections.json` | ✅ |
| DuckLake (1.0, 1.1) | `docs/stable` (1.1) | `docs/spec/ducklake-sections.json` | ⏳ add one |
| Delta protocol | to pin: a tagged `PROTOCOL.md` | ⏳ `docs/spec/delta-sections.json` | ⏳ |
| Apache ORC specification ([D8](../decisions.md#d8)) | to pin: the ORC release the port follows | ⏳ `docs/spec/orc-sections.json` | ⏳ |
| Parquet format (field IDs, logical types, variant encoding) | to pin: a `parquet-format` tag | ⏳ the sections `Lake.Data` relies on | ⏳ |

## Target

1. **One tool covers every spec.** `tools/spec_trace.py` reads one section map per spec and accepts tags in
   the form `[Spec("delta:deletion-vectors")]` and `@spec:delta:deletion-vectors`. Iceberg's existing tags keep
   working without a prefix, so nothing needs re-tagging. CI fails if a tag points to a section that doesn't
   exist, in any spec.
2. **Each format gets its own matrix.** `docs/spec-traceability.md` becomes one generated page per spec,
   uploaded as CI artifacts. Each page shows linked sections out of implementable sections, and which
   sections are explicitly refused.
3. **Each format declares what it supports.** A committed file per format (`docs/spec/<format>-support.json`)
   gives every section a status: `supported`, `refused` (with the error users see, such as ORC or an
   unsupported Delta table feature) or `planned`. A section marked `supported` must have a linked test, and a
   `refused` section must have a test showing the refusal. This turns "what we cover" from prose into
   something CI can check.
4. **Every spec gets a drift check.** The weekly job compares each pinned spec with upstream and fails on
   changes nobody has reviewed. Reviews are recorded in `docs/spec/acknowledged-drift.json`, as Iceberg's
   are today.
5. **Spec vectors come first.** Where a spec ships test vectors or an acceptance suite, they are the first
   tests written:

   | Format | Vectors |
   |---|---|
   | Iceberg | Appendix B/D hashes and transforms |
   | DuckLake | The spec's DDL script |
   | Delta | DAT reader cases |

   All of them are committed as fixtures and regenerated, never edited by hand.

## Done when

- Every format has a section map, a support declaration and a drift check.
- Every `supported` section has at least one linked test that runs in the `unit` suite or names the suite it
  needs.
- The per-spec linked-section ratio is recorded in the matrix, and CI fails if it drops.
