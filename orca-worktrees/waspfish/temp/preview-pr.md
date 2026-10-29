Prepare `0.1.0-preview.1` after the successful alpha release. The release notes list the seven preview packages, make the emulator-only AWS verification explicit, and describe the REST server as unpublished by choice. The existing alpha notes remain unchanged.

Replace the pre-split handover with the standalone repository workflow and current follow-ups, update the roadmap and decoder-limit documentation, and set the CI Spark runtime path explicitly. The REST server test output contains its own `interop/` directory, which causes the root lookup to find the wrong jar location; the latest CI run skipped four Spark cases.

Validation: changelog release parsing, spec traceability (110 sections, zero unknown links), local documentation file links and diff checks pass. All 24 existing decoder-limit tests passed. With `VERSION=0.1.0-preview.1`, the package smoke test packed exactly seven packages; the Data and LINQ consumers ran successfully and the netstandard2.0 consumer built with zero warnings or errors. Spark execution requires CI because Docker is unavailable locally.

After this merges, run the release workflow from main with `dry_run=true` and inspect the exact package set and CI verdict. Real NuGet publication requires owner confirmation.
