# Dependency updates (Renovate)

[Renovate](https://docs.renovatebot.com/) opens pull requests that update pinned versions. The configuration is
[`.github/renovate.json5`](../.github/renovate.json5). PRs come from Mend's hosted Renovate GitHub App, installed
across `rtkelly-labs` (decided 2026-10-28). The same configuration also runs locally as a plain command-line tool,
for dry runs. This page covers what it manages, the rules it follows, and how to run it. It is
the first step of the cross-repository plan in [lake/releases.md](lake/releases.md#tools-not-hand-rolled-scripts).

## What it manages

Checked with a local dry run on 2026-10-28 (Renovate 44.190.2):

| Pins | Where | Renovate manager |
|---|---|---|
| NuGet packages (Central Package Management) | `Directory.Packages.props` | `nuget` |
| .NET SDK | `global.json` (feature-band and patch updates only) | `nuget` |
| .NET tools (Stryker, SharpFuzz) | `dotnet-tools.json` | `nuget` |
| Python oracles (PyIceberg, moto, DuckDB, …) and `uv.lock` | `interop/pyproject.toml` | `pep621` |
| Emulator images whose tag is `${X_TAG:-version}` | `emulation/docker-compose.yml` | custom regex |
| Lakekeeper image (`${LAKEKEEPER_IMAGE:-repo:tag}`) | `emulation/docker-compose.yml` | `docker-compose` |
| Spark oracle image | `emulation/up.sh` | custom regex |
| GitHub Actions, pinned to SHAs with a `# vX` comment | `.github/workflows/` | `github-actions` |
| The Parquet.Net fork submodule, once merged | `.gitmodules` | `git-submodules` |

**Not managed:** the Maven jars that `emulation/up.sh` downloads (the Iceberg Spark runtime, the AWS bundle,
`hadoop-aws`, the Nessie runner). Each is pinned by a SHA-256 checksum that Renovate can't recompute, so they are
updated by hand together with the Spark image.

### Planned: Maven jars through Renovate

**Status:** planning only; nothing below is built. It waits for the DuckLake branch to merge, because that branch
also edits `emulation/up.sh`.

1. **Move the pins into a manifest.** The jar pins leave `up.sh` for a manifest (`emulation/jars.json`, or the
   `interop/engines.json` proposed in [lake/verification/regression.md](lake/verification/regression.md#reference-engine-versions)).
   Each entry has Maven coordinates, a version and a SHA-256. `up.sh` reads the manifest and checks every hash as
   it does today.
2. **Bump versions with Renovate.** A custom manager reads the manifest with the `maven` datasource.
3. **Refresh checksums on the same PR.** The hosted app can't run commands after an update, so a workflow does it
   instead. It triggers on Renovate PRs that touch the manifest, runs `scripts/UpdateJarChecksums.cs` to download
   each bumped jar and write its SHA-256 back, and pushes that commit to the PR branch. Until then, CI fails on the
   stale checksum, so a bump can never merge unverified.
4. **Keep coupled artifacts in step.**
   - `iceberg-spark-runtime` and `iceberg-aws-bundle` move together.
   - The Spark/Scala suffix (`4.0_2.13`) follows the Spark image.
   - The AWS SDK jars follow the SDK version inside the Iceberg AWS bundle, never on their own.
   - None of these are merged automatically.

Paket was considered and rejected:
- **Version resolution.** Its `http` dependencies don't resolve Maven versions, and Renovate's Paket manager
  won't update them.
- **Checksums.** As far as we know, it doesn't verify checksums of downloaded files.
- **A second package manager.** It would add one beside NuGet Central Package Management.

## Rules

| Rule | Why |
|---|---|
| Runs before 06:00 on Mondays; at most 5 open PRs and 2 per hour; lock-file refresh monthly | One batch a week instead of a constant trickle |
| Proposes a release only once it is 3 days old | Most broken or malicious releases are pulled within days |
| Major updates wait for approval on the Dependency Dashboard issue | Majors need a decision, not just a green CI |
| Patch, digest and pin updates to NuGet packages and Actions merge themselves once CI passes | Low risk, and CI is the gate |
| Never merged automatically: Parquet and Arrow (the data plane), the .NET SDK (new analyzer warnings are errors here), and everything under `interop/` and `emulation/` (labelled `oracle`) | These can change what we write, whether the build passes, or what the oracles report ([regression-suite.md](regression-suite.md)) |
| Grouped: .NET 10 libraries, AWS SDK, Parquet and Arrow, test tooling, GitHub Actions, oracles and emulators, moto (the Python package and the image together) | Packages that release together are updated together |
| Spark stays on 4.0.x | The Spark image has to match the pinned `iceberg-spark-runtime-4.0_2.13` jar |
| PR titles are `build(deps): …`, `ci(deps): …`, `build(oracles): …` | Conventional commits, which the changelog tooling reads ([lake/releases.md](lake/releases.md)) |

## Running it

**PRs: the hosted app.** Mend's Renovate GitHub App (free) is installed on the `rtkelly-labs` organisation. It reads
`.github/renovate.json5` from the default branch, so this repository starts receiving PRs once that file is on
`main`. A repository without a config gets an onboarding PR first. Things to know:

- **Auto-merge goes through GitHub's own auto-merge and the merge queue.** It only works because the `ci` check is
  required; without that check, "auto-merge once CI passes" would merge without waiting.
- **Dashboard clicks act within minutes.** The app reacts to ticking a box on the Dependency Dashboard or editing a
  Renovate PR.
- **No commands after an update.** The hosted app can't run arbitrary commands after an update. Built-in lock-file
  updates (`uv.lock`, `paket.lock` once supported) still work. Anything else, such as jar checksums, runs as a CI
  workflow on the Renovate PR.
- **Private feeds need credentials given to Mend** through its encrypted secrets, if one is ever added.
- **Run logs** are at [developer.mend.io](https://developer.mend.io), signed in with GitHub.

**Locally, to see what it would do** (no PRs, nothing written). Renovate is the npm package `renovate` (or the
`renovate/renovate` Docker image):

```bash
npm install --prefix temp/renovate renovate
GITHUB_COM_TOKEN=$(gh auth token) RENOVATE_CONFIG_FILE=.github/renovate.json5 \
  temp/renovate/node_modules/.bin/renovate --platform=local --dry-run=lookup \
  --report-type=file --report-path=temp/renovate-report.json
```

`GITHUB_COM_TOKEN` can be any read-only token. Without one, Actions lookups fail with "github-token-required"
once GitHub's anonymous rate limit runs out; everything else still works.

**Check the configuration:** `temp/renovate/node_modules/.bin/renovate-config-validator .github/renovate.json5`.

**Without the app.** If the hosted app is ever removed, the same configuration runs from a scheduled workflow
(`renovatebot/github-action`, pinned to a SHA, with a GitHub App or fine-grained token as a secret).

## Paket

The same setup covers repositories that use Paket once Renovate's Paket manager is released. The manager is
[renovatebot/renovate#37287](https://github.com/renovatebot/renovate/pull/37287); as of 2026-10-28 it is approved
and waiting to be merged, and Paket version parsing has been released since 2026-07. It will update `nuget` lines in
`paket.dependencies` and regenerate `paket.lock`. It won't update `github`, `git` or `http` dependencies. Dependabot
has no Paket support.
