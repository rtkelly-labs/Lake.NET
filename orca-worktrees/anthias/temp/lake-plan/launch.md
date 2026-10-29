# Launching units in Orca

Each agent unit runs in its own Orca worktree, created from its base ref, with the agent started on its brief.
Launch a unit only when everything in its `depends_on` is merged (or done, for human units). Units in the same wave
with no dependency between them can run at the same time, subject to the locks in README.md.

## Wave 0

### G0: Freeze the online session and back up its branches (human)

Depends on: nothing

Not an Orca agent launch: see `units/G0.md`.

### G1: Decide D3, D5, D6, D12 and the fork's stable branch (human)

Depends on: nothing

Not an Orca agent launch: see `units/G1.md`.

## Wave 1

### U01a: Land the vision and target-state docs (claude (this session) or agent)

Depends on: nothing

Not an Orca agent launch: see `units/U01a.md`.

### U01b: Relicense to Apache-2.0 (claude (this session) or agent)

Depends on: nothing

Not an Orca agent launch: see `units/U01b.md`.

### U01c: Add the Renovate config (claude (this session) or agent)

Depends on: U01a

Not an Orca agent launch: see `units/U01c.md`.

### H0: Install the Mend Renovate GitHub App (human)

Depends on: nothing

Not an Orca agent launch: see `units/H0.md`.

### U02: Recover the DuckLake incubation from the online session (antigravity)

Depends on: G0

```bash
orca worktree create --repo name:Iceberg.NET --name lake-recover-ducklake --base-branch ef5eb9d --agent antigravity --setup run --no-parent --activate --comment "lake-plan U02" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/U02.md exactly. Read it in full first, then AGENTS.md."
```

### U05: Land or park open PR #25 and stale worktrees (human)

Depends on: nothing

Not an Orca agent launch: see `units/U05.md`.

### R1: Declare Renovate in rig's GitHub Apps governance (antigravity)

Depends on: nothing

```bash
orca worktree create --repo name:rig --name renovate-app --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan R1" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/R1.md exactly. Read it in full first, then AGENTS.md."
```

### R2: Shared Renovate preset for the estate (antigravity)

Depends on: U01c

Not an Orca agent launch: see `units/R2.md`.

### R3: Package and version dashboard: `rig estate packages` (antigravity)

Depends on: nothing

```bash
orca worktree create --repo name:rig --name estate-packages --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan R3" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/R3.md exactly. Read it in full first, then AGENTS.md."
```

### R4: Repository rename support: `rig estate rename-repo` (antigravity)

Depends on: nothing

```bash
orca worktree create --repo name:rig --name estate-rename-repo --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan R4" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/R4.md exactly. Read it in full first, then AGENTS.md."
```

### R5: NuGet package retirement helper (antigravity)

Depends on: R3

```bash
orca worktree create --repo name:rig --name estate-retire-packages --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan R5" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/R5.md exactly. Read it in full first, then AGENTS.md."
```

### R6: Orca setup for Iceberg.NET agent worktrees (antigravity)

Depends on: nothing

```bash
orca worktree create --repo name:rig --name orca-iceberg --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan R6" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/R6.md exactly. Read it in full first, then AGENTS.md."
```

### U06: Commit orca.yaml for agent worktrees (antigravity)

Depends on: R6

```bash
orca worktree create --repo name:Iceberg.NET --name orca-yaml --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan U06" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/U06.md exactly. Read it in full first, then AGENTS.md."
```

## Wave 2

### H1: Create the fork's stable branch (human)

Depends on: G0, G1

Not an Orca agent launch: see `units/H1.md`.

### U03: Recover the Parquet.Net backend and fork submodule (antigravity)

Depends on: U02, H1

```bash
orca worktree create --repo name:Iceberg.NET --name lake-recover-parquet-net --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan U03" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/U03.md exactly. Read it in full first, then AGENTS.md."
```

### U04: Triage the online session's Parquet.SourceGenerator branch (antigravity)

Depends on: G0

```bash
orca worktree create --repo name:Parquet.SourceGenerator --name sg-ccr-triage --base-branch origin/ccr-87fadcf9-bxi9wr --agent antigravity --setup run --no-parent --activate --comment "lake-plan U04" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/U04.md exactly. Read it in full first, then AGENTS.md."
```

## Wave 3

### N1: Mechanical rename to Lake.* (antigravity)

Depends on: G1, U01a, U01b, U02, U03, U05

```bash
orca worktree create --repo name:Iceberg.NET --name lake-rename --base-branch origin/main --agent antigravity --setup run --no-parent --activate --comment "lake-plan N1" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/N1.md exactly. Read it in full first, then AGENTS.md."
```

### N2: Per-package versions, changelogs and public API files (antigravity)

Depends on: N1, G1

```bash
orca worktree create --repo name:Iceberg.NET --name lake-versioning --base-branch rtkelly13/lake-rename --agent antigravity --setup run --no-parent --activate --comment "lake-plan N2" --prompt "Execute the unit brief /Users/ryankelly/orca/workspaces/Iceberg.NET/anthias/temp/lake-plan/units/N2.md exactly. Read it in full first, then AGENTS.md."
```

## Wave 4

### H2: Finish the GitHub rename: publishing and app checks (human)

Depends on: R5

Not an Orca agent launch: see `units/H2.md`.

### R4x: Apply the rename on this machine (human (runs the R4 tool))

Depends on: R4

Not an Orca agent launch: see `units/R4x.md`.

### U08: Update references in other repositories (antigravity)

Depends on: N1

Not an Orca agent launch: see `units/U08.md`.

## Wave 5

### H3: First Lake.* release and retiring Iceberg.Net.* (human)

Depends on: N2, H2, R5

Not an Orca agent launch: see `units/H3.md`.

## Watching

```bash
orca worktree ps --json            # every worktree with its agent state
orca worktree list --repo name:Iceberg.NET --json
```

If a launch reply is lost, re-run the same command with `--retry-launch <operationId>` from the error; never start a second launch.
