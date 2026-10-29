# R4x: Apply the rename on this machine

**Executor:** human · **Wave:** 4 · **Depends on:** —

R4 (`rig estate rename-repo`) was dropped on 2026-10-28: rig gets no dedicated rename command. The rename is
done by hand or with existing CLIs, and rig's existing commands pick it up from there.

## Goal

Make this machine match the GitHub rename Iceberg.NET → Lake.NET (done 2026-10-28 with `gh repo rename`).

## Steps

1. GitHub: done (`gh repo rename Lake.NET`); old URLs redirect.
2. Remotes: in the main clone, `git remote set-url origin https://github.com/rtkelly-labs/Lake.NET.git`.
3. rig config: rename the project in `config/managed-projects.json` (name, URL, and the `packages[]` IDs once
   N2 ships `Lake.*`), through a normal rig PR.
4. Clone folder (optional): move `~/code/rtkelly-labs/Iceberg.NET` → `Lake.NET` when no agent worktree is open,
   then `git worktree repair` from the new path.
5. Orca: re-point or re-add the repo in Orca so new worktrees use the new path and name.
6. Check: `rig estate audit` and `uv run workspace orca doctor`. Anything they can't detect or fix after a
   rename is a gap in those commands; raise it as a rig issue rather than adding a rename command.

## Done when

- Both checks pass and every worktree fetches.
