# Maintaining the fork

> These pages hold the Lake.Parquet fork's decisions; the fork ([rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet)) carries the code
> and one change record per change. Paths in `code` are paths in the fork.

How this repository is laid out, how to move it onto a newer upstream when you decide to, and how
to bring every other branch along with one command. The rules themselves (what a change is, the record format) are in
[changes.md](changes.md); this page is the operations guide.

## The structure

Everything is built from one line of history: upstream's `master`, kept exactly as upstream wrote it.

```text
upstream/master   A ─ B ─ C ─ U                        aloneguid/parquet-dotnet, never rewritten
                              │
master                        └─ 0001 ─ 0002 ─ … ─ 0025      the stack: one commit per change
                                         │
lake/change/0017-physical-values   U ─ 0002 ─ 0008 ─ 0017   generated: upstream + a change + what it needs
your-branch                                     └─ … ─ 0025 ─ x ─ y   your own commits, on top of the stack
```

### Remotes

| Remote | Repository | Used for |
|---|---|---|
| `origin` | `rtkelly-labs/parquet-dotnet` | the fork: everything below lives here |
| `upstream` | `aloneguid/parquet-dotnet` | read only; fetched, never pushed to |

```bash
git remote add upstream https://github.com/aloneguid/parquet-dotnet.git
git fetch upstream && git fetch origin --tags
```

Keep the GitHub fork relationship: it is what lets a person open a pull request upstream from a
`lake/change/*` branch.

### Refs

| Ref | What it is | Who moves it | How |
|---|---|---|---|
| `upstream/master` | upstream's history | upstream | never touched here |
| `master` | upstream + the stack (`lake(0001)` … `lake(NNNN)`) | squash-merged PRs (new changes, fixups); you, with `lake-stack rebaseline --push` | fast-forward for PRs; `--force-with-lease` for a rebaseline, always after tagging the old head |
| `lake/change/NNNN-name` | upstream + one change + its `depends_on`, without records | `lake-stack rebaseline --push` (through `lake-stack branches`) | regenerated; never commit to one |
| `staging/<name>` | a rebaselined stack, built by CI before it may replace `master` (`staging/v0.1.1`) | `lake-stack rebaseline --stage <name>` | force-pushed while staged; deleted when published or aborted |
| `lake/stack/<date>-<sha>` (tags) | every head `master` moved away from | `lake-stack rebaseline --push` | created, never moved or deleted |
| `lake/history/stack`, `lake/history/NNNN-name` | every published version of the stack and of each change branch, one record commit each | `lake-stack branches --push`, `promote` (so `rebaseline --push`), `history record` | fast-forward only, never forced or deleted |
| `lake-v<version>` (tags) | releases | the `lake.yml` publish job | created, never moved |
| any other branch | work in progress, built on the stack | you | `lake-stack rewire` after each rebaseline |

`master` only ever moves to a newer upstream when you run a rebaseline: nothing does it on a
schedule, because upstream's changes can break the changes on top and each move needs a person to
look at it. Between rebaselines, `master` only moves forward (pull requests).

Because a rebaseline rewrites `master`, **pin a tag or a commit that a tag keeps**, never a moving
branch head you expect to stay put (Lake.NET's submodule, a consumer's build). Every head a
rebaseline moves away from is tagged first, so a pinned commit never becomes unreachable.

### Files

| Path | Belongs to | Purpose |
|---|---|---|
| `src/`, `docs/README.md`, everything else of upstream's | upstream, edited only by `lake(0002+)` | the library |
| `README.md`, `AGENTS.md`, `AI_POLICY.md`, [vision.md](vision.md), [maintaining.md](maintaining.md) | `lake(0001)` | the fork's front page, rules for agents, reasoning, this guide |
| [changes.md](changes.md), [upstreaming.md](upstreaming.md) | `lake(0001)` | the stack's rules; how to take a change upstream |
| `docs/changes/NNNN-name.md` | `lake(NNNN)` | one record per change, in the change's own commit |
| `eng/lake/Lake.Stack/`, `eng/lake/Lake.Stack.Tests/` | `lake(0001)` | the stack tool and its tests (below) |
| `eng/lake/RenameNamespace.cs`, `Lake.Parquet.targets`, `VerifyLakeAssembly.cs`, `PACKAGE_README.md` | `lake(0001)` | the build-time rename to `Lake.Parquet` |
| `.github/workflows/lake.yml` | `lake(0001)` | the fork's CI: stack rules, the renamed build and tests, releases |

None of `lake(0001)`'s files exist upstream, so the bottom of the stack never conflicts.

## The tool: `eng/lake/Lake.Stack`

A small C# console app, with its tests in `eng/lake/Lake.Stack.Tests`. Every step of the process goes
through it (CI only runs `check` and `check-pr`), so the rules live in one place and are tested on real throwaway
repositories (an upstream, a bare origin and a fork). Run it from the repository's root; the rest of
this page writes `lake-stack` for it:

```bash
alias lake-stack='dotnet run --project eng/lake/Lake.Stack --configuration Release --'
lake-stack --help                     # every command and option
dotnet test eng/lake/Lake.Stack.Tests # its tests (CI runs them before using it)
```

| Command | What it does | Moves refs? |
|---|---|---|
| `check` | every commit above upstream is `lake(NNNN)` (or a pending `fixup!`), one per change, with a valid record in that commit; no merges; `depends_on` only names changes below; an addition edits no upstream file (commit subjects themselves are also linted by commitlint) | no |
| `check-pr --title <t> --base <sha>` | a pull request's title names its change (or is a fixup), and a new change brings its record | no |
| `status` | each change, its kind, status and how many code files it touches | no |
| `rebaseline` | **the step that moves the fork onto a newer upstream**: rebase (folding in fixups), report, check, run the test suite; `--stage <name>` pushes it to `staging/<name>` for CI; `--status` shows the staged build's result; `--push` publishes it once that build is green; `--continue` resumes after a conflict or a fix; `--abort` puts everything back | the local `master`; `staging/<name>`; with `--push`, the remote's `master`, a tag and `lake/change/*` |
| `sync` | only the rebase part of `rebaseline`, for trying a rebase out locally | the current branch |
| `branches` | regenerates every `lake/change/*`; `--push origin` force-pushes them and deletes those of retired changes | `lake/change/*` |
| `rewire [--apply] (<branch>… \| --all)` | moves each branch's own commits onto the current stack; a dry run without `--apply` | the named branches |
| `verify [NNNN…] [--list]` | proves each code change alone: its branch passes the suite, and the suite fails without its library code (testing.md) | no |
| `promote --from <old> --to <new>` | the publishing part of `rebaseline --push`: refuses a new stack that fails `check`, tags the old head `lake/stack/<date>-<sha>`, and moves the remote's `master` with a lease on the old head | the remote's `master`, a tag, `lake/history/stack` |
| `history show <stack \| NNNN-name>` | the published versions of the stack or of one change, newest first, and the `git range-diff` that compares the last two | no |
| `history record` | records the remote's `master` and every local `lake/change/*` as published versions, for a publication made without the tool (a fast-forward of `master` after a staged build) | `lake/history/*` |

`--upstream <ref>` (or `LAKE_UPSTREAM`; default `upstream/master`) sets the upstream every command
works against, and `--repo <path>` the repository. Exit codes: 0 done, 1 something to fix (a broken
rule, a conflict, a branch that would not build), 2 it could not run (a dirty tree, a bad argument).

All of it is plain git underneath: `rebase --autosquash`, `cherry-pick`, `merge-base`. `rerere` is
switched on, so a conflict resolved once is resolved again automatically on the next rebase. Change
branches are rebuilt with each source commit's own author, committer and dates, so an unchanged change
gives the same commit and pushing it again is a no-op.

The code is laid out by command: `Commands/RebaselineCommand.cs`, `VerifyCommand.cs`, `SyncCommand.cs`, `BranchesCommand.cs`, `RewireCommand.cs`,
`PromoteCommand.cs`, `HistoryCommand.cs`, `CheckCommand.cs`, `CheckPrCommand.cs` and `StatusCommand.cs`, over
`StackModel.cs` (the commits above upstream and their records), `ChangeRecord.cs` (the record format)
and `Git.cs` (git, run with an argument list, never through a shell). A change to how the process
works is a change there, with a test, as a `fixup! lake(0001): …`.

## Rebaselining onto upstream

A rebaseline is a decision, not a routine: run it when you want something from upstream (a release,
a fix), and expect to spend time on it, because upstream's changes can break the changes on top.
Nothing runs it for you.

```bash
git fetch origin && git switch master && git reset --hard origin/master   # start from what is published
lake-stack rebaseline --stage v0.1.1           # rebase, verify locally, stage on staging/v0.1.1 for CI
lake-stack rebaseline --status                 # later: is its lake-ci green?
lake-stack rebaseline --continue --push        # promote it to master once it is
```

`--onto upstream/<branch>` or `--onto <an upstream release tag>` rebases onto something other than
upstream's `master`.

It stops at the first thing that needs you, and tells you what to run next:

1. **Rebase.** The stack is replayed onto upstream's `master`; upstream's commits keep their hashes,
   pending fixups are folded into their changes, and changes upstream already has drop out. It prints
   how many upstream commits came in, the range-diff of every change, the changes upstream took whole
   (dropped) and those left with only their record (upstream has the code: retire them).
2. **Conflicts.** If upstream changed code a change also changes, the rebase stops in that change's
   commit (see [When the rebase stops](#when-the-rebase-stops)). Resolve it, `git add`,
   `git rebase --continue`, then `lake-stack rebaseline --continue`.
3. **Verify.** `check` on the new stack, then the library's whole test suite
   (`src/Parquet.Test`, needs `pyarrow` and `pandas`). If a test fails, upstream broke a change: fix it
   in that change (`git commit --fixup=<its commit>`, `git rebase -i --autosquash $(git merge-base HEAD upstream/master)`),
   then `lake-stack rebaseline --continue`.
4. **Stage it for CI**: `lake-stack rebaseline --continue --stage v0.1.1` (or pass `--stage` to the
   first `rebaseline`, which stages as soon as the local checks pass). It pushes the rebased stack to
   `staging/v0.1.1`, where `lake.yml` builds it exactly as it builds `master` (the stack rules, the
   tool's tests, the renamed `Lake.Parquet` build, the whole suite on .NET 8 and 10, and the package), and
   also verifies every change alone on the new upstream, one job per change (`lake-stack verify`,
   [testing.md](testing.md)).
   `origin/master` is untouched; consumers see nothing yet.
5. **Evaluate.** `lake-stack rebaseline --status` shows where it stands and the staged build's
   `lake-ci` result (read through the `gh` CLI). Look at the range-diff and `lake-stack status` too. If
   CI fails, fix the change it points at (a fixup, `git rebase -i --autosquash`), then
   `lake-stack rebaseline --continue`: it checks and tests the new head and re-stages it.
6. **Promote to production, when you are satisfied**: `lake-stack rebaseline --continue --push`. It is
   refused unless `staging/<name>` holds exactly the head being published and `lake-ci` passed on it.
   Then it tags the old head `lake/stack/<date>-<sha>`, moves `origin/master` with a lease on the old
   head (if a pull request merged meanwhile, nothing moves: rebaseline again to include it),
   regenerates every `lake/change/*` branch (deleting those of retired changes), and deletes the
   staging branch. `--skip-ci-check` publishes without the CI gate, for when you have checked the
   build yourself.

`lake-stack rebaseline --abort` at any point puts `master` back where it started and deletes the
staging branch. Until it is pushed or aborted, the starting point and the staging branch are kept in
git config (`lake.rebaseline.*`), so `--continue` and `--status` work across sessions. Publishing needs push rights that may force-push `master` and create tags (a
repository admin, past the branch rules).

### With jj

[jj](https://jj-vcs.github.io/jj/) works inside this git clone ("colocated"): git, CI and GitHub see ordinary
commits, and you get jj's model for editing the stack. It is the recommended way to rebaseline and to edit
changes; `lake-stack` then checks, tests, stages and promotes what jj produced.

Set it up once in your clone:

```bash
jj git init --colocate
jj git remote add upstream https://github.com/aloneguid/parquet-dotnet.git   # if git does not have it yet
# upstream's history and every tagged stack are immutable: jj refuses to rewrite them
jj config set --repo 'revset-aliases."immutable_heads()"' 'master@upstream | tags()'
jj config set --repo 'revset-aliases.stack' 'master@upstream..master'
jj config set --repo 'revset-aliases.fixups' 'stack & description(glob:"fixup! *")'
```

Rebaseline:

```bash
jj git fetch --remote upstream && jj git fetch --remote origin
jj rebase -b master -d master@upstream          # the whole stack, in one step; conflicts are recorded, not fatal
# fold each pending fixup into its change
for f in $(jj log -r fixups --reversed --no-graph -T 'change_id.short() ++ "\n"'); do
  n=$(jj log -r "$f" --no-graph -T 'description.first_line()' | sed -E 's/^fixup! lake\(([0-9]{4})\).*/\1/')
  jj squash --from "$f" --into "$(jj log -r "stack & description(glob:\"lake($n):*\")" --no-graph -T 'change_id.short()')" --use-destination-message
done
jj log -r 'stack & conflicts()'                 # any change upstream conflicts with: jj edit it, resolve, jj squash
lake-stack rebaseline --rebased --stage v0.1.1   # check, test, stage for CI
lake-stack rebaseline --continue --push          # promote once lake-ci is green on the staging branch
```

`--rebased` takes the published `master` as the old head and checks the stack you rebased, instead of rebasing
itself. Everything after that is the same as above.

Edit a change in the middle of the stack: `jj edit <change>` (or `jj new <change>`, make the edit, `jj squash`),
and every change above it is rebased for you. `jj absorb` sends each hunk of a working-copy edit to the change
that last touched those lines. `jj op log` and `jj undo` take back any of it locally; the `lake/stack/*` tags
are what keep earlier stacks reachable for everyone else.

jj does not run git hooks, so nothing checks commit subjects locally; CI does (commitlint).

### When the rebase stops

The rebase stops in the commit of the change that conflicts. Resolve it in that commit only, keeping
both intents; the change's own tests say whether the result is right. `rerere` is on, so the
resolution is recorded and replayed if you abort and start again on the same machine.

- **Upstream changed code a change also changes**: keep both intents.
- **Upstream took the change** (ours, after review, possibly rewritten): take upstream's code, drop the
  change's commit (`git rebase -i`), keep any test of ours that still adds something as a new change.
  When upstream took it exactly, the rebase already dropped it and the report says so.
- **Upstream fixed the same bug differently**: take upstream's fix, keep our test; if it still fails,
  that difference is a bug to report upstream (by hand) and to keep here meanwhile.

### Your other branches, after a rebaseline

A rebaseline moves `master` and the change branches. Any other branch (a change in progress, an
experiment, an old branch from before the stack) is still built on the previous stack. One command
puts them all back on top:

```bash
git fetch origin --prune --tags && git fetch upstream
git switch master && git reset --hard origin/master    # the new stack (only if you have no local work on master)
git switch -c <branch> origin/<branch>                  # for each remote branch you want rewired, if not already local
lake-stack rewire --all                                 # dry run: what each branch would move
lake-stack rewire --apply --all                         # do it; stops at the first conflict
git push --force-with-lease origin <branch>             # for each branch it moved (it prints the command)
```

How it finds a branch's own commits: it takes the newest point where the branch meets `master`, any
`lake/stack/*` or `lake-v*` tag, or upstream, and moves only the commits after it, with
`git rebase --onto master <that point> <branch>`. So it works the same for:

- a branch cut from an older stack: only your commits move, not the old copies of the stack's;
- a branch cut from upstream before the stack existed: its commits move onto the stack, and then
  build against every change;
- a branch already on the current stack: left alone.

It needs the `lake/stack/*` tags locally (`git fetch origin --tags`); without them a branch cut from
an older stack falls back to upstream as its fork point and replays the old stack's commits too
(which then conflict or duplicate). If that happens, `git rebase --abort` and fetch the tags.

On a conflict: resolve it, `git add`, `git rebase --continue`, then run `rewire --apply --all` again;
branches already on the stack are skipped.

To turn a rewired branch into a change: squash it into one commit titled `lake(NNNN): title` with
its record, and open a pull request (a fix to an existing change: `fixup! lake(NNNN): title`).

## Editing the stack itself

| Task | How |
|---|---|
| New change | PR from a branch on `master`, one commit `lake(NNNN): title` with `docs/changes/NNNN-name.md`, squash-merged with that title |
| Fix a change | PR with a `fixup! lake(NNNN): title` commit (`git commit --fixup=<its commit>`), squash-merged with that title; the next rebaseline folds it in |
| Fix the fork's tooling or docs | the same, as `fixup! lake(0001): …` |
| Fold fixups in, without moving to a newer upstream | `lake-stack rebaseline --onto $(git merge-base master upstream/master)`: the same steps, on the base you already have |
| Reword, reorder, split or retire changes | inside a rebaseline: once it stops ready and unpublished, `git rebase -i $(git merge-base HEAD upstream/master)`, then `lake-stack rebaseline --continue` (a changed head is checked, tested and re-staged) and, once CI is green, `--continue --push`; a retired change's record goes with its commit |
| Release | Actions → Lake.Parquet → Run workflow on `master` with `publish`; tags `lake-v<version>` |

Pending fixups are fine on `master` between rebaselines: `check` reports them, the build includes
them, and the next rebaseline folds each into its change.

## History of published changes

A change branch is regenerated at each rebaseline, so the branch alone only shows its latest version;
`master` is rewritten too. Every publication is also recorded, so earlier versions stay reachable and
comparable without tags:

- `lake/history/NNNN-name` gets a commit each time `lake/change/NNNN-name` is pushed with a new tip;
  `lake/history/stack` each time `master` is promoted. A record has the published commit's tree, the
  previous record as its first parent and the published commit as its second (the first record's only
  parent is the first version).
- Records only ever fast-forward, so the branches need no force-push and can be protected against one.
  Recording fetches the remote's history first, so records made from another clone are kept.
- The change branches themselves are unchanged: still upstream plus one change, the diff an upstream
  pull request would contain.

```bash
lake-stack history show 0035-rented-decompression-buffers   # its versions, newest first
git log --first-parent lake/history/0035-rented-decompression-buffers
git range-diff <older>...<newer>                             # how the change moved between two versions
lake-stack history record                                    # after publishing master without the tool
```

## Recovering

Nothing a rebaseline replaces is lost. To see or restore an earlier stack:

```bash
git tag --list 'lake/stack/*' --sort=-creatordate | head      # previous heads, newest first
t=lake/stack/<tag>                                               # what changed since:
git range-diff $(git merge-base $t upstream/master)..$t $(git merge-base master upstream/master)..master
git push --force-with-lease origin lake/stack/<tag>:master     # roll back (an admin; tag the current head first)
```
