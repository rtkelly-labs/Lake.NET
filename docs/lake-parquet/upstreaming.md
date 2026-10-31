# Proposing a change upstream

> These pages hold the Lake.Parquet fork's decisions; the fork ([rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet)) carries the code
> and one change record per change. Paths in `code` are paths in the fork.

Every change in this stack was written with machine help. Upstream, Parquet.Net, does not accept
LLM-written issues, pull requests or comments (`docs/README.md`), and treats them as spam. So a change
goes upstream only after a person has reviewed, understood and rewritten (or at least re-owned) it, and
that person opens the pull request by hand. Nothing in this repository's automation touches upstream.

## Which change first

The stack is already in the suggested order: small bug fixes with a clear reproduction and no API
change first (0002 to 0013), since they are the easiest to get accepted and build trust for the
features after them (0014 to 0025). A change's `depends_on` names the ones that must go before it.
0008 should go with or after 0007, and before upstream starts writing `column_orders`: once a file
declares TYPE_ORDER, other readers trust bounds that were occasionally wrong without 0008.

## Reviewing one

1. Read the record. **Review** notes say where the change is least certain, or where upstream has a
   judgement to make (API shape, behaviour changes, test dependencies).
2. Reproduce the problem on upstream: check out `lake/change/NNNN-name` (upstream, the changes it
   depends on, then this one), undo the library part with `git checkout HEAD~1 -- src/Parquet`, and run
   the change's test. It should fail or not compile. A change whose problem you cannot reproduce should
   not go upstream.
3. Read the diff: `git diff upstream/master lake/change/NNNN-name`. That is exactly what the pull
   request would contain, and it includes any change it depends on.
4. Run the suite on the branch: `dotnet test src/Parquet.Test` (with `pyarrow` and `pandas` installed
   for the Arrow interop tests).
5. Write the pull request yourself, in upstream's style and conventions, and set the record's `status`
   to `proposed-upstream` with the link, in a `fixup! lake(NNNN): ...` commit here.

What has not been verified, for every change:

- **Windows and macOS** locally: everything was developed on Linux. Lake.NET's Parquet.Net backend
  branch (not yet on its main branch) runs this suite on Linux, Windows and macOS in CI.
- **Upstream's conventions.** The code follows the surrounding code, but upstream's preferences on API
  shape, naming, tests and release notes are theirs to judge.

## Once upstream has it

The next rebaseline drops the commit when upstream's code is identical, or reports it when only its record
is left or it conflicts with upstream's own form of the fix. Take upstream's version, keep any test of
ours that still adds something (as a new change), and drop the commit. If upstream fixed the same bug
differently and our test still fails, that difference is a bug to report upstream, by hand, and to
keep here meanwhile.
