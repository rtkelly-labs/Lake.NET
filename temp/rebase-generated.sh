#!/bin/sh
# Continue a rebase whose only conflicts are in generated files (the traceability matrix), then regenerate them.
# Stops, leaving the rebase in progress, if any other file conflicts.
set -eu
while [ -d "$(git rev-parse --git-path rebase-merge)" ] || [ -d "$(git rev-parse --git-path rebase-apply)" ]; do
  conflicts=$(git diff --name-only --diff-filter=U)
  [ -z "$conflicts" ] && { GIT_EDITOR=true git rebase --continue >/dev/null 2>&1 || true; continue; }
  for f in $conflicts; do
    case "$f" in
      docs/spec-traceability.md) git checkout --theirs -- "$f"; git add "$f" ;;
      *) echo "CONFLICT in $f: resolve by hand"; exit 1 ;;
    esac
  done
  GIT_EDITOR=true git rebase --continue >/dev/null 2>&1 || true
done
python3 tools/spec_trace.py matrix | tail -1
if ! git diff --quiet docs/spec-traceability.md; then
  git add docs/spec-traceability.md
  git commit -q -m "docs: regenerate the spec traceability matrix

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_018621Z7dmBg9myfLnYRoFgf"
fi
git log --oneline -4
