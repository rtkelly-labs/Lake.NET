#!/usr/bin/env bash
# The one list of test projects: every <IsTestProject> project in Iceberg.Net.slnx, grouped by its <CiSuite> property.
# CI (.github/workflows/ci.yml), run-tests.sh and the release build all go through this script, so adding a test
# project is `dotnet sln Iceberg.Net.slnx add <csproj>` plus a <CiSuite> in the csproj, and nothing else.
#   tools/test-projects.sh list [suite]                 print the projects (all suites, or one)
#   tools/test-projects.sh test <suite> [args...]       dotnet test each project of the suite; args go to dotnet test
# Suites: unit (no Docker or Python), cross-engine (oracles and engines, emulators up),
# regression (fails rather than skips when the target is unreachable; ICEBERG_REQUIRE_TARGET is the caller's).
# A test project without a known <CiSuite> is an error, so nothing in the solution goes untested by accident.
set -euo pipefail
cd "$(dirname "$0")/.."
SUITES="unit cross-engine regression"

projects() {
  local want="${1:-}" bad=0 csproj suite
  # `dotnet sln list` prints two header lines, then one project path per line.
  while read -r csproj; do
    [[ "$csproj" == *.csproj ]] || continue
    grep -q '<IsTestProject>true</IsTestProject>' "$csproj" || continue
    suite="$(sed -n 's:.*<CiSuite>\(.*\)</CiSuite>.*:\1:p' "$csproj")"
    if [[ " $SUITES " != *" $suite "* ]]; then
      echo "::error file=$csproj::test project has no <CiSuite> (one of: $SUITES), so no CI job would run it" >&2
      bad=1
      continue
    fi
    if [[ -z "$want" || "$want" == "$suite" ]]; then echo "$csproj"; fi
  done < <(dotnet sln Iceberg.Net.slnx list | tr -d '\r' | tr '\\' '/')
  return "$bad"
}

cmd="${1:-}"
shift || true
case "$cmd" in
  list)
    projects "${1:-}"
    ;;
  test)
    suite="${1:?usage: tools/test-projects.sh test <suite> [dotnet test args...]}"
    shift
    [[ " $SUITES " == *" $suite "* ]] || { echo "unknown suite '$suite' (one of: $SUITES)" >&2; exit 2; }
    list="$(projects "$suite")"
    [[ -n "$list" ]] || { echo "suite '$suite' has no projects" >&2; exit 1; }
    # Every project runs even after one fails, so one red run reports every failing project.
    failed=()
    while read -r csproj; do
      if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::group::$csproj"; fi
      dotnet test "$csproj" --nologo "$@" || failed+=("$csproj")
      if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::endgroup::"; fi
    done <<<"$list"
    if (( ${#failed[@]} )); then
      for csproj in "${failed[@]}"; do echo "::error::tests failed: $csproj" >&2; done
      exit 1
    fi
    ;;
  *)
    sed -n '2,9p' "$0" >&2
    exit 2
    ;;
esac
