#!/usr/bin/env bash
# Run the test tiers. Usage:
#   ./run-tests.sh spec          unit suites, no network
#   ./run-tests.sh emulation     start emulators, then unit + cross-engine + regression
#   ./run-tests.sh aws           unit + cross-engine + regression against AWS (needs credentials/configuration)
set -euo pipefail
cd "$(dirname "$0")"
mode="${1:-emulation}"

# Select/validate the target before any tests run: dependent suites must never silently use the wrong target.
case "$mode" in
  spec) ;;
  emulation)
    export ICEBERG_TEST_TARGET=emulation
    # Idempotent; set ICEBERG_UP_ARGS="" for only moto and the REST fixture.
    # shellcheck disable=SC2086
    ./emulation/up.sh ${ICEBERG_UP_ARGS---spark --trino --polaris --gravitino --nessie --lakekeeper}
    export ICEBERG_REQUIRE_TARGET=1
    ;;
  aws)
    export ICEBERG_TEST_TARGET=aws
    : "${ICEBERG_WAREHOUSE:?set ICEBERG_WAREHOUSE=s3://bucket/prefix}"
    export ICEBERG_REQUIRE_TARGET=1
    ;;
  *) echo "unknown mode $mode" >&2; exit 2 ;;
esac

# The solution/CiSuite registry is the one list of test projects.
./tools/test-projects.sh test unit
[[ "$mode" == "spec" ]] && exit 0
./tools/test-projects.sh test cross-engine
./tools/test-projects.sh test regression
