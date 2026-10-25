#!/usr/bin/env bash
# Run the test tiers. Usage:
#   ./run-tests.sh spec          L0: pure spec tests (no network)
#   ./run-tests.sh emulation     L0 + BDD + L1/L2 against local emulators, Spark, Trino and Polaris (starts them)
#   ./run-tests.sh aws           L0 + L1/L2/L3 against AWS (needs ICEBERG_WAREHOUSE etc.; see docs/regression-suite.md)
set -euo pipefail
cd "$(dirname "$0")"
mode="${1:-emulation}"

# The suites are the CI jobs; tools/test-projects.sh reads them from Iceberg.Net.slnx.
./tools/test-projects.sh test unit   # spec, netstandard2.0, LINQ, EF Core, REST catalog behaviour (no Docker)
[[ "$mode" == "spec" ]] && exit 0
# BDD, REST server and ecosystem. PyIceberg/Spark/DuckDB oracle cases skip without ICEBERG_PYTHON, Docker or
# ICEBERG_DUCKDB_PYTHON.
./tools/test-projects.sh test cross-engine

if [[ "$mode" == "emulation" ]]; then
  export ICEBERG_TEST_TARGET=emulation
  # Idempotent; ICEBERG_UP_ARGS="" for just moto + the REST fixture (Spark/Trino/Polaris tests then skip).
  # shellcheck disable=SC2086
  ./emulation/up.sh ${ICEBERG_UP_ARGS---spark --trino --polaris}
elif [[ "$mode" == "aws" ]]; then
  export ICEBERG_TEST_TARGET=aws
  : "${ICEBERG_WAREHOUSE:?set ICEBERG_WAREHOUSE=s3://bucket/prefix}"
else
  echo "unknown mode $mode" >&2; exit 2
fi

# Fail rather than skip when the chosen target is unreachable.
export ICEBERG_REQUIRE_TARGET=1
# Every REST operation and the REST error contract across the emulated servers, then L1/L2 regression.
./tools/test-projects.sh test regression
