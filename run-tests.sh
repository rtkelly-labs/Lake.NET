#!/usr/bin/env bash
# Run the test tiers. Usage:
#   ./run-tests.sh spec          L0: pure spec tests (no network)
#   ./run-tests.sh emulation     L0 + BDD + L1/L2 against local emulators, Spark, Trino and Polaris (starts them)
#   ./run-tests.sh aws           L0 + L1/L2/L3 against AWS (needs ICEBERG_WAREHOUSE etc.; see docs/regression-suite.md)
set -euo pipefail
cd "$(dirname "$0")"
mode="${1:-emulation}"

dotnet test tests/Iceberg.Net.Spec.Tests --nologo
dotnet test tests/Iceberg.Net.NetStandard.Tests --nologo   # the netstandard2.0 builds of Core and Avro
[[ "$mode" == "spec" ]] && exit 0
dotnet test tests/Iceberg.Net.Behaviour.Tests --nologo

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
dotnet test tests/Iceberg.Net.Regression.Tests --nologo
