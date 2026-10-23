#!/usr/bin/env bash
# Start the full-emulation stack (moto + Iceberg REST fixture) and wait until both answer.
#   --spark   also fetch the Spark oracle (apache/spark image + pinned Iceberg runtime jar into .cache/)
#   --trino   also start Trino (Iceberg connector over the REST fixture + moto S3) on :8080
set -euo pipefail
cd "$(dirname "$0")"
SPARK=0; PROFILES=()
for arg in "$@"; do
  case "$arg" in
    --spark) SPARK=1 ;;
    --trino) PROFILES+=(--profile trino) ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done
docker compose "${PROFILES[@]}" up -d --wait
echo "moto (S3 + Glue):      http://localhost:5000"
echo "Iceberg REST fixture:  http://localhost:8181"
[[ ${#PROFILES[@]} -gt 0 ]] && echo "Trino:                 http://localhost:8080"

if [[ "$SPARK" == 1 ]]; then
  SPARK_IMAGE="${ICEBERG_SPARK_IMAGE:-apache/spark:4.0.1}"
  JAR="iceberg-spark-runtime-4.0_2.13-1.12.0.jar"
  JAR_SHA256="3f50e93e8ed322ae0da0e380f1b6c27116404329c62e55f0586697b1b8378306"
  docker image inspect "$SPARK_IMAGE" >/dev/null 2>&1 || docker pull "$SPARK_IMAGE"
  mkdir -p .cache
  if [[ ! -f ".cache/$JAR" ]]; then
    curl -fsSL -o ".cache/$JAR.part" "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.12.0/$JAR"
    mv ".cache/$JAR.part" ".cache/$JAR"
  fi
  echo "$JAR_SHA256  .cache/$JAR" | sha256sum -c --quiet
  echo "Spark oracle:          $SPARK_IMAGE + $JAR"
fi
