#!/usr/bin/env bash
# Start the full-emulation stack (moto + Iceberg REST fixture) and wait until both answer.
#   --spark   also fetch the Spark oracle (apache/spark image + pinned Iceberg runtime jar into .cache/)
#   --trino   also start Trino (Iceberg connector over the REST fixture + moto S3) on :8080
#   --polaris also start Apache Polaris (second REST catalog implementation) on :8182, catalog "regression"
#   --nessie  also start Project Nessie's Iceberg REST API (fourth REST implementation) on :19120 (jar from Maven Central)
#   --gravitino also start Apache Gravitino's Iceberg REST server (third REST implementation) on :9001
set -euo pipefail
cd "$(dirname "$0")"
SPARK=0; NESSIE=0; PROFILES=()
for arg in "$@"; do
  case "$arg" in
    --spark) SPARK=1 ;;
    --trino) PROFILES+=(--profile trino) ;;
    --polaris) PROFILES+=(--profile polaris) ;;
    --gravitino) PROFILES+=(--profile gravitino) ;;
    --nessie) PROFILES+=(--profile nessie); NESSIE=1 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done
if [[ "$NESSIE" == 1 ]]; then
  NJAR="nessie-quarkus-0.109.0-runner.jar"
  NJAR_SHA512="431c672f50dd89f7142a6c73d94ac65b72d2fd54025b040a10f71472cef91b34c0795d3546904d9fda4ea9ba91af02963ac2920adad25567cde48c402a680aef"
  mkdir -p .cache
  if [[ ! -f ".cache/$NJAR" ]]; then
    curl -fsSL -o ".cache/$NJAR.part" "https://repo1.maven.org/maven2/org/projectnessie/nessie/nessie-quarkus/0.109.0/$NJAR"
    mv ".cache/$NJAR.part" ".cache/$NJAR"
  fi
  echo "$NJAR_SHA512  .cache/$NJAR" | sha512sum -c --quiet
fi
docker compose "${PROFILES[@]}" up -d --wait
echo "moto (S3 + Glue):      http://localhost:5000"
echo "Iceberg REST fixture:  http://localhost:8181"
[[ " ${PROFILES[*]} " == *" trino "* ]] && echo "Trino:                 http://localhost:8080"
[[ " ${PROFILES[*]} " == *" polaris "* ]] && echo "Polaris (REST):        http://localhost:8182/api/catalog (warehouse regression)"
[[ " ${PROFILES[*]} " == *" nessie "* ]] && echo "Nessie (REST):         http://localhost:19120/iceberg (prefix main)"
[[ " ${PROFILES[*]} " == *" gravitino "* ]] && echo "Gravitino (REST):      http://localhost:9001/iceberg"

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
