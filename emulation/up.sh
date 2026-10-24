#!/usr/bin/env bash
# Start the full-emulation stack (moto + Iceberg REST fixture) and wait until both answer.
#   --spark   also fetch the Spark oracle (apache/spark image + pinned Iceberg runtime jar into .cache/), and the jars the
#             EMR Serverless / Glue job emulation adds (iceberg-aws-bundle, hadoop-aws + the two AWS SDK modules S3A needs)
#   --trino   also start Trino (Iceberg connector over the REST fixture + moto S3) on :8080
#   --polaris also start Apache Polaris (second REST catalog implementation) on :8182, catalog "regression"
#   --nessie  also start Project Nessie's Iceberg REST API (fourth REST implementation) on :19120 (jar from Maven Central)
#   --lakekeeper also start Lakekeeper (Rust REST implementation) on :8283, warehouse "regression"; set LAKEKEEPER_IMAGE
#             to a local build (./lakekeeper_image.sh) where quay.io is unreachable
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
    --lakekeeper) PROFILES+=(--profile lakekeeper) ;;
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
# The setup containers are one-shot and nothing depends on them, so `up --wait` would fail whenever one exits
# before the wait ends. Start everything else, then run each setup in the foreground so its exit code counts.
SETUPS=()
[[ " ${PROFILES[*]} " == *" polaris "* ]] && SETUPS+=(polaris-setup)
[[ " ${PROFILES[*]} " == *" lakekeeper "* ]] && SETUPS+=(lakekeeper-setup)
SCALE=()
for s in "${SETUPS[@]}"; do SCALE+=(--scale "$s=0"); done
docker compose "${PROFILES[@]}" up -d --wait "${SCALE[@]}"
for s in "${SETUPS[@]}"; do docker compose "${PROFILES[@]}" run --rm --no-deps "$s"; done
echo "moto (S3 + Glue):      http://localhost:5000"
echo "Iceberg REST fixture:  http://localhost:8181"
[[ " ${PROFILES[*]} " == *" trino "* ]] && echo "Trino:                 http://localhost:8080"
[[ " ${PROFILES[*]} " == *" polaris "* ]] && echo "Polaris (REST):        http://localhost:8182/api/catalog (warehouse regression)"
[[ " ${PROFILES[*]} " == *" nessie "* ]] && echo "Nessie (REST):         http://localhost:19120/iceberg (prefix main)"
[[ " ${PROFILES[*]} " == *" gravitino "* ]] && echo "Gravitino (REST):      http://localhost:9001/iceberg"
[[ " ${PROFILES[*]} " == *" lakekeeper "* ]] && echo "Lakekeeper (REST):     http://localhost:8283/catalog (warehouse regression)"

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
  # The managed-Spark job emulation (SparkJobServiceEmulator): Iceberg's GlueCatalog/S3FileIO and S3A for s3:// paths.
  # The AWS SDK modules match the SDK version inside iceberg-aws-bundle-1.12.0 (2.54.17).
  while read -r sha path; do
    file="$(basename "$path")"
    if [[ ! -f ".cache/$file" ]]; then
      for attempt in 1 2 3 4 5; do
        curl -fsSL -o ".cache/$file.part" "https://repo1.maven.org/maven2/$path" && break
        sleep $((attempt * 2))
      done
      mv ".cache/$file.part" ".cache/$file"
    fi
    echo "$sha  .cache/$file" | sha256sum -c --quiet
  done <<'JARS'
dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar
01334a07e179cae2b7e251f24857f11e3759108c9d6a79dcbe362a4295227da2 org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar
7bef0ea4efd5e0860bab63e08317040951edb51b72a5be734284c28b8324ab87 software/amazon/awssdk/s3-transfer-manager/2.54.17/s3-transfer-manager-2.54.17.jar
0b00ae3d55c3d1213ca47b5651dc4c2a0080fc9b6192dde3d53c3da6c1811bcb software/amazon/awssdk/apache-client/2.54.17/apache-client-2.54.17.jar
JARS
  echo "Spark oracle:          $SPARK_IMAGE + $JAR (+ AWS jars for the EMR/Glue job emulation)"
fi
