#!/usr/bin/env bash
# Regenerate tests/Iceberg.Net.Spec.Tests/fixtures/java-plans from the Spark fixture tables: Iceberg Java plans
# each case itself and serializes the PlanTableScanResponse a REST server would return (ScanPlanOracle.java).
set -euo pipefail
cd "$(dirname "$0")/../.."
fixtures=tests/Iceberg.Net.Spec.Tests/fixtures
out=$fixtures/java-plans
# Iceberg Java 1.12 (the pinned spec version): the Spark image's JDK + the runtime jar emulation/up.sh --spark caches.
image=${SPARK_IMAGE:-apache/spark:4.0.1}
jar=iceberg-spark-runtime-4.0_2.13-1.12.0.jar
root=/tmp/sentric-spark-fixture-root/warehouse
meta() { echo "file://$root/ref/$1/metadata/$(ls "$fixtures/spark/ref/$1/metadata" | grep -E '^v[0-9]+\.metadata\.json$' | sort -V | tail -1)"; }
mkdir -p "$out"
python3 - "$out/cases.json" "$(meta mor_v2)" "$(meta mor_v3)" "$(meta evolved_v2)" "$(meta nested_v2)" <<'PY'
import json, sys
path, mor2, mor3, evolved, nested = sys.argv[1:]
cases = [
  {"name": "mor_v2_all", "table": "mor_v2", "metadata": mor2, "filter": None},
  {"name": "mor_v2_id_lt_10", "table": "mor_v2", "metadata": mor2, "filter": {"type": "lt", "term": "id", "value": 10}},
  {"name": "mor_v2_region_eu", "table": "mor_v2", "metadata": mor2, "filter": {"type": "eq", "term": "region", "value": "eu"}},
  {"name": "mor_v3_eu_from_3", "table": "mor_v3", "metadata": mor3, "filter": {"type": "and", "left": {"type": "gt-eq", "term": "id", "value": 3}, "right": {"type": "eq", "term": "region", "value": "eu"}}},
  {"name": "mor_v3_id_in", "table": "mor_v3", "metadata": mor3, "filter": {"type": "in", "term": "id", "values": [1, 2, 5, 8, 13, 21]}},
  {"name": "evolved_v2_all", "table": "evolved_v2", "metadata": evolved, "filter": None},
  {"name": "evolved_v2_id_gt_12", "table": "evolved_v2", "metadata": evolved, "filter": {"type": "gt", "term": "id", "value": 12}},
  {"name": "nested_v2_all", "table": "nested_v2", "metadata": nested, "filter": None},
]
json.dump(cases, open(path, "w"), indent=2)
PY
docker run --rm --network none --hostname localhost --user root --entrypoint /opt/java/openjdk/bin/java -v "$PWD/interop/java:/w:ro" \
  -v "$PWD/emulation/.cache:/jars:ro" -v "$PWD/$fixtures/spark:$root:ro" -v "$PWD/$out:/out:ro" \
  "$image" -cp "/jars/$jar:/opt/spark/jars/*" /w/ScanPlanOracle.java plans /out/cases.json 2>/dev/null \
  | tail -1 > "$out/responses.json"
python3 -c "import json,sys; r=json.load(open(sys.argv[1])); c=json.load(open(sys.argv[2])); assert len(r)==len(c); print(len(r), 'plans')" "$out/responses.json" "$out/cases.json"

# Expression JSON: the corpus in expressions.json as Java's ExpressionParser re-serializes it.
docker run --rm --network none --hostname localhost --user root --entrypoint /opt/java/openjdk/bin/java -v "$PWD/interop/java:/w:ro" \
  -v "$PWD/emulation/.cache:/jars:ro" -v "$PWD/$out:/out:ro" \
  "$image" -cp "/jars/$jar:/opt/spark/jars/*" /w/ScanPlanOracle.java exprs /out/expressions.json 2>/dev/null \
  | tail -1 > "$out/expressions-java.json"

# The same mor_v3 plans serialized by Iceberg Java 1.10 (the REST fixture image), whose delete-file references are
# wrong when deletion vectors share a Puffin file: the client must still read them right.
python3 -c "import json,sys; c=json.load(open(sys.argv[1])); json.dump([x for x in c if x['table']=='mor_v3'], open(sys.argv[2],'w'), indent=2)" \
  "$out/cases.json" "$out/cases-java110.json"
docker run --rm --network none --hostname localhost --entrypoint java -v "$PWD/interop/java:/w:ro" -v "$PWD/$fixtures/spark:$root:ro" -v "$PWD/$out:/out:ro" \
  apache/iceberg-rest-fixture:1.10.0 -cp /usr/lib/iceberg-rest/iceberg-rest-adapter.jar /w/ScanPlanOracle.java plans /out/cases-java110.json 2>/dev/null \
  | tail -1 > "$out/responses-java110.json"
