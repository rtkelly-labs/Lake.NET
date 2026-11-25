#!/usr/bin/env bash
# Coverage-guided fuzzing of one decoder with SharpFuzz + libFuzzer (see docs/regression-suite.md, "Fuzzing").
#
#   tools/fuzz.sh <target> [seconds]     targets: metadata view-metadata manifest-list manifest puffin
#                                                 deletion-vector roaring mumbling theta
#                                                 rest-commit rest-transaction (REST server request bodies)
#
# Needs clang with the libFuzzer runtime (Ubuntu: apt-get install clang libclang-rt-18-dev). Builds
# libfuzzer-dotnet from a pinned source file, instruments the decoder assemblies, seeds from every committed fixture
# of the target's format (fuzz/<target>/seeds; new inputs go to fuzz/<target>/corpus, which a previous run may have
# left), adds tests/Iceberg.Net.Fuzz/dicts/<target>.dict when there is one, and runs until [seconds] (default 300) or the first finding. Findings land in fuzz/<target>/findings as
# crash-*/timeout-*/oom-* files: reproduce one with `FUZZ_TARGET=<target> dotnet <dll> < file` or by passing it to
# libfuzzer-dotnet. FUZZ_STRICT=1 also reports low-level exceptions that a Corruption.Guard had to convert.
set -euo pipefail
cd "$(dirname "$0")/.."
target=${1:?usage: tools/fuzz.sh <target> [seconds]}
seconds=${2:-300}
work=${FUZZ_WORK:-fuzz}
fixtures=tests/Iceberg.Net.Spec.Tests/fixtures
# Every committed fixture of other engines, newline-separated (no fixture path holds a newline).
all_fixtures=$(find tests -path '*/fixtures/*' -type f | sort)

# libfuzzer-dotnet: the driver that runs the .NET target in a child process and feeds it libFuzzer inputs.
LFD_COMMIT=master
LFD_SHA256=90f019e2e9ad3a0b93c7ecc2c5afb2fbfc8b5aab6aac51c7e0d349ec79354f36
driver=$work/libfuzzer-dotnet
if [[ ! -x $driver ]]; then
  mkdir -p "$work"
  curl -fsSL -o "$work/libfuzzer-dotnet.cc" "https://raw.githubusercontent.com/Metalnem/libfuzzer-dotnet/$LFD_COMMIT/libfuzzer-dotnet.cc"
  echo "$LFD_SHA256  $work/libfuzzer-dotnet.cc" | sha256sum -c --quiet
  clang -fsanitize=fuzzer "$work/libfuzzer-dotnet.cc" -o "$driver"
fi

out=$work/bin
rm -rf "$out" # instrumentation rewrites the assemblies in place: always start from a fresh publish
dotnet tool restore >/dev/null
dotnet publish tests/Iceberg.Net.Fuzz -c Release -o "$out" --nologo -v quiet

corpus=$work/$target/corpus
seeds=$work/$target/seeds
rm -rf "$seeds"
mkdir -p "$corpus" "$seeds" "$work/$target/findings"
# Seeds are named by content hash: fixtures from different tables share file names (v1.metadata.json, ...).
seed() { [[ -f $1 ]] && cp "$1" "$seeds/$(sha256sum < "$1" | cut -c1-16)"; return 0; }
fixtures_matching() { grep -E "$1" <<< "$all_fixtures" || true; } # <pattern on the path>
meta=$(ls "$fixtures"/spark/ref/mor_v3/metadata/v*.metadata.json | sort -V | tail -1)
case $target in
  # Table metadata: every metadata.json and spec example that is not a view.
  metadata) while IFS= read -r f; do
              grep -q '"view-uuid"' "$f" || seed "$f"
            done < <(fixtures_matching '(\.metadata\.json|/metadata/[^/]*\.json)$') ;;
  view-metadata) while IFS= read -r f; do
                   if grep -q '"view-uuid"' "$f"; then seed "$f"; fi
                 done < <(fixtures_matching '\.json$') ;;
  manifest-list) while IFS= read -r f; do seed "$f"; done < <(fixtures_matching '/snap-[^/]*\.avro$') ;;
  # Manifests from every table, decoded against one table's spec and schema.
  manifest) while IFS= read -r f; do seed "$f"; done < <(fixtures_matching '/metadata/[^/]*-m[0-9]+\.avro$')
            export FUZZ_TABLE_METADATA=$meta ;;
  puffin) while IFS= read -r f; do seed "$f"; done < <(fixtures_matching '\.puffin$') ;;
  deletion-vector) while IFS= read -r f; do
                     dotnet "$out/Iceberg.Net.Fuzz.dll" extract-blob "$f" "$seeds/$(basename "$f").bin" || true
                   done < <(fixtures_matching '\.puffin$') ;;
  roaring) printf '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3a\x30\x00\x00\x01\x00\x00\x00\x00\x00\x02\x00\x10\x00\x00\x00\x01\x00\x02\x00\x03\x00' > "$seeds/one-container" ;;
  mumbling) printf '\x01\x03\x00\x00\x01\x00\x00\x00\x03\x00\x22\xff' > "$seeds/sparse" ;;
  # Request bodies, hand-written (tests/Iceberg.Net.Fuzz/seeds/<target>), for tables the target creates.
  rest-commit|rest-transaction) for f in tests/Iceberg.Net.Fuzz/seeds/"$target"/*.json; do seed "$f"; done ;;
  theta) while IFS= read -r f; do seed "$f"; done < <(fixtures_matching '/theta-[^/]*\.bin$') ;;
  *) echo "unknown target $target" >&2; exit 2 ;;
esac
echo "$target: $(ls "$seeds" | wc -l) seeds, $(ls "$corpus" | wc -l) inputs from earlier runs"
dict=tests/Iceberg.Net.Fuzz/dicts/$target.dict
dict_arg=()
if [[ -f $dict ]]; then dict_arg=(-dict="$dict"); fi

# Instrument after seeding: instrumented code only runs under the driver.
instrumented=(Iceberg.Net.Core Iceberg.Net.Avro Iceberg.Net.Puffin)
[[ $target == rest-* ]] && instrumented+=(Iceberg.Net.Catalog Iceberg.Net.RestServer)
for asm in "${instrumented[@]}"; do
  dotnet sharpfuzz "$out/$asm.dll" >/dev/null
done

export FUZZ_TARGET=$target
exec "$driver" --target_path=dotnet --target_arg="$out/Iceberg.Net.Fuzz.dll" \
  -max_total_time="$seconds" -timeout=10 -rss_limit_mb=4096 -artifact_prefix="$work/$target/findings/" \
  "${dict_arg[@]}" "$corpus" "$seeds"
