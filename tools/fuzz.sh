#!/usr/bin/env bash
# Coverage-guided fuzzing of one decoder with SharpFuzz + libFuzzer (see docs/regression-suite.md, "Fuzzing").
#
#   tools/fuzz.sh <target> [seconds]     targets: metadata view-metadata manifest-list manifest puffin
#                                                 deletion-vector roaring mumbling theta
#
# Needs clang with the libFuzzer runtime (Ubuntu: apt-get install clang libclang-rt-18-dev). Builds
# libfuzzer-dotnet from a pinned source file, instruments the decoder assemblies, seeds the corpus from the committed
# fixtures and runs until [seconds] (default 300) or the first finding. Findings land in fuzz/<target>/findings as
# crash-*/timeout-*/oom-* files: reproduce one with `FUZZ_TARGET=<target> dotnet <dll> < file` or by passing it to
# libfuzzer-dotnet. FUZZ_STRICT=1 also reports low-level exceptions that a Corruption.Guard had to convert.
set -euo pipefail
cd "$(dirname "$0")/.."
target=${1:?usage: tools/fuzz.sh <target> [seconds]}
seconds=${2:-300}
work=${FUZZ_WORK:-fuzz}
fixtures=tests/Iceberg.Net.Spec.Tests/fixtures

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
mkdir -p "$corpus" "$work/$target/findings"
seed() { [[ -f $1 ]] && cp "$1" "$corpus/"; return 0; }
meta=$(ls "$fixtures"/spark/ref/mor_v3/metadata/v*.metadata.json | sort -V | tail -1)
case $target in
  metadata) for f in "$fixtures"/metadata/*.json "$meta"; do seed "$f"; done ;;
  view-metadata) seed "$fixtures"/metadata/view-example.metadata.json ;;
  manifest-list) for f in "$fixtures"/spark/ref/mor_v3/metadata/snap-*.avro; do seed "$f"; done ;;
  manifest) for f in "$fixtures"/spark/ref/mor_v3/metadata/*-m*.avro; do seed "$f"; done
            export FUZZ_TABLE_METADATA=$meta ;;
  puffin) seed "$fixtures"/puffin/dv.puffin ;;
  deletion-vector) dotnet "$out/Iceberg.Net.Fuzz.dll" extract-blob "$fixtures"/puffin/dv.puffin "$corpus/dv.bin" ;;
  roaring) printf '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3a\x30\x00\x00\x01\x00\x00\x00\x00\x00\x02\x00\x10\x00\x00\x00\x01\x00\x02\x00\x03\x00' > "$corpus/one-container" ;;
  mumbling) printf '\x01\x03\x00\x00\x01\x00\x00\x00\x03\x00\x22\xff' > "$corpus/sparse" ;;
  theta) for f in "$fixtures"/puffin/theta-*.bin; do seed "$f"; done ;;
  avro) for f in "$fixtures"/spark/ref/*/data/*.avro "$fixtures"/spark/ref/mor_v3/metadata/*.avro; do seed "$f"; done ;;
  *) echo "unknown target $target" >&2; exit 2 ;;
esac

# Instrument after seeding: instrumented code only runs under the driver.
for asm in Iceberg.Net.Core Iceberg.Net.Avro Iceberg.Net.Puffin; do
  dotnet sharpfuzz "$out/$asm.dll" >/dev/null
done

export FUZZ_TARGET=$target
exec "$driver" --target_path=dotnet --target_arg="$out/Iceberg.Net.Fuzz.dll" \
  -max_total_time="$seconds" -timeout=10 -rss_limit_mb=4096 -artifact_prefix="$work/$target/findings/" "$corpus"
