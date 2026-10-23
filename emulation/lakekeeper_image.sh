#!/usr/bin/env bash
# Build Lakekeeper from its release tag into a local image, for machines that cannot pull quay.io/lakekeeper/catalog
# (the official image). Needs git, a Rust toolchain (rustup installs the one the tag pins) and Docker; the first
# build takes a while (~600 crates). Then: LAKEKEEPER_IMAGE=iceberg-dotnet/lakekeeper:$TAG ./up.sh --lakekeeper
set -euo pipefail
TAG="${LAKEKEEPER_TAG:-v0.14.1}"
WORK="${LAKEKEEPER_BUILD_DIR:-$(dirname "$0")/.cache/lakekeeper-build}"
mkdir -p "$WORK/image"
if [[ -n "${LAKEKEEPER_BINARY:-}" ]]; then
  cp "$LAKEKEEPER_BINARY" "$WORK/image/lakekeeper"  # a binary already built from $TAG
else
  if [[ ! -d "$WORK/src-$TAG" ]]; then
    git clone -q --depth 1 --branch "$TAG" https://github.com/lakekeeper/lakekeeper.git "$WORK/src-$TAG"
  fi
  # Default features: no embedded UI (the UI needs Node to build and the suite never uses it).
  (cd "$WORK/src-$TAG" && SQLX_OFFLINE=true CARGO_TARGET_DIR="$WORK/target" CARGO_PROFILE_RELEASE_DEBUG=0 \
    cargo build --release --locked --bin lakekeeper)
  cp "$WORK/target/release/lakekeeper" "$WORK/image/lakekeeper"
fi
cat > "$WORK/image/Dockerfile" <<'DOCKERFILE'
# Same layout as the official image (binary at /home/nonroot/lakekeeper, non-root user), on a glibc base.
FROM debian:trixie-slim
RUN useradd --uid 65532 --create-home nonroot
COPY --chown=nonroot lakekeeper /home/nonroot/lakekeeper
USER nonroot
ENTRYPOINT ["/home/nonroot/lakekeeper"]
DOCKERFILE
docker build -q -t "iceberg-dotnet/lakekeeper:$TAG" "$WORK/image"
echo "built iceberg-dotnet/lakekeeper:$TAG"
