#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile trino --profile polaris down --remove-orphans
