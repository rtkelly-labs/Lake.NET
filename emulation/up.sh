#!/usr/bin/env bash
# Start the full-emulation stack (moto + Iceberg REST fixture) and wait until both answer.
set -euo pipefail
cd "$(dirname "$0")"
docker compose up -d --wait
echo "moto (S3 + Glue):      http://localhost:5000"
echo "Iceberg REST fixture:  http://localhost:8181"
