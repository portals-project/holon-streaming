#!/usr/bin/env bash
# Run with: bash ./flink-remote-pull.sh   (requires bash: `set -o pipefail` is not POSIX sh)
if [ -z "${BASH_VERSION:-}" ]; then
  echo "This script must be run with bash, e.g.: bash $0 $*" >&2
  exit 1
fi
set -euo pipefail

echo "Pulling flink init-kafka..."
docker pull rubyies/flink-init-kafka-full-deployment:latest

echo "Pulling combined flink nexmark-producers image..."
docker pull rubyies/flink-nexmark-producers-full-deployment:latest

echo "Pulling producer-start-gate (generic, shared with Holon)..."
docker pull rubyies/producer-start-gate-full-deployment:latest

echo "Pulling flink output-consumer1..."
docker pull rubyies/flink-lag-append-output-consumer-full-deployment:latest

echo "Pulling flink output-consumer2..."
docker pull rubyies/flink-json-output-consumer-full-deployment:latest

echo "All images pulled successfully."