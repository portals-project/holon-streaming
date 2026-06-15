#!/usr/bin/env bash
# Run with: bash ./holon-remote-pull.sh   (requires bash: `set -o pipefail` is not POSIX sh)
if [ -z "${BASH_VERSION:-}" ]; then
  echo "This script must be run with bash, e.g.: bash $0 $*" >&2
  exit 1
fi
set -euo pipefail

echo "Pulling init-kafka..."
docker pull rubyies/init-kafka-full-deployment:latest

echo "Pulling combined nexmark-producers image..."
docker pull rubyies/nexmark-producers-full-deployment:latest

echo "Pulling combined holon-nodes image..."
docker pull rubyies/holon-nodes-full-deployment:latest

echo "Pulling output-consumer..."
docker pull rubyies/output-consumer-full-deployment:latest

echo "Pulling producer-start-gate..."
docker pull rubyies/producer-start-gate-full-deployment:latest

echo "All images pulled successfully."
