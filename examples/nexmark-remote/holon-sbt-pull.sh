#!/usr/bin/env bash
# Run with: bash ./holon-sbt-pull.sh
if [ -z "${BASH_VERSION:-}" ]; then
  echo "This script must be run with bash, e.g.: bash $0 $*" >&2
  exit 1
fi
set -euo pipefail

# Pre-pull the images the SBT "on-the-fly" stack needs. Only the dependency-baked
# base image and the start gate are private; kafka/kafka-ui are public and pulled
# by `docker compose up` anyway, but we pull them here so the bring-up is fast.

echo "Pulling holon-sbt base image (deps baked, no source)…"
docker pull rubyies/holon-sbt:latest

echo "Pulling producer-start-gate…"
docker pull rubyies/producer-start-gate-full-deployment:latest

echo "Pulling Kafka + Kafka UI…"
docker pull apache/kafka:4.0.2
docker pull ghcr.io/kafbat/kafka-ui:latest

echo "All images pulled successfully."
