#!/usr/bin/env bash
set -euo pipefail

echo "Pulling init-kafka..."
docker pull rubyies/init-kafka:latest

echo "Pulling combined nexmark-producers image..."
docker pull rubyies/nexmark-producers:latest

echo "Pulling output-consumer..."
docker pull rubyies/output-consumer:latest

echo "Pulling combined holon-nodes image..."
docker pull rubyies/holon-nodes:latest

echo "All images pulled successfully."