#!/usr/bin/env bash
set -euo pipefail

echo "Pulling init-kafka..."
docker pull rubyies/init-kafka-full-deployment:latest

echo "Pulling combined nexmark-producers image..."
docker pull rubyies/nexmark-producers-full-deployment:latest

echo "Pulling output-consumer..."
docker pull rubyies/output-consumer-full-deployment:latest

echo "Pulling combined holon-nodes image..."
docker pull rubyies/holon-nodes-full-deployment:latest

echo "All images pulled successfully."