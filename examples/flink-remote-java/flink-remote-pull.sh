#!/usr/bin/env bash
set -euo pipefail

echo "Pulling flink init-kafka..."
docker pull rubyies/flink-init-kafka-full-deployment:latest

echo "Pulling combined flink nexmark-producers image..."
docker pull rubyies/flink-nexmark-producers-full-deployment:latest

echo "Pulling flink output-consumer1..."
docker pull rubyies/flink-log-append-output-consumer-full-deployment:latest

echo "Pulling flink output-consumer2..."
docker pull rubyies/flink-json-output-consumer-full-deployment:latest

echo "All images pulled successfully."