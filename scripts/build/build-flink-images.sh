#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

echo "Building & pushing all images…"

# 1) init-kafka
docker build \
  -f examples/flink-remote-java/docker/init-kafka/Dockerfile \
  -t rubyies/flink-init-kafka-full-deployment:latest \
  .

# 2) holon-nodes
#docker build \
#  -f examples/nexmark-remote-java/docker/holon-node/Dockerfile \
#  -t rubyies/holon-nodes-full-deployment:latest \
#  .

# 3) nexmark-producers
docker build \
  -f examples/flink-remote-java/docker/nexmark-producer-json/Dockerfile \
  -t rubyies/flink-nexmark-producers-full-deployment:latest \
  .

# 3b) producer-start-gate (HTTP /ready + /start)
docker build \
  -f examples/flink-remote-java/docker/producer-start-gate/Dockerfile \
  -t rubyies/flink-producer-start-gate-full-deployment:latest \
  examples/flink-remote-java/docker/producer-start-gate

# 4) output-consumer log append
docker build \
  -f examples/flink-remote-java/docker/lag-append-output-consumer/Dockerfile \
  -t rubyies/flink-lag-append-output-consumer-full-deployment:latest \
  .

docker build \
  -f examples/flink-remote-java/docker/output-consumer-json/Dockerfile \
  -t rubyies/flink-json-output-consumer-full-deployment:latest \
  .

echo "Logging into Docker Hub"
docker login

echo "Pushing…"
docker push rubyies/flink-init-kafka-full-deployment:latest
docker push rubyies/flink-nexmark-producers-full-deployment:latest
docker push rubyies/flink-producer-start-gate-full-deployment:latest
docker push rubyies/flink-lag-append-output-consumer-full-deployment:latest
docker push rubyies/flink-json-output-consumer-full-deployment:latest

echo "Done!"
