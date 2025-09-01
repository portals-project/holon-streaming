#!/usr/bin/env bash
set -euo pipefail

echo "Building & pushing all images…"

# 1) init-kafka
docker build \
  -f examples/nexmark-remote-java/docker/init-kafka/Dockerfile \
  -t rubyies/init-kafka-full-deployment:latest \
  .

# 2) holon-nodes
docker build \
  -f examples/nexmark-remote-java/docker/holon-node/Dockerfile \
  -t rubyies/holon-nodes-full-deployment:latest \
  .

# 3) nexmark-producers
docker build \
  -f examples/nexmark-remote-java/docker/nexmark-producer/Dockerfile \
  -t rubyies/nexmark-producers-full-deployment:latest \
  .

# 4) output-consumer
docker build \
  -f examples/nexmark-remote-java/docker/output-consumer/Dockerfile \
  -t rubyies/output-consumer-full-deployment:latest \
  .

echo "Logging into Docker Hub"
docker login

echo "Pushing…"
docker push rubyies/init-kafka-full-deployment:latest
docker push rubyies/holon-nodes-full-deployment:latest
docker push rubyies/nexmark-producers-full-deployment:latest
docker push rubyies/output-consumer-full-deployment:latest

echo "Done!"
