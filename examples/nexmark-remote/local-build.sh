#!/usr/bin/env bash
set -euo pipefail

# Buildingg Holon cluster deployment located in *
echo "=== Building init-kafka ==="
docker build \
  -f docker/init-kafka/Dockerfile \
  -t rubyies/init-kafka:latest \
  ../..

echo
echo "=== Building combined nexmark-producers ==="
docker build \
  -f docker/nexmark-producer/Dockerfile \
  -t rubyies/nexmark-producers:latest \
  ../..

echo
echo "=== Building output-consumer ==="
docker build \
  -f docker/output-consumer/Dockerfile \
  -t rubyies/output-consumer:latest \
  ../..

echo
echo "=== Building combined holon-nodes ==="
docker build \
  -f docker/holon-node/Dockerfile \
  -t rubyies/holon-nodes:latest \
  ../..

echo
echo "=== Logging into Docker Hub ==="
docker login

echo
echo "=== Pushing images ==="
docker push rubyies/init-kafka:latest
docker push rubyies/nexmark-producers:latest
docker push rubyies/output-consumer:latest
docker push rubyies/holon-nodes:latest

echo
echo "All images built and pushed"
