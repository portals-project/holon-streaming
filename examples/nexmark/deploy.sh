#!/usr/bin/env bash
set -euo pipefail

echo "→ Starting Nexmark deployment to GKE"

gcloud auth configure-docker

for IMAGE in nexmark-producer output-consumer holon-node; do
  echo "→ Building & pushing $IMAGE"
  docker build -t "gcr.io/holon-458408/$IMAGE:latest" \
    -f "examples/nexmark/docker/$IMAGE/Dockerfile" .
  docker push "gcr.io/holon-458408/$IMAGE:latest"
done

gcloud config set project holon-458408
gcloud config set compute/zone us-central1-a
gcloud container clusters get-credentials crdt --zone us-central1-a

echo "✅ All done!"
