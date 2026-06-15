#!/usr/bin/env bash
set -euo pipefail

# 📌 make sure you run this from your project root (where .gcp/ lives):
#     cd /path/to/my-project && ./deploy.sh
# Run:
# cd /path/to/holon-streaming root folder
# chmod +x examples/nexmark/deploy.sh
# bash examples/nexmark/deploy.sh

PROJECT="holon-458408"
ZONE="us-central1-c"
CLUSTER="crdt"
REPO="gcr.io/${PROJECT}"
IMAGES=(nexmark-producer output-consumer holon-node)

echo "→ Configuring Docker to use GCR credentials"
gcloud auth configure-docker

for IMAGE in "${IMAGES[@]}"; do
  echo "→ Building & pushing ${IMAGE}"
  docker build \
    -t "${REPO}/${IMAGE}-ruben:latest" \
    -f "examples/nexmark/docker/${IMAGE}/Dockerfile" \
    .
  docker push "${REPO}/${IMAGE}-ruben:latest"
done

echo "→ Setting GCP project & compute zone"
gcloud config set project "${PROJECT}"
gcloud config set compute/zone "${ZONE}"

echo "→ Fetching GKE credentials for cluster ${CLUSTER}"
gcloud container clusters get-credentials "${CLUSTER}" --zone "${ZONE}"

echo "✅ All done!"
