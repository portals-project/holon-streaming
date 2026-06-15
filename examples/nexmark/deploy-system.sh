#!/usr/bin/env bash
set -euo pipefail

# Usage: ./examples/nexmark/deploy-system.sh install|upgrade
if [ $# -lt 1 ] || [[ ! "$1" =~ ^(install|upgrade)$ ]]; then
  echo "Usage: $0 <install|upgrade> [<namespace>]"
  exit 1
fi

ACTION="$1"
NAMESPACE="${2:-default}"

RELEASE_KAFKA="kafka"
CHART_KAFKA="examples/nexmark/kafka-chart"
RELEASE_HOLON="holon"
CHART_HOLON="examples/nexmark/holon-chart"
DEPLOYMENT_HOLON="holon-node-deployments"

## === CONFIGURE THIS ===
## Your Firestore project ID:
#TARGET_PROJECT="holon-458408"
## ======================
#
## 1) Where this script lives...
#SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
## 2) ...and your repo root is two levels up:
#PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
#KEY_FILE="${PROJECT_ROOT}/.gcp/holon-458408-gcs-service-account.json"
#
#if [ ! -f "$KEY_FILE" ]; then
#  echo "❌ Service-account key not found at $KEY_FILE"
#  exit 1
#fi
#
## 3) Activate the service account
#echo "→ Activating service account from $KEY_FILE"
#gcloud auth activate-service-account --key-file="$KEY_FILE"

echo "→ Helm $ACTION $RELEASE_KAFKA in namespace '$NAMESPACE'"
helm "$ACTION" "$RELEASE_KAFKA" "$CHART_KAFKA" \
  --namespace "$NAMESPACE" --create-namespace

echo "→ Waiting for deployment/$RELEASE_KAFKA to finish rolling out…"
kubectl rollout status deployment/"$RELEASE_KAFKA" \
  --namespace "$NAMESPACE" \
  --timeout=300s

echo "→ Waiting for init-kafka Job to complete…"
kubectl wait --for=condition=complete job/init-kafka \
  --namespace "$NAMESPACE" \
  --timeout=300s

echo "→ ✅ Kafka init Job is done"

echo "→ Helm $ACTION $RELEASE_HOLON in namespace '$NAMESPACE'"
helm "$ACTION" "$RELEASE_HOLON" "$CHART_HOLON" \
  --namespace "$NAMESPACE"

echo "→ Waiting for deployment/$DEPLOYMENT_HOLON to finish rolling out…"
kubectl rollout status deployment/"$DEPLOYMENT_HOLON" \
  --namespace "$NAMESPACE" \
  --timeout=300s

# List current time in this format "2025-05-26 13:05:26.64 UTC" for quering
echo "→ Current time in UTC: $(date -u +"%Y-%m-%d %H:%M:%S.%3N UTC")"

echo "✅ All releases are ready!"