#!/usr/bin/env bash
set -euo pipefail

# Usage: bash examples/nexmark/deploy-kafka.sh install|upgrade
if [ $# -ne 1 ] || [[ ! "$1" =~ ^(install|upgrade)$ ]]; then
  echo "Usage: $0 <install|upgrade>"
  exit 1
fi

ACTION="$1"
RELEASE="kafka"
CHART="examples/nexmark/kafka-chart"

echo "→ Running: helm $ACTION $RELEASE $CHART"
helm "$ACTION" "$RELEASE" "$CHART"
